/*-------------------------------------------------------------------------
 *
 * pgch_jit.cpp
 *      OPTIONAL LLVM ORC-JIT deform module (built into pg_clickhouse_jit.so,
 *      separate from the base extension). Implements pgch_jit_get(): given a
 *      scan's PgchDeformDesc it JIT-compiles a *fused* row-at-a-time deform
 *      (Strategy R, validated byte-identical against the AOT path in
 *      dev/bench/llvm_deform_jit_spike.cpp) and caches it process-wide keyed by
 *      plan shape. The base extension loads this module lazily via
 *      load_external_function and falls back to the AOT step-plan whenever the
 *      module is absent or returns NULL.
 *
 *      Reuses PostgreSQL's bundled libLLVM (the same one postgresql-NN-jit
 *      ships); the base extension never links LLVM. Compiled with the LLVM C++
 *      API (the experiment confirmed it builds cleanly under the extension's
 *      -fno-exceptions -fno-rtti discipline), so no C-API rewrite was needed.
 *
 *      Scope: every projected column must be a JIT-emittable fixed wire; any
 *      projected string (or unsupported wire) makes pgch_jit_get return NULL so
 *      the caller keeps the AOT path (which owns the StringInfo fills).
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */

/* LLVM/C++ headers FIRST: PostgreSQL's utils/datetime.h defines short tokens
 * (AM/PM/TZ/DAY/...) that collide with LLVM template parameter names. */
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <deque>
#include <functional>

#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Verifier.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Passes/PassBuilder.h"
#include "llvm/Passes/OptimizationLevel.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/ExecutionEngine/Orc/ThreadSafeModule.h"
#include "llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h"

extern "C"
{
#include "postgres.h"
#include "fmgr.h"
#include "access/tupmacs.h"     /* TYPALIGN_* */
#include "varatt.h"
}
#include "shm_deform.h"
#include "pgch_jit.h"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf

using namespace llvm;

extern "C" { PG_MODULE_MAGIC; }

namespace
{

/* ---- wire metadata (mirrors shm_deform.cpp's fill dispatch) ----------- */

enum Xf { XF_ID, XF_DATE, XF_BOOL };
struct WireInfo { int src_bytes; bool src_float; int dst_bytes; Xf xf; };

static bool
wire_supported(ShmWireType w)
{
    switch (w)
    {
        case SHM_WIRE_UINT8: case SHM_WIRE_INT16: case SHM_WIRE_INT32:
        case SHM_WIRE_INT64: case SHM_WIRE_FLOAT32: case SHM_WIRE_FLOAT64:
        case SHM_WIRE_DATE:
            return true;
        default:
            return false;
    }
}

static WireInfo
wire_info(ShmWireType w)
{
    switch (w)
    {
        case SHM_WIRE_UINT8:   return {1, false, 1, XF_BOOL};
        case SHM_WIRE_INT16:   return {2, false, 2, XF_ID};
        case SHM_WIRE_INT32:   return {4, false, 4, XF_ID};
        case SHM_WIRE_INT64:   return {8, false, 8, XF_ID};
        case SHM_WIRE_FLOAT32: return {4, true,  4, XF_ID};
        case SHM_WIRE_FLOAT64: return {8, true,  8, XF_ID};
        case SHM_WIRE_DATE:    return {4, false, 2, XF_DATE};
        default:               return {8, false, 8, XF_ID};
    }
}

static uint8_t
align_bytes(char a)
{
    switch (a)
    {
        case TYPALIGN_DOUBLE: return 8;
        case TYPALIGN_INT:    return 4;
        case TYPALIGN_SHORT:  return 2;
        default:              return 1;
    }
}

/* ---- IR emitter: fused row-at-a-time deform (Strategy R) -------------- */

class Emitter
{
public:
    LLVMContext         &C;
    Module              &M;
    IRBuilder<>          B;
    const PgchDeformDesc *d;
    bool                 group_b;
    Type *I8,*I16,*I32,*I64,*F32,*F64,*PTR,*VOID;

    Emitter(LLVMContext &c, Module &m, const PgchDeformDesc *desc, bool gb)
        : C(c), M(m), B(c), d(desc), group_b(gb)
    {
        I8=B.getInt8Ty(); I16=B.getInt16Ty(); I32=B.getInt32Ty(); I64=B.getInt64Ty();
        F32=B.getFloatTy(); F64=B.getDoubleTy(); PTR=B.getPtrTy(); VOID=B.getVoidTy();
    }

    Value *cI64(int64_t v){ return ConstantInt::get(I64, (uint64_t) v); }
    Value *cI32(int32_t v){ return ConstantInt::get(I32, (uint32_t) v); }

    Type *srcType(const WireInfo &wi)
    {
        if (wi.src_float) return wi.src_bytes == 8 ? F64 : F32;
        switch (wi.src_bytes) { case 1: return I8; case 2: return I16; case 4: return I32; default: return I64; }
    }
    Type *dstType(const WireInfo &wi)
    {
        switch (wi.dst_bytes) { case 1: return I8; case 2: return I16; case 4: return I32; default: return I64; }
    }

    static int pow2_of(int64_t x){ if (x==0) return 1<<30; int a=0; while(((x>>a)&1)==0) a++; return 1<<a; }

    Value *ptrToI(Value *p){ return B.CreatePtrToInt(p, I64); }
    Value *iToPtr(Value *i){ return B.CreateIntToPtr(i, PTR); }
    Value *alignI(Value *ci, int A)
    {
        if (A <= 1) return ci;
        return B.CreateAnd(B.CreateAdd(ci, cI64(A - 1)), cI64(~(int64_t)(A - 1)));
    }
    Value *loadAtI(Type *t, Value *ci){ return B.CreateAlignedLoad(t, iToPtr(ci), Align(1)); }

    Value *nullBit(Value *bp, int attno)
    {
        Value *byte = B.CreateAlignedLoad(I8, B.CreateGEP(I8, bp, cI64(attno >> 3)), Align(1));
        Value *m = ConstantInt::get(I8, 1u << (attno & 7));
        return B.CreateICmpNE(B.CreateAnd(byte, m), ConstantInt::get(I8, 0));
    }

    Value *advFixed(Value *ci, int A, int len){ return B.CreateAdd(alignI(ci, A), cI64(len)); }

    /* inline VARSIZE_ANY (1B/4B header) + VARATT_NOT_PAD_BYTE; branchless. No
     * external-toast handling (heap varlena skips here are inline datums). */
    Value *advVarlena(Value *ci, int A)
    {
        Value *b0 = loadAtI(I8, ci);
        Value *notpad = B.CreateICmpNE(b0, ConstantInt::get(I8, 0));
        Value *ci1 = B.CreateSelect(notpad, ci, alignI(ci, A));
        Value *b = loadAtI(I8, ci1);
        Value *is1b = B.CreateICmpEQ(B.CreateAnd(b, ConstantInt::get(I8, 1)), ConstantInt::get(I8, 1));
        Value *sz1 = B.CreateAnd(B.CreateLShr(B.CreateZExt(b, I32), cI32(1)), cI32(0x7f));
        Value *h4 = loadAtI(I32, ci1);
        Value *sz4 = B.CreateAnd(B.CreateLShr(h4, cI32(2)), cI32(0x3fffffff));
        Value *sz = B.CreateSelect(is1b, sz1, sz4);
        return B.CreateAdd(ci1, B.CreateZExt(sz, I64));
    }

    Value *xform(const WireInfo &wi, Value *v)
    {
        switch (wi.xf)
        {
            case XF_ID:   return v;
            case XF_DATE: return B.CreateTrunc(B.CreateAdd(v, cI32(PGCH_DATE_EPOCH_DIFF)), I16);
            case XF_BOOL: return B.CreateZExt(B.CreateICmpNE(v, ConstantInt::get(I8, 0)), I8);
        }
        return v;
    }

    /* projected fixed fill: load at integer cursor `ci`, transform, store into
     * dst_base[col_index] (base pre-loaded into `dstBase`) at row dstIdx. */
    void doFill(const PgchDeformCol &c, Value *ci, Value *dstIdx, Value *dstBase)
    {
        WireInfo wi = wire_info(c.wire);
        Value *v = loadAtI(srcType(wi), ci);
        Value *out = xform(wi, v);
        B.CreateStore(out, B.CreateGEP(dstType(wi), dstBase, dstIdx));
    }

    void countedLoop(Function *F, Value *n, std::function<void(Value*)> body)
    {
        BasicBlock *pre  = B.GetInsertBlock();
        BasicBlock *head = BasicBlock::Create(C, "head", F);
        BasicBlock *bb   = BasicBlock::Create(C, "body", F);
        BasicBlock *exit = BasicBlock::Create(C, "exit", F);
        B.CreateBr(head);
        B.SetInsertPoint(head);
        PHINode *r = B.CreatePHI(I64, 2, "r");
        r->addIncoming(cI64(0), pre);
        B.CreateCondBr(B.CreateICmpULT(r, n), bb, exit);
        B.SetInsertPoint(bb);
        body(r);
        BasicBlock *cont = B.GetInsertBlock();
        r->addIncoming(B.CreateAdd(r, cI64(1)), cont);
        B.CreateBr(head);
        B.SetInsertPoint(exit);
    }

    /* Cursor builder: folds runs of fixed hops to a constant offset, emitting an
     * align only when alignment increases (== AOT const-collapse, baked in IR). */
    struct Cur
    {
        Emitter *E; Value *anchor; int64_t off; int alignA;
        Cur(Emitter *e, Value *base_i, int64_t lead, int baseAlign)
            : E(e), anchor(base_i), off(lead), alignA(baseAlign) {}
        int curAlign() const { if (off==0) return alignA; int vo=pow2_of(off); return vo<alignA?vo:alignA; }
        Value *addr() { return off ? E->B.CreateAdd(anchor, E->cI64(off)) : anchor; }
        void alignTo(int A) {
            if (A<=1 || curAlign()>=A) return;
            if (alignA>=A) { off = (off + (A-1)) & ~(int64_t)(A-1); return; }
            anchor = E->alignI(addr(), A); off = 0; alignA = A;
        }
        void addFixed(int A, int L) { alignTo(A); off += L; }
        void addVarlena(int A) { anchor = E->advVarlena(addr(), A); off = 0; alignA = 1; }
        void addNullable(Value *bp, int attno, int A, int len /* <0 varlena */) {
            Value *c = addr();
            Value *adv = (len < 0) ? E->advVarlena(c, A) : E->advFixed(c, A, len);
            anchor = E->B.CreateSelect(E->nullBit(bp, attno), adv, c); off = 0; alignA = 1;
        }
    };

    /* Returns the compiled function name, or empty string if ineligible. */
    std::string emit(const std::string &fname, Value **dstBaseSlots /* unused placeholder */)
    {
        (void) dstBaseSlots;
        int      prefix_len = group_b ? d->prefix_len_b : d->prefix_len_a;
        uint32_t walk_off   = group_b ? d->walk_start_off_b : d->walk_start_off_a;
        int      last_needed = -1;
        for (int i = 0; i < d->max_attno; i++) if (d->col[i].is_needed) last_needed = i;

        FunctionType *FT = FunctionType::get(VOID, {PTR, PTR, I64, I64, PTR}, false);
        Function *F = Function::Create(FT, GlobalValue::ExternalLinkage, fname, M);
        auto A = F->arg_begin();
        Value *cur = &*A++; Value *bits = &*A++; Value *nn = &*A++;
        Value *dstRow = &*A++; Value *dstBases = &*A++;

        BasicBlock *entry = BasicBlock::Create(C, "entry", F);
        B.SetInsertPoint(entry);

        /* pre-load each projected column's dst base (loop-invariant). */
        std::unordered_map<int, Value*> baseOf;
        for (int i = 0; i < d->max_attno; i++)
            if (d->col[i].is_needed)
            {
                int k = d->col[i].col_index;
                Value *slot = B.CreateGEP(PTR, dstBases, cI64(k));
                baseOf[k] = B.CreateAlignedLoad(PTR, slot, Align(8));
            }

        countedLoop(F, nn, [&](Value *r) {
            Value *dstIdx = B.CreateAdd(dstRow, r);
            Value *base   = B.CreateAlignedLoad(PTR, B.CreateGEP(PTR, cur, r), Align(8));
            Value *basei  = ptrToI(base);
            Value *bp     = group_b ? B.CreateAlignedLoad(PTR, B.CreateGEP(PTR, bits, r), Align(8)) : nullptr;

            /* prefix: projected fixed cols at constant displacement (no advance). */
            for (int i = 0; i < prefix_len; i++)
                if (d->col[i].is_needed)
                    doFill(d->col[i], B.CreateAdd(basei, cI64(d->col[i].disp)), dstIdx,
                           baseOf[d->col[i].col_index]);

            if (last_needed < prefix_len) return;

            Cur cur2(this, basei, (int64_t) walk_off, 8);   /* tuple data start is MAXALIGN */
            for (int i = prefix_len; i <= last_needed; i++)
            {
                const PgchDeformCol &c = d->col[i];
                int A2 = align_bytes(c.attalign);
                if (c.is_needed)
                {
                    WireInfo wi = wire_info(c.wire);
                    cur2.alignTo(wi.src_bytes);
                    doFill(c, cur2.addr(), dstIdx, baseOf[c.col_index]);
                    cur2.off += wi.src_bytes;
                }
                else if (group_b && c.nullable)
                    cur2.addNullable(bp, i, A2, c.attlen > 0 ? c.attlen : -1);
                else if (c.attlen > 0)
                    cur2.addFixed(A2, c.attlen);
                else
                    cur2.addVarlena(A2);
            }
        });
        B.CreateRetVoid();
        return fname;
    }
};

/* ---- O2 pipeline ------------------------------------------------------ */

static void
runOpt(Module &M)
{
    PassBuilder PB;
    LoopAnalysisManager LAM; FunctionAnalysisManager FAM;
    CGSCCAnalysisManager CGAM; ModuleAnalysisManager MAM;
    PB.registerModuleAnalyses(MAM);
    PB.registerCGSCCAnalyses(CGAM);
    PB.registerFunctionAnalyses(FAM);
    PB.registerLoopAnalyses(LAM);
    PB.crossRegisterProxies(LAM, FAM, CGAM, MAM);
    ModulePassManager MPM = PB.buildPerModuleDefaultPipeline(OptimizationLevel::O2);
    MPM.run(M, MAM);
}

/* ---- process-wide JIT + shape-keyed cache ----------------------------- */

struct JitEntry { PgchJitDeform fn; orc::ResourceTrackerSP rt; };

static std::unique_ptr<orc::LLJIT>          g_jit;
static bool                                 g_jit_failed = false;
static std::unordered_map<std::string, JitEntry> g_cache;
static std::deque<std::string>              g_order;        /* FIFO eviction */
static const size_t                         CACHE_CAP = 256;
static int                                  g_ctr = 0;

static bool
ensure_jit()
{
    if (g_jit) return true;
    if (g_jit_failed) return false;
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    InitializeNativeTargetAsmParser();
    auto JTMB = orc::JITTargetMachineBuilder::detectHost();
    if (!JTMB) { consumeError(JTMB.takeError()); g_jit_failed = true; return false; }
    JTMB->setCodeGenOptLevel(CodeGenOptLevel::Aggressive);
    auto J = orc::LLJITBuilder().setJITTargetMachineBuilder(std::move(*JTMB)).create();
    if (!J) { consumeError(J.takeError()); g_jit_failed = true; return false; }
    g_jit = std::move(*J);
    return true;
}

/* canonical, collision-safe shape key: the codegen-relevant desc fields. */
static std::string
shape_key(const PgchDeformDesc *d, bool group_b)
{
    std::string k;
    auto put = [&](const void *p, size_t n){ k.append((const char*) p, n); };
    char g = group_b ? 1 : 0; put(&g, 1);
    put(&d->max_attno, sizeof(int));
    int pl = group_b ? d->prefix_len_b : d->prefix_len_a;
    uint32_t wo = group_b ? d->walk_start_off_b : d->walk_start_off_a;
    put(&pl, sizeof(int)); put(&wo, sizeof(uint32_t));
    for (int i = 0; i < d->max_attno; i++)
    {
        const PgchDeformCol &c = d->col[i];
        put(&c.attlen, sizeof(int16)); put(&c.attalign, 1);
        char flags = (char)((c.nullable?1:0) | (c.is_needed?2:0) | (c.is_string?4:0));
        put(&flags, 1);
        int w = (int) c.wire; put(&w, sizeof(int));
        put(&c.col_index, sizeof(int)); put(&c.disp, sizeof(uint32_t));
    }
    return k;
}

/* eligibility: every projected column must be a JIT-emittable fixed wire. */
static bool
plan_eligible(const PgchDeformDesc *d)
{
    for (int i = 0; i < d->max_attno; i++)
    {
        const PgchDeformCol &c = d->col[i];
        if (!c.is_needed) continue;
        if (c.is_string || c.attlen <= 0 || !wire_supported(c.wire))
            return false;
    }
    return true;
}

static PgchJitDeform
compile_one(const PgchDeformDesc *d, bool group_b, orc::ResourceTrackerSP &rt_out)
{
    auto ctx = std::make_unique<LLVMContext>();
    auto Mod = std::make_unique<Module>("pgch_deform", *ctx);
    Mod->setDataLayout(g_jit->getDataLayout());
    Mod->setTargetTriple(g_jit->getTargetTriple());

    std::string fname = "pgch_jit_" + std::string(group_b ? "B" : "A") + "_" + std::to_string(g_ctr++);
    Emitter E(*ctx, *Mod, d, group_b);
    E.emit(fname, nullptr);

    if (verifyModule(*Mod)) return nullptr;       /* malformed IR: decline */
    runOpt(*Mod);

    orc::ResourceTrackerSP rt = g_jit->getMainJITDylib().createResourceTracker();
    if (auto err = g_jit->addIRModule(rt, orc::ThreadSafeModule(std::move(Mod), std::move(ctx))))
    { consumeError(std::move(err)); return nullptr; }
    auto sym = g_jit->lookup(fname);
    if (!sym) { consumeError(sym.takeError()); cantFail(rt->remove()); return nullptr; }
    rt_out = rt;
    return reinterpret_cast<PgchJitDeform>(sym->getValue());
}

}   /* anonymous namespace */

/* ---- public C ABI ----------------------------------------------------- */

extern "C" PGDLLEXPORT PgchJitDeform
pgch_jit_get(const PgchDeformDesc *desc, bool group_b)
{
    if (!plan_eligible(desc)) return nullptr;
    if (!ensure_jit()) return nullptr;

    std::string key = shape_key(desc, group_b);
    auto it = g_cache.find(key);
    if (it != g_cache.end()) return it->second.fn;

    if (g_cache.size() >= CACHE_CAP && !g_order.empty())
    {
        /* evict oldest: drop its JIT'd code, then the cache entry. */
        std::string victim = g_order.front(); g_order.pop_front();
        auto v = g_cache.find(victim);
        if (v != g_cache.end()) { cantFail(v->second.rt->remove()); g_cache.erase(v); }
    }

    orc::ResourceTrackerSP rt;
    PgchJitDeform fn = compile_one(desc, group_b, rt);
    if (!fn) return nullptr;

    g_cache.emplace(key, JitEntry{fn, rt});
    g_order.push_back(key);
    return fn;
}
