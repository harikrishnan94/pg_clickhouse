#!/usr/bin/env python3
"""Parse pgch_* SHM ring files: per-slot state/sequence/retain_refcount.
Decisive for the deadlock: are non-EMPTY slots PUBLISHED+retained (held by hash
join, ring-capacity exhaustion) or PUBLISHED+refcount0 (published, never drained =
missed wakeup)?"""
import glob, struct, sys, os

STATE = {0: "EMPTY", 1: "WRITING", 2: "PUBLISHED"}

def parse(path):
    with open(path, "rb") as f:
        data = f.read(4096)  # handshake + slot table fit in first pages
    magic, abi, k, schema_count, _pad = struct.unpack_from("<QIIII", data, 0)
    slot_off, slot_stride = struct.unpack_from("<QQ", data, 24)
    out = []
    # re-read enough to cover slot table
    need = slot_off + k * slot_stride
    if need > len(data):
        with open(path, "rb") as f:
            data = f.read(need + 64)
    for i in range(k):
        base = slot_off + i * slot_stride
        state, slot_index = struct.unpack_from("<II", data, base)
        tc, seq, refc = struct.unpack_from("<QQQ", data, base + 8)
        eos = data[base + 32]
        row_count, = struct.unpack_from("<Q", data, base + 40)
        out.append((i, STATE.get(state, state), seq, refc, eos, row_count, tc))
    return k, out

files = sorted(glob.glob("/dev/shm/pgch_*"))
if not files:
    print("no rings")
    sys.exit(0)
agg = {}
for path in files:
    try:
        k, slots = parse(path)
    except Exception as e:
        print(f"{os.path.basename(path)}: ERR {e}")
        continue
    name = os.path.basename(path)
    cells = []
    for (i, st, seq, refc, eos, rc, tc) in slots:
        cells.append(f"s{i}:{st}/seq{seq}/ref{refc}/eos{eos}/rows{rc}")
        agg[st] = agg.get(st, 0) + 1
    print(f"{name}: " + "  ".join(cells))
print("\n=== aggregate slot-state counts across all rings ===")
for st, c in sorted(agg.items()):
    print(f"  {st}: {c}")
