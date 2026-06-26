# Vendored: Apache Arrow nanoarrow (with IPC) — v0.8.0

This directory vendors the **nanoarrow** C library amalgamation + its **nanoarrow_ipc**
encoder and the **flatcc** runtime it depends on. It is used by the `streamed_table()`
TCP producer (`src/shm_producer.c`, the `arrow:` transport) to serialise each block as a
standards-valid **Apache Arrow IPC** encapsulated message (Schema + RecordBatch), so the
wire speaks a standard columnar format the stock ClickHouse Arrow reader can decode
(Hot-Cold Phase 2, Branch A — decision `D-HC-0202`).

## Provenance
- Upstream: https://github.com/apache/arrow-nanoarrow
- Release: **apache-arrow-nanoarrow-0.8.0** (`NANOARROW_VERSION "0.8.0"`).
- License: Apache-2.0 (`LICENSE.txt`, `NOTICE.txt`).

## Contents
- `src/nanoarrow.c`      — core nanoarrow amalgamation.
- `src/nanoarrow_ipc.c`  — IPC encoder/decoder + inlined flatcc-generated flatbuffer
                           readers/builders (`Message`/`Schema`/`RecordBatch`).
- `src/flatcc.c`         — flatcc builder/emitter/verifier runtime (required by the IPC encoder).
- `include/nanoarrow/`   — public headers (`nanoarrow.h`, `nanoarrow_ipc.h`, + .hpp).
- `include/flatcc/`      — flatcc runtime headers (+ `portable/`).

## How it was produced
The single-file bundle is the upstream CMake "bundled" output (the `bundle.py` /
`-DNANOARROW_BUNDLE=ON -DNANOARROW_IPC=ON` target), which concatenates the generated
flatbuffer headers into `nanoarrow_ipc.c` so the three `.c` files compile self-contained
with only `-Iinclude`.

## Build wiring (see top-level Makefile)
Compiled into `MODULE_big` as three explicit objects (`src/nanoarrow/src/*.o`), kept out of
the `src/*/*.c` auto-glob by living one level deeper. Built with warnings relaxed (`-w`)
since it is third-party generated code; the base extension keeps `-Wall -Werror`. Enabled by
`-DPGCH_USE_NANOARROW` (guarded on the header's presence, mirroring `PGCH_USE_LIBURING`); the
Arrow producer path is `#ifdef PGCH_USE_NANOARROW`.

Do not hand-edit the vendored sources. To upgrade, re-bundle the desired upstream release
and replace this directory wholesale, then update the version above.
