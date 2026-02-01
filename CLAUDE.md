# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

DynamiteDB is a local, embedded, DynamoDB-style document database written in Rust (edition 2024). It stores JSON documents with partition key + optional sort key in a single file using copy-on-write pages. Designed for multi-GB databases with full MVCC transactions.

## Build Commands

This is a Cargo workspace. Build/test from the repository root:

- `cargo build` - compile all crates
- `cargo test` - run all tests across the workspace
- `cargo test -p dynamite-core` - test only the core crate
- `cargo test -p dynamite-core <test_name>` - run a single test by name
- `cargo clippy --workspace` - lint all crates
- `cargo fmt --all` - format all crates
- `cargo fmt --all --check` - check formatting without modifying files
- `cargo bench -p dynamite-core` - run benchmarks (criterion, uses tmpfs by default)
- `BENCH_DIR=/var/home/travis/development/dyna_mite/target/bench_real cargo bench --bench dynamite_file_bench` - run file-backed benchmarks on real NVMe (only after major refactors)

## Architecture

Six-layer stack, bottom to top:

1. **Storage Engine** (`storage/`) - mmap-based file I/O, atomic commits. Single file with 4KB pages. Double-buffered header (pages 0 and 1) for truly atomic commits. File locking via `flock()`.
2. **Page Manager** (`storage/`) - Page allocation, free list, copy-on-write. Never modifies pages in place; writes new pages then atomically updates the alternate header. Crash recovery = use last committed header. xxHash64 checksums on every page.
3. **B+Tree Index** (`btree/`) - One tree per table, keyed by `(partition_key, sort_key)`. Slotted page layout. Internal nodes hold `[key, child_page_id]` pairs; leaf nodes hold `[key, document]` pairs with linked `next_leaf` pointers. Overflow pages for large documents.
4. **Key Encoding** (`encoding/`) - Byte-ordered encoding for comparable `memcmp` keys. Strings/binary use escaped-terminator scheme (`0x00` → `0x00 0xFF`, terminated with `0x00 0x00`). Numbers use byte-flipped IEEE 754.
5. **Table Catalog** (`catalog/`) - Schema definitions (table name, partition key name+type, optional sort key name+type). Stored in its own B+Tree rooted from the header.
6. **MVCC / Transactions** (`mvcc/`) - Snapshot isolation only (v1). Single writer, unlimited concurrent readers. Latest document version inline in B+Tree leaf, older versions in overflow chain. Each document carries `created_txn` and `deleted_txn` IDs. Visibility: `created_txn <= snapshot && (deleted_txn is None || deleted_txn > snapshot)`.

Public API (`api/`) sits on top: `put/get/delete/query/scan/transact`.

## Workspace Layout

```
crates/
  dynamite-core/     # Core database engine (lib crate)
    src/
      storage/       # File I/O, page management, mmap
      btree/         # B+Tree implementation
      mvcc/          # Transaction manager, snapshots
      catalog/       # Table schemas
      api/           # Public API types
      encoding/      # Key/value serialization
tests/
  integration/       # Cross-crate integration tests
```

## Key Design Decisions

- **Copy-on-write over WAL**: Simpler crash recovery, no separate log file
- **mmap for reads**: OS handles caching, simplifies buffer management
- **B+Tree over LSM**: Better read performance for the embedded use case
- **4KB pages**: Matches OS page size for mmap alignment
- **Single writer, unlimited readers**: LMDB concurrency model
- **Slotted pages**: Slot array grows forward, cell data grows backward
- **No secondary indexes in v1**
- **No B+Tree rebalancing in v1**: Mark-as-dead delete, reclaim fully empty pages

## Development Process

Work proceeds incrementally. Every change must leave the project in a fully working state:

1. **Compile first**: `cargo build -p dynamite-core` must pass with zero errors before moving on.
2. **Test everything**: Write tests for each new piece of functionality before considering it done. Run `cargo test -p dynamite-core` and confirm all tests pass.
3. **Lint clean**: `cargo clippy --workspace -- -D warnings` must pass. No warnings allowed.
4. **Format**: `cargo fmt --all --check` must pass.
5. **No dead code**: Don't stub out modules or leave `todo!()` / `unimplemented!()` in committed code. Each step should produce working, tested code — not scaffolding for the future.
6. **One layer at a time**: Build bottom-up through the architecture. Do not start a higher layer until the layer beneath it compiles, passes tests, and is lint-clean.

## Benchmarks

Two benchmark suites exist in `dynamite-core`:

- `dynamite_bench` — in-memory (tmpfs) microbenchmarks. Run these routinely.
- `dynamite_file_bench` — file-backed benchmarks with real I/O. Uses tmpfs by default; set `BENCH_DIR` to point at real storage.

**Default workflow**: run benchmarks on tmpfs (`cargo bench`). Only run on real NVMe disk after major refactors where all in-memory benchmarks have improved or not regressed. NVMe results are dominated by fsync latency (~5ms per commit) which masks algorithmic changes.

## Core Dependencies

- `serde` / `serde_json` - JSON document serialization
- `memmap2` - Memory-mapped file I/O
- `parking_lot` - Fast RwLock for concurrency
- `thiserror` - Error types
- `bytes` - Byte buffer manipulation
- `xxhash-rust` - Page checksums
- `tempfile` (dev) - Test isolation
- `criterion` (dev) - Benchmarks
