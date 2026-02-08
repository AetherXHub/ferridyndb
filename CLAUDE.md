# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

FerridynDB is a local, embedded, DynamoDB-style document database written in Rust (edition 2024). It stores JSON documents with partition key + optional sort key in a single file using copy-on-write pages. The name combines ferric iron, Ferris the Rust crab, and dynamo (from DynamoDB).

## Why

DynamoDB's API is simple and effective for key-value and document workloads, but requires AWS. FerridynDB brings that same API to a single-file embedded database — no network, no cloud, no WAL. Useful for local-first apps, CLI tools, and as a backing store for systems like the ferridyn-memory Claude Code plugin.

## Build Commands

This is a Cargo workspace. Build/test from the repository root:

- `cargo build` — compile all crates
- `cargo test` — run all tests across the workspace (659 tests)
- `cargo test -p ferridyn-core` — test only the core crate
- `cargo test -p ferridyn-core <test_name>` — run a single test by name
- `cargo clippy --workspace -- -D warnings` — lint all crates (zero warnings required)
- `cargo fmt --all` — format all crates
- `cargo fmt --all --check` — check formatting without modifying files
- `cargo bench -p ferridyn-core` — run benchmarks (criterion, uses tmpfs by default)
- `BENCH_DIR=/path/to/nvme cargo bench --bench ferridyn_file_bench` — file-backed benchmarks on real storage

## Architecture

Six-layer stack, bottom to top:

1. **Storage Engine** (`storage/`) — Standard file I/O with 4KB pages. Double-buffered header (pages 0 and 1) for atomic commits. File locking via `flock()` (fs2 crate). No mmap, no WAL.
2. **Page Manager** (`storage/`) — Page allocation, free list, copy-on-write. Never modifies pages in place; writes new pages then atomically updates the alternate header. Crash recovery = use last committed header. xxHash64 checksums on every page.
3. **B+Tree Index** (`btree/`) — One tree per table, keyed by `(partition_key, sort_key)`. Slotted page layout. Internal nodes hold `[key, child_page_id]` pairs; leaf nodes hold `[key, document]` pairs with linked `next_leaf` pointers. Overflow pages for large documents.
4. **Key Encoding** (`encoding/`) — Byte-ordered encoding for `memcmp`-comparable keys. Strings use null-terminated UTF-8 (null bytes rejected at validation). Binary uses escaped-terminator scheme (`0x00` → `0x00 0x01`, terminated with `0x00 0x00`). Numbers use byte-flipped IEEE 754.
5. **Table Catalog** (`catalog/`) — Schema definitions (table name, partition key name+type, optional sort key name+type), partition schemas (prefix-based entity type metadata with attributes), and secondary index definitions. Stored in its own B+Tree rooted from the header.
6. **MVCC / Transactions** (`mvcc/`) — Snapshot isolation. Single writer, unlimited concurrent readers. Latest document version inline in B+Tree leaf, older versions in overflow chain. Each document carries `created_txn` and `deleted_txn` IDs. Visibility: `created_txn <= snapshot && (deleted_txn is None || deleted_txn > snapshot)`.

Public API (`api/`) sits on top: `put/get/delete/update/query/scan/query_index/batch_get_item/transact`, plus server-side filter expressions, condition expressions on write operations, return values on writes (type-state builders for old/new document retrieval), and introspection for partition schemas and indexes.

Documents are stored on disk as MessagePack (via rmp-serde) for compactness. The public API accepts and returns `serde_json::Value`.

## Workspace Layout

```
crates/
  ferridyn-core/       # Core database engine (lib crate)
    src/
      storage/         # File I/O, page management, header, free list
      btree/           # B+Tree: node layout, ops, overflow
      mvcc/            # Transaction manager, snapshots, version chains, GC
      catalog/         # Table schemas, partition schemas, secondary indexes
      api/             # Public API: FerridynDB, builders, batch, query, filter
      encoding/        # Key encoding: string, number, binary, composite
    benches/           # Criterion benchmarks (ferridyn_bench, ferridyn_file_bench)
  ferridyn-server/     # Unix socket server + async client library
    src/
      server.rs        # FerridynServer — tokio-based Unix socket listener
      client.rs        # FerridynClient — async client for multi-process access
      protocol.rs      # Wire protocol (JSON over length-prefixed frames)
    tests/
      integration.rs   # Server integration tests (21 tests)
  ferridyn-console/    # Interactive CLI client (connects to server via Unix socket)
    src/
      parser.rs        # SQL-like command parser
      executor.rs      # Command execution via FerridynClient + tokio Runtime
      commands.rs      # Command and type definitions (local KeyType/AttrType enums)
      display.rs       # Output formatting (pretty and JSON modes)
```

## PRD Workflow

PRDs live in `docs/prds/` and track feature implementation across phases.

- **Update the PRD status** after each phase is committed and pushed (e.g., "In Progress (Phase 1 complete)")
- **Move to `docs/prds/completed/`** with status "Complete" once all phases are committed and pushed
- Include the PRD status update or move in the same commit as the code, or as an immediate follow-up commit

## Key Design Decisions

- **Copy-on-write over WAL** — Simpler crash recovery, no separate log file
- **Standard file I/O over mmap** — Direct read/write with page-level buffering
- **MessagePack over JSON on disk** — Compact binary serialization for document storage
- **B+Tree over LSM** — Better read performance for the embedded use case
- **4KB pages** — Matches OS page size
- **Single writer, unlimited readers** — LMDB concurrency model
- **Slotted pages** — Slot array grows forward, cell data grows backward
- **Partition schemas & scoped secondary indexes** — Prefix-based entity type metadata with attribute definitions, scoped secondary indexes backed by plain B+Tree lookups with lazy GC
- **No B+Tree rebalancing in v1** — Mark-as-dead delete, reclaim fully empty pages

## Dependencies

### ferridyn-core
- `serde` / `serde_json` — JSON document API
- `rmp-serde` — MessagePack serialization for on-disk document storage
- `fs2` — Cross-platform file locking (`flock`)
- `parking_lot` — Fast RwLock for concurrency
- `thiserror` — Error types
- `bytes` — Byte buffer manipulation
- `xxhash-rust` — Page checksums (xxHash64)
- `tempfile` (dev) — Test isolation
- `criterion` (dev) — Statistical benchmarking

### ferridyn-server
- `tokio` — Async runtime for Unix socket server
- `tracing` / `tracing-subscriber` — Structured logging
- `dirs` — XDG directory resolution

### ferridyn-console
- `ferridyn-server` — Client library for server communication
- `tokio` — Async runtime (bridged to sync via `block_on`)
- `rustyline` — Line editing and history for the REPL
- `clap` — CLI argument parsing
- `dirs` — Default socket/data path resolution
- `ferridyn-core` (dev) — Test helpers for in-process server setup
