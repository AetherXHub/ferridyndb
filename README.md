# FerridynDB

A local, embedded, DynamoDB-style document database written in Rust with single-file storage and full MVCC transactions.

## Features

- **DynamoDB-compatible API** — Builder-pattern methods for `put_item`, `get_item`, `delete_item`, `query`, and `scan`
- **Single-file storage** — Copy-on-write pages with atomic double-buffered header commits (no WAL)
- **MVCC snapshot isolation** — Single writer, unlimited concurrent readers with version chains
- **B+Tree indexing** — Efficient range scans with slotted pages and overflow support
- **Byte-ordered key encoding** — Enables fast `memcmp`-based comparisons for partition and sort keys
- **Version-aware API** — Optimistic concurrency control with versioned reads and conditional writes
- **Unix socket server** — Multi-process access with async client library

## Quick Start

### Embedded Usage

```rust
use ferridyn_core::api::FerridynDB;
use ferridyn_core::types::KeyType;
use serde_json::json;

// Create or open a database
let db = FerridynDB::create("my_database.db").unwrap();

// Create a table with partition key
db.create_table("users")
    .partition_key("user_id", KeyType::String)
    .execute()
    .unwrap();

// Insert a document
db.put_item("users", json!({
    "user_id": "alice",
    "name": "Alice",
    "age": 30
})).unwrap();

// Retrieve a document
let item = db.get_item("users")
    .partition_key("alice")
    .execute()
    .unwrap();
assert_eq!(item.unwrap()["name"], "Alice");

// Query with sort key range
db.create_table("events")
    .partition_key("device_id", KeyType::String)
    .sort_key("timestamp", KeyType::Number)
    .execute()
    .unwrap();

let results = db.query("events")
    .partition_key("device_123")
    .sort_key_between(100.0, 200.0)
    .execute()
    .unwrap();
```

### Build and Test

```bash
# Compile all crates
cargo build

# Run all tests (367 tests across workspace)
cargo test

# Run tests for a specific crate
cargo test -p ferridyn-core

# Lint all crates
cargo clippy --workspace

# Format all code
cargo fmt --all

# Run benchmarks
cargo bench -p ferridyn-core
```

## Server Mode

FerridynDB can run as a standalone server accessed by multiple processes over a Unix socket. This enables multi-process access with version-aware optimistic concurrency control.

### Starting the Server

```bash
# Start server with custom database and socket paths
ferridyn-server --db ~/.local/share/ferridyn/default.db --socket /tmp/ferridyn.sock

# Or use defaults (XDG_DATA_HOME/ferridyn/default.db and XDG_RUNTIME_DIR/ferridyn.sock)
ferridyn-server
```

### Client Usage

```rust
use ferridyn_server::FerridynClient;
use serde_json::json;

// Connect to server
let mut client = FerridynClient::connect("/tmp/ferridyn.sock").await?;

// Regular operations
client.put_item("users", json!({"user_id": "bob", "name": "Bob"})).await?;
let item = client.get_item("users", json!("bob"), None).await?;

// Version-aware reads for optimistic concurrency
let versioned = client.get_item_versioned("users", json!("bob"), None).await?;
// Returns: Some(VersionedItem { item: {...}, version: 5 })

// Conditional write (fails if version changed)
client.put_item_versioned("users", json!({
    "user_id": "bob",
    "name": "Bob Updated"
}), Some(5)).await?;
// Returns error if version != 5 (someone else modified the item)
```

### Handling Version Conflicts

```rust
loop {
    let versioned = client.get_item_versioned("users", json!("bob"), None).await?;
    if let Some(mut item_data) = versioned {
        // Modify document
        item_data.item["counter"] = json!(item_data.item["counter"].as_i64().unwrap() + 1);

        // Try conditional write
        match client.put_item_versioned("users", item_data.item, Some(item_data.version)).await {
            Ok(_) => break, // Success
            Err(e) if e.to_string().contains("version conflict") => {
                // Retry with fresh version
                continue;
            }
            Err(e) => return Err(e), // Other error
        }
    }
}
```

## Workspace Layout

| Crate | Description |
|-------|-------------|
| `ferridyn-core` | Core database engine (storage, B+Tree, MVCC, public API) |
| `ferridyn-server` | Unix socket server + async client library for multi-process access |
| `ferridyn-console` | Interactive REPL for exploring and manipulating FerridynDB databases |

## Development

### Workflow

FerridynDB development follows an incremental, test-driven approach:

1. **Compile first** — `cargo build -p ferridyn-core` must pass with zero errors before proceeding
2. **Test everything** — Write tests for each new feature before considering it done
3. **Lint clean** — `cargo clippy --workspace -- -D warnings` must pass (zero warnings allowed)
4. **Format** — `cargo fmt --all --check` must pass
5. **No dead code** — No `todo!()` or `unimplemented!()` in committed code
6. **One layer at a time** — Build bottom-up through the architecture stack

### Benchmarks

Two benchmark suites are available:

- `cargo bench` — In-memory (tmpfs) microbenchmarks for routine performance checks
- `BENCH_DIR=/path/to/nvme cargo bench --bench ferridyn_file_bench` — File-backed benchmarks on real storage (run only after major refactors)

By default, benchmarks use tmpfs. Real NVMe benchmarks are dominated by fsync latency (~5ms per commit) and should only be run to validate algorithmic changes that show improvement in tmpfs benchmarks.

## Design Decisions

### Copy-on-write over Write-Ahead Log

FerridynDB uses copy-on-write semantics with double-buffered headers instead of a traditional WAL. This simplifies crash recovery (just use the last committed header) and eliminates the need for a separate log file.

### mmap for Reads

Memory-mapped I/O delegates page caching to the OS, reducing implementation complexity and leveraging kernel optimizations.

### B+Tree over LSM-Tree

B+Trees provide better read performance and simpler implementation for the embedded use case. LSM-trees excel at write-heavy workloads but require compaction and bloom filters.

### 4KB Pages

Page size matches the OS page size for optimal mmap alignment and cache efficiency.

### Single Writer, Unlimited Readers

FerridynDB follows the LMDB concurrency model: one writer at a time (via file lock), unlimited concurrent readers (via MVCC snapshots). This avoids the complexity of multi-writer coordination while providing excellent read scalability.

### No Secondary Indexes (v1)

Version 1 focuses on core functionality. Secondary indexes and more complex query patterns are planned for future releases.

### No B+Tree Rebalancing (v1)

Deleted items are marked as dead but not immediately removed. Fully empty pages are reclaimed, but partial pages are not rebalanced. Future versions may add background compaction.

## Core Dependencies

- **serde / serde_json** — JSON document serialization
- **memmap2** — Memory-mapped file I/O
- **parking_lot** — Fast read-write locks for concurrency
- **thiserror** — Ergonomic error types
- **bytes** — Efficient byte buffer manipulation
- **xxhash-rust** — Fast page checksums
- **tokio** (server/client) — Async runtime for Unix socket server
- **tempfile** (dev) — Test isolation
- **criterion** (dev) — Statistical benchmarking

## License

MIT (placeholder)
