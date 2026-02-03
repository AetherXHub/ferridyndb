# FerridynDB

A local, embedded, DynamoDB-style document database written in Rust with single-file storage and full MVCC transactions.

## Features

- **DynamoDB-compatible API** — Builder-pattern methods for `put_item`, `get_item`, `delete_item`, `update_item`, `query`, and `scan`
- **Single-file storage** — Copy-on-write pages with atomic double-buffered header commits (no WAL)
- **MVCC snapshot isolation** — Single writer, unlimited concurrent readers with version chains
- **B+Tree indexing** — Efficient range scans with slotted pages and overflow support
- **Partition schemas & secondary indexes** — Declare entity types with prefix-based schemas, create scoped secondary indexes with automatic backfill, and query by indexed attribute values
- **Byte-ordered key encoding** — Enables fast `memcmp`-based comparisons for partition and sort keys
- **TTL support** — Optional time-to-live attributes with automatic expiry filtering
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

// Atomic partial updates (upserts if item doesn't exist)
db.update_item("users")
    .partition_key("alice")
    .set("email", "alice@example.com")
    .set("address.city", "NYC")
    .remove("old_field")
    .add("login_count", 1)
    .execute()
    .unwrap();
```

### Secondary Indexes

```rust
use ferridyn_core::api::FerridynDB;
use ferridyn_core::types::{AttrType, KeyType};
use serde_json::json;

let db = FerridynDB::create("contacts.db").unwrap();

// Single-table design with partition key prefix convention
db.create_table("data")
    .partition_key("pk", KeyType::String)
    .execute()
    .unwrap();

// Declare a partition schema for CONTACT entities
db.create_partition_schema("data")
    .prefix("CONTACT")
    .description("Contact entities")
    .attribute("email", AttrType::String, true)
    .validate(true)
    .execute()
    .unwrap();

// Create a secondary index on email
db.create_index("data")
    .name("email-idx")
    .partition_schema("CONTACT")
    .index_key("email", KeyType::String)
    .execute()
    .unwrap();

// Insert documents — index is maintained automatically
db.put_item("data", json!({
    "pk": "CONTACT#alice",
    "email": "alice@example.com",
    "name": "Alice"
})).unwrap();

// Query by indexed attribute
let result = db.query_index("data", "email-idx")
    .key_value("alice@example.com")
    .execute()
    .unwrap();
assert_eq!(result.items.len(), 1);
assert_eq!(result.items[0]["name"], "Alice");
```

### Build and Test

```bash
# Compile all crates
cargo build

# Run all tests (511 tests across workspace)
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

// Atomic partial updates
use ferridyn_server::client::UpdateActionInput;
client.update_item("users", json!("bob"), None, &[
    UpdateActionInput { action: "set".into(), path: "email".into(), value: Some(json!("bob@example.com")) },
    UpdateActionInput { action: "add".into(), path: "login_count".into(), value: Some(json!(1)) },
]).await?;

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

## Console

The console connects to a running server over a Unix socket. If no server is running at the default socket, it auto-starts one.

```bash
# Connect to default server (auto-starts if needed)
ferridyn-console

# Connect to a specific server socket
ferridyn-console --socket /tmp/my-server.sock

# Execute commands non-interactively
ferridyn-console -e "CREATE TABLE users PK user_id STRING" -e "LIST TABLES"

# JSON output for scripting
ferridyn-console -j -e "SCAN users"

# Pipe mode
echo "LIST TABLES" | ferridyn-console
```

### Interactive session

The `USE` command sets a default table for the session, so you don't have to repeat it on every command. The prompt updates to show the active table.

```
ferridyn> CREATE TABLE data PK pk STRING
Table 'data' created.
ferridyn> USE data
Using table 'data'.
ferridyn:data> PUT {"pk": "CONTACT#alice", "email": "alice@example.com", "name": "Alice"}
OK
ferridyn:data> GET pk=CONTACT#alice
{ "pk": "CONTACT#alice", "email": "alice@example.com", "name": "Alice" }
ferridyn:data> UPDATE pk=CONTACT#alice SET name="Alice Smith" ADD login_count=1
OK
ferridyn:data> SCAN LIMIT 5
...
ferridyn:data> SCAN other_table LIMIT 10   ← explicit table overrides the default
...
ferridyn:data> USE                          ← clears the active table
Cleared active table.
ferridyn>
```

`USE` also works in non-interactive modes — the active table persists across `-e` flags and piped commands.

The console supports table management, partition schemas, secondary indexes, and all query operations through a SQL-like command syntax. Type `HELP` in the REPL for a full command reference.

Because the console and server are separate processes, multiple clients (console sessions, agents, MCP servers) can access the same database concurrently without file lock conflicts.

## Workspace Layout

| Crate | Description |
|-------|-------------|
| `ferridyn-core` | Core database engine (storage, B+Tree, MVCC, public API) |
| `ferridyn-server` | Unix socket server + async client library for multi-process access |
| `ferridyn-console` | Interactive CLI client — connects to server via Unix socket with auto-start |

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

- `cargo bench -p ferridyn-core` — In-memory (tmpfs) microbenchmarks for routine performance checks
- `BENCH_DIR=/path/to/nvme cargo bench --bench ferridyn_file_bench` — File-backed benchmarks on real storage (run only after major refactors)
- `cargo bench -p ferridyn-bench` — Head-to-head comparison against 6 other embedded databases (requires libclang for RocksDB)

By default, benchmarks use tmpfs. Real NVMe benchmarks are dominated by fsync latency (~5ms per commit) and should only be run to validate algorithmic changes that show improvement in tmpfs benchmarks.

### Comparison Benchmarks

Adapted from the [redb benchmark suite](https://github.com/cberner/redb/tree/master/crates/redb-bench). All databases use a 4 GiB cache with 5M bulk-loaded items (24-byte keys, 150-byte values). FerridynDB operates through its document API with hex-encoded keys, so it carries serialization overhead that raw KV stores avoid. Range scans are skipped for FerridynDB because the per-query document model overhead makes 500K iterations impractical at this scale.

| | redb | lmdb | rocksdb | sled | fjall | sqlite | ferridyndb |
|---|---|---|---|---|---|---|---|
| bulk load | 20424ms | **11448ms** | 12954ms | 21817ms | 13366ms | 26093ms | 92641ms |
| individual writes | **79ms** | 12165ms | 6225ms | 5084ms | 6220ms | 20699ms | 5034ms |
| batch writes | 1394ms | 4465ms | 808ms | 1083ms | **628ms** | 6409ms | 3377ms |
| nosync writes | 3275ms | 949ms | **168ms** | 275ms | 217ms | 1860ms | 889ms |
| len() | 0ms | 0ms | 626ms | 1514ms | 990ms | 25ms | **0ms** |
| random reads | 1115ms | **579ms** | 2148ms | 1330ms | 2116ms | 3849ms | 3582ms |
| random reads | 919ms | **589ms** | 2118ms | 1344ms | 2093ms | 3858ms | 3582ms |
| random range reads | 1118ms | **509ms** | 2778ms | 1782ms | 2518ms | 7377ms | N/A |
| random range reads | 1108ms | **508ms** | 2778ms | 1779ms | 2513ms | 7327ms | N/A |
| random reads (4 threads) | 1379ms | **770ms** | 3140ms | 1787ms | 2776ms | 6922ms | 5253ms |
| random reads (8 threads) | 765ms | **395ms** | 1855ms | 945ms | 1456ms | 8891ms | 3042ms |
| random reads (16 threads) | 654ms | **206ms** | 2354ms | 633ms | 967ms | 23613ms | 3367ms |
| random reads (32 threads) | 407ms | **139ms** | 2333ms | 407ms | 598ms | 27562ms | 4044ms |
| removals | 16319ms | 9641ms | 6483ms | 9891ms | **5634ms** | 19098ms | 41905ms |
| uncompacted size | 6.68 GiB | 2.63 GiB | **948.17 MiB** | 2.14 GiB | 1010.61 MiB | 1.10 GiB | 3.52 GiB |
| compacted size | 1.64 GiB | 1.27 GiB | **459.18 MiB** | N/A | 1010.61 MiB | 562.31 MiB | N/A |

## Design Decisions

### Copy-on-write over Write-Ahead Log

FerridynDB uses copy-on-write semantics with double-buffered headers instead of a traditional WAL. This simplifies crash recovery (just use the last committed header) and eliminates the need for a separate log file.

### Standard File I/O over mmap

Direct read/write with page-level buffering instead of memory-mapped I/O. Simpler to reason about, avoids mmap pitfalls with concurrent writes.

### B+Tree over LSM-Tree

B+Trees provide better read performance and simpler implementation for the embedded use case. LSM-trees excel at write-heavy workloads but require compaction and bloom filters.

### 4KB Pages

Page size matches the OS page size for optimal mmap alignment and cache efficiency.

### Single Writer, Unlimited Readers

FerridynDB follows the LMDB concurrency model: one writer at a time (via file lock), unlimited concurrent readers (via MVCC snapshots). This avoids the complexity of multi-writer coordination while providing excellent read scalability.

### Partition Schemas & Secondary Indexes

Secondary indexes are scoped to partition schemas — prefix-based entity type declarations that define expected attributes. Indexes are backed by plain B+Tree lookups with lazy GC for orphaned entries. This enables efficient attribute-value queries without full table scans.

### No B+Tree Rebalancing (v1)

Deleted items are marked as dead but not immediately removed. Fully empty pages are reclaimed, but partial pages are not rebalanced. Future versions may add background compaction.

## Core Dependencies

- **serde / serde_json** — JSON document serialization
- **parking_lot** — Fast read-write locks for concurrency
- **thiserror** — Ergonomic error types
- **bytes** — Efficient byte buffer manipulation
- **xxhash-rust** — Fast page checksums
- **rmp-serde** — MessagePack serialization for on-disk storage
- **tokio** (server/client) — Async runtime for Unix socket server
- **tempfile** (dev) — Test isolation
- **criterion** (dev) — Statistical benchmarking

## License

MIT (placeholder)
