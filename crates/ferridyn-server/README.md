# FerridynDB Server

Local Unix socket server for FerridynDB database, enabling multiple clients to share a single database instance without file lock conflicts.

## Protocol

JSON-over-newlines on Unix domain socket. Each request is one JSON line, each response is one JSON line.

### Request Examples

```jsonl
{"op":"get_item","table":"users","partition_key":"alice"}
{"op":"get_item_versioned","table":"users","partition_key":"alice"}
{"op":"put_item","table":"users","item":{"user_id":"alice","name":"Alice"}}
{"op":"put_item","table":"users","item":{"user_id":"alice","name":"Updated"},"expected_version":5}
{"op":"delete_item","table":"users","partition_key":"alice"}
{"op":"query","table":"users","partition_key":"alice","limit":20}
{"op":"query","table":"users","partition_key":"alice","filter":{"Gt":[{"Attr":"age"},{"Literal":25}]}}
{"op":"scan","table":"users","limit":100}
{"op":"scan","table":"users","limit":100,"filter":{"And":[{"Eq":[{"Attr":"status"},{"Literal":"active"}]},{"Gt":[{"Attr":"age"},{"Literal":18}]}]}}
{"op":"create_table","table":"users","partition_key":{"name":"user_id","type":"String"}}
{"op":"list_tables"}
{"op":"list_partition_keys","table":"users","limit":20}
{"op":"list_sort_key_prefixes","table":"users","partition_key":"alice","limit":20}
```

### Response Examples

```jsonl
{"ok":true,"item":{"user_id":"alice","name":"Alice"}}
{"ok":true,"item":{"user_id":"alice","name":"Alice"},"version":5}
{"ok":true,"items":[...]}
{"ok":true}
{"error":"VersionMismatch","message":"expected version 5, actual 8","expected":5,"actual":8}
{"error":"TableNotFound","message":"table not found: nonexistent"}
```

## Client Library

```rust
use ferridyn_server::FerridynClient;
use serde_json::json;

let mut client = FerridynClient::connect("/tmp/ferridyn.sock").await?;

// CRUD operations
client.put_item("users", json!({"user_id": "alice", "name": "Alice"})).await?;
let item = client.get_item("users", json!("alice"), None).await?;
client.delete_item("users", json!("alice"), None).await?;

// Version conflict detection
let v = client.get_item_versioned("users", json!("alice"), None).await?;
client.put_item_conditional(
    "users",
    json!({"user_id": "alice", "name": "Updated"}),
    v.unwrap().version
).await?;
```

## Server Binary

```bash
ferridyn-server [--db PATH] [--socket PATH]

# Defaults:
#   --db     ~/.local/share/ferridyn/default.db
#   --socket ~/.local/share/ferridyn/server.sock
```

### Startup Behavior

- Creates parent directories if they don't exist
- Removes stale socket file on startup (if previous server crashed)
- Gracefully shuts down on SIGINT/SIGTERM (removes socket file)

## Features

- **Concurrent reads**: Multiple clients can read simultaneously via FerridynDB's RwLock::read
- **Serialized writes**: Writes are serialized through RwLock::write (single writer at a time)
- **Graceful shutdown**: SIGINT/SIGTERM handling with socket cleanup
- **Stale socket cleanup**: Automatically removes socket file from crashed servers
- **Version tracking**: Optimistic locking with version numbers for conditional updates

## Concurrency Model

The server inherits FerridynDB's concurrency semantics:

- **Read operations** (get, query, scan, list_*) execute concurrently via read lock
- **Write operations** (put, delete, create_table) are serialized via write lock
- **Version conflicts** are detected and reported as VersionMismatch errors
- **Snapshot isolation** is maintained per-transaction
