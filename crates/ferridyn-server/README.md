# FerridynDB Server

Local Unix socket server for FerridynDB database, enabling multiple clients to share a single database instance without file lock conflicts.

## Protocol

JSON-over-newlines on Unix domain socket. Each request is one JSON line, each response is one JSON line.

### Request Examples

```jsonl
{"op":"get_item","table":"users","partition_key":"alice"}
{"op":"get_item","table":"users","partition_key":"alice","projection":["name","email"]}
{"op":"get_item_versioned","table":"users","partition_key":"alice"}
{"op":"put_item","table":"users","item":{"user_id":"alice","name":"Alice"}}
{"op":"put_item","table":"users","item":{"user_id":"alice","name":"Updated"},"expected_version":5}
{"op":"delete_item","table":"users","partition_key":"alice"}
{"op":"put_item","table":"users","item":{"user_id":"alice","name":"Updated"},"return_values":"ALL_OLD"}
{"op":"delete_item","table":"users","partition_key":"alice","return_values":"ALL_OLD"}
{"op":"update_item","table":"users","partition_key":"alice","updates":[{"action":"set","path":"name","value":"New"}],"return_values":"ALL_NEW"}
{"op":"query","table":"users","partition_key":"alice","limit":20}
{"op":"query","table":"users","partition_key":"alice","limit":20,"projection":["name","age"]}
{"op":"query","table":"users","partition_key":"alice","filter":{"Gt":[{"Attr":"age"},{"Literal":25}]}}
{"op":"scan","table":"users","limit":100}
{"op":"scan","table":"users","limit":100,"projection":["name","status"]}
{"op":"scan","table":"users","limit":100,"filter":{"And":[{"Eq":[{"Attr":"status"},{"Literal":"active"}]},{"Gt":[{"Attr":"age"},{"Literal":18}]}]}}
{"op":"create_table","table":"users","partition_key":{"name":"user_id","type":"String"}}
{"op":"list_tables"}
{"op":"batch_get_item","table":"users","keys":[{"partition_key":"alice"},{"partition_key":"bob"}]}
{"op":"batch_get_item","table":"users","keys":[{"partition_key":"alice"},{"partition_key":"bob"}],"projection":["name"]}
{"op":"create_index","table":"data","name":"email-idx","partition_schema":"CONTACT","index_key":{"name":"email","type":"String"}}
{"op":"create_index","table":"data","name":"status-idx","index_key":{"name":"status","type":"String"}}
{"op":"query_index","table":"data","index_name":"status-idx","key_value":"active"}
{"op":"query_index","table":"data","index_name":"email-idx","key_value":"alice@example.com","projection":["name"]}
{"op":"drop_index","table":"data","index_name":"status-idx"}
{"op":"list_partition_keys","table":"users","limit":20}
{"op":"list_sort_key_prefixes","table":"users","partition_key":"alice","limit":20}
```

### Response Examples

```jsonl
{"ok":true,"item":{"user_id":"alice","name":"Alice"}}
{"ok":true,"item":{"user_id":"alice","name":"Alice"},"version":5}
{"ok":true,"items":[...]}
{"ok":true,"items":[{"user_id":"alice","name":"Alice"},null]}
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
let item = client.get_item("users", json!("alice"), None, None).await?;
client.delete_item("users", json!("alice"), None).await?;

// Projection — return only selected attributes (key attrs always included)
let proj = vec!["name".to_string(), "email".to_string()];
let item = client.get_item("users", json!("alice"), None, Some(&proj)).await?;

// Batch get (single snapshot, positional results, None for missing)
let keys = vec![(json!("alice"), None), (json!("bob"), None)];
let results = client.batch_get_item("users", &keys, None).await?;
// With projection
let results = client.batch_get_item("users", &keys, Some(&proj)).await?;

// ReturnValues — get old/new document atomically
let old = client.put_item_returning_old("users", json!({"user_id": "alice", "name": "Updated"})).await?;
// old == Some(previous document) or None if new

let deleted = client.delete_item_returning_old("users", json!("alice"), None).await?;

use ferridyn_server::client::UpdateActionInput;
let new_doc = client.update_item_returning_new("users", json!("bob"), None, &[
    UpdateActionInput { action: "set".into(), path: "name".into(), value: Some(json!("Bob Updated")) },
]).await?;

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
- **Projection expressions**: Return only selected attributes from read operations (get, query, scan, batch_get, query_index)
- **Secondary indexes**: Scoped (partition schema prefix) and global (table-wide) secondary indexes with automatic backfill and page reclamation on drop

## Concurrency Model

The server inherits FerridynDB's concurrency semantics:

- **Read operations** (get, batch_get, query, scan, list_*) execute concurrently via read lock
- **Write operations** (put, delete, update, create_table) are serialized via write lock
- **Version conflicts** are detected and reported as VersionMismatch errors
- **Snapshot isolation** is maintained per-transaction
