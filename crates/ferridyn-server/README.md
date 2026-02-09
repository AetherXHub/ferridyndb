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
{"op":"query","table":"events","partition_key":"device1","sort_key_condition":{"op":"between","low":100,"high":200}}
{"op":"query","table":"events","partition_key":"device1","sort_key_condition":{"op":"gt","value":1000}}
{"op":"query","table":"events","partition_key":"device1","sort_key_condition":{"op":"eq","value":"specific_key"}}
{"op":"scan","table":"users","limit":100}
{"op":"scan","table":"users","limit":100,"projection":["name","status"]}
{"op":"scan","table":"users","limit":100,"filter":{"And":[{"Eq":[{"Attr":"status"},{"Literal":"active"}]},{"Gt":[{"Attr":"age"},{"Literal":18}]}]}}
{"op":"create_table","table":"users","partition_key":{"name":"user_id","type":"String"}}
{"op":"list_tables"}
{"op":"batch_get_item","table":"users","keys":[{"partition_key":"alice"},{"partition_key":"bob"}]}
{"op":"batch_get_item","table":"users","keys":[{"partition_key":"alice"},{"partition_key":"bob"}],"projection":["name"]}
{"op":"create_index","table":"data","name":"email-idx","partition_schema":"CONTACT","index_key":{"name":"email","type":"String"}}
{"op":"create_index","table":"data","name":"status-idx","index_key":{"name":"status","type":"String"}}
{"op":"create_index","table":"data","name":"age-status-idx","index_key":{"name":"status","type":"String"},"index_sort_key":{"name":"age","type":"Number"}}
{"op":"create_index","table":"data","name":"email-proj-idx","partition_schema":"CONTACT","index_key":{"name":"email","type":"String"},"projection_type":"INCLUDE","projection_attributes":["name","phone"]}
{"op":"create_index","table":"data","name":"status-all-idx","index_key":{"name":"status","type":"String"},"projection_type":"ALL"}
{"op":"create_index","table":"orders","name":"ts-idx","index_sort_key":{"name":"timestamp","type":"Number"},"is_local":true}
{"op":"query_index","table":"data","index_name":"status-idx","key_value":"active"}
{"op":"query_index","table":"data","index_name":"age-status-idx","key_value":"active","sort_key_condition":{"op":"between","low":25,"high":50}}
{"op":"query_index","table":"data","index_name":"age-status-idx","key_value":"active","sort_key_condition":{"op":"gt","value":30}}
{"op":"query_index","table":"data","index_name":"age-status-idx","key_value":"active","sort_key_condition":{"op":"eq","value":25}}
{"op":"query_index","table":"data","index_name":"email-idx","key_value":"alice@example.com","projection":["name"]}
{"op":"drop_index","table":"data","index_name":"status-idx"}
{"op":"list_partition_keys","table":"users","limit":20}
{"op":"list_sort_key_prefixes","table":"users","partition_key":"alice","limit":20}
{"op":"enable_stream","table":"orders","view_type":"NEW_AND_OLD_IMAGES"}
{"op":"disable_stream","table":"orders"}
{"op":"get_stream_records","table":"orders"}
{"op":"get_stream_records","table":"orders","after_sequence":5,"limit":100}
{"op":"get_stream_info","table":"orders"}
{"op":"prune_stream","table":"orders"}
{"op":"batch_write_item","operations":[{"op":"put","table":"users","item":{"user_id":"alice","name":"Alice"}},{"op":"delete","table":"users","partition_key":"old_user"}]}
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
{"ok":true,"succeeded":2}
{"ok":true,"records":[{"sequence_number":3,"sub_sequence":0,"event_type":"INSERT","keys":{"order_id":"o1"},"timestamp":1700000000.0}]}
{"ok":true,"stream_info":{"enabled":true,"view_type":"NEW_AND_OLD_IMAGES","oldest_sequence":3,"latest_sequence":5,"record_count":3}}
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

// Atomic batch writes (up to 25 ops, all-or-nothing)
use ferridyn_server::client::BatchWriteInput;
let count = client.batch_write_item(&[
    BatchWriteInput::Put { table: "users".into(), item: json!({"user_id": "charlie", "name": "Charlie"}) },
    BatchWriteInput::Delete { table: "users".into(), partition_key: json!("old_user"), sort_key: None },
]).await?;
// count == 2

// Change streams
client.enable_stream("orders", "NEW_AND_OLD_IMAGES").await?;
let records = client.get_stream_records("orders", None, Some(100)).await?;
let info = client.get_stream_info("orders").await?;
client.prune_stream("orders").await?;
client.disable_stream("orders").await?;
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
- **Secondary indexes**: Scoped (partition schema prefix), global (table-wide), and local (same partition key, alternate sort key) secondary indexes with composite keys (partition + sort), index projections (KEYS_ONLY, INCLUDE, ALL), automatic backfill, sort key range conditions, and page reclamation on drop
- **Batch writes**: Atomic multi-table put/delete batches (up to 25 operations) with all-or-nothing semantics
- **Change streams**: Per-table change data capture with configurable view types (KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES), poll-based consumption with sequence pagination, stream info, retention pruning, and enable/disable on existing tables

## Concurrency Model

The server inherits FerridynDB's concurrency semantics:

- **Read operations** (get, batch_get, query, scan, list_*) execute concurrently via read lock
- **Write operations** (put, delete, update, create_table) are serialized via write lock
- **Version conflicts** are detected and reported as VersionMismatch errors
- **Snapshot isolation** is maintained per-transaction
