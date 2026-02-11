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
{"op":"set_ttl","table":"cache","partition_key":"a","ttl_seconds":3600}
{"op":"remove_ttl","table":"cache","partition_key":"a"}
{"op":"get_ttl","table":"cache","partition_key":"a"}
{"op":"sweep_expired_ttl","table":"cache"}
{"op":"count","table":"users","partition_key":"alice"}
{"op":"count","table":"events","partition_key":"device1","sort_key_condition":{"op":"between","low":100,"high":200}}
{"op":"count","table":"users","partition_key":"alice","filter":{"Eq":[{"Attr":"status"},{"Literal":"active"}]}}
{"op":"count_index","table":"data","index_name":"status-idx","key_value":"active"}
{"op":"count_index","table":"data","index_name":"cat-price-idx","key_value":"electronics","sort_key_condition":{"op":"between","low":30,"high":70}}
{"op":"drop_table","table":"old_table"}
{"op":"describe_table","table":"users"}
{"op":"list_indexes","table":"data"}
{"op":"describe_index","table":"data","name":"email-idx"}
{"op":"create_vector_index","table":"articles","name":"emb-idx","attribute":"embedding","dimensions":384,"metric":"cosine"}
{"op":"query_vector_index","table":"articles","index_name":"emb-idx","vector":[0.1,0.8,0.3],"top_k":10}
{"op":"query_vector_index","table":"articles","index_name":"emb-idx","vector":[0.1,0.8,0.3],"top_k":10,"filter":{"Eq":[{"Attr":"category"},{"Literal":"science"}]},"oversampling_factor":5}
{"op":"drop_vector_index","table":"articles","index_name":"emb-idx"}
{"op":"list_vector_indexes","table":"articles"}
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
{"ok":true,"remaining_seconds":3595}
{"ok":true,"remaining_seconds":null}
{"ok":true,"count":42}
{"ok":true,"schema":{"name":"users","partition_key":{"name":"user_id","type":"String"},"sort_key":null,"ttl_attribute":null}}
{"ok":true,"items":[{"item":{"id":"a1","title":"Rust concurrency","embedding":[0.1,0.8,0.3]},"score":0.99},{"item":{"id":"a3","title":"Async Rust","embedding":[0.2,0.7,0.4]},"score":0.95}]}
{"ok":true,"indexes":[{"name":"emb-idx","attribute":"embedding","dimensions":384,"metric":"cosine"}]}
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

// Count (without transferring document bodies)
let count = client.count("users", json!("alice"), None, None).await?;
// With sort key condition
let count = client.count("events", json!("device1"),
    Some(SortKeyCondition::Between { low: json!(100), high: json!(200) }), None).await?;

// Count on a secondary index
let count = client.count_index("data", "status-idx", json!("active"), None, None).await?;

// TTL management
client.set_ttl("cache", json!("session_123"), None, 3600).await?;      // expire in 1 hour
let remaining = client.get_ttl("cache", json!("session_123"), None).await?; // Some(3599)
client.remove_ttl("cache", json!("session_123"), None).await?;           // make permanent
let swept = client.sweep_expired_ttl("cache").await?;                    // delete expired items

// Vector index operations
client.create_vector_index("articles", "emb-idx", "embedding", 384, "cosine").await?;

use ferridyn_server::client::ScoredItemInfo;
let results: Vec<ScoredItemInfo> = client.query_vector_index(
    "articles", "emb-idx", &[0.1, 0.8, 0.3], 10, None, None
).await?;
// results[0].item — the matching document
// results[0].score — similarity score

// With post-ANN filter and oversampling
use ferridyn_core::api::FilterExpr;
let results = client.query_vector_index(
    "articles", "emb-idx", &[0.1, 0.8, 0.3], 5,
    Some(FilterExpr::eq(FilterExpr::attr("category"), FilterExpr::Literal(json!("science")))),
    Some(5), // oversampling factor
).await?;

let indexes = client.list_vector_indexes("articles").await?;
client.drop_vector_index("articles", "emb-idx").await?;

// Table management
client.drop_table("old_table").await?;
let schema = client.describe_table("users").await?;
// schema.name, schema.partition_key_name, schema.partition_key_type
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
- **Count aggregation**: Count matching items without transferring document bodies; supports partition key, sort key conditions, and filter expressions — also available on secondary indexes via `count_index`
- **TTL management**: Set, remove, and query item TTLs over the wire protocol; sweep expired items on demand
- **Vector indexes**: Create, query, drop, and list in-memory HNSW vector indexes over the wire protocol; supports cosine, euclidean, and dot product metrics with post-ANN filter expressions and configurable oversampling
- **Table management**: Create, drop, describe, and list tables; describe includes partition/sort key schema and TTL attribute configuration

## Concurrency Model

The server inherits FerridynDB's concurrency semantics:

- **Read operations** (get, batch_get, query, scan, count, count_index, query_vector_index, list_*) execute concurrently via read lock
- **Write operations** (put, delete, update, create_table, drop_table, create_index, create_vector_index) are serialized via write lock
- **Version conflicts** are detected and reported as VersionMismatch errors
- **Snapshot isolation** is maintained per-transaction
