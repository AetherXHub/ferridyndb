//! Integration tests for ferridyn-server: start server, connect client, verify ops.

use serde_json::json;
use tempfile::tempdir;
use tokio::time::{Duration, sleep};

use ferridyn_core::api::{FerridynDB, FilterExpr};
use ferridyn_server::client::{AttributeDefInput, FerridynClient, UpdateActionInput};
use ferridyn_server::protocol::{KeyDef, SortKeyCondition};
use ferridyn_server::server::FerridynServer;

/// Start a server on a temp socket and return the socket path.
/// The server runs in a background tokio task.
async fn start_test_server() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let socket_path = dir.path().join("test.sock");

    let db = FerridynDB::create(&db_path).unwrap();
    let server = FerridynServer::new(db, socket_path.clone());

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    // Give the server a moment to bind.
    sleep(Duration::from_millis(50)).await;

    (dir, socket_path)
}

#[tokio::test]
async fn test_create_table_and_crud() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // Create table.
    client
        .create_table(
            "users",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // List tables.
    let tables = client.list_tables().await.unwrap();
    assert_eq!(tables, vec!["users"]);

    // Put item.
    client
        .put_item("users", json!({"id": "alice", "name": "Alice", "age": 30}))
        .await
        .unwrap();

    // Get item.
    let item = client
        .get_item("users", json!("alice"), None, None)
        .await
        .unwrap();
    assert!(item.is_some());
    let item = item.unwrap();
    assert_eq!(item["name"], "Alice");
    assert_eq!(item["age"], 30);

    // Get nonexistent item.
    let missing = client
        .get_item("users", json!("bob"), None, None)
        .await
        .unwrap();
    assert!(missing.is_none());

    // Delete item.
    client
        .delete_item("users", json!("alice"), None)
        .await
        .unwrap();
    let deleted = client
        .get_item("users", json!("alice"), None, None)
        .await
        .unwrap();
    assert!(deleted.is_none());
}

#[tokio::test]
async fn test_versioned_get_and_conditional_put() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Put initial item.
    client
        .put_item("items", json!({"id": "key1", "value": "v1"}))
        .await
        .unwrap();

    // Get versioned.
    let vi = client
        .get_item_versioned("items", json!("key1"), None)
        .await
        .unwrap();
    assert!(vi.is_some());
    let vi = vi.unwrap();
    assert_eq!(vi.item["value"], "v1");
    let version = vi.version;

    // Conditional put with correct version — should succeed.
    client
        .put_item_conditional("items", json!({"id": "key1", "value": "v2"}), version)
        .await
        .unwrap();

    // Conditional put with stale version — should fail.
    let result = client
        .put_item_conditional("items", json!({"id": "key1", "value": "v3"}), version)
        .await;
    assert!(result.is_err());

    let err = result.unwrap_err();
    match err {
        ferridyn_server::error::ClientError::VersionMismatch { .. } => {} // expected
        other => panic!("expected VersionMismatch, got: {other:?}"),
    }

    // Verify the value is v2, not v3.
    let item = client
        .get_item("items", json!("key1"), None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item["value"], "v2");
}

#[tokio::test]
async fn test_concurrent_version_conflict() {
    let (_dir, sock) = start_test_server().await;

    // Two clients sharing the same server.
    let mut client_a = FerridynClient::connect(&sock).await.unwrap();
    let mut client_b = FerridynClient::connect(&sock).await.unwrap();

    client_a
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    client_a
        .put_item("items", json!({"id": "shared", "data": "initial"}))
        .await
        .unwrap();

    // Both clients read the same version.
    let vi_a = client_a
        .get_item_versioned("items", json!("shared"), None)
        .await
        .unwrap()
        .unwrap();
    let vi_b = client_b
        .get_item_versioned("items", json!("shared"), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(vi_a.version, vi_b.version);

    // Client A writes first — succeeds.
    client_a
        .put_item_conditional(
            "items",
            json!({"id": "shared", "data": "from_a"}),
            vi_a.version,
        )
        .await
        .unwrap();

    // Client B writes with stale version — fails.
    let result = client_b
        .put_item_conditional(
            "items",
            json!({"id": "shared", "data": "from_b"}),
            vi_b.version,
        )
        .await;
    assert!(result.is_err());

    // Verify A's write won.
    let item = client_a
        .get_item("items", json!("shared"), None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item["data"], "from_a");
}

#[tokio::test]
async fn test_query_and_scan() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "events",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "sk".to_string(),
                key_type: "Number".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    for i in 0..10 {
        client
            .put_item(
                "events",
                json!({"pk": "user1", "sk": i as f64, "data": format!("event_{i}")}),
            )
            .await
            .unwrap();
    }

    // Query all for user1.
    let result = client
        .query("events", json!("user1"), None, None, None, None, None, None)
        .await
        .unwrap();
    assert_eq!(result.items.len(), 10);

    // Query with limit.
    let result = client
        .query(
            "events",
            json!("user1"),
            None,
            Some(3),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 3);
    assert!(result.last_evaluated_key.is_some());

    // Scan all.
    let result = client.scan("events", None, None, None, None).await.unwrap();
    assert_eq!(result.items.len(), 10);

    // Scan with limit.
    let result = client
        .scan("events", Some(5), None, None, None)
        .await
        .unwrap();
    assert_eq!(result.items.len(), 5);
}

#[tokio::test]
async fn test_describe_and_drop_table() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "mydata",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "sk".to_string(),
                key_type: "Number".to_string(),
            }),
            Some("ttl".to_string()),
        )
        .await
        .unwrap();

    let schema = client.describe_table("mydata").await.unwrap();
    assert_eq!(schema.name, "mydata");
    assert_eq!(schema.partition_key_name, "pk");
    assert_eq!(schema.partition_key_type, "String");
    assert_eq!(schema.sort_key_name, Some("sk".to_string()));
    assert_eq!(schema.sort_key_type, Some("Number".to_string()));
    assert_eq!(schema.ttl_attribute, Some("ttl".to_string()));

    // Drop table.
    client.drop_table("mydata").await.unwrap();
    let tables = client.list_tables().await.unwrap();
    assert!(tables.is_empty());
}

#[tokio::test]
async fn test_list_partition_keys_and_prefixes() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "memories",
            KeyDef {
                name: "category".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "entry".to_string(),
                key_type: "String".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    client
        .put_item(
            "memories",
            json!({"category": "rust", "entry": "ownership#borrowing", "data": "..."}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "memories",
            json!({"category": "rust", "entry": "ownership#moves", "data": "..."}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "memories",
            json!({"category": "python", "entry": "basics#types", "data": "..."}),
        )
        .await
        .unwrap();

    // List partition keys.
    let keys = client.list_partition_keys("memories", None).await.unwrap();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], json!("python"));
    assert_eq!(keys[1], json!("rust"));

    // List sort key prefixes.
    let prefixes = client
        .list_sort_key_prefixes("memories", json!("rust"), None)
        .await
        .unwrap();
    assert_eq!(prefixes.len(), 1);
    assert_eq!(prefixes[0], json!("ownership"));
}

#[tokio::test]
async fn test_error_table_not_found() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    let result = client
        .get_item("nonexistent", json!("key"), None, None)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_error_table_already_exists() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "dupe",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    let result = client
        .create_table(
            "dupe",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// UpdateItem tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_update_item_server_round_trip() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "users",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Put initial item.
    client
        .put_item("users", json!({"id": "alice", "name": "Alice", "age": 25}))
        .await
        .unwrap();

    // Update: SET email, REMOVE age.
    client
        .update_item(
            "users",
            json!("alice"),
            None,
            &[
                UpdateActionInput {
                    action: "set".to_string(),
                    path: "email".to_string(),
                    value: Some(json!("alice@example.com")),
                },
                UpdateActionInput {
                    action: "remove".to_string(),
                    path: "age".to_string(),
                    value: None,
                },
            ],
        )
        .await
        .unwrap();

    // Verify the update.
    let item = client
        .get_item("users", json!("alice"), None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item["name"], "Alice");
    assert_eq!(item["email"], "alice@example.com");
    assert!(item.get("age").is_none());
}

#[tokio::test]
async fn test_update_item_upsert_via_server() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "users",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Update non-existent item — should upsert.
    client
        .update_item(
            "users",
            json!("bob"),
            None,
            &[UpdateActionInput {
                action: "set".to_string(),
                path: "name".to_string(),
                value: Some(json!("Bob")),
            }],
        )
        .await
        .unwrap();

    let item = client
        .get_item("users", json!("bob"), None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item["id"], "bob");
    assert_eq!(item["name"], "Bob");
}

#[tokio::test]
async fn test_update_item_add_and_delete_via_server() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "counters",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Put initial item with a counter and tags.
    client
        .put_item(
            "counters",
            json!({"id": "item1", "count": 10, "tags": ["a", "b", "c"]}),
        )
        .await
        .unwrap();

    // ADD to counter, DELETE from tags.
    client
        .update_item(
            "counters",
            json!("item1"),
            None,
            &[
                UpdateActionInput {
                    action: "add".to_string(),
                    path: "count".to_string(),
                    value: Some(json!(5)),
                },
                UpdateActionInput {
                    action: "delete".to_string(),
                    path: "tags".to_string(),
                    value: Some(json!(["b"])),
                },
            ],
        )
        .await
        .unwrap();

    let item = client
        .get_item("counters", json!("item1"), None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item["count"].as_f64().unwrap(), 15.0);
    let tags = item["tags"].as_array().unwrap();
    assert_eq!(tags.len(), 2);
    assert!(tags.contains(&json!("a")));
    assert!(tags.contains(&json!("c")));
    assert!(!tags.contains(&json!("b")));
}

// ---------------------------------------------------------------------------
// Partition schema tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_partition_schema_crud() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // Create table with sort key (schemas need a table).
    client
        .create_table(
            "data",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "sk".to_string(),
                key_type: "String".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    // Create schema.
    client
        .create_schema(
            "data",
            "CONTACT",
            Some("People and contacts"),
            &[
                AttributeDefInput {
                    name: "email".to_string(),
                    attr_type: "String".to_string(),
                    required: true,
                },
                AttributeDefInput {
                    name: "age".to_string(),
                    attr_type: "Number".to_string(),
                    required: false,
                },
            ],
            true,
        )
        .await
        .unwrap();

    // List schemas — should have 1.
    let schemas = client.list_schemas("data").await.unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].prefix, "CONTACT");

    // Describe schema.
    let schema = client.describe_schema("data", "CONTACT").await.unwrap();
    assert_eq!(schema.prefix, "CONTACT");
    assert_eq!(schema.description, "People and contacts");
    assert!(schema.validate);
    assert_eq!(schema.attributes.len(), 2);
    assert_eq!(schema.attributes[0].name, "email");
    assert_eq!(schema.attributes[0].attr_type, "String");
    assert!(schema.attributes[0].required);
    assert_eq!(schema.attributes[1].name, "age");
    assert_eq!(schema.attributes[1].attr_type, "Number");
    assert!(!schema.attributes[1].required);

    // Drop schema.
    client.drop_schema("data", "CONTACT").await.unwrap();

    // List schemas — should be empty.
    let schemas = client.list_schemas("data").await.unwrap();
    assert!(schemas.is_empty());
}

#[tokio::test]
async fn test_index_crud() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "data",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "sk".to_string(),
                key_type: "String".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    // Create schema first (index requires a schema).
    client
        .create_schema(
            "data",
            "CONTACT",
            Some("People"),
            &[AttributeDefInput {
                name: "email".to_string(),
                attr_type: "String".to_string(),
                required: true,
            }],
            false,
        )
        .await
        .unwrap();

    // Create index.
    client
        .create_index(
            "data",
            "email-idx",
            Some("CONTACT"),
            Some("email"),
            Some("String"),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // List indexes — should have 1.
    let indexes = client.list_indexes("data").await.unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "email-idx");
    assert_eq!(indexes[0].partition_schema, Some("CONTACT".to_string()));
    assert_eq!(indexes[0].index_key_name, "email");
    assert_eq!(indexes[0].index_key_type, "String");

    // Describe index.
    let index = client.describe_index("data", "email-idx").await.unwrap();
    assert_eq!(index.name, "email-idx");
    assert_eq!(index.partition_schema, Some("CONTACT".to_string()));

    // Drop index.
    client.drop_index("data", "email-idx").await.unwrap();

    // List indexes — should be empty.
    let indexes = client.list_indexes("data").await.unwrap();
    assert!(indexes.is_empty());
}

#[tokio::test]
async fn test_query_index() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "data",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "sk".to_string(),
                key_type: "String".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    client
        .create_schema(
            "data",
            "CONTACT",
            None,
            &[AttributeDefInput {
                name: "email".to_string(),
                attr_type: "String".to_string(),
                required: true,
            }],
            false,
        )
        .await
        .unwrap();

    client
        .create_index(
            "data",
            "email-idx",
            Some("CONTACT"),
            Some("email"),
            Some("String"),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Put items matching the schema prefix.
    client
        .put_item(
            "data",
            json!({"pk": "CONTACT#1", "sk": "profile", "email": "alice@example.com"}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "data",
            json!({"pk": "CONTACT#2", "sk": "profile", "email": "bob@example.com"}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "data",
            json!({"pk": "CONTACT#3", "sk": "profile", "email": "alice@example.com"}),
        )
        .await
        .unwrap();

    // Query index for alice — should get 2 items.
    let result = client
        .query_index(
            "data",
            "email-idx",
            json!("alice@example.com"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 2);

    // Query index for bob — should get 1 item.
    let result = client
        .query_index(
            "data",
            "email-idx",
            json!("bob@example.com"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0]["pk"], "CONTACT#2");

    // Query index for nonexistent — should get 0 items.
    let result = client
        .query_index(
            "data",
            "email-idx",
            json!("nobody@example.com"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(result.items.is_empty());
}

#[tokio::test]
async fn test_create_schema_error_table_not_found() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    let result = client
        .create_schema("nonexistent", "PREFIX", None, &[], false)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_query_index_with_limit() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "data",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "sk".to_string(),
                key_type: "String".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    client
        .create_schema(
            "data",
            "ITEM",
            None,
            &[AttributeDefInput {
                name: "status".to_string(),
                attr_type: "String".to_string(),
                required: false,
            }],
            false,
        )
        .await
        .unwrap();

    client
        .create_index(
            "data",
            "status-idx",
            Some("ITEM"),
            Some("status"),
            Some("String"),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    for i in 0..5 {
        client
            .put_item(
                "data",
                json!({"pk": format!("ITEM#{i}"), "sk": "info", "status": "active"}),
            )
            .await
            .unwrap();
    }

    // Query with limit 2.
    let result = client
        .query_index(
            "data",
            "status-idx",
            json!("active"),
            None,
            Some(2),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 2);
}

#[tokio::test]
async fn test_query_with_filter_over_wire() {
    use ferridyn_core::api::FilterExpr;

    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "sk".to_string(),
                key_type: "String".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    // Insert items with varying status.
    for i in 0..6 {
        let status = if i % 2 == 0 { "active" } else { "inactive" };
        client
            .put_item(
                "items",
                json!({
                    "pk": "org1",
                    "sk": format!("user#{:04}", i),
                    "status": status,
                    "score": i * 10,
                }),
            )
            .await
            .unwrap();
    }

    // Query with filter: only active items.
    let filter = FilterExpr::eq(FilterExpr::attr("status"), FilterExpr::literal("active"));
    let result = client
        .query(
            "items",
            json!("org1"),
            None,
            None,
            None,
            None,
            Some(filter),
            None,
        )
        .await
        .unwrap();

    assert_eq!(result.items.len(), 3);
    for item in &result.items {
        assert_eq!(item["status"], "active");
    }

    // Query with filter + limit (DynamoDB semantics: limit counts evaluated).
    let filter = FilterExpr::eq(FilterExpr::attr("status"), FilterExpr::literal("active"));
    let result = client
        .query(
            "items",
            json!("org1"),
            None,
            Some(4),
            None,
            None,
            Some(filter),
            None,
        )
        .await
        .unwrap();

    // 4 items evaluated (user#0000..user#0003), 2 active (0, 2).
    assert_eq!(result.items.len(), 2);
    assert!(result.last_evaluated_key.is_some());
}

#[tokio::test]
async fn test_scan_with_filter_over_wire() {
    use ferridyn_core::api::FilterExpr;

    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "docs",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    for i in 0..5 {
        client
            .put_item(
                "docs",
                json!({
                    "id": format!("doc{}", i),
                    "priority": i + 1,
                }),
            )
            .await
            .unwrap();
    }

    // Scan with filter: priority > 3.
    let filter = FilterExpr::gt(FilterExpr::attr("priority"), FilterExpr::literal(3));
    let result = client
        .scan("docs", None, None, Some(filter), None)
        .await
        .unwrap();

    // priority 4 and 5.
    assert_eq!(result.items.len(), 2);
    for item in &result.items {
        assert!(item["priority"].as_i64().unwrap() > 3);
    }
}

// ---------------------------------------------------------------------------
// Condition expression tests (Phase 4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_server_put_with_condition() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Put with attribute_not_exists on new item → passes.
    client
        .put_item_with_condition(
            "items",
            json!({"id": "a", "val": 1}),
            FilterExpr::attribute_not_exists("id"),
        )
        .await
        .unwrap();

    let item = client
        .get_item("items", json!("a"), None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item["val"], 1);

    // Put with attribute_not_exists on existing item → fails.
    let result = client
        .put_item_with_condition(
            "items",
            json!({"id": "a", "val": 2}),
            FilterExpr::attribute_not_exists("id"),
        )
        .await;
    assert!(result.is_err());

    // Original value should remain.
    let item = client
        .get_item("items", json!("a"), None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item["val"], 1);
}

#[tokio::test]
async fn test_server_delete_with_condition() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    client
        .put_item("items", json!({"id": "a", "status": "archived"}))
        .await
        .unwrap();

    // Delete with matching condition → passes.
    client
        .delete_item_with_condition(
            "items",
            json!("a"),
            None,
            FilterExpr::eq(
                FilterExpr::attr("status"),
                FilterExpr::literal(json!("archived")),
            ),
        )
        .await
        .unwrap();

    let item = client
        .get_item("items", json!("a"), None, None)
        .await
        .unwrap();
    assert!(item.is_none());
}

#[tokio::test]
async fn test_server_condition_check_failed_response() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    client
        .put_item("items", json!({"id": "a", "val": 1}))
        .await
        .unwrap();

    // Condition fails → server returns ConditionCheckFailed error.
    let result = client
        .put_item_with_condition(
            "items",
            json!({"id": "a", "val": 2}),
            FilterExpr::attribute_not_exists("id"),
        )
        .await;
    assert!(result.is_err());
    let err = format!("{:?}", result.unwrap_err());
    assert!(err.contains("ConditionCheckFailed"));
}

// ---------------------------------------------------------------------------
// Index query pagination tests (PRD-04)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_index_pagination_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "data",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "sk".to_string(),
                key_type: "String".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    client
        .create_schema(
            "data",
            "ITEM",
            Some("Items"),
            &[AttributeDefInput {
                name: "status".to_string(),
                attr_type: "String".to_string(),
                required: false,
            }],
            false,
        )
        .await
        .unwrap();

    client
        .create_index(
            "data",
            "status-idx",
            Some("ITEM"),
            Some("status"),
            Some("String"),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Insert 10 items.
    for i in 0..10 {
        client
            .put_item(
                "data",
                json!({
                    "pk": format!("ITEM#{i:03}"),
                    "sk": "info",
                    "status": "active",
                }),
            )
            .await
            .unwrap();
    }

    // Page 1: limit 4.
    let page1 = client
        .query_index(
            "data",
            "status-idx",
            json!("active"),
            None,
            Some(4),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(page1.items.len(), 4);
    assert!(page1.last_evaluated_key.is_some());

    // Page 2: limit 4 with cursor.
    let page2 = client
        .query_index(
            "data",
            "status-idx",
            json!("active"),
            None,
            Some(4),
            None,
            None,
            page1.last_evaluated_key,
            None,
        )
        .await
        .unwrap();
    assert_eq!(page2.items.len(), 4);
    assert!(page2.last_evaluated_key.is_some());

    // Page 3: limit 4 with cursor — should get 2 remaining.
    let page3 = client
        .query_index(
            "data",
            "status-idx",
            json!("active"),
            None,
            Some(4),
            None,
            None,
            page2.last_evaluated_key,
            None,
        )
        .await
        .unwrap();
    assert_eq!(page3.items.len(), 2);
    assert!(page3.last_evaluated_key.is_none());

    // Verify all 10 items covered, no duplicates.
    let mut all_pks: Vec<String> = Vec::new();
    for page in [&page1.items, &page2.items, &page3.items] {
        for item in page {
            all_pks.push(item["pk"].as_str().unwrap().to_string());
        }
    }
    all_pks.sort();
    all_pks.dedup();
    assert_eq!(all_pks.len(), 10);
}

#[tokio::test]
async fn test_batch_get_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // Create table.
    client
        .create_table(
            "users",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Insert 3 items.
    client
        .put_item("users", json!({"id": "alice", "name": "Alice"}))
        .await
        .unwrap();
    client
        .put_item("users", json!({"id": "bob", "name": "Bob"}))
        .await
        .unwrap();
    client
        .put_item("users", json!({"id": "charlie", "name": "Charlie"}))
        .await
        .unwrap();

    // Batch get 4 keys (1 missing).
    let keys = vec![
        (json!("alice"), None),
        (json!("bob"), None),
        (json!("missing"), None),
        (json!("charlie"), None),
    ];
    let results = client.batch_get_item("users", &keys, None).await.unwrap();

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].as_ref().unwrap()["name"], "Alice");
    assert_eq!(results[1].as_ref().unwrap()["name"], "Bob");
    assert!(results[2].is_none());
    assert_eq!(results[3].as_ref().unwrap()["name"], "Charlie");
}

#[tokio::test]
async fn test_batch_get_exceeds_limit() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // Create table.
    client
        .create_table(
            "users",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Build 1001 keys (exceeds 1000 limit).
    let keys: Vec<(serde_json::Value, Option<serde_json::Value>)> = (0..1001)
        .map(|i| (json!(format!("key-{i}")), None))
        .collect();

    let result = client.batch_get_item("users", &keys, None).await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("BatchSizeLimitExceeded"),
        "expected BatchSizeLimitExceeded error, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// ReturnValues tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_server_put_return_old() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "users",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Put initial item.
    client
        .put_item("users", json!({"id": "alice", "name": "Alice", "age": 30}))
        .await
        .unwrap();

    // Put again with return_values=ALL_OLD.
    let old = client
        .put_item_returning_old("users", json!({"id": "alice", "name": "Alice2", "age": 31}))
        .await
        .unwrap();

    assert!(old.is_some());
    let old = old.unwrap();
    assert_eq!(old["name"], "Alice");
    assert_eq!(old["age"], 30);

    // Put new item with return_values=ALL_OLD — should return None.
    let old = client
        .put_item_returning_old("users", json!({"id": "bob", "name": "Bob"}))
        .await
        .unwrap();
    assert!(old.is_none());
}

#[tokio::test]
async fn test_server_delete_return_old() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "users",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    client
        .put_item("users", json!({"id": "alice", "name": "Alice"}))
        .await
        .unwrap();

    // Delete with return_values=ALL_OLD.
    let old = client
        .delete_item_returning_old("users", json!("alice"), None)
        .await
        .unwrap();

    assert!(old.is_some());
    assert_eq!(old.unwrap()["name"], "Alice");

    // Verify deleted.
    let item = client
        .get_item("users", json!("alice"), None, None)
        .await
        .unwrap();
    assert!(item.is_none());

    // Delete non-existent with return_values=ALL_OLD.
    let old = client
        .delete_item_returning_old("users", json!("nobody"), None)
        .await
        .unwrap();
    assert!(old.is_none());
}

#[tokio::test]
async fn test_server_update_return_new() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "users",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    client
        .put_item("users", json!({"id": "alice", "name": "Alice", "age": 30}))
        .await
        .unwrap();

    // Update with return_values=ALL_NEW.
    let new_doc = client
        .update_item_returning_new(
            "users",
            json!("alice"),
            None,
            &[UpdateActionInput {
                action: "set".to_string(),
                path: "name".to_string(),
                value: Some(json!("Alice2")),
            }],
        )
        .await
        .unwrap();

    assert!(new_doc.is_some());
    let new_doc = new_doc.unwrap();
    assert_eq!(new_doc["name"], "Alice2");
    assert_eq!(new_doc["age"], 30);

    // Update with return_values=ALL_OLD.
    let old = client
        .update_item_returning_old(
            "users",
            json!("alice"),
            None,
            &[UpdateActionInput {
                action: "set".to_string(),
                path: "name".to_string(),
                value: Some(json!("Alice3")),
            }],
        )
        .await
        .unwrap();

    assert!(old.is_some());
    let old = old.unwrap();
    assert_eq!(old["name"], "Alice2"); // was Alice2 before this update
}

#[tokio::test]
async fn test_projection_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // Create table with pk + sk.
    client
        .create_table(
            "users",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "sk".to_string(),
                key_type: "String".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    // Put multi-attribute documents.
    for i in 0..3 {
        client
            .put_item(
                "users",
                json!({
                    "pk": "org1",
                    "sk": format!("user#{i}"),
                    "name": format!("User {i}"),
                    "age": 20 + i,
                    "email": format!("user{i}@test.com"),
                    "address": {"city": "NYC", "zip": "10001"}
                }),
            )
            .await
            .unwrap();
    }

    let proj = vec!["name".to_string(), "address.city".to_string()];

    // get_item with projection.
    let item = client
        .get_item("users", json!("org1"), Some(json!("user#0")), Some(&proj))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item["pk"], "org1");
    assert_eq!(item["sk"], "user#0");
    assert_eq!(item["name"], "User 0");
    assert_eq!(item["address"]["city"], "NYC");
    assert!(item.get("age").is_none());
    assert!(item.get("email").is_none());
    assert!(item["address"].get("zip").is_none());

    // query with projection.
    let result = client
        .query(
            "users",
            json!("org1"),
            None,
            None,
            None,
            None,
            None,
            Some(&proj),
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 3);
    for item in &result.items {
        assert!(item.get("pk").is_some());
        assert!(item.get("sk").is_some());
        assert!(item.get("name").is_some());
        assert!(item.get("age").is_none());
        assert!(item.get("email").is_none());
    }

    // scan with projection.
    let name_only = vec!["name".to_string()];
    let result = client
        .scan("users", None, None, None, Some(&name_only))
        .await
        .unwrap();
    assert_eq!(result.items.len(), 3);
    for item in &result.items {
        assert!(item.get("pk").is_some());
        assert!(item.get("sk").is_some());
        assert!(item.get("name").is_some());
        assert!(item.get("age").is_none());
    }

    // batch_get_item with projection.
    let keys = vec![
        (json!("org1"), Some(json!("user#0"))),
        (json!("org1"), Some(json!("user#2"))),
    ];
    let results = client
        .batch_get_item("users", &keys, Some(&name_only))
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    for item in results.iter().flatten() {
        assert!(item.get("pk").is_some());
        assert!(item.get("name").is_some());
        assert!(item.get("age").is_none());
    }
}

#[tokio::test]
async fn test_composite_index_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "data",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Create composite index with sort key.
    client
        .create_index(
            "data",
            "cat-price-idx",
            None,
            Some("category"),
            Some("String"),
            Some("price"),
            Some("Number"),
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Verify index has sort key info.
    let idx = client
        .describe_index("data", "cat-price-idx")
        .await
        .unwrap();
    assert_eq!(idx.index_key_name, "category");
    assert_eq!(idx.index_sort_key_name, Some("price".to_string()));
    assert_eq!(idx.index_sort_key_type, Some("Number".to_string()));

    // Insert documents.
    for (pk, cat, price) in [
        ("p1", "electronics", 10.0),
        ("p2", "electronics", 50.0),
        ("p3", "electronics", 100.0),
        ("p4", "books", 20.0),
    ] {
        client
            .put_item("data", json!({"pk": pk, "category": cat, "price": price}))
            .await
            .unwrap();
    }

    // Query with sort condition via wire protocol.
    let result = client
        .query_index(
            "data",
            "cat-price-idx",
            json!("electronics"),
            Some(SortKeyCondition::Gt { value: json!(40.0) }),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 2);

    // Query without sort condition — all electronics.
    let result = client
        .query_index(
            "data",
            "cat-price-idx",
            json!("electronics"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 3);
}

#[tokio::test]
async fn test_local_index_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // Create table with sort key (required for LSI).
    client
        .create_table(
            "orders",
            KeyDef {
                name: "customer".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "order_id".to_string(),
                key_type: "String".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    // Create a local secondary index (no index_key needed — auto-inferred).
    client
        .create_index(
            "orders",
            "ts-idx",
            None,
            None,
            None,
            Some("timestamp"),
            Some("Number"),
            None,
            None,
            Some(true),
        )
        .await
        .unwrap();

    // Verify the index is created and marked as local.
    let idx = client.describe_index("orders", "ts-idx").await.unwrap();
    assert_eq!(idx.name, "ts-idx");
    assert!(idx.is_local);
    assert_eq!(idx.index_key_name, "customer");
    assert_eq!(idx.index_sort_key_name, Some("timestamp".to_string()));

    // Insert orders for two customers.
    client
        .put_item(
            "orders",
            json!({"customer": "alice", "order_id": "o1", "timestamp": 100.0, "total": 50.0}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "orders",
            json!({"customer": "alice", "order_id": "o2", "timestamp": 200.0, "total": 75.0}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "orders",
            json!({"customer": "bob", "order_id": "o3", "timestamp": 50.0, "total": 100.0}),
        )
        .await
        .unwrap();

    // Query LSI with table pk value.
    let result = client
        .query_index(
            "orders",
            "ts-idx",
            json!("alice"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 2);
    assert_eq!(result.items[0]["timestamp"], 100.0);
    assert_eq!(result.items[1]["timestamp"], 200.0);

    // Query with sort key condition.
    let result = client
        .query_index(
            "orders",
            "ts-idx",
            json!("alice"),
            Some(SortKeyCondition::Gt {
                value: json!(150.0),
            }),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0]["timestamp"], 200.0);

    // Bob's orders.
    let result = client
        .query_index(
            "orders",
            "ts-idx",
            json!("bob"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0]["total"], 100.0);
}

// ---------------------------------------------------------------------------
// Change stream tests (PRD-10 Phase 4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stream_records_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // Create table.
    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Enable stream.
    client.enable_stream("items", "KEYS_ONLY").await.unwrap();

    // Write some items.
    client
        .put_item("items", json!({"id": "a", "val": 1}))
        .await
        .unwrap();
    client
        .put_item("items", json!({"id": "b", "val": 2}))
        .await
        .unwrap();

    // Update item a.
    client
        .update_item(
            "items",
            json!("a"),
            None,
            &[UpdateActionInput {
                action: "set".to_string(),
                path: "val".to_string(),
                value: Some(json!(10)),
            }],
        )
        .await
        .unwrap();

    // Delete item b.
    client.delete_item("items", json!("b"), None).await.unwrap();

    // Get all stream records.
    let records = client
        .get_stream_records("items", None, None)
        .await
        .unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(records[0].event_type, "INSERT");
    assert_eq!(records[1].event_type, "INSERT");
    assert_eq!(records[2].event_type, "MODIFY");
    assert_eq!(records[3].event_type, "REMOVE");

    // Pagination: after_sequence.
    let first_seq = records[0].sequence_number;
    let after = client
        .get_stream_records("items", Some(first_seq), None)
        .await
        .unwrap();
    assert_eq!(after.len(), 3); // skipped the first record

    // Pagination: limit.
    let limited = client
        .get_stream_records("items", None, Some(2))
        .await
        .unwrap();
    assert_eq!(limited.len(), 2);
}

#[tokio::test]
async fn test_stream_info_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // Create table.
    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Enable stream.
    client
        .enable_stream("items", "NEW_AND_OLD_IMAGES")
        .await
        .unwrap();

    // Check stream info on empty stream.
    let info = client.get_stream_info("items").await.unwrap();
    assert!(info.enabled);
    assert_eq!(info.view_type, "NEW_AND_OLD_IMAGES");
    assert_eq!(info.record_count, 0);
    assert!(info.oldest_sequence.is_none());
    assert!(info.latest_sequence.is_none());

    // Write items.
    client
        .put_item("items", json!({"id": "x", "val": 1}))
        .await
        .unwrap();
    client
        .put_item("items", json!({"id": "y", "val": 2}))
        .await
        .unwrap();

    // Check stream info after writes.
    let info = client.get_stream_info("items").await.unwrap();
    assert_eq!(info.record_count, 2);
    assert!(info.oldest_sequence.is_some());
    assert!(info.latest_sequence.is_some());
    assert!(info.latest_sequence.unwrap() >= info.oldest_sequence.unwrap());

    // Disable stream.
    client.disable_stream("items").await.unwrap();

    // Records should still be readable.
    let records = client
        .get_stream_records("items", None, None)
        .await
        .unwrap();
    assert_eq!(records.len(), 2);

    // Info should show disabled.
    let info = client.get_stream_info("items").await.unwrap();
    assert!(!info.enabled);

    // Enable stream with a different error (already has stream config).
    // Re-enable should work.
    client.enable_stream("items", "KEYS_ONLY").await.unwrap();
    let info = client.get_stream_info("items").await.unwrap();
    assert!(info.enabled);
    assert_eq!(info.view_type, "KEYS_ONLY");
}

#[tokio::test]
async fn test_stream_not_enabled_error() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // Create table without stream.
    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Trying to get stream records should fail.
    let result = client.get_stream_records("items", None, None).await;
    assert!(result.is_err());

    // Trying to get stream info should fail.
    let result = client.get_stream_info("items").await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Sort key range query tests (PRD-11)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sort_key_range_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // Create table with sort key.
    client
        .create_table(
            "events",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "ts".to_string(),
                key_type: "Number".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    // Insert 5 items with same pk, varying sort key.
    for i in 1..=5 {
        client
            .put_item(
                "events",
                json!({"pk": "sensor1", "ts": (i as f64) * 10.0, "val": i}),
            )
            .await
            .unwrap();
    }

    // Between condition: ts BETWEEN 20.0 AND 40.0.
    let result = client
        .query(
            "events",
            json!("sensor1"),
            Some(SortKeyCondition::Between {
                low: json!(20.0),
                high: json!(40.0),
            }),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 3);
    let ts: Vec<f64> = result
        .items
        .iter()
        .map(|i| i["ts"].as_f64().unwrap())
        .collect();
    assert_eq!(ts, vec![20.0, 30.0, 40.0]);

    // Gt condition: ts > 30.0.
    let result = client
        .query(
            "events",
            json!("sensor1"),
            Some(SortKeyCondition::Gt { value: json!(30.0) }),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 2);
    let ts: Vec<f64> = result
        .items
        .iter()
        .map(|i| i["ts"].as_f64().unwrap())
        .collect();
    assert_eq!(ts, vec![40.0, 50.0]);

    // Lt condition: ts < 30.0.
    let result = client
        .query(
            "events",
            json!("sensor1"),
            Some(SortKeyCondition::Lt { value: json!(30.0) }),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 2);
    let ts: Vec<f64> = result
        .items
        .iter()
        .map(|i| i["ts"].as_f64().unwrap())
        .collect();
    assert_eq!(ts, vec![10.0, 20.0]);

    // Le condition: ts <= 30.0.
    let result = client
        .query(
            "events",
            json!("sensor1"),
            Some(SortKeyCondition::Le { value: json!(30.0) }),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 3);

    // Ge condition: ts >= 30.0.
    let result = client
        .query(
            "events",
            json!("sensor1"),
            Some(SortKeyCondition::Ge { value: json!(30.0) }),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 3);
}

#[tokio::test]
async fn test_sort_key_eq_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // Create table with string sort key.
    client
        .create_table(
            "items",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "sk".to_string(),
                key_type: "String".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    // Insert items.
    for name in &["alpha", "bravo", "charlie"] {
        client
            .put_item(
                "items",
                json!({"pk": "p1", "sk": *name, "data": format!("val_{name}")}),
            )
            .await
            .unwrap();
    }

    // Exact match via Eq condition.
    let result = client
        .query(
            "items",
            json!("p1"),
            Some(SortKeyCondition::Eq {
                value: json!("bravo"),
            }),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0]["sk"], "bravo");
    assert_eq!(result.items[0]["data"], "val_bravo");

    // Eq with nonexistent value — empty result.
    let result = client
        .query(
            "items",
            json!("p1"),
            Some(SortKeyCondition::Eq {
                value: json!("zulu"),
            }),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 0);
}

// ---------------------------------------------------------------------------
// Batch write tests (PRD-12)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_batch_write_put_over_wire() {
    use ferridyn_server::client::BatchWriteInput;

    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    let ops = vec![
        BatchWriteInput::Put {
            table: "items".to_string(),
            item: json!({"id": "a", "val": 1}),
        },
        BatchWriteInput::Put {
            table: "items".to_string(),
            item: json!({"id": "b", "val": 2}),
        },
        BatchWriteInput::Put {
            table: "items".to_string(),
            item: json!({"id": "c", "val": 3}),
        },
    ];

    let count = client.batch_write_item(&ops).await.unwrap();
    assert_eq!(count, 3);

    // Verify all items readable.
    for (key, expected_val) in [("a", 1), ("b", 2), ("c", 3)] {
        let item = client
            .get_item("items", json!(key), None, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item["val"], expected_val);
    }
}

#[tokio::test]
async fn test_batch_write_delete_over_wire() {
    use ferridyn_server::client::BatchWriteInput;

    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Insert items first.
    for key in &["a", "b", "c"] {
        client
            .put_item("items", json!({"id": *key, "val": 1}))
            .await
            .unwrap();
    }

    // Batch delete a and b.
    let ops = vec![
        BatchWriteInput::Delete {
            table: "items".to_string(),
            partition_key: json!("a"),
            sort_key: None,
        },
        BatchWriteInput::Delete {
            table: "items".to_string(),
            partition_key: json!("b"),
            sort_key: None,
        },
    ];

    let count = client.batch_write_item(&ops).await.unwrap();
    assert_eq!(count, 2);

    // Verify a and b are gone, c remains.
    assert!(
        client
            .get_item("items", json!("a"), None, None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        client
            .get_item("items", json!("b"), None, None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        client
            .get_item("items", json!("c"), None, None)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn test_batch_write_mixed_over_wire() {
    use ferridyn_server::client::BatchWriteInput;

    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Insert initial items.
    client
        .put_item("items", json!({"id": "a", "val": 1}))
        .await
        .unwrap();
    client
        .put_item("items", json!({"id": "b", "val": 2}))
        .await
        .unwrap();

    // Mixed batch: delete a, put c.
    let ops = vec![
        BatchWriteInput::Delete {
            table: "items".to_string(),
            partition_key: json!("a"),
            sort_key: None,
        },
        BatchWriteInput::Put {
            table: "items".to_string(),
            item: json!({"id": "c", "val": 3}),
        },
    ];

    let count = client.batch_write_item(&ops).await.unwrap();
    assert_eq!(count, 2);

    assert!(
        client
            .get_item("items", json!("a"), None, None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        client
            .get_item("items", json!("b"), None, None)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        client
            .get_item("items", json!("c"), None, None)
            .await
            .unwrap()
            .unwrap()["val"],
        3
    );
}

#[tokio::test]
async fn test_batch_write_cross_table() {
    use ferridyn_server::client::BatchWriteInput;

    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // Create two tables.
    client
        .create_table(
            "users",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();
    client
        .create_table(
            "orders",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Batch write across both tables.
    let ops = vec![
        BatchWriteInput::Put {
            table: "users".to_string(),
            item: json!({"id": "alice", "name": "Alice"}),
        },
        BatchWriteInput::Put {
            table: "orders".to_string(),
            item: json!({"id": "order1", "user": "alice", "total": 42}),
        },
    ];

    let count = client.batch_write_item(&ops).await.unwrap();
    assert_eq!(count, 2);

    let user = client
        .get_item("users", json!("alice"), None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(user["name"], "Alice");

    let order = client
        .get_item("orders", json!("order1"), None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(order["total"], 42);
}

#[tokio::test]
async fn test_batch_write_exceeds_limit() {
    use ferridyn_server::client::BatchWriteInput;

    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Build 26 operations (exceeds limit of 25).
    let ops: Vec<BatchWriteInput> = (0..26)
        .map(|i| BatchWriteInput::Put {
            table: "items".to_string(),
            item: json!({"id": format!("item-{i}"), "val": i}),
        })
        .collect();

    let result = client.batch_write_item(&ops).await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("BatchSizeLimitExceeded"),
        "expected BatchSizeLimitExceeded error, got: {err}"
    );
}

#[tokio::test]
async fn test_batch_write_error_rolls_back() {
    use ferridyn_server::client::BatchWriteInput;

    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Mix a valid put with a put to a nonexistent table.
    let ops = vec![
        BatchWriteInput::Put {
            table: "items".to_string(),
            item: json!({"id": "good", "val": 1}),
        },
        BatchWriteInput::Put {
            table: "nonexistent".to_string(),
            item: json!({"id": "bad"}),
        },
    ];

    let result = client.batch_write_item(&ops).await;
    assert!(result.is_err());

    // "good" should NOT be visible because entire batch was rolled back.
    let item = client
        .get_item("items", json!("good"), None, None)
        .await
        .unwrap();
    assert!(
        item.is_none(),
        "failed batch should not leave partial writes"
    );
}

#[tokio::test]
async fn test_batch_write_empty() {
    use ferridyn_server::client::BatchWriteInput;

    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    // No table needed — empty batch should succeed without touching anything.
    let ops: Vec<BatchWriteInput> = vec![];
    let count = client.batch_write_item(&ops).await.unwrap();
    assert_eq!(count, 0);
}

// ---------------------------------------------------------------------------
// TTL operations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_set_ttl_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "cache",
            KeyDef {
                name: "key".to_string(),
                key_type: "String".to_string(),
            },
            None,
            Some("expires".to_string()),
        )
        .await
        .unwrap();

    client
        .put_item("cache", json!({"key": "a", "val": "data"}))
        .await
        .unwrap();

    // Set a 3600-second TTL.
    client
        .set_ttl("cache", json!("a"), None, 3600)
        .await
        .unwrap();

    // Verify via get_ttl.
    let remaining = client.get_ttl("cache", json!("a"), None).await.unwrap();
    assert!(remaining.is_some());
    let secs = remaining.unwrap();
    assert!(
        secs >= 3595 && secs <= 3600,
        "expected ~3600 remaining, got {secs}"
    );
}

#[tokio::test]
async fn test_remove_ttl_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "cache",
            KeyDef {
                name: "key".to_string(),
                key_type: "String".to_string(),
            },
            None,
            Some("expires".to_string()),
        )
        .await
        .unwrap();

    client
        .put_item("cache", json!({"key": "a", "val": "data"}))
        .await
        .unwrap();

    // Set a TTL, then remove it.
    client.set_ttl("cache", json!("a"), None, 60).await.unwrap();
    client.remove_ttl("cache", json!("a"), None).await.unwrap();

    // get_ttl should return None (TTL=0 means permanent).
    let remaining = client.get_ttl("cache", json!("a"), None).await.unwrap();
    assert!(
        remaining.is_none(),
        "after remove_ttl, get_ttl should return None"
    );

    // Item should still be accessible.
    let item = client
        .get_item("cache", json!("a"), None, None)
        .await
        .unwrap();
    assert!(item.is_some(), "item should be permanent after remove_ttl");
}

#[tokio::test]
async fn test_get_ttl_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "cache",
            KeyDef {
                name: "key".to_string(),
                key_type: "String".to_string(),
            },
            None,
            Some("expires".to_string()),
        )
        .await
        .unwrap();

    // Item without TTL.
    client
        .put_item("cache", json!({"key": "no_ttl", "val": "permanent"}))
        .await
        .unwrap();
    let remaining = client
        .get_ttl("cache", json!("no_ttl"), None)
        .await
        .unwrap();
    assert!(remaining.is_none(), "item without TTL should return None");

    // Item with expired TTL.
    client
        .put_item("cache", json!({"key": "old", "expires": 1000}))
        .await
        .unwrap();
    let remaining = client.get_ttl("cache", json!("old"), None).await.unwrap();
    assert_eq!(remaining, Some(0), "expired item should return Some(0)");
}

#[tokio::test]
async fn test_sweep_ttl_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "cache",
            KeyDef {
                name: "key".to_string(),
                key_type: "String".to_string(),
            },
            None,
            Some("expires".to_string()),
        )
        .await
        .unwrap();

    // Insert 2 expired and 1 alive.
    client
        .put_item("cache", json!({"key": "exp1", "expires": 1000}))
        .await
        .unwrap();
    client
        .put_item("cache", json!({"key": "exp2", "expires": 2000}))
        .await
        .unwrap();
    client
        .put_item("cache", json!({"key": "alive", "expires": 9999999999.0}))
        .await
        .unwrap();

    let count = client.sweep_expired_ttl("cache").await.unwrap();
    assert_eq!(count, 2, "should sweep 2 expired items");

    // Second sweep should find nothing.
    let count2 = client.sweep_expired_ttl("cache").await.unwrap();
    assert_eq!(count2, 0, "second sweep should find nothing");

    // Alive item should still be there.
    let item = client
        .get_item("cache", json!("alive"), None, None)
        .await
        .unwrap();
    assert!(item.is_some(), "non-expired item should survive sweep");
}

#[tokio::test]
async fn test_count_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "sk".to_string(),
                key_type: "String".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    for i in 0..5 {
        client
            .put_item("items", json!({"pk": "a", "sk": format!("s{i}"), "v": i}))
            .await
            .unwrap();
    }

    // Count all items in partition.
    let count = client.count("items", json!("a"), None, None).await.unwrap();
    assert_eq!(count, 5);

    // Count with sort key condition.
    let count_with_sk = client
        .count(
            "items",
            json!("a"),
            Some(SortKeyCondition::BeginsWith {
                prefix: "s0".to_string(),
            }),
            None,
        )
        .await
        .unwrap();
    assert_eq!(count_with_sk, 1);

    // Count empty partition.
    let count_empty = client
        .count("items", json!("nonexistent"), None, None)
        .await
        .unwrap();
    assert_eq!(count_empty, 0);
}

#[tokio::test]
async fn test_count_with_filter_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            Some(KeyDef {
                name: "sk".to_string(),
                key_type: "Number".to_string(),
            }),
            None,
        )
        .await
        .unwrap();

    for i in 1..=10 {
        client
            .put_item("items", json!({"pk": "a", "sk": i, "even": i % 2 == 0}))
            .await
            .unwrap();
    }

    let filter = FilterExpr::Eq(
        Box::new(FilterExpr::Attr("even".to_string())),
        Box::new(FilterExpr::Literal(json!(true))),
    );

    let count = client
        .count("items", json!("a"), None, Some(filter))
        .await
        .unwrap();
    assert_eq!(count, 5, "should count only even items over wire");
}

#[tokio::test]
async fn test_count_index_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "data",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    client
        .create_index(
            "data",
            "cat-price-idx",
            None,
            Some("category"),
            Some("String"),
            Some("price"),
            Some("Number"),
            None,
            None,
            None,
        )
        .await
        .unwrap();

    for i in 1..=10 {
        client
            .put_item(
                "data",
                json!({"pk": format!("p{i}"), "category": "electronics", "price": i * 10}),
            )
            .await
            .unwrap();
    }
    client
        .put_item(
            "data",
            json!({"pk": "other", "category": "books", "price": 15}),
        )
        .await
        .unwrap();

    // Count all electronics.
    let count = client
        .count_index("data", "cat-price-idx", json!("electronics"), None, None)
        .await
        .unwrap();
    assert_eq!(count, 10);

    // Count with sort key condition (price between 30 and 70).
    let count_range = client
        .count_index(
            "data",
            "cat-price-idx",
            json!("electronics"),
            Some(SortKeyCondition::Between {
                low: json!(30),
                high: json!(70),
            }),
            None,
        )
        .await
        .unwrap();
    assert_eq!(count_range, 5);

    // Count empty result.
    let count_empty = client
        .count_index("data", "cat-price-idx", json!("nonexistent"), None, None)
        .await
        .unwrap();
    assert_eq!(count_empty, 0);
}

#[tokio::test]
async fn test_count_index_with_filter_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "data",
            KeyDef {
                name: "pk".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    client
        .create_index(
            "data",
            "status-idx",
            None,
            Some("status"),
            Some("String"),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    for i in 1..=10 {
        client
            .put_item(
                "data",
                json!({"pk": format!("p{i}"), "status": "active", "score": i}),
            )
            .await
            .unwrap();
    }

    let filter = FilterExpr::Gt(
        Box::new(FilterExpr::Attr("score".to_string())),
        Box::new(FilterExpr::Literal(json!(5))),
    );

    let count = client
        .count_index("data", "status-idx", json!("active"), None, Some(filter))
        .await
        .unwrap();
    assert_eq!(count, 5, "should count only items with score > 5 over wire");
}

// ---------------------------------------------------------------------------
// Vector index tests (PRD-15 Phase 4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_vector_create_index_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Create vector index.
    client
        .create_vector_index("items", "emb-idx", "embedding", 3, "cosine")
        .await
        .unwrap();

    // List vector indexes.
    let indexes = client.list_vector_indexes("items").await.unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "emb-idx");
    assert_eq!(indexes[0].attribute, "embedding");
    assert_eq!(indexes[0].dimensions, 3);
    assert_eq!(indexes[0].metric, "cosine");

    // Drop vector index.
    client.drop_vector_index("items", "emb-idx").await.unwrap();

    // List should be empty now.
    let indexes = client.list_vector_indexes("items").await.unwrap();
    assert_eq!(indexes.len(), 0);
}

#[tokio::test]
async fn test_vector_query_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    // Create vector index with euclidean metric.
    client
        .create_vector_index("items", "emb-idx", "embedding", 3, "euclidean")
        .await
        .unwrap();

    // Insert items with embeddings.
    client
        .put_item(
            "items",
            json!({"id": "a", "embedding": [1.0, 0.0, 0.0], "label": "x-axis"}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "items",
            json!({"id": "b", "embedding": [0.0, 1.0, 0.0], "label": "y-axis"}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "items",
            json!({"id": "c", "embedding": [0.9, 0.1, 0.0], "label": "near-x"}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "items",
            json!({"id": "d", "embedding": [0.0, 0.0, 1.0], "label": "z-axis"}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "items",
            json!({"id": "e", "embedding": [0.5, 0.5, 0.0], "label": "xy-mid"}),
        )
        .await
        .unwrap();

    // Query nearest to [1.0, 0.0, 0.0], top 3.
    let results = client
        .query_vector_index("items", "emb-idx", &[1.0, 0.0, 0.0], 3, None, None)
        .await
        .unwrap();

    assert_eq!(results.len(), 3);

    // First result should be exact match "a" with distance ~0.
    assert_eq!(results[0].item["id"], "a");
    assert!(
        results[0].score < 0.01,
        "exact match should have ~0 distance"
    );

    // Results should be sorted by distance (ascending).
    for i in 1..results.len() {
        assert!(
            results[i].score >= results[i - 1].score,
            "results should be sorted by distance"
        );
    }
}

#[tokio::test]
async fn test_vector_query_with_filter_over_wire() {
    let (_dir, sock) = start_test_server().await;
    let mut client = FerridynClient::connect(&sock).await.unwrap();

    client
        .create_table(
            "items",
            KeyDef {
                name: "id".to_string(),
                key_type: "String".to_string(),
            },
            None,
            None,
        )
        .await
        .unwrap();

    client
        .create_vector_index("items", "emb-idx", "embedding", 3, "euclidean")
        .await
        .unwrap();

    // Insert items in two categories.
    client
        .put_item(
            "items",
            json!({"id": "a1", "embedding": [1.0, 0.0, 0.0], "category": "A"}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "items",
            json!({"id": "a2", "embedding": [0.9, 0.1, 0.0], "category": "A"}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "items",
            json!({"id": "b1", "embedding": [0.95, 0.05, 0.0], "category": "B"}),
        )
        .await
        .unwrap();
    client
        .put_item(
            "items",
            json!({"id": "b2", "embedding": [0.0, 1.0, 0.0], "category": "B"}),
        )
        .await
        .unwrap();

    // Query with filter: category == "A".
    let filter = FilterExpr::Eq(
        Box::new(FilterExpr::Attr("category".to_string())),
        Box::new(FilterExpr::Literal(json!("A"))),
    );
    let results = client
        .query_vector_index("items", "emb-idx", &[1.0, 0.0, 0.0], 10, Some(filter), None)
        .await
        .unwrap();

    // All results should have category "A".
    assert!(!results.is_empty(), "should return results");
    for r in &results {
        assert_eq!(
            r.item["category"], "A",
            "all results should have category A"
        );
    }
    assert_eq!(results.len(), 2, "should return exactly 2 category A items");
}
