//! Integration tests for ferridyn-server: start server, connect client, verify ops.

use serde_json::json;
use tempfile::tempdir;
use tokio::time::{Duration, sleep};

use ferridyn_core::api::FerridynDB;
use ferridyn_server::client::{AttributeDefInput, FerridynClient, UpdateActionInput};
use ferridyn_server::protocol::KeyDef;
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
        .get_item("users", json!("alice"), None)
        .await
        .unwrap();
    assert!(item.is_some());
    let item = item.unwrap();
    assert_eq!(item["name"], "Alice");
    assert_eq!(item["age"], 30);

    // Get nonexistent item.
    let missing = client.get_item("users", json!("bob"), None).await.unwrap();
    assert!(missing.is_none());

    // Delete item.
    client
        .delete_item("users", json!("alice"), None)
        .await
        .unwrap();
    let deleted = client
        .get_item("users", json!("alice"), None)
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
        .get_item("items", json!("key1"), None)
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
        .get_item("items", json!("shared"), None)
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
        .query("events", json!("user1"), None, None, None, None)
        .await
        .unwrap();
    assert_eq!(result.items.len(), 10);

    // Query with limit.
    let result = client
        .query("events", json!("user1"), None, Some(3), None, None)
        .await
        .unwrap();
    assert_eq!(result.items.len(), 3);
    assert!(result.last_evaluated_key.is_some());

    // Scan all.
    let result = client.scan("events", None, None).await.unwrap();
    assert_eq!(result.items.len(), 10);

    // Scan with limit.
    let result = client.scan("events", Some(5), None).await.unwrap();
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

    let result = client.get_item("nonexistent", json!("key"), None).await;
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
        .get_item("users", json!("alice"), None)
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
        .get_item("users", json!("bob"), None)
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
        .get_item("counters", json!("item1"), None)
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
        .create_index("data", "email-idx", "CONTACT", "email", "String")
        .await
        .unwrap();

    // List indexes — should have 1.
    let indexes = client.list_indexes("data").await.unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "email-idx");
    assert_eq!(indexes[0].partition_schema, "CONTACT");
    assert_eq!(indexes[0].index_key_name, "email");
    assert_eq!(indexes[0].index_key_type, "String");

    // Describe index.
    let index = client.describe_index("data", "email-idx").await.unwrap();
    assert_eq!(index.name, "email-idx");
    assert_eq!(index.partition_schema, "CONTACT");

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
        .create_index("data", "email-idx", "CONTACT", "email", "String")
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
        .query_index("data", "email-idx", json!("alice@example.com"), None, None)
        .await
        .unwrap();
    assert_eq!(result.items.len(), 2);

    // Query index for bob — should get 1 item.
    let result = client
        .query_index("data", "email-idx", json!("bob@example.com"), None, None)
        .await
        .unwrap();
    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0]["pk"], "CONTACT#2");

    // Query index for nonexistent — should get 0 items.
    let result = client
        .query_index("data", "email-idx", json!("nobody@example.com"), None, None)
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
        .create_index("data", "status-idx", "ITEM", "status", "String")
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
        .query_index("data", "status-idx", json!("active"), Some(2), None)
        .await
        .unwrap();
    assert_eq!(result.items.len(), 2);
}
