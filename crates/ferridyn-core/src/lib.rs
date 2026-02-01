//! # FerridynDB
//!
//! A local, embedded DynamoDB-style document database written in Rust.
//!
//! FerridynDB provides a DynamoDB-compatible key-value and document storage
//! engine backed by a single file, with MVCC snapshot isolation, a B+Tree
//! index, and a builder-pattern query API.
//!
//! ## Quick Start
//!
//! ```no_run
//! use ferridyn_core::api::FerridynDB;
//! use ferridyn_core::types::KeyType;
//! use serde_json::json;
//!
//! // Create or open a database
//! let db = FerridynDB::create("my_database.db").unwrap();
//!
//! // Create a table
//! db.create_table("users")
//!     .partition_key("user_id", KeyType::String)
//!     .execute()
//!     .unwrap();
//!
//! // Insert an item
//! db.put_item("users", json!({
//!     "user_id": "alice",
//!     "name": "Alice",
//!     "age": 30
//! })).unwrap();
//!
//! // Get an item
//! let item = db.get_item("users")
//!     .partition_key("alice")
//!     .execute()
//!     .unwrap();
//! assert_eq!(item.unwrap()["name"], "Alice");
//! ```

pub mod api;
pub mod btree;
pub mod catalog;
pub mod encoding;
pub mod error;
pub mod mvcc;
pub mod storage;
pub mod types;
