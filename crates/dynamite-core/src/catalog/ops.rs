//! Catalog operations: create, drop, get, and list tables.
//!
//! The catalog stores table metadata in a dedicated B+Tree. Each entry is keyed
//! by the table name (encoded via `encode_string`) and the value is a
//! JSON-serialized `CatalogEntry`.

use crate::btree::PageStore;
use crate::btree::ops;
use crate::encoding::string::encode_string;
use crate::error::{Error, SchemaError, StorageError};
use crate::types::{PageId, TableSchema};

use super::CatalogEntry;

/// Create a new table in the catalog.
///
/// Encodes the table name, checks for duplicates, allocates a new empty B+Tree
/// for the table's data, serializes the catalog entry, and inserts it into the
/// catalog B+Tree.
///
/// Returns the (possibly new) catalog root page ID and the created entry.
pub fn create_table(
    store: &mut impl PageStore,
    catalog_root: PageId,
    schema: TableSchema,
) -> Result<(PageId, CatalogEntry), Error> {
    let encoded_name = encode_string(&schema.name);

    // Check if the table already exists.
    let existing = ops::search(store, catalog_root, &encoded_name)?;
    if existing.is_some() {
        return Err(SchemaError::TableAlreadyExists(schema.name).into());
    }

    // Allocate a new empty B+Tree for the table's data.
    let data_root_page = ops::create_tree(store)?;

    let entry = CatalogEntry {
        schema,
        data_root_page,
    };

    let json_bytes = serde_json::to_vec(&entry).map_err(|e| {
        StorageError::CorruptedPage(format!("failed to serialize catalog entry: {e}"))
    })?;

    let new_catalog_root = ops::insert(store, catalog_root, &encoded_name, &json_bytes)?;

    Ok((new_catalog_root, entry))
}

/// Drop a table from the catalog.
///
/// Removes the catalog entry for the named table. Does not free the table's
/// data pages (that requires the page manager which is not yet wired up).
///
/// Returns the (possibly new) catalog root page ID.
pub fn drop_table(
    store: &mut impl PageStore,
    catalog_root: PageId,
    table_name: &str,
) -> Result<PageId, Error> {
    let encoded_name = encode_string(table_name);

    // Verify the table exists.
    let existing = ops::search(store, catalog_root, &encoded_name)?;
    if existing.is_none() {
        return Err(SchemaError::TableNotFound(table_name.to_string()).into());
    }

    let new_catalog_root = ops::delete(store, catalog_root, &encoded_name)?;

    Ok(new_catalog_root)
}

/// Look up a table in the catalog by name.
///
/// Returns the deserialized `CatalogEntry` containing the table's schema and
/// data root page.
pub fn get_table(
    store: &impl PageStore,
    catalog_root: PageId,
    table_name: &str,
) -> Result<CatalogEntry, Error> {
    let encoded_name = encode_string(table_name);

    let value = ops::search(store, catalog_root, &encoded_name)?;
    let json_bytes = value.ok_or_else(|| SchemaError::TableNotFound(table_name.to_string()))?;

    let entry: CatalogEntry = serde_json::from_slice(&json_bytes).map_err(|e| {
        StorageError::CorruptedPage(format!("failed to deserialize catalog entry: {e}"))
    })?;

    Ok(entry)
}

/// List all table names in the catalog.
///
/// Performs a full range scan of the catalog B+Tree and returns table names
/// in sorted order (by their encoded key representation).
pub fn list_tables(store: &impl PageStore, catalog_root: PageId) -> Result<Vec<String>, Error> {
    let pairs = ops::range_scan(store, catalog_root, None, None)?;

    let mut names = Vec::with_capacity(pairs.len());
    for (_key, value) in pairs {
        let entry: CatalogEntry = serde_json::from_slice(&value).map_err(|e| {
            StorageError::CorruptedPage(format!("failed to deserialize catalog entry: {e}"))
        })?;
        names.push(entry.schema.name);
    }

    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::InMemoryPageStore;
    use crate::types::{KeyDefinition, KeyType};

    /// Helper: create a fresh catalog (empty B+Tree) and return (store, root).
    fn setup() -> (InMemoryPageStore, PageId) {
        let mut store = InMemoryPageStore::new();
        let root = ops::create_tree(&mut store).unwrap();
        (store, root)
    }

    /// Helper: build a simple table schema with a string partition key.
    fn make_schema(name: &str) -> TableSchema {
        TableSchema {
            name: name.to_string(),
            partition_key: KeyDefinition {
                name: "pk".to_string(),
                key_type: KeyType::String,
            },
            sort_key: None,
        }
    }

    /// Helper: build a table schema with both partition and sort key.
    fn make_schema_with_sort_key(name: &str) -> TableSchema {
        TableSchema {
            name: name.to_string(),
            partition_key: KeyDefinition {
                name: "pk".to_string(),
                key_type: KeyType::String,
            },
            sort_key: Some(KeyDefinition {
                name: "sk".to_string(),
                key_type: KeyType::Number,
            }),
        }
    }

    #[test]
    fn test_create_and_get_table() {
        let (mut store, mut root) = setup();
        let schema = make_schema("users");

        let (new_root, entry) = create_table(&mut store, root, schema.clone()).unwrap();
        root = new_root;

        assert_eq!(entry.schema.name, "users");
        assert_eq!(entry.schema.partition_key.name, "pk");
        assert_eq!(entry.schema.partition_key.key_type, KeyType::String);
        assert!(entry.schema.sort_key.is_none());

        // Retrieve it back.
        let retrieved = get_table(&store, root, "users").unwrap();
        assert_eq!(retrieved.schema.name, "users");
        assert_eq!(retrieved.schema.partition_key.name, "pk");
        assert_eq!(retrieved.data_root_page, entry.data_root_page);
    }

    #[test]
    fn test_create_table_already_exists() {
        let (mut store, mut root) = setup();
        let schema = make_schema("orders");

        let (new_root, _) = create_table(&mut store, root, schema.clone()).unwrap();
        root = new_root;

        // Try to create the same table again.
        let result = create_table(&mut store, root, make_schema("orders"));
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = format!("{err}");
        assert!(
            err_msg.contains("already exists"),
            "expected 'already exists' error, got: {err_msg}"
        );
    }

    #[test]
    fn test_drop_table() {
        let (mut store, mut root) = setup();

        let (new_root, _) = create_table(&mut store, root, make_schema("sessions")).unwrap();
        root = new_root;

        // Verify it exists.
        assert!(get_table(&store, root, "sessions").is_ok());

        // Drop it.
        let new_root = drop_table(&mut store, root, "sessions").unwrap();
        root = new_root;

        // Verify it is gone.
        let result = get_table(&store, root, "sessions");
        assert!(result.is_err());

        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("not found"),
            "expected 'not found' error, got: {err_msg}"
        );
    }

    #[test]
    fn test_drop_nonexistent_table() {
        let (mut store, root) = setup();

        let result = drop_table(&mut store, root, "phantom");
        assert!(result.is_err());

        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("not found"),
            "expected 'not found' error, got: {err_msg}"
        );
    }

    #[test]
    fn test_list_tables() {
        let (mut store, mut root) = setup();

        let (new_root, _) = create_table(&mut store, root, make_schema("alpha")).unwrap();
        root = new_root;
        let (new_root, _) = create_table(&mut store, root, make_schema("beta")).unwrap();
        root = new_root;
        let (new_root, _) = create_table(&mut store, root, make_schema("gamma")).unwrap();
        root = new_root;

        let names = list_tables(&store, root).unwrap();
        assert_eq!(names.len(), 3);
        assert_eq!(names, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn test_list_tables_empty() {
        let (store, root) = setup();
        let names = list_tables(&store, root).unwrap();
        assert!(names.is_empty());
    }

    #[test]
    fn test_get_nonexistent_table() {
        let (store, root) = setup();

        let result = get_table(&store, root, "nonexistent");
        assert!(result.is_err());

        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("not found"),
            "expected 'not found' error, got: {err_msg}"
        );
    }

    #[test]
    fn test_multiple_tables_independent() {
        let (mut store, mut root) = setup();

        let schema_users = TableSchema {
            name: "users".to_string(),
            partition_key: KeyDefinition {
                name: "user_id".to_string(),
                key_type: KeyType::String,
            },
            sort_key: None,
        };
        let schema_orders = TableSchema {
            name: "orders".to_string(),
            partition_key: KeyDefinition {
                name: "order_id".to_string(),
                key_type: KeyType::Number,
            },
            sort_key: None,
        };

        let (new_root, entry_users) = create_table(&mut store, root, schema_users).unwrap();
        root = new_root;
        let (new_root, entry_orders) = create_table(&mut store, root, schema_orders).unwrap();
        root = new_root;

        // Each table should have its own independent data root page.
        assert_ne!(entry_users.data_root_page, entry_orders.data_root_page);

        // Verify schemas are stored independently.
        let users = get_table(&store, root, "users").unwrap();
        assert_eq!(users.schema.partition_key.name, "user_id");
        assert_eq!(users.schema.partition_key.key_type, KeyType::String);

        let orders = get_table(&store, root, "orders").unwrap();
        assert_eq!(orders.schema.partition_key.name, "order_id");
        assert_eq!(orders.schema.partition_key.key_type, KeyType::Number);
    }

    #[test]
    fn test_table_with_sort_key() {
        let (mut store, mut root) = setup();
        let schema = make_schema_with_sort_key("events");

        let (new_root, entry) = create_table(&mut store, root, schema).unwrap();
        root = new_root;

        assert_eq!(entry.schema.name, "events");
        assert!(entry.schema.sort_key.is_some());
        let sk = entry.schema.sort_key.as_ref().unwrap();
        assert_eq!(sk.name, "sk");
        assert_eq!(sk.key_type, KeyType::Number);

        // Round-trip through get_table.
        let retrieved = get_table(&store, root, "events").unwrap();
        assert_eq!(retrieved.schema.name, "events");
        let retrieved_sk = retrieved.schema.sort_key.as_ref().unwrap();
        assert_eq!(retrieved_sk.name, "sk");
        assert_eq!(retrieved_sk.key_type, KeyType::Number);
    }
}
