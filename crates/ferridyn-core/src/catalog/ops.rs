//! Catalog operations: create, drop, get, and list tables.
//!
//! The catalog stores table metadata in a dedicated B+Tree. Each entry is keyed
//! by the table name (encoded via `encode_string`) and the value is a
//! JSON-serialized `CatalogEntry`.

use crate::btree::PageStore;
use crate::btree::ops;
use crate::encoding::string::encode_string;
use crate::error::{Error, SchemaError, StorageError};
use crate::types::{IndexDefinition, KeyDefinition, PageId, PartitionSchema, TableSchema, TxnId};

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
        partition_schemas: Vec::new(),
        indexes: Vec::new(),
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

/// Add a partition schema to a table's catalog entry.
pub fn create_partition_schema(
    store: &mut impl PageStore,
    catalog_root: PageId,
    table_name: &str,
    schema: PartitionSchema,
) -> Result<PageId, Error> {
    let mut entry = get_table(store, catalog_root, table_name)?;

    // Check for duplicate prefix.
    if entry
        .partition_schemas
        .iter()
        .any(|ps| ps.prefix == schema.prefix)
    {
        return Err(SchemaError::PartitionSchemaAlreadyExists(schema.prefix).into());
    }

    entry.partition_schemas.push(schema);

    // Re-serialize and re-insert.
    let json_bytes = serde_json::to_vec(&entry).map_err(|e| {
        StorageError::CorruptedPage(format!("failed to serialize catalog entry: {e}"))
    })?;
    let encoded_name = encode_string(table_name);
    let new_catalog_root = ops::insert(store, catalog_root, &encoded_name, &json_bytes)?;

    Ok(new_catalog_root)
}

/// Remove a partition schema from a table's catalog entry.
/// Fails if any indexes reference this partition schema.
pub fn drop_partition_schema(
    store: &mut impl PageStore,
    catalog_root: PageId,
    table_name: &str,
    prefix: &str,
) -> Result<PageId, Error> {
    let mut entry = get_table(store, catalog_root, table_name)?;

    // Check if any indexes reference this schema.
    if entry
        .indexes
        .iter()
        .any(|idx| idx.partition_schema.as_deref() == Some(prefix))
    {
        return Err(SchemaError::PartitionSchemaHasIndexes {
            prefix: prefix.to_string(),
        }
        .into());
    }

    let original_len = entry.partition_schemas.len();
    entry.partition_schemas.retain(|ps| ps.prefix != prefix);

    if entry.partition_schemas.len() == original_len {
        return Err(SchemaError::PartitionSchemaNotFound(prefix.to_string()).into());
    }

    let json_bytes = serde_json::to_vec(&entry).map_err(|e| {
        StorageError::CorruptedPage(format!("failed to serialize catalog entry: {e}"))
    })?;
    let encoded_name = encode_string(table_name);
    let new_catalog_root = ops::insert(store, catalog_root, &encoded_name, &json_bytes)?;

    Ok(new_catalog_root)
}

/// List all partition schemas for a table.
pub fn list_partition_schemas(
    store: &impl PageStore,
    catalog_root: PageId,
    table_name: &str,
) -> Result<Vec<PartitionSchema>, Error> {
    let entry = get_table(store, catalog_root, table_name)?;
    Ok(entry.partition_schemas)
}

/// Create a secondary index on a table, with synchronous backfill.
///
/// When `partition_schema` is `Some(prefix)`, the index is scoped to that
/// partition schema and only documents whose partition key starts with the
/// prefix are indexed. When `None`, the index is global and all documents
/// with the indexed attribute are included.
pub fn create_index(
    store: &mut impl PageStore,
    catalog_root: PageId,
    table_name: &str,
    name: String,
    partition_schema: Option<String>,
    index_key: KeyDefinition,
    txn_id: TxnId,
) -> Result<PageId, Error> {
    use crate::api::key_utils;
    use crate::mvcc::ops as mvcc_ops;

    let mut entry = get_table(store, catalog_root, table_name)?;

    // Validate partition schema exists (only for scoped indexes).
    if let Some(ref ps) = partition_schema
        && !entry
            .partition_schemas
            .iter()
            .any(|schema| schema.prefix == *ps)
    {
        return Err(SchemaError::PartitionSchemaNotFound(ps.clone()).into());
    }

    // Check for duplicate index name.
    if entry.indexes.iter().any(|idx| idx.name == name) {
        return Err(SchemaError::IndexAlreadyExists(name).into());
    }

    // Create new empty B+Tree for the index.
    let mut index_root = ops::create_tree(store)?;

    // Build the IndexDefinition (root_page will be updated after backfill).
    let index_def = IndexDefinition {
        name: name.clone(),
        partition_schema: partition_schema.clone(),
        index_key,
        root_page: index_root,
    };

    // Backfill: scan all existing documents in the primary table.
    let all_entries = mvcc_ops::mvcc_range_scan(store, entry.data_root_page, None, None, txn_id)?;

    for (key_bytes, value_bytes) in &all_entries {
        // For scoped indexes, check if the document's prefix matches.
        if let Some(ref ps) = partition_schema {
            let has_sort_key = entry.schema.sort_key.is_some();
            let (pk_kv, _sk_kv) =
                crate::encoding::composite::decode_composite(key_bytes, has_sort_key)?;
            let prefix = key_utils::extract_pk_prefix(&pk_kv);
            if prefix.as_deref() != Some(ps.as_str()) {
                continue; // Different prefix — skip.
            }
        }

        // Deserialize the document.
        let doc: serde_json::Value = rmp_serde::from_slice(value_bytes).map_err(|e| {
            StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
        })?;

        // Build the index key.
        if let Some(idx_key) = key_utils::build_index_key(&index_def, &doc, &entry.schema)? {
            index_root = ops::insert(store, index_root, &idx_key, &[])?;
        }
    }

    // Add the index definition with the final root page.
    let final_index_def = IndexDefinition {
        root_page: index_root,
        ..index_def
    };
    entry.indexes.push(final_index_def);

    // Re-serialize and re-insert the catalog entry.
    let json_bytes = serde_json::to_vec(&entry).map_err(|e| {
        StorageError::CorruptedPage(format!("failed to serialize catalog entry: {e}"))
    })?;
    let encoded_name = encode_string(table_name);
    let new_catalog_root = ops::insert(store, catalog_root, &encoded_name, &json_bytes)?;

    Ok(new_catalog_root)
}

/// Remove an index from a table's catalog entry.
/// Frees all B+Tree pages used by the index.
pub fn drop_index(
    store: &mut impl PageStore,
    catalog_root: PageId,
    table_name: &str,
    index_name: &str,
) -> Result<PageId, Error> {
    let mut entry = get_table(store, catalog_root, table_name)?;

    // Find the index to get its root page before removing.
    let idx_pos = entry.indexes.iter().position(|idx| idx.name == index_name);
    let idx_pos = match idx_pos {
        Some(pos) => pos,
        None => return Err(SchemaError::IndexNotFound(index_name.to_string()).into()),
    };

    // Free the index B+Tree pages.
    let index_root = entry.indexes[idx_pos].root_page;
    ops::free_tree(store, index_root)?;

    entry.indexes.remove(idx_pos);

    let json_bytes = serde_json::to_vec(&entry).map_err(|e| {
        StorageError::CorruptedPage(format!("failed to serialize catalog entry: {e}"))
    })?;
    let encoded_name = encode_string(table_name);
    let new_catalog_root = ops::insert(store, catalog_root, &encoded_name, &json_bytes)?;

    Ok(new_catalog_root)
}

/// List all indexes for a table.
pub fn list_indexes(
    store: &impl PageStore,
    catalog_root: PageId,
    table_name: &str,
) -> Result<Vec<IndexDefinition>, Error> {
    let entry = get_table(store, catalog_root, table_name)?;
    Ok(entry.indexes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::InMemoryPageStore;
    use crate::types::{KeyDefinition, KeyType, PartitionSchema};

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
            ttl_attribute: None,
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
            ttl_attribute: None,
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
            ttl_attribute: None,
        };
        let schema_orders = TableSchema {
            name: "orders".to_string(),
            partition_key: KeyDefinition {
                name: "order_id".to_string(),
                key_type: KeyType::Number,
            },
            sort_key: None,
            ttl_attribute: None,
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

    // --- Partition schema tests ---

    #[test]
    fn test_create_and_list_partition_schema() {
        let (mut store, mut root) = setup();
        let (new_root, _) = create_table(&mut store, root, make_schema("data")).unwrap();
        root = new_root;

        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        root = create_partition_schema(&mut store, root, "data", ps).unwrap();

        let schemas = list_partition_schemas(&store, root, "data").unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].prefix, "CONTACT");
        assert_eq!(schemas[0].description, "People");
    }

    #[test]
    fn test_duplicate_partition_schema() {
        let (mut store, mut root) = setup();
        let (new_root, _) = create_table(&mut store, root, make_schema("data")).unwrap();
        root = new_root;

        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        root = create_partition_schema(&mut store, root, "data", ps.clone()).unwrap();
        let result = create_partition_schema(&mut store, root, "data", ps);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("already exists"));
    }

    #[test]
    fn test_drop_partition_schema() {
        let (mut store, mut root) = setup();
        let (new_root, _) = create_table(&mut store, root, make_schema("data")).unwrap();
        root = new_root;

        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        root = create_partition_schema(&mut store, root, "data", ps).unwrap();
        root = drop_partition_schema(&mut store, root, "data", "CONTACT").unwrap();

        let schemas = list_partition_schemas(&store, root, "data").unwrap();
        assert!(schemas.is_empty());
    }

    #[test]
    fn test_drop_partition_schema_not_found() {
        let (mut store, mut root) = setup();
        let (new_root, _) = create_table(&mut store, root, make_schema("data")).unwrap();
        root = new_root;

        let result = drop_partition_schema(&mut store, root, "data", "NONEXISTENT");
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("not found"));
    }

    #[test]
    fn test_drop_partition_schema_with_indexes() {
        let (mut store, mut root) = setup();
        let (new_root, _) = create_table(&mut store, root, make_schema("data")).unwrap();
        root = new_root;

        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        root = create_partition_schema(&mut store, root, "data", ps).unwrap();

        root = create_index(
            &mut store,
            root,
            "data",
            "email-idx".to_string(),
            Some("CONTACT".to_string()),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            1,
        )
        .unwrap();

        let result = drop_partition_schema(&mut store, root, "data", "CONTACT");
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("has associated indexes"));
    }

    // --- Index tests ---

    #[test]
    fn test_create_index_empty_table() {
        let (mut store, mut root) = setup();
        let (new_root, _) = create_table(&mut store, root, make_schema("data")).unwrap();
        root = new_root;

        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        root = create_partition_schema(&mut store, root, "data", ps).unwrap();

        root = create_index(
            &mut store,
            root,
            "data",
            "email-idx".to_string(),
            Some("CONTACT".to_string()),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            1,
        )
        .unwrap();

        let indexes = list_indexes(&store, root, "data").unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "email-idx");
        assert_eq!(indexes[0].partition_schema, Some("CONTACT".to_string()));
    }

    #[test]
    fn test_create_index_duplicate_name() {
        let (mut store, mut root) = setup();
        let (new_root, _) = create_table(&mut store, root, make_schema("data")).unwrap();
        root = new_root;

        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        root = create_partition_schema(&mut store, root, "data", ps).unwrap();

        root = create_index(
            &mut store,
            root,
            "data",
            "email-idx".to_string(),
            Some("CONTACT".to_string()),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            1,
        )
        .unwrap();

        let result = create_index(
            &mut store,
            root,
            "data",
            "email-idx".to_string(),
            Some("CONTACT".to_string()),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            1,
        );
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("already exists"));
    }

    #[test]
    fn test_create_index_nonexistent_schema() {
        let (mut store, mut root) = setup();
        let (new_root, _) = create_table(&mut store, root, make_schema("data")).unwrap();
        root = new_root;

        let result = create_index(
            &mut store,
            root,
            "data",
            "email-idx".to_string(),
            Some("NONEXISTENT".to_string()),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            1,
        );
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("not found"));
    }

    #[test]
    fn test_create_index_with_backfill() {
        use crate::encoding::KeyValue;
        use crate::encoding::composite;
        use crate::mvcc::ops as mvcc_ops;

        let (mut store, mut root) = setup();
        let (new_root, entry) = create_table(&mut store, root, make_schema("data")).unwrap();
        root = new_root;
        let mut data_root = entry.data_root_page;

        // Insert some documents.
        let doc1 = serde_json::json!({"pk": "CONTACT#alice", "email": "alice@ex.com"});
        let doc1_bytes = rmp_serde::to_vec(&doc1).unwrap();
        let key1 =
            composite::encode_composite(&KeyValue::String("CONTACT#alice".to_string()), None)
                .unwrap();
        data_root = mvcc_ops::mvcc_put(&mut store, data_root, &key1, &doc1_bytes, 1).unwrap();

        let doc2 = serde_json::json!({"pk": "CONTACT#bob", "email": "bob@ex.com"});
        let doc2_bytes = rmp_serde::to_vec(&doc2).unwrap();
        let key2 = composite::encode_composite(&KeyValue::String("CONTACT#bob".to_string()), None)
            .unwrap();
        data_root = mvcc_ops::mvcc_put(&mut store, data_root, &key2, &doc2_bytes, 2).unwrap();

        // A doc with a different prefix — should NOT be indexed.
        let doc3 = serde_json::json!({"pk": "ORDER#123", "email": "order@ex.com"});
        let doc3_bytes = rmp_serde::to_vec(&doc3).unwrap();
        let key3 =
            composite::encode_composite(&KeyValue::String("ORDER#123".to_string()), None).unwrap();
        data_root = mvcc_ops::mvcc_put(&mut store, data_root, &key3, &doc3_bytes, 3).unwrap();

        // Update the catalog entry with the new data root.
        let mut updated_entry = get_table(&store, root, "data").unwrap();
        updated_entry.data_root_page = data_root;
        let json_bytes = serde_json::to_vec(&updated_entry).unwrap();
        let encoded_name = encode_string("data");
        root = ops::insert(&mut store, root, &encoded_name, &json_bytes).unwrap();

        // Create partition schema and index.
        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        root = create_partition_schema(&mut store, root, "data", ps).unwrap();

        root = create_index(
            &mut store,
            root,
            "data",
            "email-idx".to_string(),
            Some("CONTACT".to_string()),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            4,
        )
        .unwrap();

        // Verify the index has entries.
        let entry = get_table(&store, root, "data").unwrap();
        assert_eq!(entry.indexes.len(), 1);
        let index_root = entry.indexes[0].root_page;

        // Scan the index B+Tree to count entries.
        let index_entries = ops::range_scan(&store, index_root, None, None).unwrap();
        assert_eq!(index_entries.len(), 2); // alice and bob, NOT order
    }

    #[test]
    fn test_create_index_backfill_skips_missing_attr() {
        use crate::encoding::KeyValue;
        use crate::encoding::composite;
        use crate::mvcc::ops as mvcc_ops;

        let (mut store, mut root) = setup();
        let (new_root, entry) = create_table(&mut store, root, make_schema("data")).unwrap();
        root = new_root;
        let mut data_root = entry.data_root_page;

        // Doc WITH email.
        let doc1 = serde_json::json!({"pk": "CONTACT#alice", "email": "alice@ex.com"});
        let doc1_bytes = rmp_serde::to_vec(&doc1).unwrap();
        let key1 =
            composite::encode_composite(&KeyValue::String("CONTACT#alice".to_string()), None)
                .unwrap();
        data_root = mvcc_ops::mvcc_put(&mut store, data_root, &key1, &doc1_bytes, 1).unwrap();

        // Doc WITHOUT email.
        let doc2 = serde_json::json!({"pk": "CONTACT#bob", "name": "Bob"});
        let doc2_bytes = rmp_serde::to_vec(&doc2).unwrap();
        let key2 = composite::encode_composite(&KeyValue::String("CONTACT#bob".to_string()), None)
            .unwrap();
        data_root = mvcc_ops::mvcc_put(&mut store, data_root, &key2, &doc2_bytes, 2).unwrap();

        // Update catalog.
        let mut updated_entry = get_table(&store, root, "data").unwrap();
        updated_entry.data_root_page = data_root;
        let json_bytes = serde_json::to_vec(&updated_entry).unwrap();
        let encoded_name = encode_string("data");
        root = ops::insert(&mut store, root, &encoded_name, &json_bytes).unwrap();

        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        root = create_partition_schema(&mut store, root, "data", ps).unwrap();

        root = create_index(
            &mut store,
            root,
            "data",
            "email-idx".to_string(),
            Some("CONTACT".to_string()),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            3,
        )
        .unwrap();

        let entry = get_table(&store, root, "data").unwrap();
        let index_entries =
            ops::range_scan(&store, entry.indexes[0].root_page, None, None).unwrap();
        assert_eq!(index_entries.len(), 1); // Only alice
    }

    #[test]
    fn test_drop_index() {
        let (mut store, mut root) = setup();
        let (new_root, _) = create_table(&mut store, root, make_schema("data")).unwrap();
        root = new_root;

        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        root = create_partition_schema(&mut store, root, "data", ps).unwrap();

        root = create_index(
            &mut store,
            root,
            "data",
            "email-idx".to_string(),
            Some("CONTACT".to_string()),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            1,
        )
        .unwrap();

        root = drop_index(&mut store, root, "data", "email-idx").unwrap();

        let indexes = list_indexes(&store, root, "data").unwrap();
        assert!(indexes.is_empty());
    }

    #[test]
    fn test_drop_index_not_found() {
        let (mut store, mut root) = setup();
        let (new_root, _) = create_table(&mut store, root, make_schema("data")).unwrap();
        root = new_root;

        let result = drop_index(&mut store, root, "data", "nonexistent");
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("not found"));
    }
}
