use serde_json::Value;

use crate::catalog;
use crate::encoding::composite;
use crate::error::{Error, QueryError, StorageError};
use crate::mvcc::ops as mvcc_ops;
use crate::types::{PageId, TxnId};

use super::key_utils;
use super::page_store::BufferedPageStore;

/// A write transaction that buffers all changes until commit.
///
/// All mutations go through the `BufferedPageStore`; on commit the overlay
/// is flushed to disk. If the closure returns an error (or panics),
/// the buffered writes are simply dropped (auto-abort).
pub struct Transaction {
    pub(crate) store: BufferedPageStore,
    pub(crate) catalog_root: PageId,
    pub(crate) txn_id: TxnId,
}

impl Transaction {
    /// Insert or replace an item in a table.
    pub fn put_item(&mut self, table: &str, document: Value) -> Result<(), Error> {
        // 1. Validate document size.
        let doc_bytes = key_utils::validate_document_size(&document)?;

        // 2. Look up table schema.
        let entry = catalog::ops::get_table(&self.store, self.catalog_root, table)?;
        let schema = &entry.schema;

        // 3. Extract partition key.
        let pk = key_utils::extract_key_from_doc(&document, &schema.partition_key)?;

        // 4. Extract sort key if present.
        let sk = if let Some(ref sk_def) = schema.sort_key {
            Some(key_utils::extract_key_from_doc(&document, sk_def)?)
        } else {
            None
        };

        // 5. Encode composite key.
        let composite_key = composite::encode_composite(&pk, sk.as_ref())?;

        // 6. Put with MVCC versioning.
        let new_data_root = mvcc_ops::mvcc_put(
            &mut self.store,
            entry.data_root_page,
            &composite_key,
            &doc_bytes,
            self.txn_id,
        )?;

        // 7. Update catalog if data_root changed.
        if new_data_root != entry.data_root_page {
            self.update_catalog_data_root(table, &entry, new_data_root)?;
        }

        Ok(())
    }

    /// Get an item by key.
    pub fn get_item(
        &self,
        table: &str,
        partition_key: &Value,
        sort_key: Option<&Value>,
    ) -> Result<Option<Value>, Error> {
        let entry = catalog::ops::get_table(&self.store, self.catalog_root, table)?;
        let schema = &entry.schema;

        let pk = key_utils::json_to_key_value(
            partition_key,
            schema.partition_key.key_type,
            &schema.partition_key.name,
        )?;

        let sk = match (&schema.sort_key, sort_key) {
            (Some(sk_def), Some(sk_val)) => Some(key_utils::json_to_key_value(
                sk_val,
                sk_def.key_type,
                &sk_def.name,
            )?),
            (Some(_), None) => None,
            (None, Some(_)) => return Err(QueryError::SortKeyNotSupported.into()),
            (None, None) => None,
        };

        let composite_key = composite::encode_composite(&pk, sk.as_ref())?;
        let data = mvcc_ops::mvcc_get(
            &self.store,
            entry.data_root_page,
            &composite_key,
            self.txn_id,
        )?;

        match data {
            Some(bytes) => {
                let val: Value = serde_json::from_slice(&bytes).map_err(|e| {
                    StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
                })?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// Delete an item by key.
    pub fn delete_item(
        &mut self,
        table: &str,
        partition_key: &Value,
        sort_key: Option<&Value>,
    ) -> Result<(), Error> {
        let entry = catalog::ops::get_table(&self.store, self.catalog_root, table)?;
        let schema = &entry.schema;

        let pk = key_utils::json_to_key_value(
            partition_key,
            schema.partition_key.key_type,
            &schema.partition_key.name,
        )?;

        let sk = match (&schema.sort_key, sort_key) {
            (Some(sk_def), Some(sk_val)) => Some(key_utils::json_to_key_value(
                sk_val,
                sk_def.key_type,
                &sk_def.name,
            )?),
            (Some(_), None) => None,
            (None, Some(_)) => return Err(QueryError::SortKeyNotSupported.into()),
            (None, None) => None,
        };

        let composite_key = composite::encode_composite(&pk, sk.as_ref())?;
        let new_data_root = mvcc_ops::mvcc_delete(
            &mut self.store,
            entry.data_root_page,
            &composite_key,
            self.txn_id,
        )?;

        if new_data_root != entry.data_root_page {
            self.update_catalog_data_root(table, &entry, new_data_root)?;
        }

        Ok(())
    }

    /// Update the catalog entry's data_root_page after a B+Tree mutation.
    fn update_catalog_data_root(
        &mut self,
        table_name: &str,
        old_entry: &catalog::CatalogEntry,
        new_data_root: PageId,
    ) -> Result<(), Error> {
        let updated_entry = catalog::CatalogEntry {
            schema: old_entry.schema.clone(),
            data_root_page: new_data_root,
        };

        let json_bytes = serde_json::to_vec(&updated_entry).map_err(|e| {
            StorageError::CorruptedPage(format!("failed to serialize catalog entry: {e}"))
        })?;

        let encoded_name = crate::encoding::string::encode_string(table_name);
        let new_catalog_root = crate::btree::ops::insert(
            &mut self.store,
            self.catalog_root,
            &encoded_name,
            &json_bytes,
        )?;
        self.catalog_root = new_catalog_root;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::InMemoryPageStore;
    use crate::btree::ops as btree_ops;
    use crate::types::{KeyDefinition, KeyType, TableSchema};
    use serde_json::json;

    /// Build a Transaction backed by an InMemoryPageStore (for unit tests
    /// without file I/O).
    ///
    /// We cannot use BufferedPageStore (requires a File), so we test the
    /// Transaction methods indirectly through the full database integration
    /// tests. Here we test the helper logic via the MVCC + catalog layer
    /// directly.
    fn setup_catalog_with_table(
        store: &mut InMemoryPageStore,
        name: &str,
        sort_key: bool,
    ) -> (PageId, catalog::CatalogEntry) {
        let catalog_root = btree_ops::create_tree(store).unwrap();
        let schema = TableSchema {
            name: name.to_string(),
            partition_key: KeyDefinition {
                name: "pk".to_string(),
                key_type: KeyType::String,
            },
            sort_key: if sort_key {
                Some(KeyDefinition {
                    name: "sk".to_string(),
                    key_type: KeyType::Number,
                })
            } else {
                None
            },
        };
        let (new_root, entry) = catalog::ops::create_table(store, catalog_root, schema).unwrap();
        (new_root, entry)
    }

    #[test]
    fn test_put_and_get_via_mvcc_layer() {
        let mut store = InMemoryPageStore::new();
        let (catalog_root, entry) = setup_catalog_with_table(&mut store, "users", false);

        // Simulate what Transaction::put_item does.
        let doc = json!({"pk": "alice", "name": "Alice"});
        let doc_bytes = serde_json::to_vec(&doc).unwrap();
        let pk = key_utils::extract_key_from_doc(&doc, &entry.schema.partition_key).unwrap();
        let composite_key = composite::encode_composite(&pk, None).unwrap();

        let new_data_root = mvcc_ops::mvcc_put(
            &mut store,
            entry.data_root_page,
            &composite_key,
            &doc_bytes,
            1,
        )
        .unwrap();

        // Read back.
        let data = mvcc_ops::mvcc_get(&store, new_data_root, &composite_key, 1).unwrap();
        assert!(data.is_some());
        let retrieved: Value = serde_json::from_slice(&data.unwrap()).unwrap();
        assert_eq!(retrieved["name"], "Alice");

        // Verify catalog lookup still works.
        let _ = catalog::ops::get_table(&store, catalog_root, "users").unwrap();
    }

    #[test]
    fn test_delete_via_mvcc_layer() {
        let mut store = InMemoryPageStore::new();
        let (_catalog_root, entry) = setup_catalog_with_table(&mut store, "items", false);

        let doc = json!({"pk": "item1", "val": 42});
        let doc_bytes = serde_json::to_vec(&doc).unwrap();
        let pk = key_utils::extract_key_from_doc(&doc, &entry.schema.partition_key).unwrap();
        let composite_key = composite::encode_composite(&pk, None).unwrap();

        let data_root = mvcc_ops::mvcc_put(
            &mut store,
            entry.data_root_page,
            &composite_key,
            &doc_bytes,
            1,
        )
        .unwrap();

        // Delete.
        let data_root = mvcc_ops::mvcc_delete(&mut store, data_root, &composite_key, 2).unwrap();

        // Snapshot at txn 2: deleted.
        let data = mvcc_ops::mvcc_get(&store, data_root, &composite_key, 2).unwrap();
        assert!(data.is_none());
    }

    #[test]
    fn test_put_with_sort_key() {
        let mut store = InMemoryPageStore::new();
        let (_catalog_root, entry) = setup_catalog_with_table(&mut store, "events", true);

        let doc = json!({"pk": "user1", "sk": 100.0, "data": "event1"});
        let doc_bytes = serde_json::to_vec(&doc).unwrap();
        let pk = key_utils::extract_key_from_doc(&doc, &entry.schema.partition_key).unwrap();
        let sk =
            key_utils::extract_key_from_doc(&doc, entry.schema.sort_key.as_ref().unwrap()).unwrap();
        let composite_key = composite::encode_composite(&pk, Some(&sk)).unwrap();

        let data_root = mvcc_ops::mvcc_put(
            &mut store,
            entry.data_root_page,
            &composite_key,
            &doc_bytes,
            1,
        )
        .unwrap();

        let data = mvcc_ops::mvcc_get(&store, data_root, &composite_key, 1).unwrap();
        assert!(data.is_some());
        let retrieved: Value = serde_json::from_slice(&data.unwrap()).unwrap();
        assert_eq!(retrieved["data"], "event1");
    }
}
