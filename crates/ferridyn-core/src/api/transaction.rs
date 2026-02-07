use serde_json::Value;

use crate::catalog;
use crate::encoding::composite;
use crate::error::{Error, QueryError, SchemaError, StorageError, TxnError};
use crate::mvcc::ops as mvcc_ops;
use crate::storage::tombstone::TombstoneEntry;
use crate::types::{PageId, TxnId, VersionedItem};

use super::filter::FilterExpr;
use super::key_utils;
use super::page_store::BufferedPageStore;
use super::update::{self, UpdateAction};

/// Validate a document against its matching partition schema (if any).
///
/// Only runs if the matching schema has `validate: true`.
fn validate_against_partition_schema(
    doc: &Value,
    schema: &crate::types::PartitionSchema,
) -> Result<(), Error> {
    if !schema.validate {
        return Ok(());
    }
    let mut errors = Vec::new();
    for attr in &schema.attributes {
        match doc.get(&attr.name) {
            None if attr.required => {
                errors.push(format!("missing required attribute: {}", attr.name));
            }
            Some(val) => {
                let type_ok = match attr.attr_type {
                    crate::types::AttrType::String => val.is_string(),
                    crate::types::AttrType::Number => val.is_number(),
                    crate::types::AttrType::Boolean => val.is_boolean(),
                };
                if !type_ok {
                    errors.push(format!(
                        "attribute '{}' expected {:?}, got {:?}",
                        attr.name, attr.attr_type, val
                    ));
                }
            }
            _ => {} // optional and missing = ok
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(SchemaError::ValidationFailed {
            prefix: schema.prefix.clone(),
            errors,
        }
        .into())
    }
}

/// Maintain secondary indexes after a primary write.
///
/// For puts: removes old index entries (if old_doc exists) and adds new entries.
/// For deletes: removes old index entries only.
/// Returns the updated CatalogEntry with new index root pages.
fn maintain_indexes(
    store: &mut impl crate::btree::PageStore,
    entry: &catalog::CatalogEntry,
    pk: &crate::encoding::KeyValue,
    old_doc: Option<&Value>,
    new_doc: Option<&Value>,
) -> Result<catalog::CatalogEntry, Error> {
    // If no indexes exist, return entry unchanged.
    if entry.indexes.is_empty() {
        return Ok(entry.clone());
    }

    // Extract prefix - if None (non-String PK), no indexes can apply.
    let prefix = match key_utils::extract_pk_prefix(pk) {
        Some(p) => p,
        None => return Ok(entry.clone()),
    };

    // Find indexes that match this prefix.
    let relevant: Vec<usize> = entry
        .indexes
        .iter()
        .enumerate()
        .filter(|(_, idx)| idx.partition_schema == prefix)
        .map(|(i, _)| i)
        .collect();

    if relevant.is_empty() {
        return Ok(entry.clone());
    }

    let mut updated = entry.clone();

    for &idx_pos in &relevant {
        let index = &entry.indexes[idx_pos];
        let mut current_root = updated.indexes[idx_pos].root_page;

        // Remove old index entry if old_doc exists and has an indexable key.
        if let Some(old) = old_doc
            && let Some(old_key) = key_utils::build_index_key(index, old, &entry.schema)?
        {
            // Only delete if the key actually exists in the index.
            if crate::btree::ops::search(store, current_root, &old_key)?.is_some() {
                current_root = crate::btree::ops::delete(store, current_root, &old_key)?;
            }
        }

        // Add new index entry if new_doc exists and has an indexable key.
        if let Some(new) = new_doc
            && let Some(new_key) = key_utils::build_index_key(index, new, &entry.schema)?
        {
            current_root = crate::btree::ops::insert(store, current_root, &new_key, &[])?;
        }

        updated.indexes[idx_pos].root_page = current_root;
    }

    Ok(updated)
}

/// A write transaction that buffers all changes until commit.
///
/// All mutations go through the `BufferedPageStore`; on commit the overlay
/// is flushed to disk. If the closure returns an error (or panics),
/// the buffered writes are simply dropped (auto-abort).
pub struct Transaction {
    pub(crate) store: BufferedPageStore,
    pub(crate) catalog_root: PageId,
    pub(crate) txn_id: TxnId,
    /// Keys deleted during this transaction (for incremental GC).
    pub(crate) tombstones: Vec<TombstoneEntry>,
}

/// Evaluate a condition expression against an existing document.
///
/// If the item does not exist, the condition is evaluated against an empty
/// object `{}`, matching DynamoDB behavior where `attribute_not_exists`
/// passes for non-existent items.
fn evaluate_condition(condition: &FilterExpr, existing_doc: Option<&Value>) -> Result<(), Error> {
    let empty = Value::Object(serde_json::Map::new());
    let doc = existing_doc.unwrap_or(&empty);
    let passed = condition.eval(doc)?;
    if passed {
        Ok(())
    } else {
        Err(TxnError::ConditionCheckFailed("the conditional request failed".to_string()).into())
    }
}

impl Transaction {
    /// Insert or replace an item in a table.
    ///
    /// If `condition` is provided, the existing item (or empty object for
    /// non-existent items) must satisfy the condition or the operation fails
    /// with `ConditionCheckFailed`.
    pub fn put_item(
        &mut self,
        table: &str,
        document: Value,
        condition: Option<&FilterExpr>,
    ) -> Result<(), Error> {
        // 1. Validate document size.
        let doc_bytes = key_utils::validate_document_size(&document)?;

        // 2. Look up table schema.
        let entry = catalog::ops::get_table(&self.store, self.catalog_root, table)?;
        let schema = &entry.schema;

        // 3. Extract and validate partition key.
        let pk = key_utils::extract_key_from_doc(&document, &schema.partition_key)?;
        key_utils::validate_partition_key_size(&pk)?;

        // 3a. Validate against partition schema if one matches.
        if let Some(prefix) = key_utils::extract_pk_prefix(&pk)
            && let Some(ps) = entry
                .partition_schemas
                .iter()
                .find(|ps| ps.prefix == prefix)
        {
            validate_against_partition_schema(&document, ps)?;
        }

        // 4. Extract and validate sort key if present.
        let sk = if let Some(ref sk_def) = schema.sort_key {
            let sk_val = key_utils::extract_key_from_doc(&document, sk_def)?;
            key_utils::validate_sort_key_size(&sk_val)?;
            Some(sk_val)
        } else {
            None
        };

        // 5. Encode composite key.
        let composite_key = composite::encode_composite(&pk, sk.as_ref())?;

        // 5a. Read old document for condition evaluation and index maintenance.
        let old_doc: Option<Value> = if condition.is_some() || !entry.indexes.is_empty() {
            mvcc_ops::mvcc_get(
                &self.store,
                entry.data_root_page,
                &composite_key,
                self.txn_id,
            )?
            .map(|bytes| rmp_serde::from_slice(&bytes))
            .transpose()
            .map_err(|e| {
                StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
            })?
        } else {
            None
        };

        // 5b. Evaluate condition expression against existing document.
        if let Some(cond) = condition {
            evaluate_condition(cond, old_doc.as_ref())?;
        }

        // 6. Put with MVCC versioning.
        let new_data_root = mvcc_ops::mvcc_put(
            &mut self.store,
            entry.data_root_page,
            &composite_key,
            &doc_bytes,
            self.txn_id,
        )?;

        // 7. Update entry and maintain indexes.
        let mut updated = entry.clone();
        updated.data_root_page = new_data_root;
        updated = maintain_indexes(
            &mut self.store,
            &updated,
            &pk,
            old_doc.as_ref(),
            Some(&document),
        )?;

        // 8. Write catalog if anything changed.
        if updated.data_root_page != entry.data_root_page || updated.indexes != entry.indexes {
            self.update_catalog_entry(table, &updated)?;
        }

        Ok(())
    }

    /// Insert or replace an item with optimistic concurrency control.
    ///
    /// If `expected_version` does not match the current version of the item,
    /// the write is rejected with `TxnError::VersionMismatch`. This prevents
    /// lost updates when multiple writers target the same key.
    pub fn put_item_conditional(
        &mut self,
        table: &str,
        document: Value,
        expected_version: u64,
    ) -> Result<(), Error> {
        let doc_bytes = key_utils::validate_document_size(&document)?;

        let entry = catalog::ops::get_table(&self.store, self.catalog_root, table)?;
        let schema = &entry.schema;

        let pk = key_utils::extract_key_from_doc(&document, &schema.partition_key)?;
        key_utils::validate_partition_key_size(&pk)?;

        // Validate against partition schema if one matches.
        if let Some(prefix) = key_utils::extract_pk_prefix(&pk)
            && let Some(ps) = entry
                .partition_schemas
                .iter()
                .find(|ps| ps.prefix == prefix)
        {
            validate_against_partition_schema(&document, ps)?;
        }

        let sk = if let Some(ref sk_def) = schema.sort_key {
            let sk_val = key_utils::extract_key_from_doc(&document, sk_def)?;
            key_utils::validate_sort_key_size(&sk_val)?;
            Some(sk_val)
        } else {
            None
        };

        let composite_key = composite::encode_composite(&pk, sk.as_ref())?;

        // Read old document for index maintenance.
        let old_doc: Option<Value> = if !entry.indexes.is_empty() {
            mvcc_ops::mvcc_get(
                &self.store,
                entry.data_root_page,
                &composite_key,
                self.txn_id,
            )?
            .map(|bytes| rmp_serde::from_slice(&bytes))
            .transpose()
            .map_err(|e| {
                StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
            })?
        } else {
            None
        };

        let new_data_root = mvcc_ops::mvcc_put_conditional(
            &mut self.store,
            entry.data_root_page,
            &composite_key,
            &doc_bytes,
            self.txn_id,
            expected_version,
        )?;

        let mut updated = entry.clone();
        updated.data_root_page = new_data_root;
        updated = maintain_indexes(
            &mut self.store,
            &updated,
            &pk,
            old_doc.as_ref(),
            Some(&document),
        )?;

        if updated.data_root_page != entry.data_root_page || updated.indexes != entry.indexes {
            self.update_catalog_entry(table, &updated)?;
        }

        Ok(())
    }

    /// Get an item by key, returning the version number.
    ///
    /// The returned `VersionedItem` includes the document and its version
    /// (the `created_txn` of the MVCC version). Pass this version to
    /// `put_item_conditional` for optimistic concurrency control.
    pub fn get_item_versioned(
        &self,
        table: &str,
        partition_key: &Value,
        sort_key: Option<&Value>,
    ) -> Result<Option<VersionedItem>, Error> {
        let entry = catalog::ops::get_table(&self.store, self.catalog_root, table)?;
        let schema = &entry.schema;

        let pk = key_utils::json_to_key_value(
            partition_key,
            schema.partition_key.key_type,
            &schema.partition_key.name,
        )?;
        key_utils::validate_partition_key_size(&pk)?;

        let sk = match (&schema.sort_key, sort_key) {
            (Some(sk_def), Some(sk_val)) => {
                let sk = key_utils::json_to_key_value(sk_val, sk_def.key_type, &sk_def.name)?;
                key_utils::validate_sort_key_size(&sk)?;
                Some(sk)
            }
            (Some(_), None) => None,
            (None, Some(_)) => return Err(QueryError::SortKeyNotSupported.into()),
            (None, None) => None,
        };

        let composite_key = composite::encode_composite(&pk, sk.as_ref())?;
        let data = mvcc_ops::mvcc_get_versioned(
            &self.store,
            entry.data_root_page,
            &composite_key,
            self.txn_id,
        )?;

        match data {
            Some((bytes, version)) => {
                let val: Value = rmp_serde::from_slice(&bytes).map_err(|e| {
                    StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
                })?;
                Ok(Some(VersionedItem { item: val, version }))
            }
            None => Ok(None),
        }
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
        key_utils::validate_partition_key_size(&pk)?;

        let sk = match (&schema.sort_key, sort_key) {
            (Some(sk_def), Some(sk_val)) => {
                let sk = key_utils::json_to_key_value(sk_val, sk_def.key_type, &sk_def.name)?;
                key_utils::validate_sort_key_size(&sk)?;
                Some(sk)
            }
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
                let val: Value = rmp_serde::from_slice(&bytes).map_err(|e| {
                    StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
                })?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// Delete an item by key.
    ///
    /// If `condition` is provided, the existing item must satisfy the condition
    /// or the operation fails with `ConditionCheckFailed`.
    pub fn delete_item(
        &mut self,
        table: &str,
        partition_key: &Value,
        sort_key: Option<&Value>,
        condition: Option<&FilterExpr>,
    ) -> Result<(), Error> {
        let entry = catalog::ops::get_table(&self.store, self.catalog_root, table)?;
        let schema = &entry.schema;

        let pk = key_utils::json_to_key_value(
            partition_key,
            schema.partition_key.key_type,
            &schema.partition_key.name,
        )?;
        key_utils::validate_partition_key_size(&pk)?;

        let sk = match (&schema.sort_key, sort_key) {
            (Some(sk_def), Some(sk_val)) => {
                let sk = key_utils::json_to_key_value(sk_val, sk_def.key_type, &sk_def.name)?;
                key_utils::validate_sort_key_size(&sk)?;
                Some(sk)
            }
            (Some(_), None) => None,
            (None, Some(_)) => return Err(QueryError::SortKeyNotSupported.into()),
            (None, None) => None,
        };

        let composite_key = composite::encode_composite(&pk, sk.as_ref())?;

        // Read document before delete for condition evaluation and index maintenance.
        let old_doc: Option<Value> = if condition.is_some() || !entry.indexes.is_empty() {
            mvcc_ops::mvcc_get(
                &self.store,
                entry.data_root_page,
                &composite_key,
                self.txn_id,
            )?
            .map(|bytes| rmp_serde::from_slice(&bytes))
            .transpose()
            .map_err(|e| {
                StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
            })?
        } else {
            None
        };

        // Evaluate condition expression against existing document.
        if let Some(cond) = condition {
            evaluate_condition(cond, old_doc.as_ref())?;
        }

        let new_data_root = mvcc_ops::mvcc_delete(
            &mut self.store,
            entry.data_root_page,
            &composite_key,
            self.txn_id,
        )?;

        // Record tombstone for incremental GC.
        self.tombstones.push(TombstoneEntry {
            table: table.to_string(),
            key: composite_key.clone(),
        });

        let mut updated = entry.clone();
        updated.data_root_page = new_data_root;
        updated = maintain_indexes(
            &mut self.store,
            &updated,
            &pk,
            old_doc.as_ref(),
            None, // delete: no new document
        )?;

        if updated.data_root_page != entry.data_root_page || updated.indexes != entry.indexes {
            self.update_catalog_entry(table, &updated)?;
        }

        Ok(())
    }

    /// Partially update an item by applying a list of update actions.
    ///
    /// If the item doesn't exist, an upsert creates it with the key
    /// attributes plus the SET values.
    ///
    /// If `condition` is provided, the existing item (or empty object for
    /// non-existent items) must satisfy the condition or the operation fails
    /// with `ConditionCheckFailed`.
    pub fn update_item(
        &mut self,
        table: &str,
        partition_key: &Value,
        sort_key: Option<&Value>,
        actions: &[UpdateAction],
        condition: Option<&FilterExpr>,
    ) -> Result<(), Error> {
        // 1. Look up table schema.
        let entry = catalog::ops::get_table(&self.store, self.catalog_root, table)?;
        let schema = &entry.schema;

        // 2. Convert pk/sk to KeyValue, validate sizes, encode composite key.
        let pk = key_utils::json_to_key_value(
            partition_key,
            schema.partition_key.key_type,
            &schema.partition_key.name,
        )?;
        key_utils::validate_partition_key_size(&pk)?;

        let sk = match (&schema.sort_key, sort_key) {
            (Some(sk_def), Some(sk_val)) => {
                let sk = key_utils::json_to_key_value(sk_val, sk_def.key_type, &sk_def.name)?;
                key_utils::validate_sort_key_size(&sk)?;
                Some(sk)
            }
            (Some(_), None) => None,
            (None, Some(_)) => return Err(QueryError::SortKeyNotSupported.into()),
            (None, None) => None,
        };

        let composite_key = composite::encode_composite(&pk, sk.as_ref())?;

        // 3. Validate no key attribute updates.
        let sk_name = schema.sort_key.as_ref().map(|sk| sk.name.as_str());
        update::validate_no_key_updates(actions, &schema.partition_key.name, sk_name)?;

        // 4. Read existing document.
        let existing_bytes = mvcc_ops::mvcc_get(
            &self.store,
            entry.data_root_page,
            &composite_key,
            self.txn_id,
        )?;

        let (old_doc, mut document) = match existing_bytes {
            Some(bytes) => {
                let val: Value = rmp_serde::from_slice(&bytes).map_err(|e| {
                    StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
                })?;
                (Some(val.clone()), val)
            }
            None => {
                // 5. Upsert: create base document with key attributes.
                let mut base = serde_json::Map::new();
                base.insert(schema.partition_key.name.clone(), partition_key.clone());
                if let (Some(sk_def), Some(sk_val)) = (&schema.sort_key, sort_key) {
                    base.insert(sk_def.name.clone(), sk_val.clone());
                }
                (None, Value::Object(base))
            }
        };

        // 5a. Evaluate condition expression against existing document.
        if let Some(cond) = condition {
            evaluate_condition(cond, old_doc.as_ref())?;
        }

        // 6. Apply update actions.
        update::apply_updates(&mut document, actions)?;

        // 7. Validate against partition schema (on final document).
        if let Some(prefix) = key_utils::extract_pk_prefix(&pk)
            && let Some(ps) = entry
                .partition_schemas
                .iter()
                .find(|ps| ps.prefix == prefix)
        {
            validate_against_partition_schema(&document, ps)?;
        }

        // 8. Validate document size.
        let doc_bytes = key_utils::validate_document_size(&document)?;

        // 9. Write with MVCC versioning.
        let new_data_root = mvcc_ops::mvcc_put(
            &mut self.store,
            entry.data_root_page,
            &composite_key,
            &doc_bytes,
            self.txn_id,
        )?;

        // 10. Maintain indexes.
        let mut updated = entry.clone();
        updated.data_root_page = new_data_root;
        updated = maintain_indexes(
            &mut self.store,
            &updated,
            &pk,
            old_doc.as_ref(),
            Some(&document),
        )?;

        // 11. Write catalog if anything changed.
        if updated.data_root_page != entry.data_root_page || updated.indexes != entry.indexes {
            self.update_catalog_entry(table, &updated)?;
        }

        Ok(())
    }

    /// Update a catalog entry after a B+Tree mutation.
    ///
    /// Serializes the full entry (including partition_schemas and indexes)
    /// and re-inserts it into the catalog B+Tree.
    fn update_catalog_entry(
        &mut self,
        table_name: &str,
        updated_entry: &catalog::CatalogEntry,
    ) -> Result<(), Error> {
        let json_bytes = serde_json::to_vec(updated_entry).map_err(|e| {
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
    use crate::encoding::KeyValue;
    use crate::types::{KeyDefinition, KeyType, PartitionSchema, TableSchema};
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
            ttl_attribute: None,
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
        let doc_bytes = rmp_serde::to_vec(&doc).unwrap();
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
        let retrieved: Value = rmp_serde::from_slice(&data.unwrap()).unwrap();
        assert_eq!(retrieved["name"], "Alice");

        // Verify catalog lookup still works.
        let _ = catalog::ops::get_table(&store, catalog_root, "users").unwrap();
    }

    #[test]
    fn test_delete_via_mvcc_layer() {
        let mut store = InMemoryPageStore::new();
        let (_catalog_root, entry) = setup_catalog_with_table(&mut store, "items", false);

        let doc = json!({"pk": "item1", "val": 42});
        let doc_bytes = rmp_serde::to_vec(&doc).unwrap();
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
        let doc_bytes = rmp_serde::to_vec(&doc).unwrap();
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
        let retrieved: Value = rmp_serde::from_slice(&data.unwrap()).unwrap();
        assert_eq!(retrieved["data"], "event1");
    }

    #[test]
    fn test_validate_conforming_document() {
        let schema = crate::types::PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![
                crate::types::AttributeDef {
                    name: "email".to_string(),
                    attr_type: crate::types::AttrType::String,
                    required: true,
                },
                crate::types::AttributeDef {
                    name: "age".to_string(),
                    attr_type: crate::types::AttrType::Number,
                    required: false,
                },
            ],
            validate: true,
        };
        let doc = json!({"pk": "CONTACT#alice", "email": "alice@ex.com", "age": 30});
        assert!(validate_against_partition_schema(&doc, &schema).is_ok());
    }

    #[test]
    fn test_validate_missing_required_attribute() {
        let schema = crate::types::PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![crate::types::AttributeDef {
                name: "email".to_string(),
                attr_type: crate::types::AttrType::String,
                required: true,
            }],
            validate: true,
        };
        let doc = json!({"pk": "CONTACT#alice", "name": "Alice"});
        let result = validate_against_partition_schema(&doc, &schema);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("missing required"));
    }

    #[test]
    fn test_validate_wrong_type() {
        let schema = crate::types::PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![crate::types::AttributeDef {
                name: "email".to_string(),
                attr_type: crate::types::AttrType::String,
                required: true,
            }],
            validate: true,
        };
        // email is a number instead of string
        let doc = json!({"pk": "CONTACT#alice", "email": 42});
        let result = validate_against_partition_schema(&doc, &schema);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("expected"));
    }

    #[test]
    fn test_validate_skipped_when_disabled() {
        let schema = crate::types::PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![crate::types::AttributeDef {
                name: "email".to_string(),
                attr_type: crate::types::AttrType::String,
                required: true,
            }],
            validate: false, // disabled!
        };
        // Missing email but validate is false — should pass
        let doc = json!({"pk": "CONTACT#alice"});
        assert!(validate_against_partition_schema(&doc, &schema).is_ok());
    }

    #[test]
    fn test_validate_optional_missing_ok() {
        let schema = crate::types::PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![crate::types::AttributeDef {
                name: "nickname".to_string(),
                attr_type: crate::types::AttrType::String,
                required: false,
            }],
            validate: true,
        };
        // Missing optional attribute — should pass
        let doc = json!({"pk": "CONTACT#alice"});
        assert!(validate_against_partition_schema(&doc, &schema).is_ok());
    }

    #[test]
    fn test_validate_multiple_errors() {
        let schema = crate::types::PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![
                crate::types::AttributeDef {
                    name: "email".to_string(),
                    attr_type: crate::types::AttrType::String,
                    required: true,
                },
                crate::types::AttributeDef {
                    name: "active".to_string(),
                    attr_type: crate::types::AttrType::Boolean,
                    required: true,
                },
            ],
            validate: true,
        };
        // Missing both required attributes
        let doc = json!({"pk": "CONTACT#alice"});
        let result = validate_against_partition_schema(&doc, &schema);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        // Both errors should be in the message (joined with "; ")
        assert!(err_msg.contains("email"), "should mention email: {err_msg}");
        assert!(
            err_msg.contains("active"),
            "should mention active: {err_msg}"
        );
    }

    #[test]
    fn test_maintain_indexes_put_creates_entry() {
        let mut store = InMemoryPageStore::new();
        let (mut catalog_root, _entry) = setup_catalog_with_table(&mut store, "data", false);

        // Add partition schema and index.
        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        catalog_root =
            catalog::ops::create_partition_schema(&mut store, catalog_root, "data", ps).unwrap();
        catalog_root = catalog::ops::create_index(
            &mut store,
            catalog_root,
            "data",
            "email-idx".to_string(),
            "CONTACT".to_string(),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            1,
        )
        .unwrap();

        let entry = catalog::ops::get_table(&store, catalog_root, "data").unwrap();
        let pk = KeyValue::String("CONTACT#alice".to_string());
        let doc = json!({"pk": "CONTACT#alice", "email": "alice@ex.com"});

        let updated = maintain_indexes(&mut store, &entry, &pk, None, Some(&doc)).unwrap();

        // Verify index entry was created.
        let index_entries =
            btree_ops::range_scan(&store, updated.indexes[0].root_page, None, None).unwrap();
        assert_eq!(index_entries.len(), 1);
    }

    #[test]
    fn test_maintain_indexes_delete_removes_entry() {
        let mut store = InMemoryPageStore::new();
        let (mut catalog_root, _entry) = setup_catalog_with_table(&mut store, "data", false);

        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        catalog_root =
            catalog::ops::create_partition_schema(&mut store, catalog_root, "data", ps).unwrap();
        catalog_root = catalog::ops::create_index(
            &mut store,
            catalog_root,
            "data",
            "email-idx".to_string(),
            "CONTACT".to_string(),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            1,
        )
        .unwrap();

        let entry = catalog::ops::get_table(&store, catalog_root, "data").unwrap();
        let pk = KeyValue::String("CONTACT#alice".to_string());
        let doc = json!({"pk": "CONTACT#alice", "email": "alice@ex.com"});

        // First: add index entry.
        let updated = maintain_indexes(&mut store, &entry, &pk, None, Some(&doc)).unwrap();

        // Now: delete (old_doc = doc, new_doc = None).
        let updated = maintain_indexes(&mut store, &updated, &pk, Some(&doc), None).unwrap();

        let index_entries =
            btree_ops::range_scan(&store, updated.indexes[0].root_page, None, None).unwrap();
        assert_eq!(index_entries.len(), 0);
    }

    #[test]
    fn test_maintain_indexes_update_changes_entry() {
        let mut store = InMemoryPageStore::new();
        let (mut catalog_root, _entry) = setup_catalog_with_table(&mut store, "data", false);

        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        catalog_root =
            catalog::ops::create_partition_schema(&mut store, catalog_root, "data", ps).unwrap();
        catalog_root = catalog::ops::create_index(
            &mut store,
            catalog_root,
            "data",
            "email-idx".to_string(),
            "CONTACT".to_string(),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            1,
        )
        .unwrap();

        let entry = catalog::ops::get_table(&store, catalog_root, "data").unwrap();
        let pk = KeyValue::String("CONTACT#alice".to_string());
        let old_doc = json!({"pk": "CONTACT#alice", "email": "old@ex.com"});
        let new_doc = json!({"pk": "CONTACT#alice", "email": "new@ex.com"});

        // Insert initial.
        let updated = maintain_indexes(&mut store, &entry, &pk, None, Some(&old_doc)).unwrap();

        // Update email.
        let updated =
            maintain_indexes(&mut store, &updated, &pk, Some(&old_doc), Some(&new_doc)).unwrap();

        // Should still have exactly one index entry (old removed, new added).
        let index_entries =
            btree_ops::range_scan(&store, updated.indexes[0].root_page, None, None).unwrap();
        assert_eq!(index_entries.len(), 1);
    }

    #[test]
    fn test_maintain_indexes_no_indexes_noop() {
        let mut store = InMemoryPageStore::new();
        let (catalog_root, _entry) = setup_catalog_with_table(&mut store, "data", false);

        let entry = catalog::ops::get_table(&store, catalog_root, "data").unwrap();
        let pk = KeyValue::String("CONTACT#alice".to_string());
        let doc = json!({"pk": "CONTACT#alice", "email": "alice@ex.com"});

        // No indexes — should return entry unchanged.
        let updated = maintain_indexes(&mut store, &entry, &pk, None, Some(&doc)).unwrap();
        assert_eq!(updated.indexes.len(), 0);
    }

    #[test]
    fn test_maintain_indexes_different_prefix_ignored() {
        let mut store = InMemoryPageStore::new();
        let (mut catalog_root, _entry) = setup_catalog_with_table(&mut store, "data", false);

        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        catalog_root =
            catalog::ops::create_partition_schema(&mut store, catalog_root, "data", ps).unwrap();
        catalog_root = catalog::ops::create_index(
            &mut store,
            catalog_root,
            "data",
            "email-idx".to_string(),
            "CONTACT".to_string(),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            1,
        )
        .unwrap();

        let entry = catalog::ops::get_table(&store, catalog_root, "data").unwrap();
        // Different prefix — should NOT create index entry.
        let pk = KeyValue::String("ORDER#123".to_string());
        let doc = json!({"pk": "ORDER#123", "email": "order@ex.com"});

        let updated = maintain_indexes(&mut store, &entry, &pk, None, Some(&doc)).unwrap();

        let index_entries =
            btree_ops::range_scan(&store, updated.indexes[0].root_page, None, None).unwrap();
        assert_eq!(index_entries.len(), 0);
    }

    #[test]
    fn test_maintain_indexes_missing_attribute_skipped() {
        let mut store = InMemoryPageStore::new();
        let (mut catalog_root, _entry) = setup_catalog_with_table(&mut store, "data", false);

        let ps = PartitionSchema {
            prefix: "CONTACT".to_string(),
            description: "People".to_string(),
            attributes: vec![],
            validate: false,
        };
        catalog_root =
            catalog::ops::create_partition_schema(&mut store, catalog_root, "data", ps).unwrap();
        catalog_root = catalog::ops::create_index(
            &mut store,
            catalog_root,
            "data",
            "email-idx".to_string(),
            "CONTACT".to_string(),
            KeyDefinition {
                name: "email".to_string(),
                key_type: KeyType::String,
            },
            1,
        )
        .unwrap();

        let entry = catalog::ops::get_table(&store, catalog_root, "data").unwrap();
        let pk = KeyValue::String("CONTACT#alice".to_string());
        // No email attribute.
        let doc = json!({"pk": "CONTACT#alice", "name": "Alice"});

        let updated = maintain_indexes(&mut store, &entry, &pk, None, Some(&doc)).unwrap();

        let index_entries =
            btree_ops::range_scan(&store, updated.indexes[0].root_page, None, None).unwrap();
        assert_eq!(index_entries.len(), 0);
    }
}
