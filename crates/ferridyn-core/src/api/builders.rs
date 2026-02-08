use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::btree::ops as btree_ops;
use crate::catalog;
use crate::encoding::composite;
use crate::error::{Error, QueryError, SchemaError, StorageError};
use crate::mvcc::ops as mvcc_ops;
use crate::types::{AttrType, AttributeDef, KeyDefinition, KeyType, TableSchema, VersionedItem};

use super::database::FerridynDB;
use super::filter::FilterExpr;
use super::key_utils;
use super::query::{QueryResult, SortCondition, compute_scan_bounds};
use super::update::UpdateAction;

// ---------------------------------------------------------------------------
// TableBuilder
// ---------------------------------------------------------------------------

/// Builder for creating a new table.
pub struct TableBuilder<'a> {
    db: &'a FerridynDB,
    name: String,
    partition_key: Option<(String, KeyType)>,
    sort_key: Option<(String, KeyType)>,
    ttl_attribute: Option<String>,
}

impl<'a> TableBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, name: String) -> Self {
        Self {
            db,
            name,
            partition_key: None,
            sort_key: None,
            ttl_attribute: None,
        }
    }

    /// Set the partition key attribute and type.
    pub fn partition_key(mut self, name: &str, key_type: KeyType) -> Self {
        self.partition_key = Some((name.to_string(), key_type));
        self
    }

    /// Set the (optional) sort key attribute and type.
    pub fn sort_key(mut self, name: &str, key_type: KeyType) -> Self {
        self.sort_key = Some((name.to_string(), key_type));
        self
    }

    /// Set the TTL attribute name. Items with this numeric attribute set to a
    /// Unix epoch seconds value in the past will be treated as expired.
    pub fn ttl_attribute(mut self, name: &str) -> Self {
        self.ttl_attribute = Some(name.to_string());
        self
    }

    /// Execute the table creation.
    pub fn execute(self) -> Result<(), Error> {
        let (pk_name, pk_type) = self.partition_key.ok_or(QueryError::PartitionKeyRequired)?;

        let schema = TableSchema {
            name: self.name,
            partition_key: KeyDefinition {
                name: pk_name,
                key_type: pk_type,
            },
            sort_key: self
                .sort_key
                .map(|(name, key_type)| KeyDefinition { name, key_type }),
            ttl_attribute: self.ttl_attribute,
        };

        self.db.transact(move |txn| {
            let (new_root, _entry) =
                catalog::ops::create_table(&mut txn.store, txn.catalog_root, schema)?;
            txn.catalog_root = new_root;
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// GetItemBuilder
// ---------------------------------------------------------------------------

/// Builder for getting a single item by key.
pub struct GetItemBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    partition_key: Option<Value>,
    sort_key: Option<Value>,
}

impl<'a> GetItemBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String) -> Self {
        Self {
            db,
            table,
            partition_key: None,
            sort_key: None,
        }
    }

    /// Set the partition key value.
    pub fn partition_key(mut self, value: impl Into<Value>) -> Self {
        self.partition_key = Some(value.into());
        self
    }

    /// Set the sort key value (for tables with a sort key).
    pub fn sort_key(mut self, value: impl Into<Value>) -> Self {
        self.sort_key = Some(value.into());
        self
    }

    /// Execute the get operation.
    pub fn execute(self) -> Result<Option<Value>, Error> {
        let pk_val = self.partition_key.ok_or(QueryError::PartitionKeyRequired)?;

        self.db.read_snapshot(|store, catalog_root, snapshot_txn| {
            let entry = self.db.cached_get_table(store, catalog_root, &self.table)?;
            let schema = &entry.schema;

            let pk = key_utils::json_to_key_value(
                &pk_val,
                schema.partition_key.key_type,
                &schema.partition_key.name,
            )?;
            key_utils::validate_partition_key_size(&pk)?;

            let sk = match (&schema.sort_key, &self.sort_key) {
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
            let data =
                mvcc_ops::mvcc_get(store, entry.data_root_page, &composite_key, snapshot_txn)?;

            match data {
                Some(bytes) => {
                    let val: Value = rmp_serde::from_slice(&bytes).map_err(|e| {
                        StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
                    })?;
                    if is_ttl_expired(&val, schema) {
                        Ok(None)
                    } else {
                        Ok(Some(val))
                    }
                }
                None => Ok(None),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// GetItemVersionedBuilder
// ---------------------------------------------------------------------------

/// Builder for getting a single item by key, including its version number.
///
/// Returns a `VersionedItem` containing the document and the MVCC version
/// (`created_txn`). Pass this version to `put_item_conditional` for
/// optimistic concurrency control.
pub struct GetItemVersionedBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    partition_key: Option<Value>,
    sort_key: Option<Value>,
}

impl<'a> GetItemVersionedBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String) -> Self {
        Self {
            db,
            table,
            partition_key: None,
            sort_key: None,
        }
    }

    /// Set the partition key value.
    pub fn partition_key(mut self, value: impl Into<Value>) -> Self {
        self.partition_key = Some(value.into());
        self
    }

    /// Set the sort key value (for tables with a sort key).
    pub fn sort_key(mut self, value: impl Into<Value>) -> Self {
        self.sort_key = Some(value.into());
        self
    }

    /// Execute the get operation, returning the item and its version.
    pub fn execute(self) -> Result<Option<VersionedItem>, Error> {
        let pk_val = self.partition_key.ok_or(QueryError::PartitionKeyRequired)?;

        self.db.read_snapshot(|store, catalog_root, snapshot_txn| {
            let entry = self.db.cached_get_table(store, catalog_root, &self.table)?;
            let schema = &entry.schema;

            let pk = key_utils::json_to_key_value(
                &pk_val,
                schema.partition_key.key_type,
                &schema.partition_key.name,
            )?;
            key_utils::validate_partition_key_size(&pk)?;

            let sk = match (&schema.sort_key, &self.sort_key) {
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
                store,
                entry.data_root_page,
                &composite_key,
                snapshot_txn,
            )?;

            match data {
                Some((bytes, version)) => {
                    let val: Value = rmp_serde::from_slice(&bytes).map_err(|e| {
                        StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
                    })?;
                    if is_ttl_expired(&val, schema) {
                        Ok(None)
                    } else {
                        Ok(Some(VersionedItem { item: val, version }))
                    }
                }
                None => Ok(None),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// BatchGetItemBuilder
// ---------------------------------------------------------------------------

/// Builder for retrieving multiple items by key in a single call.
///
/// All items are read from a single MVCC snapshot, providing a consistent
/// point-in-time view. Results are positional: `results[i]` corresponds to
/// `keys[i]`. Missing items return `None`.
pub struct BatchGetItemBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    keys: Vec<(Value, Option<Value>)>,
}

impl<'a> BatchGetItemBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String) -> Self {
        Self {
            db,
            table,
            keys: Vec::new(),
        }
    }

    /// Add a key to retrieve. `partition_key` is the partition key value,
    /// `sort_key` is the optional sort key value (for composite key tables).
    pub fn key(mut self, partition_key: impl Into<Value>, sort_key: Option<Value>) -> Self {
        self.keys.push((partition_key.into(), sort_key));
        self
    }

    /// Execute the batch get operation.
    pub fn execute(self) -> Result<Vec<Option<Value>>, Error> {
        if self.keys.is_empty() {
            return Ok(Vec::new());
        }

        self.db.read_snapshot(|store, catalog_root, snapshot_txn| {
            let entry = self.db.cached_get_table(store, catalog_root, &self.table)?;
            let schema = &entry.schema;

            let mut results = Vec::with_capacity(self.keys.len());

            for (pk_val, sk_val) in &self.keys {
                let pk = key_utils::json_to_key_value(
                    pk_val,
                    schema.partition_key.key_type,
                    &schema.partition_key.name,
                )?;
                key_utils::validate_partition_key_size(&pk)?;

                let sk = match (&schema.sort_key, sk_val) {
                    (Some(sk_def), Some(sk_v)) => {
                        let sk = key_utils::json_to_key_value(sk_v, sk_def.key_type, &sk_def.name)?;
                        key_utils::validate_sort_key_size(&sk)?;
                        Some(sk)
                    }
                    (Some(_), None) => None,
                    (None, Some(_)) => return Err(QueryError::SortKeyNotSupported.into()),
                    (None, None) => None,
                };

                let composite_key = composite::encode_composite(&pk, sk.as_ref())?;
                let data =
                    mvcc_ops::mvcc_get(store, entry.data_root_page, &composite_key, snapshot_txn)?;

                match data {
                    Some(bytes) => {
                        let val: Value = rmp_serde::from_slice(&bytes).map_err(|e| {
                            StorageError::CorruptedPage(format!(
                                "failed to deserialize document: {e}"
                            ))
                        })?;
                        if is_ttl_expired(&val, schema) {
                            results.push(None);
                        } else {
                            results.push(Some(val));
                        }
                    }
                    None => results.push(None),
                }
            }

            Ok(results)
        })
    }
}

// ---------------------------------------------------------------------------
// DeleteItemBuilder
// ---------------------------------------------------------------------------

/// Builder for inserting or replacing an item with optional condition.
pub struct PutItemBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    document: Value,
    condition: Option<FilterExpr>,
}

impl<'a> PutItemBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String, document: Value) -> Self {
        Self {
            db,
            table,
            document,
            condition: None,
        }
    }

    /// Set a condition expression that must be satisfied by the existing item
    /// (or empty object for non-existent items) for the put to proceed.
    pub fn condition(mut self, expr: FilterExpr) -> Self {
        self.condition = Some(expr);
        self
    }

    /// Execute the put operation.
    pub fn execute(self) -> Result<(), Error> {
        let table = self.table;
        let document = self.document;
        let condition = self.condition;

        self.db
            .transact(move |txn| txn.put_item(&table, document, condition.as_ref()))
    }
}

/// Builder for deleting an item by key.
pub struct DeleteItemBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    partition_key: Option<Value>,
    sort_key: Option<Value>,
    condition: Option<FilterExpr>,
}

impl<'a> DeleteItemBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String) -> Self {
        Self {
            db,
            table,
            partition_key: None,
            sort_key: None,
            condition: None,
        }
    }

    /// Set the partition key value.
    pub fn partition_key(mut self, value: impl Into<Value>) -> Self {
        self.partition_key = Some(value.into());
        self
    }

    /// Set the sort key value.
    pub fn sort_key(mut self, value: impl Into<Value>) -> Self {
        self.sort_key = Some(value.into());
        self
    }

    /// Set a condition expression that must be satisfied by the existing item
    /// for the delete to proceed.
    pub fn condition(mut self, expr: FilterExpr) -> Self {
        self.condition = Some(expr);
        self
    }

    /// Execute the delete operation.
    pub fn execute(self) -> Result<(), Error> {
        let pk_val = self.partition_key.ok_or(QueryError::PartitionKeyRequired)?;
        let sk_val = self.sort_key;
        let table = self.table;
        let condition = self.condition;

        self.db.transact(move |txn| {
            txn.delete_item(&table, &pk_val, sk_val.as_ref(), condition.as_ref())
        })
    }
}

// ---------------------------------------------------------------------------
// UpdateItemBuilder
// ---------------------------------------------------------------------------

/// Builder for partially updating an item by key.
///
/// Collects SET and REMOVE actions, then applies them atomically inside a
/// write transaction. If the item doesn't exist, an upsert creates it with
/// the key attributes plus the SET values.
pub struct UpdateItemBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    partition_key: Option<Value>,
    sort_key: Option<Value>,
    actions: Vec<UpdateAction>,
    condition: Option<FilterExpr>,
}

impl<'a> UpdateItemBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String) -> Self {
        Self {
            db,
            table,
            partition_key: None,
            sort_key: None,
            actions: Vec::new(),
            condition: None,
        }
    }

    /// Set the partition key value.
    pub fn partition_key(mut self, value: impl Into<Value>) -> Self {
        self.partition_key = Some(value.into());
        self
    }

    /// Set the sort key value (for tables with a sort key).
    pub fn sort_key(mut self, value: impl Into<Value>) -> Self {
        self.sort_key = Some(value.into());
        self
    }

    /// Add a SET action: set the attribute at `path` to `value`.
    ///
    /// Dot-separated paths navigate into nested objects (e.g. `"address.city"`).
    /// Intermediate objects are created if they don't exist.
    pub fn set(mut self, path: &str, value: impl Into<Value>) -> Self {
        self.actions.push(UpdateAction::Set {
            path: path.to_string(),
            value: value.into(),
        });
        self
    }

    /// Add a REMOVE action: remove the attribute at `path`.
    ///
    /// Dot-separated paths navigate into nested objects. Silent no-op if the
    /// path doesn't exist.
    pub fn remove(mut self, path: &str) -> Self {
        self.actions.push(UpdateAction::Remove {
            path: path.to_string(),
        });
        self
    }

    /// Add an ADD action: increment a number or union elements into an array.
    ///
    /// If the attribute doesn't exist, initializes it to `value`.
    /// Returns a type error if the existing attribute is not a number or array.
    pub fn add(mut self, path: &str, value: impl Into<Value>) -> Self {
        self.actions.push(UpdateAction::Add {
            path: path.to_string(),
            value: value.into(),
        });
        self
    }

    /// Add a DELETE action: remove elements from an array (set difference).
    ///
    /// `value` must be an array of elements to remove. If the resulting array
    /// is empty, the attribute is removed entirely. Silent no-op if the
    /// attribute doesn't exist.
    pub fn delete(mut self, path: &str, value: impl Into<Value>) -> Self {
        self.actions.push(UpdateAction::Delete {
            path: path.to_string(),
            value: value.into(),
        });
        self
    }

    /// Set a condition expression that must be satisfied by the existing item
    /// (or empty object for non-existent items) for the update to proceed.
    pub fn condition(mut self, expr: FilterExpr) -> Self {
        self.condition = Some(expr);
        self
    }

    /// Execute the update operation.
    pub fn execute(self) -> Result<(), Error> {
        let pk_val = self.partition_key.ok_or(QueryError::PartitionKeyRequired)?;
        let sk_val = self.sort_key;
        let table = self.table;
        let actions = self.actions;
        let condition = self.condition;

        self.db.transact(move |txn| {
            txn.update_item(
                &table,
                &pk_val,
                sk_val.as_ref(),
                &actions,
                condition.as_ref(),
            )
        })
    }
}

// ---------------------------------------------------------------------------
// QueryBuilder
// ---------------------------------------------------------------------------

/// Builder for querying items with partition key and optional sort conditions.
pub struct QueryBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    partition_key: Option<Value>,
    sort_condition: Option<SortCondition>,
    limit: Option<usize>,
    scan_forward: bool,
    exclusive_start_key: Option<Value>,
    filter: Option<FilterExpr>,
}

impl<'a> QueryBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String) -> Self {
        Self {
            db,
            table,
            partition_key: None,
            sort_condition: None,
            limit: None,
            scan_forward: true,
            exclusive_start_key: None,
            filter: None,
        }
    }

    pub fn partition_key(mut self, value: impl Into<Value>) -> Self {
        self.partition_key = Some(value.into());
        self
    }

    pub fn filter(mut self, expr: FilterExpr) -> Self {
        self.filter = Some(expr);
        self
    }

    pub fn sort_key_eq(mut self, value: impl Into<Value>) -> Self {
        self.sort_condition = Some(SortCondition::Eq(value.into()));
        self
    }

    pub fn sort_key_lt(mut self, value: impl Into<Value>) -> Self {
        self.sort_condition = Some(SortCondition::Lt(value.into()));
        self
    }

    pub fn sort_key_le(mut self, value: impl Into<Value>) -> Self {
        self.sort_condition = Some(SortCondition::Le(value.into()));
        self
    }

    pub fn sort_key_gt(mut self, value: impl Into<Value>) -> Self {
        self.sort_condition = Some(SortCondition::Gt(value.into()));
        self
    }

    pub fn sort_key_ge(mut self, value: impl Into<Value>) -> Self {
        self.sort_condition = Some(SortCondition::Ge(value.into()));
        self
    }

    pub fn sort_key_between(mut self, low: impl Into<Value>, high: impl Into<Value>) -> Self {
        self.sort_condition = Some(SortCondition::Between(low.into(), high.into()));
        self
    }

    pub fn sort_key_begins_with(mut self, prefix: &str) -> Self {
        self.sort_condition = Some(SortCondition::BeginsWith(prefix.to_string()));
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn scan_forward(mut self, forward: bool) -> Self {
        self.scan_forward = forward;
        self
    }

    pub fn exclusive_start_key(mut self, key: impl Into<Value>) -> Self {
        self.exclusive_start_key = Some(key.into());
        self
    }

    /// Execute the query.
    pub fn execute(self) -> Result<QueryResult, Error> {
        let pk_val = self.partition_key.ok_or(QueryError::PartitionKeyRequired)?;

        self.db.read_snapshot(|store, catalog_root, snapshot_txn| {
            let entry = self.db.cached_get_table(store, catalog_root, &self.table)?;
            let schema = &entry.schema;

            let pk = key_utils::json_to_key_value(
                &pk_val,
                schema.partition_key.key_type,
                &schema.partition_key.name,
            )?;
            key_utils::validate_partition_key_size(&pk)?;

            // Validate sort condition against schema.
            if self.sort_condition.is_some() && schema.sort_key.is_none() {
                return Err(QueryError::SortKeyNotSupported.into());
            }

            let sk_type = schema.sort_key.as_ref().map(|sk| sk.key_type);

            // Compute range bounds.
            let (start_key, end_key) =
                compute_scan_bounds(&pk, self.sort_condition.as_ref(), sk_type)?;

            // Execute range scan with MVCC filtering.
            let raw_results = mvcc_ops::mvcc_range_scan(
                store,
                entry.data_root_page,
                start_key.as_deref(),
                end_key.as_deref(),
                snapshot_txn,
            )?;

            // Build the exclusive_start_key encoded form if provided.
            let skip_key = if let Some(ref esk) = self.exclusive_start_key {
                Some(encode_exclusive_start_key(esk, schema)?)
            } else {
                None
            };

            // Deserialize, TTL-filter, collect.
            let mut items: Vec<(Vec<u8>, Value)> = Vec::new();
            for (key_bytes, value_bytes) in raw_results {
                // Skip items <= exclusive_start_key.
                if let Some(ref sk) = skip_key
                    && key_bytes <= *sk
                {
                    continue;
                }

                let val: Value = rmp_serde::from_slice(&value_bytes).map_err(|e| {
                    StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
                })?;

                if is_ttl_expired(&val, schema) {
                    continue;
                }

                items.push((key_bytes, val));
            }

            if !self.scan_forward {
                items.reverse();
            }

            // Evaluate limit (counts evaluated items) and filter.
            let mut result_items = Vec::new();
            let mut last_evaluated_bytes: Option<Vec<u8>> = None;
            let mut has_more = false;

            for (evaluated, (key_bytes, val)) in items.iter().enumerate() {
                if let Some(limit) = self.limit
                    && evaluated >= limit
                {
                    has_more = true;
                    break;
                }

                last_evaluated_bytes = Some(key_bytes.clone());

                let passes = match &self.filter {
                    Some(filter) => filter.eval(val).unwrap_or(false),
                    None => true,
                };

                if passes {
                    result_items.push(val.clone());
                }
            }

            // If no limit was hit but we consumed all items, no pagination needed.
            let last_evaluated_key = if has_more {
                if let Some(ref last_key_bytes) = last_evaluated_bytes {
                    Some(build_last_evaluated_key(last_key_bytes, schema)?)
                } else {
                    None
                }
            } else {
                None
            };

            Ok(QueryResult {
                items: result_items,
                last_evaluated_key,
            })
        })
    }
}

// ---------------------------------------------------------------------------
// ScanBuilder
// ---------------------------------------------------------------------------

/// Builder for scanning all items in a table.
pub struct ScanBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    limit: Option<usize>,
    exclusive_start_key: Option<Value>,
    filter: Option<FilterExpr>,
}

impl<'a> ScanBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String) -> Self {
        Self {
            db,
            table,
            limit: None,
            exclusive_start_key: None,
            filter: None,
        }
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn exclusive_start_key(mut self, key: impl Into<Value>) -> Self {
        self.exclusive_start_key = Some(key.into());
        self
    }

    pub fn filter(mut self, expr: FilterExpr) -> Self {
        self.filter = Some(expr);
        self
    }

    /// Execute the scan.
    pub fn execute(self) -> Result<QueryResult, Error> {
        self.db.read_snapshot(|store, catalog_root, snapshot_txn| {
            let entry = self.db.cached_get_table(store, catalog_root, &self.table)?;
            let schema = &entry.schema;

            // Full range scan with MVCC filtering.
            let raw_results =
                mvcc_ops::mvcc_range_scan(store, entry.data_root_page, None, None, snapshot_txn)?;

            let skip_key = if let Some(ref esk) = self.exclusive_start_key {
                Some(encode_exclusive_start_key(esk, schema)?)
            } else {
                None
            };

            let mut items: Vec<(Vec<u8>, Value)> = Vec::new();
            for (key_bytes, value_bytes) in raw_results {
                if let Some(ref sk) = skip_key
                    && key_bytes <= *sk
                {
                    continue;
                }

                let val: Value = rmp_serde::from_slice(&value_bytes).map_err(|e| {
                    StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
                })?;
                if is_ttl_expired(&val, schema) {
                    continue;
                }
                items.push((key_bytes, val));
            }

            // Evaluate limit (counts evaluated items) and filter.
            let mut result_items = Vec::new();
            let mut last_evaluated_bytes: Option<Vec<u8>> = None;
            let mut has_more = false;

            for (evaluated, (key_bytes, val)) in items.iter().enumerate() {
                if let Some(limit) = self.limit
                    && evaluated >= limit
                {
                    has_more = true;
                    break;
                }

                last_evaluated_bytes = Some(key_bytes.clone());

                let passes = match &self.filter {
                    Some(filter) => filter.eval(val).unwrap_or(false),
                    None => true,
                };

                if passes {
                    result_items.push(val.clone());
                }
            }

            let last_evaluated_key = if has_more {
                if let Some(ref last_key_bytes) = last_evaluated_bytes {
                    Some(build_last_evaluated_key(last_key_bytes, schema)?)
                } else {
                    None
                }
            } else {
                None
            };

            Ok(QueryResult {
                items: result_items,
                last_evaluated_key,
            })
        })
    }
}

// ---------------------------------------------------------------------------
// ListPartitionKeysBuilder
// ---------------------------------------------------------------------------

/// Builder for listing distinct partition keys in a table.
pub struct ListPartitionKeysBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    limit: Option<usize>,
}

impl<'a> ListPartitionKeysBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String) -> Self {
        Self {
            db,
            table,
            limit: None,
        }
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Execute the operation, returning distinct partition key values as JSON.
    pub fn execute(self) -> Result<Vec<Value>, Error> {
        self.db.read_snapshot(|store, catalog_root, snapshot_txn| {
            let entry = self.db.cached_get_table(store, catalog_root, &self.table)?;
            let schema = &entry.schema;
            let has_sort_key = schema.sort_key.is_some();

            let raw_results =
                mvcc_ops::mvcc_range_scan(store, entry.data_root_page, None, None, snapshot_txn)?;

            let mut keys: Vec<Value> = Vec::new();
            let mut last_pk_bytes: Option<Vec<u8>> = None;

            for (key_bytes, _value_bytes) in raw_results {
                let (pk, _sk) = composite::decode_composite(&key_bytes, has_sort_key)?;
                let pk_encoded = composite::encode_composite(&pk, None)?;

                if last_pk_bytes.as_ref() == Some(&pk_encoded) {
                    continue;
                }
                last_pk_bytes = Some(pk_encoded);

                keys.push(key_utils::key_value_to_json(&pk));

                if let Some(limit) = self.limit
                    && keys.len() >= limit
                {
                    break;
                }
            }

            Ok(keys)
        })
    }
}

// ---------------------------------------------------------------------------
// ListSortKeyPrefixesBuilder
// ---------------------------------------------------------------------------

/// Builder for listing distinct sort key prefixes (split on `#`) for a given
/// partition key.
///
/// For string sort keys the value is split on the first `#` and only the
/// prefix segment is returned (deduplicated). For non-string sort keys the
/// raw decoded values are returned (deduplicated).
///
/// This is useful for agent memory pipelines where SKs follow the DynamoDB
/// convention of `#`-separated hierarchical keys and you want to discover
/// what top-level categories exist under a partition key.
pub struct ListSortKeyPrefixesBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    partition_key: Option<Value>,
    limit: Option<usize>,
}

impl<'a> ListSortKeyPrefixesBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String) -> Self {
        Self {
            db,
            table,
            partition_key: None,
            limit: None,
        }
    }

    pub fn partition_key(mut self, pk: impl Into<Value>) -> Self {
        self.partition_key = Some(pk.into());
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Execute the operation, returning distinct sort key prefixes as JSON
    /// strings.
    pub fn execute(self) -> Result<Vec<Value>, Error> {
        let pk_val = self.partition_key.ok_or(QueryError::PartitionKeyRequired)?;

        self.db.read_snapshot(|store, catalog_root, snapshot_txn| {
            let entry = self.db.cached_get_table(store, catalog_root, &self.table)?;
            let schema = &entry.schema;

            let sk_def = schema
                .sort_key
                .as_ref()
                .ok_or(QueryError::InvalidCondition(
                    "table has no sort key".to_string(),
                ))?;

            let pk = key_utils::json_to_key_value(
                &pk_val,
                schema.partition_key.key_type,
                &schema.partition_key.name,
            )?;
            key_utils::validate_partition_key_size(&pk)?;

            // Scan only this partition.
            let (start_key, end_key) = compute_scan_bounds(&pk, None, Some(sk_def.key_type))?;

            let raw_results = mvcc_ops::mvcc_range_scan(
                store,
                entry.data_root_page,
                start_key.as_deref(),
                end_key.as_deref(),
                snapshot_txn,
            )?;

            let mut prefixes: Vec<Value> = Vec::new();
            let mut last_prefix: Option<String> = None;

            for (key_bytes, _value_bytes) in raw_results {
                let (_pk, sk) = composite::decode_composite(&key_bytes, true)?;
                let sk = sk.expect("sort key should be present");

                let prefix = extract_prefix(&sk);

                if last_prefix.as_ref() == Some(&prefix) {
                    continue;
                }
                last_prefix = Some(prefix.clone());

                prefixes.push(Value::String(prefix));

                if let Some(limit) = self.limit
                    && prefixes.len() >= limit
                {
                    break;
                }
            }

            Ok(prefixes)
        })
    }
}

/// Extract the `#`-delimited prefix from a sort key value.
///
/// For string keys: splits on the first `#` and returns the prefix segment.
/// For number/binary keys: returns the string representation of the value.
fn extract_prefix(kv: &crate::encoding::KeyValue) -> String {
    match kv {
        crate::encoding::KeyValue::String(s) => match s.split_once('#') {
            Some((prefix, _)) => prefix.to_string(),
            None => s.clone(),
        },
        crate::encoding::KeyValue::Number(n) => n.to_string(),
        crate::encoding::KeyValue::Binary(b) => format!("{b:?}"),
    }
}

// ---------------------------------------------------------------------------
// IndexQueryBuilder
// ---------------------------------------------------------------------------

/// Builder for querying a secondary index.
///
/// Scans the index B+Tree for entries matching `key_value` (the indexed
/// attribute value), then fetches full documents from the primary table.
/// Deleted or invisible documents are silently skipped (lazy GC).
pub struct IndexQueryBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    index_name: String,
    key_value: Option<Value>,
    limit: Option<usize>,
    scan_forward: bool,
    filter: Option<FilterExpr>,
    exclusive_start_key: Option<Value>,
}

impl<'a> IndexQueryBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String, index_name: String) -> Self {
        Self {
            db,
            table,
            index_name,
            key_value: None,
            limit: None,
            scan_forward: true,
            filter: None,
            exclusive_start_key: None,
        }
    }

    /// Set the indexed attribute value to search for.
    pub fn key_value(mut self, val: impl Into<Value>) -> Self {
        self.key_value = Some(val.into());
        self
    }

    /// Limit the number of results.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Set scan direction (default: forward/ascending).
    pub fn scan_forward(mut self, forward: bool) -> Self {
        self.scan_forward = forward;
        self
    }

    /// Set a filter expression to apply to results.
    pub fn filter(mut self, expr: FilterExpr) -> Self {
        self.filter = Some(expr);
        self
    }

    /// Set the exclusive start key for pagination.
    pub fn exclusive_start_key(mut self, key: impl Into<Value>) -> Self {
        self.exclusive_start_key = Some(key.into());
        self
    }

    /// Execute the index query.
    pub fn execute(self) -> Result<QueryResult, Error> {
        let search_value = self.key_value.ok_or(QueryError::IndexKeyRequired)?;

        self.db.read_snapshot(|store, catalog_root, snapshot_txn| {
            let entry = self.db.cached_get_table(store, catalog_root, &self.table)?;
            let schema = &entry.schema;

            // Find the index definition.
            let index = entry
                .indexes
                .iter()
                .find(|idx| idx.name == self.index_name)
                .ok_or_else(|| SchemaError::IndexNotFound(self.index_name.clone()))?;

            // Convert the search value to a KeyValue.
            let indexed_kv = key_utils::json_to_key_value(
                &search_value,
                index.index_key.key_type,
                &index.index_key.name,
            )?;

            // Compute scan bounds for all entries with this indexed value.
            let (start_key, end_key) = compute_scan_bounds(&indexed_kv, None, None)?;

            // Encode the exclusive_start_key if provided.
            let skip_key = if let Some(ref esk) = self.exclusive_start_key {
                Some(encode_index_exclusive_start_key(esk, schema, index)?)
            } else {
                None
            };

            // Scan the index B+Tree (plain B+Tree, no MVCC wrapping).
            let index_entries = btree_ops::range_scan(
                store,
                index.root_page,
                start_key.as_deref(),
                end_key.as_deref(),
            )?;

            // Collect results by looking up each primary document via MVCC.
            // Track index key bytes alongside documents for cursor building.
            let mut items: Vec<(Vec<u8>, Value)> = Vec::new();
            for (index_key, _empty_val) in &index_entries {
                // Skip items based on exclusive_start_key (direction-aware).
                // Forward: skip items already seen (key <= cursor).
                // Reverse: skip items already seen (key >= cursor).
                if let Some(ref sk) = skip_key {
                    if self.scan_forward {
                        if *index_key <= *sk {
                            continue;
                        }
                    } else if *index_key >= *sk {
                        continue;
                    }
                }

                // Decode the primary composite key bytes from the index entry.
                let primary_key_bytes = key_utils::decode_primary_key_from_index_entry(index_key)?;

                // Fetch the full document from the primary table.
                let doc_bytes = mvcc_ops::mvcc_get(
                    store,
                    entry.data_root_page,
                    &primary_key_bytes,
                    snapshot_txn,
                )?;

                match doc_bytes {
                    Some(bytes) => {
                        let val: Value = rmp_serde::from_slice(&bytes).map_err(|e| {
                            StorageError::CorruptedPage(format!(
                                "failed to deserialize document: {e}"
                            ))
                        })?;
                        // Check TTL expiration.
                        if is_ttl_expired(&val, &entry.schema) {
                            continue;
                        }
                        items.push((index_key.clone(), val));
                    }
                    None => {
                        // Document deleted or invisible — lazy GC skip.
                        continue;
                    }
                }
            }

            // Apply scan direction.
            if !self.scan_forward {
                items.reverse();
            }

            // Evaluate limit (counts evaluated items) and filter.
            let mut result_items = Vec::new();
            let mut last_evaluated_bytes: Option<Vec<u8>> = None;
            let mut has_more = false;

            for (evaluated, (index_key_bytes, val)) in items.iter().enumerate() {
                if let Some(limit) = self.limit
                    && evaluated >= limit
                {
                    has_more = true;
                    break;
                }

                last_evaluated_bytes = Some(index_key_bytes.clone());

                let passes = match &self.filter {
                    Some(filter) => filter.eval(val).unwrap_or(false),
                    None => true,
                };

                if passes {
                    result_items.push(val.clone());
                }
            }

            // Build pagination cursor if limit was hit and more items remain.
            let last_evaluated_key = if has_more {
                if let Some(ref last_key) = last_evaluated_bytes {
                    Some(build_index_last_evaluated_key(last_key, schema, index)?)
                } else {
                    None
                }
            } else {
                None
            };

            Ok(QueryResult {
                items: result_items,
                last_evaluated_key,
            })
        })
    }
}

// ---------------------------------------------------------------------------
// PartitionSchemaBuilder
// ---------------------------------------------------------------------------

/// Builder for declaring a partition schema on a table.
pub struct PartitionSchemaBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    prefix: Option<String>,
    description: Option<String>,
    attributes: Vec<AttributeDef>,
    validate: bool,
}

impl<'a> PartitionSchemaBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String) -> Self {
        Self {
            db,
            table,
            prefix: None,
            description: None,
            attributes: Vec::new(),
            validate: false,
        }
    }

    /// Set the partition key prefix (e.g. "CONTACT").
    pub fn prefix(mut self, p: &str) -> Self {
        self.prefix = Some(p.to_string());
        self
    }

    /// Set a human-readable description.
    pub fn description(mut self, d: &str) -> Self {
        self.description = Some(d.to_string());
        self
    }

    /// Declare an expected attribute with type and required flag.
    pub fn attribute(mut self, name: &str, attr_type: AttrType, required: bool) -> Self {
        self.attributes.push(AttributeDef {
            name: name.to_string(),
            attr_type,
            required,
        });
        self
    }

    /// Enable or disable write validation for documents matching this prefix.
    pub fn validate(mut self, v: bool) -> Self {
        self.validate = v;
        self
    }

    /// Execute the partition schema creation.
    pub fn execute(self) -> Result<(), Error> {
        let prefix = self.prefix.ok_or(QueryError::PartitionKeyRequired)?;
        let schema = crate::types::PartitionSchema {
            prefix,
            description: self.description.unwrap_or_default(),
            attributes: self.attributes,
            validate: self.validate,
        };
        let table = self.table;
        self.db.transact(move |txn| {
            let new_root = catalog::ops::create_partition_schema(
                &mut txn.store,
                txn.catalog_root,
                &table,
                schema,
            )?;
            txn.catalog_root = new_root;
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// CreateIndexBuilder
// ---------------------------------------------------------------------------

/// Builder for creating a secondary index on a table.
///
/// The index is scoped to a partition schema (prefix). Documents whose
/// partition key starts with the prefix AND contain the indexed attribute
/// will have entries in the index B+Tree.
///
/// If the table already has data matching the prefix, a synchronous backfill
/// is performed within the transaction. This blocks all reads and writes
/// for the duration.
pub struct CreateIndexBuilder<'a> {
    db: &'a FerridynDB,
    table: String,
    name: Option<String>,
    partition_schema: Option<String>,
    index_key: Option<(String, KeyType)>,
}

impl<'a> CreateIndexBuilder<'a> {
    pub(crate) fn new(db: &'a FerridynDB, table: String) -> Self {
        Self {
            db,
            table,
            name: None,
            partition_schema: None,
            index_key: None,
        }
    }

    /// Set the index name.
    pub fn name(mut self, n: &str) -> Self {
        self.name = Some(n.to_string());
        self
    }

    /// Set the partition schema prefix this index is scoped to.
    pub fn partition_schema(mut self, prefix: &str) -> Self {
        self.partition_schema = Some(prefix.to_string());
        self
    }

    /// Set the attribute to index and its key type.
    pub fn index_key(mut self, attr_name: &str, key_type: KeyType) -> Self {
        self.index_key = Some((attr_name.to_string(), key_type));
        self
    }

    /// Execute the index creation (with synchronous backfill).
    pub fn execute(self) -> Result<(), Error> {
        let name = self.name.ok_or(QueryError::InvalidCondition(
            "index name required".to_string(),
        ))?;
        let partition_schema = self.partition_schema.ok_or(QueryError::InvalidCondition(
            "partition_schema required".to_string(),
        ))?;
        let (attr_name, key_type) = self.index_key.ok_or(QueryError::IndexKeyRequired)?;

        let key_def = KeyDefinition {
            name: attr_name,
            key_type,
        };
        let table = self.table;

        self.db.transact(move |txn| {
            let new_root = catalog::ops::create_index(
                &mut txn.store,
                txn.catalog_root,
                &table,
                name,
                partition_schema,
                key_def,
                txn.txn_id,
            )?;
            txn.catalog_root = new_root;
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Encode the exclusive_start_key JSON object into composite key bytes.
fn encode_exclusive_start_key(esk: &Value, schema: &TableSchema) -> Result<Vec<u8>, Error> {
    let pk_val = esk
        .get(&schema.partition_key.name)
        .ok_or(QueryError::PartitionKeyRequired)?;
    let pk = key_utils::json_to_key_value(
        pk_val,
        schema.partition_key.key_type,
        &schema.partition_key.name,
    )?;

    let sk = if let Some(ref sk_def) = schema.sort_key {
        if let Some(sk_val) = esk.get(&sk_def.name) {
            Some(key_utils::json_to_key_value(
                sk_val,
                sk_def.key_type,
                &sk_def.name,
            )?)
        } else {
            None
        }
    } else {
        None
    };

    let encoded = composite::encode_composite(&pk, sk.as_ref())?;
    Ok(encoded)
}

/// Build a last_evaluated_key JSON object from composite key bytes.
fn build_last_evaluated_key(key_bytes: &[u8], schema: &TableSchema) -> Result<Value, Error> {
    let has_sort_key = schema.sort_key.is_some();
    let (pk, sk) = composite::decode_composite(key_bytes, has_sort_key)?;

    let mut obj = serde_json::Map::new();
    obj.insert(
        schema.partition_key.name.clone(),
        key_utils::key_value_to_json(&pk),
    );
    if let (Some(sk_def), Some(sk_val)) = (&schema.sort_key, sk) {
        obj.insert(sk_def.name.clone(), key_utils::key_value_to_json(&sk_val));
    }
    Ok(Value::Object(obj))
}

/// Encode an index query exclusive_start_key JSON object into index B+Tree key bytes.
fn encode_index_exclusive_start_key(
    esk: &Value,
    schema: &TableSchema,
    index: &crate::types::IndexDefinition,
) -> Result<Vec<u8>, Error> {
    use crate::encoding::{binary, composite};

    // Extract and validate the indexed attribute value.
    let index_attr_val = esk.get(&index.index_key.name).ok_or_else(|| {
        QueryError::InvalidIndexCursor(format!(
            "missing indexed attribute '{}' in cursor",
            index.index_key.name
        ))
    })?;
    let index_kv = key_utils::json_to_key_value(
        index_attr_val,
        index.index_key.key_type,
        &index.index_key.name,
    )
    .map_err(|e| {
        QueryError::InvalidIndexCursor(format!(
            "invalid type for indexed attribute '{}': {e}",
            index.index_key.name
        ))
    })?;

    // Extract and validate the partition key.
    let pk_val = esk.get(&schema.partition_key.name).ok_or_else(|| {
        QueryError::InvalidIndexCursor(format!(
            "missing partition key '{}' in cursor",
            schema.partition_key.name
        ))
    })?;
    let pk = key_utils::json_to_key_value(
        pk_val,
        schema.partition_key.key_type,
        &schema.partition_key.name,
    )
    .map_err(|e| {
        QueryError::InvalidIndexCursor(format!(
            "invalid type for partition key '{}': {e}",
            schema.partition_key.name
        ))
    })?;

    // Extract and validate the sort key (if table has one).
    let sk = if let Some(ref sk_def) = schema.sort_key {
        let sk_val = esk.get(&sk_def.name).ok_or_else(|| {
            QueryError::InvalidIndexCursor(format!("missing sort key '{}' in cursor", sk_def.name))
        })?;
        Some(
            key_utils::json_to_key_value(sk_val, sk_def.key_type, &sk_def.name).map_err(|e| {
                QueryError::InvalidIndexCursor(format!(
                    "invalid type for sort key '{}': {e}",
                    sk_def.name
                ))
            })?,
        )
    } else {
        None
    };

    // Build the primary composite key bytes.
    let primary_key_bytes = composite::encode_composite(&pk, sk.as_ref())?;

    // Build the index key: [tag][indexed_value][TAG_BINARY][primary_key_bytes]
    // Same format as build_index_key() in key_utils.rs.
    let mut index_key = Vec::new();

    index_key.push(composite::key_value_tag(&index_kv));
    index_key.extend(match &index_kv {
        crate::encoding::KeyValue::String(s) => crate::encoding::string::encode_string(s),
        crate::encoding::KeyValue::Number(n) => {
            crate::encoding::number::encode_number(*n)?.to_vec()
        }
        crate::encoding::KeyValue::Binary(b) => binary::encode_binary(b),
    });

    index_key.push(composite::TAG_BINARY);
    index_key.extend(&primary_key_bytes);

    Ok(index_key)
}

/// Build a last_evaluated_key JSON object from index B+Tree key bytes.
fn build_index_last_evaluated_key(
    index_key_bytes: &[u8],
    schema: &TableSchema,
    index: &crate::types::IndexDefinition,
) -> Result<Value, Error> {
    if index_key_bytes.is_empty() {
        return Err(crate::error::EncodingError::MalformedKey.into());
    }

    // Decode the indexed attribute value from the first component.
    let tag = index_key_bytes[0];
    let (index_kv, _consumed) = composite::decode_key_value(tag, &index_key_bytes[1..])?;

    // Extract primary key bytes from the index entry.
    let primary_bytes = key_utils::decode_primary_key_from_index_entry(index_key_bytes)?;

    // Decode the primary composite key.
    let has_sort_key = schema.sort_key.is_some();
    let (pk, sk) = composite::decode_composite(&primary_bytes, has_sort_key)?;

    // Build the JSON cursor object.
    let mut obj = serde_json::Map::new();
    obj.insert(
        index.index_key.name.clone(),
        key_utils::key_value_to_json(&index_kv),
    );
    obj.insert(
        schema.partition_key.name.clone(),
        key_utils::key_value_to_json(&pk),
    );
    if let (Some(sk_def), Some(sk_val)) = (&schema.sort_key, sk) {
        obj.insert(sk_def.name.clone(), key_utils::key_value_to_json(&sk_val));
    }

    Ok(Value::Object(obj))
}

/// Check if a document is expired according to the table's TTL configuration.
///
/// Returns `true` (expired) only when ALL of:
/// - The table has a `ttl_attribute` configured
/// - The document contains that attribute
/// - The attribute value is a number
/// - The number is non-zero
/// - The number is <= current wall-clock epoch seconds
fn is_ttl_expired(doc: &Value, schema: &TableSchema) -> bool {
    let Some(ttl_attr) = &schema.ttl_attribute else {
        return false;
    };
    let Some(ttl_val) = doc.get(ttl_attr) else {
        return false;
    };
    let Some(epoch_secs) = ttl_val.as_f64() else {
        return false;
    };
    if epoch_secs == 0.0 {
        return false;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    epoch_secs <= now
}
