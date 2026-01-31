use serde_json::Value;

use crate::catalog;
use crate::encoding::composite;
use crate::error::{Error, QueryError, StorageError};
use crate::mvcc::ops as mvcc_ops;
use crate::types::{KeyDefinition, KeyType, TableSchema};

use super::database::DynaMite;
use super::key_utils;
use super::query::{QueryResult, SortCondition, compute_scan_bounds};

// ---------------------------------------------------------------------------
// TableBuilder
// ---------------------------------------------------------------------------

/// Builder for creating a new table.
pub struct TableBuilder<'a> {
    db: &'a DynaMite,
    name: String,
    partition_key: Option<(String, KeyType)>,
    sort_key: Option<(String, KeyType)>,
}

impl<'a> TableBuilder<'a> {
    pub(crate) fn new(db: &'a DynaMite, name: String) -> Self {
        Self {
            db,
            name,
            partition_key: None,
            sort_key: None,
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
    db: &'a DynaMite,
    table: String,
    partition_key: Option<Value>,
    sort_key: Option<Value>,
}

impl<'a> GetItemBuilder<'a> {
    pub(crate) fn new(db: &'a DynaMite, table: String) -> Self {
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
            let entry = catalog::ops::get_table(store, catalog_root, &self.table)?;
            let schema = &entry.schema;

            let pk = key_utils::json_to_key_value(
                &pk_val,
                schema.partition_key.key_type,
                &schema.partition_key.name,
            )?;

            let sk = match (&schema.sort_key, &self.sort_key) {
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
            let data =
                mvcc_ops::mvcc_get(store, entry.data_root_page, &composite_key, snapshot_txn)?;

            match data {
                Some(bytes) => {
                    let val: Value = serde_json::from_slice(&bytes).map_err(|e| {
                        StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
                    })?;
                    Ok(Some(val))
                }
                None => Ok(None),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// DeleteItemBuilder
// ---------------------------------------------------------------------------

/// Builder for deleting an item by key.
pub struct DeleteItemBuilder<'a> {
    db: &'a DynaMite,
    table: String,
    partition_key: Option<Value>,
    sort_key: Option<Value>,
}

impl<'a> DeleteItemBuilder<'a> {
    pub(crate) fn new(db: &'a DynaMite, table: String) -> Self {
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

    /// Set the sort key value.
    pub fn sort_key(mut self, value: impl Into<Value>) -> Self {
        self.sort_key = Some(value.into());
        self
    }

    /// Execute the delete operation.
    pub fn execute(self) -> Result<(), Error> {
        let pk_val = self.partition_key.ok_or(QueryError::PartitionKeyRequired)?;
        let sk_val = self.sort_key;
        let table = self.table;

        self.db
            .transact(move |txn| txn.delete_item(&table, &pk_val, sk_val.as_ref()))
    }
}

// ---------------------------------------------------------------------------
// QueryBuilder
// ---------------------------------------------------------------------------

/// Builder for querying items with partition key and optional sort conditions.
pub struct QueryBuilder<'a> {
    db: &'a DynaMite,
    table: String,
    partition_key: Option<Value>,
    sort_condition: Option<SortCondition>,
    limit: Option<usize>,
    scan_forward: bool,
    exclusive_start_key: Option<Value>,
}

impl<'a> QueryBuilder<'a> {
    pub(crate) fn new(db: &'a DynaMite, table: String) -> Self {
        Self {
            db,
            table,
            partition_key: None,
            sort_condition: None,
            limit: None,
            scan_forward: true,
            exclusive_start_key: None,
        }
    }

    pub fn partition_key(mut self, value: impl Into<Value>) -> Self {
        self.partition_key = Some(value.into());
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
            let entry = catalog::ops::get_table(store, catalog_root, &self.table)?;
            let schema = &entry.schema;

            let pk = key_utils::json_to_key_value(
                &pk_val,
                schema.partition_key.key_type,
                &schema.partition_key.name,
            )?;

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

            // Filter, reverse, limit.
            let mut items: Vec<(Vec<u8>, Value)> = Vec::new();
            for (key_bytes, value_bytes) in raw_results {
                // Skip items <= exclusive_start_key.
                if let Some(ref sk) = skip_key
                    && key_bytes <= *sk
                {
                    continue;
                }

                let val: Value = serde_json::from_slice(&value_bytes).map_err(|e| {
                    StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
                })?;
                items.push((key_bytes, val));
            }

            if !self.scan_forward {
                items.reverse();
            }

            // Apply limit and compute last_evaluated_key.
            let (limited, last_key) = if let Some(limit) = self.limit {
                if items.len() > limit {
                    let last = items[limit - 1].0.clone();
                    items.truncate(limit);
                    (items, Some(last))
                } else {
                    (items, None)
                }
            } else {
                (items, None)
            };

            let last_evaluated_key = if let Some(last_key_bytes) = last_key {
                Some(build_last_evaluated_key(&last_key_bytes, schema)?)
            } else {
                None
            };

            let result_items: Vec<Value> = limited.into_iter().map(|(_, v)| v).collect();

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
    db: &'a DynaMite,
    table: String,
    limit: Option<usize>,
    exclusive_start_key: Option<Value>,
}

impl<'a> ScanBuilder<'a> {
    pub(crate) fn new(db: &'a DynaMite, table: String) -> Self {
        Self {
            db,
            table,
            limit: None,
            exclusive_start_key: None,
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

    /// Execute the scan.
    pub fn execute(self) -> Result<QueryResult, Error> {
        self.db.read_snapshot(|store, catalog_root, snapshot_txn| {
            let entry = catalog::ops::get_table(store, catalog_root, &self.table)?;
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

                let val: Value = serde_json::from_slice(&value_bytes).map_err(|e| {
                    StorageError::CorruptedPage(format!("failed to deserialize document: {e}"))
                })?;
                items.push((key_bytes, val));
            }

            let (limited, last_key) = if let Some(limit) = self.limit {
                if items.len() > limit {
                    let last = items[limit - 1].0.clone();
                    items.truncate(limit);
                    (items, Some(last))
                } else {
                    (items, None)
                }
            } else {
                (items, None)
            };

            let last_evaluated_key = if let Some(last_key_bytes) = last_key {
                Some(build_last_evaluated_key(&last_key_bytes, schema)?)
            } else {
                None
            };

            let result_items: Vec<Value> = limited.into_iter().map(|(_, v)| v).collect();

            Ok(QueryResult {
                items: result_items,
                last_evaluated_key,
            })
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
