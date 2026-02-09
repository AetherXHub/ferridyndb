//! Client library for connecting to a `ferridyn-server` via Unix socket.
//!
//! Each method serializes a JSON-line request, sends it, reads a JSON-line
//! response, and returns the parsed result.

use std::path::Path;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

use crate::error::ClientError;
use crate::protocol::{ErrorResponse, KeyDef, SortKeyCondition};
use ferridyn_core::api::FilterExpr;

/// Result type alias for client operations.
pub type Result<T> = std::result::Result<T, ClientError>;

/// A versioned item returned from `get_item_versioned`.
#[derive(Debug, Clone)]
pub struct VersionedItem {
    pub item: Value,
    pub version: u64,
}

/// Result of a query or scan operation.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub items: Vec<Value>,
    pub last_evaluated_key: Option<Value>,
}

/// Table schema returned from `describe_table`.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub partition_key_name: String,
    pub partition_key_type: String,
    pub sort_key_name: Option<String>,
    pub sort_key_type: Option<String>,
    pub ttl_attribute: Option<String>,
}

/// Partition schema returned from server.
#[derive(Debug, Clone)]
pub struct PartitionSchemaInfo {
    pub prefix: String,
    pub description: String,
    pub attributes: Vec<AttributeInfo>,
    pub validate: bool,
}

/// Attribute definition returned from server.
#[derive(Debug, Clone)]
pub struct AttributeInfo {
    pub name: String,
    pub attr_type: String,
    pub required: bool,
}

/// Index definition returned from server.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub partition_schema: Option<String>,
    pub index_key_name: String,
    pub index_key_type: String,
    pub index_sort_key_name: Option<String>,
    pub index_sort_key_type: Option<String>,
    pub projection_type: String,
    pub projection_attributes: Option<Vec<String>>,
    pub is_local: bool,
}

/// Input for creating a partition schema attribute.
#[derive(Debug, Clone)]
pub struct AttributeDefInput {
    pub name: String,
    pub attr_type: String,
    pub required: bool,
}

/// Input for an update action sent to the server.
#[derive(Debug, Clone)]
pub struct UpdateActionInput {
    pub action: String,
    pub path: String,
    pub value: Option<Value>,
}

/// Input for a batch write operation.
#[derive(Debug, Clone)]
pub enum BatchWriteInput {
    Put {
        table: String,
        item: Value,
    },
    Delete {
        table: String,
        partition_key: Value,
        sort_key: Option<Value>,
    },
}

/// Stream record returned from server.
#[derive(Debug, Clone)]
pub struct StreamRecordInfo {
    pub sequence_number: u64,
    pub sub_sequence: u32,
    pub event_type: String,
    pub keys: Value,
    pub timestamp: f64,
    pub new_image: Option<Value>,
    pub old_image: Option<Value>,
}

/// Stream info returned from server.
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub enabled: bool,
    pub view_type: String,
    pub oldest_sequence: Option<u64>,
    pub latest_sequence: Option<u64>,
    pub record_count: usize,
}

/// Client for a FerridynDB server.
pub struct FerridynClient {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
    line_buf: String,
}

impl FerridynClient {
    /// Connect to a FerridynDB server at the given Unix socket path.
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self> {
        let stream = UnixStream::connect(path.as_ref()).await?;
        let (read_half, write_half) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(read_half),
            writer: BufWriter::new(write_half),
            line_buf: String::new(),
        })
    }

    /// Get an item by key.
    pub async fn get_item(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key: Option<Value>,
        projection: Option<&[String]>,
    ) -> Result<Option<Value>> {
        let mut req = serde_json::json!({
            "op": "get_item",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
        });
        if let Some(paths) = projection {
            req.as_object_mut()
                .unwrap()
                .insert("projection".to_string(), serde_json::json!(paths));
        }
        let resp = self.send_request(&req).await?;
        match item_from_response(&resp)? {
            Some(item) => Ok(Some(item)),
            None => Ok(None),
        }
    }

    /// Get an item by key with its version number.
    pub async fn get_item_versioned(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key: Option<Value>,
    ) -> Result<Option<VersionedItem>> {
        let req = serde_json::json!({
            "op": "get_item_versioned",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
        });
        let resp = self.send_request(&req).await?;
        versioned_item_from_response(&resp)
    }

    /// Put an item (unconditional).
    pub async fn put_item(&mut self, table: &str, item: Value) -> Result<()> {
        let req = serde_json::json!({
            "op": "put_item",
            "table": table,
            "item": item,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Put an item with optimistic concurrency control.
    pub async fn put_item_conditional(
        &mut self,
        table: &str,
        item: Value,
        expected_version: u64,
    ) -> Result<()> {
        let req = serde_json::json!({
            "op": "put_item",
            "table": table,
            "item": item,
            "expected_version": expected_version,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Put an item with a condition expression.
    pub async fn put_item_with_condition(
        &mut self,
        table: &str,
        item: Value,
        condition: FilterExpr,
    ) -> Result<()> {
        let req = serde_json::json!({
            "op": "put_item",
            "table": table,
            "item": item,
            "condition": condition,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Delete an item by key.
    pub async fn delete_item(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key: Option<Value>,
    ) -> Result<()> {
        let req = serde_json::json!({
            "op": "delete_item",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Delete an item with a condition expression.
    pub async fn delete_item_with_condition(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key: Option<Value>,
        condition: FilterExpr,
    ) -> Result<()> {
        let req = serde_json::json!({
            "op": "delete_item",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
            "condition": condition,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Put an item, returning the old document if one existed.
    pub async fn put_item_returning_old(
        &mut self,
        table: &str,
        item: Value,
    ) -> Result<Option<Value>> {
        let req = serde_json::json!({
            "op": "put_item",
            "table": table,
            "item": item,
            "return_values": "ALL_OLD",
        });
        let resp = self.send_request(&req).await?;
        item_from_response(&resp)
    }

    /// Delete an item, returning the old document if one existed.
    pub async fn delete_item_returning_old(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key: Option<Value>,
    ) -> Result<Option<Value>> {
        let req = serde_json::json!({
            "op": "delete_item",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
            "return_values": "ALL_OLD",
        });
        let resp = self.send_request(&req).await?;
        item_from_response(&resp)
    }

    /// Update an item, returning the old document.
    pub async fn update_item_returning_old(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key: Option<Value>,
        updates: &[UpdateActionInput],
    ) -> Result<Option<Value>> {
        let updates_json = updates_to_json(updates);
        let req = serde_json::json!({
            "op": "update_item",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
            "updates": updates_json,
            "return_values": "ALL_OLD",
        });
        let resp = self.send_request(&req).await?;
        item_from_response(&resp)
    }

    /// Update an item, returning the new document after updates are applied.
    pub async fn update_item_returning_new(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key: Option<Value>,
        updates: &[UpdateActionInput],
    ) -> Result<Option<Value>> {
        let updates_json = updates_to_json(updates);
        let req = serde_json::json!({
            "op": "update_item",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
            "updates": updates_json,
            "return_values": "ALL_NEW",
        });
        let resp = self.send_request(&req).await?;
        item_from_response(&resp)
    }

    /// Update an item with a set of update actions.
    pub async fn update_item(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key: Option<Value>,
        updates: &[UpdateActionInput],
    ) -> Result<()> {
        let updates_json: Vec<Value> = updates
            .iter()
            .map(|u| {
                let mut obj = serde_json::json!({
                    "action": u.action,
                    "path": u.path,
                });
                if let Some(v) = &u.value {
                    obj.as_object_mut()
                        .unwrap()
                        .insert("value".to_string(), v.clone());
                }
                obj
            })
            .collect();
        let req = serde_json::json!({
            "op": "update_item",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
            "updates": updates_json,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Update an item with a condition expression.
    pub async fn update_item_with_condition(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key: Option<Value>,
        updates: &[UpdateActionInput],
        condition: FilterExpr,
    ) -> Result<()> {
        let updates_json: Vec<Value> = updates
            .iter()
            .map(|u| {
                let mut obj = serde_json::json!({
                    "action": u.action,
                    "path": u.path,
                });
                if let Some(v) = &u.value {
                    obj.as_object_mut()
                        .unwrap()
                        .insert("value".to_string(), v.clone());
                }
                obj
            })
            .collect();
        let req = serde_json::json!({
            "op": "update_item",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
            "updates": updates_json,
            "condition": condition,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Query items with partition key and optional sort key conditions.
    #[allow(clippy::too_many_arguments)]
    pub async fn query(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key_condition: Option<SortKeyCondition>,
        limit: Option<usize>,
        scan_forward: Option<bool>,
        exclusive_start_key: Option<Value>,
        filter: Option<FilterExpr>,
        projection: Option<&[String]>,
    ) -> Result<QueryResult> {
        let mut req = serde_json::json!({
            "op": "query",
            "table": table,
            "partition_key": partition_key,
        });
        let obj = req.as_object_mut().unwrap();
        if let Some(cond) = sort_key_condition {
            obj.insert(
                "sort_key_condition".to_string(),
                serde_json::to_value(cond).unwrap(),
            );
        }
        if let Some(n) = limit {
            obj.insert("limit".to_string(), serde_json::json!(n));
        }
        if let Some(fwd) = scan_forward {
            obj.insert("scan_forward".to_string(), serde_json::json!(fwd));
        }
        if let Some(esk) = exclusive_start_key {
            obj.insert("exclusive_start_key".to_string(), esk);
        }
        if let Some(f) = filter {
            obj.insert("filter".to_string(), serde_json::to_value(f).unwrap());
        }
        if let Some(paths) = projection {
            obj.insert("projection".to_string(), serde_json::json!(paths));
        }

        let resp = self.send_request(&req).await?;
        items_from_response(&resp)
    }

    /// Scan all items in a table.
    pub async fn scan(
        &mut self,
        table: &str,
        limit: Option<usize>,
        exclusive_start_key: Option<Value>,
        filter: Option<FilterExpr>,
        projection: Option<&[String]>,
    ) -> Result<QueryResult> {
        let mut req = serde_json::json!({
            "op": "scan",
            "table": table,
        });
        let obj = req.as_object_mut().unwrap();
        if let Some(n) = limit {
            obj.insert("limit".to_string(), serde_json::json!(n));
        }
        if let Some(esk) = exclusive_start_key {
            obj.insert("exclusive_start_key".to_string(), esk);
        }
        if let Some(f) = filter {
            obj.insert("filter".to_string(), serde_json::to_value(f).unwrap());
        }
        if let Some(paths) = projection {
            obj.insert("projection".to_string(), serde_json::json!(paths));
        }

        let resp = self.send_request(&req).await?;
        items_from_response(&resp)
    }

    /// Create a table.
    pub async fn create_table(
        &mut self,
        table: &str,
        partition_key: KeyDef,
        sort_key: Option<KeyDef>,
        ttl_attribute: Option<String>,
    ) -> Result<()> {
        let mut req = serde_json::json!({
            "op": "create_table",
            "table": table,
            "partition_key": {
                "name": partition_key.name,
                "type": partition_key.key_type,
            },
        });
        let obj = req.as_object_mut().unwrap();
        if let Some(sk) = sort_key {
            obj.insert(
                "sort_key".to_string(),
                serde_json::json!({"name": sk.name, "type": sk.key_type}),
            );
        }
        if let Some(ttl) = ttl_attribute {
            obj.insert("ttl_attribute".to_string(), serde_json::json!(ttl));
        }

        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Drop a table.
    pub async fn drop_table(&mut self, table: &str) -> Result<()> {
        let req = serde_json::json!({
            "op": "drop_table",
            "table": table,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// List all tables.
    pub async fn list_tables(&mut self) -> Result<Vec<String>> {
        let req = serde_json::json!({"op": "list_tables"});
        let resp = self.send_request(&req).await?;
        tables_from_response(&resp)
    }

    /// Describe a table's schema.
    pub async fn describe_table(&mut self, table: &str) -> Result<TableSchema> {
        let req = serde_json::json!({
            "op": "describe_table",
            "table": table,
        });
        let resp = self.send_request(&req).await?;
        schema_from_response(&resp)
    }

    /// List distinct partition keys.
    pub async fn list_partition_keys(
        &mut self,
        table: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let mut req = serde_json::json!({
            "op": "list_partition_keys",
            "table": table,
        });
        if let Some(n) = limit {
            req.as_object_mut()
                .unwrap()
                .insert("limit".to_string(), serde_json::json!(n));
        }
        let resp = self.send_request(&req).await?;
        keys_from_response(&resp)
    }

    /// List distinct sort key prefixes.
    pub async fn list_sort_key_prefixes(
        &mut self,
        table: &str,
        partition_key: Value,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let mut req = serde_json::json!({
            "op": "list_sort_key_prefixes",
            "table": table,
            "partition_key": partition_key,
        });
        if let Some(n) = limit {
            req.as_object_mut()
                .unwrap()
                .insert("limit".to_string(), serde_json::json!(n));
        }
        let resp = self.send_request(&req).await?;
        keys_from_response(&resp)
    }

    // -- Partition schema operations --

    /// Create a partition schema.
    pub async fn create_schema(
        &mut self,
        table: &str,
        prefix: &str,
        description: Option<&str>,
        attributes: &[AttributeDefInput],
        validate: bool,
    ) -> Result<()> {
        let attrs: Vec<Value> = attributes
            .iter()
            .map(|a| {
                serde_json::json!({
                    "name": a.name,
                    "type": a.attr_type,
                    "required": a.required,
                })
            })
            .collect();
        let req = serde_json::json!({
            "op": "create_schema",
            "table": table,
            "prefix": prefix,
            "description": description,
            "attributes": attrs,
            "validate": validate,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Drop a partition schema.
    pub async fn drop_schema(&mut self, table: &str, prefix: &str) -> Result<()> {
        let req = serde_json::json!({
            "op": "drop_schema",
            "table": table,
            "prefix": prefix,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// List partition schemas for a table.
    pub async fn list_schemas(&mut self, table: &str) -> Result<Vec<PartitionSchemaInfo>> {
        let req = serde_json::json!({
            "op": "list_schemas",
            "table": table,
        });
        let resp = self.send_request(&req).await?;
        partition_schemas_from_response(&resp)
    }

    /// Describe a partition schema.
    pub async fn describe_schema(
        &mut self,
        table: &str,
        prefix: &str,
    ) -> Result<PartitionSchemaInfo> {
        let req = serde_json::json!({
            "op": "describe_schema",
            "table": table,
            "prefix": prefix,
        });
        let resp = self.send_request(&req).await?;
        partition_schema_from_response(&resp)
    }

    // -- Secondary index operations --

    /// Create a secondary index.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_index(
        &mut self,
        table: &str,
        name: &str,
        partition_schema: Option<&str>,
        index_key_name: Option<&str>,
        index_key_type: Option<&str>,
        index_sort_key_name: Option<&str>,
        index_sort_key_type: Option<&str>,
        projection_type: Option<&str>,
        projection_attributes: Option<&[String]>,
        is_local: Option<bool>,
    ) -> Result<()> {
        let mut req = serde_json::json!({
            "op": "create_index",
            "table": table,
            "name": name,
        });
        if let (Some(ik_name), Some(ik_type)) = (index_key_name, index_key_type) {
            req["index_key"] = serde_json::json!({
                "name": ik_name,
                "type": ik_type,
            });
        }
        if let Some(ps) = partition_schema {
            req["partition_schema"] = serde_json::Value::String(ps.to_string());
        }
        if let (Some(sk_name), Some(sk_type)) = (index_sort_key_name, index_sort_key_type) {
            req["index_sort_key"] = serde_json::json!({
                "name": sk_name,
                "type": sk_type,
            });
        }
        if let Some(pt) = projection_type {
            req["projection_type"] = serde_json::Value::String(pt.to_string());
        }
        if let Some(pa) = projection_attributes {
            req["projection_attributes"] = serde_json::json!(pa);
        }
        if let Some(true) = is_local {
            req["is_local"] = serde_json::json!(true);
        }
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Drop a secondary index.
    pub async fn drop_index(&mut self, table: &str, name: &str) -> Result<()> {
        let req = serde_json::json!({
            "op": "drop_index",
            "table": table,
            "name": name,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// List secondary indexes for a table.
    pub async fn list_indexes(&mut self, table: &str) -> Result<Vec<IndexInfo>> {
        let req = serde_json::json!({
            "op": "list_indexes",
            "table": table,
        });
        let resp = self.send_request(&req).await?;
        indexes_from_response(&resp)
    }

    /// Describe a secondary index.
    pub async fn describe_index(&mut self, table: &str, name: &str) -> Result<IndexInfo> {
        let req = serde_json::json!({
            "op": "describe_index",
            "table": table,
            "name": name,
        });
        let resp = self.send_request(&req).await?;
        index_from_response(&resp)
    }

    /// Retrieve multiple items by key in a single call.
    ///
    /// Results are positional: `results[i]` corresponds to `keys[i]`.
    /// Missing items return `None`.
    pub async fn batch_get_item(
        &mut self,
        table: &str,
        keys: &[(Value, Option<Value>)],
        projection: Option<&[String]>,
    ) -> Result<Vec<Option<Value>>> {
        let keys_json: Vec<Value> = keys
            .iter()
            .map(|(pk, sk)| {
                let mut obj = serde_json::json!({"partition_key": pk});
                if let Some(sk) = sk {
                    obj.as_object_mut()
                        .unwrap()
                        .insert("sort_key".to_string(), sk.clone());
                }
                obj
            })
            .collect();
        let mut req = serde_json::json!({
            "op": "batch_get_item",
            "table": table,
            "keys": keys_json,
        });
        if let Some(paths) = projection {
            req.as_object_mut()
                .unwrap()
                .insert("projection".to_string(), serde_json::json!(paths));
        }
        let resp = self.send_request(&req).await?;
        batch_items_from_response(&resp)
    }

    /// Write multiple items (puts and deletes) in a single atomic batch.
    ///
    /// Returns the number of operations that succeeded. The batch is
    /// all-or-nothing: either all operations commit or none do.
    pub async fn batch_write_item(&mut self, operations: &[BatchWriteInput]) -> Result<usize> {
        let ops_json: Vec<Value> = operations
            .iter()
            .map(|op| match op {
                BatchWriteInput::Put { table, item } => {
                    serde_json::json!({
                        "op": "put",
                        "table": table,
                        "item": item,
                    })
                }
                BatchWriteInput::Delete {
                    table,
                    partition_key,
                    sort_key,
                } => {
                    let mut obj = serde_json::json!({
                        "op": "delete",
                        "table": table,
                        "partition_key": partition_key,
                    });
                    if let Some(sk) = sort_key {
                        obj.as_object_mut()
                            .unwrap()
                            .insert("sort_key".to_string(), sk.clone());
                    }
                    obj
                }
            })
            .collect();
        let req = serde_json::json!({
            "op": "batch_write_item",
            "operations": ops_json,
        });
        let resp = self.send_request(&req).await?;
        succeeded_from_response(&resp)
    }

    /// Query a secondary index.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_index(
        &mut self,
        table: &str,
        index_name: &str,
        key_value: Value,
        sort_key_condition: Option<SortKeyCondition>,
        limit: Option<usize>,
        scan_forward: Option<bool>,
        filter: Option<FilterExpr>,
        exclusive_start_key: Option<Value>,
        projection: Option<&[String]>,
    ) -> Result<QueryResult> {
        let mut req = serde_json::json!({
            "op": "query_index",
            "table": table,
            "index_name": index_name,
            "key_value": key_value,
        });
        let obj = req.as_object_mut().unwrap();
        if let Some(cond) = sort_key_condition {
            obj.insert(
                "sort_key_condition".to_string(),
                serde_json::to_value(cond).unwrap(),
            );
        }
        if let Some(n) = limit {
            obj.insert("limit".to_string(), serde_json::json!(n));
        }
        if let Some(fwd) = scan_forward {
            obj.insert("scan_forward".to_string(), serde_json::json!(fwd));
        }
        if let Some(f) = filter {
            obj.insert("filter".to_string(), serde_json::to_value(f).unwrap());
        }
        if let Some(esk) = exclusive_start_key {
            obj.insert("exclusive_start_key".to_string(), esk);
        }
        if let Some(paths) = projection {
            obj.insert("projection".to_string(), serde_json::json!(paths));
        }
        let resp = self.send_request(&req).await?;
        items_from_response(&resp)
    }

    // -- Stream operations --

    /// Enable a change stream on a table.
    pub async fn enable_stream(&mut self, table: &str, view_type: &str) -> Result<()> {
        let req = serde_json::json!({
            "op": "enable_stream",
            "table": table,
            "view_type": view_type,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Disable a change stream on a table.
    pub async fn disable_stream(&mut self, table: &str) -> Result<()> {
        let req = serde_json::json!({
            "op": "disable_stream",
            "table": table,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Get stream records from a table.
    pub async fn get_stream_records(
        &mut self,
        table: &str,
        after_sequence: Option<u64>,
        limit: Option<usize>,
    ) -> Result<Vec<StreamRecordInfo>> {
        let mut req = serde_json::json!({
            "op": "get_stream_records",
            "table": table,
        });
        let obj = req.as_object_mut().unwrap();
        if let Some(seq) = after_sequence {
            obj.insert("after_sequence".to_string(), serde_json::json!(seq));
        }
        if let Some(n) = limit {
            obj.insert("limit".to_string(), serde_json::json!(n));
        }
        let resp = self.send_request(&req).await?;
        stream_records_from_response(&resp)
    }

    /// Get stream info for a table.
    pub async fn get_stream_info(&mut self, table: &str) -> Result<StreamInfo> {
        let req = serde_json::json!({
            "op": "get_stream_info",
            "table": table,
        });
        let resp = self.send_request(&req).await?;
        stream_info_from_response(&resp)
    }

    // -- TTL operations --

    /// Set a TTL on an existing item.
    pub async fn set_ttl(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key: Option<Value>,
        ttl_seconds: u64,
    ) -> Result<()> {
        let req = serde_json::json!({
            "op": "set_ttl",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
            "ttl_seconds": ttl_seconds,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Remove the TTL from an item, making it permanent.
    pub async fn remove_ttl(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key: Option<Value>,
    ) -> Result<()> {
        let req = serde_json::json!({
            "op": "remove_ttl",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    /// Get the remaining TTL (in seconds) for an item.
    pub async fn get_ttl(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key: Option<Value>,
    ) -> Result<Option<u64>> {
        let req = serde_json::json!({
            "op": "get_ttl",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
        });
        let resp = self.send_request(&req).await?;
        ttl_from_response(&resp)
    }

    /// Sweep expired TTL items from a table.
    pub async fn sweep_expired_ttl(&mut self, table: &str) -> Result<usize> {
        let req = serde_json::json!({
            "op": "sweep_expired_ttl",
            "table": table,
        });
        let resp = self.send_request(&req).await?;
        succeeded_from_response(&resp)
    }

    /// Prune old stream records based on retention settings.
    pub async fn prune_stream(&mut self, table: &str) -> Result<()> {
        let req = serde_json::json!({
            "op": "prune_stream",
            "table": table,
        });
        let resp = self.send_request(&req).await?;
        check_ok(&resp)
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    async fn send_request(&mut self, req: &Value) -> Result<Value> {
        let mut data = serde_json::to_vec(req).map_err(ClientError::Serialization)?;
        data.push(b'\n');
        self.writer.write_all(&data).await?;
        self.writer.flush().await?;

        self.line_buf.clear();
        let n = self.reader.read_line(&mut self.line_buf).await?;
        if n == 0 {
            return Err(ClientError::Disconnected);
        }

        let resp: Value =
            serde_json::from_str(self.line_buf.trim()).map_err(ClientError::Serialization)?;
        Ok(resp)
    }
}

// ---------------------------------------------------------------------------
// Request helpers
// ---------------------------------------------------------------------------

fn updates_to_json(updates: &[UpdateActionInput]) -> Vec<Value> {
    updates
        .iter()
        .map(|u| {
            let mut obj = serde_json::json!({
                "action": u.action,
                "path": u.path,
            });
            if let Some(v) = &u.value {
                obj.as_object_mut()
                    .unwrap()
                    .insert("value".to_string(), v.clone());
            }
            obj
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Response parsing helpers
// ---------------------------------------------------------------------------

fn check_error(resp: &Value) -> Result<()> {
    if let Some(err) = resp.get("error") {
        let error = err.as_str().unwrap_or("Unknown").to_string();
        let message = resp
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_string();

        if error == "VersionMismatch" {
            let expected = resp.get("expected").and_then(|v| v.as_u64()).unwrap_or(0);
            let actual = resp.get("actual").and_then(|v| v.as_u64()).unwrap_or(0);
            return Err(ClientError::VersionMismatch { expected, actual });
        }

        return Err(ClientError::Server(ErrorResponse {
            error,
            message,
            expected: None,
            actual: None,
        }));
    }
    Ok(())
}

fn check_ok(resp: &Value) -> Result<()> {
    check_error(resp)?;
    Ok(())
}

fn item_from_response(resp: &Value) -> Result<Option<Value>> {
    check_error(resp)?;
    Ok(resp
        .get("item")
        .and_then(|v| if v.is_null() { None } else { Some(v.clone()) }))
}

fn versioned_item_from_response(resp: &Value) -> Result<Option<VersionedItem>> {
    check_error(resp)?;
    let item = resp
        .get("item")
        .and_then(|v| if v.is_null() { None } else { Some(v.clone()) });
    let version = resp.get("version").and_then(|v| v.as_u64());

    match (item, version) {
        (Some(item), Some(version)) => Ok(Some(VersionedItem { item, version })),
        _ => Ok(None),
    }
}

fn items_from_response(resp: &Value) -> Result<QueryResult> {
    check_error(resp)?;
    let items = resp
        .get("items")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let last_evaluated_key = resp
        .get("last_evaluated_key")
        .and_then(|v| if v.is_null() { None } else { Some(v.clone()) });
    Ok(QueryResult {
        items,
        last_evaluated_key,
    })
}

fn batch_items_from_response(resp: &Value) -> Result<Vec<Option<Value>>> {
    check_error(resp)?;
    let items = resp
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            ClientError::Protocol("missing 'items' array in batch_get_item response".to_string())
        })?;
    Ok(items
        .iter()
        .map(|v| if v.is_null() { None } else { Some(v.clone()) })
        .collect())
}

fn keys_from_response(resp: &Value) -> Result<Vec<Value>> {
    check_error(resp)?;
    Ok(resp
        .get("keys")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default())
}

fn tables_from_response(resp: &Value) -> Result<Vec<String>> {
    check_error(resp)?;
    let tables = resp
        .get("tables")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    Ok(tables)
}

fn schema_from_response(resp: &Value) -> Result<TableSchema> {
    check_error(resp)?;
    let schema = resp
        .get("schema")
        .ok_or_else(|| ClientError::Protocol("missing 'schema' in response".to_string()))?;

    Ok(TableSchema {
        name: schema
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        partition_key_name: schema
            .get("partition_key")
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        partition_key_type: schema
            .get("partition_key")
            .and_then(|v| v.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        sort_key_name: schema
            .get("sort_key")
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        sort_key_type: schema
            .get("sort_key")
            .and_then(|v| v.get("type"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        ttl_attribute: schema
            .get("ttl_attribute")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    })
}

fn partition_schemas_from_response(resp: &Value) -> Result<Vec<PartitionSchemaInfo>> {
    check_error(resp)?;
    let schemas = resp
        .get("schemas")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().map(parse_schema_info).collect())
        .unwrap_or_default();
    Ok(schemas)
}

fn partition_schema_from_response(resp: &Value) -> Result<PartitionSchemaInfo> {
    check_error(resp)?;
    let schema = resp
        .get("schema")
        .ok_or_else(|| ClientError::Protocol("missing 'schema' in response".to_string()))?;
    Ok(parse_schema_info(schema))
}

fn parse_schema_info(v: &Value) -> PartitionSchemaInfo {
    PartitionSchemaInfo {
        prefix: v
            .get("prefix")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        description: v
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        attributes: v
            .get("attributes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|a| AttributeInfo {
                        name: a
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        attr_type: a
                            .get("type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        required: a.get("required").and_then(|v| v.as_bool()).unwrap_or(false),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        validate: v.get("validate").and_then(|v| v.as_bool()).unwrap_or(false),
    }
}

fn indexes_from_response(resp: &Value) -> Result<Vec<IndexInfo>> {
    check_error(resp)?;
    let indexes = resp
        .get("indexes")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().map(parse_index_info).collect())
        .unwrap_or_default();
    Ok(indexes)
}

fn index_from_response(resp: &Value) -> Result<IndexInfo> {
    check_error(resp)?;
    let index = resp
        .get("index")
        .ok_or_else(|| ClientError::Protocol("missing 'index' in response".to_string()))?;
    Ok(parse_index_info(index))
}

fn parse_index_info(v: &Value) -> IndexInfo {
    IndexInfo {
        name: v
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        partition_schema: v
            .get("partition_schema")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        index_key_name: v
            .get("index_key")
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        index_key_type: v
            .get("index_key")
            .and_then(|v| v.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        index_sort_key_name: v
            .get("index_sort_key")
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        index_sort_key_type: v
            .get("index_sort_key")
            .and_then(|v| v.get("type"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        projection_type: v
            .get("projection_type")
            .and_then(|v| v.as_str())
            .unwrap_or("KEYS_ONLY")
            .to_string(),
        projection_attributes: v
            .get("projection_attributes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            }),
        is_local: v.get("is_local").and_then(|v| v.as_bool()).unwrap_or(false),
    }
}

fn succeeded_from_response(resp: &Value) -> Result<usize> {
    check_error(resp)?;
    resp.get("succeeded")
        .and_then(|v| v.as_u64())
        .map(|n| n as usize)
        .ok_or_else(|| ClientError::Protocol("missing 'succeeded' in response".to_string()))
}

fn stream_records_from_response(resp: &Value) -> Result<Vec<StreamRecordInfo>> {
    check_error(resp)?;
    let records = resp
        .get("records")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().map(parse_stream_record).collect())
        .unwrap_or_default();
    Ok(records)
}

fn stream_info_from_response(resp: &Value) -> Result<StreamInfo> {
    check_error(resp)?;
    let info = resp
        .get("stream_info")
        .ok_or_else(|| ClientError::Protocol("missing 'stream_info' in response".to_string()))?;
    Ok(StreamInfo {
        enabled: info
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        view_type: info
            .get("view_type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        oldest_sequence: info.get("oldest_sequence").and_then(|v| v.as_u64()),
        latest_sequence: info.get("latest_sequence").and_then(|v| v.as_u64()),
        record_count: info
            .get("record_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize,
    })
}

fn ttl_from_response(resp: &Value) -> Result<Option<u64>> {
    check_error(resp)?;
    Ok(resp
        .get("remaining_seconds")
        .and_then(|v| if v.is_null() { None } else { v.as_u64() }))
}

fn parse_stream_record(v: &Value) -> StreamRecordInfo {
    StreamRecordInfo {
        sequence_number: v
            .get("sequence_number")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        sub_sequence: v.get("sub_sequence").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
        event_type: v
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        keys: v.get("keys").cloned().unwrap_or(Value::Null),
        timestamp: v.get("timestamp").and_then(|v| v.as_f64()).unwrap_or(0.0),
        new_image: v
            .get("new_image")
            .and_then(|v| if v.is_null() { None } else { Some(v.clone()) }),
        old_image: v
            .get("old_image")
            .and_then(|v| if v.is_null() { None } else { Some(v.clone()) }),
    }
}
