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
    pub partition_schema: String,
    pub index_key_name: String,
    pub index_key_type: String,
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
    pub async fn create_index(
        &mut self,
        table: &str,
        name: &str,
        partition_schema: &str,
        index_key_name: &str,
        index_key_type: &str,
    ) -> Result<()> {
        let req = serde_json::json!({
            "op": "create_index",
            "table": table,
            "name": name,
            "partition_schema": partition_schema,
            "index_key": {
                "name": index_key_name,
                "type": index_key_type,
            },
        });
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

    /// Query a secondary index.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_index(
        &mut self,
        table: &str,
        index_name: &str,
        key_value: Value,
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
            .unwrap_or("")
            .to_string(),
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
    }
}
