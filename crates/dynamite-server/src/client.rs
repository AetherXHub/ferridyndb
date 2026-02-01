//! Client library for connecting to a `dynamite-server` via Unix socket.
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

/// Client for a DynamiteDB server.
pub struct DynamiteClient {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
    line_buf: String,
}

impl DynamiteClient {
    /// Connect to a DynamiteDB server at the given Unix socket path.
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
    ) -> Result<Option<Value>> {
        let req = serde_json::json!({
            "op": "get_item",
            "table": table,
            "partition_key": partition_key,
            "sort_key": sort_key,
        });
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

    /// Query items with partition key and optional sort key conditions.
    pub async fn query(
        &mut self,
        table: &str,
        partition_key: Value,
        sort_key_condition: Option<SortKeyCondition>,
        limit: Option<usize>,
        scan_forward: Option<bool>,
        exclusive_start_key: Option<Value>,
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

        let resp = self.send_request(&req).await?;
        items_from_response(&resp)
    }

    /// Scan all items in a table.
    pub async fn scan(
        &mut self,
        table: &str,
        limit: Option<usize>,
        exclusive_start_key: Option<Value>,
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
