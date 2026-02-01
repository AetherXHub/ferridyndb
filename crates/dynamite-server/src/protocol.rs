//! Wire protocol: JSON-over-newlines request/response types.
//!
//! Each request is a single JSON line; each response is a single JSON line.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A request from a client.
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    GetItem {
        table: String,
        partition_key: Value,
        #[serde(default)]
        sort_key: Option<Value>,
    },
    GetItemVersioned {
        table: String,
        partition_key: Value,
        #[serde(default)]
        sort_key: Option<Value>,
    },
    PutItem {
        table: String,
        item: Value,
        #[serde(default)]
        expected_version: Option<u64>,
    },
    DeleteItem {
        table: String,
        partition_key: Value,
        #[serde(default)]
        sort_key: Option<Value>,
    },
    Query {
        table: String,
        partition_key: Value,
        #[serde(default)]
        sort_key_condition: Option<SortKeyCondition>,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        scan_forward: Option<bool>,
        #[serde(default)]
        exclusive_start_key: Option<Value>,
    },
    Scan {
        table: String,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        exclusive_start_key: Option<Value>,
    },
    CreateTable {
        table: String,
        partition_key: KeyDef,
        #[serde(default)]
        sort_key: Option<KeyDef>,
        #[serde(default)]
        ttl_attribute: Option<String>,
    },
    DropTable {
        table: String,
    },
    ListTables,
    DescribeTable {
        table: String,
    },
    ListPartitionKeys {
        table: String,
        #[serde(default)]
        limit: Option<usize>,
    },
    ListSortKeyPrefixes {
        table: String,
        partition_key: Value,
        #[serde(default)]
        limit: Option<usize>,
    },
}

/// Sort key condition for query requests.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum SortKeyCondition {
    Eq { value: Value },
    Lt { value: Value },
    Le { value: Value },
    Gt { value: Value },
    Ge { value: Value },
    Between { low: Value, high: Value },
    BeginsWith { prefix: String },
}

/// Key definition for create_table.
#[derive(Debug, Deserialize)]
pub struct KeyDef {
    pub name: String,
    #[serde(rename = "type")]
    pub key_type: String,
}

/// A response sent back to the client.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Response {
    Ok(OkResponse),
    Error(ErrorResponse),
}

/// Successful response variants.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum OkResponse {
    Item {
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        item: Option<Value>,
    },
    VersionedItem {
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        item: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        version: Option<u64>,
    },
    Items {
        ok: bool,
        items: Vec<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_evaluated_key: Option<Value>,
    },
    Keys {
        ok: bool,
        keys: Vec<Value>,
    },
    Tables {
        ok: bool,
        tables: Vec<String>,
    },
    Schema {
        ok: bool,
        schema: TableSchemaWire,
    },
    Empty {
        ok: bool,
    },
}

/// Table schema in wire format.
#[derive(Debug, Serialize)]
pub struct TableSchemaWire {
    pub name: String,
    pub partition_key: KeyDefWire,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_key: Option<KeyDefWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_attribute: Option<String>,
}

/// Key definition in wire format.
#[derive(Debug, Serialize)]
pub struct KeyDefWire {
    pub name: String,
    #[serde(rename = "type")]
    pub key_type: String,
}

/// Error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<u64>,
}

impl Response {
    pub fn ok_empty() -> Self {
        Response::Ok(OkResponse::Empty { ok: true })
    }

    pub fn ok_item(item: Option<Value>) -> Self {
        Response::Ok(OkResponse::Item { ok: true, item })
    }

    pub fn ok_versioned_item(item: Option<Value>, version: Option<u64>) -> Self {
        Response::Ok(OkResponse::VersionedItem {
            ok: true,
            item,
            version,
        })
    }

    pub fn ok_items(items: Vec<Value>, last_evaluated_key: Option<Value>) -> Self {
        Response::Ok(OkResponse::Items {
            ok: true,
            items,
            last_evaluated_key,
        })
    }

    pub fn ok_keys(keys: Vec<Value>) -> Self {
        Response::Ok(OkResponse::Keys { ok: true, keys })
    }

    pub fn ok_tables(tables: Vec<String>) -> Self {
        Response::Ok(OkResponse::Tables { ok: true, tables })
    }

    pub fn ok_schema(schema: TableSchemaWire) -> Self {
        Response::Ok(OkResponse::Schema { ok: true, schema })
    }

    pub fn error(error: impl Into<String>, message: impl Into<String>) -> Self {
        Response::Error(ErrorResponse {
            error: error.into(),
            message: message.into(),
            expected: None,
            actual: None,
        })
    }

    pub fn version_mismatch(expected: u64, actual: u64) -> Self {
        Response::Error(ErrorResponse {
            error: "VersionMismatch".to_string(),
            message: format!("expected version {expected}, actual {actual}"),
            expected: Some(expected),
            actual: Some(actual),
        })
    }
}
