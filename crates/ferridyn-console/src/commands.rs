use ferridyn_core::types::{AttrType, KeyType};
use serde_json::Value;

/// A parsed console command.
#[derive(Debug)]
pub enum Command {
    CreateTable {
        name: String,
        pk_name: String,
        pk_type: KeyType,
        sk: Option<(String, KeyType)>,
        ttl: Option<String>,
    },
    DropTable {
        name: String,
    },
    ListTables,
    DescribeTable {
        name: String,
    },
    Put {
        table: String,
        document: Value,
    },
    Get {
        table: String,
        pk: Value,
        sk: Option<Value>,
    },
    Delete {
        table: String,
        pk: Value,
        sk: Option<Value>,
    },
    Query {
        table: String,
        pk: Value,
        sort_condition: Option<SortClause>,
        limit: Option<usize>,
        desc: bool,
    },
    Scan {
        table: String,
        limit: Option<usize>,
    },
    ListKeys {
        table: String,
        limit: Option<usize>,
    },
    ListPrefixes {
        table: String,
        pk: Value,
        limit: Option<usize>,
    },
    CreateSchema {
        table: String,
        prefix: String,
        description: Option<String>,
        attributes: Vec<(String, AttrType, bool)>, // (name, type, required)
        validate: bool,
    },
    DropSchema {
        table: String,
        prefix: String,
    },
    ListSchemas {
        table: String,
    },
    DescribeSchema {
        table: String,
        prefix: String,
    },
    CreateIndex {
        table: String,
        name: String,
        schema_prefix: String,
        key_attr: String,
        key_type: KeyType,
    },
    DropIndex {
        table: String,
        name: String,
    },
    ListIndexes {
        table: String,
    },
    DescribeIndex {
        table: String,
        name: String,
    },
    QueryIndex {
        table: String,
        index_name: String,
        key_value: Value,
        limit: Option<usize>,
        desc: bool,
    },
    Help(Option<String>),
    Exit,
}

/// Sort key condition in a QUERY command.
#[derive(Debug)]
pub enum SortClause {
    Eq(Value),
    Lt(Value),
    Le(Value),
    Gt(Value),
    Ge(Value),
    Between(Value, Value),
    BeginsWith(String),
}
