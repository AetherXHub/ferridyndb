use ferridyn_core::types::KeyType;
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
