use dynamite_core::api::{DynaMite, QueryResult};
use dynamite_core::error::Error;
use dynamite_core::types::TableSchema;
use serde_json::Value;

use crate::commands::{Command, SortClause};

/// Structured result from executing a command.
pub enum CommandResult {
    /// Mutation succeeded (PUT, DELETE, CREATE TABLE, DROP TABLE).
    Ok(String),
    /// Single item returned (GET).
    Item(Option<Value>),
    /// Multiple items returned (QUERY, SCAN).
    QueryResult(QueryResult),
    /// Table list (LIST TABLES).
    TableList(Vec<String>),
    /// Table schema (DESCRIBE TABLE).
    TableSchema(TableSchema),
    /// Distinct partition keys (LIST KEYS).
    PartitionKeys(Vec<Value>),
    /// Distinct sort key prefixes (LIST PREFIXES).
    SortKeyPrefixes(Vec<Value>),
    /// Help text (optional topic for per-command help).
    Help(Option<String>),
    /// Exit signal.
    Exit,
}

/// Execute a parsed command against the database.
pub fn execute(db: &DynaMite, cmd: Command) -> Result<CommandResult, Error> {
    match cmd {
        Command::CreateTable {
            name,
            pk_name,
            pk_type,
            sk,
            ttl,
        } => exec_create_table(db, &name, &pk_name, pk_type, sk, ttl.as_deref()),
        Command::DropTable { name } => exec_drop_table(db, &name),
        Command::ListTables => exec_list_tables(db),
        Command::DescribeTable { name } => exec_describe_table(db, &name),
        Command::Put { table, document } => exec_put(db, &table, document),
        Command::Get { table, pk, sk } => exec_get(db, &table, pk, sk),
        Command::Delete { table, pk, sk } => exec_delete(db, &table, pk, sk),
        Command::Query {
            table,
            pk,
            sort_condition,
            limit,
            desc,
        } => exec_query(db, &table, pk, sort_condition, limit, desc),
        Command::Scan { table, limit } => exec_scan(db, &table, limit),
        Command::ListKeys { table, limit } => exec_list_keys(db, &table, limit),
        Command::ListPrefixes { table, pk, limit } => exec_list_prefixes(db, &table, pk, limit),
        Command::Help(topic) => Ok(CommandResult::Help(topic)),
        Command::Exit => Ok(CommandResult::Exit),
    }
}

fn exec_create_table(
    db: &DynaMite,
    name: &str,
    pk_name: &str,
    pk_type: dynamite_core::types::KeyType,
    sk: Option<(String, dynamite_core::types::KeyType)>,
    ttl: Option<&str>,
) -> Result<CommandResult, Error> {
    let mut builder = db.create_table(name).partition_key(pk_name, pk_type);
    if let Some((sk_name, sk_type)) = sk {
        builder = builder.sort_key(&sk_name, sk_type);
    }
    if let Some(ttl_attr) = ttl {
        builder = builder.ttl_attribute(ttl_attr);
    }
    builder.execute()?;
    Ok(CommandResult::Ok(format!("Table '{name}' created.")))
}

fn exec_drop_table(db: &DynaMite, name: &str) -> Result<CommandResult, Error> {
    db.drop_table(name)?;
    Ok(CommandResult::Ok(format!("Table '{name}' dropped.")))
}

fn exec_list_tables(db: &DynaMite) -> Result<CommandResult, Error> {
    let tables = db.list_tables()?;
    Ok(CommandResult::TableList(tables))
}

fn exec_describe_table(db: &DynaMite, name: &str) -> Result<CommandResult, Error> {
    let schema = db.describe_table(name)?;
    Ok(CommandResult::TableSchema(schema))
}

fn exec_put(db: &DynaMite, table: &str, document: Value) -> Result<CommandResult, Error> {
    db.put_item(table, document)?;
    Ok(CommandResult::Ok("OK".to_string()))
}

fn exec_get(
    db: &DynaMite,
    table: &str,
    pk: Value,
    sk: Option<Value>,
) -> Result<CommandResult, Error> {
    let mut builder = db.get_item(table).partition_key(pk);
    if let Some(sk_val) = sk {
        builder = builder.sort_key(sk_val);
    }
    let item = builder.execute()?;
    Ok(CommandResult::Item(item))
}

fn exec_delete(
    db: &DynaMite,
    table: &str,
    pk: Value,
    sk: Option<Value>,
) -> Result<CommandResult, Error> {
    let mut builder = db.delete_item(table).partition_key(pk);
    if let Some(sk_val) = sk {
        builder = builder.sort_key(sk_val);
    }
    builder.execute()?;
    Ok(CommandResult::Ok("OK".to_string()))
}

fn exec_query(
    db: &DynaMite,
    table: &str,
    pk: Value,
    sort_condition: Option<SortClause>,
    limit: Option<usize>,
    desc: bool,
) -> Result<CommandResult, Error> {
    let mut builder = db.query(table).partition_key(pk);

    if let Some(sc) = sort_condition {
        builder = match sc {
            SortClause::Eq(v) => builder.sort_key_eq(v),
            SortClause::Lt(v) => builder.sort_key_lt(v),
            SortClause::Le(v) => builder.sort_key_le(v),
            SortClause::Gt(v) => builder.sort_key_gt(v),
            SortClause::Ge(v) => builder.sort_key_ge(v),
            SortClause::Between(lo, hi) => builder.sort_key_between(lo, hi),
            SortClause::BeginsWith(prefix) => builder.sort_key_begins_with(&prefix),
        };
    }

    if let Some(n) = limit {
        builder = builder.limit(n);
    }

    if desc {
        builder = builder.scan_forward(false);
    }

    let result = builder.execute()?;
    Ok(CommandResult::QueryResult(result))
}

fn exec_scan(db: &DynaMite, table: &str, limit: Option<usize>) -> Result<CommandResult, Error> {
    let mut builder = db.scan(table);
    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    let result = builder.execute()?;
    Ok(CommandResult::QueryResult(result))
}

fn exec_list_keys(
    db: &DynaMite,
    table: &str,
    limit: Option<usize>,
) -> Result<CommandResult, Error> {
    let mut builder = db.list_partition_keys(table);
    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    let keys = builder.execute()?;
    Ok(CommandResult::PartitionKeys(keys))
}

fn exec_list_prefixes(
    db: &DynaMite,
    table: &str,
    pk: Value,
    limit: Option<usize>,
) -> Result<CommandResult, Error> {
    let mut builder = db.list_sort_key_prefixes(table).partition_key(pk);
    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    let prefixes = builder.execute()?;
    Ok(CommandResult::SortKeyPrefixes(prefixes))
}
