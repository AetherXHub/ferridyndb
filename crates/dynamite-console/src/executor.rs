use dynamite_core::api::DynaMite;
use dynamite_core::error::Error;
use serde_json::Value;

use crate::commands::{Command, SortClause};
use crate::display;

/// Execute a parsed command against the database.
///
/// Returns `Ok(true)` to continue the REPL loop, or `Ok(false)` to exit.
pub fn execute(db: &DynaMite, cmd: Command) -> Result<bool, Error> {
    match cmd {
        Command::CreateTable {
            name,
            pk_name,
            pk_type,
            sk,
        } => exec_create_table(db, &name, &pk_name, pk_type, sk),
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
        Command::Help => {
            display::print_help();
            Ok(true)
        }
        Command::Exit => Ok(false),
    }
}

/// Execute a CREATE TABLE command.
///
/// Uses: `db.create_table(name).partition_key(pk_name, pk_type)[.sort_key(sk_name, sk_type)].execute()`
fn exec_create_table(
    db: &DynaMite,
    name: &str,
    pk_name: &str,
    pk_type: dynamite_core::types::KeyType,
    sk: Option<(String, dynamite_core::types::KeyType)>,
) -> Result<bool, Error> {
    let mut builder = db.create_table(name).partition_key(pk_name, pk_type);
    if let Some((sk_name, sk_type)) = sk {
        builder = builder.sort_key(&sk_name, sk_type);
    }
    builder.execute()?;
    display::print_ok(&format!("Table '{name}' created."));
    Ok(true)
}

/// Execute a DROP TABLE command.
///
/// Uses: `db.drop_table(name)`
fn exec_drop_table(db: &DynaMite, name: &str) -> Result<bool, Error> {
    db.drop_table(name)?;
    display::print_ok(&format!("Table '{name}' dropped."));
    Ok(true)
}

/// Execute a LIST TABLES command.
///
/// Uses: `db.list_tables()`
fn exec_list_tables(db: &DynaMite) -> Result<bool, Error> {
    let tables = db.list_tables()?;
    display::print_table_list(&tables);
    Ok(true)
}

/// Execute a DESCRIBE TABLE command.
///
/// Uses: `db.describe_table(name)`
fn exec_describe_table(db: &DynaMite, name: &str) -> Result<bool, Error> {
    let schema = db.describe_table(name)?;
    display::print_table_schema(&schema);
    Ok(true)
}

/// Execute a PUT command.
///
/// Uses: `db.put_item(table, document)`
fn exec_put(db: &DynaMite, table: &str, document: Value) -> Result<bool, Error> {
    db.put_item(table, document)?;
    display::print_ok("OK");
    Ok(true)
}

/// Execute a GET command.
///
/// Uses: `db.get_item(table).partition_key(pk)[.sort_key(sk)].execute()`
fn exec_get(db: &DynaMite, table: &str, pk: Value, sk: Option<Value>) -> Result<bool, Error> {
    let mut builder = db.get_item(table).partition_key(pk);
    if let Some(sk_val) = sk {
        builder = builder.sort_key(sk_val);
    }
    match builder.execute()? {
        Some(item) => display::print_item(&item),
        None => display::print_not_found(),
    }
    Ok(true)
}

/// Execute a DELETE command.
///
/// Uses: `db.delete_item(table).partition_key(pk)[.sort_key(sk)].execute()`
fn exec_delete(db: &DynaMite, table: &str, pk: Value, sk: Option<Value>) -> Result<bool, Error> {
    let mut builder = db.delete_item(table).partition_key(pk);
    if let Some(sk_val) = sk {
        builder = builder.sort_key(sk_val);
    }
    builder.execute()?;
    display::print_ok("OK");
    Ok(true)
}

/// Execute a QUERY command.
///
/// Uses: `db.query(table).partition_key(pk)[.sort_key_*(...)][.limit(n)][.scan_forward(false)].execute()`
fn exec_query(
    db: &DynaMite,
    table: &str,
    pk: Value,
    sort_condition: Option<SortClause>,
    limit: Option<usize>,
    desc: bool,
) -> Result<bool, Error> {
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
    display::print_query_result(&result);
    Ok(true)
}

/// Execute a SCAN command.
///
/// Uses: `db.scan(table)[.limit(n)].execute()`
fn exec_scan(db: &DynaMite, table: &str, limit: Option<usize>) -> Result<bool, Error> {
    let mut builder = db.scan(table);
    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    let result = builder.execute()?;
    display::print_query_result(&result);
    Ok(true)
}
