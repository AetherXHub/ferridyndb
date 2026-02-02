use ferridyn_core::api::{FerridynDB, QueryResult};
use ferridyn_core::error::Error;
use ferridyn_core::types::{IndexDefinition, PartitionSchema, TableSchema};
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
    /// Partition schema list (LIST SCHEMAS).
    SchemaList(Vec<PartitionSchema>),
    /// Partition schema detail (DESCRIBE SCHEMA).
    SchemaDetail(PartitionSchema),
    /// Index list (LIST INDEXES).
    IndexList(Vec<IndexDefinition>),
    /// Index detail (DESCRIBE INDEX).
    IndexDetail(IndexDefinition),
    /// Help text (optional topic for per-command help).
    Help(Option<String>),
    /// Exit signal.
    Exit,
}

/// Execute a parsed command against the database.
pub fn execute(db: &FerridynDB, cmd: Command) -> Result<CommandResult, Error> {
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
        Command::CreateSchema {
            table,
            prefix,
            description,
            attributes,
            validate,
        } => exec_create_schema(db, &table, &prefix, description, attributes, validate),
        Command::DropSchema { table, prefix } => exec_drop_schema(db, &table, &prefix),
        Command::ListSchemas { table } => exec_list_schemas(db, &table),
        Command::DescribeSchema { table, prefix } => exec_describe_schema(db, &table, &prefix),
        Command::CreateIndex {
            table,
            name,
            schema_prefix,
            key_attr,
            key_type,
        } => exec_create_index(db, &table, &name, &schema_prefix, &key_attr, key_type),
        Command::DropIndex { table, name } => exec_drop_index(db, &table, &name),
        Command::ListIndexes { table } => exec_list_indexes(db, &table),
        Command::DescribeIndex { table, name } => exec_describe_index(db, &table, &name),
        Command::QueryIndex {
            table,
            index_name,
            key_value,
            limit,
            desc,
        } => exec_query_index(db, &table, &index_name, key_value, limit, desc),
        Command::Help(topic) => Ok(CommandResult::Help(topic)),
        Command::Exit => Ok(CommandResult::Exit),
    }
}

fn exec_create_table(
    db: &FerridynDB,
    name: &str,
    pk_name: &str,
    pk_type: ferridyn_core::types::KeyType,
    sk: Option<(String, ferridyn_core::types::KeyType)>,
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

fn exec_drop_table(db: &FerridynDB, name: &str) -> Result<CommandResult, Error> {
    db.drop_table(name)?;
    Ok(CommandResult::Ok(format!("Table '{name}' dropped.")))
}

fn exec_list_tables(db: &FerridynDB) -> Result<CommandResult, Error> {
    let tables = db.list_tables()?;
    Ok(CommandResult::TableList(tables))
}

fn exec_describe_table(db: &FerridynDB, name: &str) -> Result<CommandResult, Error> {
    let schema = db.describe_table(name)?;
    Ok(CommandResult::TableSchema(schema))
}

fn exec_put(db: &FerridynDB, table: &str, document: Value) -> Result<CommandResult, Error> {
    db.put_item(table, document)?;
    Ok(CommandResult::Ok("OK".to_string()))
}

fn exec_get(
    db: &FerridynDB,
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
    db: &FerridynDB,
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
    db: &FerridynDB,
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

fn exec_scan(db: &FerridynDB, table: &str, limit: Option<usize>) -> Result<CommandResult, Error> {
    let mut builder = db.scan(table);
    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    let result = builder.execute()?;
    Ok(CommandResult::QueryResult(result))
}

fn exec_list_keys(
    db: &FerridynDB,
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
    db: &FerridynDB,
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

fn exec_create_schema(
    db: &FerridynDB,
    table: &str,
    prefix: &str,
    description: Option<String>,
    attributes: Vec<(String, ferridyn_core::types::AttrType, bool)>,
    validate: bool,
) -> Result<CommandResult, Error> {
    let mut builder = db.create_partition_schema(table).prefix(prefix);
    if let Some(desc) = description {
        builder = builder.description(&desc);
    }
    for (name, attr_type, required) in attributes {
        builder = builder.attribute(&name, attr_type, required);
    }
    if validate {
        builder = builder.validate(true);
    }
    builder.execute()?;
    Ok(CommandResult::Ok(format!(
        "Partition schema '{prefix}' created on table '{table}'."
    )))
}

fn exec_drop_schema(db: &FerridynDB, table: &str, prefix: &str) -> Result<CommandResult, Error> {
    db.drop_partition_schema(table, prefix)?;
    Ok(CommandResult::Ok(format!(
        "Partition schema '{prefix}' dropped."
    )))
}

fn exec_list_schemas(db: &FerridynDB, table: &str) -> Result<CommandResult, Error> {
    let schemas = db.list_partition_schemas(table)?;
    Ok(CommandResult::SchemaList(schemas))
}

fn exec_describe_schema(
    db: &FerridynDB,
    table: &str,
    prefix: &str,
) -> Result<CommandResult, Error> {
    let schema = db.describe_partition_schema(table, prefix)?;
    Ok(CommandResult::SchemaDetail(schema))
}

fn exec_create_index(
    db: &FerridynDB,
    table: &str,
    name: &str,
    schema_prefix: &str,
    key_attr: &str,
    key_type: ferridyn_core::types::KeyType,
) -> Result<CommandResult, Error> {
    db.create_index(table)
        .name(name)
        .partition_schema(schema_prefix)
        .index_key(key_attr, key_type)
        .execute()?;
    Ok(CommandResult::Ok(format!(
        "Index '{name}' created on table '{table}'."
    )))
}

fn exec_drop_index(db: &FerridynDB, table: &str, name: &str) -> Result<CommandResult, Error> {
    db.drop_index(table, name)?;
    Ok(CommandResult::Ok(format!("Index '{name}' dropped.")))
}

fn exec_list_indexes(db: &FerridynDB, table: &str) -> Result<CommandResult, Error> {
    let indexes = db.list_indexes(table)?;
    Ok(CommandResult::IndexList(indexes))
}

fn exec_describe_index(db: &FerridynDB, table: &str, name: &str) -> Result<CommandResult, Error> {
    let index = db.describe_index(table, name)?;
    Ok(CommandResult::IndexDetail(index))
}

fn exec_query_index(
    db: &FerridynDB,
    table: &str,
    index_name: &str,
    key_value: Value,
    limit: Option<usize>,
    desc: bool,
) -> Result<CommandResult, Error> {
    let mut builder = db.query_index(table, index_name).key_value(key_value);
    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    if desc {
        builder = builder.scan_forward(false);
    }
    let result = builder.execute()?;
    Ok(CommandResult::QueryResult(result))
}
