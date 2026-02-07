use ferridyn_server::client::{
    AttributeDefInput, FerridynClient, IndexInfo, PartitionSchemaInfo, QueryResult, TableSchema,
    UpdateActionInput,
};
use ferridyn_server::error::ClientError;
use ferridyn_server::protocol::{KeyDef, SortKeyCondition};
use serde_json::Value;
use tokio::runtime::Runtime;

use crate::commands::{AttrType, Command, KeyType, SortClause, UpdateActionCmd};

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
    SchemaList(Vec<PartitionSchemaInfo>),
    /// Partition schema detail (DESCRIBE SCHEMA).
    SchemaDetail(PartitionSchemaInfo),
    /// Index list (LIST INDEXES).
    IndexList(Vec<IndexInfo>),
    /// Index detail (DESCRIBE INDEX).
    IndexDetail(IndexInfo),
    /// Active table changed (Some = set, None = cleared).
    Use(Option<String>),
    /// Help text (optional topic for per-command help).
    Help(Option<String>),
    /// Exit signal.
    Exit,
}

/// Execute a parsed command against the server via the client.
pub fn execute(
    client: &mut FerridynClient,
    rt: &Runtime,
    cmd: Command,
) -> Result<CommandResult, ClientError> {
    match cmd {
        Command::CreateTable {
            name,
            pk_name,
            pk_type,
            sk,
            ttl,
        } => exec_create_table(client, rt, &name, &pk_name, pk_type, sk, ttl.as_deref()),
        Command::DropTable { name } => exec_drop_table(client, rt, &name),
        Command::ListTables => exec_list_tables(client, rt),
        Command::DescribeTable { name } => exec_describe_table(client, rt, &name),
        Command::Put { table, document } => exec_put(client, rt, &table, document),
        Command::Get { table, pk, sk } => exec_get(client, rt, &table, pk, sk),
        Command::Delete { table, pk, sk } => exec_delete(client, rt, &table, pk, sk),
        Command::Query {
            table,
            pk,
            sort_condition,
            limit,
            desc,
        } => exec_query(client, rt, &table, pk, sort_condition, limit, desc),
        Command::Scan { table, limit } => exec_scan(client, rt, &table, limit),
        Command::ListKeys { table, limit } => exec_list_keys(client, rt, &table, limit),
        Command::ListPrefixes { table, pk, limit } => {
            exec_list_prefixes(client, rt, &table, pk, limit)
        }
        Command::CreateSchema {
            table,
            prefix,
            description,
            attributes,
            validate,
        } => exec_create_schema(
            client,
            rt,
            &table,
            &prefix,
            description,
            attributes,
            validate,
        ),
        Command::DropSchema { table, prefix } => exec_drop_schema(client, rt, &table, &prefix),
        Command::ListSchemas { table } => exec_list_schemas(client, rt, &table),
        Command::DescribeSchema { table, prefix } => {
            exec_describe_schema(client, rt, &table, &prefix)
        }
        Command::CreateIndex {
            table,
            name,
            schema_prefix,
            key_attr,
            key_type,
        } => exec_create_index(
            client,
            rt,
            &table,
            &name,
            &schema_prefix,
            &key_attr,
            key_type,
        ),
        Command::DropIndex { table, name } => exec_drop_index(client, rt, &table, &name),
        Command::ListIndexes { table } => exec_list_indexes(client, rt, &table),
        Command::DescribeIndex { table, name } => exec_describe_index(client, rt, &table, &name),
        Command::QueryIndex {
            table,
            index_name,
            key_value,
            limit,
            desc,
        } => exec_query_index(client, rt, &table, &index_name, key_value, limit, desc),
        Command::Update {
            table,
            pk,
            sk,
            actions,
        } => exec_update(client, rt, &table, pk, sk, actions),
        Command::Use { table } => Ok(CommandResult::Use(table)),
        Command::Help(topic) => Ok(CommandResult::Help(topic)),
        Command::Exit => Ok(CommandResult::Exit),
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

fn key_type_to_str(kt: KeyType) -> &'static str {
    match kt {
        KeyType::String => "String",
        KeyType::Number => "Number",
        KeyType::Binary => "Binary",
    }
}

fn attr_type_to_str(at: AttrType) -> &'static str {
    match at {
        AttrType::String => "String",
        AttrType::Number => "Number",
        AttrType::Boolean => "Boolean",
    }
}

fn sort_clause_to_condition(sc: SortClause) -> SortKeyCondition {
    match sc {
        SortClause::Eq(v) => SortKeyCondition::Eq { value: v },
        SortClause::Lt(v) => SortKeyCondition::Lt { value: v },
        SortClause::Le(v) => SortKeyCondition::Le { value: v },
        SortClause::Gt(v) => SortKeyCondition::Gt { value: v },
        SortClause::Ge(v) => SortKeyCondition::Ge { value: v },
        SortClause::Between(lo, hi) => SortKeyCondition::Between { low: lo, high: hi },
        SortClause::BeginsWith(prefix) => SortKeyCondition::BeginsWith { prefix },
    }
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

fn exec_create_table(
    client: &mut FerridynClient,
    rt: &Runtime,
    name: &str,
    pk_name: &str,
    pk_type: KeyType,
    sk: Option<(String, KeyType)>,
    ttl: Option<&str>,
) -> Result<CommandResult, ClientError> {
    let pk = KeyDef {
        name: pk_name.to_string(),
        key_type: key_type_to_str(pk_type).to_string(),
    };
    let sk_def = sk.map(|(n, t)| KeyDef {
        name: n,
        key_type: key_type_to_str(t).to_string(),
    });
    let ttl_attr = ttl.map(|s| s.to_string());
    rt.block_on(client.create_table(name, pk, sk_def, ttl_attr))?;
    Ok(CommandResult::Ok(format!("Table '{name}' created.")))
}

fn exec_drop_table(
    client: &mut FerridynClient,
    rt: &Runtime,
    name: &str,
) -> Result<CommandResult, ClientError> {
    rt.block_on(client.drop_table(name))?;
    Ok(CommandResult::Ok(format!("Table '{name}' dropped.")))
}

fn exec_list_tables(
    client: &mut FerridynClient,
    rt: &Runtime,
) -> Result<CommandResult, ClientError> {
    let tables = rt.block_on(client.list_tables())?;
    Ok(CommandResult::TableList(tables))
}

fn exec_describe_table(
    client: &mut FerridynClient,
    rt: &Runtime,
    name: &str,
) -> Result<CommandResult, ClientError> {
    let schema = rt.block_on(client.describe_table(name))?;
    Ok(CommandResult::TableSchema(schema))
}

fn exec_put(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    document: Value,
) -> Result<CommandResult, ClientError> {
    rt.block_on(client.put_item(table, document))?;
    Ok(CommandResult::Ok("OK".to_string()))
}

fn exec_get(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    pk: Value,
    sk: Option<Value>,
) -> Result<CommandResult, ClientError> {
    let item = rt.block_on(client.get_item(table, pk, sk))?;
    Ok(CommandResult::Item(item))
}

fn exec_delete(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    pk: Value,
    sk: Option<Value>,
) -> Result<CommandResult, ClientError> {
    rt.block_on(client.delete_item(table, pk, sk))?;
    Ok(CommandResult::Ok("OK".to_string()))
}

fn exec_update(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    pk: Value,
    sk: Option<Value>,
    actions: Vec<UpdateActionCmd>,
) -> Result<CommandResult, ClientError> {
    let updates: Vec<UpdateActionInput> = actions
        .into_iter()
        .map(|a| match a {
            UpdateActionCmd::Set(path, value) => UpdateActionInput {
                action: "set".to_string(),
                path,
                value: Some(value),
            },
            UpdateActionCmd::Remove(path) => UpdateActionInput {
                action: "remove".to_string(),
                path,
                value: None,
            },
            UpdateActionCmd::Add(path, value) => UpdateActionInput {
                action: "add".to_string(),
                path,
                value: Some(value),
            },
            UpdateActionCmd::Delete(path, value) => UpdateActionInput {
                action: "delete".to_string(),
                path,
                value: Some(value),
            },
        })
        .collect();
    rt.block_on(client.update_item(table, pk, sk, &updates))?;
    Ok(CommandResult::Ok("OK".to_string()))
}

fn exec_query(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    pk: Value,
    sort_condition: Option<SortClause>,
    limit: Option<usize>,
    desc: bool,
) -> Result<CommandResult, ClientError> {
    let cond = sort_condition.map(sort_clause_to_condition);
    let fwd = if desc { Some(false) } else { None };
    let result = rt.block_on(client.query(table, pk, cond, limit, fwd, None, None))?;
    Ok(CommandResult::QueryResult(result))
}

fn exec_scan(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    limit: Option<usize>,
) -> Result<CommandResult, ClientError> {
    let result = rt.block_on(client.scan(table, limit, None, None))?;
    Ok(CommandResult::QueryResult(result))
}

fn exec_list_keys(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    limit: Option<usize>,
) -> Result<CommandResult, ClientError> {
    let keys = rt.block_on(client.list_partition_keys(table, limit))?;
    Ok(CommandResult::PartitionKeys(keys))
}

fn exec_list_prefixes(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    pk: Value,
    limit: Option<usize>,
) -> Result<CommandResult, ClientError> {
    let prefixes = rt.block_on(client.list_sort_key_prefixes(table, pk, limit))?;
    Ok(CommandResult::SortKeyPrefixes(prefixes))
}

fn exec_create_schema(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    prefix: &str,
    description: Option<String>,
    attributes: Vec<(String, AttrType, bool)>,
    validate: bool,
) -> Result<CommandResult, ClientError> {
    let attrs: Vec<AttributeDefInput> = attributes
        .into_iter()
        .map(|(name, at, req)| AttributeDefInput {
            name,
            attr_type: attr_type_to_str(at).to_string(),
            required: req,
        })
        .collect();
    rt.block_on(client.create_schema(table, prefix, description.as_deref(), &attrs, validate))?;
    Ok(CommandResult::Ok(format!(
        "Partition schema '{prefix}' created on table '{table}'."
    )))
}

fn exec_drop_schema(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    prefix: &str,
) -> Result<CommandResult, ClientError> {
    rt.block_on(client.drop_schema(table, prefix))?;
    Ok(CommandResult::Ok(format!(
        "Partition schema '{prefix}' dropped."
    )))
}

fn exec_list_schemas(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
) -> Result<CommandResult, ClientError> {
    let schemas = rt.block_on(client.list_schemas(table))?;
    Ok(CommandResult::SchemaList(schemas))
}

fn exec_describe_schema(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    prefix: &str,
) -> Result<CommandResult, ClientError> {
    let schema = rt.block_on(client.describe_schema(table, prefix))?;
    Ok(CommandResult::SchemaDetail(schema))
}

fn exec_create_index(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    name: &str,
    schema_prefix: &str,
    key_attr: &str,
    key_type: KeyType,
) -> Result<CommandResult, ClientError> {
    rt.block_on(client.create_index(
        table,
        name,
        schema_prefix,
        key_attr,
        key_type_to_str(key_type),
    ))?;
    Ok(CommandResult::Ok(format!(
        "Index '{name}' created on table '{table}'."
    )))
}

fn exec_drop_index(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    name: &str,
) -> Result<CommandResult, ClientError> {
    rt.block_on(client.drop_index(table, name))?;
    Ok(CommandResult::Ok(format!("Index '{name}' dropped.")))
}

fn exec_list_indexes(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
) -> Result<CommandResult, ClientError> {
    let indexes = rt.block_on(client.list_indexes(table))?;
    Ok(CommandResult::IndexList(indexes))
}

fn exec_describe_index(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    name: &str,
) -> Result<CommandResult, ClientError> {
    let index = rt.block_on(client.describe_index(table, name))?;
    Ok(CommandResult::IndexDetail(index))
}

fn exec_query_index(
    client: &mut FerridynClient,
    rt: &Runtime,
    table: &str,
    index_name: &str,
    key_value: Value,
    limit: Option<usize>,
    desc: bool,
) -> Result<CommandResult, ClientError> {
    let fwd = if desc { Some(false) } else { None };
    let result =
        rt.block_on(client.query_index(table, index_name, key_value, limit, fwd, None, None))?;
    Ok(CommandResult::QueryResult(result))
}
