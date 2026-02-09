//! Unix domain socket server that wraps a `FerridynDB` database handle.
//!
//! Each connected client sends JSON-line requests and receives JSON-line
//! responses. Reads are concurrent (via `RwLock::read`), writes are
//! serialized (via `RwLock::write`) — the lock is internal to `FerridynDB`.

use std::path::PathBuf;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tracing::{error, info, warn};

use ferridyn_core::api::{FerridynDB, FilterExpr};
use ferridyn_core::error::{Error as DynError, QueryError, SchemaError, TxnError};
use ferridyn_core::types::{AttrType, IndexDefinition, KeyType, PartitionSchema, TableSchema};

use crate::protocol::{
    AttributeDefWire, BatchGetItemKey, BatchWriteOp, IndexDefWire, KeyDef, KeyDefWire,
    PartitionSchemaWire, Request, Response, SortKeyCondition, StreamInfoWire, StreamRecordWire,
    TableSchemaWire, UpdateActionWire,
};

/// A FerridynDB server listening on a Unix socket.
pub struct FerridynServer {
    db: FerridynDB,
    socket_path: PathBuf,
}

impl FerridynServer {
    pub fn new(db: FerridynDB, socket_path: PathBuf) -> Self {
        Self { db, socket_path }
    }

    /// Run the server, accepting connections until a shutdown signal is received.
    ///
    /// On startup, removes any stale socket file and binds a new one.
    /// On shutdown (SIGINT or SIGTERM), removes the socket file before exiting.
    pub async fn run(&self) -> std::io::Result<()> {
        // Remove stale socket file if it exists.
        if self.socket_path.exists() {
            std::fs::remove_file(&self.socket_path)?;
        }

        let listener = UnixListener::bind(&self.socket_path)?;
        info!(path = %self.socket_path.display(), "server listening");

        let accept_loop = async {
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => {
                        let db = self.db.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(db, stream).await {
                                warn!(error = %e, "connection handler error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "accept error");
                    }
                }
            }
        };

        // Wait for either the accept loop (runs forever) or a shutdown signal.
        tokio::select! {
            _ = accept_loop => {}
            _ = shutdown_signal() => {
                info!("shutdown signal received");
            }
        }

        // Clean up the socket file.
        if self.socket_path.exists() {
            if let Err(e) = std::fs::remove_file(&self.socket_path) {
                warn!(error = %e, "failed to remove socket file on shutdown");
            } else {
                info!(path = %self.socket_path.display(), "socket file removed");
            }
        }

        Ok(())
    }
}

async fn handle_connection(db: FerridynDB, stream: tokio::net::UnixStream) -> std::io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            // Client disconnected.
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<Request>(trimmed) {
            Ok(req) => dispatch(&db, req),
            Err(e) => Response::error("ParseError", e.to_string()),
        };

        let mut resp_bytes = serde_json::to_vec(&response).unwrap_or_else(|e| {
            let fallback = Response::error("SerializationError", e.to_string());
            serde_json::to_vec(&fallback).unwrap()
        });
        resp_bytes.push(b'\n');

        writer.write_all(&resp_bytes).await?;
        writer.flush().await?;
    }

    Ok(())
}

fn dispatch(db: &FerridynDB, req: Request) -> Response {
    match req {
        Request::GetItem {
            table,
            partition_key,
            sort_key,
            projection,
        } => handle_get_item(db, &table, partition_key, sort_key, projection),

        Request::GetItemVersioned {
            table,
            partition_key,
            sort_key,
        } => handle_get_item_versioned(db, &table, partition_key, sort_key),

        Request::PutItem {
            table,
            item,
            expected_version,
            condition,
            return_values,
        } => handle_put_item(db, &table, item, expected_version, condition, return_values),

        Request::DeleteItem {
            table,
            partition_key,
            sort_key,
            condition,
            return_values,
        } => handle_delete_item(
            db,
            &table,
            partition_key,
            sort_key,
            condition,
            return_values,
        ),

        Request::UpdateItem {
            table,
            partition_key,
            sort_key,
            updates,
            condition,
            return_values,
        } => handle_update_item(
            db,
            &table,
            partition_key,
            sort_key,
            updates,
            condition,
            return_values,
        ),

        Request::Query {
            table,
            partition_key,
            sort_key_condition,
            limit,
            scan_forward,
            exclusive_start_key,
            filter,
            projection,
        } => handle_query(
            db,
            &table,
            partition_key,
            sort_key_condition,
            limit,
            scan_forward,
            exclusive_start_key,
            filter,
            projection,
        ),

        Request::Scan {
            table,
            limit,
            exclusive_start_key,
            filter,
            projection,
        } => handle_scan(db, &table, limit, exclusive_start_key, filter, projection),

        Request::CreateTable {
            table,
            partition_key,
            sort_key,
            ttl_attribute,
        } => handle_create_table(db, &table, partition_key, sort_key, ttl_attribute),

        Request::DropTable { table } => handle_drop_table(db, &table),

        Request::ListTables => handle_list_tables(db),

        Request::DescribeTable { table } => handle_describe_table(db, &table),

        Request::ListPartitionKeys { table, limit } => {
            handle_list_partition_keys(db, &table, limit)
        }

        Request::ListSortKeyPrefixes {
            table,
            partition_key,
            limit,
        } => handle_list_sort_key_prefixes(db, &table, partition_key, limit),

        Request::CreateSchema {
            table,
            prefix,
            description,
            attributes,
            validate,
        } => handle_create_schema(db, &table, &prefix, description, attributes, validate),

        Request::DropSchema { table, prefix } => handle_drop_schema(db, &table, &prefix),

        Request::ListSchemas { table } => handle_list_schemas(db, &table),

        Request::DescribeSchema { table, prefix } => handle_describe_schema(db, &table, &prefix),

        Request::CreateIndex {
            table,
            name,
            partition_schema,
            index_key,
            index_sort_key,
            projection_type,
            projection_attributes,
            is_local,
        } => handle_create_index(
            db,
            &table,
            &name,
            partition_schema.as_deref(),
            index_key,
            index_sort_key,
            projection_type.as_deref(),
            projection_attributes,
            is_local.unwrap_or(false),
        ),

        Request::DropIndex { table, name } => handle_drop_index(db, &table, &name),

        Request::ListIndexes { table } => handle_list_indexes(db, &table),

        Request::DescribeIndex { table, name } => handle_describe_index(db, &table, &name),

        Request::QueryIndex {
            table,
            index_name,
            key_value,
            sort_key_condition,
            limit,
            scan_forward,
            filter,
            exclusive_start_key,
            projection,
        } => handle_query_index(
            db,
            &table,
            &index_name,
            key_value,
            sort_key_condition,
            limit,
            scan_forward,
            filter,
            exclusive_start_key,
            projection,
        ),

        Request::BatchGetItem {
            table,
            keys,
            projection,
        } => handle_batch_get_item(db, &table, keys, projection),

        Request::BatchWriteItem { operations } => handle_batch_write_item(db, operations),

        Request::EnableStream { table, view_type } => handle_enable_stream(db, &table, &view_type),

        Request::DisableStream { table } => handle_disable_stream(db, &table),

        Request::GetStreamRecords {
            table,
            after_sequence,
            limit,
        } => handle_get_stream_records(db, &table, after_sequence, limit),

        Request::GetStreamInfo { table } => handle_get_stream_info(db, &table),

        Request::PruneStream { table } => handle_prune_stream(db, &table),

        Request::SetTtl {
            table,
            partition_key,
            sort_key,
            ttl_seconds,
        } => handle_set_ttl(db, &table, partition_key, sort_key, ttl_seconds),

        Request::RemoveTtl {
            table,
            partition_key,
            sort_key,
        } => handle_remove_ttl(db, &table, partition_key, sort_key),

        Request::GetTtl {
            table,
            partition_key,
            sort_key,
        } => handle_get_ttl(db, &table, partition_key, sort_key),

        Request::SweepExpiredTtl { table } => handle_sweep_expired_ttl(db, &table),

        Request::Count {
            table,
            partition_key,
            sort_key_condition,
            filter,
        } => handle_count(db, &table, partition_key, sort_key_condition, filter),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn handle_get_item(
    db: &FerridynDB,
    table: &str,
    partition_key: serde_json::Value,
    sort_key: Option<serde_json::Value>,
    projection: Option<Vec<String>>,
) -> Response {
    let mut builder = db.get_item(table).partition_key(partition_key);
    if let Some(sk) = sort_key {
        builder = builder.sort_key(sk);
    }
    if let Some(ref paths) = projection {
        let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        builder = builder.projection(&refs);
    }
    match builder.execute() {
        Ok(item) => Response::ok_item(item),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_get_item_versioned(
    db: &FerridynDB,
    table: &str,
    partition_key: serde_json::Value,
    sort_key: Option<serde_json::Value>,
) -> Response {
    let mut builder = db.get_item_versioned(table).partition_key(partition_key);
    if let Some(sk) = sort_key {
        builder = builder.sort_key(sk);
    }
    match builder.execute() {
        Ok(Some(vi)) => Response::ok_versioned_item(Some(vi.item), Some(vi.version)),
        Ok(None) => Response::ok_versioned_item(None, None),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_put_item(
    db: &FerridynDB,
    table: &str,
    item: serde_json::Value,
    expected_version: Option<u64>,
    condition: Option<FilterExpr>,
    return_values: Option<String>,
) -> Response {
    let want_old = matches!(
        return_values.as_deref(),
        Some("ALL_OLD" | "all_old" | "ALLOLD")
    );

    if let Some(ev) = expected_version {
        // Conditional put does not support return_values.
        match db.put_item_conditional(table, item, ev) {
            Ok(()) => Response::ok_empty(),
            Err(e) => dyn_error_to_response(e),
        }
    } else if want_old {
        let mut builder = db.put(table, item);
        if let Some(cond) = condition {
            builder = builder.condition(cond);
        }
        match builder.return_old().execute() {
            Ok(old) => Response::ok_item(old),
            Err(e) => dyn_error_to_response(e),
        }
    } else {
        let mut builder = db.put(table, item);
        if let Some(cond) = condition {
            builder = builder.condition(cond);
        }
        match builder.execute() {
            Ok(()) => Response::ok_empty(),
            Err(e) => dyn_error_to_response(e),
        }
    }
}

fn handle_delete_item(
    db: &FerridynDB,
    table: &str,
    partition_key: serde_json::Value,
    sort_key: Option<serde_json::Value>,
    condition: Option<FilterExpr>,
    return_values: Option<String>,
) -> Response {
    let want_old = matches!(
        return_values.as_deref(),
        Some("ALL_OLD" | "all_old" | "ALLOLD")
    );

    if want_old {
        let mut builder = db.delete_item(table).partition_key(partition_key);
        if let Some(sk) = sort_key {
            builder = builder.sort_key(sk);
        }
        if let Some(cond) = condition {
            builder = builder.condition(cond);
        }
        match builder.return_old().execute() {
            Ok(old) => Response::ok_item(old),
            Err(e) => dyn_error_to_response(e),
        }
    } else {
        let mut builder = db.delete_item(table).partition_key(partition_key);
        if let Some(sk) = sort_key {
            builder = builder.sort_key(sk);
        }
        if let Some(cond) = condition {
            builder = builder.condition(cond);
        }
        match builder.execute() {
            Ok(()) => Response::ok_empty(),
            Err(e) => dyn_error_to_response(e),
        }
    }
}

fn handle_update_item(
    db: &FerridynDB,
    table: &str,
    partition_key: serde_json::Value,
    sort_key: Option<serde_json::Value>,
    updates: Vec<UpdateActionWire>,
    condition: Option<FilterExpr>,
    return_values: Option<String>,
) -> Response {
    let want_old = matches!(
        return_values.as_deref(),
        Some("ALL_OLD" | "all_old" | "ALLOLD")
    );
    let want_new = matches!(
        return_values.as_deref(),
        Some("ALL_NEW" | "all_new" | "ALLNEW")
    );

    // Build the common update actions on a NoReturn builder first.
    let mut builder = db.update_item(table).partition_key(partition_key);
    if let Some(sk) = sort_key {
        builder = builder.sort_key(sk);
    }
    for update in &updates {
        match update.action.as_str() {
            "set" => {
                let value = match &update.value {
                    Some(v) => v.clone(),
                    None => {
                        return Response::error(
                            "InvalidUpdateAction",
                            "SET requires a value".to_string(),
                        );
                    }
                };
                builder = builder.set(&update.path, value);
            }
            "remove" => {
                builder = builder.remove(&update.path);
            }
            "add" => {
                let value = match &update.value {
                    Some(v) => v.clone(),
                    None => {
                        return Response::error(
                            "InvalidUpdateAction",
                            "ADD requires a value".to_string(),
                        );
                    }
                };
                builder = builder.add(&update.path, value);
            }
            "delete" => {
                let value = match &update.value {
                    Some(v) => v.clone(),
                    None => {
                        return Response::error(
                            "InvalidUpdateAction",
                            "DELETE requires a value".to_string(),
                        );
                    }
                };
                builder = builder.delete(&update.path, value);
            }
            other => {
                return Response::error("InvalidUpdateAction", format!("unknown action: {other}"));
            }
        }
    }
    if let Some(cond) = condition {
        builder = builder.condition(cond);
    }

    if want_old {
        match builder.return_old().execute() {
            Ok(old) => Response::ok_item(old),
            Err(e) => dyn_error_to_response(e),
        }
    } else if want_new {
        match builder.return_new().execute() {
            Ok(new_doc) => Response::ok_item(new_doc),
            Err(e) => dyn_error_to_response(e),
        }
    } else {
        match builder.execute() {
            Ok(()) => Response::ok_empty(),
            Err(e) => dyn_error_to_response(e),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_query(
    db: &FerridynDB,
    table: &str,
    partition_key: serde_json::Value,
    sort_key_condition: Option<SortKeyCondition>,
    limit: Option<usize>,
    scan_forward: Option<bool>,
    exclusive_start_key: Option<serde_json::Value>,
    filter: Option<FilterExpr>,
    projection: Option<Vec<String>>,
) -> Response {
    let mut builder = db.query(table).partition_key(partition_key);

    if let Some(cond) = sort_key_condition {
        builder = match cond {
            SortKeyCondition::Eq { value } => builder.sort_key_eq(value),
            SortKeyCondition::Lt { value } => builder.sort_key_lt(value),
            SortKeyCondition::Le { value } => builder.sort_key_le(value),
            SortKeyCondition::Gt { value } => builder.sort_key_gt(value),
            SortKeyCondition::Ge { value } => builder.sort_key_ge(value),
            SortKeyCondition::Between { low, high } => builder.sort_key_between(low, high),
            SortKeyCondition::BeginsWith { prefix } => builder.sort_key_begins_with(&prefix),
        };
    }

    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    if let Some(forward) = scan_forward {
        builder = builder.scan_forward(forward);
    }
    if let Some(esk) = exclusive_start_key {
        builder = builder.exclusive_start_key(esk);
    }
    if let Some(f) = filter {
        builder = builder.filter(f);
    }
    if let Some(ref paths) = projection {
        let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        builder = builder.projection(&refs);
    }

    match builder.execute() {
        Ok(result) => Response::ok_items(result.items, result.last_evaluated_key),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_scan(
    db: &FerridynDB,
    table: &str,
    limit: Option<usize>,
    exclusive_start_key: Option<serde_json::Value>,
    filter: Option<FilterExpr>,
    projection: Option<Vec<String>>,
) -> Response {
    let mut builder = db.scan(table);
    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    if let Some(esk) = exclusive_start_key {
        builder = builder.exclusive_start_key(esk);
    }
    if let Some(f) = filter {
        builder = builder.filter(f);
    }
    if let Some(ref paths) = projection {
        let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        builder = builder.projection(&refs);
    }
    match builder.execute() {
        Ok(result) => Response::ok_items(result.items, result.last_evaluated_key),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_create_table(
    db: &FerridynDB,
    table: &str,
    partition_key: KeyDef,
    sort_key: Option<KeyDef>,
    ttl_attribute: Option<String>,
) -> Response {
    let pk_type = match parse_key_type(&partition_key.key_type) {
        Some(t) => t,
        None => {
            return Response::error(
                "InvalidKeyType",
                format!("unknown partition key type: {}", partition_key.key_type),
            );
        }
    };

    let mut builder = db
        .create_table(table)
        .partition_key(&partition_key.name, pk_type);

    if let Some(sk) = sort_key {
        let sk_type = match parse_key_type(&sk.key_type) {
            Some(t) => t,
            None => {
                return Response::error(
                    "InvalidKeyType",
                    format!("unknown sort key type: {}", sk.key_type),
                );
            }
        };
        builder = builder.sort_key(&sk.name, sk_type);
    }

    if let Some(ttl) = ttl_attribute {
        builder = builder.ttl_attribute(&ttl);
    }

    match builder.execute() {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_drop_table(db: &FerridynDB, table: &str) -> Response {
    match db.drop_table(table) {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_list_tables(db: &FerridynDB) -> Response {
    match db.list_tables() {
        Ok(tables) => Response::ok_tables(tables),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_describe_table(db: &FerridynDB, table: &str) -> Response {
    match db.describe_table(table) {
        Ok(schema) => Response::ok_schema(schema_to_wire(&schema)),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_list_partition_keys(db: &FerridynDB, table: &str, limit: Option<usize>) -> Response {
    let mut builder = db.list_partition_keys(table);
    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    match builder.execute() {
        Ok(keys) => Response::ok_keys(keys),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_list_sort_key_prefixes(
    db: &FerridynDB,
    table: &str,
    partition_key: serde_json::Value,
    limit: Option<usize>,
) -> Response {
    let mut builder = db
        .list_sort_key_prefixes(table)
        .partition_key(partition_key);
    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    match builder.execute() {
        Ok(keys) => Response::ok_keys(keys),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_create_schema(
    db: &FerridynDB,
    table: &str,
    prefix: &str,
    description: Option<String>,
    attributes: Vec<AttributeDefWire>,
    validate: bool,
) -> Response {
    let mut builder = db.create_partition_schema(table).prefix(prefix);
    if let Some(desc) = description {
        builder = builder.description(&desc);
    }
    for attr in attributes {
        let attr_type = match parse_attr_type(&attr.attr_type) {
            Some(t) => t,
            None => {
                return Response::error(
                    "InvalidAttrType",
                    format!("unknown attribute type: {}", attr.attr_type),
                );
            }
        };
        builder = builder.attribute(&attr.name, attr_type, attr.required);
    }
    if validate {
        builder = builder.validate(true);
    }
    match builder.execute() {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_drop_schema(db: &FerridynDB, table: &str, prefix: &str) -> Response {
    match db.drop_partition_schema(table, prefix) {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_list_schemas(db: &FerridynDB, table: &str) -> Response {
    match db.list_partition_schemas(table) {
        Ok(schemas) => {
            Response::ok_partition_schemas(schemas.iter().map(partition_schema_to_wire).collect())
        }
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_describe_schema(db: &FerridynDB, table: &str, prefix: &str) -> Response {
    match db.describe_partition_schema(table, prefix) {
        Ok(schema) => Response::ok_partition_schema(partition_schema_to_wire(&schema)),
        Err(e) => dyn_error_to_response(e),
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_create_index(
    db: &FerridynDB,
    table: &str,
    name: &str,
    partition_schema: Option<&str>,
    index_key: Option<KeyDef>,
    index_sort_key: Option<KeyDef>,
    projection_type: Option<&str>,
    projection_attributes: Option<Vec<String>>,
    is_local: bool,
) -> Response {
    let mut builder = db.create_index(table).name(name);
    if is_local {
        builder = builder.local();
    } else {
        let ik = match index_key {
            Some(k) => k,
            None => {
                return Response::error("InvalidRequest", "index_key is required for GSI");
            }
        };
        let key_type = match parse_key_type(&ik.key_type) {
            Some(t) => t,
            None => {
                return Response::error(
                    "InvalidKeyType",
                    format!("unknown key type: {}", ik.key_type),
                );
            }
        };
        builder = builder.index_key(&ik.name, key_type);
    }
    if let Some(ps) = partition_schema {
        builder = builder.partition_schema(ps);
    }
    if let Some(sk) = index_sort_key {
        let sk_type = match parse_key_type(&sk.key_type) {
            Some(t) => t,
            None => {
                return Response::error(
                    "InvalidKeyType",
                    format!("unknown index sort key type: {}", sk.key_type),
                );
            }
        };
        builder = builder.index_sort_key(&sk.name, sk_type);
    }
    // Parse index projection type.
    let projection = match projection_type {
        Some("ALL") => ferridyn_core::types::IndexProjection::All,
        Some("INCLUDE") => ferridyn_core::types::IndexProjection::Include(
            projection_attributes.unwrap_or_default(),
        ),
        Some("KEYS_ONLY") | None => ferridyn_core::types::IndexProjection::KeysOnly,
        Some(other) => {
            return Response::error(
                "InvalidProjectionType",
                format!("unknown projection type: {other}. Expected KEYS_ONLY, INCLUDE, or ALL"),
            );
        }
    };
    builder = builder.projection_type(projection);
    match builder.execute() {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_drop_index(db: &FerridynDB, table: &str, name: &str) -> Response {
    match db.drop_index(table, name) {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_list_indexes(db: &FerridynDB, table: &str) -> Response {
    match db.list_indexes(table) {
        Ok(indexes) => Response::ok_indexes(indexes.iter().map(index_to_wire).collect()),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_describe_index(db: &FerridynDB, table: &str, name: &str) -> Response {
    match db.describe_index(table, name) {
        Ok(index) => Response::ok_index(index_to_wire(&index)),
        Err(e) => dyn_error_to_response(e),
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_query_index(
    db: &FerridynDB,
    table: &str,
    index_name: &str,
    key_value: serde_json::Value,
    sort_key_condition: Option<SortKeyCondition>,
    limit: Option<usize>,
    scan_forward: Option<bool>,
    filter: Option<FilterExpr>,
    exclusive_start_key: Option<serde_json::Value>,
    projection: Option<Vec<String>>,
) -> Response {
    let mut builder = db.query_index(table, index_name).key_value(key_value);

    if let Some(cond) = sort_key_condition {
        builder = match cond {
            SortKeyCondition::Eq { value } => builder.sort_key_eq(value),
            SortKeyCondition::Lt { value } => builder.sort_key_lt(value),
            SortKeyCondition::Le { value } => builder.sort_key_le(value),
            SortKeyCondition::Gt { value } => builder.sort_key_gt(value),
            SortKeyCondition::Ge { value } => builder.sort_key_ge(value),
            SortKeyCondition::Between { low, high } => builder.sort_key_between(low, high),
            SortKeyCondition::BeginsWith { prefix } => builder.sort_key_begins_with(&prefix),
        };
    }

    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    if let Some(fwd) = scan_forward {
        builder = builder.scan_forward(fwd);
    }
    if let Some(f) = filter {
        builder = builder.filter(f);
    }
    if let Some(esk) = exclusive_start_key {
        builder = builder.exclusive_start_key(esk);
    }
    if let Some(ref paths) = projection {
        let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        builder = builder.projection(&refs);
    }
    match builder.execute() {
        Ok(result) => Response::ok_items(result.items, result.last_evaluated_key),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_batch_get_item(
    db: &FerridynDB,
    table: &str,
    keys: Vec<BatchGetItemKey>,
    projection: Option<Vec<String>>,
) -> Response {
    const MAX_BATCH_SIZE: usize = 1000;
    if keys.len() > MAX_BATCH_SIZE {
        return Response::error(
            "BatchSizeLimitExceeded",
            format!(
                "batch size {} exceeds limit of {MAX_BATCH_SIZE}",
                keys.len()
            ),
        );
    }
    let mut builder = db.batch_get_item(table);
    for k in keys {
        builder = builder.key(k.partition_key, k.sort_key);
    }
    if let Some(ref paths) = projection {
        let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        builder = builder.projection(&refs);
    }
    match builder.execute() {
        Ok(items) => Response::ok_batch_items(items),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_batch_write_item(db: &FerridynDB, operations: Vec<BatchWriteOp>) -> Response {
    const MAX_BATCH_SIZE: usize = 25;
    if operations.len() > MAX_BATCH_SIZE {
        return Response::error(
            "BatchSizeLimitExceeded",
            format!(
                "batch size {} exceeds limit of {MAX_BATCH_SIZE}",
                operations.len()
            ),
        );
    }
    let count = operations.len();
    let mut batch = db.write_batch();
    for op in operations {
        match op {
            BatchWriteOp::Put { table, item } => {
                batch.put_item(&table, item);
            }
            BatchWriteOp::Delete {
                table,
                partition_key,
                sort_key,
            } => {
                batch.delete_item(&table, partition_key, sort_key);
            }
        }
    }
    match batch.commit() {
        Ok(()) => Response::ok_succeeded(count),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_enable_stream(db: &FerridynDB, table: &str, view_type: &str) -> Response {
    let vt = match parse_stream_view_type(view_type) {
        Some(vt) => vt,
        None => {
            return Response::error(
                "InvalidStreamViewType",
                format!(
                    "unknown view type: {view_type}. Expected KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, or NEW_AND_OLD_IMAGES"
                ),
            );
        }
    };
    match db.enable_stream(table, vt) {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_disable_stream(db: &FerridynDB, table: &str) -> Response {
    match db.disable_stream(table) {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_get_stream_records(
    db: &FerridynDB,
    table: &str,
    after_sequence: Option<u64>,
    limit: Option<usize>,
) -> Response {
    let mut builder = db.get_stream_records(table);
    if let Some(seq) = after_sequence {
        builder = builder.after_sequence(seq);
    }
    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    match builder.execute() {
        Ok(records) => {
            let wire_records: Vec<StreamRecordWire> =
                records.iter().map(stream_record_to_wire).collect();
            Response::ok_stream_records(wire_records)
        }
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_get_stream_info(db: &FerridynDB, table: &str) -> Response {
    match db.get_stream_info(table) {
        Ok(info) => Response::ok_stream_info(stream_info_to_wire(&info)),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_prune_stream(db: &FerridynDB, table: &str) -> Response {
    match db.prune_stream(table) {
        Ok(_pruned) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_set_ttl(
    db: &FerridynDB,
    table: &str,
    partition_key: serde_json::Value,
    sort_key: Option<serde_json::Value>,
    ttl_seconds: u64,
) -> Response {
    match db.set_ttl(table, partition_key, sort_key, ttl_seconds) {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_remove_ttl(
    db: &FerridynDB,
    table: &str,
    partition_key: serde_json::Value,
    sort_key: Option<serde_json::Value>,
) -> Response {
    match db.remove_ttl(table, partition_key, sort_key) {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_get_ttl(
    db: &FerridynDB,
    table: &str,
    partition_key: serde_json::Value,
    sort_key: Option<serde_json::Value>,
) -> Response {
    match db.get_ttl(table, partition_key, sort_key) {
        Ok(remaining) => Response::ok_ttl(remaining),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_sweep_expired_ttl(db: &FerridynDB, table: &str) -> Response {
    match db.sweep_expired_ttl(table) {
        Ok(count) => Response::ok_succeeded(count),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_count(
    db: &FerridynDB,
    table: &str,
    partition_key: serde_json::Value,
    sort_key_condition: Option<SortKeyCondition>,
    filter: Option<FilterExpr>,
) -> Response {
    let mut builder = db.count(table).partition_key(partition_key);

    if let Some(cond) = sort_key_condition {
        builder = match cond {
            SortKeyCondition::Eq { value } => builder.sort_key_eq(value),
            SortKeyCondition::Lt { value } => builder.sort_key_lt(value),
            SortKeyCondition::Le { value } => builder.sort_key_le(value),
            SortKeyCondition::Gt { value } => builder.sort_key_gt(value),
            SortKeyCondition::Ge { value } => builder.sort_key_ge(value),
            SortKeyCondition::Between { low, high } => builder.sort_key_between(low, high),
            SortKeyCondition::BeginsWith { prefix } => builder.sort_key_begins_with(&prefix),
        };
    }

    if let Some(f) = filter {
        builder = builder.filter(f);
    }

    match builder.execute() {
        Ok(count) => Response::ok_count(count),
        Err(e) => dyn_error_to_response(e),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_stream_view_type(s: &str) -> Option<ferridyn_core::stream::StreamViewType> {
    use ferridyn_core::stream::StreamViewType;
    match s.to_uppercase().as_str() {
        "KEYS_ONLY" | "KEYSONLY" => Some(StreamViewType::KeysOnly),
        "NEW_IMAGE" | "NEWIMAGE" => Some(StreamViewType::NewImage),
        "OLD_IMAGE" | "OLDIMAGE" => Some(StreamViewType::OldImage),
        "NEW_AND_OLD_IMAGES" | "NEWANDOLDIMAGES" => Some(StreamViewType::NewAndOldImages),
        _ => None,
    }
}

fn stream_view_type_str(vt: ferridyn_core::stream::StreamViewType) -> &'static str {
    use ferridyn_core::stream::StreamViewType;
    match vt {
        StreamViewType::KeysOnly => "KEYS_ONLY",
        StreamViewType::NewImage => "NEW_IMAGE",
        StreamViewType::OldImage => "OLD_IMAGE",
        StreamViewType::NewAndOldImages => "NEW_AND_OLD_IMAGES",
    }
}

fn event_type_str(et: ferridyn_core::stream::EventType) -> &'static str {
    use ferridyn_core::stream::EventType;
    match et {
        EventType::Insert => "INSERT",
        EventType::Modify => "MODIFY",
        EventType::Remove => "REMOVE",
    }
}

fn stream_record_to_wire(record: &ferridyn_core::stream::StreamRecord) -> StreamRecordWire {
    StreamRecordWire {
        sequence_number: record.sequence_number,
        sub_sequence: record.sub_sequence,
        event_type: event_type_str(record.event_type).to_string(),
        keys: record.keys.clone(),
        timestamp: record.timestamp,
        new_image: record.new_image.clone(),
        old_image: record.old_image.clone(),
    }
}

fn stream_info_to_wire(info: &ferridyn_core::stream::StreamInfo) -> StreamInfoWire {
    StreamInfoWire {
        enabled: info.enabled,
        view_type: stream_view_type_str(info.view_type).to_string(),
        oldest_sequence: info.oldest_sequence,
        latest_sequence: info.latest_sequence,
        record_count: info.record_count,
    }
}

fn parse_key_type(s: &str) -> Option<KeyType> {
    match s.to_lowercase().as_str() {
        "string" | "s" => Some(KeyType::String),
        "number" | "n" => Some(KeyType::Number),
        "binary" | "b" => Some(KeyType::Binary),
        _ => None,
    }
}

fn key_type_str(kt: KeyType) -> &'static str {
    match kt {
        KeyType::String => "String",
        KeyType::Number => "Number",
        KeyType::Binary => "Binary",
    }
}

fn parse_attr_type(s: &str) -> Option<AttrType> {
    match s.to_lowercase().as_str() {
        "string" | "s" => Some(AttrType::String),
        "number" | "n" => Some(AttrType::Number),
        "boolean" | "bool" | "b" => Some(AttrType::Boolean),
        _ => None,
    }
}

fn attr_type_str(at: AttrType) -> &'static str {
    match at {
        AttrType::String => "String",
        AttrType::Number => "Number",
        AttrType::Boolean => "Boolean",
    }
}

fn partition_schema_to_wire(schema: &PartitionSchema) -> PartitionSchemaWire {
    PartitionSchemaWire {
        prefix: schema.prefix.clone(),
        description: schema.description.clone(),
        attributes: schema
            .attributes
            .iter()
            .map(|a| AttributeDefWire {
                name: a.name.clone(),
                attr_type: attr_type_str(a.attr_type).to_string(),
                required: a.required,
            })
            .collect(),
        validate: schema.validate,
    }
}

fn index_to_wire(index: &IndexDefinition) -> IndexDefWire {
    let (proj_type, proj_attrs) = match &index.projection {
        ferridyn_core::types::IndexProjection::KeysOnly => ("KEYS_ONLY".to_string(), None),
        ferridyn_core::types::IndexProjection::Include(attrs) => {
            ("INCLUDE".to_string(), Some(attrs.clone()))
        }
        ferridyn_core::types::IndexProjection::All => ("ALL".to_string(), None),
    };
    IndexDefWire {
        name: index.name.clone(),
        partition_schema: index.partition_schema.clone(),
        index_key: KeyDefWire {
            name: index.index_key.name.clone(),
            key_type: key_type_str(index.index_key.key_type).to_string(),
        },
        index_sort_key: index.index_sort_key.as_ref().map(|sk| KeyDefWire {
            name: sk.name.clone(),
            key_type: key_type_str(sk.key_type).to_string(),
        }),
        projection_type: proj_type,
        projection_attributes: proj_attrs,
        is_local: index.is_local,
    }
}

fn schema_to_wire(schema: &TableSchema) -> TableSchemaWire {
    TableSchemaWire {
        name: schema.name.clone(),
        partition_key: KeyDefWire {
            name: schema.partition_key.name.clone(),
            key_type: key_type_str(schema.partition_key.key_type).to_string(),
        },
        sort_key: schema.sort_key.as_ref().map(|sk| KeyDefWire {
            name: sk.name.clone(),
            key_type: key_type_str(sk.key_type).to_string(),
        }),
        ttl_attribute: schema.ttl_attribute.clone(),
    }
}

/// Wait for SIGINT (Ctrl-C) or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
    }
}

fn dyn_error_to_response(err: DynError) -> Response {
    match &err {
        DynError::Transaction(TxnError::VersionMismatch { expected, actual }) => {
            Response::version_mismatch(*expected, *actual)
        }
        DynError::Transaction(TxnError::ConditionCheckFailed(msg)) => {
            Response::error("ConditionCheckFailed", msg.clone())
        }
        DynError::Schema(SchemaError::TableNotFound(name)) => {
            Response::error("TableNotFound", format!("table not found: {name}"))
        }
        DynError::Schema(SchemaError::TableAlreadyExists(name)) => Response::error(
            "TableAlreadyExists",
            format!("table already exists: {name}"),
        ),
        DynError::Schema(SchemaError::TtlNotConfigured(name)) => Response::error(
            "TtlNotConfigured",
            format!("table has no ttl_attribute configured: {name}"),
        ),
        DynError::Query(QueryError::ItemNotFound) => {
            Response::error("ItemNotFound", "item not found")
        }
        _ => Response::error("InternalError", err.to_string()),
    }
}
