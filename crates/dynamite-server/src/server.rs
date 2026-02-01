//! Unix domain socket server that wraps a `DynamiteDB` database handle.
//!
//! Each connected client sends JSON-line requests and receives JSON-line
//! responses. Reads are concurrent (via `RwLock::read`), writes are
//! serialized (via `RwLock::write`) — the lock is internal to `DynamiteDB`.

use std::path::PathBuf;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tracing::{error, info, warn};

use dynamite_core::api::DynamiteDB;
use dynamite_core::error::{Error as DynError, SchemaError, TxnError};
use dynamite_core::types::{KeyType, TableSchema};

use crate::protocol::{KeyDef, KeyDefWire, Request, Response, SortKeyCondition, TableSchemaWire};

/// A DynamiteDB server listening on a Unix socket.
pub struct DynamiteServer {
    db: DynamiteDB,
    socket_path: PathBuf,
}

impl DynamiteServer {
    pub fn new(db: DynamiteDB, socket_path: PathBuf) -> Self {
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

async fn handle_connection(db: DynamiteDB, stream: tokio::net::UnixStream) -> std::io::Result<()> {
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

fn dispatch(db: &DynamiteDB, req: Request) -> Response {
    match req {
        Request::GetItem {
            table,
            partition_key,
            sort_key,
        } => handle_get_item(db, &table, partition_key, sort_key),

        Request::GetItemVersioned {
            table,
            partition_key,
            sort_key,
        } => handle_get_item_versioned(db, &table, partition_key, sort_key),

        Request::PutItem {
            table,
            item,
            expected_version,
        } => handle_put_item(db, &table, item, expected_version),

        Request::DeleteItem {
            table,
            partition_key,
            sort_key,
        } => handle_delete_item(db, &table, partition_key, sort_key),

        Request::Query {
            table,
            partition_key,
            sort_key_condition,
            limit,
            scan_forward,
            exclusive_start_key,
        } => handle_query(
            db,
            &table,
            partition_key,
            sort_key_condition,
            limit,
            scan_forward,
            exclusive_start_key,
        ),

        Request::Scan {
            table,
            limit,
            exclusive_start_key,
        } => handle_scan(db, &table, limit, exclusive_start_key),

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
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn handle_get_item(
    db: &DynamiteDB,
    table: &str,
    partition_key: serde_json::Value,
    sort_key: Option<serde_json::Value>,
) -> Response {
    let mut builder = db.get_item(table).partition_key(partition_key);
    if let Some(sk) = sort_key {
        builder = builder.sort_key(sk);
    }
    match builder.execute() {
        Ok(item) => Response::ok_item(item),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_get_item_versioned(
    db: &DynamiteDB,
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
    db: &DynamiteDB,
    table: &str,
    item: serde_json::Value,
    expected_version: Option<u64>,
) -> Response {
    let result = if let Some(ev) = expected_version {
        db.put_item_conditional(table, item, ev)
    } else {
        db.put_item(table, item)
    };
    match result {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_delete_item(
    db: &DynamiteDB,
    table: &str,
    partition_key: serde_json::Value,
    sort_key: Option<serde_json::Value>,
) -> Response {
    let mut builder = db.delete_item(table).partition_key(partition_key);
    if let Some(sk) = sort_key {
        builder = builder.sort_key(sk);
    }
    match builder.execute() {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_query(
    db: &DynamiteDB,
    table: &str,
    partition_key: serde_json::Value,
    sort_key_condition: Option<SortKeyCondition>,
    limit: Option<usize>,
    scan_forward: Option<bool>,
    exclusive_start_key: Option<serde_json::Value>,
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

    match builder.execute() {
        Ok(result) => Response::ok_items(result.items, result.last_evaluated_key),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_scan(
    db: &DynamiteDB,
    table: &str,
    limit: Option<usize>,
    exclusive_start_key: Option<serde_json::Value>,
) -> Response {
    let mut builder = db.scan(table);
    if let Some(n) = limit {
        builder = builder.limit(n);
    }
    if let Some(esk) = exclusive_start_key {
        builder = builder.exclusive_start_key(esk);
    }
    match builder.execute() {
        Ok(result) => Response::ok_items(result.items, result.last_evaluated_key),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_create_table(
    db: &DynamiteDB,
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

fn handle_drop_table(db: &DynamiteDB, table: &str) -> Response {
    match db.drop_table(table) {
        Ok(()) => Response::ok_empty(),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_list_tables(db: &DynamiteDB) -> Response {
    match db.list_tables() {
        Ok(tables) => Response::ok_tables(tables),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_describe_table(db: &DynamiteDB, table: &str) -> Response {
    match db.describe_table(table) {
        Ok(schema) => Response::ok_schema(schema_to_wire(&schema)),
        Err(e) => dyn_error_to_response(e),
    }
}

fn handle_list_partition_keys(db: &DynamiteDB, table: &str, limit: Option<usize>) -> Response {
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
    db: &DynamiteDB,
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
        DynError::Schema(SchemaError::TableNotFound(name)) => {
            Response::error("TableNotFound", format!("table not found: {name}"))
        }
        DynError::Schema(SchemaError::TableAlreadyExists(name)) => Response::error(
            "TableAlreadyExists",
            format!("table already exists: {name}"),
        ),
        _ => Response::error("InternalError", err.to_string()),
    }
}
