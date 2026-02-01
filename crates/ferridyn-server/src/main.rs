//! FerridynDB server binary.
//!
//! Opens or creates a database and serves it over a Unix domain socket.

use std::path::PathBuf;

use ferridyn_core::api::FerridynDB;
use ferridyn_server::FerridynServer;
use tracing::info;

fn default_data_dir() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("ferridyn")
}

fn parse_args() -> (PathBuf, PathBuf) {
    let args: Vec<String> = std::env::args().collect();
    let mut db_path: Option<PathBuf> = None;
    let mut socket_path: Option<PathBuf> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--db" => {
                i += 1;
                db_path = Some(PathBuf::from(&args[i]));
            }
            "--socket" => {
                i += 1;
                socket_path = Some(PathBuf::from(&args[i]));
            }
            other => {
                eprintln!("unknown argument: {other}");
                eprintln!("usage: ferridyn-server [--db PATH] [--socket PATH]");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let data_dir = default_data_dir();
    let db = db_path.unwrap_or_else(|| data_dir.join("default.db"));
    let sock = socket_path.unwrap_or_else(|| data_dir.join("server.sock"));

    (db, sock)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let (db_path, socket_path) = parse_args();

    // Ensure parent directories exist.
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    info!(db = %db_path.display(), socket = %socket_path.display(), "starting");

    // Open or create the database.
    let db = if db_path.exists() {
        FerridynDB::open(&db_path)?
    } else {
        FerridynDB::create(&db_path)?
    };

    let server = FerridynServer::new(db, socket_path);
    server.run().await?;

    Ok(())
}
