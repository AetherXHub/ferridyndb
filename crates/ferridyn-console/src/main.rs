use std::io::{BufRead, IsTerminal};
use std::path::PathBuf;
use std::process;

use clap::Parser;
use ferridyn_server::client::FerridynClient;
use rustyline::DefaultEditor;
use tokio::runtime::Runtime;

mod commands;
mod display;
mod executor;
mod parser;

use display::OutputMode;

/// FerridynDB Console — interactive and scriptable CLI for FerridynDB databases.
#[derive(Parser, Debug)]
#[command(name = "ferridyn-console", version)]
struct Cli {
    /// Unix socket path to connect to (default: ~/.local/share/ferridyn/server.sock).
    #[arg(short, long)]
    socket: Option<String>,

    /// Execute a command non-interactively (can be repeated).
    #[arg(short, long = "exec")]
    exec: Vec<String>,

    /// Output results as machine-parseable JSON.
    #[arg(short, long)]
    json: bool,
}

fn default_socket_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("ferridyn")
        .join("server.sock")
}

fn main() {
    let cli = Cli::parse();

    let socket_path = cli
        .socket
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(default_socket_path);
    let using_default = cli.socket.is_none();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");

    // Try to connect to an existing server.
    let mut client = match runtime.block_on(FerridynClient::connect(&socket_path)) {
        Ok(c) => c,
        Err(_) if using_default => {
            // Auto-start the default server.
            match auto_start_server(&socket_path) {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("Failed to auto-start server: {e}");
                    process::exit(1);
                }
            }
            // Retry connection with backoff.
            match retry_connect(&runtime, &socket_path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to connect after starting server: {e}");
                    process::exit(1);
                }
            }
        }
        Err(e) => {
            eprintln!(
                "Cannot connect to server at {}: {e}\n\
                 Start a server with: ferridyn-server --socket {}",
                socket_path.display(),
                socket_path.display()
            );
            process::exit(1);
        }
    };

    if !cli.exec.is_empty() {
        let code = run_exec_mode(&mut client, &runtime, &cli.exec, cli.json);
        process::exit(code);
    } else if !std::io::stdin().is_terminal() {
        let code = run_pipe_mode(&mut client, &runtime, cli.json);
        process::exit(code);
    } else {
        run_repl(&mut client, &runtime);
    }
}

fn auto_start_server(socket_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("ferridyn");

    // Create data dir if needed.
    std::fs::create_dir_all(&data_dir)?;

    let db_path = data_dir.join("default.db");
    let log_path = data_dir.join("server.log");

    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let log_stderr = log_file.try_clone()?;

    process::Command::new("ferridyn-server")
        .args([
            "--db",
            db_path.to_str().unwrap_or("default.db"),
            "--socket",
            socket_path.to_str().unwrap_or("server.sock"),
        ])
        .stdout(log_file)
        .stderr(log_stderr)
        .spawn()?;

    Ok(())
}

fn retry_connect(
    rt: &Runtime,
    socket_path: &std::path::Path,
) -> Result<FerridynClient, ferridyn_server::error::ClientError> {
    for _ in 0..200 {
        std::thread::sleep(std::time::Duration::from_millis(10));
        if let Ok(c) = rt.block_on(FerridynClient::connect(socket_path)) {
            return Ok(c);
        }
    }
    rt.block_on(FerridynClient::connect(socket_path))
}

/// Execute one or more commands non-interactively (--exec mode).
///
/// Returns exit code: 0 = all succeeded, 1 = first error stops execution.
fn run_exec_mode(
    client: &mut FerridynClient,
    rt: &Runtime,
    commands: &[String],
    json_mode: bool,
) -> i32 {
    let mode = if json_mode {
        OutputMode::Json
    } else {
        OutputMode::Pretty
    };

    let mut default_table: Option<String> = None;

    for cmd_str in commands {
        let cmd = match parser::parse(cmd_str, default_table.as_deref()) {
            Ok(cmd) => cmd,
            Err(e) => {
                display::render_error(&e, &mode);
                return 1;
            }
        };

        match executor::execute(client, rt, cmd) {
            Ok(result) => {
                if let executor::CommandResult::Use(ref t) = result {
                    default_table = t.clone();
                }
                display::render(&result, &mode);
            }
            Err(e) => {
                display::render_error(&e, &mode);
                return 1;
            }
        }
    }

    0
}

/// Read commands from stdin (pipe mode).
///
/// Returns exit code: 0 = all succeeded, 1 = first error.
fn run_pipe_mode(client: &mut FerridynClient, rt: &Runtime, json_mode: bool) -> i32 {
    let mode = if json_mode {
        OutputMode::Json
    } else {
        OutputMode::Pretty
    };

    let mut default_table: Option<String> = None;

    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                display::render_error(&e, &mode);
                return 1;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let cmd = match parser::parse(trimmed, default_table.as_deref()) {
            Ok(cmd) => cmd,
            Err(e) => {
                display::render_error(&e, &mode);
                return 1;
            }
        };

        match executor::execute(client, rt, cmd) {
            Ok(result) => {
                if let executor::CommandResult::Use(ref t) = result {
                    default_table = t.clone();
                }
                if !display::render(&result, &mode) {
                    return 0; // EXIT command
                }
            }
            Err(e) => {
                display::render_error(&e, &mode);
                return 1;
            }
        }
    }

    0
}

/// Interactive REPL mode.
fn run_repl(client: &mut FerridynClient, rt: &Runtime) {
    println!("FerridynDB Console v0.1.0");
    println!("Type HELP for available commands.\n");

    let mut rl = DefaultEditor::new().expect("failed to initialize line editor");
    let mut current_table: Option<String> = None;

    loop {
        let prompt = match &current_table {
            Some(t) => format!("ferridyn:{t}> "),
            None => "ferridyn> ".to_string(),
        };
        match rl.readline(&prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(trimmed);

                let cmd = match parser::parse(trimmed, current_table.as_deref()) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        display::print_error(&e);
                        continue;
                    }
                };

                match executor::execute(client, rt, cmd) {
                    Ok(result) => {
                        if let executor::CommandResult::Use(ref t) = result {
                            current_table = t.clone();
                        }
                        if !display::render(&result, &OutputMode::Pretty) {
                            break; // EXIT command
                        }
                    }
                    Err(e) => display::print_error(&e),
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!();
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!("Bye!");
                break;
            }
            Err(e) => {
                eprintln!("Readline error: {e}");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferridyn_core::api::FerridynDB;
    use ferridyn_server::server::FerridynServer;

    fn test_setup() -> (tempfile::TempDir, Runtime, FerridynClient) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let socket_path = dir.path().join("test.sock");

        let db = FerridynDB::create(db_path.to_str().unwrap()).unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let server = FerridynServer::new(db, socket_path.clone());
        rt.spawn(async move {
            server.run().await.unwrap();
        });

        let client = rt.block_on(async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            FerridynClient::connect(&socket_path).await.unwrap()
        });

        (dir, rt, client)
    }

    // ---- Cli parsing tests ----

    #[test]
    fn test_cli_no_args() {
        let cli = Cli::try_parse_from(["bin"]).unwrap();
        assert!(cli.socket.is_none());
        assert!(cli.exec.is_empty());
        assert!(!cli.json);
    }

    #[test]
    fn test_cli_socket() {
        let cli = Cli::try_parse_from(["bin", "--socket", "/tmp/test.sock"]).unwrap();
        assert_eq!(cli.socket, Some("/tmp/test.sock".to_string()));
    }

    #[test]
    fn test_cli_exec_single() {
        let cli = Cli::try_parse_from(["bin", "--exec", "LIST TABLES"]).unwrap();
        assert_eq!(cli.exec, vec!["LIST TABLES"]);
    }

    #[test]
    fn test_cli_exec_multiple() {
        let cli =
            Cli::try_parse_from(["bin", "-e", "LIST TABLES", "--exec", "SCAN items"]).unwrap();
        assert_eq!(cli.exec, vec!["LIST TABLES", "SCAN items"]);
    }

    #[test]
    fn test_cli_json_flag() {
        let cli = Cli::try_parse_from(["bin", "--json"]).unwrap();
        assert!(cli.json);
    }

    #[test]
    fn test_cli_json_short() {
        let cli = Cli::try_parse_from(["bin", "-j"]).unwrap();
        assert!(cli.json);
    }

    #[test]
    fn test_cli_exec_and_json() {
        let cli = Cli::try_parse_from(["bin", "-e", "HELP", "--json"]).unwrap();
        assert_eq!(cli.exec, vec!["HELP"]);
        assert!(cli.json);
    }

    #[test]
    fn test_cli_exec_missing_value() {
        let result = Cli::try_parse_from(["bin", "--exec"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_unknown_flag() {
        let result = Cli::try_parse_from(["bin", "--verbose"]);
        assert!(result.is_err());
    }

    // ---- exec mode integration tests ----

    #[test]
    fn test_exec_list_tables_empty() {
        let (_dir, rt, mut client) = test_setup();
        let code = run_exec_mode(&mut client, &rt, &["LIST TABLES".to_string()], false);
        assert_eq!(code, 0);
    }

    #[test]
    fn test_exec_json_output() {
        let (_dir, rt, mut client) = test_setup();
        let code = run_exec_mode(&mut client, &rt, &["LIST TABLES".to_string()], true);
        assert_eq!(code, 0);
    }

    #[test]
    fn test_exec_error_returns_1() {
        let (_dir, rt, mut client) = test_setup();
        let code = run_exec_mode(&mut client, &rt, &["SCAN nonexistent".to_string()], false);
        assert_eq!(code, 1);
    }

    #[test]
    fn test_exec_multiple_commands() {
        let (_dir, rt, mut client) = test_setup();
        let code = run_exec_mode(
            &mut client,
            &rt,
            &[
                "CREATE TABLE items PK id STRING".to_string(),
                "LIST TABLES".to_string(),
            ],
            false,
        );
        assert_eq!(code, 0);
    }

    #[test]
    fn test_exec_error_stops_early() {
        let (_dir, rt, mut client) = test_setup();
        let code = run_exec_mode(
            &mut client,
            &rt,
            &["SCAN nonexistent".to_string(), "LIST TABLES".to_string()],
            false,
        );
        assert_eq!(code, 1);
    }

    #[test]
    fn test_exec_parse_error_returns_1() {
        let (_dir, rt, mut client) = test_setup();
        let code = run_exec_mode(&mut client, &rt, &["INVALID GIBBERISH".to_string()], false);
        assert_eq!(code, 1);
    }

    #[test]
    fn test_exec_json_error_returns_1() {
        let (_dir, rt, mut client) = test_setup();
        let code = run_exec_mode(&mut client, &rt, &["SCAN nonexistent".to_string()], true);
        assert_eq!(code, 1);
    }

    #[test]
    fn test_exec_use_persists() {
        let (_dir, rt, mut client) = test_setup();
        let code = run_exec_mode(
            &mut client,
            &rt,
            &[
                "CREATE TABLE data PK pk STRING".to_string(),
                "USE data".to_string(),
                "LIST SCHEMAS".to_string(),
            ],
            false,
        );
        assert_eq!(code, 0);
    }
}
