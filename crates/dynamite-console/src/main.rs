use std::io::{BufRead, IsTerminal};
use std::path::Path;
use std::process;

use clap::Parser;
use dynamite_core::api::DynamiteDB;
use rustyline::DefaultEditor;

mod commands;
mod display;
mod executor;
mod parser;

use display::OutputMode;

/// DynamiteDB Console — interactive and scriptable CLI for DynamiteDB databases.
#[derive(Parser, Debug)]
#[command(name = "dynamite-console", version)]
struct Cli {
    /// Path to the database file (created if it doesn't exist).
    database: String,

    /// Execute a command non-interactively (can be repeated).
    #[arg(short, long = "exec")]
    exec: Vec<String>,

    /// Output results as machine-parseable JSON.
    #[arg(short, long)]
    json: bool,
}

fn main() {
    let cli = Cli::parse();

    let db = if Path::new(&cli.database).exists() {
        DynamiteDB::open(&cli.database)
    } else {
        DynamiteDB::create(&cli.database)
    };
    let db = match db {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Failed to open database: {e}");
            process::exit(1);
        }
    };

    if !cli.exec.is_empty() {
        let code = run_exec_mode(&db, &cli.exec, cli.json);
        process::exit(code);
    } else if !std::io::stdin().is_terminal() {
        let code = run_pipe_mode(&db, cli.json);
        process::exit(code);
    } else {
        run_repl(&db);
    }
}

/// Execute one or more commands non-interactively (--exec mode).
///
/// Returns exit code: 0 = all succeeded, 1 = first error stops execution.
fn run_exec_mode(db: &DynamiteDB, commands: &[String], json_mode: bool) -> i32 {
    let mode = if json_mode {
        OutputMode::Json
    } else {
        OutputMode::Pretty
    };

    for cmd_str in commands {
        let cmd = match parser::parse(cmd_str) {
            Ok(cmd) => cmd,
            Err(e) => {
                display::render_error(&e, &mode);
                return 1;
            }
        };

        match executor::execute(db, cmd) {
            Ok(result) => {
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
fn run_pipe_mode(db: &DynamiteDB, json_mode: bool) -> i32 {
    let mode = if json_mode {
        OutputMode::Json
    } else {
        OutputMode::Pretty
    };

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

        let cmd = match parser::parse(trimmed) {
            Ok(cmd) => cmd,
            Err(e) => {
                display::render_error(&e, &mode);
                return 1;
            }
        };

        match executor::execute(db, cmd) {
            Ok(result) => {
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

/// Interactive REPL mode (unchanged behavior).
fn run_repl(db: &DynamiteDB) {
    println!("DynamiteDB Console v0.1.0");
    println!("Type HELP for available commands.\n");

    let mut rl = DefaultEditor::new().expect("failed to initialize line editor");

    loop {
        match rl.readline("dynamite> ") {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(trimmed);

                let cmd = match parser::parse(trimmed) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        display::print_error(&e);
                        continue;
                    }
                };

                match executor::execute(db, cmd) {
                    Ok(result) => {
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

    // ---- Cli parsing tests (via clap try_parse_from) ----

    #[test]
    fn test_cli_db_path_only() {
        let cli = Cli::try_parse_from(["bin", "my.db"]).unwrap();
        assert_eq!(cli.database, "my.db");
        assert!(cli.exec.is_empty());
        assert!(!cli.json);
    }

    #[test]
    fn test_cli_exec_single() {
        let cli = Cli::try_parse_from(["bin", "my.db", "--exec", "LIST TABLES"]).unwrap();
        assert_eq!(cli.database, "my.db");
        assert_eq!(cli.exec, vec!["LIST TABLES"]);
        assert!(!cli.json);
    }

    #[test]
    fn test_cli_exec_multiple() {
        let cli =
            Cli::try_parse_from(["bin", "my.db", "-e", "LIST TABLES", "--exec", "SCAN items"])
                .unwrap();
        assert_eq!(cli.database, "my.db");
        assert_eq!(cli.exec, vec!["LIST TABLES", "SCAN items"]);
    }

    #[test]
    fn test_cli_json_flag() {
        let cli = Cli::try_parse_from(["bin", "my.db", "--json"]).unwrap();
        assert!(cli.json);
    }

    #[test]
    fn test_cli_json_short() {
        let cli = Cli::try_parse_from(["bin", "my.db", "-j"]).unwrap();
        assert!(cli.json);
    }

    #[test]
    fn test_cli_exec_and_json() {
        let cli = Cli::try_parse_from(["bin", "my.db", "-e", "HELP", "--json"]).unwrap();
        assert_eq!(cli.database, "my.db");
        assert_eq!(cli.exec, vec!["HELP"]);
        assert!(cli.json);
    }

    #[test]
    fn test_cli_no_db_path() {
        let result = Cli::try_parse_from(["bin"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_exec_missing_value() {
        let result = Cli::try_parse_from(["bin", "my.db", "--exec"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_unknown_flag() {
        let result = Cli::try_parse_from(["bin", "my.db", "--verbose"]);
        assert!(result.is_err());
    }

    // ---- exec mode integration tests ----

    #[test]
    fn test_exec_list_tables_empty() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = DynamiteDB::create(db_path.to_str().unwrap()).unwrap();

        let code = run_exec_mode(&db, &["LIST TABLES".to_string()], false);
        assert_eq!(code, 0);
    }

    #[test]
    fn test_exec_json_output() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = DynamiteDB::create(db_path.to_str().unwrap()).unwrap();

        let code = run_exec_mode(&db, &["LIST TABLES".to_string()], true);
        assert_eq!(code, 0);
    }

    #[test]
    fn test_exec_error_returns_1() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = DynamiteDB::create(db_path.to_str().unwrap()).unwrap();

        let code = run_exec_mode(&db, &["SCAN nonexistent".to_string()], false);
        assert_eq!(code, 1);
    }

    #[test]
    fn test_exec_multiple_commands() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = DynamiteDB::create(db_path.to_str().unwrap()).unwrap();

        let code = run_exec_mode(
            &db,
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
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = DynamiteDB::create(db_path.to_str().unwrap()).unwrap();

        let code = run_exec_mode(
            &db,
            &["SCAN nonexistent".to_string(), "LIST TABLES".to_string()],
            false,
        );
        assert_eq!(code, 1);
    }

    #[test]
    fn test_exec_parse_error_returns_1() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = DynamiteDB::create(db_path.to_str().unwrap()).unwrap();

        let code = run_exec_mode(&db, &["INVALID GIBBERISH".to_string()], false);
        assert_eq!(code, 1);
    }

    #[test]
    fn test_exec_json_error_returns_1() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = DynamiteDB::create(db_path.to_str().unwrap()).unwrap();

        let code = run_exec_mode(&db, &["SCAN nonexistent".to_string()], true);
        assert_eq!(code, 1);
    }
}
