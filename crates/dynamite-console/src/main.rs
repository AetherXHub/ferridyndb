use std::env;
use std::path::Path;
use std::process;

use dynamite_core::api::DynaMite;
use rustyline::DefaultEditor;

mod commands;
mod display;
mod executor;
mod parser;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: dynamite-console <database-path>");
        process::exit(1);
    }
    let db_path = &args[1];

    let db = if Path::new(db_path).exists() {
        DynaMite::open(db_path)
    } else {
        DynaMite::create(db_path)
    };
    let db = match db {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Failed to open database: {e}");
            process::exit(1);
        }
    };

    println!("DynaMite Console v0.1.0");
    println!("Database: {db_path}");
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

                match executor::execute(&db, cmd) {
                    Ok(true) => {}
                    Ok(false) => break,
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
