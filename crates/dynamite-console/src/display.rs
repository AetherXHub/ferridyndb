use dynamite_core::api::QueryResult;
use dynamite_core::types::{KeyType, TableSchema};
use serde_json::{Value, json};

use crate::executor::CommandResult;

/// Output mode for rendering command results.
pub enum OutputMode {
    /// Human-readable pretty-printed output.
    Pretty,
    /// Machine-parseable JSON (one JSON object per result on stdout).
    Json,
}

/// Render a command result to stdout in the given mode.
///
/// Returns `true` to continue execution, `false` to signal exit.
pub fn render(result: &CommandResult, mode: &OutputMode) -> bool {
    match result {
        CommandResult::Ok(msg) => match mode {
            OutputMode::Pretty => print_ok(msg),
            OutputMode::Json => println!("{}", json!({"ok": true, "message": msg})),
        },
        CommandResult::Item(item) => match mode {
            OutputMode::Pretty => match item {
                Some(v) => print_item(v),
                None => print_not_found(),
            },
            OutputMode::Json => match item {
                Some(v) => println!("{}", json!({"found": true, "item": v})),
                None => println!("{}", json!({"found": false})),
            },
        },
        CommandResult::QueryResult(result) => match mode {
            OutputMode::Pretty => print_query_result(result),
            OutputMode::Json => {
                let has_more = result.last_evaluated_key.is_some();
                println!(
                    "{}",
                    json!({
                        "items": result.items,
                        "count": result.items.len(),
                        "has_more": has_more,
                    })
                );
            }
        },
        CommandResult::TableList(tables) => match mode {
            OutputMode::Pretty => print_table_list(tables),
            OutputMode::Json => println!("{}", json!({"tables": tables})),
        },
        CommandResult::TableSchema(schema) => match mode {
            OutputMode::Pretty => print_table_schema(schema),
            OutputMode::Json => {
                let sk = schema.sort_key.as_ref().map(|sk| {
                    json!({
                        "name": sk.name,
                        "type": format_key_type(sk.key_type),
                    })
                });
                println!(
                    "{}",
                    json!({
                        "name": schema.name,
                        "partition_key": {
                            "name": schema.partition_key.name,
                            "type": format_key_type(schema.partition_key.key_type),
                        },
                        "sort_key": sk,
                    })
                );
            }
        },
        CommandResult::Help => match mode {
            OutputMode::Pretty => print_help(),
            OutputMode::Json => println!("{}", json!({"help": HELP_TEXT})),
        },
        CommandResult::Exit => return false,
    }
    true
}

/// Render an error in the given mode (always to stderr).
pub fn render_error(err: &dyn std::fmt::Display, mode: &OutputMode) {
    match mode {
        OutputMode::Pretty => print_error(err),
        OutputMode::Json => {
            eprintln!("{}", json!({"error": err.to_string()}));
        }
    }
}

// ---- Pretty-print helpers (unchanged from original) ----

/// Pretty-print a single item with 2-space indentation.
pub fn print_item(item: &Value) {
    match serde_json::to_string_pretty(item) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!("Error formatting item: {e}"),
    }
}

/// Print a "not found" message.
pub fn print_not_found() {
    println!("Item not found.");
}

/// Print the result of a QUERY or SCAN operation.
pub fn print_query_result(result: &QueryResult) {
    for item in &result.items {
        print_item(item);
    }
    let n = result.items.len();
    println!("Returned {n} item(s).");
    if result.last_evaluated_key.is_some() {
        println!("(more results available)");
    }
}

/// Print a list of table names.
pub fn print_table_list(tables: &[String]) {
    if tables.is_empty() {
        println!("No tables.");
    } else {
        for name in tables {
            println!("{name}");
        }
    }
}

/// Print the schema of a table.
pub fn print_table_schema(schema: &TableSchema) {
    println!("Table: {}", schema.name);
    println!(
        "  Partition key: {} ({})",
        schema.partition_key.name,
        format_key_type(schema.partition_key.key_type)
    );
    match &schema.sort_key {
        Some(sk) => {
            println!(
                "  Sort key:      {} ({})",
                sk.name,
                format_key_type(sk.key_type)
            );
        }
        None => {
            println!("  Sort key:      (none)");
        }
    }
}

/// Print a success message.
pub fn print_ok(msg: &str) {
    println!("{msg}");
}

/// Print an error message to stderr.
pub fn print_error(err: &dyn std::fmt::Display) {
    eprintln!("Error: {err}");
}

const HELP_TEXT: &str = "\
DynaMite Console - Command Reference
=====================================

Table Management:
  CREATE TABLE <name> PK <attr> <STRING|NUMBER|BINARY> [SK <attr> <type>]
  DROP TABLE <name>
  LIST TABLES
  DESCRIBE TABLE <name>

Data Operations:
  PUT <table> {json}
  GET <table> pk=<value> [sk=<value>]
  DELETE <table> pk=<value> [sk=<value>]

Query and Scan:
  QUERY <table> pk=<value> [SK <op> <value>] [LIMIT <n>] [DESC]
    Sort key operators: =  <  <=  >  >=
    Special conditions:  BETWEEN <lo> AND <hi>
                         BEGINS_WITH <prefix>
  SCAN <table> [LIMIT <n>]

Other:
  HELP                    Show this help
  EXIT / QUIT             Exit the console

Values:
  Numbers:   42, 3.14
  Strings:   alice, \"hello world\"  (use quotes for spaces)
  JSON:      {\"key\": \"value\", \"num\": 42}";

/// Print the full command reference.
pub fn print_help() {
    println!("{HELP_TEXT}");
}

/// Format a key type as a human-readable string.
pub fn format_key_type(kt: KeyType) -> &'static str {
    match kt {
        KeyType::String => "String",
        KeyType::Number => "Number",
        KeyType::Binary => "Binary",
    }
}
