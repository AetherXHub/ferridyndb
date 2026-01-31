use dynamite_core::api::QueryResult;
use dynamite_core::types::{KeyType, TableSchema};
use serde_json::Value;

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
///
/// Each item is pretty-printed, followed by a summary line showing the count.
/// If there are more results available (pagination), an additional note is printed.
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

/// Print the full command reference.
pub fn print_help() {
    println!(
        "\
DynaMite Console - Command Reference
=====================================

Table Management:
  CREATE TABLE <name> PK <attr> <STRING|NUMBER|BINARY> [SK <attr> <type>]
  DROP TABLE <name>
  LIST TABLES
  DESCRIBE TABLE <name>

Data Operations:
  PUT <table> {{json}}
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
  JSON:      {{\"key\": \"value\", \"num\": 42}}"
    );
}

/// Format a key type as a human-readable string.
pub fn format_key_type(kt: KeyType) -> &'static str {
    match kt {
        KeyType::String => "String",
        KeyType::Number => "Number",
        KeyType::Binary => "Binary",
    }
}
