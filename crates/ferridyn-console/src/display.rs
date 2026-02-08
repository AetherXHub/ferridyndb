use ferridyn_server::client::{IndexInfo, PartitionSchemaInfo, QueryResult, TableSchema};
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
        CommandResult::PartitionKeys(keys) => match mode {
            OutputMode::Pretty => print_partition_keys(keys),
            OutputMode::Json => println!("{}", json!({"partition_keys": keys})),
        },
        CommandResult::SortKeyPrefixes(prefixes) => match mode {
            OutputMode::Pretty => print_sort_key_prefixes(prefixes),
            OutputMode::Json => println!("{}", json!({"sort_key_prefixes": prefixes})),
        },
        CommandResult::TableSchema(schema) => match mode {
            OutputMode::Pretty => print_table_schema(schema),
            OutputMode::Json => {
                let sk = match (&schema.sort_key_name, &schema.sort_key_type) {
                    (Some(name), Some(typ)) => Some(json!({"name": name, "type": typ})),
                    _ => None,
                };
                println!(
                    "{}",
                    json!({
                        "name": schema.name,
                        "partition_key": {
                            "name": schema.partition_key_name,
                            "type": schema.partition_key_type,
                        },
                        "sort_key": sk,
                        "ttl_attribute": schema.ttl_attribute,
                    })
                );
            }
        },
        CommandResult::SchemaList(schemas) => match mode {
            OutputMode::Pretty => print_schema_list(schemas),
            OutputMode::Json => {
                let items: Vec<Value> = schemas
                    .iter()
                    .map(|s| {
                        json!({
                            "prefix": s.prefix,
                            "description": s.description,
                            "attributes": s.attributes.iter().map(|a| json!({
                                "name": a.name,
                                "type": a.attr_type,
                                "required": a.required,
                            })).collect::<Vec<_>>(),
                            "validate": s.validate,
                        })
                    })
                    .collect();
                println!("{}", json!({"partition_schemas": items}));
            }
        },
        CommandResult::SchemaDetail(schema) => match mode {
            OutputMode::Pretty => print_schema_detail(schema),
            OutputMode::Json => {
                println!(
                    "{}",
                    json!({
                        "prefix": schema.prefix,
                        "description": schema.description,
                        "attributes": schema.attributes.iter().map(|a| json!({
                            "name": a.name,
                            "type": a.attr_type,
                            "required": a.required,
                        })).collect::<Vec<Value>>(),
                        "validate": schema.validate,
                    })
                );
            }
        },
        CommandResult::IndexList(indexes) => match mode {
            OutputMode::Pretty => print_index_list(indexes),
            OutputMode::Json => {
                let items: Vec<Value> = indexes
                    .iter()
                    .map(|idx| {
                        json!({
                            "name": idx.name,
                            "partition_schema": idx.partition_schema,
                            "index_key": {
                                "name": idx.index_key_name,
                                "type": idx.index_key_type,
                            },
                        })
                    })
                    .collect();
                println!("{}", json!({"indexes": items}));
            }
        },
        CommandResult::IndexDetail(index) => match mode {
            OutputMode::Pretty => print_index_detail(index),
            OutputMode::Json => {
                println!(
                    "{}",
                    json!({
                        "name": index.name,
                        "partition_schema": index.partition_schema,
                        "index_key": {
                            "name": index.index_key_name,
                            "type": index.index_key_type,
                        },
                    })
                );
            }
        },
        CommandResult::Use(Some(name)) => match mode {
            OutputMode::Pretty => println!("Using table '{name}'."),
            OutputMode::Json => println!("{}", json!({"ok": true, "active_table": name})),
        },
        CommandResult::Use(None) => match mode {
            OutputMode::Pretty => println!("Cleared active table."),
            OutputMode::Json => println!("{}", json!({"ok": true, "active_table": null})),
        },
        CommandResult::Help(topic) => match mode {
            OutputMode::Pretty => render_help_pretty(topic.as_deref()),
            OutputMode::Json => render_help_json(topic.as_deref()),
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

// ---- Pretty-print helpers ----

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
        schema.partition_key_name, schema.partition_key_type
    );
    match (&schema.sort_key_name, &schema.sort_key_type) {
        (Some(name), Some(typ)) => println!("  Sort key:      {} ({})", name, typ),
        _ => println!("  Sort key:      (none)"),
    }
    match &schema.ttl_attribute {
        Some(attr) => println!("  TTL attribute: {attr}"),
        None => println!("  TTL attribute: (none)"),
    }
}

/// Print a list of distinct partition keys.
pub fn print_partition_keys(keys: &[Value]) {
    if keys.is_empty() {
        println!("No partition keys.");
    } else {
        for key in keys {
            match key {
                Value::String(s) => println!("  {s}"),
                other => println!("  {other}"),
            }
        }
        let n = keys.len();
        println!("({n} key(s))");
    }
}

/// Print a list of distinct sort key prefixes.
pub fn print_sort_key_prefixes(prefixes: &[Value]) {
    if prefixes.is_empty() {
        println!("No sort key prefixes.");
    } else {
        for prefix in prefixes {
            match prefix {
                Value::String(s) => println!("  {s}"),
                other => println!("  {other}"),
            }
        }
        let n = prefixes.len();
        println!("({n} prefix(es))");
    }
}

/// Print a list of partition schemas.
fn print_schema_list(schemas: &[PartitionSchemaInfo]) {
    if schemas.is_empty() {
        println!("No partition schemas.");
    } else {
        for s in schemas {
            println!("  {} \u{2014} {}", s.prefix, s.description);
        }
        println!("({} schema(s))", schemas.len());
    }
}

/// Print details of a single partition schema.
fn print_schema_detail(schema: &PartitionSchemaInfo) {
    println!("Partition schema: {}", schema.prefix);
    println!("  Description: {}", schema.description);
    println!("  Validate:    {}", schema.validate);
    if schema.attributes.is_empty() {
        println!("  Attributes:  (none)");
    } else {
        println!("  Attributes:");
        for attr in &schema.attributes {
            let req = if attr.required { " (required)" } else { "" };
            println!("    {} {}{}", attr.name, attr.attr_type, req);
        }
    }
}

/// Print a list of secondary indexes.
fn print_index_list(indexes: &[IndexInfo]) {
    if indexes.is_empty() {
        println!("No indexes.");
    } else {
        for idx in indexes {
            let schema = idx.partition_schema.as_deref().unwrap_or("(global)");
            println!(
                "  {} \u{2014} schema: {}, key: {} ({})",
                idx.name, schema, idx.index_key_name, idx.index_key_type
            );
        }
        println!("({} index(es))", indexes.len());
    }
}

/// Print details of a single secondary index.
fn print_index_detail(index: &IndexInfo) {
    println!("Index: {}", index.name);
    let schema = index.partition_schema.as_deref().unwrap_or("(global)");
    println!("  Partition schema: {}", schema);
    println!(
        "  Index key:        {} ({})",
        index.index_key_name, index.index_key_type
    );
}

/// Print a success message.
pub fn print_ok(msg: &str) {
    println!("{msg}");
}

/// Print an error message to stderr.
pub fn print_error(err: &dyn std::fmt::Display) {
    eprintln!("Error: {err}");
}

// ---------------------------------------------------------------------------
// Structured per-command help
// ---------------------------------------------------------------------------

struct CommandHelp {
    name: &'static str,
    summary: &'static str,
    syntax: &'static str,
    details: &'static str,
    examples: &'static [&'static str],
}

/// Lookup key(s) that match this command (lowercase).
/// First element is the canonical short form; extra elements are aliases.
fn topic_keys(cmd: &CommandHelp) -> Vec<&'static str> {
    match cmd.name {
        "CREATE TABLE" => vec!["create table"],
        "DROP TABLE" => vec!["drop table"],
        "LIST TABLES" => vec!["list tables", "list"],
        "LIST KEYS" => vec!["list keys"],
        "LIST PREFIXES" => vec!["list prefixes"],
        "DESCRIBE TABLE" => vec!["describe table"],
        "CREATE SCHEMA" => vec!["create schema"],
        "DROP SCHEMA" => vec!["drop schema"],
        "LIST SCHEMAS" => vec!["list schemas"],
        "DESCRIBE SCHEMA" => vec!["describe schema"],
        "CREATE INDEX" => vec!["create index"],
        "DROP INDEX" => vec!["drop index"],
        "LIST INDEXES" => vec!["list indexes"],
        "DESCRIBE INDEX" => vec!["describe index"],
        "QUERY INDEX" => vec!["query index"],
        "PUT" => vec!["put"],
        "GET" => vec!["get"],
        "DELETE" => vec!["delete"],
        "UPDATE" => vec!["update"],
        "QUERY" => vec!["query"],
        "SCAN" => vec!["scan"],
        "HELP" => vec!["help"],
        "USE" => vec!["use"],
        "EXIT / QUIT" => vec!["exit", "quit"],
        _ => vec![],
    }
}

const COMMANDS: &[CommandHelp] = &[
    // -- Table Management --
    CommandHelp {
        name: "CREATE TABLE",
        summary: "Create a new table with partition and optional sort key",
        syntax: "CREATE TABLE <name> PK <attr> <STRING|NUMBER|BINARY> [SK <attr> <type>] [TTL <attr>]",
        details: "\
Key types: STRING, NUMBER, BINARY.
A partition key is required. A sort key is optional but enables \
range queries and ordered access patterns.

TTL (Time-To-Live): optionally specify an attribute name that holds \
a Unix epoch-seconds timestamp. Items whose TTL value is in the past \
become invisible to reads and eligible for cleanup. A value of 0 or \
a missing/non-numeric attribute means the item never expires.",
        examples: &[
            "CREATE TABLE users PK user_id STRING",
            "CREATE TABLE events PK user_id STRING SK timestamp NUMBER",
            "CREATE TABLE blobs PK hash BINARY",
            "CREATE TABLE cache PK key STRING TTL expires",
            "CREATE TABLE sessions PK user_id STRING SK session_id STRING TTL ttl",
        ],
    },
    CommandHelp {
        name: "DROP TABLE",
        summary: "Delete a table and all its data",
        syntax: "DROP TABLE <name>",
        details: "Permanently removes the table and every item it contains.",
        examples: &["DROP TABLE users"],
    },
    CommandHelp {
        name: "LIST TABLES",
        summary: "Show all tables in the database",
        syntax: "LIST TABLES",
        details: "Prints one table name per line, or \"No tables.\" if the database is empty.",
        examples: &["LIST TABLES"],
    },
    CommandHelp {
        name: "DESCRIBE TABLE",
        summary: "Show a table's key schema",
        syntax: "DESCRIBE TABLE <name>",
        details: "Displays the partition key and sort key (if any) with their types.",
        examples: &["DESCRIBE TABLE users", "DESCRIBE TABLE events"],
    },
    // -- Partition Schema Management --
    CommandHelp {
        name: "CREATE SCHEMA",
        summary: "Declare a partition schema on a table",
        syntax: "CREATE SCHEMA <table> PREFIX <prefix> [DESCRIPTION \"text\"] [ATTR <name> <STRING|NUMBER|BOOLEAN> [REQUIRED]]... [VALIDATE]",
        details: "Defines entity metadata for a partition key prefix. Attributes declare expected fields with types. VALIDATE enables write-time validation.",
        examples: &[
            "CREATE SCHEMA data PREFIX CONTACT",
            "CREATE SCHEMA data PREFIX CONTACT DESCRIPTION \"People\" ATTR email STRING REQUIRED ATTR age NUMBER VALIDATE",
        ],
    },
    CommandHelp {
        name: "DROP SCHEMA",
        summary: "Remove a partition schema from a table",
        syntax: "DROP SCHEMA <table> <prefix>",
        details: "Fails if any indexes reference the partition schema. Drop indexes first.",
        examples: &["DROP SCHEMA data CONTACT"],
    },
    CommandHelp {
        name: "LIST SCHEMAS",
        summary: "List partition schemas for a table",
        syntax: "LIST SCHEMAS <table>",
        details: "Shows all declared partition schemas with their prefixes and descriptions.",
        examples: &["LIST SCHEMAS data"],
    },
    CommandHelp {
        name: "DESCRIBE SCHEMA",
        summary: "Show details of a partition schema",
        syntax: "DESCRIBE SCHEMA <table> <prefix>",
        details: "Displays the schema's attributes, types, required flags, and validation setting.",
        examples: &["DESCRIBE SCHEMA data CONTACT"],
    },
    // -- Index Management --
    CommandHelp {
        name: "CREATE INDEX",
        summary: "Create a secondary index on a table",
        syntax: "CREATE INDEX <table> <name> SCHEMA <prefix> KEY <attr> <STRING|NUMBER|BINARY>",
        details: "Creates an index scoped to a partition schema. Existing data matching the prefix is backfilled synchronously.",
        examples: &[
            "CREATE INDEX data email-idx SCHEMA CONTACT KEY email STRING",
            "CREATE INDEX data price-idx SCHEMA PRODUCT KEY price NUMBER",
        ],
    },
    CommandHelp {
        name: "DROP INDEX",
        summary: "Remove a secondary index from a table",
        syntax: "DROP INDEX <table> <name>",
        details: "Removes the index definition. Index B+Tree pages are not reclaimed in v1.",
        examples: &["DROP INDEX data email-idx"],
    },
    CommandHelp {
        name: "LIST INDEXES",
        summary: "List secondary indexes for a table",
        syntax: "LIST INDEXES <table>",
        details: "Shows all indexes with their names, schemas, and key attributes.",
        examples: &["LIST INDEXES data"],
    },
    CommandHelp {
        name: "DESCRIBE INDEX",
        summary: "Show details of a secondary index",
        syntax: "DESCRIBE INDEX <table> <name>",
        details: "Displays the index's partition schema, key attribute, and key type.",
        examples: &["DESCRIBE INDEX data email-idx"],
    },
    CommandHelp {
        name: "QUERY INDEX",
        summary: "Find items by secondary index value",
        syntax: "QUERY INDEX <table> <index_name> key=<value> [LIMIT <n>] [DESC]",
        details: "Looks up documents by an indexed attribute value. Returns full documents from the primary table. Expired (TTL) documents are filtered out.",
        examples: &[
            "QUERY INDEX data email-idx key=\"alice@example.com\"",
            "QUERY INDEX data email-idx key=\"alice@example.com\" LIMIT 5",
            "QUERY INDEX data status-idx key=active DESC",
        ],
    },
    CommandHelp {
        name: "LIST KEYS",
        summary: "List distinct partition keys in a table",
        syntax: "LIST KEYS <table> [LIMIT <n>]",
        details: "\
Returns the distinct partition key values present in the table. \
Only the key is extracted; document values are not read. \
Useful for discovering what categories or groupings exist.",
        examples: &["LIST KEYS memories", "LIST KEYS memories LIMIT 10"],
    },
    CommandHelp {
        name: "LIST PREFIXES",
        summary: "List distinct sort key prefixes for a partition key",
        syntax: "LIST PREFIXES <table> pk=<value> [LIMIT <n>]",
        details: "\
Returns the distinct sort key prefixes (split on the first '#' delimiter) \
for a given partition key. Only the key bytes are read; document values \
are not deserialized.

Follows the DynamoDB convention of '#'-separated hierarchical sort keys. \
For example, sort keys 'ownership#borrowing' and 'ownership#moves' both \
produce the prefix 'ownership'.",
        examples: &[
            "LIST PREFIXES memories pk=rust-patterns",
            "LIST PREFIXES memories pk=rust-patterns LIMIT 5",
        ],
    },
    // -- Data Operations --
    CommandHelp {
        name: "PUT",
        summary: "Insert or replace an item",
        syntax: "PUT <table> {json}",
        details: "\
The JSON document must include the table's partition key (and sort key if defined).
If an item with the same key(s) already exists, it is replaced entirely.",
        examples: &[
            "PUT users {\"user_id\": \"alice\", \"name\": \"Alice\"}",
            "PUT events {\"user_id\": \"alice\", \"timestamp\": 1000, \"action\": \"login\"}",
        ],
    },
    CommandHelp {
        name: "GET",
        summary: "Retrieve a single item by key",
        syntax: "GET <table> pk=<value> [sk=<value>]",
        details: "\
Fetches exactly one item. Provide the sort key if the table has one.
Returns the full JSON document or \"Item not found.\"",
        examples: &[
            "GET users pk=alice",
            "GET events pk=alice sk=1000",
            "GET users pk=\"hello world\"",
        ],
    },
    CommandHelp {
        name: "DELETE",
        summary: "Remove an item by key",
        syntax: "DELETE <table> pk=<value> [sk=<value>]",
        details: "Deletes the item matching the given key(s). No error if the item does not exist.",
        examples: &["DELETE users pk=alice", "DELETE events pk=alice sk=1000"],
    },
    CommandHelp {
        name: "UPDATE",
        summary: "Atomically modify attributes of an existing item (or upsert)",
        syntax: "UPDATE <table> pk=<value> [sk=<value>] SET path=value [REMOVE path] [ADD path=value] [DELETE path=value]",
        details: "\
Actions (applied in order):
  SET path=value          Set an attribute to a value
  REMOVE path             Remove an attribute
  ADD path=value          Numeric increment or array union
  DELETE path=value       Array set difference (remove elements)

Paths support dot-separated nesting (e.g., address.city).
If the item does not exist, it is created (upsert).
Updates to partition key or sort key attributes are rejected.",
        examples: &[
            "UPDATE users pk=alice SET name=\"Alice\"",
            "UPDATE users pk=alice SET age=30 SET email=\"a@b.com\"",
            "UPDATE users pk=alice SET address.city=NYC",
            "UPDATE users pk=alice REMOVE old_field",
            "UPDATE users pk=alice ADD login_count=1",
            "UPDATE events pk=alice sk=1000 SET status=processed",
        ],
    },
    // -- Query & Scan --
    CommandHelp {
        name: "QUERY",
        summary: "Find items by partition key with optional sort key filters",
        syntax: "QUERY <table> pk=<value> [SK <op> <value>] [LIMIT <n>] [DESC]",
        details: "\
Sort key operators:
  SK = <value>              Exact match
  SK < <value>              Less than
  SK <= <value>             Less than or equal
  SK > <value>              Greater than
  SK >= <value>             Greater than or equal
  BETWEEN <lo> AND <hi>     Range (inclusive)
  BEGINS_WITH <prefix>      String prefix match

LIMIT restricts the number of results. DESC reverses sort order.",
        examples: &[
            "QUERY users pk=alice",
            "QUERY events pk=alice SK > 100 LIMIT 10",
            "QUERY events pk=alice SK >= 100 LIMIT 5 DESC",
            "QUERY events pk=alice BETWEEN 100 AND 500",
            "QUERY logs pk=server1 BEGINS_WITH \"2024-01\"",
        ],
    },
    CommandHelp {
        name: "SCAN",
        summary: "Read all items in a table",
        syntax: "SCAN <table> [LIMIT <n>]",
        details: "Returns every item in the table (unordered). Use LIMIT to cap results.",
        examples: &["SCAN users", "SCAN events LIMIT 20"],
    },
    // -- Other --
    CommandHelp {
        name: "HELP",
        summary: "Show this overview, or detailed help for a command",
        syntax: "HELP [command]",
        details: "Without arguments, lists all commands. With a command name, shows detailed syntax and examples.",
        examples: &["HELP", "HELP QUERY", "HELP CREATE TABLE"],
    },
    CommandHelp {
        name: "USE",
        summary: "Set or clear the active table for the session",
        syntax: "USE [table]",
        details: "\
Sets a default table so you can omit the table name from commands. \
With no argument, clears the active table.

When a table is active, the prompt changes to show it (e.g., ferridyn:data>). \
You can still specify a table explicitly on any command to override the default.

Works in REPL, pipe, and exec modes.",
        examples: &[
            "USE data",
            "SCAN LIMIT 5",
            "GET pk=alice",
            "SCAN other LIMIT 10",
            "USE",
        ],
    },
    CommandHelp {
        name: "EXIT / QUIT",
        summary: "Exit the console",
        syntax: "EXIT  (or QUIT)",
        details: "Closes the FerridynDB console session.",
        examples: &["EXIT", "QUIT"],
    },
];

fn find_command(topic: &str) -> Option<&'static CommandHelp> {
    let lower = topic.to_lowercase();
    COMMANDS
        .iter()
        .find(|cmd| topic_keys(cmd).iter().any(|k| *k == lower))
}

fn render_help_pretty(topic: Option<&str>) {
    match topic {
        None => print_help_overview(),
        Some(t) => match find_command(t) {
            Some(cmd) => print_command_help(cmd),
            None => {
                println!("Unknown help topic '{t}'. Type HELP to see available commands.");
            }
        },
    }
}

fn print_help_overview() {
    println!("FerridynDB Console \u{2014} Command Reference");
    println!();
    println!("  Table Management");
    println!("    CREATE TABLE   Create a new table with partition and optional sort key");
    println!("    DROP TABLE     Delete a table and all its data");
    println!("    LIST TABLES    Show all tables in the database");
    println!("    LIST KEYS      List distinct partition keys in a table");
    println!("    LIST PREFIXES  List distinct sort key prefixes for a partition key");
    println!("    DESCRIBE TABLE Show a table's key schema");
    println!();
    println!("  Partition Schemas");
    println!("    CREATE SCHEMA  Declare a partition schema on a table");
    println!("    DROP SCHEMA    Remove a partition schema from a table");
    println!("    LIST SCHEMAS   List partition schemas for a table");
    println!("    DESCRIBE SCHEMA Show details of a partition schema");
    println!();
    println!("  Data Operations");
    println!("    PUT            Insert or replace an item");
    println!("    GET            Retrieve a single item by key");
    println!("    DELETE         Remove an item by key");
    println!("    UPDATE         Atomically modify attributes of an item");
    println!();
    println!("  Query & Scan");
    println!("    QUERY          Find items by partition key with optional sort key filters");
    println!("    SCAN           Read all items in a table");
    println!();
    println!("  Secondary Indexes");
    println!("    CREATE INDEX   Create a secondary index on a table");
    println!("    DROP INDEX     Remove a secondary index from a table");
    println!("    LIST INDEXES   List secondary indexes for a table");
    println!("    DESCRIBE INDEX Show details of a secondary index");
    println!("    QUERY INDEX    Find items by secondary index value");
    println!();
    println!("  Session");
    println!("    USE [table]    Set or clear the active table for the session");
    println!();
    println!("  Other");
    println!("    HELP [command] Show this overview, or detailed help for a command");
    println!("    EXIT / QUIT    Exit the console");
    println!();
    println!("Type HELP <command> for detailed usage and examples.");
}

fn print_command_help(cmd: &CommandHelp) {
    println!("{} \u{2014} {}", cmd.name, cmd.summary);
    println!();
    println!("Syntax:");
    println!("  {}", cmd.syntax);
    if !cmd.details.is_empty() {
        println!();
        for line in cmd.details.lines() {
            if line.is_empty() {
                println!();
            } else {
                println!("{line}");
            }
        }
    }
    if !cmd.examples.is_empty() {
        println!();
        println!("Examples:");
        for ex in cmd.examples {
            println!("  {ex}");
        }
    }
}

fn render_help_json(topic: Option<&str>) {
    match topic {
        None => {
            let commands: Vec<Value> = COMMANDS
                .iter()
                .map(|cmd| {
                    json!({
                        "name": cmd.name,
                        "summary": cmd.summary,
                    })
                })
                .collect();
            println!("{}", json!({ "commands": commands }));
        }
        Some(t) => match find_command(t) {
            Some(cmd) => {
                println!(
                    "{}",
                    json!({
                        "command": cmd.name,
                        "summary": cmd.summary,
                        "syntax": cmd.syntax,
                        "details": cmd.details,
                        "examples": cmd.examples,
                    })
                );
            }
            None => {
                eprintln!("{}", json!({"error": format!("Unknown help topic '{t}'")}));
            }
        },
    }
}
