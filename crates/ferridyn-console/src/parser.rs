use serde_json::Value;

use crate::commands::{AttrType, Command, KeyType, SortClause, UpdateActionCmd};

/// Tokenize an input line into a vector of string tokens.
///
/// Handles:
/// - Whitespace-separated words
/// - Quoted strings: `"hello world"` becomes a single token (quotes preserved)
/// - JSON bodies: when `{` is encountered, scans to the matching `}` (tracking
///   nesting and string literals inside the JSON), returning the entire `{...}`
///   as one token
/// - Operators: `<=`, `>=`, `<`, `>`, `=` as separate tokens
fn tokenize(input: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // Skip whitespace.
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }

        // JSON body.
        if chars[i] == '{' {
            let start = i;
            let mut depth = 0;
            let mut in_string = false;
            loop {
                if i >= len {
                    return Err("Unterminated JSON object".to_string());
                }
                let c = chars[i];
                if in_string {
                    if c == '\\' {
                        // Skip escaped character.
                        i += 1;
                    } else if c == '"' {
                        in_string = false;
                    }
                } else {
                    match c {
                        '"' => in_string = true,
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                i += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                i += 1;
            }
            let token: String = chars[start..i].iter().collect();
            tokens.push(token);
            continue;
        }

        // JSON array.
        if chars[i] == '[' {
            let start = i;
            let mut depth = 0;
            let mut in_string = false;
            loop {
                if i >= len {
                    return Err("Unterminated JSON array".to_string());
                }
                let c = chars[i];
                if in_string {
                    if c == '\\' {
                        i += 1;
                    } else if c == '"' {
                        in_string = false;
                    }
                } else {
                    match c {
                        '"' => in_string = true,
                        '[' => depth += 1,
                        ']' => {
                            depth -= 1;
                            if depth == 0 {
                                i += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                i += 1;
            }
            let token: String = chars[start..i].iter().collect();
            tokens.push(token);
            continue;
        }

        // Quoted string.
        if chars[i] == '"' {
            let start = i;
            i += 1;
            while i < len && chars[i] != '"' {
                if chars[i] == '\\' {
                    i += 1; // skip escaped char
                }
                i += 1;
            }
            if i >= len {
                return Err("Unterminated quoted string".to_string());
            }
            i += 1; // skip closing quote
            let token: String = chars[start..i].iter().collect();
            tokens.push(token);
            continue;
        }

        // Operators: <=, >=, <, >
        // Note: `=` is NOT split here -- it is part of word tokens like `pk=alice`
        // and also appears as a standalone word token in `SK = 100`.
        if chars[i] == '<' || chars[i] == '>' {
            if i + 1 < len && chars[i + 1] == '=' {
                let token: String = chars[i..i + 2].iter().collect();
                tokens.push(token);
                i += 2;
            } else {
                tokens.push(chars[i].to_string());
                i += 1;
            }
            continue;
        }

        // Regular word token: everything up to whitespace, quote, brace, bracket, or angle bracket.
        // Special case: if the word ends with `=` and the next character is `"`,
        // consume the quoted string as part of this token (e.g., `pk="hello world"`).
        let start = i;
        while i < len
            && !chars[i].is_whitespace()
            && chars[i] != '"'
            && chars[i] != '{'
            && chars[i] != '['
            && chars[i] != '<'
            && chars[i] != '>'
        {
            i += 1;
        }
        // If we stopped at a `"` and the last char was `=`, absorb the quoted string.
        if i < len && chars[i] == '"' && i > start && chars[i - 1] == '=' {
            i += 1; // skip opening quote
            while i < len && chars[i] != '"' {
                if chars[i] == '\\' {
                    i += 1;
                }
                i += 1;
            }
            if i >= len {
                return Err("Unterminated quoted string".to_string());
            }
            i += 1; // skip closing quote
        }
        if i > start {
            let token: String = chars[start..i].iter().collect();
            tokens.push(token);
        }
    }

    Ok(tokens)
}

/// Parse a raw token into a JSON value.
///
/// - If wrapped in `"..."`, strip the quotes and return a JSON string.
/// - If parseable as `f64`, return a JSON number.
/// - Otherwise, return a JSON string (bare word).
fn parse_value(s: &str) -> Value {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        let inner = &s[1..s.len() - 1];
        Value::String(inner.to_string())
    } else if let Ok(n) = s.parse::<f64>() {
        serde_json::Number::from_f64(n).map_or_else(|| Value::String(s.to_string()), Value::Number)
    } else {
        Value::String(s.to_string())
    }
}

/// Parse a key type token: STRING, NUMBER, or BINARY (case-insensitive).
fn parse_key_type(s: &str) -> Result<KeyType, String> {
    match s.to_uppercase().as_str() {
        "STRING" => Ok(KeyType::String),
        "NUMBER" => Ok(KeyType::Number),
        "BINARY" => Ok(KeyType::Binary),
        _ => Err(format!(
            "Invalid key type '{s}'. Expected STRING, NUMBER, or BINARY."
        )),
    }
}

/// Parse a `key=value` pair token such as `pk=alice` or `sk=42`.
///
/// Splits on the first `=` and parses the value portion.
fn parse_kv_pair(token: &str) -> Result<(&str, Value), String> {
    let eq_pos = token
        .find('=')
        .ok_or_else(|| format!("Expected key=value pair, got '{token}'"))?;
    let key = &token[..eq_pos];
    let val_str = &token[eq_pos + 1..];
    if val_str.is_empty() {
        return Err(format!("Missing value after '=' in '{token}'"));
    }
    Ok((key, parse_value(val_str)))
}

/// Parse an input line into a [`Command`].
pub fn parse(input: &str, default_table: Option<&str>) -> Result<Command, String> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err("Empty command".to_string());
    }

    let first = tokens[0].to_uppercase();
    match first.as_str() {
        "CREATE" => {
            if tokens.len() < 2 {
                return Err("Expected TABLE, SCHEMA, or INDEX after CREATE".to_string());
            }
            match tokens[1].to_uppercase().as_str() {
                "TABLE" => parse_create_table(&tokens, default_table),
                "SCHEMA" => parse_create_schema(&tokens, default_table),
                "INDEX" => parse_create_index(&tokens, default_table),
                _ => Err("Expected TABLE, SCHEMA, or INDEX after CREATE".to_string()),
            }
        }
        "DROP" => {
            if tokens.len() < 2 {
                return Err("Expected TABLE, SCHEMA, or INDEX after DROP".to_string());
            }
            match tokens[1].to_uppercase().as_str() {
                "TABLE" => parse_drop_table(&tokens, default_table),
                "SCHEMA" => parse_drop_schema(&tokens, default_table),
                "INDEX" => parse_drop_index(&tokens, default_table),
                _ => Err("Expected TABLE, SCHEMA, or INDEX after DROP".to_string()),
            }
        }
        "LIST" => parse_list(&tokens, default_table),
        "DESCRIBE" => {
            if tokens.len() < 2 {
                return Err("Expected TABLE, SCHEMA, or INDEX after DESCRIBE".to_string());
            }
            match tokens[1].to_uppercase().as_str() {
                "TABLE" => parse_describe_table(&tokens, default_table),
                "SCHEMA" => parse_describe_schema(&tokens, default_table),
                "INDEX" => parse_describe_index(&tokens, default_table),
                _ => Err("Expected TABLE, SCHEMA, or INDEX after DESCRIBE".to_string()),
            }
        }
        "USE" => parse_use(&tokens),
        "PUT" => parse_put(&tokens, default_table),
        "GET" => parse_get(&tokens, default_table),
        "DELETE" => parse_delete(&tokens, default_table),
        "QUERY" => {
            if tokens.len() > 1 && tokens[1].to_uppercase() == "INDEX" {
                parse_query_index(&tokens, default_table)
            } else {
                parse_query(&tokens, default_table)
            }
        }
        "SCAN" => parse_scan(&tokens, default_table),
        "UPDATE" => parse_update(&tokens, default_table),
        "HELP" => {
            let topic = if tokens.len() > 1 {
                Some(tokens[1..].join(" "))
            } else {
                None
            };
            Ok(Command::Help(topic))
        }
        "EXIT" | "QUIT" => Ok(Command::Exit),
        _ => Err(format!("Unknown command '{}'", tokens[0])),
    }
}

/// USE [table]
fn parse_use(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 2 {
        Ok(Command::Use { table: None })
    } else {
        Ok(Command::Use {
            table: Some(tokens[1].clone()),
        })
    }
}

/// CREATE TABLE <name> PK <attr> <type> [SK <attr> <type>]
fn parse_create_table(tokens: &[String], _default_table: Option<&str>) -> Result<Command, String> {
    if tokens.len() < 5 {
        return Err(
            "Usage: CREATE TABLE <name> PK <attr> <STRING|NUMBER|BINARY> [SK <attr> <type>] [TTL <attr>]  (Type HELP CREATE TABLE for details)"
                .to_string(),
        );
    }
    if tokens[1].to_uppercase() != "TABLE" {
        return Err(format!("Expected TABLE after CREATE, got '{}'", tokens[1]));
    }
    let name = tokens[2].clone();

    if tokens[3].to_uppercase() != "PK" {
        return Err(format!("Expected PK, got '{}'", tokens[3]));
    }
    if tokens.len() < 6 {
        return Err("Missing partition key attribute name or type".to_string());
    }
    let pk_name = tokens[4].clone();
    let pk_type = parse_key_type(&tokens[5])?;

    let sk = if tokens.len() > 6 && tokens[6].to_uppercase() == "SK" {
        if tokens.len() < 9 {
            return Err("Missing sort key attribute name or type".to_string());
        }
        let sk_name = tokens[7].clone();
        let sk_type = parse_key_type(&tokens[8])?;
        Some((sk_name, sk_type))
    } else {
        None
    };

    // Determine the start index for optional TTL clause.
    let ttl_start = if sk.is_some() { 9 } else { 6 };
    let ttl = if tokens.len() > ttl_start && tokens[ttl_start].to_uppercase() == "TTL" {
        if tokens.len() < ttl_start + 2 {
            return Err(
                "TTL requires an attribute name  (Type HELP CREATE TABLE for details)".to_string(),
            );
        }
        Some(tokens[ttl_start + 1].clone())
    } else {
        None
    };

    Ok(Command::CreateTable {
        name,
        pk_name,
        pk_type,
        sk,
        ttl,
    })
}

/// DROP TABLE <name>
fn parse_drop_table(tokens: &[String], _default_table: Option<&str>) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err("Usage: DROP TABLE <name>  (Type HELP DROP TABLE for details)".to_string());
    }
    if tokens[1].to_uppercase() != "TABLE" {
        return Err(format!("Expected TABLE after DROP, got '{}'", tokens[1]));
    }
    Ok(Command::DropTable {
        name: tokens[2].clone(),
    })
}

/// LIST TABLES | LIST KEYS [table] [LIMIT <n>]
fn parse_list(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    if tokens.len() < 2 {
        return Err(
            "Usage: LIST TABLES | LIST KEYS [table] | LIST PREFIXES [table] pk=<val>  (Type HELP LIST for details)"
                .to_string(),
        );
    }
    match tokens[1].to_uppercase().as_str() {
        "TABLES" => Ok(Command::ListTables),
        "KEYS" => parse_list_keys(tokens, default_table),
        "PREFIXES" => parse_list_prefixes(tokens, default_table),
        "SCHEMAS" => parse_list_schemas(tokens, default_table),
        "INDEXES" => parse_list_indexes(tokens, default_table),
        _ => Err(format!(
            "Expected TABLES, KEYS, PREFIXES, SCHEMAS, or INDEXES after LIST, got '{}'",
            tokens[1]
        )),
    }
}

/// LIST KEYS [table] [LIMIT <n>]
fn parse_list_keys(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    // Detect: tokens[2] is table if it exists AND is NOT "LIMIT"
    let (table, offset) = if tokens.len() < 3 || tokens[2].to_uppercase() == "LIMIT" {
        (resolve_table(None, default_table)?, 2)
    } else {
        (tokens[2].clone(), 3)
    };
    let mut limit = None;

    if tokens.len() > offset {
        if tokens[offset].to_uppercase() == "LIMIT" {
            if tokens.len() < offset + 2 {
                return Err("LIMIT requires a number".to_string());
            }
            limit = Some(
                tokens[offset + 1]
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid LIMIT value '{}'", tokens[offset + 1]))?,
            );
        } else {
            return Err(format!(
                "Unexpected token '{}' in LIST KEYS",
                tokens[offset]
            ));
        }
    }

    Ok(Command::ListKeys { table, limit })
}

/// LIST PREFIXES [table] pk=<value> [LIMIT <n>]
fn parse_list_prefixes(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: LIST PREFIXES [table] pk=<value> [LIMIT <n>]  (Type HELP LIST PREFIXES for details)"
                .to_string(),
        );
    }
    // Detect: tokens[2] is table if it does NOT contain '='
    let (table, offset) = if tokens[2].contains('=') {
        (resolve_table(None, default_table)?, 2)
    } else {
        if tokens.len() < 4 {
            return Err(
                "Usage: LIST PREFIXES [table] pk=<value> [LIMIT <n>]  (Type HELP LIST PREFIXES for details)"
                    .to_string(),
            );
        }
        (tokens[2].clone(), 3)
    };

    let (key_name, pk) = parse_kv_pair(&tokens[offset])?;
    if key_name.to_uppercase() != "PK" {
        return Err(format!("Expected pk=<value>, got '{}'", tokens[offset]));
    }

    let mut limit = None;
    if tokens.len() > offset + 1 {
        if tokens[offset + 1].to_uppercase() == "LIMIT" {
            if tokens.len() < offset + 3 {
                return Err("LIMIT requires a number".to_string());
            }
            limit = Some(
                tokens[offset + 2]
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid LIMIT value '{}'", tokens[offset + 2]))?,
            );
        } else {
            return Err(format!(
                "Unexpected token '{}' in LIST PREFIXES",
                tokens[offset + 1]
            ));
        }
    }

    Ok(Command::ListPrefixes { table, pk, limit })
}

/// DESCRIBE TABLE [name]
fn parse_describe_table(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    if tokens[1].to_uppercase() != "TABLE" {
        return Err(format!(
            "Expected TABLE after DESCRIBE, got '{}'",
            tokens[1]
        ));
    }
    let name = if tokens.len() >= 3 {
        tokens[2].clone()
    } else {
        resolve_table(None, default_table)?
    };
    Ok(Command::DescribeTable { name })
}

/// PUT [table] {json}
fn parse_put(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    if tokens.len() < 2 {
        return Err("Usage: PUT [table] {json}  (Type HELP PUT for details)".to_string());
    }
    // Detect: if tokens[1] starts with '{', no table name given
    let (table, json_idx) = if tokens[1].starts_with('{') {
        (resolve_table(None, default_table)?, 1)
    } else {
        if tokens.len() < 3 {
            return Err("Usage: PUT [table] {json}  (Type HELP PUT for details)".to_string());
        }
        (tokens[1].clone(), 2)
    };
    let json_str = &tokens[json_idx];
    let document: Value =
        serde_json::from_str(json_str).map_err(|e| format!("Invalid JSON: {e}"))?;
    Ok(Command::Put { table, document })
}

/// GET [table] pk=<value> [sk=<value>]
fn parse_get(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    if tokens.len() < 2 {
        return Err(
            "Usage: GET [table] pk=<value> [sk=<value>]  (Type HELP GET for details)".to_string(),
        );
    }
    // Detect: if tokens[1] contains '=', no table name given
    let (table, offset) = if tokens[1].contains('=') {
        (resolve_table(None, default_table)?, 1)
    } else {
        if tokens.len() < 3 {
            return Err(
                "Usage: GET [table] pk=<value> [sk=<value>]  (Type HELP GET for details)"
                    .to_string(),
            );
        }
        (tokens[1].clone(), 2)
    };

    let (key_name, pk) = parse_kv_pair(&tokens[offset])?;
    if key_name.to_uppercase() != "PK" {
        return Err(format!("Expected pk=<value>, got '{}'", tokens[offset]));
    }

    let sk = if tokens.len() > offset + 1 {
        let (sk_key, sk_val) = parse_kv_pair(&tokens[offset + 1])?;
        if sk_key.to_uppercase() != "SK" {
            return Err(format!("Expected sk=<value>, got '{}'", tokens[offset + 1]));
        }
        Some(sk_val)
    } else {
        None
    };

    Ok(Command::Get { table, pk, sk })
}

/// DELETE [table] pk=<value> [sk=<value>]
fn parse_delete(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    if tokens.len() < 2 {
        return Err(
            "Usage: DELETE [table] pk=<value> [sk=<value>]  (Type HELP DELETE for details)"
                .to_string(),
        );
    }
    // Detect: if tokens[1] contains '=', no table name given
    let (table, offset) = if tokens[1].contains('=') {
        (resolve_table(None, default_table)?, 1)
    } else {
        if tokens.len() < 3 {
            return Err(
                "Usage: DELETE [table] pk=<value> [sk=<value>]  (Type HELP DELETE for details)"
                    .to_string(),
            );
        }
        (tokens[1].clone(), 2)
    };

    let (key_name, pk) = parse_kv_pair(&tokens[offset])?;
    if key_name.to_uppercase() != "PK" {
        return Err(format!("Expected pk=<value>, got '{}'", tokens[offset]));
    }

    let sk = if tokens.len() > offset + 1 {
        let (sk_key, sk_val) = parse_kv_pair(&tokens[offset + 1])?;
        if sk_key.to_uppercase() != "SK" {
            return Err(format!("Expected sk=<value>, got '{}'", tokens[offset + 1]));
        }
        Some(sk_val)
    } else {
        None
    };

    Ok(Command::Delete { table, pk, sk })
}

/// UPDATE [table] pk=<value> [sk=<value>] SET path=value ... REMOVE path ...
///   ADD path=value ... DELETE path=value ...
fn parse_update(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: UPDATE [table] pk=<value> [sk=<value>] SET path=value [REMOVE path] [ADD path=value] [DELETE path=value]  (Type HELP UPDATE for details)"
                .to_string(),
        );
    }
    // Detect: tokens[1] is table if it does NOT contain '='
    let (table, offset) = if tokens[1].contains('=') {
        (resolve_table(None, default_table)?, 1)
    } else {
        if tokens.len() < 4 {
            return Err(
                "Usage: UPDATE [table] pk=<value> [sk=<value>] SET path=value [REMOVE path] [ADD path=value] [DELETE path=value]  (Type HELP UPDATE for details)"
                    .to_string(),
            );
        }
        (tokens[1].clone(), 2)
    };

    let (key_name, pk) = parse_kv_pair(&tokens[offset])?;
    if key_name.to_uppercase() != "PK" {
        return Err(format!("Expected pk=<value>, got '{}'", tokens[offset]));
    }

    let mut sk = None;
    let mut action_start = offset + 1;

    // Check for optional sk=<value>
    if action_start < tokens.len() && tokens[action_start].contains('=') {
        let upper_prefix: String = tokens[action_start]
            .chars()
            .take_while(|c| *c != '=')
            .collect();
        if upper_prefix.to_uppercase() == "SK" {
            let (_, sk_val) = parse_kv_pair(&tokens[action_start])?;
            sk = Some(sk_val);
            action_start += 1;
        }
    }

    let mut actions = Vec::new();
    let mut i = action_start;

    while i < tokens.len() {
        let upper = tokens[i].to_uppercase();
        match upper.as_str() {
            "SET" | "ADD" => {
                let action_name = upper.clone();
                i += 1;
                if i >= tokens.len() {
                    return Err(format!("{action_name} requires path=value"));
                }
                let (path, value) = if tokens[i].ends_with('=') && i + 1 < tokens.len() {
                    // Value is in the next token (JSON body: {...} or [...])
                    let path = &tokens[i][..tokens[i].len() - 1];
                    i += 1;
                    let val: serde_json::Value = serde_json::from_str(&tokens[i])
                        .map_err(|e| format!("Invalid JSON value: {e}"))?;
                    (path.to_string(), val)
                } else {
                    let (p, v) = parse_kv_pair(&tokens[i])?;
                    (p.to_string(), v)
                };
                if action_name == "SET" {
                    actions.push(UpdateActionCmd::Set(path, value));
                } else {
                    actions.push(UpdateActionCmd::Add(path, value));
                }
                i += 1;
            }
            "REMOVE" => {
                i += 1;
                if i >= tokens.len() {
                    return Err("REMOVE requires a path".to_string());
                }
                actions.push(UpdateActionCmd::Remove(tokens[i].clone()));
                i += 1;
            }
            "DELETE" => {
                i += 1;
                if i >= tokens.len() {
                    return Err("DELETE requires path=value".to_string());
                }
                let (path, value) = if tokens[i].ends_with('=') && i + 1 < tokens.len() {
                    let path = &tokens[i][..tokens[i].len() - 1];
                    i += 1;
                    let val: serde_json::Value = serde_json::from_str(&tokens[i])
                        .map_err(|e| format!("Invalid JSON value: {e}"))?;
                    (path.to_string(), val)
                } else {
                    let (p, v) = parse_kv_pair(&tokens[i])?;
                    (p.to_string(), v)
                };
                actions.push(UpdateActionCmd::Delete(path, value));
                i += 1;
            }
            _ => {
                return Err(format!(
                    "Unexpected token '{}' in UPDATE. Expected SET, REMOVE, ADD, or DELETE.",
                    tokens[i]
                ));
            }
        }
    }

    if actions.is_empty() {
        return Err(
            "UPDATE requires at least one action (SET, REMOVE, ADD, or DELETE)".to_string(),
        );
    }

    Ok(Command::Update {
        table,
        pk,
        sk,
        actions,
    })
}

/// QUERY [table] pk=<value> [SK <op> <value>] [BETWEEN <lo> AND <hi>]
/// [BEGINS_WITH <prefix>] [LIMIT <n>] [DESC]
fn parse_query(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    if tokens.len() < 2 {
        return Err(
            "Usage: QUERY [table] pk=<value> [SK <op> <val>] [LIMIT <n>] [DESC]  (Type HELP QUERY for details)".to_string(),
        );
    }
    // Detect: tokens[1] is table if it does NOT contain '=' and is not "INDEX"
    let (table, offset) = if tokens[1].contains('=') {
        (resolve_table(None, default_table)?, 1)
    } else {
        if tokens.len() < 3 {
            return Err(
                "Usage: QUERY [table] pk=<value> [SK <op> <val>] [LIMIT <n>] [DESC]  (Type HELP QUERY for details)".to_string(),
            );
        }
        (tokens[1].clone(), 2)
    };

    let (key_name, pk) = parse_kv_pair(&tokens[offset])?;
    if key_name.to_uppercase() != "PK" {
        return Err(format!("Expected pk=<value>, got '{}'", tokens[offset]));
    }

    let mut sort_condition: Option<SortClause> = None;
    let mut limit: Option<usize> = None;
    let mut desc = false;
    let mut i = offset + 1;

    while i < tokens.len() {
        let upper = tokens[i].to_uppercase();
        match upper.as_str() {
            "BETWEEN" => {
                if i + 3 >= tokens.len() {
                    return Err("BETWEEN requires: BETWEEN <lo> AND <hi>".to_string());
                }
                let lo = parse_value(&tokens[i + 1]);
                if tokens[i + 2].to_uppercase() != "AND" {
                    return Err(format!(
                        "Expected AND after BETWEEN <lo>, got '{}'",
                        tokens[i + 2]
                    ));
                }
                let hi = parse_value(&tokens[i + 3]);
                sort_condition = Some(SortClause::Between(lo, hi));
                i += 4;
            }
            "BEGINS_WITH" => {
                if i + 1 >= tokens.len() {
                    return Err("BEGINS_WITH requires a prefix argument".to_string());
                }
                let prefix = strip_quotes(&tokens[i + 1]);
                sort_condition = Some(SortClause::BeginsWith(prefix));
                i += 2;
            }
            "SK" => {
                // SK <op> <value>
                if i + 2 >= tokens.len() {
                    return Err("SK requires: SK <op> <value>".to_string());
                }
                let op = &tokens[i + 1];
                let val = parse_value(&tokens[i + 2]);
                sort_condition = Some(match op.as_str() {
                    "=" => SortClause::Eq(val),
                    "<" => SortClause::Lt(val),
                    "<=" => SortClause::Le(val),
                    ">" => SortClause::Gt(val),
                    ">=" => SortClause::Ge(val),
                    _ => return Err(format!("Unknown sort key operator '{op}'")),
                });
                i += 3;
            }
            "LIMIT" => {
                if i + 1 >= tokens.len() {
                    return Err("LIMIT requires a number".to_string());
                }
                limit = Some(
                    tokens[i + 1]
                        .parse::<usize>()
                        .map_err(|_| format!("Invalid LIMIT value '{}'", tokens[i + 1]))?,
                );
                i += 2;
            }
            "DESC" => {
                desc = true;
                i += 1;
            }
            _ => {
                return Err(format!("Unexpected token '{}' in QUERY", tokens[i]));
            }
        }
    }

    Ok(Command::Query {
        table,
        pk,
        sort_condition,
        limit,
        desc,
    })
}

/// SCAN [table] [LIMIT <n>]
fn parse_scan(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    // Detect: tokens[1] is table if it exists AND is NOT "LIMIT" (case-insensitive)
    let (table, offset) = if tokens.len() < 2 || tokens[1].to_uppercase() == "LIMIT" {
        (resolve_table(None, default_table)?, 1)
    } else {
        (tokens[1].clone(), 2)
    };
    let mut limit = None;

    if tokens.len() > offset {
        if tokens[offset].to_uppercase() == "LIMIT" {
            if tokens.len() < offset + 2 {
                return Err("LIMIT requires a number".to_string());
            }
            limit = Some(
                tokens[offset + 1]
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid LIMIT value '{}'", tokens[offset + 1]))?,
            );
        } else {
            return Err(format!("Unexpected token '{}' in SCAN", tokens[offset]));
        }
    }

    Ok(Command::Scan { table, limit })
}

/// Strip surrounding double quotes from a string, if present.
fn strip_quotes(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

fn resolve_table(explicit: Option<String>, default: Option<&str>) -> Result<String, String> {
    explicit
        .or_else(|| default.map(String::from))
        .ok_or_else(|| "No table specified. Set a default with: USE <table>".to_string())
}

/// Parse an attribute type token: STRING, NUMBER, or BOOLEAN (case-insensitive).
fn parse_attr_type(s: &str) -> Result<AttrType, String> {
    match s.to_uppercase().as_str() {
        "STRING" => Ok(AttrType::String),
        "NUMBER" => Ok(AttrType::Number),
        "BOOLEAN" => Ok(AttrType::Boolean),
        _ => Err(format!(
            "Invalid attribute type '{s}'. Expected STRING, NUMBER, or BOOLEAN."
        )),
    }
}

/// CREATE SCHEMA [table] PREFIX <prefix> [DESCRIPTION "text"]
///   [ATTR <name> <STRING|NUMBER|BOOLEAN> [REQUIRED]]... [VALIDATE]
fn parse_create_schema(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    // Minimum: CREATE SCHEMA PREFIX <prefix> = 4 tokens (without table)
    if tokens.len() < 4 {
        return Err(
            "Usage: CREATE SCHEMA [table] PREFIX <prefix> [DESCRIPTION \"text\"] [ATTR <name> <type> [REQUIRED]]... [VALIDATE]  (Type HELP CREATE SCHEMA for details)"
                .to_string(),
        );
    }
    // Detect: tokens[2] is table if it is NOT "PREFIX" (case-insensitive)
    let (table, offset) = if tokens[2].to_uppercase() == "PREFIX" {
        (resolve_table(None, default_table)?, 0)
    } else {
        if tokens.len() < 5 {
            return Err(
                "Usage: CREATE SCHEMA [table] PREFIX <prefix> [DESCRIPTION \"text\"] [ATTR <name> <type> [REQUIRED]]... [VALIDATE]  (Type HELP CREATE SCHEMA for details)"
                    .to_string(),
            );
        }
        (tokens[2].clone(), 1)
    };
    // PREFIX keyword is at 2+offset (i.e., 3 with table, 2 without)
    if tokens[2 + offset].to_uppercase() != "PREFIX" {
        return Err(format!(
            "Expected PREFIX after table name, got '{}'",
            tokens[2 + offset]
        ));
    }
    let prefix = tokens[3 + offset].clone();

    let mut description = None;
    let mut attributes = Vec::new();
    let mut validate = false;
    let mut i = 4 + offset;

    while i < tokens.len() {
        let upper = tokens[i].to_uppercase();
        match upper.as_str() {
            "DESCRIPTION" => {
                if i + 1 >= tokens.len() {
                    return Err("DESCRIPTION requires a value".to_string());
                }
                description = Some(strip_quotes(&tokens[i + 1]));
                i += 2;
            }
            "ATTR" => {
                if i + 2 >= tokens.len() {
                    return Err("ATTR requires a name and type".to_string());
                }
                let attr_name = tokens[i + 1].clone();
                let attr_type = parse_attr_type(&tokens[i + 2])?;
                let required = if i + 3 < tokens.len() && tokens[i + 3].to_uppercase() == "REQUIRED"
                {
                    i += 4;
                    true
                } else {
                    i += 3;
                    false
                };
                attributes.push((attr_name, attr_type, required));
            }
            "VALIDATE" => {
                validate = true;
                i += 1;
            }
            _ => {
                return Err(format!("Unexpected token '{}' in CREATE SCHEMA", tokens[i]));
            }
        }
    }

    Ok(Command::CreateSchema {
        table,
        prefix,
        description,
        attributes,
        validate,
    })
}

/// CREATE INDEX [table] <name> SCHEMA <prefix> KEY <attr> <type>
fn parse_create_index(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    // Without table: CREATE INDEX <name> SCHEMA <prefix> KEY <attr> <type> = 8 tokens
    // With table:    CREATE INDEX <table> <name> SCHEMA <prefix> KEY <attr> <type> = 9 tokens
    if tokens.len() < 8 {
        return Err(
            "Usage: CREATE INDEX [table] <name> SCHEMA <prefix> KEY <attr> <STRING|NUMBER|BINARY>  (Type HELP CREATE INDEX for details)"
                .to_string(),
        );
    }
    // Detect: if tokens[3] == "SCHEMA", no table given (tokens[2] is index name)
    let (table, name, offset) = if tokens[3].to_uppercase() == "SCHEMA" {
        (resolve_table(None, default_table)?, tokens[2].clone(), 0)
    } else {
        if tokens.len() < 9 {
            return Err(
                "Usage: CREATE INDEX [table] <name> SCHEMA <prefix> KEY <attr> <STRING|NUMBER|BINARY>  (Type HELP CREATE INDEX for details)"
                    .to_string(),
            );
        }
        (tokens[2].clone(), tokens[3].clone(), 1)
    };
    // SCHEMA keyword at 3+offset
    if tokens[3 + offset].to_uppercase() != "SCHEMA" {
        return Err(format!("Expected SCHEMA, got '{}'", tokens[3 + offset]));
    }
    let schema_prefix = tokens[4 + offset].clone();
    if tokens[5 + offset].to_uppercase() != "KEY" {
        return Err(format!("Expected KEY, got '{}'", tokens[5 + offset]));
    }
    let key_attr = tokens[6 + offset].clone();
    let key_type = parse_key_type(&tokens[7 + offset])?;

    Ok(Command::CreateIndex {
        table,
        name,
        schema_prefix,
        key_attr,
        key_type,
    })
}

/// DROP SCHEMA [table] <prefix>
fn parse_drop_schema(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: DROP SCHEMA [table] <prefix>  (Type HELP DROP SCHEMA for details)".to_string(),
        );
    }
    // If tokens.len() == 3, only prefix given -> no table -> use default
    // If tokens.len() >= 4, tokens[2] is table and tokens[3] is prefix
    let (table, prefix) = if tokens.len() == 3 {
        (resolve_table(None, default_table)?, tokens[2].clone())
    } else {
        (tokens[2].clone(), tokens[3].clone())
    };
    Ok(Command::DropSchema { table, prefix })
}

/// DROP INDEX [table] <name>
fn parse_drop_index(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: DROP INDEX [table] <name>  (Type HELP DROP INDEX for details)".to_string(),
        );
    }
    // If tokens.len() == 3, only name given -> no table -> use default
    // If tokens.len() >= 4, tokens[2] is table and tokens[3] is name
    let (table, name) = if tokens.len() == 3 {
        (resolve_table(None, default_table)?, tokens[2].clone())
    } else {
        (tokens[2].clone(), tokens[3].clone())
    };
    Ok(Command::DropIndex { table, name })
}

/// DESCRIBE SCHEMA [table] <prefix>
fn parse_describe_schema(
    tokens: &[String],
    default_table: Option<&str>,
) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: DESCRIBE SCHEMA [table] <prefix>  (Type HELP DESCRIBE SCHEMA for details)"
                .to_string(),
        );
    }
    // If tokens.len() == 3, only prefix given -> no table -> use default
    // If tokens.len() >= 4, tokens[2] is table and tokens[3] is prefix
    let (table, prefix) = if tokens.len() == 3 {
        (resolve_table(None, default_table)?, tokens[2].clone())
    } else {
        (tokens[2].clone(), tokens[3].clone())
    };
    Ok(Command::DescribeSchema { table, prefix })
}

/// DESCRIBE INDEX [table] <name>
fn parse_describe_index(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: DESCRIBE INDEX [table] <name>  (Type HELP DESCRIBE INDEX for details)"
                .to_string(),
        );
    }
    // If tokens.len() == 3, only name given -> no table -> use default
    // If tokens.len() >= 4, tokens[2] is table and tokens[3] is name
    let (table, name) = if tokens.len() == 3 {
        (resolve_table(None, default_table)?, tokens[2].clone())
    } else {
        (tokens[2].clone(), tokens[3].clone())
    };
    Ok(Command::DescribeIndex { table, name })
}

/// LIST SCHEMAS [table]
fn parse_list_schemas(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    let table = if tokens.len() < 3 {
        resolve_table(None, default_table)?
    } else {
        tokens[2].clone()
    };
    Ok(Command::ListSchemas { table })
}

/// LIST INDEXES [table]
fn parse_list_indexes(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    let table = if tokens.len() < 3 {
        resolve_table(None, default_table)?
    } else {
        tokens[2].clone()
    };
    Ok(Command::ListIndexes { table })
}

/// QUERY INDEX [table] <index_name> key=<value> [LIMIT <n>] [DESC]
fn parse_query_index(tokens: &[String], default_table: Option<&str>) -> Result<Command, String> {
    // Without table: QUERY INDEX <index_name> key=<value> = 4 tokens
    // With table:    QUERY INDEX <table> <index_name> key=<value> = 5 tokens
    if tokens.len() < 4 {
        return Err(
            "Usage: QUERY INDEX [table] <index_name> key=<value> [LIMIT <n>] [DESC]  (Type HELP QUERY INDEX for details)"
                .to_string(),
        );
    }
    // Detect: if tokens[3] contains '=', tokens[2] is the index name (no table)
    let (table, index_name, key_idx) = if tokens[3].contains('=') {
        (resolve_table(None, default_table)?, tokens[2].clone(), 3)
    } else {
        if tokens.len() < 5 {
            return Err(
                "Usage: QUERY INDEX [table] <index_name> key=<value> [LIMIT <n>] [DESC]  (Type HELP QUERY INDEX for details)"
                    .to_string(),
            );
        }
        (tokens[2].clone(), tokens[3].clone(), 4)
    };

    let (key_name, key_value) = parse_kv_pair(&tokens[key_idx])?;
    if key_name.to_uppercase() != "KEY" {
        return Err(format!("Expected key=<value>, got '{}'", tokens[key_idx]));
    }

    let mut limit = None;
    let mut desc = false;
    let mut i = key_idx + 1;

    while i < tokens.len() {
        let upper = tokens[i].to_uppercase();
        match upper.as_str() {
            "LIMIT" => {
                if i + 1 >= tokens.len() {
                    return Err("LIMIT requires a number".to_string());
                }
                limit = Some(
                    tokens[i + 1]
                        .parse::<usize>()
                        .map_err(|_| format!("Invalid LIMIT value '{}'", tokens[i + 1]))?,
                );
                i += 2;
            }
            "DESC" => {
                desc = true;
                i += 1;
            }
            _ => {
                return Err(format!("Unexpected token '{}' in QUERY INDEX", tokens[i]));
            }
        }
    }

    Ok(Command::QueryIndex {
        table,
        index_name,
        key_value,
        limit,
        desc,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -----------------------------------------------------------------------
    // CREATE TABLE
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_table_pk_only() {
        let cmd = parse("CREATE TABLE users PK user_id STRING", None).unwrap();
        match cmd {
            Command::CreateTable {
                name,
                pk_name,
                pk_type,
                sk,
                ttl,
            } => {
                assert_eq!(name, "users");
                assert_eq!(pk_name, "user_id");
                assert_eq!(pk_type, KeyType::String);
                assert!(sk.is_none());
                assert!(ttl.is_none());
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_create_table_with_sort_key() {
        let cmd = parse(
            "CREATE TABLE events PK user_id STRING SK timestamp NUMBER",
            None,
        )
        .unwrap();
        match cmd {
            Command::CreateTable {
                name,
                pk_name,
                pk_type,
                sk,
                ttl,
            } => {
                assert_eq!(name, "events");
                assert_eq!(pk_name, "user_id");
                assert_eq!(pk_type, KeyType::String);
                let (sk_name, sk_type) = sk.unwrap();
                assert_eq!(sk_name, "timestamp");
                assert_eq!(sk_type, KeyType::Number);
                assert!(ttl.is_none());
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_create_table_case_insensitive() {
        let cmd = parse("create table Items pk id string", None).unwrap();
        match cmd {
            Command::CreateTable {
                name, pk_type, ttl, ..
            } => {
                assert_eq!(name, "Items");
                assert_eq!(pk_type, KeyType::String);
                assert!(ttl.is_none());
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_create_table_binary_key() {
        let cmd = parse("CREATE TABLE blobs PK hash BINARY", None).unwrap();
        match cmd {
            Command::CreateTable { pk_type, ttl, .. } => {
                assert_eq!(pk_type, KeyType::Binary);
                assert!(ttl.is_none());
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_create_table_with_ttl() {
        let cmd = parse("CREATE TABLE cache PK key STRING TTL expires", None).unwrap();
        match cmd {
            Command::CreateTable { name, ttl, sk, .. } => {
                assert_eq!(name, "cache");
                assert!(sk.is_none());
                assert_eq!(ttl, Some("expires".to_string()));
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_create_table_with_sk_and_ttl() {
        let cmd = parse(
            "CREATE TABLE cache PK key STRING SK sort NUMBER TTL expires",
            None,
        )
        .unwrap();
        match cmd {
            Command::CreateTable { sk, ttl, .. } => {
                assert!(sk.is_some());
                assert_eq!(ttl, Some("expires".to_string()));
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_create_table_ttl_missing_attr_name() {
        let result = parse("CREATE TABLE cache PK key STRING TTL", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("TTL requires"));
    }

    // -----------------------------------------------------------------------
    // PUT
    // -----------------------------------------------------------------------

    #[test]
    fn test_put_with_json() {
        let cmd = parse(r#"PUT users {"user_id": "alice", "name": "Alice"}"#, None).unwrap();
        match cmd {
            Command::Put { table, document } => {
                assert_eq!(table, "users");
                assert_eq!(document["user_id"], "alice");
                assert_eq!(document["name"], "Alice");
            }
            _ => panic!("Expected Put"),
        }
    }

    #[test]
    fn test_put_with_nested_json() {
        let cmd = parse(
            r#"PUT items {"id": "x", "meta": {"nested": true, "count": 5}}"#,
            None,
        )
        .unwrap();
        match cmd {
            Command::Put { document, .. } => {
                assert_eq!(document["meta"]["nested"], true);
                assert_eq!(document["meta"]["count"], 5);
            }
            _ => panic!("Expected Put"),
        }
    }

    #[test]
    fn test_put_bad_json() {
        let result = parse("PUT users {not valid json}", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid JSON"));
    }

    // -----------------------------------------------------------------------
    // GET
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_pk_only() {
        let cmd = parse("GET users pk=alice", None).unwrap();
        match cmd {
            Command::Get { table, pk, sk } => {
                assert_eq!(table, "users");
                assert_eq!(pk, json!("alice"));
                assert!(sk.is_none());
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_get_pk_and_sk() {
        let cmd = parse("GET events pk=alice sk=100", None).unwrap();
        match cmd {
            Command::Get { table, pk, sk } => {
                assert_eq!(table, "events");
                assert_eq!(pk, json!("alice"));
                assert_eq!(sk.unwrap(), json!(100.0));
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_get_quoted_value() {
        let cmd = parse(r#"GET users pk="hello world""#, None).unwrap();
        match cmd {
            Command::Get { pk, .. } => {
                assert_eq!(pk, json!("hello world"));
            }
            _ => panic!("Expected Get"),
        }
    }

    // -----------------------------------------------------------------------
    // DELETE
    // -----------------------------------------------------------------------

    #[test]
    fn test_delete_pk_only() {
        let cmd = parse("DELETE users pk=alice", None).unwrap();
        match cmd {
            Command::Delete { table, pk, sk } => {
                assert_eq!(table, "users");
                assert_eq!(pk, json!("alice"));
                assert!(sk.is_none());
            }
            _ => panic!("Expected Delete"),
        }
    }

    #[test]
    fn test_delete_pk_and_sk() {
        let cmd = parse("DELETE events pk=alice sk=100", None).unwrap();
        match cmd {
            Command::Delete { table, pk, sk } => {
                assert_eq!(table, "events");
                assert_eq!(pk, json!("alice"));
                assert_eq!(sk.unwrap(), json!(100.0));
            }
            _ => panic!("Expected Delete"),
        }
    }

    // -----------------------------------------------------------------------
    // QUERY with various sort conditions
    // -----------------------------------------------------------------------

    #[test]
    fn test_query_pk_only() {
        let cmd = parse("QUERY events pk=alice", None).unwrap();
        match cmd {
            Command::Query {
                table,
                pk,
                sort_condition,
                limit,
                desc,
            } => {
                assert_eq!(table, "events");
                assert_eq!(pk, json!("alice"));
                assert!(sort_condition.is_none());
                assert!(limit.is_none());
                assert!(!desc);
            }
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_sk_eq() {
        let cmd = parse("QUERY events pk=alice SK = 100", None).unwrap();
        match cmd {
            Command::Query { sort_condition, .. } => match sort_condition.unwrap() {
                SortClause::Eq(v) => assert_eq!(v, json!(100.0)),
                _ => panic!("Expected Eq"),
            },
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_sk_lt() {
        let cmd = parse("QUERY events pk=alice SK < 50", None).unwrap();
        match cmd {
            Command::Query { sort_condition, .. } => match sort_condition.unwrap() {
                SortClause::Lt(v) => assert_eq!(v, json!(50.0)),
                _ => panic!("Expected Lt"),
            },
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_sk_le() {
        let cmd = parse("QUERY events pk=alice SK <= 50", None).unwrap();
        match cmd {
            Command::Query { sort_condition, .. } => match sort_condition.unwrap() {
                SortClause::Le(v) => assert_eq!(v, json!(50.0)),
                _ => panic!("Expected Le"),
            },
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_sk_gt() {
        let cmd = parse("QUERY events pk=alice SK > 200", None).unwrap();
        match cmd {
            Command::Query { sort_condition, .. } => match sort_condition.unwrap() {
                SortClause::Gt(v) => assert_eq!(v, json!(200.0)),
                _ => panic!("Expected Gt"),
            },
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_sk_ge() {
        let cmd = parse("QUERY events pk=alice SK >= 200", None).unwrap();
        match cmd {
            Command::Query { sort_condition, .. } => match sort_condition.unwrap() {
                SortClause::Ge(v) => assert_eq!(v, json!(200.0)),
                _ => panic!("Expected Ge"),
            },
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_between() {
        let cmd = parse("QUERY events pk=alice BETWEEN 100 AND 500", None).unwrap();
        match cmd {
            Command::Query { sort_condition, .. } => match sort_condition.unwrap() {
                SortClause::Between(lo, hi) => {
                    assert_eq!(lo, json!(100.0));
                    assert_eq!(hi, json!(500.0));
                }
                _ => panic!("Expected Between"),
            },
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_begins_with() {
        let cmd = parse(r#"QUERY events pk=alice BEGINS_WITH "abc""#, None).unwrap();
        match cmd {
            Command::Query { sort_condition, .. } => match sort_condition.unwrap() {
                SortClause::BeginsWith(prefix) => assert_eq!(prefix, "abc"),
                _ => panic!("Expected BeginsWith"),
            },
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_begins_with_bare_word() {
        let cmd = parse("QUERY events pk=alice BEGINS_WITH abc", None).unwrap();
        match cmd {
            Command::Query { sort_condition, .. } => match sort_condition.unwrap() {
                SortClause::BeginsWith(prefix) => assert_eq!(prefix, "abc"),
                _ => panic!("Expected BeginsWith"),
            },
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_with_limit() {
        let cmd = parse("QUERY events pk=alice LIMIT 10", None).unwrap();
        match cmd {
            Command::Query { limit, .. } => {
                assert_eq!(limit, Some(10));
            }
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_with_desc() {
        let cmd = parse("QUERY events pk=alice DESC", None).unwrap();
        match cmd {
            Command::Query { desc, .. } => {
                assert!(desc);
            }
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_sk_limit_desc() {
        let cmd = parse("QUERY events pk=alice SK >= 100 LIMIT 5 DESC", None).unwrap();
        match cmd {
            Command::Query {
                sort_condition,
                limit,
                desc,
                ..
            } => {
                match sort_condition.unwrap() {
                    SortClause::Ge(v) => assert_eq!(v, json!(100.0)),
                    _ => panic!("Expected Ge"),
                }
                assert_eq!(limit, Some(5));
                assert!(desc);
            }
            _ => panic!("Expected Query"),
        }
    }

    // -----------------------------------------------------------------------
    // SCAN
    // -----------------------------------------------------------------------

    #[test]
    fn test_scan_no_limit() {
        let cmd = parse("SCAN items", None).unwrap();
        match cmd {
            Command::Scan { table, limit } => {
                assert_eq!(table, "items");
                assert!(limit.is_none());
            }
            _ => panic!("Expected Scan"),
        }
    }

    #[test]
    fn test_scan_with_limit() {
        let cmd = parse("SCAN items LIMIT 20", None).unwrap();
        match cmd {
            Command::Scan { table, limit } => {
                assert_eq!(table, "items");
                assert_eq!(limit, Some(20));
            }
            _ => panic!("Expected Scan"),
        }
    }

    // -----------------------------------------------------------------------
    // LIST TABLES, DESCRIBE TABLE
    // -----------------------------------------------------------------------

    #[test]
    fn test_list_tables() {
        let cmd = parse("LIST TABLES", None).unwrap();
        assert!(matches!(cmd, Command::ListTables));
    }

    #[test]
    fn test_list_tables_case_insensitive() {
        let cmd = parse("list tables", None).unwrap();
        assert!(matches!(cmd, Command::ListTables));
    }

    #[test]
    fn test_list_keys() {
        let cmd = parse("LIST KEYS mytable", None).unwrap();
        match cmd {
            Command::ListKeys { table, limit } => {
                assert_eq!(table, "mytable");
                assert!(limit.is_none());
            }
            _ => panic!("Expected ListKeys"),
        }
    }

    #[test]
    fn test_list_keys_with_limit() {
        let cmd = parse("LIST KEYS mytable LIMIT 10", None).unwrap();
        match cmd {
            Command::ListKeys { table, limit } => {
                assert_eq!(table, "mytable");
                assert_eq!(limit, Some(10));
            }
            _ => panic!("Expected ListKeys"),
        }
    }

    #[test]
    fn test_list_keys_case_insensitive() {
        let cmd = parse("list keys mytable limit 5", None).unwrap();
        match cmd {
            Command::ListKeys { table, limit } => {
                assert_eq!(table, "mytable");
                assert_eq!(limit, Some(5));
            }
            _ => panic!("Expected ListKeys"),
        }
    }

    #[test]
    fn test_list_keys_missing_table() {
        let result = parse("LIST KEYS", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_keys_invalid_limit() {
        let result = parse("LIST KEYS mytable LIMIT abc", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid LIMIT"));
    }

    #[test]
    fn test_list_keys_unexpected_token() {
        let result = parse("LIST KEYS mytable BADTOKEN", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unexpected token"));
    }

    // -----------------------------------------------------------------------
    // LIST PREFIXES
    // -----------------------------------------------------------------------

    #[test]
    fn test_list_prefixes() {
        let cmd = parse("LIST PREFIXES mytable pk=alice", None).unwrap();
        match cmd {
            Command::ListPrefixes { table, pk, limit } => {
                assert_eq!(table, "mytable");
                assert_eq!(pk, json!("alice"));
                assert!(limit.is_none());
            }
            _ => panic!("Expected ListPrefixes"),
        }
    }

    #[test]
    fn test_list_prefixes_with_limit() {
        let cmd = parse("LIST PREFIXES mytable pk=alice LIMIT 5", None).unwrap();
        match cmd {
            Command::ListPrefixes { table, pk, limit } => {
                assert_eq!(table, "mytable");
                assert_eq!(pk, json!("alice"));
                assert_eq!(limit, Some(5));
            }
            _ => panic!("Expected ListPrefixes"),
        }
    }

    #[test]
    fn test_list_prefixes_case_insensitive() {
        let cmd = parse("list prefixes mytable pk=alice limit 3", None).unwrap();
        match cmd {
            Command::ListPrefixes { table, pk, limit } => {
                assert_eq!(table, "mytable");
                assert_eq!(pk, json!("alice"));
                assert_eq!(limit, Some(3));
            }
            _ => panic!("Expected ListPrefixes"),
        }
    }

    #[test]
    fn test_list_prefixes_missing_pk() {
        let result = parse("LIST PREFIXES mytable", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_prefixes_invalid_limit() {
        let result = parse("LIST PREFIXES mytable pk=alice LIMIT abc", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid LIMIT"));
    }

    #[test]
    fn test_list_prefixes_unexpected_token() {
        let result = parse("LIST PREFIXES mytable pk=alice BADTOKEN", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unexpected token"));
    }

    #[test]
    fn test_describe_table() {
        let cmd = parse("DESCRIBE TABLE users", None).unwrap();
        match cmd {
            Command::DescribeTable { name } => assert_eq!(name, "users"),
            _ => panic!("Expected DescribeTable"),
        }
    }

    // -----------------------------------------------------------------------
    // HELP, EXIT, QUIT
    // -----------------------------------------------------------------------

    #[test]
    fn test_help() {
        let cmd = parse("HELP", None).unwrap();
        assert!(matches!(cmd, Command::Help(None)));
    }

    #[test]
    fn test_help_with_topic() {
        let cmd = parse("HELP QUERY", None).unwrap();
        match cmd {
            Command::Help(Some(topic)) => assert_eq!(topic, "QUERY"),
            _ => panic!("Expected Help with topic"),
        }
    }

    #[test]
    fn test_help_with_multi_word_topic() {
        let cmd = parse("HELP CREATE TABLE", None).unwrap();
        match cmd {
            Command::Help(Some(topic)) => assert_eq!(topic, "CREATE TABLE"),
            _ => panic!("Expected Help with topic"),
        }
    }

    #[test]
    fn test_exit() {
        let cmd = parse("EXIT", None).unwrap();
        assert!(matches!(cmd, Command::Exit));
    }

    #[test]
    fn test_quit() {
        let cmd = parse("QUIT", None).unwrap();
        assert!(matches!(cmd, Command::Exit));
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_unknown_command() {
        let result = parse("FOOBAR", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown command"));
    }

    #[test]
    fn test_empty_input() {
        let result = parse("", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_missing_args() {
        let result = parse("CREATE TABLE", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_missing_pk() {
        let result = parse("GET users", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_missing_table_name() {
        let result = parse("DROP TABLE", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_query_invalid_limit() {
        let result = parse("QUERY events pk=alice LIMIT abc", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid LIMIT"));
    }

    #[test]
    fn test_between_missing_and() {
        let result = parse("QUERY events pk=alice BETWEEN 10 NOPE 20", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("AND"));
    }

    #[test]
    fn test_scan_invalid_limit() {
        let result = parse("SCAN items LIMIT xyz", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_unterminated_json() {
        let result = parse("PUT users {\"id\": \"x\"", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unterminated JSON"));
    }

    #[test]
    fn test_unterminated_string() {
        let result = parse(r#"GET users pk="hello"#, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unterminated quoted string"));
    }

    #[test]
    fn test_describe_missing_table_keyword() {
        let result = parse("DESCRIBE users", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_missing_subcommand() {
        let result = parse("LIST", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_invalid_subcommand() {
        let result = parse("LIST FOOBAR", None);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Expected TABLES, KEYS, PREFIXES, SCHEMAS, or INDEXES")
        );
    }

    // -----------------------------------------------------------------------
    // Tokenizer edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_tokenize_operators() {
        let tokens = tokenize("SK <= 100").unwrap();
        assert_eq!(tokens, vec!["SK", "<=", "100"]);
    }

    #[test]
    fn test_tokenize_gt_operator() {
        let tokens = tokenize("SK > 50").unwrap();
        assert_eq!(tokens, vec!["SK", ">", "50"]);
    }

    #[test]
    fn test_tokenize_json_with_strings() {
        let tokens = tokenize(r#"PUT t {"key": "value with spaces"}"#).unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], "PUT");
        assert_eq!(tokens[1], "t");
        assert!(tokens[2].starts_with('{'));
        assert!(tokens[2].ends_with('}'));
    }

    #[test]
    fn test_parse_value_number() {
        assert_eq!(parse_value("42"), json!(42.0));
        assert_eq!(parse_value("3.14"), json!(3.14));
    }

    #[test]
    fn test_parse_value_quoted_string() {
        assert_eq!(parse_value("\"hello\""), json!("hello"));
    }

    #[test]
    fn test_parse_value_bare_word() {
        assert_eq!(parse_value("alice"), json!("alice"));
    }

    #[test]
    fn test_query_unexpected_token() {
        let result = parse("QUERY events pk=alice BADTOKEN", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unexpected token"));
    }

    #[test]
    fn test_scan_unexpected_token() {
        let result = parse("SCAN items BADTOKEN", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unexpected token"));
    }

    #[test]
    fn test_create_table_bad_keyword_after_create() {
        let result = parse("CREATE WRONG name PK id STRING", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Expected TABLE"));
    }

    #[test]
    fn test_sk_missing_value() {
        let result = parse("QUERY events pk=alice SK >=", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_begins_with_missing_prefix() {
        let result = parse("QUERY events pk=alice BEGINS_WITH", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_between_missing_values() {
        let result = parse("QUERY events pk=alice BETWEEN 10", None);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // CREATE SCHEMA
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_schema_basic() {
        let cmd = parse("CREATE SCHEMA data PREFIX CONTACT", None).unwrap();
        match cmd {
            Command::CreateSchema {
                table,
                prefix,
                description,
                attributes,
                validate,
            } => {
                assert_eq!(table, "data");
                assert_eq!(prefix, "CONTACT");
                assert!(description.is_none());
                assert!(attributes.is_empty());
                assert!(!validate);
            }
            _ => panic!("Expected CreateSchema"),
        }
    }

    #[test]
    fn test_create_schema_full() {
        let cmd = parse("CREATE SCHEMA data PREFIX CONTACT DESCRIPTION \"People\" ATTR email STRING REQUIRED ATTR age NUMBER VALIDATE", None)
        .unwrap();
        match cmd {
            Command::CreateSchema {
                table,
                prefix,
                description,
                attributes,
                validate,
            } => {
                assert_eq!(table, "data");
                assert_eq!(prefix, "CONTACT");
                assert_eq!(description, Some("People".to_string()));
                assert_eq!(attributes.len(), 2);
                assert_eq!(attributes[0].0, "email");
                assert_eq!(attributes[0].1, AttrType::String);
                assert!(attributes[0].2); // required
                assert_eq!(attributes[1].0, "age");
                assert_eq!(attributes[1].1, AttrType::Number);
                assert!(!attributes[1].2); // not required
                assert!(validate);
            }
            _ => panic!("Expected CreateSchema"),
        }
    }

    #[test]
    fn test_create_schema_missing_prefix() {
        let result = parse("CREATE SCHEMA data", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_schema_bad_keyword() {
        let result = parse("CREATE SCHEMA data WRONG CONTACT", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("PREFIX"));
    }

    // -----------------------------------------------------------------------
    // DROP SCHEMA
    // -----------------------------------------------------------------------

    #[test]
    fn test_drop_schema() {
        let cmd = parse("DROP SCHEMA data CONTACT", None).unwrap();
        match cmd {
            Command::DropSchema { table, prefix } => {
                assert_eq!(table, "data");
                assert_eq!(prefix, "CONTACT");
            }
            _ => panic!("Expected DropSchema"),
        }
    }

    #[test]
    fn test_drop_schema_missing_prefix() {
        let result = parse("DROP SCHEMA data", None);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // LIST SCHEMAS / LIST INDEXES
    // -----------------------------------------------------------------------

    #[test]
    fn test_list_schemas() {
        let cmd = parse("LIST SCHEMAS data", None).unwrap();
        match cmd {
            Command::ListSchemas { table } => {
                assert_eq!(table, "data");
            }
            _ => panic!("Expected ListSchemas"),
        }
    }

    #[test]
    fn test_list_schemas_missing_table() {
        let result = parse("LIST SCHEMAS", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_indexes() {
        let cmd = parse("LIST INDEXES data", None).unwrap();
        match cmd {
            Command::ListIndexes { table } => {
                assert_eq!(table, "data");
            }
            _ => panic!("Expected ListIndexes"),
        }
    }

    // -----------------------------------------------------------------------
    // DESCRIBE SCHEMA / DESCRIBE INDEX
    // -----------------------------------------------------------------------

    #[test]
    fn test_describe_schema() {
        let cmd = parse("DESCRIBE SCHEMA data CONTACT", None).unwrap();
        match cmd {
            Command::DescribeSchema { table, prefix } => {
                assert_eq!(table, "data");
                assert_eq!(prefix, "CONTACT");
            }
            _ => panic!("Expected DescribeSchema"),
        }
    }

    #[test]
    fn test_describe_schema_missing_prefix() {
        let result = parse("DESCRIBE SCHEMA data", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_describe_index() {
        let cmd = parse("DESCRIBE INDEX data email-idx", None).unwrap();
        match cmd {
            Command::DescribeIndex { table, name } => {
                assert_eq!(table, "data");
                assert_eq!(name, "email-idx");
            }
            _ => panic!("Expected DescribeIndex"),
        }
    }

    // -----------------------------------------------------------------------
    // CREATE INDEX
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_index() {
        let cmd = parse(
            "CREATE INDEX data email-idx SCHEMA CONTACT KEY email STRING",
            None,
        )
        .unwrap();
        match cmd {
            Command::CreateIndex {
                table,
                name,
                schema_prefix,
                key_attr,
                key_type,
            } => {
                assert_eq!(table, "data");
                assert_eq!(name, "email-idx");
                assert_eq!(schema_prefix, "CONTACT");
                assert_eq!(key_attr, "email");
                assert_eq!(key_type, KeyType::String);
            }
            _ => panic!("Expected CreateIndex"),
        }
    }

    #[test]
    fn test_create_index_number_key() {
        let cmd = parse(
            "CREATE INDEX data price-idx SCHEMA PRODUCT KEY price NUMBER",
            None,
        )
        .unwrap();
        match cmd {
            Command::CreateIndex { key_type, .. } => {
                assert_eq!(key_type, KeyType::Number);
            }
            _ => panic!("Expected CreateIndex"),
        }
    }

    #[test]
    fn test_create_index_missing_args() {
        let result = parse("CREATE INDEX data email-idx", None);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // DROP INDEX
    // -----------------------------------------------------------------------

    #[test]
    fn test_drop_index() {
        let cmd = parse("DROP INDEX data email-idx", None).unwrap();
        match cmd {
            Command::DropIndex { table, name } => {
                assert_eq!(table, "data");
                assert_eq!(name, "email-idx");
            }
            _ => panic!("Expected DropIndex"),
        }
    }

    // -----------------------------------------------------------------------
    // QUERY INDEX
    // -----------------------------------------------------------------------

    #[test]
    fn test_query_index_basic() {
        let cmd = parse("QUERY INDEX data email-idx key=alice", None).unwrap();
        match cmd {
            Command::QueryIndex {
                table,
                index_name,
                key_value,
                limit,
                desc,
            } => {
                assert_eq!(table, "data");
                assert_eq!(index_name, "email-idx");
                assert_eq!(key_value, json!("alice"));
                assert!(limit.is_none());
                assert!(!desc);
            }
            _ => panic!("Expected QueryIndex"),
        }
    }

    #[test]
    fn test_query_index_with_limit_desc() {
        let cmd = parse(
            "QUERY INDEX data email-idx key=\"alice@test.com\" LIMIT 5 DESC",
            None,
        )
        .unwrap();
        match cmd {
            Command::QueryIndex {
                table,
                index_name,
                key_value,
                limit,
                desc,
            } => {
                assert_eq!(table, "data");
                assert_eq!(index_name, "email-idx");
                assert_eq!(key_value, json!("alice@test.com"));
                assert_eq!(limit, Some(5));
                assert!(desc);
            }
            _ => panic!("Expected QueryIndex"),
        }
    }

    #[test]
    fn test_query_index_missing_key() {
        let result = parse("QUERY INDEX data email-idx", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_query_index_wrong_key_name() {
        let result = parse("QUERY INDEX data email-idx pk=alice", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("key="));
    }

    // -----------------------------------------------------------------------
    // Case insensitivity for new commands
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_schema_case_insensitive() {
        let cmd = parse("create schema data prefix CONTACT", None).unwrap();
        assert!(matches!(cmd, Command::CreateSchema { .. }));
    }

    #[test]
    fn test_query_index_case_insensitive() {
        let cmd = parse("query index data email-idx key=alice", None).unwrap();
        assert!(matches!(cmd, Command::QueryIndex { .. }));
    }

    // -----------------------------------------------------------------------
    // USE command
    // -----------------------------------------------------------------------

    #[test]
    fn test_use_set() {
        let cmd = parse("USE data", None).unwrap();
        match cmd {
            Command::Use { table } => assert_eq!(table, Some("data".to_string())),
            _ => panic!("Expected Use"),
        }
    }

    #[test]
    fn test_use_clear() {
        let cmd = parse("USE", None).unwrap();
        match cmd {
            Command::Use { table } => assert!(table.is_none()),
            _ => panic!("Expected Use"),
        }
    }

    #[test]
    fn test_use_case_insensitive() {
        let cmd = parse("use mytable", None).unwrap();
        match cmd {
            Command::Use { table } => assert_eq!(table, Some("mytable".to_string())),
            _ => panic!("Expected Use"),
        }
    }

    // -----------------------------------------------------------------------
    // Table-optional commands (with default_table)
    // -----------------------------------------------------------------------

    #[test]
    fn test_put_without_table() {
        let cmd = parse(r#"PUT {"pk":"a","name":"Alice"}"#, Some("data")).unwrap();
        match cmd {
            Command::Put { table, document } => {
                assert_eq!(table, "data");
                assert_eq!(document["pk"], "a");
            }
            _ => panic!("Expected Put"),
        }
    }

    #[test]
    fn test_get_without_table() {
        let cmd = parse("GET pk=alice", Some("data")).unwrap();
        match cmd {
            Command::Get { table, pk, .. } => {
                assert_eq!(table, "data");
                assert_eq!(pk, json!("alice"));
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_delete_without_table() {
        let cmd = parse("DELETE pk=alice", Some("data")).unwrap();
        match cmd {
            Command::Delete { table, pk, .. } => {
                assert_eq!(table, "data");
                assert_eq!(pk, json!("alice"));
            }
            _ => panic!("Expected Delete"),
        }
    }

    #[test]
    fn test_query_without_table() {
        let cmd = parse("QUERY pk=alice", Some("data")).unwrap();
        match cmd {
            Command::Query { table, pk, .. } => {
                assert_eq!(table, "data");
                assert_eq!(pk, json!("alice"));
            }
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_scan_without_table() {
        let cmd = parse("SCAN LIMIT 5", Some("data")).unwrap();
        match cmd {
            Command::Scan { table, limit } => {
                assert_eq!(table, "data");
                assert_eq!(limit, Some(5));
            }
            _ => panic!("Expected Scan"),
        }
    }

    #[test]
    fn test_scan_bare_without_table() {
        let cmd = parse("SCAN", Some("data")).unwrap();
        match cmd {
            Command::Scan { table, limit } => {
                assert_eq!(table, "data");
                assert!(limit.is_none());
            }
            _ => panic!("Expected Scan"),
        }
    }

    #[test]
    fn test_list_schemas_without_table() {
        let cmd = parse("LIST SCHEMAS", Some("data")).unwrap();
        match cmd {
            Command::ListSchemas { table } => assert_eq!(table, "data"),
            _ => panic!("Expected ListSchemas"),
        }
    }

    #[test]
    fn test_list_indexes_without_table() {
        let cmd = parse("LIST INDEXES", Some("data")).unwrap();
        match cmd {
            Command::ListIndexes { table } => assert_eq!(table, "data"),
            _ => panic!("Expected ListIndexes"),
        }
    }

    #[test]
    fn test_list_keys_without_table() {
        let cmd = parse("LIST KEYS", Some("data")).unwrap();
        match cmd {
            Command::ListKeys { table, .. } => assert_eq!(table, "data"),
            _ => panic!("Expected ListKeys"),
        }
    }

    #[test]
    fn test_describe_table_without_name() {
        let cmd = parse("DESCRIBE TABLE", Some("data")).unwrap();
        match cmd {
            Command::DescribeTable { name } => assert_eq!(name, "data"),
            _ => panic!("Expected DescribeTable"),
        }
    }

    #[test]
    fn test_create_schema_without_table() {
        let cmd = parse("CREATE SCHEMA PREFIX CONTACT", Some("data")).unwrap();
        match cmd {
            Command::CreateSchema { table, prefix, .. } => {
                assert_eq!(table, "data");
                assert_eq!(prefix, "CONTACT");
            }
            _ => panic!("Expected CreateSchema"),
        }
    }

    #[test]
    fn test_drop_schema_without_table() {
        let cmd = parse("DROP SCHEMA CONTACT", Some("data")).unwrap();
        match cmd {
            Command::DropSchema { table, prefix } => {
                assert_eq!(table, "data");
                assert_eq!(prefix, "CONTACT");
            }
            _ => panic!("Expected DropSchema"),
        }
    }

    #[test]
    fn test_describe_schema_without_table() {
        let cmd = parse("DESCRIBE SCHEMA CONTACT", Some("data")).unwrap();
        match cmd {
            Command::DescribeSchema { table, prefix } => {
                assert_eq!(table, "data");
                assert_eq!(prefix, "CONTACT");
            }
            _ => panic!("Expected DescribeSchema"),
        }
    }

    #[test]
    fn test_create_index_without_table() {
        let cmd = parse(
            "CREATE INDEX email-idx SCHEMA CONTACT KEY email STRING",
            Some("data"),
        )
        .unwrap();
        match cmd {
            Command::CreateIndex {
                table,
                name,
                schema_prefix,
                ..
            } => {
                assert_eq!(table, "data");
                assert_eq!(name, "email-idx");
                assert_eq!(schema_prefix, "CONTACT");
            }
            _ => panic!("Expected CreateIndex"),
        }
    }

    #[test]
    fn test_drop_index_without_table() {
        let cmd = parse("DROP INDEX email-idx", Some("data")).unwrap();
        match cmd {
            Command::DropIndex { table, name } => {
                assert_eq!(table, "data");
                assert_eq!(name, "email-idx");
            }
            _ => panic!("Expected DropIndex"),
        }
    }

    #[test]
    fn test_describe_index_without_table() {
        let cmd = parse("DESCRIBE INDEX email-idx", Some("data")).unwrap();
        match cmd {
            Command::DescribeIndex { table, name } => {
                assert_eq!(table, "data");
                assert_eq!(name, "email-idx");
            }
            _ => panic!("Expected DescribeIndex"),
        }
    }

    #[test]
    fn test_query_index_without_table() {
        let cmd = parse("QUERY INDEX email-idx key=alice", Some("data")).unwrap();
        match cmd {
            Command::QueryIndex {
                table, index_name, ..
            } => {
                assert_eq!(table, "data");
                assert_eq!(index_name, "email-idx");
            }
            _ => panic!("Expected QueryIndex"),
        }
    }

    #[test]
    fn test_no_table_no_default_errors() {
        let result = parse("SCAN", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No table specified"));
    }

    #[test]
    fn test_explicit_table_overrides_default() {
        let cmd = parse("SCAN other", Some("data")).unwrap();
        match cmd {
            Command::Scan { table, .. } => assert_eq!(table, "other"),
            _ => panic!("Expected Scan"),
        }
    }
}
