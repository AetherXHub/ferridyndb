use serde_json::Value;

use crate::commands::{AttrType, Command, KeyType, SortClause};

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

        // Regular word token: everything up to whitespace, quote, brace, or angle bracket.
        // Special case: if the word ends with `=` and the next character is `"`,
        // consume the quoted string as part of this token (e.g., `pk="hello world"`).
        let start = i;
        while i < len
            && !chars[i].is_whitespace()
            && chars[i] != '"'
            && chars[i] != '{'
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
pub fn parse(input: &str) -> Result<Command, String> {
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
                "TABLE" => parse_create_table(&tokens),
                "SCHEMA" => parse_create_schema(&tokens),
                "INDEX" => parse_create_index(&tokens),
                _ => Err("Expected TABLE, SCHEMA, or INDEX after CREATE".to_string()),
            }
        }
        "DROP" => {
            if tokens.len() < 2 {
                return Err("Expected TABLE, SCHEMA, or INDEX after DROP".to_string());
            }
            match tokens[1].to_uppercase().as_str() {
                "TABLE" => parse_drop_table(&tokens),
                "SCHEMA" => parse_drop_schema(&tokens),
                "INDEX" => parse_drop_index(&tokens),
                _ => Err("Expected TABLE, SCHEMA, or INDEX after DROP".to_string()),
            }
        }
        "LIST" => parse_list(&tokens),
        "DESCRIBE" => {
            if tokens.len() < 2 {
                return Err("Expected TABLE, SCHEMA, or INDEX after DESCRIBE".to_string());
            }
            match tokens[1].to_uppercase().as_str() {
                "TABLE" => parse_describe_table(&tokens),
                "SCHEMA" => parse_describe_schema(&tokens),
                "INDEX" => parse_describe_index(&tokens),
                _ => Err("Expected TABLE, SCHEMA, or INDEX after DESCRIBE".to_string()),
            }
        }
        "PUT" => parse_put(&tokens),
        "GET" => parse_get(&tokens),
        "DELETE" => parse_delete(&tokens),
        "QUERY" => {
            if tokens.len() > 1 && tokens[1].to_uppercase() == "INDEX" {
                parse_query_index(&tokens)
            } else {
                parse_query(&tokens)
            }
        }
        "SCAN" => parse_scan(&tokens),
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

/// CREATE TABLE <name> PK <attr> <type> [SK <attr> <type>]
fn parse_create_table(tokens: &[String]) -> Result<Command, String> {
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
fn parse_drop_table(tokens: &[String]) -> Result<Command, String> {
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

/// LIST TABLES | LIST KEYS <table> [LIMIT <n>]
fn parse_list(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 2 {
        return Err(
            "Usage: LIST TABLES | LIST KEYS <table> | LIST PREFIXES <table> pk=<val>  (Type HELP LIST for details)"
                .to_string(),
        );
    }
    match tokens[1].to_uppercase().as_str() {
        "TABLES" => Ok(Command::ListTables),
        "KEYS" => parse_list_keys(tokens),
        "PREFIXES" => parse_list_prefixes(tokens),
        "SCHEMAS" => parse_list_schemas(tokens),
        "INDEXES" => parse_list_indexes(tokens),
        _ => Err(format!(
            "Expected TABLES, KEYS, PREFIXES, SCHEMAS, or INDEXES after LIST, got '{}'",
            tokens[1]
        )),
    }
}

/// LIST KEYS <table> [LIMIT <n>]
fn parse_list_keys(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: LIST KEYS <table> [LIMIT <n>]  (Type HELP LIST KEYS for details)".to_string(),
        );
    }
    let table = tokens[2].clone();
    let mut limit = None;

    if tokens.len() > 3 {
        if tokens[3].to_uppercase() == "LIMIT" {
            if tokens.len() < 5 {
                return Err("LIMIT requires a number".to_string());
            }
            limit = Some(
                tokens[4]
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid LIMIT value '{}'", tokens[4]))?,
            );
        } else {
            return Err(format!("Unexpected token '{}' in LIST KEYS", tokens[3]));
        }
    }

    Ok(Command::ListKeys { table, limit })
}

/// LIST PREFIXES <table> pk=<value> [LIMIT <n>]
fn parse_list_prefixes(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 4 {
        return Err(
            "Usage: LIST PREFIXES <table> pk=<value> [LIMIT <n>]  (Type HELP LIST PREFIXES for details)"
                .to_string(),
        );
    }
    let table = tokens[2].clone();

    let (key_name, pk) = parse_kv_pair(&tokens[3])?;
    if key_name.to_uppercase() != "PK" {
        return Err(format!("Expected pk=<value>, got '{}'", tokens[3]));
    }

    let mut limit = None;
    if tokens.len() > 4 {
        if tokens[4].to_uppercase() == "LIMIT" {
            if tokens.len() < 6 {
                return Err("LIMIT requires a number".to_string());
            }
            limit = Some(
                tokens[5]
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid LIMIT value '{}'", tokens[5]))?,
            );
        } else {
            return Err(format!("Unexpected token '{}' in LIST PREFIXES", tokens[4]));
        }
    }

    Ok(Command::ListPrefixes { table, pk, limit })
}

/// DESCRIBE TABLE <name>
fn parse_describe_table(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: DESCRIBE TABLE <name>  (Type HELP DESCRIBE TABLE for details)".to_string(),
        );
    }
    if tokens[1].to_uppercase() != "TABLE" {
        return Err(format!(
            "Expected TABLE after DESCRIBE, got '{}'",
            tokens[1]
        ));
    }
    Ok(Command::DescribeTable {
        name: tokens[2].clone(),
    })
}

/// PUT <table> {json}
fn parse_put(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err("Usage: PUT <table> {json}  (Type HELP PUT for details)".to_string());
    }
    let table = tokens[1].clone();
    let json_str = &tokens[2];
    let document: Value =
        serde_json::from_str(json_str).map_err(|e| format!("Invalid JSON: {e}"))?;
    Ok(Command::Put { table, document })
}

/// GET <table> pk=<value> [sk=<value>]
fn parse_get(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: GET <table> pk=<value> [sk=<value>]  (Type HELP GET for details)".to_string(),
        );
    }
    let table = tokens[1].clone();

    let (key_name, pk) = parse_kv_pair(&tokens[2])?;
    if key_name.to_uppercase() != "PK" {
        return Err(format!("Expected pk=<value>, got '{}'", tokens[2]));
    }

    let sk = if tokens.len() > 3 {
        let (sk_key, sk_val) = parse_kv_pair(&tokens[3])?;
        if sk_key.to_uppercase() != "SK" {
            return Err(format!("Expected sk=<value>, got '{}'", tokens[3]));
        }
        Some(sk_val)
    } else {
        None
    };

    Ok(Command::Get { table, pk, sk })
}

/// DELETE <table> pk=<value> [sk=<value>]
fn parse_delete(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: DELETE <table> pk=<value> [sk=<value>]  (Type HELP DELETE for details)"
                .to_string(),
        );
    }
    let table = tokens[1].clone();

    let (key_name, pk) = parse_kv_pair(&tokens[2])?;
    if key_name.to_uppercase() != "PK" {
        return Err(format!("Expected pk=<value>, got '{}'", tokens[2]));
    }

    let sk = if tokens.len() > 3 {
        let (sk_key, sk_val) = parse_kv_pair(&tokens[3])?;
        if sk_key.to_uppercase() != "SK" {
            return Err(format!("Expected sk=<value>, got '{}'", tokens[3]));
        }
        Some(sk_val)
    } else {
        None
    };

    Ok(Command::Delete { table, pk, sk })
}

/// QUERY <table> pk=<value> [SK <op> <value>] [BETWEEN <lo> AND <hi>]
/// [BEGINS_WITH <prefix>] [LIMIT <n>] [DESC]
fn parse_query(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: QUERY <table> pk=<value> [SK <op> <val>] [LIMIT <n>] [DESC]  (Type HELP QUERY for details)".to_string(),
        );
    }
    let table = tokens[1].clone();

    let (key_name, pk) = parse_kv_pair(&tokens[2])?;
    if key_name.to_uppercase() != "PK" {
        return Err(format!("Expected pk=<value>, got '{}'", tokens[2]));
    }

    let mut sort_condition: Option<SortClause> = None;
    let mut limit: Option<usize> = None;
    let mut desc = false;
    let mut i = 3;

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

/// SCAN <table> [LIMIT <n>]
fn parse_scan(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 2 {
        return Err("Usage: SCAN <table> [LIMIT <n>]  (Type HELP SCAN for details)".to_string());
    }
    let table = tokens[1].clone();
    let mut limit = None;

    if tokens.len() > 2 {
        if tokens[2].to_uppercase() == "LIMIT" {
            if tokens.len() < 4 {
                return Err("LIMIT requires a number".to_string());
            }
            limit = Some(
                tokens[3]
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid LIMIT value '{}'", tokens[3]))?,
            );
        } else {
            return Err(format!("Unexpected token '{}' in SCAN", tokens[2]));
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

/// CREATE SCHEMA <table> PREFIX <prefix> [DESCRIPTION "text"]
///   [ATTR <name> <STRING|NUMBER|BOOLEAN> [REQUIRED]]... [VALIDATE]
fn parse_create_schema(tokens: &[String]) -> Result<Command, String> {
    // Minimum: CREATE SCHEMA <table> PREFIX <prefix> = 5 tokens
    if tokens.len() < 5 {
        return Err(
            "Usage: CREATE SCHEMA <table> PREFIX <prefix> [DESCRIPTION \"text\"] [ATTR <name> <type> [REQUIRED]]... [VALIDATE]  (Type HELP CREATE SCHEMA for details)"
                .to_string(),
        );
    }
    let table = tokens[2].clone();
    if tokens[3].to_uppercase() != "PREFIX" {
        return Err(format!(
            "Expected PREFIX after table name, got '{}'",
            tokens[3]
        ));
    }
    let prefix = tokens[4].clone();

    let mut description = None;
    let mut attributes = Vec::new();
    let mut validate = false;
    let mut i = 5;

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

/// CREATE INDEX <table> <name> SCHEMA <prefix> KEY <attr> <type>
fn parse_create_index(tokens: &[String]) -> Result<Command, String> {
    // Exactly 9 tokens: CREATE INDEX <table> <name> SCHEMA <prefix> KEY <attr> <type>
    if tokens.len() < 9 {
        return Err(
            "Usage: CREATE INDEX <table> <name> SCHEMA <prefix> KEY <attr> <STRING|NUMBER|BINARY>  (Type HELP CREATE INDEX for details)"
                .to_string(),
        );
    }
    let table = tokens[2].clone();
    let name = tokens[3].clone();
    if tokens[4].to_uppercase() != "SCHEMA" {
        return Err(format!("Expected SCHEMA, got '{}'", tokens[4]));
    }
    let schema_prefix = tokens[5].clone();
    if tokens[6].to_uppercase() != "KEY" {
        return Err(format!("Expected KEY, got '{}'", tokens[6]));
    }
    let key_attr = tokens[7].clone();
    let key_type = parse_key_type(&tokens[8])?;

    Ok(Command::CreateIndex {
        table,
        name,
        schema_prefix,
        key_attr,
        key_type,
    })
}

/// DROP SCHEMA <table> <prefix>
fn parse_drop_schema(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 4 {
        return Err(
            "Usage: DROP SCHEMA <table> <prefix>  (Type HELP DROP SCHEMA for details)".to_string(),
        );
    }
    Ok(Command::DropSchema {
        table: tokens[2].clone(),
        prefix: tokens[3].clone(),
    })
}

/// DROP INDEX <table> <name>
fn parse_drop_index(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 4 {
        return Err(
            "Usage: DROP INDEX <table> <name>  (Type HELP DROP INDEX for details)".to_string(),
        );
    }
    Ok(Command::DropIndex {
        table: tokens[2].clone(),
        name: tokens[3].clone(),
    })
}

/// DESCRIBE SCHEMA <table> <prefix>
fn parse_describe_schema(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 4 {
        return Err(
            "Usage: DESCRIBE SCHEMA <table> <prefix>  (Type HELP DESCRIBE SCHEMA for details)"
                .to_string(),
        );
    }
    Ok(Command::DescribeSchema {
        table: tokens[2].clone(),
        prefix: tokens[3].clone(),
    })
}

/// DESCRIBE INDEX <table> <name>
fn parse_describe_index(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 4 {
        return Err(
            "Usage: DESCRIBE INDEX <table> <name>  (Type HELP DESCRIBE INDEX for details)"
                .to_string(),
        );
    }
    Ok(Command::DescribeIndex {
        table: tokens[2].clone(),
        name: tokens[3].clone(),
    })
}

/// LIST SCHEMAS <table>
fn parse_list_schemas(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: LIST SCHEMAS <table>  (Type HELP LIST SCHEMAS for details)".to_string(),
        );
    }
    Ok(Command::ListSchemas {
        table: tokens[2].clone(),
    })
}

/// LIST INDEXES <table>
fn parse_list_indexes(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 3 {
        return Err(
            "Usage: LIST INDEXES <table>  (Type HELP LIST INDEXES for details)".to_string(),
        );
    }
    Ok(Command::ListIndexes {
        table: tokens[2].clone(),
    })
}

/// QUERY INDEX <table> <index_name> key=<value> [LIMIT <n>] [DESC]
fn parse_query_index(tokens: &[String]) -> Result<Command, String> {
    // Minimum: QUERY INDEX <table> <index_name> key=<value> = 5 tokens
    if tokens.len() < 5 {
        return Err(
            "Usage: QUERY INDEX <table> <index_name> key=<value> [LIMIT <n>] [DESC]  (Type HELP QUERY INDEX for details)"
                .to_string(),
        );
    }
    let table = tokens[2].clone();
    let index_name = tokens[3].clone();

    let (key_name, key_value) = parse_kv_pair(&tokens[4])?;
    if key_name.to_uppercase() != "KEY" {
        return Err(format!("Expected key=<value>, got '{}'", tokens[4]));
    }

    let mut limit = None;
    let mut desc = false;
    let mut i = 5;

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
        let cmd = parse("CREATE TABLE users PK user_id STRING").unwrap();
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
        let cmd = parse("CREATE TABLE events PK user_id STRING SK timestamp NUMBER").unwrap();
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
        let cmd = parse("create table Items pk id string").unwrap();
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
        let cmd = parse("CREATE TABLE blobs PK hash BINARY").unwrap();
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
        let cmd = parse("CREATE TABLE cache PK key STRING TTL expires").unwrap();
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
        let cmd = parse("CREATE TABLE cache PK key STRING SK sort NUMBER TTL expires").unwrap();
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
        let result = parse("CREATE TABLE cache PK key STRING TTL");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("TTL requires"));
    }

    // -----------------------------------------------------------------------
    // PUT
    // -----------------------------------------------------------------------

    #[test]
    fn test_put_with_json() {
        let cmd = parse(r#"PUT users {"user_id": "alice", "name": "Alice"}"#).unwrap();
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
        let cmd = parse(r#"PUT items {"id": "x", "meta": {"nested": true, "count": 5}}"#).unwrap();
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
        let result = parse("PUT users {not valid json}");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid JSON"));
    }

    // -----------------------------------------------------------------------
    // GET
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_pk_only() {
        let cmd = parse("GET users pk=alice").unwrap();
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
        let cmd = parse("GET events pk=alice sk=100").unwrap();
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
        let cmd = parse(r#"GET users pk="hello world""#).unwrap();
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
        let cmd = parse("DELETE users pk=alice").unwrap();
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
        let cmd = parse("DELETE events pk=alice sk=100").unwrap();
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
        let cmd = parse("QUERY events pk=alice").unwrap();
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
        let cmd = parse("QUERY events pk=alice SK = 100").unwrap();
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
        let cmd = parse("QUERY events pk=alice SK < 50").unwrap();
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
        let cmd = parse("QUERY events pk=alice SK <= 50").unwrap();
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
        let cmd = parse("QUERY events pk=alice SK > 200").unwrap();
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
        let cmd = parse("QUERY events pk=alice SK >= 200").unwrap();
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
        let cmd = parse("QUERY events pk=alice BETWEEN 100 AND 500").unwrap();
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
        let cmd = parse(r#"QUERY events pk=alice BEGINS_WITH "abc""#).unwrap();
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
        let cmd = parse("QUERY events pk=alice BEGINS_WITH abc").unwrap();
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
        let cmd = parse("QUERY events pk=alice LIMIT 10").unwrap();
        match cmd {
            Command::Query { limit, .. } => {
                assert_eq!(limit, Some(10));
            }
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_with_desc() {
        let cmd = parse("QUERY events pk=alice DESC").unwrap();
        match cmd {
            Command::Query { desc, .. } => {
                assert!(desc);
            }
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_query_sk_limit_desc() {
        let cmd = parse("QUERY events pk=alice SK >= 100 LIMIT 5 DESC").unwrap();
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
        let cmd = parse("SCAN items").unwrap();
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
        let cmd = parse("SCAN items LIMIT 20").unwrap();
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
        let cmd = parse("LIST TABLES").unwrap();
        assert!(matches!(cmd, Command::ListTables));
    }

    #[test]
    fn test_list_tables_case_insensitive() {
        let cmd = parse("list tables").unwrap();
        assert!(matches!(cmd, Command::ListTables));
    }

    #[test]
    fn test_list_keys() {
        let cmd = parse("LIST KEYS mytable").unwrap();
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
        let cmd = parse("LIST KEYS mytable LIMIT 10").unwrap();
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
        let cmd = parse("list keys mytable limit 5").unwrap();
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
        let result = parse("LIST KEYS");
        assert!(result.is_err());
    }

    #[test]
    fn test_list_keys_invalid_limit() {
        let result = parse("LIST KEYS mytable LIMIT abc");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid LIMIT"));
    }

    #[test]
    fn test_list_keys_unexpected_token() {
        let result = parse("LIST KEYS mytable BADTOKEN");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unexpected token"));
    }

    // -----------------------------------------------------------------------
    // LIST PREFIXES
    // -----------------------------------------------------------------------

    #[test]
    fn test_list_prefixes() {
        let cmd = parse("LIST PREFIXES mytable pk=alice").unwrap();
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
        let cmd = parse("LIST PREFIXES mytable pk=alice LIMIT 5").unwrap();
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
        let cmd = parse("list prefixes mytable pk=alice limit 3").unwrap();
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
        let result = parse("LIST PREFIXES mytable");
        assert!(result.is_err());
    }

    #[test]
    fn test_list_prefixes_invalid_limit() {
        let result = parse("LIST PREFIXES mytable pk=alice LIMIT abc");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid LIMIT"));
    }

    #[test]
    fn test_list_prefixes_unexpected_token() {
        let result = parse("LIST PREFIXES mytable pk=alice BADTOKEN");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unexpected token"));
    }

    #[test]
    fn test_describe_table() {
        let cmd = parse("DESCRIBE TABLE users").unwrap();
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
        let cmd = parse("HELP").unwrap();
        assert!(matches!(cmd, Command::Help(None)));
    }

    #[test]
    fn test_help_with_topic() {
        let cmd = parse("HELP QUERY").unwrap();
        match cmd {
            Command::Help(Some(topic)) => assert_eq!(topic, "QUERY"),
            _ => panic!("Expected Help with topic"),
        }
    }

    #[test]
    fn test_help_with_multi_word_topic() {
        let cmd = parse("HELP CREATE TABLE").unwrap();
        match cmd {
            Command::Help(Some(topic)) => assert_eq!(topic, "CREATE TABLE"),
            _ => panic!("Expected Help with topic"),
        }
    }

    #[test]
    fn test_exit() {
        let cmd = parse("EXIT").unwrap();
        assert!(matches!(cmd, Command::Exit));
    }

    #[test]
    fn test_quit() {
        let cmd = parse("QUIT").unwrap();
        assert!(matches!(cmd, Command::Exit));
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_unknown_command() {
        let result = parse("FOOBAR");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown command"));
    }

    #[test]
    fn test_empty_input() {
        let result = parse("");
        assert!(result.is_err());
    }

    #[test]
    fn test_create_missing_args() {
        let result = parse("CREATE TABLE");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_missing_pk() {
        let result = parse("GET users");
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_missing_table_name() {
        let result = parse("DROP TABLE");
        assert!(result.is_err());
    }

    #[test]
    fn test_query_invalid_limit() {
        let result = parse("QUERY events pk=alice LIMIT abc");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid LIMIT"));
    }

    #[test]
    fn test_between_missing_and() {
        let result = parse("QUERY events pk=alice BETWEEN 10 NOPE 20");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("AND"));
    }

    #[test]
    fn test_scan_invalid_limit() {
        let result = parse("SCAN items LIMIT xyz");
        assert!(result.is_err());
    }

    #[test]
    fn test_unterminated_json() {
        let result = parse("PUT users {\"id\": \"x\"");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unterminated JSON"));
    }

    #[test]
    fn test_unterminated_string() {
        let result = parse(r#"GET users pk="hello"#);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unterminated quoted string"));
    }

    #[test]
    fn test_describe_missing_table_keyword() {
        let result = parse("DESCRIBE users");
        assert!(result.is_err());
    }

    #[test]
    fn test_list_missing_subcommand() {
        let result = parse("LIST");
        assert!(result.is_err());
    }

    #[test]
    fn test_list_invalid_subcommand() {
        let result = parse("LIST FOOBAR");
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
        let result = parse("QUERY events pk=alice BADTOKEN");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unexpected token"));
    }

    #[test]
    fn test_scan_unexpected_token() {
        let result = parse("SCAN items BADTOKEN");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unexpected token"));
    }

    #[test]
    fn test_create_table_bad_keyword_after_create() {
        let result = parse("CREATE WRONG name PK id STRING");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Expected TABLE"));
    }

    #[test]
    fn test_sk_missing_value() {
        let result = parse("QUERY events pk=alice SK >=");
        assert!(result.is_err());
    }

    #[test]
    fn test_begins_with_missing_prefix() {
        let result = parse("QUERY events pk=alice BEGINS_WITH");
        assert!(result.is_err());
    }

    #[test]
    fn test_between_missing_values() {
        let result = parse("QUERY events pk=alice BETWEEN 10");
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // CREATE SCHEMA
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_schema_basic() {
        let cmd = parse("CREATE SCHEMA data PREFIX CONTACT").unwrap();
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
        let cmd = parse(
            "CREATE SCHEMA data PREFIX CONTACT DESCRIPTION \"People\" ATTR email STRING REQUIRED ATTR age NUMBER VALIDATE",
        )
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
        let result = parse("CREATE SCHEMA data");
        assert!(result.is_err());
    }

    #[test]
    fn test_create_schema_bad_keyword() {
        let result = parse("CREATE SCHEMA data WRONG CONTACT");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("PREFIX"));
    }

    // -----------------------------------------------------------------------
    // DROP SCHEMA
    // -----------------------------------------------------------------------

    #[test]
    fn test_drop_schema() {
        let cmd = parse("DROP SCHEMA data CONTACT").unwrap();
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
        let result = parse("DROP SCHEMA data");
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // LIST SCHEMAS / LIST INDEXES
    // -----------------------------------------------------------------------

    #[test]
    fn test_list_schemas() {
        let cmd = parse("LIST SCHEMAS data").unwrap();
        match cmd {
            Command::ListSchemas { table } => {
                assert_eq!(table, "data");
            }
            _ => panic!("Expected ListSchemas"),
        }
    }

    #[test]
    fn test_list_schemas_missing_table() {
        let result = parse("LIST SCHEMAS");
        assert!(result.is_err());
    }

    #[test]
    fn test_list_indexes() {
        let cmd = parse("LIST INDEXES data").unwrap();
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
        let cmd = parse("DESCRIBE SCHEMA data CONTACT").unwrap();
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
        let result = parse("DESCRIBE SCHEMA data");
        assert!(result.is_err());
    }

    #[test]
    fn test_describe_index() {
        let cmd = parse("DESCRIBE INDEX data email-idx").unwrap();
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
        let cmd = parse("CREATE INDEX data email-idx SCHEMA CONTACT KEY email STRING").unwrap();
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
        let cmd = parse("CREATE INDEX data price-idx SCHEMA PRODUCT KEY price NUMBER").unwrap();
        match cmd {
            Command::CreateIndex { key_type, .. } => {
                assert_eq!(key_type, KeyType::Number);
            }
            _ => panic!("Expected CreateIndex"),
        }
    }

    #[test]
    fn test_create_index_missing_args() {
        let result = parse("CREATE INDEX data email-idx");
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // DROP INDEX
    // -----------------------------------------------------------------------

    #[test]
    fn test_drop_index() {
        let cmd = parse("DROP INDEX data email-idx").unwrap();
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
        let cmd = parse("QUERY INDEX data email-idx key=alice").unwrap();
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
        let cmd = parse("QUERY INDEX data email-idx key=\"alice@test.com\" LIMIT 5 DESC").unwrap();
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
        let result = parse("QUERY INDEX data email-idx");
        assert!(result.is_err());
    }

    #[test]
    fn test_query_index_wrong_key_name() {
        let result = parse("QUERY INDEX data email-idx pk=alice");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("key="));
    }

    // -----------------------------------------------------------------------
    // Case insensitivity for new commands
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_schema_case_insensitive() {
        let cmd = parse("create schema data prefix CONTACT").unwrap();
        assert!(matches!(cmd, Command::CreateSchema { .. }));
    }

    #[test]
    fn test_query_index_case_insensitive() {
        let cmd = parse("query index data email-idx key=alice").unwrap();
        assert!(matches!(cmd, Command::QueryIndex { .. }));
    }
}
