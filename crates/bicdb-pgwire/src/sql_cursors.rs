//! SQL-level cursors: `DECLARE name CURSOR FOR query`, `FETCH`, `MOVE` and
//! `CLOSE` (what client libraries use for "server-side" or named cursors).
//!
//! The query runs once at DECLARE, like PostgreSQL's snapshot semantics for
//! a cursor, and its rows are kept with the connection; FETCH returns them in
//! order. Cursors are forward-only. A cursor declared WITHOUT HOLD (the
//! default) can only be declared inside a transaction block and is closed when
//! that transaction ends; WITH HOLD cursors live until CLOSE or disconnect.
use super::*;

#[derive(Debug)]
pub(crate) struct SqlCursor {
    result: SqlResult,
    position: usize,
    hold: bool,
}

#[derive(Debug, PartialEq)]
enum CursorCommand {
    Declare {
        name: String,
        hold: bool,
        query: String,
    },
    Fetch {
        name: String,
        count: Option<usize>,
        tag: &'static str,
    },
    Close {
        name: Option<String>,
    },
}

/// Read an identifier: a double-quoted name keeps its case, a bare one folds
/// to lower case. Returns the name and the rest of the input.
fn identifier(input: &str) -> Option<(String, &str)> {
    let input = input.trim_start();
    if let Some(rest) = input.strip_prefix('"') {
        let mut name = String::new();
        let mut chars = rest.char_indices();
        while let Some((index, ch)) = chars.next() {
            if ch == '"' {
                if rest[index + 1..].starts_with('"') {
                    name.push('"');
                    chars.next();
                    continue;
                }
                return Some((name, &rest[index + 1..]));
            }
            name.push(ch);
        }
        return None;
    }
    let end = input
        .find(|ch: char| !(ch.is_alphanumeric() || ch == '_' || ch == '$'))
        .unwrap_or(input.len());
    if end == 0 {
        return None;
    }
    Some((input[..end].to_ascii_lowercase(), &input[end..]))
}

/// Split off the next bare word (lower-cased) and the remaining text.
fn word(input: &str) -> Option<(String, &str)> {
    let input = input.trim_start();
    let end = input
        .find(|ch: char| ch.is_whitespace() || ch == ';' || ch == '"')
        .unwrap_or(input.len());
    if end == 0 {
        return None;
    }
    Some((input[..end].to_ascii_lowercase(), &input[end..]))
}

fn parse(sql: &str) -> Result<Option<CursorCommand>> {
    let text = sql.trim().trim_end_matches(';').trim_end();
    let Some((first, rest)) = word(text) else {
        return Ok(None);
    };
    match first.as_str() {
        "declare" => {
            let Some((name, mut rest)) = identifier(rest) else {
                return Ok(None);
            };
            // [BINARY] [ASENSITIVE | INSENSITIVE] [[NO] SCROLL] CURSOR
            loop {
                let Some((option, after)) = word(rest) else {
                    return Ok(None);
                };
                rest = after;
                match option.as_str() {
                    "cursor" => break,
                    "asensitive" | "insensitive" => {}
                    "no" => {
                        let Some((keyword, after)) = word(rest) else {
                            return Ok(None);
                        };
                        if keyword != "scroll" {
                            return Ok(None);
                        }
                        rest = after;
                    }
                    "scroll" => {
                        return Err(PgWireError::Sql(SqlError::Unsupported(
                            "scrollable cursors are not supported; declare the cursor NO SCROLL"
                                .into(),
                        )))
                    }
                    "binary" => {
                        return Err(PgWireError::Sql(SqlError::Unsupported(
                            "binary cursors are not supported".into(),
                        )))
                    }
                    _ => return Ok(None),
                }
            }
            let mut hold = false;
            let (next, after) = word(rest).unwrap_or_default();
            if next == "with" || next == "without" {
                let (keyword, after) = word(after).unwrap_or_default();
                if keyword != "hold" {
                    return Ok(None);
                }
                hold = next == "with";
                rest = after;
            }
            let (keyword, query) = word(rest).unwrap_or_default();
            if keyword != "for" || query.trim().is_empty() {
                return Ok(None);
            }
            Ok(Some(CursorCommand::Declare {
                name,
                hold,
                query: query.trim().to_string(),
            }))
        }
        "fetch" | "move" => {
            let tag = if first == "fetch" { "FETCH" } else { "MOVE" };
            // FETCH [ direction ] [ FROM | IN ] name
            let mut rest = rest;
            let mut count = Some(1);
            let (direction, after) = word(rest).unwrap_or_default();
            match direction.as_str() {
                "next" => rest = after,
                "all" => {
                    count = None;
                    rest = after;
                }
                "forward" => {
                    rest = after;
                    let (amount, after) = word(rest).unwrap_or_default();
                    if amount == "all" {
                        count = None;
                        rest = after;
                    } else if let Ok(amount) = amount.parse::<usize>() {
                        count = Some(amount);
                        rest = after;
                    }
                }
                "prior" | "first" | "last" | "absolute" | "relative" | "backward" => {
                    return Err(PgWireError::Sql(SqlError::Unsupported(
                        "cursors are forward-only; only NEXT, FORWARD, ALL and a count are supported"
                            .into(),
                    )))
                }
                other => {
                    if let Ok(amount) = other.parse::<usize>() {
                        count = Some(amount);
                        rest = after;
                    } else if other.starts_with('-') {
                        return Err(PgWireError::Sql(SqlError::Unsupported(
                            "cursors are forward-only".into(),
                        )));
                    }
                }
            }
            let (keyword, after) = word(rest).unwrap_or_default();
            if keyword == "from" || keyword == "in" {
                rest = after;
            }
            let Some((name, tail)) = identifier(rest) else {
                return Ok(None);
            };
            if !tail.trim().is_empty() {
                return Ok(None);
            }
            Ok(Some(CursorCommand::Fetch { name, count, tag }))
        }
        "close" => {
            let (keyword, _) = word(rest).unwrap_or_default();
            if keyword == "all" && !rest.trim_start().starts_with('"') {
                return Ok(Some(CursorCommand::Close { name: None }));
            }
            let Some((name, tail)) = identifier(rest) else {
                return Ok(None);
            };
            if !tail.trim().is_empty() {
                return Ok(None);
            }
            Ok(Some(CursorCommand::Close { name: Some(name) }))
        }
        _ => Ok(None),
    }
}

fn missing_cursor(name: &str) -> PgWireError {
    PgWireError::Sql(SqlError::DataException {
        sqlstate: "34000",
        message: format!("cursor \"{name}\" does not exist"),
        data_type: None,
    })
}

/// Run a cursor command, or return `None` for any other statement.
pub(crate) fn execute(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    sql: &str,
    normalized: &str,
) -> Result<Option<SqlResult>> {
    if !(normalized.starts_with("declare ")
        || normalized.starts_with("fetch ")
        || normalized.starts_with("move ")
        || normalized.starts_with("close "))
    {
        return Ok(None);
    }
    let Some(command) = parse(sql)? else {
        return Ok(None);
    };
    if state.failed_transaction {
        return Err(PgWireError::InFailedTransaction);
    }
    match command {
        CursorCommand::Declare { name, hold, query } => {
            if !hold && !state.in_transaction {
                return Err(PgWireError::Sql(SqlError::DataException {
                    sqlstate: "25P01",
                    message: "DECLARE CURSOR can only be used in transaction blocks".into(),
                    data_type: None,
                }));
            }
            if state.sql_cursors.contains_key(&name) {
                return Err(PgWireError::Sql(SqlError::DataException {
                    sqlstate: "42P03",
                    message: format!("cursor \"{name}\" already exists"),
                    data_type: None,
                }));
            }
            let normalized_query = normalize_executable_sql(&query);
            if !(normalized_query.starts_with("select ")
                || normalized_query.starts_with("with ")
                || normalized_query.starts_with("values ")
                || normalized_query.starts_with("table ")
                || normalized_query.starts_with('('))
            {
                return Err(PgWireError::Sql(SqlError::InvalidSql(
                    "DECLARE CURSOR requires a SELECT or VALUES query".into(),
                )));
            }
            let mut result = execute_server_sql(server, state, &query)?;
            result.command_tag = None;
            state.sql_cursors.insert(
                name,
                SqlCursor {
                    result,
                    position: 0,
                    hold,
                },
            );
            Ok(Some(SqlResult::command("DECLARE CURSOR")))
        }
        CursorCommand::Fetch { name, count, tag } => {
            let cursor = state
                .sql_cursors
                .get_mut(&name)
                .ok_or_else(|| missing_cursor(&name))?;
            let available = cursor.result.rows.len() - cursor.position;
            let taken = count.map_or(available, |count| count.min(available));
            let start = cursor.position;
            cursor.position += taken;
            if tag == "MOVE" {
                return Ok(Some(SqlResult::command(format!("MOVE {taken}"))));
            }
            let mut result = SqlResult {
                columns: cursor.result.columns.clone(),
                rows: cursor.result.rows[start..start + taken].to_vec(),
                command_tag: Some(format!("FETCH {taken}")),
                column_types: cursor.result.column_types.clone(),
                column_metadata: cursor.result.column_metadata.clone(),
                notices: Vec::new(),
            };
            // Rows already handed out are not kept.
            if cursor.position == cursor.result.rows.len() {
                cursor.result.rows.clear();
                cursor.position = 0;
            }
            result.notices.clear();
            Ok(Some(result))
        }
        CursorCommand::Close { name: None } => {
            state.sql_cursors.clear();
            Ok(Some(SqlResult::command("CLOSE CURSOR")))
        }
        CursorCommand::Close { name: Some(name) } => {
            state
                .sql_cursors
                .remove(&name)
                .ok_or_else(|| missing_cursor(&name))?;
            Ok(Some(SqlResult::command("CLOSE CURSOR")))
        }
    }
}

/// Close the cursors that end with the transaction (WITHOUT HOLD).
pub(crate) fn close_transaction_cursors(state: &mut ConnectionState) {
    state.sql_cursors.retain(|_, cursor| cursor.hold);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_client_library_cursor_commands() {
        assert_eq!(
            parse("DECLARE \"ss_cursor\" CURSOR WITHOUT HOLD FOR SELECT * FROM \"tabCountry\" ORDER BY name").unwrap(),
            Some(CursorCommand::Declare {
                name: "ss_cursor".into(),
                hold: false,
                query: "SELECT * FROM \"tabCountry\" ORDER BY name".into(),
            })
        );
        assert_eq!(
            parse("declare C1 no scroll cursor with hold for values (1)").unwrap(),
            Some(CursorCommand::Declare {
                name: "c1".into(),
                hold: true,
                query: "values (1)".into(),
            })
        );
        assert_eq!(
            parse("FETCH FORWARD 2000 FROM \"ss_cursor\"").unwrap(),
            Some(CursorCommand::Fetch {
                name: "ss_cursor".into(),
                count: Some(2000),
                tag: "FETCH",
            })
        );
        assert_eq!(
            parse("fetch all in c1;").unwrap(),
            Some(CursorCommand::Fetch {
                name: "c1".into(),
                count: None,
                tag: "FETCH",
            })
        );
        assert_eq!(
            parse("FETCH c1").unwrap(),
            Some(CursorCommand::Fetch {
                name: "c1".into(),
                count: Some(1),
                tag: "FETCH",
            })
        );
        assert_eq!(
            parse("CLOSE \"ss_cursor\"").unwrap(),
            Some(CursorCommand::Close {
                name: Some("ss_cursor".into())
            })
        );
        assert_eq!(
            parse("close all").unwrap(),
            Some(CursorCommand::Close { name: None })
        );
        assert!(parse("FETCH PRIOR FROM c1").is_err());
        assert_eq!(parse("declare x int").unwrap(), None);
    }
}
