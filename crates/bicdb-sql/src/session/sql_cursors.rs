//! SQL-level cursors: `DECLARE`, `FETCH`, `MOVE`, `CLOSE`, `pg_cursors` and
//! `UPDATE/DELETE ... WHERE CURRENT OF`.
//!
//! A cursor's query runs when it is declared, in the declaring statement's
//! transaction with the declaring role's privileges and row security, and its
//! rows are kept with the cursor: later changes in the transaction are not
//! seen (PostgreSQL's cursors are insensitive), and a `WITH HOLD` cursor
//! keeps its rows after COMMIT exactly as PostgreSQL materialises it then.
//! A transaction's end closes its cursors as PostgreSQL does: COMMIT closes
//! the ones without HOLD, ROLLBACK every cursor the transaction declared.

use crate::*;
use std::sync::{Arc, Mutex};

/// The cursors of one connection, shared by every session state copy.
#[derive(Clone, Debug, Default)]
pub struct SqlCursors(Arc<Mutex<Vec<SqlCursor>>>);

#[derive(Debug)]
pub(crate) struct SqlCursor {
    name: String,
    statement: String,
    holdable: bool,
    binary: bool,
    scrollable: bool,
    creation_time: SqlValue,
    columns: Vec<String>,
    column_types: Vec<Option<String>>,
    rows: Vec<Vec<SqlValue>>,
    /// 0 = before the first row, 1..=len on a row, len + 1 after the last.
    position: usize,
    /// Survives its transaction (a committed WITH HOLD cursor).
    committed: bool,
    target: Option<CursorTarget>,
}

/// The table row a `WHERE CURRENT OF` names: the cursor's rows carry the
/// table's primary key after their visible columns.
#[derive(Debug)]
struct CursorTarget {
    table: String,
    display: String,
    key: Vec<(String, String)>,
}

impl SqlCursors {
    fn with<T>(&self, f: impl FnOnce(&mut Vec<SqlCursor>) -> T) -> T {
        let mut guard = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        f(&mut guard)
    }

    /// COMMIT: cursors without HOLD close; held ones survive.
    pub(crate) fn commit(&self) {
        self.with(|cursors| {
            cursors.retain(|cursor| cursor.holdable);
            for cursor in cursors.iter_mut() {
                cursor.committed = true;
            }
        });
    }

    /// ROLLBACK: every cursor the transaction declared closes.
    pub(crate) fn rollback(&self) {
        self.with(|cursors| cursors.retain(|cursor| cursor.committed));
    }

    pub(crate) fn close_all(&self) {
        self.with(Vec::clear);
    }

    /// `pg_cursors` rows, ordered by name.
    pub(crate) fn catalog_rows(&self) -> Vec<BTreeMap<String, SqlValue>> {
        self.with(|cursors| {
            let mut rows = cursors
                .iter()
                .map(|cursor| {
                    BTreeMap::from([
                        ("name".to_string(), SqlValue::String(cursor.name.clone())),
                        (
                            "statement".to_string(),
                            SqlValue::String(cursor.statement.clone()),
                        ),
                        ("is_holdable".to_string(), SqlValue::Bool(cursor.holdable)),
                        ("is_binary".to_string(), SqlValue::Bool(cursor.binary)),
                        (
                            "is_scrollable".to_string(),
                            SqlValue::Bool(cursor.scrollable),
                        ),
                        ("creation_time".to_string(), cursor.creation_time.clone()),
                    ])
                })
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| left["name"].to_cell().cmp(&right["name"].to_cell()));
            rows
        })
    }
}

thread_local! {
    static ACTIVE_CURSORS: std::cell::RefCell<Option<SqlCursors>> = const { std::cell::RefCell::new(None) };
}

/// Makes a session's cursors visible to `pg_cursors` while it executes.
pub(crate) struct CursorScope {
    previous: Option<SqlCursors>,
}

impl CursorScope {
    pub(crate) fn enter(cursors: &SqlCursors) -> Self {
        let previous = ACTIVE_CURSORS.with(|active| active.replace(Some(cursors.clone())));
        Self { previous }
    }
}

impl Drop for CursorScope {
    fn drop(&mut self) {
        let previous = self.previous.take();
        ACTIVE_CURSORS.with(|active| *active.borrow_mut() = previous);
    }
}

/// The rows of `pg_cursors` for the executing session.
pub(crate) fn pg_cursors_rows() -> Vec<BTreeMap<String, SqlValue>> {
    ACTIVE_CURSORS.with(|active| {
        active
            .borrow()
            .as_ref()
            .map(SqlCursors::catalog_rows)
            .unwrap_or_default()
    })
}

pub(crate) const PG_CURSORS_COLUMNS: [(&str, &str); 6] = [
    ("name", "text"),
    ("statement", "text"),
    ("is_holdable", "bool"),
    ("is_binary", "bool"),
    ("is_scrollable", "bool"),
    ("creation_time", "timestamptz"),
];

fn undefined_cursor(name: &str) -> SqlError {
    SqlError::data_exception_public("34000", format!("cursor \"{name}\" does not exist"), None)
}

/// Splits SQL into words, keeping a double-quoted identifier as one word.
fn words(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut chars = text.char_indices().peekable();
    while let Some((start, ch)) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        if ch == '"' {
            let mut end = text.len();
            while let Some((index, next)) = chars.next() {
                if next == '"' {
                    if chars.peek().is_some_and(|(_, after)| *after == '"') {
                        chars.next();
                        continue;
                    }
                    end = index + 1;
                    break;
                }
            }
            words.push(text[start..end].to_string());
            continue;
        }
        let mut end = text.len();
        while let Some(&(index, next)) = chars.peek() {
            if next.is_whitespace() {
                end = index;
                break;
            }
            chars.next();
        }
        words.push(text[start..end].to_string());
    }
    words
}

fn cursor_name(word: &str) -> String {
    match word
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => word.to_ascii_lowercase(),
    }
}

fn keyword(word: Option<&String>, expected: &str) -> bool {
    word.is_some_and(|word| word.eq_ignore_ascii_case(expected))
}

/// How a FETCH or MOVE moves the cursor.
#[derive(Debug, PartialEq)]
enum Motion {
    /// `n` rows forward (negative: backward); `None` = ALL.
    Forward(Option<i64>),
    Backward(Option<i64>),
    Absolute(i64),
    Relative(i64),
}

fn parse_count(word: &str) -> Option<i64> {
    word.parse::<i64>().ok()
}

/// `FETCH`/`MOVE` arguments: the motion and the cursor name.
fn parse_fetch(rest: &[String]) -> Result<(Motion, String)> {
    let syntax = || SqlError::InvalidSql("syntax error in FETCH/MOVE".to_string());
    let (name, mut args) = rest.split_last().ok_or_else(syntax)?;
    if args
        .last()
        .is_some_and(|word| word.eq_ignore_ascii_case("from") || word.eq_ignore_ascii_case("in"))
    {
        args = &args[..args.len() - 1];
    }
    let upper = args
        .iter()
        .map(|word| word.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let refs = upper.iter().map(String::as_str).collect::<Vec<_>>();
    let motion = match refs.as_slice() {
        [] | ["NEXT"] => Motion::Forward(Some(1)),
        ["PRIOR"] => Motion::Backward(Some(1)),
        ["FIRST"] => Motion::Absolute(1),
        ["LAST"] => Motion::Absolute(-1),
        ["ALL"] | ["FORWARD", "ALL"] => Motion::Forward(None),
        ["BACKWARD", "ALL"] => Motion::Backward(None),
        ["FORWARD"] => Motion::Forward(Some(1)),
        ["BACKWARD"] => Motion::Backward(Some(1)),
        ["ABSOLUTE", n] => Motion::Absolute(parse_count(n).ok_or_else(syntax)?),
        ["RELATIVE", n] => Motion::Relative(parse_count(n).ok_or_else(syntax)?),
        ["FORWARD", n] => Motion::Forward(Some(parse_count(n).ok_or_else(syntax)?)),
        ["BACKWARD", n] => Motion::Backward(Some(parse_count(n).ok_or_else(syntax)?)),
        [n] => Motion::Forward(Some(parse_count(n).ok_or_else(syntax)?)),
        _ => return Err(syntax()),
    };
    Ok((motion, cursor_name(name)))
}

impl SqlCursor {
    fn len(&self) -> usize {
        self.rows.len()
    }

    fn require_backward(&self) -> Result<()> {
        if self.scrollable {
            Ok(())
        } else {
            Err(SqlError::data_exception_public(
                "55000",
                "cursor can only scan forward",
                None,
            ))
        }
    }

    /// Apply `motion`, returning the indexes (0-based) of the rows passed.
    fn advance(&mut self, motion: Motion) -> Result<Vec<usize>> {
        let len = self.len();
        let mut rows = Vec::new();
        let step_forward = |cursor: &mut Self, count: Option<i64>, rows: &mut Vec<usize>| {
            let mut remaining = count;
            loop {
                if remaining == Some(0) {
                    break;
                }
                if cursor.position > len {
                    break;
                }
                cursor.position += 1;
                if cursor.position > len {
                    break;
                }
                rows.push(cursor.position - 1);
                remaining = remaining.map(|count| count - 1);
            }
        };
        let step_backward = |cursor: &mut Self, count: Option<i64>, rows: &mut Vec<usize>| {
            let mut remaining = count;
            loop {
                if remaining == Some(0) || cursor.position == 0 {
                    break;
                }
                cursor.position -= 1;
                if cursor.position == 0 {
                    break;
                }
                rows.push(cursor.position - 1);
                remaining = remaining.map(|count| count - 1);
            }
        };
        let current = |cursor: &Self, rows: &mut Vec<usize>| {
            if (1..=len).contains(&cursor.position) {
                rows.push(cursor.position - 1);
            }
        };
        match motion {
            Motion::Forward(Some(0)) | Motion::Backward(Some(0)) | Motion::Relative(0) => {
                current(self, &mut rows)
            }
            Motion::Forward(Some(count)) if count < 0 => {
                self.require_backward()?;
                step_backward(self, Some(-count), &mut rows);
            }
            Motion::Backward(Some(count)) if count < 0 => {
                step_forward(self, Some(-count), &mut rows)
            }
            Motion::Forward(count) => step_forward(self, count, &mut rows),
            Motion::Backward(count) => {
                self.require_backward()?;
                step_backward(self, count, &mut rows);
            }
            Motion::Absolute(index) => {
                let target = if index > 0 {
                    (index as usize).min(len + 1)
                } else if index < 0 {
                    (len as i64 + 1 + index).max(0) as usize
                } else {
                    0
                };
                if target < self.position || (index < 0 && self.position <= len) {
                    self.require_backward()?;
                }
                self.position = target;
                current(self, &mut rows);
            }
            Motion::Relative(offset) => {
                if offset < 0 {
                    self.require_backward()?;
                }
                let target = (self.position as i64 + offset).clamp(0, len as i64 + 1) as usize;
                self.position = target;
                current(self, &mut rows);
            }
        }
        Ok(rows)
    }
}

/// Whether PostgreSQL's plan for `query` could run backward, which decides
/// the scrollability of a cursor declared without SCROLL or NO SCROLL: a
/// plain scan of one table or function can, a query without FROM, a join,
/// grouping, DISTINCT, a set operation or row locking cannot.
fn query_supports_backward_scan(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let [from] = select.from.as_slice() else {
        return false;
    };
    query.locks.is_empty()
        && from.joins.is_empty()
        && matches!(
            from.relation,
            TableFactor::Table { .. } | TableFactor::Function { .. }
        )
        && select.distinct.is_none()
        && select.having.is_none()
        && matches!(&select.group_by, GroupByExpr::Expressions(expressions, _) if expressions.is_empty())
        && !select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                expr_contains_aggregate(expr)
            }
            _ => false,
        })
}

/// Strip a trailing semicolon and surrounding whitespace.
fn statement_text(sql: &str) -> &str {
    sql.trim().trim_end_matches(';').trim_end()
}

impl SqlSession<'_> {
    /// DECLARE / FETCH / MOVE / CLOSE, and `... WHERE CURRENT OF` (rewritten
    /// to the cursor's current key): `None` when `sql` is none of them.
    pub(crate) fn execute_raw_cursor_statement(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let text = statement_text(sql);
        let first = text
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();
        if !matches!(first.as_str(), "declare" | "fetch" | "move" | "close") {
            return Ok(None);
        }
        if split_sql_statements(sql).len() != 1 {
            return Ok(None);
        }
        let words = words(text);
        match first.as_str() {
            "declare" => self.declare_cursor(sql, text, &words).map(Some),
            "fetch" | "move" => {
                let (motion, name) = parse_fetch(&words[1..])?;
                let fetch = first == "fetch";
                let cursors = self.sql_cursors.clone();
                cursors.with(|cursors| {
                    let cursor = cursors
                        .iter_mut()
                        .find(|cursor| cursor.name == name)
                        .ok_or_else(|| undefined_cursor(&name))?;
                    let passed = cursor.advance(motion)?;
                    if !fetch {
                        return Ok(Some(SqlResult::command(format!("MOVE {}", passed.len()))));
                    }
                    let visible = cursor.columns.len();
                    let rows = passed
                        .iter()
                        .map(|index| cursor.rows[*index][..visible].to_vec())
                        .collect::<Vec<_>>();
                    let mut result = SqlResult::new(cursor.columns.clone(), rows)
                        .with_column_types(cursor.column_types.clone());
                    result.command_tag = Some(format!("FETCH {}", passed.len()));
                    Ok(Some(result))
                })
            }
            _ => {
                let target = words.get(1).ok_or_else(|| {
                    SqlError::InvalidSql("CLOSE requires a cursor name or ALL".to_string())
                })?;
                if words.len() == 2 && target.eq_ignore_ascii_case("all") {
                    self.sql_cursors.close_all();
                    return Ok(Some(SqlResult::command("CLOSE CURSOR ALL")));
                }
                if words.len() != 2 {
                    return Err(SqlError::InvalidSql("syntax error in CLOSE".to_string()));
                }
                let name = cursor_name(target);
                self.sql_cursors.with(|cursors| {
                    let before = cursors.len();
                    cursors.retain(|cursor| cursor.name != name);
                    if cursors.len() == before {
                        Err(undefined_cursor(&name))
                    } else {
                        Ok(Some(SqlResult::command("CLOSE CURSOR")))
                    }
                })
            }
        }
    }

    fn declare_cursor(&mut self, sql: &str, text: &str, words: &[String]) -> Result<SqlResult> {
        let syntax = || SqlError::InvalidSql(format!("syntax error in DECLARE: {text}"));
        let name = cursor_name(words.get(1).ok_or_else(syntax)?);
        let mut index = 2;
        let (mut binary, mut scroll) = (false, None);
        loop {
            let word = words.get(index).ok_or_else(syntax)?.to_ascii_uppercase();
            match word.as_str() {
                "BINARY" => binary = true,
                "ASENSITIVE" | "INSENSITIVE" => {}
                "SCROLL" => scroll = Some(true),
                "NO" if keyword(words.get(index + 1), "scroll") => {
                    scroll = Some(false);
                    index += 1;
                }
                "CURSOR" => break,
                _ => return Err(syntax()),
            }
            index += 1;
        }
        index += 1;
        let mut holdable = false;
        if keyword(words.get(index), "with") && keyword(words.get(index + 1), "hold") {
            holdable = true;
            index += 2;
        } else if keyword(words.get(index), "without") && keyword(words.get(index + 1), "hold") {
            index += 2;
        }
        if !keyword(words.get(index), "for") {
            return Err(syntax());
        }
        // The query text follows the FOR keyword.
        let query_text = {
            let mut seen = 0usize;
            let mut offset = 0usize;
            let mut rest = text;
            while seen <= index {
                let trimmed = rest.trim_start();
                offset += rest.len() - trimmed.len();
                let word = &words[seen];
                offset += word.len();
                rest = &text[offset..];
                seen += 1;
            }
            rest.trim()
        };
        if !holdable && self.tx.is_none() {
            return Err(SqlError::data_exception_public(
                "25P01",
                "DECLARE CURSOR can only be used in transaction blocks",
                None,
            ));
        }
        if self
            .sql_cursors
            .with(|cursors| cursors.iter().any(|cursor| cursor.name == name))
        {
            return Err(SqlError::data_exception_public(
                "42P03",
                format!("cursor \"{name}\" already exists"),
                None,
            ));
        }
        let mut statements = parse_statements(query_text)?;
        let [Statement::Query(_)] = statements.as_slice() else {
            return Err(SqlError::data_exception_public(
                "42601",
                "DECLARE CURSOR requires a SELECT or VALUES query",
                None,
            ));
        };
        let Some(Statement::Query(mut query)) = statements.pop() else {
            unreachable!("checked above");
        };
        let default_scrollable = query_supports_backward_scan(&query);
        let target = self.cursor_target(&mut query)?;
        let autocommit = self.tx.is_none();
        let result = self.execute(&query.to_string())?;
        let hidden = target.as_ref().map_or(0, |target| target.key.len());
        let visible = result.columns.len().saturating_sub(hidden);
        let mut column_types = result.column_types.clone();
        column_types.resize(result.columns.len(), None);
        column_types.truncate(visible);
        let cursor = SqlCursor {
            name,
            statement: sql.trim().to_string(),
            holdable,
            binary,
            scrollable: scroll.unwrap_or(default_scrollable),
            creation_time: SqlValue::String(
                chrono::Utc::now()
                    .format("%Y-%m-%d %H:%M:%S%.6f+00")
                    .to_string(),
            ),
            columns: result.columns[..visible].to_vec(),
            column_types,
            rows: result.rows,
            position: 0,
            // A WITH HOLD cursor declared outside a transaction block belongs
            // to the statement's own, already committed, transaction.
            committed: autocommit,
            target,
        };
        self.sql_cursors.with(|cursors| cursors.push(cursor));
        Ok(SqlResult::command("DECLARE CURSOR"))
    }

    /// For a simple scan of one table with a primary key, append the key
    /// columns to the cursor's query so `WHERE CURRENT OF` can name the row.
    fn cursor_target(&self, query: &mut Query) -> Result<Option<CursorTarget>> {
        let SetExpr::Select(select) = query.body.as_mut() else {
            return Ok(None);
        };
        let [from] = select.from.as_slice() else {
            return Ok(None);
        };
        if !from.joins.is_empty()
            || select.distinct.is_some()
            || select.having.is_some()
            || !matches!(&select.group_by, GroupByExpr::Expressions(expressions, _) if expressions.is_empty())
        {
            return Ok(None);
        }
        let TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } = &from.relation
        else {
            return Ok(None);
        };
        let display = object_name(name)?;
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &relation_name(name)?);
        let Some(schema) = load_schema(self.db_ref(), &table)? else {
            return Ok(None);
        };
        let key = primary_key_columns_for_schema(&schema);
        if key.is_empty() {
            return Ok(None);
        }
        let qualifier = alias
            .as_ref()
            .map(|alias| alias.name.clone())
            .unwrap_or_else(|| {
                name.0
                    .last()
                    .and_then(|part| part.as_ident().cloned())
                    .unwrap_or_else(|| Ident::new(display.clone()))
            });
        let mut key_columns = Vec::with_capacity(key.len());
        for column in &key {
            let pg_type = schema
                .column(column)
                .map(|column| column.pg_type.clone())
                .unwrap_or_else(|| "text".to_string());
            select.projection.push(SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(vec![
                    qualifier.clone(),
                    Ident::with_quote('"', column.clone()),
                ]),
                alias: Ident::with_quote('"', format!("bicdb$cursor_key${column}")),
            });
            key_columns.push((column.clone(), pg_type));
        }
        Ok(Some(CursorTarget {
            table,
            display,
            key: key_columns,
        }))
    }

    /// `UPDATE ... WHERE CURRENT OF c` / `DELETE ... WHERE CURRENT OF c`
    /// with the clause replaced by the cursor's current primary key.
    pub(crate) fn rewrite_where_current_of(&self, sql: &str) -> Result<Option<String>> {
        let lower = sql.to_ascii_lowercase();
        let Some(at) = lower.find("current of") else {
            return Ok(None);
        };
        let first = lower.split_whitespace().next().unwrap_or("");
        if !matches!(first, "update" | "delete") || split_sql_statements(sql).len() != 1 {
            return Ok(None);
        }
        let before = sql[..at].trim_end();
        if !before.to_ascii_lowercase().ends_with("where") {
            return Ok(None);
        }
        let after = &sql[at + "current of".len()..];
        let after_trimmed = after.trim_start();
        let name_len = if after_trimmed.starts_with('"') {
            after_trimmed[1..]
                .find('"')
                .map(|end| end + 2)
                .unwrap_or(after_trimmed.len())
        } else {
            after_trimmed
                .find(|ch: char| !(ch.is_alphanumeric() || ch == '_' || ch == '$'))
                .unwrap_or(after_trimmed.len())
        };
        let name = cursor_name(&after_trimmed[..name_len]);
        let tail = &after_trimmed[name_len..];
        let (table, display, condition) = self.sql_cursors.with(|cursors| {
            let cursor = cursors
                .iter()
                .find(|cursor| cursor.name == name)
                .ok_or_else(|| undefined_cursor(&name))?;
            let target = cursor.target.as_ref().ok_or_else(|| {
                SqlError::Unsupported(format!(
                    "WHERE CURRENT OF needs cursor \"{name}\" to scan one table with a primary key"
                ))
            })?;
            if !(1..=cursor.len()).contains(&cursor.position) {
                return Err(SqlError::data_exception_public(
                    "24000",
                    format!("cursor \"{name}\" is not positioned on a row"),
                    None,
                ));
            }
            let row = &cursor.rows[cursor.position - 1];
            let visible = cursor.columns.len();
            let condition = target
                .key
                .iter()
                .enumerate()
                .map(|(index, (column, pg_type))| {
                    let value = &row[visible + index];
                    format!(
                        "\"{}\" = '{}'::{}",
                        column.replace('"', "\"\""),
                        value.to_cell().replace('\'', "''"),
                        pg_type
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            Ok((target.table.clone(), target.display.clone(), condition))
        })?;
        let rewritten = format!("{before} {condition}{tail}");
        // The statement must target the cursor's table.
        let statements = parse_statements(&rewritten)?;
        let target_table = match statements.as_slice() {
            [Statement::Update(update)] => match &update.table.relation {
                TableFactor::Table { name, .. } => Some(name.clone()),
                _ => None,
            },
            [Statement::Delete(delete)] => match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                    tables.first().and_then(|table| match &table.relation {
                        TableFactor::Table { name, .. } => Some(name.clone()),
                        _ => None,
                    })
                }
            },
            _ => None,
        };
        let same_table = match target_table {
            Some(name) => {
                resolve_session_relation_name_if_exists(self.db_ref(), &relation_name(&name)?)
                    == table
            }
            None => false,
        };
        if !same_table {
            return Err(SqlError::data_exception_public(
                "24000",
                format!("cursor \"{name}\" is not a simple updatable scan of table \"{display}\""),
                None,
            ));
        }
        Ok(Some(rewritten))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor(rows: usize, scrollable: bool) -> SqlCursor {
        SqlCursor {
            name: "c".into(),
            statement: String::new(),
            holdable: false,
            binary: false,
            scrollable,
            creation_time: SqlValue::Null,
            columns: vec!["g".into()],
            column_types: vec![None],
            rows: (1..=rows as i64).map(|n| vec![SqlValue::Int(n)]).collect(),
            position: 0,
            committed: false,
            target: None,
        }
    }

    #[test]
    fn fetch_directions_follow_postgres() {
        let mut c = cursor(5, true);
        assert_eq!(c.advance(Motion::Forward(Some(2))).unwrap(), [0, 1]);
        assert_eq!(c.advance(Motion::Absolute(-1)).unwrap(), [4]);
        assert_eq!(c.advance(Motion::Backward(Some(1))).unwrap(), [3]);
        assert_eq!(c.advance(Motion::Absolute(1)).unwrap(), [0]);
        assert_eq!(c.advance(Motion::Absolute(4)).unwrap(), [3]);
        assert_eq!(c.advance(Motion::Relative(-2)).unwrap(), [1]);
        assert_eq!(c.advance(Motion::Forward(None)).unwrap(), [2, 3, 4]);
        assert!(c.advance(Motion::Forward(Some(1))).unwrap().is_empty());
        assert_eq!(c.advance(Motion::Backward(None)).unwrap(), [4, 3, 2, 1, 0]);
        assert_eq!(c.position, 0);
        assert!(c.advance(Motion::Absolute(9)).unwrap().is_empty());
        assert_eq!(c.position, 6);
        let mut forward_only = cursor(3, false);
        forward_only.advance(Motion::Forward(Some(2))).unwrap();
        assert_eq!(
            forward_only
                .advance(Motion::Backward(Some(1)))
                .unwrap_err()
                .sqlstate(),
            "55000"
        );
    }

    #[test]
    fn fetch_arguments_parse() {
        let parse = |text: &str| parse_fetch(&words(text)).unwrap();
        assert_eq!(parse("c"), (Motion::Forward(Some(1)), "c".into()));
        assert_eq!(parse("2 FROM C"), (Motion::Forward(Some(2)), "c".into()));
        assert_eq!(
            parse("BACKWARD ALL IN c"),
            (Motion::Backward(None), "c".into())
        );
        assert_eq!(
            parse("ABSOLUTE -1 FROM \"X\""),
            (Motion::Absolute(-1), "X".into())
        );
        assert_eq!(parse("RELATIVE 0 c"), (Motion::Relative(0), "c".into()));
    }
}
