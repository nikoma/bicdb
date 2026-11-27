//! `INSERT ... OVERRIDING {SYSTEM | USER} VALUE` and the same clause on a
//! MERGE `INSERT` action.
//!
//! `OVERRIDING SYSTEM VALUE` lets a value be written into a `GENERATED
//! ALWAYS AS IDENTITY` column; `OVERRIDING USER VALUE` discards the values
//! written for identity columns, which take their sequence values instead.
//! Generated (expression) columns are not affected by either.
//!
//! The clause is removed before parsing. USER VALUE rewrites the statement
//! (identity values become DEFAULT). SYSTEM VALUE is handed to the one
//! INSERT it belongs to through [`take_system_value`], which that INSERT
//! consumes before anything it runs (triggers, functions in its source) can
//! issue another INSERT, so no other statement is exempted.
use super::*;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::cell::RefCell;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Overriding {
    System,
    User,
}

thread_local! {
    /// The resolved table of the INSERT that may write identity columns.
    static SYSTEM_VALUE_FOR: RefCell<Option<String>> = const { RefCell::new(None) };
    /// The OVERRIDING kind of the MERGE about to run.
    static MERGE_OVERRIDING: RefCell<Option<Overriding>> = const { RefCell::new(None) };
}

/// True (once) when the INSERT into `table` being executed was written with
/// `OVERRIDING SYSTEM VALUE`.
pub(crate) fn take_system_value(table: &str) -> bool {
    SYSTEM_VALUE_FOR.with(|cell| {
        let mut cell = cell.borrow_mut();
        if cell.as_deref() == Some(table) {
            *cell = None;
            true
        } else {
            false
        }
    })
}

/// The OVERRIDING clause of the MERGE being executed (taken once).
pub(crate) fn take_merge_overriding() -> Option<Overriding> {
    MERGE_OVERRIDING.with(|cell| cell.borrow_mut().take())
}

thread_local! {
    /// The OVERRIDING clause of the MERGE statement executing now.
    static MERGE_SCOPE: std::cell::Cell<Option<Overriding>> = const { std::cell::Cell::new(None) };
}

/// Holds a MERGE's OVERRIDING clause while it runs; a MERGE nested in it
/// (a trigger's) starts with none.
pub(crate) struct MergeScope(Option<Overriding>);

impl MergeScope {
    pub(crate) fn enter() -> Self {
        let kind = take_merge_overriding();
        Self(MERGE_SCOPE.with(|cell| cell.replace(kind)))
    }
}

impl Drop for MergeScope {
    fn drop(&mut self) {
        MERGE_SCOPE.with(|cell| cell.set(self.0));
    }
}

pub(crate) fn disarm_system_value() {
    SYSTEM_VALUE_FOR.with(|cell| *cell.borrow_mut() = None);
}

/// The OVERRIDING clause of the MERGE executing now.
pub(crate) fn merge_overriding() -> Option<Overriding> {
    MERGE_SCOPE.with(std::cell::Cell::get)
}

/// Lets the next INSERT into `table` write identity columns (a MERGE
/// action with OVERRIDING SYSTEM VALUE).
pub(crate) fn arm_system_value(table: &str) {
    SYSTEM_VALUE_FOR.with(|cell| *cell.borrow_mut() = Some(table.to_string()));
}

fn keyword(token: &Token, word: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

struct Stripped {
    sql: String,
    merge: bool,
    overriding: Overriding,
}

fn strip(sql: &str) -> Result<Option<Stripped>> {
    if !sql.to_ascii_lowercase().contains("overriding") {
        return Ok(None);
    }
    let Ok(tokens) = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, sql).tokenize()
    else {
        return Ok(None);
    };
    let significant: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_)))
        .map(|(index, _)| index)
        .collect();
    // One statement at a time (a batch is split before it gets here).
    if significant[..significant.len().saturating_sub(1)]
        .iter()
        .any(|&index| tokens[index] == Token::SemiColon)
    {
        return Ok(None);
    }
    let first = significant.first().map(|&index| &tokens[index]);
    let merge = first.is_some_and(|token| keyword(token, "MERGE"));
    if !merge && !first.is_some_and(|token| keyword(token, "INSERT")) {
        return Ok(None);
    }
    let mut removed = Vec::new();
    let mut kinds = Vec::new();
    for (position, &index) in significant.iter().enumerate() {
        if !keyword(&tokens[index], "OVERRIDING") {
            continue;
        }
        let at = |offset: usize| significant.get(position + offset).map(|&i| &tokens[i]);
        let kind = match at(1) {
            Some(token) if keyword(token, "SYSTEM") => Overriding::System,
            Some(token) if keyword(token, "USER") => Overriding::User,
            _ => continue,
        };
        if !at(2).is_some_and(|token| keyword(token, "VALUE")) {
            continue;
        }
        kinds.push(kind);
        removed.extend([index, significant[position + 1], significant[position + 2]]);
    }
    let Some(&overriding) = kinds.first() else {
        return Ok(None);
    };
    if kinds.iter().any(|kind| *kind != overriding) {
        return Err(SqlError::Unsupported(
            "MERGE INSERT actions with different OVERRIDING clauses".to_string(),
        ));
    }
    let sql = tokens
        .iter()
        .enumerate()
        .filter(|(index, _)| !removed.contains(index))
        .map(|(_, token)| crate::eval::rewrite_helpers::token_sql(token))
        .collect();
    Ok(Some(Stripped {
        sql,
        merge,
        overriding,
    }))
}

impl<'a> SqlSession<'a> {
    pub(crate) fn execute_raw_insert_overriding(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some(stripped) = strip(sql)? else {
            return Ok(None);
        };
        let statements = parse_statements(&stripped.sql)?;
        let [statement] = statements.as_slice() else {
            return Ok(None);
        };
        if stripped.merge {
            if !matches!(statement, Statement::Merge(_)) {
                return Ok(None);
            }
            MERGE_OVERRIDING.with(|cell| *cell.borrow_mut() = Some(stripped.overriding));
            let result = self.execute_inner(&stripped.sql);
            MERGE_OVERRIDING.with(|cell| *cell.borrow_mut() = None);
            return result.map(Some);
        }
        let Statement::Insert(insert) = statement else {
            return Ok(None);
        };
        let TableObject::TableName(table_name) = &insert.table else {
            return Ok(None);
        };
        let table =
            resolve_session_relation_name_if_exists(self.db_ref(), &relation_name(table_name)?);
        match stripped.overriding {
            Overriding::System => {
                SYSTEM_VALUE_FOR.with(|cell| *cell.borrow_mut() = Some(table));
                let result = self.execute_inner(&stripped.sql);
                SYSTEM_VALUE_FOR.with(|cell| *cell.borrow_mut() = None);
                result.map(Some)
            }
            Overriding::User => {
                let Some(schema) = load_schema(self.db_ref(), &table)? else {
                    return self.execute_inner(&stripped.sql).map(Some);
                };
                let rewritten = discard_identity_values(insert, &schema)?;
                self.execute_inner(&rewritten.to_string()).map(Some)
            }
        }
    }
}

/// `insert` with the values written for identity columns replaced by their
/// defaults (OVERRIDING USER VALUE).
fn discard_identity_values(insert: &Insert, schema: &TableSchema) -> Result<Insert> {
    let mut insert = insert.clone();
    let targets: Vec<String> = if insert.columns.is_empty() {
        schema
            .columns
            .iter()
            .filter(|column| !column.hidden && column.generated_expr.is_none())
            .map(|column| column.name.clone())
            .collect()
    } else {
        insert
            .columns
            .iter()
            .map(|name| object_name(name).unwrap_or_default())
            .collect()
    };
    let identity: Vec<bool> = targets
        .iter()
        .map(|name| {
            schema
                .column(name)
                .is_some_and(|column| column.identity.is_some())
        })
        .collect();
    if !identity.contains(&true) {
        return Ok(insert);
    }
    let Some(source) = insert.source.as_mut() else {
        return Ok(insert);
    };
    if let SetExpr::Values(values) = source.body.as_mut() {
        if source.with.is_none() && source.order_by.is_none() && source.limit_clause.is_none() {
            for row in &mut values.rows {
                for (position, value) in row.iter_mut().enumerate() {
                    if identity.get(position).copied().unwrap_or(false) {
                        *value = Expr::Identifier(Ident::new("DEFAULT"));
                    }
                }
            }
            return Ok(insert);
        }
    }
    // Any other source: keep only the non-identity positions of its rows.
    let width = targets.len();
    let names = (1..=width)
        .map(|position| format!("__bicdb_ov{position}"))
        .collect::<Vec<_>>();
    let kept = (0..width)
        .filter(|&position| !identity[position])
        .collect::<Vec<_>>();
    let wrapped = if kept.is_empty() {
        None
    } else {
        Some(format!(
            "SELECT {} FROM ({}) AS __bicdb_ov ({})",
            kept.iter()
                .map(|&position| format!("__bicdb_ov.{}", names[position]))
                .collect::<Vec<_>>()
                .join(", "),
            source,
            names.join(", ")
        ))
    };
    let Some(wrapped) = wrapped else {
        return Err(SqlError::Unsupported(
            "INSERT ... OVERRIDING USER VALUE writing only identity columns from a query"
                .to_string(),
        ));
    };
    let parsed = parse_statements(&wrapped)?;
    let [Statement::Query(query)] = parsed.as_slice() else {
        return Err(SqlError::InvalidSql(
            "OVERRIDING USER VALUE rewrite did not produce a query".to_string(),
        ));
    };
    insert.source = Some(query.clone());
    insert.columns = kept
        .iter()
        .map(|&position| ObjectName::from(vec![Ident::with_quote('"', targets[position].clone())]))
        .collect();
    Ok(insert)
}
