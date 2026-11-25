//! Stored rows of a materialized view and `REFRESH MATERIALIZED VIEW`.
//!
//! `CREATE MATERIALIZED VIEW` evaluates its query once (as the creator, who
//! owns the view) and keeps the rows in the view's catalog descriptor, so
//! later reads return that snapshot until `REFRESH MATERIALIZED VIEW`
//! recomputes it as the view's owner. `WITH NO DATA` leaves the view
//! unpopulated; reading it then fails as in PostgreSQL. Descriptors written
//! before snapshots existed carry none and keep expanding their query.
use super::*;
use sqlparser::tokenizer::{Token, Tokenizer, Word};

/// The rows a materialized view returns.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct MaterializedSnapshot {
    pub(crate) populated: bool,
    #[serde(default)]
    pub(crate) rows: Vec<Vec<SqlValue>>,
}

pub(crate) fn not_populated(view: &str) -> SqlError {
    SqlError::RaisedException {
        sqlstate: "55000".to_string(),
        message: format!(
            "materialized view \"{}\" has not been populated",
            view.rsplit('.').next().unwrap_or(view)
        ),
        detail: None,
    }
}

pub(crate) fn cannot_change(view: &str) -> SqlError {
    SqlError::RaisedException {
        sqlstate: "42809".to_string(),
        message: format!(
            "cannot change materialized view \"{}\"",
            view.rsplit('.').next().unwrap_or(view)
        ),
        detail: None,
    }
}

/// Whether the view's column values survive the descriptor's storage form:
/// built-in scalar types do; array, composite, user-defined and spatial
/// values do not, and such a view keeps expanding its query when populated.
pub(crate) fn snapshot_supported(view: &ViewSchema) -> bool {
    view.columns.iter().all(|column| {
        column.user_type.is_none()
            && pg_type_oid_by_name(&column.pg_type).is_some()
            && !column.pg_type.ends_with("[]")
            && !column.pg_type.to_ascii_lowercase().starts_with("geometry")
            && !column.pg_type.to_ascii_lowercase().starts_with("geography")
            && !column.pg_type.to_ascii_lowercase().starts_with("tsquery")
            && !column.pg_type.to_ascii_lowercase().starts_with("record")
    })
}

/// The snapshot to store: `None` (query expansion) for a populated view whose
/// values cannot be stored.
pub(crate) fn snapshot_for(
    view: &ViewSchema,
    populated: bool,
    rows: Vec<Vec<SqlValue>>,
) -> Option<MaterializedSnapshot> {
    if populated && !snapshot_supported(view) {
        return None;
    }
    Some(MaterializedSnapshot { populated, rows })
}

/// The rows a snapshot materialized view returns, `None` for a legacy one.
pub(crate) fn snapshot_rows(view: &ViewSchema) -> Result<Option<CteResult>> {
    if !view.materialized {
        return Ok(None);
    }
    let Some(snapshot) = &view.snapshot else {
        return Ok(None);
    };
    if !snapshot.populated {
        return Err(not_populated(&view.name));
    }
    // Stored values come back in their JSON form; restore each column's type.
    let mut rows = snapshot.rows.clone();
    for row in &mut rows {
        for (value, column) in row.iter_mut().zip(&view.columns) {
            if matches!(value, SqlValue::Null) {
                continue;
            }
            let needs_cast = match value {
                SqlValue::String(_) => !matches!(
                    column.pg_type.as_str(),
                    "text" | "varchar" | "bpchar" | "name"
                ),
                SqlValue::Int(_) => !matches!(column.pg_type.as_str(), "int2" | "int4" | "int8"),
                SqlValue::Float(_) => !matches!(column.pg_type.as_str(), "float4" | "float8"),
                SqlValue::Json(_) => !matches!(column.pg_type.as_str(), "json" | "jsonb"),
                _ => false,
            };
            if needs_cast {
                *value = cast_value_to_pg_type(value.clone(), &column.pg_type)?;
            }
        }
    }
    Ok(Some(
        CteResult::new(
            view.name.clone(),
            view.columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            rows,
        )
        .with_column_types(
            view.columns
                .iter()
                .map(|column| Some(column.pg_type.clone()))
                .collect(),
        ),
    ))
}

struct RawRefresh {
    concurrently: bool,
    name: ObjectName,
    with_data: bool,
}

fn word(token: &Token) -> Option<String> {
    match token {
        Token::Word(Word {
            value,
            quote_style: None,
            ..
        }) => Some(value.to_ascii_uppercase()),
        _ => None,
    }
}

/// `REFRESH MATERIALIZED VIEW [CONCURRENTLY] name [WITH [NO] DATA]`.
fn parse_refresh(sql: &str) -> Result<Option<RawRefresh>> {
    let head = sql.trim_start();
    if head.len() < 7 || !head[..7].eq_ignore_ascii_case("refresh") {
        return Ok(None);
    }
    let dialect = PostgreSqlDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        return Ok(None);
    };
    let mut tokens: Vec<Token> = tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();
    while matches!(tokens.last(), Some(Token::SemiColon)) {
        tokens.pop();
    }
    if tokens.iter().any(|token| matches!(token, Token::SemiColon)) {
        return Ok(None);
    }
    let syntax = || SqlError::InvalidSql("syntax error in REFRESH MATERIALIZED VIEW".to_string());
    let words: Vec<Option<String>> = tokens.iter().map(word).collect();
    if words.first().cloned().flatten().as_deref() != Some("REFRESH") {
        return Ok(None);
    }
    // Other REFRESH statements (for example `REFRESH CUBE`) belong to their
    // own handlers.
    if words.get(1).cloned().flatten().as_deref() != Some("MATERIALIZED") {
        return Ok(None);
    }
    if words.get(2).cloned().flatten().as_deref() != Some("VIEW") {
        return Err(syntax());
    }
    let mut index = 3;
    let mut concurrently = false;
    if words.get(index).cloned().flatten().as_deref() == Some("CONCURRENTLY") {
        concurrently = true;
        index += 1;
    }
    let mut end = tokens.len();
    let mut with_data = true;
    let upper_at = |i: usize| words.get(i).cloned().flatten();
    if end >= index + 3
        && upper_at(end - 1).as_deref() == Some("DATA")
        && upper_at(end - 2).as_deref() == Some("NO")
        && upper_at(end - 3).as_deref() == Some("WITH")
    {
        with_data = false;
        end -= 3;
    } else if end >= index + 2
        && upper_at(end - 1).as_deref() == Some("DATA")
        && upper_at(end - 2).as_deref() == Some("WITH")
    {
        end -= 2;
    }
    let mut parts = Vec::new();
    let mut position = index;
    loop {
        let Some(Token::Word(word)) = tokens.get(position) else {
            return Err(syntax());
        };
        parts.push(Ident {
            value: word.value.clone(),
            quote_style: word.quote_style,
            span: sqlparser::tokenizer::Span::empty(),
        });
        position += 1;
        if position == end {
            break;
        }
        if !matches!(tokens.get(position), Some(Token::Period)) {
            return Err(syntax());
        }
        position += 1;
    }
    Ok(Some(RawRefresh {
        concurrently,
        name: ObjectName::from(parts),
        with_data,
    }))
}

impl SqlSession<'_> {
    /// The materialized rows of `view` computed now, as its owner.
    pub(crate) fn compute_materialized_rows(
        &self,
        view: &ViewSchema,
    ) -> Result<Vec<Vec<SqlValue>>> {
        let mut live = view.clone();
        live.snapshot = None;
        // The query reads its sources with the owner's privileges, as
        // PostgreSQL runs a materialized view's query as its owner.
        let owner = normalize_role_name(view.owner.as_deref().unwrap_or(BOOTSTRAP_ROLE_NAME));
        let mut checker = self.sql_engine();
        let mut gucs = (*checker.session_gucs).clone();
        gucs.insert(RLS_CHECK_AS_GUC.to_string(), owner);
        checker.session_gucs = Arc::new(gucs);
        checker.require_from_relations_readable(&parse_query(&view.query_sql)?)?;
        let alias = view
            .name
            .rsplit('.')
            .next()
            .unwrap_or(&view.name)
            .to_string();
        let result = self
            .sql_engine()
            .materialize_view_with_selection(&live, &alias, None, None, false)?;
        Ok(result.rows.as_ref().clone())
    }

    pub(crate) fn execute_raw_refresh_materialized_view(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(refresh) = parse_refresh(sql)? else {
            return Ok(None);
        };
        self.require_transaction_write()?;
        let requested = relation_name(&refresh.name)?;
        let name = resolve_session_relation_name_if_exists(self.db_ref(), &requested);
        let short = name.rsplit('.').next().unwrap_or(&name).to_string();
        let Some(mut view) = load_view(self.db_ref(), &name)? else {
            if load_schema(self.db_ref(), &name)?.is_some() {
                return Err(SqlError::RaisedException {
                    sqlstate: "42809".to_string(),
                    message: format!("\"{short}\" is not a materialized view"),
                    detail: None,
                });
            }
            return Err(SqlError::RaisedException {
                sqlstate: "42P01".to_string(),
                message: format!("relation \"{short}\" does not exist"),
                detail: None,
            });
        };
        if !view.materialized {
            return Err(SqlError::RaisedException {
                sqlstate: "42809".to_string(),
                message: format!("\"{short}\" is not a materialized view"),
                detail: None,
            });
        }
        if self
            .require_relation_ownership(&name, "refresh materialized view")
            .is_err()
        {
            return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                "permission denied for materialized view {short}"
            ))));
        }
        if refresh.concurrently && !refresh.with_data {
            return Err(SqlError::InvalidSql(
                "REFRESH options CONCURRENTLY and WITH NO DATA cannot be used together".to_string(),
            ));
        }
        let populated = view
            .snapshot
            .as_ref()
            .is_none_or(|snapshot| snapshot.populated);
        if refresh.concurrently {
            if !populated {
                return Err(SqlError::RaisedException {
                    sqlstate: "0A000".to_string(),
                    message:
                        "CONCURRENTLY cannot be used when the materialized view is not populated"
                            .to_string(),
                    detail: None,
                });
            }
            let has_unique = view
                .indexes
                .iter()
                .any(|index| index.unique && index.predicate.is_none());
            if !has_unique {
                return Err(SqlError::RaisedException {
                    sqlstate: "55000".to_string(),
                    message: format!(
                        "cannot refresh materialized view \"{}\" concurrently",
                        if name.contains('.') {
                            name.clone()
                        } else {
                            format!("public.{name}")
                        }
                    ),
                    detail: None,
                });
            }
        }
        let rows = if refresh.with_data && snapshot_supported(&view) {
            self.compute_materialized_rows(&view)?
        } else {
            Vec::new()
        };
        view.snapshot = snapshot_for(&view, refresh.with_data, rows);
        self.save_session_view(&view)?;
        Ok(Some(SqlResult::command("REFRESH MATERIALIZED VIEW")))
    }
}

/// Splits `CREATE MATERIALIZED VIEW ... AS query WITH [NO] DATA` into the
/// statement without the clause and whether it has data.
pub(crate) fn strip_materialized_data_clause(sql: &str) -> Option<(String, bool)> {
    let trimmed = sql.trim_end().trim_end_matches(';').trim_end();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.trim_start().starts_with("CREATE") || !upper.contains("MATERIALIZED") {
        return None;
    }
    let before = upper.strip_suffix("DATA")?.trim_end();
    let (before, with_data) = match before.strip_suffix("NO") {
        Some(rest) if rest.ends_with(char::is_whitespace) => (rest.trim_end(), false),
        _ => (before, true),
    };
    let before = before.strip_suffix("WITH")?;
    if !before.ends_with(|c: char| c.is_whitespace() || c == ')') {
        return None;
    }
    Some((trimmed[..before.len()].trim_end().to_string(), with_data))
}

#[cfg(test)]
mod refresh_parse_tests {
    use super::parse_refresh;

    #[test]
    fn other_refresh_statements_are_left_to_their_own_handlers() {
        assert!(parse_refresh("REFRESH CUBE refreshed").unwrap().is_none());
        assert!(parse_refresh("REFRESH CUBE \"../victim\"")
            .unwrap()
            .is_none());
        assert!(parse_refresh("REFRESH MATERIALIZED TABLE x").is_err());
        assert!(parse_refresh("REFRESH MATERIALIZED VIEW mv")
            .unwrap()
            .is_some());
    }
}
