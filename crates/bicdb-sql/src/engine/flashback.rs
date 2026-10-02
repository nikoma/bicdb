//! Flashback (time travel) queries.
//!
//! Accepted syntax, after a table reference:
//!
//! * Oracle: `AS OF SCN expr`, `AS OF TIMESTAMP expr`,
//!   `VERSIONS BETWEEN {SCN | TIMESTAMP} {expr | MINVALUE} AND {expr | MAXVALUE}`
//! * SQL:2011 / SQL Server / MariaDB: `FOR SYSTEM_TIME AS OF expr`,
//!   `FOR SYSTEM_TIME BETWEEN expr AND expr`, `FOR SYSTEM_TIME FROM expr TO expr`,
//!   `FOR SYSTEM_TIME ALL`
//!
//! Before parsing, each clause is rewritten into table-function arguments on
//! the same table: `t AS OF SCN 5` becomes `t (__bicdb_flashback => 'scn', 5)`.
//! That encoding is deliberate. Every relation fast path in this engine (point
//! lookups, index scans, count shortcuts, join fast paths, plan caches) only
//! matches `TableFactor::Table { args: None, .. }`, so a historical relation
//! can never be answered from current data by a fast path: it always reaches
//! [`SqlEngine::flashback_row_set`], which applies the relation's privilege
//! check, the core collection policy and row-level security exactly as a
//! current-data scan does.
use super::*;
#[allow(unused_imports)]
use crate::*;
use bicdb_core::{FlashbackOperation, FlashbackPoint};
use sqlparser::dialect::{Dialect, Precedence};
use sqlparser::tokenizer::{Location, TokenWithSpan, Tokenizer};

/// Named table-function argument that marks a flashback relation.
pub(crate) const FLASHBACK_ARG: &str = "__bicdb_flashback";

/// Oracle `VERSIONS BETWEEN` pseudocolumns. They are hidden from `*` in a
/// `SELECT` whose `FROM` contains a `VERSIONS` source, as in Oracle.
pub(crate) const FLASHBACK_PSEUDO_COLUMNS: [&str; 6] = [
    "versions_startscn",
    "versions_starttime",
    "versions_endscn",
    "versions_endtime",
    "versions_xid",
    "versions_operation",
];

pub(crate) fn is_flashback_pseudo_column(column: &str) -> bool {
    let name = column.rsplit('.').next().unwrap_or(column);
    FLASHBACK_PSEUDO_COLUMNS
        .iter()
        .any(|pseudo| name.eq_ignore_ascii_case(pseudo))
}

// ------------------------------------------------------------------ rewrite

fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

fn is_word(token: &Token, word: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

fn is_space(token: &Token) -> bool {
    matches!(token, Token::Whitespace(_))
}

fn next_solid(tokens: &[TokenWithSpan], from: usize) -> Option<usize> {
    (from..tokens.len())
        .find(|&idx| !is_space(&tokens[idx].token) && tokens[idx].token != Token::EOF)
}

fn prev_solid(tokens: &[TokenWithSpan], before: usize) -> Option<usize> {
    (0..before).rev().find(|&idx| !is_space(&tokens[idx].token))
}

/// Byte offset of a tokenizer location (1-based line and character column).
fn byte_offset(sql: &str, line_starts: &[usize], location: Location) -> Option<usize> {
    let line_start = *line_starts.get((location.line as usize).checked_sub(1)?)?;
    let column = (location.column as usize).checked_sub(1)?;
    let rest = sql.get(line_start..)?;
    Some(
        line_start
            + rest
                .char_indices()
                .nth(column)
                .map(|(offset, _)| offset)
                .unwrap_or(rest.len()),
    )
}

struct Rewriter<'a> {
    sql: &'a str,
    tokens: Vec<TokenWithSpan>,
    line_starts: Vec<usize>,
}

impl Rewriter<'_> {
    fn start(&self, idx: usize) -> Option<usize> {
        byte_offset(self.sql, &self.line_starts, self.tokens[idx].span.start)
    }

    fn end(&self, idx: usize) -> Option<usize> {
        byte_offset(self.sql, &self.line_starts, self.tokens[idx].span.end)
    }

    /// Parses one bound expression starting at token `from`. Returns its source
    /// text and the index of its last token. `MINVALUE`/`MAXVALUE` become NULL.
    fn bound(&self, from: usize, stop_before_and: bool) -> Option<(String, usize)> {
        let first = next_solid(&self.tokens, from)?;
        if is_word(&self.tokens[first].token, "MINVALUE")
            || is_word(&self.tokens[first].token, "MAXVALUE")
        {
            return Some(("NULL".to_string(), first));
        }
        let dialect = PostgreSqlDialect {};
        let mut parser =
            Parser::new(&dialect).with_tokens_with_locations(self.tokens[first..].to_vec());
        let parsed = if stop_before_and {
            parser.parse_subexpr(dialect.prec_value(Precedence::And) + 1)
        } else {
            parser.parse_expr()
        };
        parsed.ok()?;
        let consumed = parser.index();
        let last = prev_solid(&self.tokens, first + consumed)?;
        if last < first {
            return None;
        }
        let text = self
            .sql
            .get(self.start(first)?..self.end(last)?)?
            .to_string();
        Some((text, last))
    }

    /// Recognizes a clause starting at token `idx`. Returns the replacement
    /// text and the index of the clause's last token.
    fn clause(&self, idx: usize) -> Option<(String, usize)> {
        let token = &self.tokens[idx].token;
        // A clause must follow a table reference (a name or a quoted name).
        let previous = prev_solid(&self.tokens, idx)?;
        if !matches!(self.tokens[previous].token, Token::Word(_)) {
            return None;
        }
        let at = |offset: usize| next_solid(&self.tokens, offset);
        if is_word(token, "AS") {
            let of = at(idx + 1)?;
            if !is_word(&self.tokens[of].token, "OF") {
                return None;
            }
            let kind_idx = at(of + 1)?;
            let kind = if is_word(&self.tokens[kind_idx].token, "SCN") {
                "scn"
            } else if is_word(&self.tokens[kind_idx].token, "TIMESTAMP") {
                "timestamp"
            } else {
                return None;
            };
            let (expr, last) = self.bound(kind_idx + 1, false)?;
            return Some((format!(" ({FLASHBACK_ARG} => '{kind}', {expr})"), last));
        }
        if is_word(token, "VERSIONS") {
            let between = at(idx + 1)?;
            if !is_word(&self.tokens[between].token, "BETWEEN") {
                return None;
            }
            let kind_idx = at(between + 1)?;
            let kind = if is_word(&self.tokens[kind_idx].token, "SCN") {
                "versions_scn"
            } else if is_word(&self.tokens[kind_idx].token, "TIMESTAMP") {
                "versions_timestamp"
            } else {
                return None;
            };
            let (low, low_last) = self.bound(kind_idx + 1, true)?;
            let and = at(low_last + 1)?;
            if !is_word(&self.tokens[and].token, "AND") {
                return None;
            }
            let (high, last) = self.bound(and + 1, false)?;
            return Some((
                format!(" ({FLASHBACK_ARG} => '{kind}', {low}, {high})"),
                last,
            ));
        }
        if is_word(token, "FOR") {
            let system_time = at(idx + 1)?;
            if !is_word(&self.tokens[system_time].token, "SYSTEM_TIME") {
                return None;
            }
            let next = at(system_time + 1)?;
            let next_token = &self.tokens[next].token;
            if is_word(next_token, "ALL") {
                return Some((
                    format!(" ({FLASHBACK_ARG} => 'versions_system_time', NULL, NULL)"),
                    next,
                ));
            }
            if is_word(next_token, "AS") {
                let of = at(next + 1)?;
                if !is_word(&self.tokens[of].token, "OF") {
                    return None;
                }
                let (expr, last) = self.bound(of + 1, false)?;
                return Some((format!(" ({FLASHBACK_ARG} => 'system_time', {expr})"), last));
            }
            let (separator, stop_before_and) = if is_word(next_token, "BETWEEN") {
                ("AND", true)
            } else if is_word(next_token, "FROM") {
                ("TO", false)
            } else {
                return None;
            };
            let (low, low_last) = self.bound(next + 1, stop_before_and)?;
            let sep = at(low_last + 1)?;
            if !is_word(&self.tokens[sep].token, separator) {
                return None;
            }
            let (high, last) = self.bound(sep + 1, false)?;
            return Some((
                format!(" ({FLASHBACK_ARG} => 'versions_system_time', {low}, {high})"),
                last,
            ));
        }
        None
    }
}

/// Rewrites flashback clauses into table-function arguments (see the module
/// docs). Returns `None` when the statement contains none.
pub(crate) fn rewrite_flashback_clauses(sql: &str) -> Option<String> {
    // Cheap pre-filter: nearly every statement skips tokenization entirely.
    if !(contains_ascii_case_insensitive(sql, "AS OF")
        || contains_ascii_case_insensitive(sql, "VERSIONS")
        || contains_ascii_case_insensitive(sql, "SYSTEM_TIME"))
    {
        return None;
    }
    let dialect = PostgreSqlDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .ok()?;
    let mut line_starts = vec![0];
    line_starts.extend(sql.match_indices('\n').map(|(offset, _)| offset + 1));
    let rewriter = Rewriter {
        sql,
        tokens,
        line_starts,
    };
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut idx = 0;
    while idx < rewriter.tokens.len() {
        if let Some((replacement, last)) = rewriter.clause(idx) {
            let start = rewriter.start(idx)?;
            let end = rewriter.end(last)?;
            edits.push((start, end, replacement));
            idx = last + 1;
        } else {
            idx += 1;
        }
    }
    if edits.is_empty() {
        return None;
    }
    let mut rewritten = sql.to_string();
    for (start, end, replacement) in edits.into_iter().rev() {
        rewritten.replace_range(start..end, &replacement);
    }
    Some(rewritten)
}

// -------------------------------------------------------------- evaluation

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PointKind {
    Scn,
    Timestamp,
    /// SQL:2011 `FOR SYSTEM_TIME`: a timestamp, or an SCN when the value is an
    /// integer.
    Auto,
}

pub(crate) struct FlashbackRequest {
    versions: bool,
    kind: PointKind,
    bounds: Vec<Expr>,
}

/// Decodes the marker arguments of a flashback relation, if `args` carry them.
pub(crate) fn flashback_request(args: &TableFunctionArgs) -> Result<Option<FlashbackRequest>> {
    // PostgreSQL's `name => value` parses as `ExprNamed`; accept both forms.
    let (name, arg) = match args.args.first() {
        Some(FunctionArg::Named { name, arg, .. }) => (name.value.as_str(), arg),
        Some(FunctionArg::ExprNamed {
            name: Expr::Identifier(name),
            arg,
            ..
        }) => (name.value.as_str(), arg),
        _ => return Ok(None),
    };
    if !name.eq_ignore_ascii_case(FLASHBACK_ARG) {
        return Ok(None);
    }
    let FunctionArgExpr::Expr(Expr::Value(value)) = arg else {
        return Err(SqlError::InvalidSql(
            "malformed flashback clause".to_string(),
        ));
    };
    let Value::SingleQuotedString(kind) = &value.value else {
        return Err(SqlError::InvalidSql(
            "malformed flashback clause".to_string(),
        ));
    };
    let (versions, kind, arity) = match kind.as_str() {
        "scn" => (false, PointKind::Scn, 1),
        "timestamp" => (false, PointKind::Timestamp, 1),
        "system_time" => (false, PointKind::Auto, 1),
        "versions_scn" => (true, PointKind::Scn, 2),
        "versions_timestamp" => (true, PointKind::Timestamp, 2),
        "versions_system_time" => (true, PointKind::Auto, 2),
        other => {
            return Err(SqlError::InvalidSql(format!(
                "unknown flashback clause `{other}`"
            )))
        }
    };
    let bounds = args.args[1..]
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr.clone()),
            _ => Err(SqlError::InvalidSql(
                "malformed flashback clause".to_string(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    if bounds.len() != arity {
        return Err(SqlError::InvalidSql(
            "malformed flashback clause".to_string(),
        ));
    }
    Ok(Some(FlashbackRequest {
        versions,
        kind,
        bounds,
    }))
}

/// True when a `SELECT` reads a `VERSIONS` source, whose pseudocolumns `*`
/// must not expand to.
pub(crate) fn select_has_flashback_versions(select: &Select) -> bool {
    fn factor_has(factor: &TableFactor) -> bool {
        match factor {
            TableFactor::Table {
                args: Some(args), ..
            } => matches!(flashback_request(args), Ok(Some(request)) if request.versions),
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => table_has(table_with_joins),
            _ => false,
        }
    }
    fn table_has(table: &TableWithJoins) -> bool {
        factor_has(&table.relation) || table.joins.iter().any(|join| factor_has(&join.relation))
    }
    select.from.iter().any(table_has)
}

pub(crate) fn unix_micros_from_timestamp_text(text: &str) -> Result<i64> {
    let timestamp = parse_timestamptz(text).map_err(|error| {
        SqlError::InvalidSql(format!("invalid flashback timestamp `{text}`: {error}"))
    })?;
    let micros = timestamp
        .finite_micros()
        .ok_or_else(|| SqlError::InvalidSql("flashback timestamp must be finite".to_string()))?;
    micros.checked_add(946_684_800_000_000).ok_or_else(|| {
        SqlError::InvalidSql(format!("flashback timestamp `{text}` is out of range"))
    })
}

pub(crate) fn timestamp_text_from_unix_micros(micros: i64) -> String {
    render_timestamptz(PgTimestamp::Finite(
        micros.saturating_sub(946_684_800_000_000),
    ))
}

fn point_from_value(kind: PointKind, value: SqlValue) -> Result<Option<FlashbackPoint>> {
    let as_scn = |value: &SqlValue| -> Result<u64> {
        sql_value_i64(value)
            .and_then(|scn| u64::try_from(scn).ok())
            .ok_or_else(|| SqlError::InvalidSql(format!("invalid SCN {}", value.to_cell())))
    };
    Ok(Some(match (kind, &value) {
        (_, SqlValue::Null) => return Ok(None),
        (PointKind::Scn, value) => FlashbackPoint::Scn(as_scn(value)?),
        (PointKind::Auto, SqlValue::Int(_)) => FlashbackPoint::Scn(as_scn(&value)?),
        (PointKind::Timestamp | PointKind::Auto, value) => {
            FlashbackPoint::TimestampMicros(unix_micros_from_timestamp_text(&value.to_cell())?)
        }
    }))
}

impl SqlEngine<'_> {
    fn flashback_point(&self, kind: PointKind, expr: &Expr) -> Result<Option<FlashbackPoint>> {
        point_from_value(kind, self.eval_select_constant_expr(expr)?)
    }

    /// Rows of `name` as of a past point, or every version in a range, with
    /// the same authorization as a scan of the current table.
    pub(crate) fn flashback_row_set(
        &self,
        name: &ObjectName,
        alias: Option<&TableAlias>,
        request: FlashbackRequest,
    ) -> Result<RowSet> {
        let table = relation_name(name)?;
        let table = resolve_session_relation_name(self.db_ref(), &table)?;
        self.require_relation_privilege(&table, "SELECT")?;
        let alias_name = alias
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(&table).to_string());
        let db = self.db_ref();
        let (records, versions) = if request.versions {
            let low = self.flashback_point(request.kind, &request.bounds[0])?;
            let high = self.flashback_point(request.kind, &request.bounds[1])?;
            let versions = match self.security_context.as_ref() {
                Some(ctx) => db.flashback_versions_with_context(ctx, &table, low, high),
                None => db.flashback_versions(&table, low, high),
            }?;
            let records = versions
                .iter()
                .map(|version| version.record.clone())
                .collect();
            (records, Some(versions))
        } else {
            let point = self
                .flashback_point(request.kind, &request.bounds[0])?
                .ok_or_else(|| SqlError::InvalidSql("AS OF point cannot be NULL".to_string()))?;
            let records = match self.security_context.as_ref() {
                Some(ctx) => db.flashback_rows_with_context(ctx, &table, point),
                None => db.flashback_rows(&table, point),
            }?;
            (records, None)
        };

        // SQL row-level security, exactly as `scan_records` applies it.
        let schema = load_schema_shared(db, &table)?;
        let bypass = self
            .security_context
            .as_ref()
            .is_some_and(|ctx| ctx.bypass_policy.is_some());
        let rls = !bypass && schema.as_deref().is_some_and(|schema| schema.rls_enabled);
        let mut kept: Vec<(Arc<Record>, usize)> = Vec::with_capacity(records.len());
        for (idx, record) in records.into_iter().enumerate() {
            let record = Arc::new(record);
            if rls
                && !rls_allows_record_with_schema(
                    self,
                    &table,
                    PolicyAction::Select,
                    &record,
                    schema.as_deref(),
                )?
            {
                continue;
            }
            kept.push((record, idx));
        }

        let source_records = kept
            .iter()
            .map(|(record, _)| Arc::clone(record))
            .collect::<Vec<_>>();
        let fields = row_fields_for_records(schema.as_deref(), &source_records);
        let mut columns = row_output_columns_from_fields(&table, &alias_name, &fields);
        columns.extend(postgres_system_output_columns(&table, &alias_name));
        if versions.is_some() {
            columns.extend(
                FLASHBACK_PSEUDO_COLUMNS
                    .iter()
                    .map(|column| format!("{alias_name}.{column}")),
            );
        }
        let mut rows = Vec::with_capacity(kept.len());
        for (position, (record, idx)) in kept.iter().enumerate() {
            if position % 1024 == 0 {
                self.check_cancellation()?;
            }
            let mut row = slot_row_from_record_fields(&table, &alias_name, &fields, record)?;
            row.extend(self.postgres_system_values(&table, &alias_name, schema.as_deref(), None));
            if let Some(versions) = versions.as_ref() {
                let version = &versions[*idx];
                let scn = |scn: Option<u64>| {
                    scn.map(|scn| SqlValue::Int(scn as i64))
                        .unwrap_or(SqlValue::Null)
                };
                let time = |scn: Option<u64>| {
                    scn.map(|scn| {
                        SqlValue::String(timestamp_text_from_unix_micros(
                            bicdb_core::scn_to_unix_micros(scn),
                        ))
                    })
                    .unwrap_or(SqlValue::Null)
                };
                row.push(scn(version.start_scn));
                row.push(time(version.start_scn));
                row.push(scn(version.end_scn));
                row.push(time(version.end_scn));
                row.push(if version.operation == FlashbackOperation::Baseline {
                    SqlValue::Null
                } else {
                    SqlValue::Int(version.xid as i64)
                });
                row.push(
                    version
                        .operation
                        .code()
                        .map(|code| SqlValue::String(code.to_string()))
                        .unwrap_or(SqlValue::Null),
                );
            }
            rows.push(row);
        }
        Ok(RowSet { rows, columns })
    }
}
