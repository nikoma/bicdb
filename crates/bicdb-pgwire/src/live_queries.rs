//! Live queries: `SUBSCRIBE <select>` streams a query's result and then every
//! change to it, for as long as the client stays connected.
//!
//! Rows carry two leading columns: `commit_seq`, the commit the result
//! reflects, and `change`, `+` for a row that entered the result and `-` for
//! one that left it. The initial result arrives as `+` rows. A changed row
//! appears as its old version with `-` and its new version with `+`.
//!
//! Changes are found by re-evaluating the query as the subscribing connection
//! (its permissions, row-level security and session settings), so every row
//! a subscriber receives is one it may read. How much is re-evaluated depends
//! on the query:
//!
//! - A filter over one table (`SELECT ... FROM t WHERE ...`, no join, group,
//!   limit or subquery) is re-evaluated for the rows a commit touched only:
//!   the table remembers the ids of recently written rows, and the query runs
//!   with `AND <key> IN (those ids)` added. Cost per change is proportional
//!   to the rows written, not to the table.
//! - A grouped aggregate over one table (`... GROUP BY g`) re-aggregates only
//!   the groups the written rows belonged to or belong to now.
//! - Anything else (joins, `LIMIT`, `DISTINCT`, subqueries, views) is re-run
//!   whole whenever a table it reads changed, and the results are diffed.
//!
//! Commits that arrive during an evaluation are picked up by the next one, so
//! a burst costs one evaluation. A table whose window of remembered writes
//! was exceeded falls back to a whole re-run for that round.
//!
//! Over the PostgreSQL protocol the rows stream as `DataRow` messages of one
//! open statement (drivers read them row by row in streaming or single-row
//! mode); `COPY (SUBSCRIBE ...) TO STDOUT` streams them as COPY data, which
//! `psql` prints as they arrive. A subscription ends when the client cancels
//! the statement (`SQLSTATE 57014`) or disconnects.
mod embedded;
pub use embedded::EmbeddedSubscription;

use super::*;
use sqlparser::ast::{
    visit_expressions, visit_relations, BinaryOperator, Expr, GroupByExpr, Ident, ObjectName,
    ObjectNamePart, SelectItem, SetExpr, Statement, TableFactor, Value as SqlAstValue,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::ControlFlow;

/// Longest wait for a commit before the connection is checked again.
const POLL: Duration = Duration::from_millis(250);
/// More changed rows than this in one round are re-read as a whole (each
/// changed row costs one key lookup).
const MAX_KEYED_ROWS: usize = 200;
/// Grouped plans remember which rows form each group; a table larger than
/// this is re-aggregated whole instead.
const MAX_MEMBERSHIP_ROWS: usize = 1_000_000;
/// Hidden projection columns of incremental plans.
const RID_COLUMN: &str = "__bicdb_rid";
const GROUP_COLUMN: &str = "__bicdb_group";

/// The query of a `SUBSCRIBE <query>` statement, if `sql` is one.
pub(crate) fn subscribed_query(sql: &str) -> Option<&str> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let keyword = trimmed.get(..9)?;
    if !keyword.eq_ignore_ascii_case("subscribe") {
        return None;
    }
    let rest = &trimmed[9..];
    // `SUBSCRIBE` must be a whole word: `SUBSCRIBED` is not it.
    if !rest.starts_with(|first: char| first.is_whitespace() || first == '(') {
        return None;
    }
    Some(rest.trim_start())
}

// ------------------------------------------------------------------ planning

/// A relation name as the SQL layer resolves it: `public.t` and `t` are the
/// collection `t`; anything else is left to the engine.
fn collection_of(name: &ObjectName) -> Option<String> {
    let parts = name
        .0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.to_ascii_lowercase()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    match parts.as_slice() {
        [name] => Some(name.clone()),
        [schema, name] if schema == "public" => Some(name.clone()),
        _ => None,
    }
}

/// Tables a query reads, as collection names, or `None` when a relation
/// could not be resolved (a view, a CTE name, another schema): the
/// subscription then re-runs on every commit.
fn watched_collections(db: &BicDb, statements: &[Statement]) -> Option<Vec<String>> {
    let mut ctes = Vec::new();
    for statement in statements {
        if let Statement::Query(query) = statement {
            if let Some(with) = &query.with {
                ctes.extend(
                    with.cte_tables
                        .iter()
                        .map(|cte| cte.alias.name.value.to_ascii_lowercase()),
                );
            }
        }
    }
    let mut names = Vec::new();
    for statement in statements {
        let flow: ControlFlow<()> = visit_relations(statement, |relation| {
            names.push(collection_of(relation));
            ControlFlow::Continue(())
        });
        let _ = flow;
    }
    let existing = db
        .collections()
        .into_iter()
        .map(|meta| meta.name)
        .collect::<HashSet<_>>();
    let mut watched = Vec::new();
    for name in names {
        let name = name?;
        if ctes.contains(&name) {
            continue;
        }
        if !existing.contains(&name) {
            return None;
        }
        watched.push(name);
    }
    watched.sort();
    watched.dedup();
    Some(watched)
}

/// How a subscription is kept current.
enum Plan {
    /// Re-run the whole query when a watched table changed.
    Full,
    /// One table, filtered: re-evaluate for the changed rows only.
    Filter(Keyed),
    /// One table, grouped by one column: re-aggregate the affected groups.
    Grouped(Keyed),
}

/// What an incremental plan needs to address rows and groups by key.
#[derive(Clone)]
struct Keyed {
    collection: String,
    /// The query with the row id (filtered) or the group key (grouped) added
    /// as a hidden last projection column.
    statement: Statement,
    /// The relation's qualifier in the query: its alias or its name.
    qualifier: String,
    key_column: String,
    key_type: String,
    /// Grouped: the group column and its type (learned from the first run).
    group: Option<(String, Option<String>)>,
}

const AGGREGATES: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "string_agg",
    "array_agg",
    "json_agg",
    "jsonb_agg",
    "json_object_agg",
    "jsonb_object_agg",
    "bool_and",
    "bool_or",
    "every",
    "bit_and",
    "bit_or",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "variance",
    "var_pop",
    "var_samp",
    "percentile_cont",
    "percentile_disc",
    "mode",
    "approx_count_distinct",
    "hll_count",
];

fn function_is_aggregate(name: &ObjectName) -> bool {
    name.0.last().is_some_and(|part| match part {
        ObjectNamePart::Identifier(ident) => {
            AGGREGATES.contains(&ident.value.to_ascii_lowercase().as_str())
        }
        _ => false,
    })
}

/// (any aggregate, any window function or subquery) in a statement.
fn expression_shape(statement: &Statement) -> (bool, bool) {
    let mut aggregate = false;
    let mut complex = false;
    let flow: ControlFlow<()> = visit_expressions(statement, |expr| {
        match expr {
            Expr::Function(function) => {
                if function.over.is_some() {
                    complex = true;
                }
                if function_is_aggregate(&function.name) {
                    aggregate = true;
                }
            }
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => complex = true,
            _ => {}
        }
        ControlFlow::Continue(())
    });
    let _ = flow;
    (aggregate, complex)
}

fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.clone()),
        _ => None,
    }
}

fn qualified(qualifier: &str, column: &str) -> Expr {
    Expr::CompoundIdentifier(vec![Ident::new(qualifier), Ident::new(column)])
}

fn hidden_item(qualifier: &str, column: &str, alias: &str) -> SelectItem {
    SelectItem::ExprWithAlias {
        expr: qualified(qualifier, column),
        alias: Ident::new(alias),
    }
}

/// Decides how a parsed query is kept current. Anything the incremental
/// plans cannot address by key is `Full`.
fn plan(db: &BicDb, statements: &[Statement]) -> Plan {
    let [statement] = statements else {
        return Plan::Full;
    };
    let (aggregate, complex) = expression_shape(statement);
    if complex {
        return Plan::Full;
    }
    let Statement::Query(query) = statement else {
        return Plan::Full;
    };
    if query.with.is_some() || query.limit_clause.is_some() || query.fetch.is_some() {
        return Plan::Full;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Plan::Full;
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.from.len() != 1
        || !select.from[0].joins.is_empty()
    {
        return Plan::Full;
    }
    let TableFactor::Table {
        name, alias, args, ..
    } = &select.from[0].relation
    else {
        return Plan::Full;
    };
    if args.is_some() {
        return Plan::Full;
    }
    let Some(collection) = collection_of(name) else {
        return Plan::Full;
    };
    let qualifier = alias
        .as_ref()
        .map(|alias| alias.name.value.clone())
        .unwrap_or_else(|| match name.0.last() {
            Some(ObjectNamePart::Identifier(ident)) => ident.value.clone(),
            _ => collection.clone(),
        });
    let Ok(Some((key_column, key_type))) = bicdb_sql::plain_primary_key_column(db, &collection)
    else {
        return Plan::Full;
    };
    // A projection that names the hidden columns would collide with them.
    if select.projection.iter().any(|item| match item {
        SelectItem::ExprWithAlias { alias, .. } => {
            alias.value.eq_ignore_ascii_case(RID_COLUMN)
                || alias.value.eq_ignore_ascii_case(GROUP_COLUMN)
        }
        _ => false,
    }) {
        return Plan::Full;
    }
    let group = match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty() => {
            if aggregate || select.having.is_some() {
                // A global aggregate: every row is one group.
                return Plan::Full;
            }
            None
        }
        GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => {
            let [expr] = exprs.as_slice() else {
                return Plan::Full;
            };
            match column_name(expr) {
                Some(column) => Some(column),
                None => return Plan::Full,
            }
        }
        _ => return Plan::Full,
    };
    let mut statement = statement.clone();
    if let Statement::Query(query) = &mut statement {
        if let SetExpr::Select(select) = query.body.as_mut() {
            select.projection.push(match &group {
                None => hidden_item(&qualifier, &key_column, RID_COLUMN),
                Some(column) => hidden_item(&qualifier, column, GROUP_COLUMN),
            });
        }
    }
    let keyed = Keyed {
        collection,
        statement,
        qualifier,
        key_column,
        key_type,
        group: group.map(|column| (column, None)),
    };
    if keyed.group.is_some() {
        Plan::Grouped(keyed)
    } else {
        Plan::Filter(keyed)
    }
}

fn is_numeric_type(pg_type: &str) -> bool {
    matches!(
        pg_type.to_ascii_lowercase().as_str(),
        "int2"
            | "int4"
            | "int8"
            | "smallint"
            | "int"
            | "integer"
            | "bigint"
            | "oid"
            | "numeric"
            | "decimal"
            | "float4"
            | "float8"
            | "real"
            | "double precision"
    )
}

/// A literal for a key's text form, typed like its column.
fn literal(text: &str, pg_type: &str) -> Expr {
    let numeric = is_numeric_type(pg_type)
        && !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'-' || byte == b'.');
    let value = if numeric {
        SqlAstValue::Number(text.to_string(), false)
    } else {
        SqlAstValue::SingleQuotedString(text.to_string())
    };
    Expr::Value(value.into())
}

/// `statement` with `AND <qualifier>.<column> IN (keys)` on its WHERE.
fn restricted(
    statement: &Statement,
    qualifier: &str,
    column: &str,
    column_type: &str,
    keys: &BTreeSet<String>,
) -> String {
    let mut statement = statement.clone();
    if let Statement::Query(query) = &mut statement {
        if let SetExpr::Select(select) = query.body.as_mut() {
            // One key is an equality: the engine's key fast path plans
            // `AND <key> = <value>`, not `IN`, so rows are addressed one
            // lookup at a time.
            let restriction = match keys.iter().collect::<Vec<_>>().as_slice() {
                [key] => Expr::BinaryOp {
                    left: Box::new(qualified(qualifier, column)),
                    op: BinaryOperator::Eq,
                    right: Box::new(literal(key, column_type)),
                },
                _ => Expr::InList {
                    expr: Box::new(qualified(qualifier, column)),
                    list: keys.iter().map(|key| literal(key, column_type)).collect(),
                    negated: false,
                },
            };
            select.selection = Some(match select.selection.take() {
                Some(existing) => Expr::BinaryOp {
                    left: Box::new(Expr::Nested(Box::new(existing))),
                    op: BinaryOperator::And,
                    right: Box::new(restriction),
                },
                None => restriction,
            });
        }
    }
    statement.to_string()
}

impl Keyed {
    /// The whole query, hidden column included.
    fn whole_sql(&self) -> String {
        self.statement.to_string()
    }

    /// The query for the rows with these keys (filtered plans).
    fn rows_sql(&self, keys: &BTreeSet<String>) -> String {
        restricted(
            &self.statement,
            &self.qualifier,
            &self.key_column,
            &self.key_type,
            keys,
        )
    }

    /// The query for these groups (grouped plans).
    fn groups_sql(&self, groups: &BTreeSet<String>) -> String {
        let (column, pg_type) = self
            .group
            .as_ref()
            .map(|(column, pg_type)| (column.clone(), pg_type.clone()))
            .unwrap_or_default();
        restricted(
            &self.statement,
            &self.qualifier,
            &column,
            pg_type.as_deref().unwrap_or("text"),
            groups,
        )
    }

    /// Grouped plans: `(row id, group key)` of the filtered rows, for the
    /// given keys or all of them.
    fn membership_sql(&self, keys: Option<&BTreeSet<String>>) -> String {
        let mut statement = self.statement.clone();
        if let Statement::Query(query) = &mut statement {
            if let SetExpr::Select(select) = query.body.as_mut() {
                select.projection =
                    vec![hidden_item(&self.qualifier, &self.key_column, RID_COLUMN)];
                if let Some((group, _)) = &self.group {
                    select
                        .projection
                        .push(hidden_item(&self.qualifier, group, GROUP_COLUMN));
                }
                select.group_by = GroupByExpr::Expressions(Vec::new(), Vec::new());
                select.having = None;
                query.order_by = None;
            }
        }
        match keys {
            Some(keys) => restricted(
                &statement,
                &self.qualifier,
                &self.key_column,
                &self.key_type,
                keys,
            ),
            None => statement.to_string(),
        }
    }

    /// The subscriber's query as written (the hidden column removed).
    fn original_sql(&self) -> String {
        let mut statement = self.statement.clone();
        if let Statement::Query(query) = &mut statement {
            if let SetExpr::Select(select) = query.body.as_mut() {
                select.projection.pop();
            }
        }
        statement.to_string()
    }
}

// ------------------------------------------------------------------ results

/// A result with its hidden trailing columns split off: `visible` is what
/// the subscriber sees; `hidden[i]` the hidden cells of row `i`.
struct Split {
    visible: SqlResult,
    hidden: Vec<Vec<String>>,
}

fn split_hidden(result: SqlResult, hidden_columns: usize) -> Split {
    let keep = result.columns.len().saturating_sub(hidden_columns);
    let mut hidden = Vec::with_capacity(result.rows.len());
    let rows = result
        .rows
        .into_iter()
        .map(|mut row| {
            hidden.push(
                row.drain(keep.min(row.len())..)
                    .map(|cell| cell.to_cell())
                    .collect(),
            );
            row
        })
        .collect();
    let mut visible = SqlResult::new(result.columns[..keep].to_vec(), rows);
    if result.column_types.len() >= keep {
        visible = visible.with_column_types(result.column_types[..keep].to_vec());
    }
    Split { visible, hidden }
}

type RowCounts = BTreeMap<Vec<String>, (usize, Vec<SqlValue>)>;

fn count_row(counts: &mut RowCounts, row: &[SqlValue]) {
    let key = row.iter().map(SqlValue::to_cell).collect::<Vec<_>>();
    counts.entry(key).or_insert_with(|| (0, row.to_vec())).0 += 1;
}

/// The multiset of rows, by their text cells.
fn row_counts<'a>(rows: impl Iterator<Item = &'a Vec<SqlValue>>) -> RowCounts {
    let mut counts = RowCounts::new();
    for row in rows {
        count_row(&mut counts, row);
    }
    counts
}

/// Rows that left (`-`) and entered (`+`) between two multisets.
fn changes(previous: &RowCounts, current: &RowCounts, commit_seq: u64) -> Vec<Vec<SqlValue>> {
    let mut out = Vec::new();
    let mut emit = |change: &str, row: &[SqlValue], times: usize| {
        for _ in 0..times {
            let mut values = Vec::with_capacity(row.len() + 2);
            values.push(SqlValue::Int(commit_seq as i64));
            values.push(SqlValue::String(change.to_string()));
            values.extend_from_slice(row);
            out.push(values);
        }
    };
    for (key, (count, row)) in previous {
        let now = current.get(key).map_or(0, |(count, _)| *count);
        if *count > now {
            emit("-", row, count - now);
        }
    }
    for (key, (count, row)) in current {
        let before = previous.get(key).map_or(0, |(count, _)| *count);
        if *count > before {
            emit("+", row, count - before);
        }
    }
    out
}

/// A batch of change rows with the subscription's columns and types.
fn batch(template: &SqlResult, rows: Vec<Vec<SqlValue>>) -> SqlResult {
    let mut columns = vec!["commit_seq".to_string(), "change".to_string()];
    columns.extend(template.columns.iter().cloned());
    let mut types = vec![Some("int8".to_string()), Some("text".to_string())];
    if template.column_types.len() == template.columns.len() {
        types.extend(template.column_types.iter().cloned());
    } else {
        types.extend(std::iter::repeat_n(None, template.columns.len()));
    }
    SqlResult::new(columns, rows).with_column_types(types)
}

/// True when the client closed the connection. Bytes waiting to be read
/// (the extended protocol's `Sync` after `Execute`, a pipelined query) do
/// not end the subscription; they are handled once it ends.
fn client_gone(stream: &ClientStream) -> bool {
    let tcp = stream.tcp();
    if tcp.set_nonblocking(true).is_err() {
        return true;
    }
    let mut probe = [0u8; 1];
    let gone = match tcp.peek(&mut probe) {
        Ok(0) => true,
        Ok(_) => false,
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => false,
        Err(_) => true,
    };
    let _ = tcp.set_nonblocking(false);
    gone
}

/// The result shape of a subscription, for Describe: the inner query's
/// columns behind `commit_seq` and `change`, with no rows.
pub(crate) fn describe_subscription(
    server: &Arc<PgWireServer>,
    query: &str,
    session_state: &SqlSessionGucState,
    security_context: Option<&SecurityContext>,
) -> Result<SqlResult> {
    let template = execute_server_sql_for_describe_with_session(
        server,
        query,
        session_state,
        security_context,
    )?;
    Ok(batch(&template, Vec::new()))
}

// ------------------------------------------------------------------ state

/// Runs SQL for a subscription: as the connection, or as a saved session.
/// The flag asks for the result-row limit to be enforced.
type Evaluator<'a> = &'a mut dyn FnMut(&str, bool) -> Result<SqlResult>;

/// What a subscription remembers between evaluations.
enum Memory {
    Full {
        counts: RowCounts,
    },
    Filter {
        /// Visible row per record id.
        rows: BTreeMap<String, Vec<SqlValue>>,
    },
    Grouped {
        /// Group key per record id of the rows that pass the filter.
        group_of: BTreeMap<String, String>,
        /// Visible result rows per group key.
        groups: BTreeMap<String, RowCounts>,
    },
}

/// Every visible row the memory holds, as a multiset.
fn all_counts(memory: &Memory) -> RowCounts {
    match memory {
        Memory::Full { counts } => counts.clone(),
        Memory::Filter { rows } => row_counts(rows.values()),
        Memory::Grouped { groups, .. } => {
            let mut all = RowCounts::new();
            for counts in groups.values() {
                for (key, (count, row)) in counts {
                    all.entry(key.clone()).or_insert_with(|| (0, row.clone())).0 += count;
                }
            }
            all
        }
    }
}

struct Subscription {
    /// The query as written.
    query: String,
    plan: Plan,
    memory: Memory,
    /// Columns and types the subscriber sees.
    template: SqlResult,
    /// Watched collections and their change counters at the last round.
    watched: Option<Vec<String>>,
    generations: Vec<u64>,
}

/// One round's outcome.
struct Round {
    rows: Vec<Vec<SqlValue>>,
    /// Whether the round re-read the whole query.
    full: bool,
}

impl Subscription {
    fn memory_estimate(&self) -> usize {
        fn row_bytes(row: &[SqlValue]) -> usize {
            row.iter()
                .map(|value| value.to_cell().len() + std::mem::size_of::<SqlValue>())
                .sum::<usize>()
                + 64
        }
        fn counts_bytes(counts: &RowCounts) -> usize {
            counts
                .iter()
                .map(|(key, (_, row))| key.len() + row_bytes(row) + 64)
                .sum()
        }
        let bytes = match &self.memory {
            Memory::Full { counts } => counts_bytes(counts),
            Memory::Filter { rows } => rows
                .iter()
                .map(|(key, row)| key.len() + row_bytes(row) + 64)
                .sum(),
            Memory::Grouped { group_of, groups } => {
                group_of
                    .iter()
                    .map(|(id, group)| id.len() + group.len() + 64)
                    .sum::<usize>()
                    + groups
                        .iter()
                        .map(|(key, counts)| key.len() + counts_bytes(counts) + 64)
                        .sum::<usize>()
            }
        };
        bytes + self.query.len() + self.template.memory_estimate()
    }
    fn new(db: &BicDb, query: &str) -> Self {
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, query).ok();
        let (watched, plan) = match &statements {
            Some(statements) => (watched_collections(db, statements), plan(db, statements)),
            None => (None, Plan::Full),
        };
        let mut subscription = Self {
            query: query.to_string(),
            plan,
            memory: Memory::Full {
                counts: RowCounts::new(),
            },
            template: SqlResult::new(Vec::new(), Vec::new()),
            watched,
            generations: Vec::new(),
        };
        subscription.generations = subscription.generations(db);
        subscription
    }

    fn generations(&self, db: &BicDb) -> Vec<u64> {
        self.watched
            .as_ref()
            .map(|watched| {
                watched
                    .iter()
                    .map(|collection| db.collection_generation(collection))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Re-reads the whole query and adopts the result; returns the changes.
    fn whole_round(&mut self, eval: Evaluator<'_>, commit_seq: u64) -> Result<Round> {
        let (sql, hidden) = match &self.plan {
            Plan::Full => (self.query.clone(), 0),
            Plan::Filter(keyed) | Plan::Grouped(keyed) => (keyed.whole_sql(), 1),
        };
        let split = split_hidden(eval(&sql, true)?, hidden);
        let previous = all_counts(&self.memory);
        let current = row_counts(split.visible.rows.iter());
        let rows = changes(&previous, &current, commit_seq);
        self.template = SqlResult::new(split.visible.columns.clone(), Vec::new())
            .with_column_types(split.visible.column_types.clone());
        let split_columns = (
            split.visible.columns.clone(),
            split.visible.column_types.clone(),
        );
        let mut learned_group_type = None;
        let mut too_many_members = false;
        self.memory = match &self.plan {
            Plan::Full => Memory::Full { counts: current },
            Plan::Filter(_) => Memory::Filter {
                rows: split
                    .hidden
                    .into_iter()
                    .zip(split.visible.rows)
                    .map(|(hidden, row)| (hidden.into_iter().next().unwrap_or_default(), row))
                    .collect(),
            },
            Plan::Grouped(keyed) => {
                let mut groups: BTreeMap<String, RowCounts> = BTreeMap::new();
                for (hidden, row) in split.hidden.into_iter().zip(split.visible.rows.iter()) {
                    let group = hidden.into_iter().next().unwrap_or_default();
                    count_row(groups.entry(group).or_default(), row);
                }
                // Which filtered rows make up each group, and the group
                // column's type (for the literals that address groups).
                let membership = eval(&keyed.membership_sql(None), false)?;
                if membership.rows.len() > MAX_MEMBERSHIP_ROWS {
                    too_many_members = true;
                    Memory::Full {
                        counts: current.clone(),
                    }
                } else {
                    learned_group_type = membership.column_types.get(1).cloned().flatten();
                    let membership = split_hidden(membership, 2);
                    let mut group_of = BTreeMap::new();
                    for hidden in membership.hidden {
                        if let [id, group] = hidden.as_slice() {
                            group_of.insert(id.clone(), group.clone());
                        }
                    }
                    Memory::Grouped { group_of, groups }
                }
            }
        };
        if too_many_members {
            if let Plan::Grouped(keyed) = &self.plan {
                self.query = keyed.original_sql();
            }
            self.plan = Plan::Full;
            self.template =
                SqlResult::new(split_columns.0, Vec::new()).with_column_types(split_columns.1);
        } else if let (Plan::Grouped(keyed), Some(group_type)) =
            (&mut self.plan, learned_group_type)
        {
            if let Some((_, pg_type)) = &mut keyed.group {
                *pg_type = Some(group_type);
            }
        }
        Ok(Round { rows, full: true })
    }

    /// Falls back to re-running the whole query from now on.
    fn demote(&mut self, why: &str) {
        eprintln!("bicdb live query: {why}; re-running the whole query from now on");
        if let Plan::Filter(keyed) | Plan::Grouped(keyed) = &self.plan {
            self.query = keyed.original_sql();
        }
        self.plan = Plan::Full;
        self.memory = Memory::Full {
            counts: RowCounts::new(),
        };
    }

    /// Re-evaluates for the rows written since `previous_generations`.
    fn change_round(
        &mut self,
        db_changes: &dyn Fn(&str, u64) -> Option<Vec<bicdb_core::ChangedRecord>>,
        eval: Evaluator<'_>,
        previous_generations: &[u64],
        commit_seq: u64,
    ) -> Result<Round> {
        let keyed = match &self.plan {
            Plan::Full => return self.whole_round(eval, commit_seq),
            Plan::Filter(keyed) | Plan::Grouped(keyed) => keyed.clone(),
        };
        let changed = self
            .watched
            .as_ref()
            .and_then(|watched| watched.iter().position(|name| *name == keyed.collection))
            .and_then(|index| previous_generations.get(index))
            .and_then(|generation| db_changes(&keyed.collection, *generation));
        let changed = match changed {
            Some(changed) if changed.len() <= MAX_KEYED_ROWS => changed,
            _ => return self.whole_round(eval, commit_seq),
        };
        let mut touched = BTreeSet::new();
        let mut deleted = BTreeSet::new();
        for change in &changed {
            if change.deleted {
                deleted.insert(change.record_id.clone());
            } else {
                deleted.remove(&change.record_id);
            }
            touched.insert(change.record_id.clone());
        }
        let to_read: BTreeSet<String> = touched.difference(&deleted).cloned().collect();
        match &mut self.memory {
            Memory::Filter { rows } => {
                let mut before = RowCounts::new();
                let mut after = RowCounts::new();
                for id in &touched {
                    if let Some(row) = rows.remove(id) {
                        count_row(&mut before, &row);
                    }
                }
                for id in &to_read {
                    let one = BTreeSet::from([id.clone()]);
                    let split = split_hidden(eval(&keyed.rows_sql(&one), true)?, 1);
                    for (hidden, row) in split.hidden.into_iter().zip(split.visible.rows) {
                        let id = hidden.into_iter().next().unwrap_or_default();
                        if !to_read.contains(&id) {
                            // The key column did not come back as itself
                            // (masked or transformed): rows cannot be
                            // addressed by key.
                            self.demote("the key column is not readable as itself");
                            return self.whole_round(eval, commit_seq);
                        }
                        count_row(&mut after, &row);
                        rows.insert(id, row);
                    }
                }
                Ok(Round {
                    rows: changes(&before, &after, commit_seq),
                    full: false,
                })
            }
            Memory::Grouped { group_of, groups } => {
                let mut affected = BTreeSet::new();
                for id in &touched {
                    if let Some(group) = group_of.remove(id) {
                        affected.insert(group);
                    }
                }
                for id in &to_read {
                    let one = BTreeSet::from([id.clone()]);
                    let membership =
                        split_hidden(eval(&keyed.membership_sql(Some(&one)), true)?, 2);
                    for hidden in membership.hidden {
                        let [id, group] = hidden.as_slice() else {
                            continue;
                        };
                        if !to_read.contains(id) {
                            self.demote("the key column is not readable as itself");
                            return self.whole_round(eval, commit_seq);
                        }
                        affected.insert(group.clone());
                        group_of.insert(id.clone(), group.clone());
                    }
                }
                if affected.is_empty() {
                    return Ok(Round {
                        rows: Vec::new(),
                        full: false,
                    });
                }
                let mut before = RowCounts::new();
                for group in &affected {
                    if let Some(counts) = groups.remove(group) {
                        for (key, (count, row)) in counts {
                            before.entry(key).or_insert_with(|| (0, row)).0 += count;
                        }
                    }
                }
                let split = split_hidden(eval(&keyed.groups_sql(&affected), true)?, 1);
                let mut after = RowCounts::new();
                for (hidden, row) in split.hidden.into_iter().zip(split.visible.rows) {
                    let group = hidden.into_iter().next().unwrap_or_default();
                    count_row(&mut after, &row);
                    count_row(groups.entry(group).or_default(), &row);
                }
                Ok(Round {
                    rows: changes(&before, &after, commit_seq),
                    full: false,
                })
            }
            Memory::Full { .. } => self.whole_round(eval, commit_seq),
        }
    }

    /// Whether any watched table changed; updates the remembered counters
    /// and returns the previous ones when so.
    fn take_changed_generations(&mut self, db: &BicDb) -> Option<Vec<u64>> {
        let now = self.generations(db);
        if self.watched.is_some() && now == self.generations {
            return None;
        }
        Some(std::mem::replace(&mut self.generations, now))
    }
}

/// An evaluator running statements as the connection itself.
fn connection_evaluator<'a>(
    server: &'a Arc<PgWireServer>,
    state: &'a mut ConnectionState,
) -> impl FnMut(&str, bool) -> Result<SqlResult> + 'a {
    move |sql: &str, enforce_limit: bool| {
        let timeout =
            Duration::from_millis(server.config.workload.live_evaluation_timeout_ms.max(1));
        let _deadline =
            bicdb_core::query_budget::QueryMemoryScope::with_deadline(0, Instant::now() + timeout);
        execute_server_sql_with_options(server, state, sql, enforce_limit)
    }
}

fn check_fanout(server: &PgWireServer, rows: usize) -> Result<()> {
    let limit = server.config.workload.live_max_changes_per_round;
    if limit > 0 && rows > limit {
        return Err(PgWireError::QueryRejected(format!(
            "live query produced {rows} changes; budget is {limit}"
        )));
    }
    Ok(())
}

// ------------------------------------------------------------------ registry

/// `SUBSCRIBE <name> AS <query>`: a subscription that lives on the
/// connection and delivers its changes as notifications on channel `name`,
/// so one connection carries many subscriptions and none holds a thread.
struct Registered {
    cache_reservation: workload::CacheReservation,
    user: String,
    connection_id: u64,
    name: String,
    session_state: SqlSessionGucState,
    security_context: Option<SecurityContext>,
    subscription: Subscription,
}

/// The registered subscriptions of a server, evaluated by one dispatcher.
#[derive(Default)]
pub(crate) struct LiveRegistry {
    entries: Mutex<Vec<Registered>>,
}

impl std::fmt::Debug for LiveRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LiveRegistry")
            .field("subscriptions", &self.len())
            .finish()
    }
}

impl LiveRegistry {
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    /// Drops every subscription of a connection that closed.
    pub(crate) fn remove_connection(&self, connection_id: u64) {
        self.entries
            .lock()
            .unwrap()
            .retain(|entry| entry.connection_id != connection_id);
    }

    fn remove(&self, connection_id: u64, name: Option<&str>) -> usize {
        let mut entries = self.entries.lock().unwrap();
        let before = entries.len();
        entries.retain(|entry| {
            entry.connection_id != connection_id || name.is_some_and(|name| entry.name != name)
        });
        before - entries.len()
    }
}

/// A `SUBSCRIBE` statement: streaming (`SUBSCRIBE <query>`) or registered
/// (`SUBSCRIBE <name> AS <query>`).
pub(crate) struct SubscribeStatement<'a> {
    pub(crate) name: Option<String>,
    pub(crate) query: &'a str,
}

/// Parses a `SUBSCRIBE` statement, if `sql` is one.
pub(crate) fn parse_subscribe(sql: &str) -> Option<SubscribeStatement<'_>> {
    let rest = subscribed_query(sql)?;
    // `SUBSCRIBE name AS SELECT ...`: an identifier, then AS.
    let mut words = rest.splitn(3, char::is_whitespace);
    let first = words.next()?;
    let valid_name = !first.is_empty()
        && first.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !first.chars().next().is_some_and(|c| c.is_ascii_digit());
    if valid_name && !first.eq_ignore_ascii_case("select") && !first.eq_ignore_ascii_case("with") {
        if let Some(keyword) = words.next() {
            if keyword.eq_ignore_ascii_case("as") {
                let query = rest[first.len()..].trim_start()[keyword.len()..].trim_start();
                return Some(SubscribeStatement {
                    name: Some(first.to_ascii_lowercase()),
                    query,
                });
            }
        }
        return None;
    }
    Some(SubscribeStatement {
        name: None,
        query: rest,
    })
}

/// Parses `UNSUBSCRIBE <name>` / `UNSUBSCRIBE ALL`: `Some(None)` for ALL.
pub(crate) fn parse_unsubscribe(sql: &str) -> Option<Option<String>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let keyword = trimmed.get(..11)?;
    if !keyword.eq_ignore_ascii_case("unsubscribe") {
        return None;
    }
    let rest = trimmed[11..].trim();
    if rest.is_empty() || !trimmed[11..].starts_with(char::is_whitespace) {
        return None;
    }
    if rest.eq_ignore_ascii_case("all") {
        return Some(None);
    }
    Some(Some(rest.to_ascii_lowercase()))
}

/// Registers `SUBSCRIBE name AS query` on the connection: returns the initial
/// result (as the streaming form would) and from then on delivers changes as
/// notifications on channel `name`, until `UNSUBSCRIBE` or disconnect. The
/// connection listens on the channel.
pub(crate) fn register_subscription(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    name: &str,
    query: &str,
) -> Result<SqlResult> {
    if state.in_transaction {
        return Err(PgWireError::Server(
            "SUBSCRIBE cannot run inside a transaction block".to_string(),
        ));
    }
    let normalized = normalize_executable_sql(query);
    if !(normalized.starts_with("select ") || normalized.starts_with("with ")) {
        return Err(PgWireError::Server(
            "SUBSCRIBE takes a SELECT query".to_string(),
        ));
    }
    let limit = server.config.max_live_subscriptions_per_connection;
    let existing = server
        .live_registry
        .entries
        .lock()
        .unwrap()
        .iter()
        .filter(|entry| entry.connection_id == state.connection_id)
        .count();
    if limit > 0 && existing >= limit {
        return Err(PgWireError::Server(format!(
            "this connection already has {existing} subscriptions (the limit); UNSUBSCRIBE one first"
        )));
    }
    // A name already in use on this connection is replaced.
    server.live_registry.remove(state.connection_id, Some(name));
    let mut subscription = {
        let db = server.read_db()?;
        Subscription::new(&db, query)
    };
    let commit_seq = server.replication_feed.durable_seq();
    let initial = {
        let mut eval = connection_evaluator(server, state);
        subscription.whole_round(&mut eval, commit_seq)?
    };
    check_fanout(server, initial.rows.len())?;
    let cache_reservation = workload::CacheReservation::new(
        server,
        &state.user,
        state.security_context.as_ref(),
        subscription.memory_estimate(),
    )?;
    let result = batch(&subscription.template, initial.rows);
    server
        .live_registry
        .entries
        .lock()
        .unwrap()
        .push(Registered {
            cache_reservation,
            user: state.user.clone(),
            connection_id: state.connection_id,
            name: name.to_string(),
            session_state: state.session_state.clone(),
            security_context: state.security_context.clone(),
            subscription,
        });
    let mut result = result;
    result.command_tag = Some(format!("SUBSCRIBE {}", result.rows.len()));
    Ok(result)
}

/// `UNSUBSCRIBE name` / `UNSUBSCRIBE ALL` on the connection.
pub(crate) fn unsubscribe(
    server: &Arc<PgWireServer>,
    state: &ConnectionState,
    name: Option<&str>,
) -> Result<SqlResult> {
    let removed = server.live_registry.remove(state.connection_id, name);
    if removed == 0 {
        if let Some(name) = name {
            return Err(PgWireError::Server(format!(
                "no subscription named {name} on this connection"
            )));
        }
    }
    Ok(SqlResult::command(&format!("UNSUBSCRIBE {removed}")))
}

/// The notification payload of one change row.
fn change_payload(name: &str, template: &SqlResult, row: &[SqlValue]) -> String {
    let mut object = serde_json::Map::new();
    object.insert(
        "subscription".into(),
        serde_json::Value::String(name.to_string()),
    );
    if let Some(SqlValue::Int(seq)) = row.first() {
        object.insert("commit_seq".into(), serde_json::Value::from(*seq));
    }
    if let Some(change) = row.get(1) {
        object.insert("change".into(), serde_json::Value::String(change.to_cell()));
    }
    let mut columns = serde_json::Map::new();
    for (column, value) in template.columns.iter().zip(row.iter().skip(2)) {
        let json = if matches!(value, SqlValue::Null) {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(value.to_cell())
        };
        columns.insert(column.clone(), json);
    }
    object.insert("row".into(), serde_json::Value::Object(columns));
    serde_json::Value::Object(object).to_string()
}

/// Starts the dispatcher that evaluates registered subscriptions after each
/// commit and queues their changes as notifications.
pub(crate) fn start_dispatcher(server: Arc<PgWireServer>) -> Result<()> {
    // The thread must not keep the server (and the database) alive after
    // shutdown: it waits on its own feed handle and borrows the server only
    // while it works.
    let feed = server.replication_feed.clone();
    let server = Arc::downgrade(&server);
    std::thread::Builder::new()
        .name("bicdb-live-dispatcher".to_string())
        .spawn(move || {
            let mut last_seq = feed.durable_seq();
            loop {
                let durable = feed.wait_beyond(last_seq, POLL);
                let Some(server) = server.upgrade() else {
                    return;
                };
                if server.is_shutdown_requested() {
                    return;
                }
                if durable <= last_seq {
                    continue;
                }
                last_seq = durable;
                if server.live_registry.len() == 0 {
                    continue;
                }
                let db_changes = |collection: &str, generation: u64| {
                    server.read_db().ok().and_then(|db| db.collection_changes_since(collection, generation))
                };
                let mut entries = server.live_registry.entries.lock().unwrap();
                let mut retry_needed = false;
                let mut failed = HashSet::new();
                let mut failed_connections = HashSet::new();
                for (index, entry) in entries.iter_mut().enumerate() {
                    if failed_connections.contains(&entry.connection_id) { continue; }
                    let result: Result<Option<Round>> = (|| {
                        let role_permit = workload::admit(&server, &entry.user, entry.security_context.as_ref(), &entry.subscription.query)?;
                        // Background subscriptions never wait in the foreground
                        // queue while holding the subscription registry.
                        let Some(_permit) = server.try_acquire_query_now(QueryKind::Read, Instant::now())? else { retry_needed = true; return Ok(None); };
                        let previous = {
                            let db = server.read_db()?;
                            entry.subscription.take_changed_generations(&db)
                        };
                        let Some(previous) = previous else { return Ok(None); };
                        let configured = Duration::from_millis(server.config.workload.live_evaluation_timeout_ms.max(1));
                        let timeout = if role_permit.timeout.is_zero() { configured } else { configured.min(role_permit.timeout) };
                        let cancellation = CancellationToken::new(Arc::new(AtomicBool::new(false)), Some(Instant::now() + timeout));
                        let memory = bicdb_core::query_budget::QueryMemoryScope::new(role_permit.memory_bytes);
                        let mut eval = |sql: &str, _enforce: bool| {
                            cancellation.check()?;
                            execute_server_db_sql(&server, sql, &cancellation, &entry.session_state, entry.security_context.as_ref())
                        };
                        let round = entry.subscription.change_round(&db_changes, &mut eval, &previous, durable)?;
                        cancellation.check()?;
                        role_permit.observe(memory.peak_bytes());
                        check_fanout(&server, round.rows.len())?;
                        drop(memory);
                        drop(role_permit);
                        entry.cache_reservation.resize(entry.subscription.memory_estimate())?;
                        Ok(Some(round))
                    })();
                    match result {
                        Ok(Some(round)) => {
                            if round.full { server.live_full_runs.fetch_add(1, Ordering::Relaxed); }
                            else { server.live_incremental_runs.fetch_add(1, Ordering::Relaxed); }
                            let payloads = round.rows.iter().map(|row| change_payload(&entry.name, &entry.subscription.template, row)).collect();
                            if let Ok(mut bus) = server.notifications.lock() {
                                if !bus.notify_private_batch(entry.connection_id, &entry.name, payloads) {
                                    server.live_breaker_trips.fetch_add(1, Ordering::Relaxed);
                                    bus.fail_private_subscriptions(entry.connection_id, &entry.name, "live query backlog exceeded; resubscribe all");
                                    failed_connections.insert(entry.connection_id);
                                }
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            server.live_breaker_trips.fetch_add(1, Ordering::Relaxed);
                            if let Ok(mut bus) = server.notifications.lock() {
                                let payload = json!({"subscription": entry.name, "error": error.to_string(), "resubscribe": true}).to_string();
                                if !bus.notify_private_batch(entry.connection_id, &entry.name, vec![payload]) {
                                    bus.fail_private_subscriptions(entry.connection_id, &entry.name, "live query backlog exceeded; resubscribe all");
                                    failed_connections.insert(entry.connection_id);
                                }
                            }
                            failed.insert(index);
                        }
                    }
                }
                let mut index = 0;
                entries.retain(|entry| {
                    let keep = !failed.contains(&index) && !failed_connections.contains(&entry.connection_id);
                    index += 1;
                    keep
                });
                drop(entries);
                if retry_needed {
                    last_seq = last_seq.saturating_sub(1);
                    std::thread::sleep(POLL);
                }
            }
        })?;
    Ok(())
}

// ------------------------------------------------------------------ driver

/// Runs a streaming subscription until the client cancels or disconnects.
/// With `copy`, rows stream as COPY data in that format. In the extended
/// protocol the row description was sent by Describe (`describe_rows`
/// false) and `formats` are the portal's result formats.
pub(crate) fn run_subscription(
    stream: &mut ClientStream,
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    query: &str,
    copy: Option<CopyFormat>,
    describe_rows: bool,
    formats: &[i16],
) -> Result<()> {
    if state.in_transaction {
        return Err(PgWireError::Server(
            "SUBSCRIBE cannot run inside a transaction block".to_string(),
        ));
    }
    let normalized = normalize_executable_sql(query);
    if !(normalized.starts_with("select ") || normalized.starts_with("with ")) {
        return Err(PgWireError::Server(
            "SUBSCRIBE takes a SELECT query".to_string(),
        ));
    }
    let mut subscription = {
        let db = server.read_db()?;
        Subscription::new(&db, query)
    };
    let mut last_seq = server.replication_feed.durable_seq();
    let initial = {
        let mut eval = connection_evaluator(server, state);
        subscription.whole_round(&mut eval, last_seq)?
    };
    check_fanout(server, initial.rows.len())?;
    let first = batch(&subscription.template, initial.rows);
    let column_types = {
        let db = server.read_db()?;
        result_column_types_with_db(&db, &first, None)?
    };
    let mut rows_streamed = 0u64;
    let mut write = |stream: &mut ClientStream, rows: &SqlResult, first: bool| -> Result<()> {
        match copy {
            Some(format) => {
                if first {
                    copy_out_response(stream, rows)?;
                }
                let db = server.read_db()?;
                for row in &rows.rows {
                    let payload = render_copy_row(&db, row, &column_types, format)?;
                    copy_data(stream, &payload)?;
                }
            }
            None => {
                write_query_result(
                    stream,
                    server,
                    rows,
                    formats,
                    None,
                    first && describe_rows,
                    false,
                )?;
            }
        }
        rows_streamed += rows.rows.len() as u64;
        stream.flush()?;
        Ok(())
    };
    write(stream, &first, true)?;
    server.live_subscriptions.fetch_add(1, Ordering::SeqCst);
    struct Active<'a>(&'a PgWireServer);
    impl Drop for Active<'_> {
        fn drop(&mut self) {
            self.0.live_subscriptions.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let _active = Active(server);
    let cancellation = CancellationToken::new(state.cancel_token.clone(), None);
    loop {
        if let Err(error) = cancellation.check() {
            return Err(classify_bicdb_interrupt(server, state, error));
        }
        if server.is_shutdown_requested() || client_gone(stream) {
            break;
        }
        let durable = server.replication_feed.wait_beyond(last_seq, POLL);
        if durable <= last_seq {
            continue;
        }
        last_seq = durable;
        let previous = {
            let db = server.read_db()?;
            subscription.take_changed_generations(&db)
        };
        let Some(previous) = previous else {
            continue;
        };
        let round = {
            let db_changes = |collection: &str, generation: u64| {
                server
                    .read_db()
                    .ok()
                    .and_then(|db| db.collection_changes_since(collection, generation))
            };
            let mut eval = connection_evaluator(server, state);
            subscription.change_round(&db_changes, &mut eval, &previous, last_seq)?
        };
        check_fanout(server, round.rows.len())?;
        if round.full {
            server.live_full_runs.fetch_add(1, Ordering::Relaxed);
        } else {
            server.live_incremental_runs.fetch_add(1, Ordering::Relaxed);
        }
        if round.rows.is_empty() {
            continue;
        }
        write(stream, &batch(&subscription.template, round.rows), false)?;
    }
    if copy.is_some() {
        command_complete(stream, &format!("COPY {rows_streamed}"))?;
    } else {
        command_complete(stream, &format!("SUBSCRIBE {rows_streamed}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(sql: &str) -> Statement {
        Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .remove(0)
    }

    #[test]
    fn recognizes_subscribe_statements() {
        assert_eq!(subscribed_query("SUBSCRIBE SELECT 1;"), Some("SELECT 1"));
        assert_eq!(
            subscribed_query("  subscribe\nselect * from t"),
            Some("select * from t")
        );
        assert_eq!(subscribed_query("SUBSCRIBED SELECT 1"), None);
        assert_eq!(subscribed_query("SELECT 1"), None);
    }

    #[test]
    fn diffs_are_multisets_with_removals_first() {
        let row = |a: i64, b: &str| vec![SqlValue::Int(a), SqlValue::String(b.to_string())];
        let before_rows = vec![row(1, "x"), row(1, "x"), row(2, "y")];
        let after_rows = vec![row(1, "x"), row(3, "z")];
        let diff = changes(
            &row_counts(before_rows.iter()),
            &row_counts(after_rows.iter()),
            7,
        );
        let rendered = diff
            .iter()
            .map(|values| {
                values
                    .iter()
                    .map(SqlValue::to_cell)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect::<Vec<_>>();
        assert_eq!(rendered, vec!["7,-,1,x", "7,-,2,y", "7,+,3,z"]);
    }

    #[test]
    fn keyed_queries_add_the_restriction_and_hidden_columns() {
        let mut statement =
            parsed("SELECT p.id, p.body FROM posts p WHERE p.author = 'bob' ORDER BY p.id");
        if let Statement::Query(query) = &mut statement {
            if let SetExpr::Select(select) = query.body.as_mut() {
                select.projection.push(hidden_item("p", "id", RID_COLUMN));
            }
        }
        let keyed = Keyed {
            collection: "posts".into(),
            statement,
            qualifier: "p".into(),
            key_column: "id".into(),
            key_type: "int4".into(),
            group: None,
        };
        let keys = ["3".to_string(), "10".to_string()].into_iter().collect();
        assert_eq!(
            keyed.rows_sql(&keys),
            "SELECT p.id, p.body, p.id AS __bicdb_rid FROM posts p \
             WHERE (p.author = 'bob') AND p.id IN (10, 3) ORDER BY p.id"
        );
        let one = ["3".to_string()].into_iter().collect();
        assert!(keyed
            .rows_sql(&one)
            .contains("WHERE (p.author = 'bob') AND p.id = 3 ORDER BY"));
        assert_eq!(
            keyed.original_sql(),
            "SELECT p.id, p.body FROM posts p WHERE p.author = 'bob' ORDER BY p.id"
        );
        let text_keys = ["a'b".to_string()].into_iter().collect();
        let text = Keyed {
            key_type: "text".into(),
            ..keyed.clone()
        };
        assert!(text.rows_sql(&text_keys).contains("p.id = 'a''b'"));

        let mut grouped_statement = parsed(
            "SELECT author, count(*) FROM posts WHERE body <> '' GROUP BY author HAVING count(*) > 1",
        );
        if let Statement::Query(query) = &mut grouped_statement {
            if let SetExpr::Select(select) = query.body.as_mut() {
                select
                    .projection
                    .push(hidden_item("posts", "author", GROUP_COLUMN));
            }
        }
        let grouped = Keyed {
            collection: "posts".into(),
            statement: grouped_statement,
            qualifier: "posts".into(),
            key_column: "id".into(),
            key_type: "int4".into(),
            group: Some(("author".into(), Some("text".into()))),
        };
        let groups = ["bob".to_string()].into_iter().collect();
        assert_eq!(
            grouped.groups_sql(&groups),
            "SELECT author, count(*), posts.author AS __bicdb_group FROM posts \
             WHERE (body <> '') AND posts.author = 'bob' GROUP BY author HAVING count(*) > 1"
        );
        let ids = ["7".to_string()].into_iter().collect();
        assert_eq!(
            grouped.membership_sql(Some(&ids)),
            "SELECT posts.id AS __bicdb_rid, posts.author AS __bicdb_group FROM posts \
             WHERE (body <> '') AND posts.id = 7"
        );
    }

    #[test]
    fn registered_and_streaming_forms_are_told_apart() {
        let streaming = parse_subscribe("SUBSCRIBE SELECT 1").unwrap();
        assert_eq!((streaming.name, streaming.query), (None, "SELECT 1"));
        let registered = parse_subscribe("SUBSCRIBE Feed_42 AS\n SELECT * FROM posts").unwrap();
        assert_eq!(
            (registered.name.as_deref(), registered.query),
            (Some("feed_42"), "SELECT * FROM posts")
        );
        assert!(parse_subscribe("SUBSCRIBE feed SELECT 1").is_none());
        assert_eq!(
            parse_unsubscribe("UNSUBSCRIBE feed_42;"),
            Some(Some("feed_42".into()))
        );
        assert_eq!(parse_unsubscribe("unsubscribe all"), Some(None));
        assert_eq!(parse_unsubscribe("UNSUBSCRIBE"), None);
        assert_eq!(parse_unsubscribe("UNSUBSCRIBED x"), None);
    }

    #[test]
    fn literals_follow_the_key_type() {
        assert_eq!(literal("42", "int8").to_string(), "42");
        assert_eq!(literal("42", "text").to_string(), "'42'");
        assert_eq!(literal("x", "int4").to_string(), "'x'");
    }

    #[test]
    fn shapes_that_cannot_be_keyed_are_recognized() {
        let (aggregate, complex) = expression_shape(&parsed(
            "SELECT count(*) FROM t WHERE id IN (SELECT id FROM u)",
        ));
        assert!(aggregate && complex);
        let (aggregate, complex) =
            expression_shape(&parsed("SELECT row_number() OVER (ORDER BY id) FROM t"));
        assert!(!aggregate && complex);
        let (aggregate, complex) = expression_shape(&parsed("SELECT upper(name) FROM t"));
        assert!(!aggregate && !complex);
    }
}
