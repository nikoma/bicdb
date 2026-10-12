//! Live queries: `SUBSCRIBE <select>` streams a query's result and then every
//! change to it, for as long as the client stays connected.
//!
//! Rows carry two leading columns: `commit_seq`, the commit the result
//! reflects, and `change`, `+` for a row that entered the result and `-` for
//! one that left it. The initial result arrives as `+` rows. A changed row
//! appears as its old version with `-` and its new version with `+`.
//!
//! Correctness comes from re-running the query, as the subscribing connection
//! (its permissions, row-level security and session settings), whenever a
//! commit touched a table it reads, and sending the difference to the
//! previous result. Commits that arrive during a re-run are picked up by the
//! next one, so bursts cost one re-run, not one per commit. A query over
//! views or relations that cannot be resolved to tables re-runs on every
//! commit.
//!
//! Over the PostgreSQL protocol the rows stream as `DataRow` messages of one
//! open statement (drivers read them row by row in streaming or single-row
//! mode); `COPY (SUBSCRIBE ...) TO STDOUT` streams them as COPY data, which
//! `psql` prints as they arrive. A subscription ends when the client cancels
//! the statement (`SQLSTATE 57014`) or disconnects.
use super::*;
use sqlparser::ast::{visit_relations, ObjectNamePart};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;
use std::ops::ControlFlow;

/// Longest wait for a commit before the connection is checked again.
const POLL: Duration = Duration::from_millis(250);

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

/// Tables a query reads, as collection names, or `None` when a relation
/// could not be resolved (a view, a CTE name, another schema): the
/// subscription then re-runs on every commit.
fn watched_collections(db: &BicDb, query: &str) -> Option<Vec<String>> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, query).ok()?;
    let mut names = Vec::new();
    let mut ctes = Vec::new();
    for statement in &statements {
        if let sqlparser::ast::Statement::Query(query) = statement {
            if let Some(with) = &query.with {
                ctes.extend(
                    with.cte_tables
                        .iter()
                        .map(|cte| cte.alias.name.value.to_ascii_lowercase()),
                );
            }
        }
    }
    let flow: ControlFlow<()> = visit_relations(&statements, |relation| {
        let parts = relation
            .0
            .iter()
            .map(|part| match part {
                ObjectNamePart::Identifier(ident) => Some(ident.value.to_ascii_lowercase()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        names.push(parts);
        ControlFlow::Continue(())
    });
    let _ = flow;
    let existing = db
        .collections()
        .into_iter()
        .map(|meta| meta.name)
        .collect::<std::collections::HashSet<_>>();
    let mut watched = Vec::new();
    for parts in names {
        let parts = parts?;
        let name = match parts.as_slice() {
            [name] => name.clone(),
            [schema, name] if schema == "public" => name.clone(),
            _ => return None,
        };
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

fn generations(db: &BicDb, watched: &[String]) -> Vec<u64> {
    watched
        .iter()
        .map(|collection| db.collection_generation(collection))
        .collect()
}

/// The multiset of a result's rows, by their text cells.
fn row_counts(result: &SqlResult) -> BTreeMap<Vec<String>, (usize, Vec<SqlValue>)> {
    let mut counts: BTreeMap<Vec<String>, (usize, Vec<SqlValue>)> = BTreeMap::new();
    for row in &result.rows {
        let key = row.iter().map(SqlValue::to_cell).collect::<Vec<_>>();
        counts.entry(key).or_insert_with(|| (0, row.clone())).0 += 1;
    }
    counts
}

/// Rows that left (`-`) and entered (`+`) the result between two runs.
fn changes(
    previous: &BTreeMap<Vec<String>, (usize, Vec<SqlValue>)>,
    current: &BTreeMap<Vec<String>, (usize, Vec<SqlValue>)>,
    commit_seq: u64,
) -> Vec<Vec<SqlValue>> {
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

/// Runs a subscription until the client cancels, speaks or disconnects.
/// With `copy`, rows stream as COPY data in that format.
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

/// Runs a subscription until the client cancels, speaks or disconnects.
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
    let watched = {
        let db = server.read_db()?;
        watched_collections(&db, query)
    };
    let feed_seq = server.replication_feed.durable_seq();
    let mut result = execute_server_sql(server, state, query)?;
    let mut counts = row_counts(&result);
    let mut last_seq = feed_seq.max(server.replication_feed.durable_seq());
    let mut known = match &watched {
        Some(watched) => {
            let db = server.read_db()?;
            Some(generations(&db, watched))
        }
        None => None,
    };
    let initial = changes(&BTreeMap::new(), &counts, last_seq);
    let first = batch(&result, initial);
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
        if let (Some(watched), Some(known_generations)) = (&watched, &mut known) {
            let now = {
                let db = server.read_db()?;
                generations(&db, watched)
            };
            if now == *known_generations {
                continue;
            }
            *known_generations = now;
        }
        result = execute_server_sql(server, state, query)?;
        let current = row_counts(&result);
        let diff = changes(&counts, &current, last_seq);
        counts = current;
        if diff.is_empty() {
            continue;
        }
        write(stream, &batch(&result, diff), false)?;
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
        let before = row_counts(&SqlResult::new(
            vec!["a".into(), "b".into()],
            vec![row(1, "x"), row(1, "x"), row(2, "y")],
        ));
        let after = row_counts(&SqlResult::new(
            vec!["a".into(), "b".into()],
            vec![row(1, "x"), row(3, "z")],
        ));
        let diff = changes(&before, &after, 7);
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
}
