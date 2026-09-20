//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn parse_advisory_lock_call(sql: &str) -> Result<Option<AdvisoryLockCall>> {
    parse_advisory_lock_call_with(sql, advisory_integer_expr)
}

pub(crate) fn parse_advisory_lock_call_with(
    sql: &str,
    mut evaluate: impl FnMut(&Expr) -> Result<Option<i64>>,
) -> Result<Option<AdvisoryLockCall>> {
    if !contains_advisory_lock_function(sql) {
        return Ok(None);
    }
    // Only SELECT can be a direct lock call. Dollar-quoted routine bodies and
    // catalog string literals may mention a lock function without invoking it.
    // Do not feed DO or other extension statements to the generic SQL parser.
    let tokens = sqlparser::tokenizer::Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize()
        .map_err(|error| PgWireError::Sql(SqlError::InvalidSql(error.to_string())))?;
    if !matches!(tokens.iter().find(|token| !matches!(token, sqlparser::tokenizer::Token::Whitespace(_))),
        Some(sqlparser::tokenizer::Token::Word(word)) if word.keyword == sqlparser::keywords::Keyword::SELECT)
    {
        return Ok(None);
    }
    // This fast path implements a direct SELECT of one advisory-lock function.
    // A function definition can legitimately mention pg_advisory_unlock inside
    // a dollar-quoted body, and a simple-query migration can contain that
    // definition alongside statements sqlparser does not understand. Parsing
    // the entire batch here made the body text change whether earlier, valid
    // statements were accepted.
    if split_sql_statements(sql).len() != 1 {
        return Ok(None);
    }
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|error| PgWireError::Sql(SqlError::InvalidSql(error.to_string())))?;
    let [Statement::Query(query)] = statements.as_mut_slice() else {
        return Ok(None);
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Ok(None);
    };
    if !select.from.is_empty() || select.projection.len() != 1 {
        return Ok(None);
    }
    let SelectItem::UnnamedExpr(Expr::Function(function)) = &select.projection[0] else {
        return Ok(None);
    };
    let name = function
        .name
        .to_string()
        .trim_matches('"')
        .to_ascii_lowercase();
    let name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
    let operation = match name {
        "pg_advisory_lock" => AdvisoryLockOperation::Acquire {
            mode: AdvisoryLockMode::Exclusive,
            scope: AdvisoryLockScope::Session,
            try_only: false,
        },
        "pg_advisory_lock_shared" => AdvisoryLockOperation::Acquire {
            mode: AdvisoryLockMode::Shared,
            scope: AdvisoryLockScope::Session,
            try_only: false,
        },
        "pg_try_advisory_lock" => AdvisoryLockOperation::Acquire {
            mode: AdvisoryLockMode::Exclusive,
            scope: AdvisoryLockScope::Session,
            try_only: true,
        },
        "pg_try_advisory_lock_shared" => AdvisoryLockOperation::Acquire {
            mode: AdvisoryLockMode::Shared,
            scope: AdvisoryLockScope::Session,
            try_only: true,
        },
        "pg_advisory_xact_lock" => AdvisoryLockOperation::Acquire {
            mode: AdvisoryLockMode::Exclusive,
            scope: AdvisoryLockScope::Transaction,
            try_only: false,
        },
        "pg_advisory_xact_lock_shared" => AdvisoryLockOperation::Acquire {
            mode: AdvisoryLockMode::Shared,
            scope: AdvisoryLockScope::Transaction,
            try_only: false,
        },
        "pg_try_advisory_xact_lock" => AdvisoryLockOperation::Acquire {
            mode: AdvisoryLockMode::Exclusive,
            scope: AdvisoryLockScope::Transaction,
            try_only: true,
        },
        "pg_try_advisory_xact_lock_shared" => AdvisoryLockOperation::Acquire {
            mode: AdvisoryLockMode::Shared,
            scope: AdvisoryLockScope::Transaction,
            try_only: true,
        },
        "pg_advisory_unlock" => AdvisoryLockOperation::Unlock {
            mode: AdvisoryLockMode::Exclusive,
        },
        "pg_advisory_unlock_shared" => AdvisoryLockOperation::Unlock {
            mode: AdvisoryLockMode::Shared,
        },
        "pg_advisory_unlock_all" => AdvisoryLockOperation::UnlockAll,
        _ => return Ok(None),
    };
    let args = match &function.args {
        FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
                _ => Err(PgWireError::Sql(SqlError::InvalidSql(format!(
                    "{name} expects positional integer arguments"
                )))),
            })
            .collect::<Result<Vec<_>>>()?,
        _ => Vec::new(),
    };
    let key = if matches!(operation, AdvisoryLockOperation::UnlockAll) {
        if !args.is_empty() {
            return Err(SqlError::InvalidSql(format!("{name} expects no arguments")).into());
        }
        None
    } else {
        match args.as_slice() {
            [value] => evaluate(value)?.map(AdvisoryLockKey::BigInt),
            [left, right] => match (evaluate(left)?, evaluate(right)?) {
                (Some(left), Some(right)) => Some(AdvisoryLockKey::IntPair(
                    i32::try_from(left).map_err(|_| {
                        PgWireError::Sql(SqlError::InvalidSql(format!(
                            "{name} integer key is out of range"
                        )))
                    })?,
                    i32::try_from(right).map_err(|_| {
                        PgWireError::Sql(SqlError::InvalidSql(format!(
                            "{name} integer key is out of range"
                        )))
                    })?,
                )),
                _ => None,
            },
            _ => {
                return Err(SqlError::InvalidSql(format!(
                    "{name} expects one bigint or two integer arguments"
                ))
                .into());
            }
        }
    };
    Ok(Some(AdvisoryLockCall {
        operation,
        key,
        result_name: match name {
            "pg_advisory_lock" => "pg_advisory_lock",
            "pg_advisory_lock_shared" => "pg_advisory_lock_shared",
            "pg_try_advisory_lock" => "pg_try_advisory_lock",
            "pg_try_advisory_lock_shared" => "pg_try_advisory_lock_shared",
            "pg_advisory_xact_lock" => "pg_advisory_xact_lock",
            "pg_advisory_xact_lock_shared" => "pg_advisory_xact_lock_shared",
            "pg_try_advisory_xact_lock" => "pg_try_advisory_xact_lock",
            "pg_try_advisory_xact_lock_shared" => "pg_try_advisory_xact_lock_shared",
            "pg_advisory_unlock" => "pg_advisory_unlock",
            "pg_advisory_unlock_shared" => "pg_advisory_unlock_shared",
            "pg_advisory_unlock_all" => "pg_advisory_unlock_all",
            _ => unreachable!(),
        },
    }))
}

/// Whether the text holds no statement at all (only whitespace, `;` and
/// comments) — what `split_sql_statements(sql).is_empty()` answers, without
/// building the statement list. Quoted or dollar-quoted text counts as a
/// statement, so anything that is not a comment or separator is one.
pub(crate) fn sql_is_blank(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        match bytes[idx] {
            b';' => idx += 1,
            byte if byte.is_ascii_whitespace() => idx += 1,
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                idx += 2;
                while idx < bytes.len() && bytes[idx] != b'\n' {
                    idx += 1;
                }
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                idx += 2;
                while idx + 1 < bytes.len() && !(bytes[idx] == b'*' && bytes[idx + 1] == b'/') {
                    idx += 1;
                }
                idx = (idx + 2).min(bytes.len());
            }
            _ => return false,
        }
    }
    true
}

pub(crate) fn contains_advisory_lock_function(sql: &str) -> bool {
    // Both names contain "advisory"; scan for it case-insensitively before
    // paying for a lowercase copy of the whole text.
    if !contains_ascii_case_insensitive(sql, "advisory") {
        return false;
    }
    let lower = sql.to_ascii_lowercase();
    lower.contains("pg_advisory") || lower.contains("pg_try_advisory")
}

/// `haystack.to_ascii_lowercase().contains(needle)` without the copy
/// (`needle` must already be lowercase ASCII).
pub(crate) fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    let needle = needle.as_bytes();
    if needle.is_empty() {
        return true;
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

pub(crate) fn advisory_integer_expr(expr: &Expr) -> Result<Option<i64>> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Number(value, _) => value
                .parse::<i64>()
                .map(Some)
                .map_err(|_| SqlError::InvalidSql("invalid advisory lock key".to_string()).into()),
            Value::Null => Ok(None),
            _ => {
                Err(SqlError::InvalidSql("advisory lock key must be an integer".to_string()).into())
            }
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match advisory_integer_expr(expr)? {
            Some(value) => value.checked_neg().map(Some).ok_or_else(|| {
                SqlError::InvalidSql("advisory lock key is out of range".to_string()).into()
            }),
            None => Ok(None),
        },
        Expr::Nested(expr) | Expr::Cast { expr, .. } => advisory_integer_expr(expr),
        Expr::Function(function) if function.name.to_string().eq_ignore_ascii_case("hashtext") => {
            let FunctionArguments::List(list) = &function.args else {
                return Err(
                    SqlError::InvalidSql("hashtext expects one argument".to_string()).into(),
                );
            };
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value)))] =
                list.args.as_slice()
            else {
                return Err(
                    SqlError::InvalidSql("hashtext expects one text argument".to_string()).into(),
                );
            };
            let Value::SingleQuotedString(value) = &value.value else {
                return Err(
                    SqlError::InvalidSql("hashtext expects one text argument".to_string()).into(),
                );
            };
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            value.hash(&mut hasher);
            Ok(Some(i64::from(hasher.finish() as i32)))
        }
        _ => Err(SqlError::InvalidSql("advisory lock key must be an integer".to_string()).into()),
    }
}

pub(crate) fn parse_pg_sleep(sql: &str) -> Option<Duration> {
    parse_pg_sleep_normalized(&normalize_executable_sql(sql))
}

pub(crate) fn parse_pg_sleep_normalized(normalized: &str) -> Option<Duration> {
    let argument = normalized
        .strip_prefix("select pg_sleep(")?
        .strip_suffix(')')?
        .trim();
    let seconds = argument.parse::<f64>().ok()?;
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    Some(Duration::from_secs_f64(seconds))
}

/// Fire the deferred constraint triggers riding a buffered transaction, in a
/// short-lived shared session bound to it. The server commits buffered
/// transactions itself (never through `SqlSession::commit`), so without this
/// drain a DEFERRABLE INITIALLY DEFERRED trigger — the shape a ledger's
/// commit-time balance check takes — would be queued and then silently thrown
/// away at exactly the moment it was supposed to run.
pub(crate) fn drain_deferred_trigger_hooks(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    cancellation: &CancellationToken,
    tx: Transaction,
) -> Result<Transaction> {
    if !tx.has_deferred_hooks() {
        return Ok(tx);
    }
    let catalog_cache = std::mem::take(&mut state.catalog_cache);
    let (result, pending, catalog_cache) = {
        let db = server.read_db_for_transaction_progress()?;
        let mut session =
            with_connection_guc_state(sql_session_shared_for_server(server, &db), server, state)
                .with_catalog_cache(catalog_cache)
                .with_cancellation(cancellation.clone())
                .with_pending_transaction(tx);
        let result = session.fire_deferred_row_triggers();
        let pending = session.take_pending_transaction();
        let catalog_cache = session.into_catalog_cache();
        (result, pending, catalog_cache)
    };
    state.catalog_cache = catalog_cache;
    match (result, pending) {
        (Ok(()), Some(tx)) => Ok(tx),
        (Ok(()), None) => Err(PgWireError::Server(
            "deferred trigger drain lost the open transaction".to_string(),
        )),
        (Err(error), pending) => {
            if let Some(tx) = pending {
                let _ = tx.rollback();
            }
            Err(PgWireError::from(error))
        }
    }
}

pub(crate) fn commit_buffered_transaction(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    cancellation: &CancellationToken,
) -> Result<SqlResult> {
    ensure_server_writable(server)?;
    if let Err(error) = cancellation.check() {
        if let Some(tx) = state.tx.take() {
            let _ = tx.rollback();
        }
        let _ = rollback_transaction_ddl_to_len(server, state, 0);
        state.session_state.rollback_transaction();
        state.tx_shared_role_ddl.clear();
        clear_open_transaction_state(server, state);
        server
            .advisory_locks
            .release_transaction(state.connection_id);
        return Err(error.into());
    }
    if let Some(tx) = state.tx.take() {
        let started = Instant::now();
        // Deferred constraint triggers fire first, with the transaction still
        // open; a failure here fails the COMMIT and ends the transaction
        // block, exactly as a failed statement-time check would.
        let mut tx = match drain_deferred_trigger_hooks(server, state, cancellation, tx) {
            Ok(tx) => tx,
            Err(error) => {
                let _ = rollback_transaction_ddl_to_len(server, state, 0);
                state.session_state.rollback_transaction();
                state.tx_shared_role_ddl.clear();
                clear_open_transaction_state(server, state);
                server
                    .advisory_locks
                    .release_transaction(state.connection_id);
                return Err(error);
            }
        };
        tx.prepare_wal_payloads();
        let admission = server.try_admit_write()?;
        let rdb = server.read_db_for_transaction_progress()?;
        match rdb.commit_buffered_transaction(&mut tx) {
            Ok(commit_seq) => {
                drop(rdb);
                drop(admission);
                state.session_state.commit_transaction();
                let durability = server.tx_log.write_durable(commit_seq);
                let admission_finalization = durability
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| BicDbError::Cluster(error.to_string()))
                    .and_then(|_| tx.finalize_commit_admission());
                let finalization = tx.finalize_committed_memory_jobs();
                state.last_commit_seq = state.last_commit_seq.max(commit_seq);
                server.record_write_execution(duration_nanos_u64(started.elapsed()));
                state.committed_shared_role_ddl = std::mem::take(&mut state.tx_shared_role_ddl);
                clear_open_transaction_state(server, state);
                server
                    .advisory_locks
                    .release_transaction(state.connection_id);
                durability?;
                admission_finalization?;
                finalization?;
                return Ok(SqlResult::command("COMMIT"));
            }
            Err(error) => {
                // PostgreSQL ends the transaction block on COMMIT whether it
                // succeeds or fails. Keeping it open left `in_transaction`
                // true, so a driver that (correctly) sent BEGIN next was told
                // `nested transactions are not supported` — the error seen
                // right after recovery, when conflict-heavy retries make
                // commits fail.
                let _ = tx.rollback();
                let _ = rollback_transaction_ddl_to_len(server, state, 0);
                state.session_state.rollback_transaction();
                state.tx_shared_role_ddl.clear();
                clear_open_transaction_state(server, state);
                server
                    .advisory_locks
                    .release_transaction(state.connection_id);
                return Err(PgWireError::from(error));
            }
        }
    }
    let admission = server.try_admit_write()?;
    let mut db = server.write_db_with_admission(&admission)?;
    validate_buffered_transaction_generations(&db, state)?;
    let catalog_cache = std::mem::take(&mut state.catalog_cache);
    let mut session =
        with_connection_guc_state(sql_session_for_server(server, &mut db), server, state)
            .with_catalog_cache(catalog_cache)
            .with_cancellation(cancellation.clone());
    let started = Instant::now();
    session.execute("BEGIN")?;
    for operation in &state.tx_statements {
        if let Err(error) = cancellation.check() {
            let _ = session.execute("ROLLBACK");
            capture_connection_guc_state(state, &session);
            state.catalog_cache = session.into_catalog_cache();
            server.record_write_execution(duration_nanos_u64(started.elapsed()));
            state.tx_shared_role_ddl.clear();
            clear_open_transaction_state(server, state);
            server
                .advisory_locks
                .release_transaction(state.connection_id);
            return Err(error.into());
        }
        let result = match operation {
            TxBufferedOperation::Sql(statement) => session.execute(statement),
            TxBufferedOperation::CopyRows {
                relation,
                columns,
                rows,
            } => session
                .copy_insert_rows(relation, columns, rows.clone())
                .map(|count| SqlResult::command(format!("COPY {count}"))),
        };
        if let Err(error) = result {
            let _ = session.execute("ROLLBACK");
            capture_connection_guc_state(state, &session);
            state.catalog_cache = session.into_catalog_cache();
            server.record_write_execution(duration_nanos_u64(started.elapsed()));
            state.tx_shared_role_ddl.clear();
            clear_open_transaction_state(server, state);
            server
                .advisory_locks
                .release_transaction(state.connection_id);
            return Err(error.into());
        }
    }
    let result = session.execute("COMMIT");
    capture_connection_guc_state(state, &session);
    state.catalog_cache = session.into_catalog_cache();
    server.record_write_execution(duration_nanos_u64(started.elapsed()));
    if result.is_ok() {
        state.committed_shared_role_ddl = std::mem::take(&mut state.tx_shared_role_ddl);
    } else {
        state.tx_shared_role_ddl.clear();
    }
    clear_open_transaction_state(server, state);
    server
        .advisory_locks
        .release_transaction(state.connection_id);
    result.map_err(PgWireError::from)
}

pub(crate) fn rollback_open_transaction(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
) -> Result<()> {
    state.tx_statements.clear();
    let transaction_rollback = state
        .tx
        .take()
        .map(Transaction::rollback)
        .unwrap_or(Ok(()))
        .map_err(PgWireError::from);
    let ddl_rollback = rollback_transaction_ddl_to_len(server, state, 0);
    state.session_state.rollback_transaction();
    state.tx_shared_role_ddl.clear();
    state.committed_shared_role_ddl.clear();
    clear_open_transaction_state(server, state);
    server
        .advisory_locks
        .release_transaction(state.connection_id);
    transaction_rollback?;
    ddl_rollback?;
    Ok(())
}

pub(crate) fn clear_open_transaction_state(server: &PgWireServer, state: &mut ConnectionState) {
    delegation::clear_delegation(server, state);
    state.in_transaction = false;
    state.failed_transaction = false;
    state.tx_statements.clear();
    state.tx = None;
    state.tx_ddl_undo = SqlSessionDdlUndoLog::default();
    state.tx_collection_generations.clear();
    state.tx_write_tables.clear();
    state.savepoints.clear();
}

pub(crate) fn rollback_transaction_ddl_to_len(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    len: usize,
) -> Result<()> {
    if state.tx_ddl_undo.len() <= len {
        return Ok(());
    }
    ensure_server_writable(server)?;
    let admission = server.try_admit_write()?;
    let mut db = server.write_db_with_admission(&admission)?;
    let catalog_cache = std::mem::take(&mut state.catalog_cache);
    let ddl_undo_log = std::mem::take(&mut state.tx_ddl_undo);
    let mut session =
        with_connection_guc_state(sql_session_for_server(server, &mut db), server, state)
            .with_catalog_cache(catalog_cache)
            .with_ddl_undo_log(ddl_undo_log);
    let result = session.rollback_ddl_undo_to_len(len);
    capture_connection_guc_state(state, &session);
    state.tx_ddl_undo = session.take_ddl_undo_log();
    state.catalog_cache = session.into_catalog_cache();
    result.map_err(PgWireError::from)
}

pub(crate) fn validate_buffered_transaction_generations(
    db: &BicDb,
    state: &ConnectionState,
) -> Result<()> {
    for table in &state.tx_write_tables {
        let snapshot_generation = state
            .tx_collection_generations
            .get(table)
            .copied()
            .unwrap_or_default();
        if db.collection_generation(table) != snapshot_generation {
            return Err(PgWireError::BicDb(BicDbError::TransactionConflict(
                format!("{table} changed since transaction snapshot; no lock wait occurred"),
            )));
        }
    }
    Ok(())
}

pub(crate) fn ensure_server_writable(server: &Arc<PgWireServer>) -> Result<()> {
    if !server.config.standby_read_only {
        return Ok(());
    }
    let db = server.read_db_for_transaction_progress()?;
    if db.ha_role() == HaRole::Standby {
        return Err(PgWireError::Server(
            "database is read-only standby; promote before accepting writes".to_string(),
        ));
    }
    Ok(())
}

/// Apply the connection-view visibility rule to one snapshot.
///
/// Your own session, or any session when you administer the server, is
/// returned intact; every other session keeps its identity row but loses
/// the columns that carry data — peer address and in-flight SQL.
pub(crate) fn redact_connection_for_caller(
    connection: ServerConnectionSnapshot,
    caller: Option<VirtualQueryCaller<'_>>,
    privileged: bool,
) -> ServerConnectionSnapshot {
    let own = caller.is_some_and(|caller| caller.connection_id == connection.connection_id);
    if privileged || own {
        return connection;
    }
    ServerConnectionSnapshot {
        peer_addr: None,
        active_query: None,
        ..connection
    }
}

/// Who is asking, for views that expose other sessions.
///
/// `None` means the caller could not be identified (the describe path,
/// which never returns rows) and is treated as least-privileged.
#[derive(Clone, Copy)]
pub(crate) struct VirtualQueryCaller<'a> {
    pub(crate) connection_id: u64,
    pub(crate) user: &'a str,
}

pub(crate) fn execute_server_virtual_query(
    server: &Arc<PgWireServer>,
    sql: &str,
) -> Result<Option<SqlResult>> {
    execute_server_virtual_query_for(server, sql, None)
}

pub(crate) fn execute_server_virtual_query_for(
    server: &Arc<PgWireServer>,
    sql: &str,
    caller: Option<VirtualQueryCaller<'_>>,
) -> Result<Option<SqlResult>> {
    execute_server_virtual_query_for_normalized(server, sql, &normalize_executable_sql(sql), caller)
}

pub(crate) fn execute_server_virtual_query_for_normalized(
    server: &Arc<PgWireServer>,
    sql: &str,
    normalized: &str,
    caller: Option<VirtualQueryCaller<'_>>,
) -> Result<Option<SqlResult>> {
    if let Some(call) = parse_advisory_lock_call_with(sql, |_| Ok(None))? {
        return Ok(Some(
            SqlResult::new(
                vec![call.result_name.to_string()],
                vec![vec![SqlValue::Null]],
            )
            .with_column_types(vec![Some(call.result_type().to_string())]),
        ));
    }
    match normalized {
        "select sum(xact_commit + xact_rollback) from pg_stat_database"
        | "select sum(xact_commit + xact_rollback) from pg_catalog.pg_stat_database" => {
            let stats = server.stats_snapshot();
            Ok(Some(SqlResult::new(
                vec!["sum".to_string()],
                vec![vec![SqlValue::Int(
                    stats.queries_executed.saturating_add(stats.failed_queries) as i64,
                )]],
            )))
        }
        "select * from bicdb_server_connections" => {
            // Other sessions' peer addresses and in-flight SQL are not
            // public. Query text routinely embeds literals — patient
            // identifiers, tokens, another tenant's keys — so an
            // unprivileged session reading this view was a cross-session
            // disclosure channel. PostgreSQL redacts `pg_stat_activity.query`
            // for non-superusers without pg_read_all_stats; this is the
            // same rule: your own row in full, everyone else's without the
            // sensitive columns, unless you administer the server.
            let privileged = caller.is_some_and(|caller| server.user_is_admin(caller.user));
            let rows = server
                .connection_snapshots()
                .into_iter()
                .map(|connection| {
                    let connection = redact_connection_for_caller(connection, caller, privileged);
                    vec![
                        SqlValue::Int(connection.connection_id as i64),
                        SqlValue::String(connection.user),
                        connection
                            .peer_addr
                            .map(SqlValue::String)
                            .unwrap_or(SqlValue::Null),
                        SqlValue::Int(connection.connected_at),
                        connection
                            .last_query_at
                            .map(SqlValue::Int)
                            .unwrap_or(SqlValue::Null),
                        SqlValue::Int(connection.queries_executed as i64),
                        SqlValue::Int(connection.failed_queries as i64),
                        SqlValue::Bool(connection.in_transaction),
                        connection
                            .active_query
                            .map(SqlValue::String)
                            .unwrap_or(SqlValue::Null),
                    ]
                })
                .collect();
            Ok(Some(SqlResult::new(
                vec![
                    "connection_id".to_string(),
                    "user".to_string(),
                    "peer_addr".to_string(),
                    "connected_at".to_string(),
                    "last_query_at".to_string(),
                    "queries_executed".to_string(),
                    "failed_queries".to_string(),
                    "in_transaction".to_string(),
                    "active_query".to_string(),
                ],
                rows,
            )))
        }
        "select * from bicdb_server_stats" => {
            let stats = server.stats_snapshot();
            Ok(Some(SqlResult::new(
                vec![
                    "max_connections".to_string(),
                    "active_connections".to_string(),
                    "peak_active_connections".to_string(),
                    "total_connections".to_string(),
                    "rejected_connections".to_string(),
                    "max_active_queries".to_string(),
                    "active_queries".to_string(),
                    "peak_active_queries".to_string(),
                    "rejected_queries".to_string(),
                    "queries_executed".to_string(),
                    "failed_queries".to_string(),
                    "canceled_queries".to_string(),
                    "timed_out_queries".to_string(),
                    "last_cancel_at".to_string(),
                    "last_cancel_connection_id".to_string(),
                    "last_cancel_reason".to_string(),
                    "last_cancel_sqlstate".to_string(),
                    "uptime_seconds".to_string(),
                    "db_size_bytes".to_string(),
                    "memory_estimate_bytes".to_string(),
                    "last_checkpoint".to_string(),
                    "writes_executed".to_string(),
                    "proc_neword".to_string(),
                    "proc_payment".to_string(),
                    "proc_delivery".to_string(),
                    "proc_orderstatus".to_string(),
                    "proc_stocklevel".to_string(),
                    "routine_serialization_failure".to_string(),
                    "routine_deadlock_detected".to_string(),
                    "routine_no_data_found".to_string(),
                    "routine_other".to_string(),
                    "wal_written_seq".to_string(),
                    "wal_write_calls".to_string(),
                    "wal_sync_calls".to_string(),
                    "wal_bytes_written".to_string(),
                    "wal_commits_written".to_string(),
                    "wal_max_batch_commits".to_string(),
                    "max_queued_writes".to_string(),
                    "write_queue_depth".to_string(),
                    "write_queue_depth_max".to_string(),
                    "write_wait_total_ns".to_string(),
                    "write_wait_max_ns".to_string(),
                    "write_execution_total_ns".to_string(),
                    "write_execution_max_ns".to_string(),
                    "write_rejected_count".to_string(),
                    "write_timed_out_count".to_string(),
                    "db_lock_acquisitions".to_string(),
                    "db_lock_wait_total_ns".to_string(),
                    "db_lock_wait_max_ns".to_string(),
                    "db_lock_hold_total_ns".to_string(),
                    "db_lock_hold_max_ns".to_string(),
                    "db_read_lock_acquisitions".to_string(),
                    "db_read_lock_wait_total_ns".to_string(),
                    "db_read_lock_wait_max_ns".to_string(),
                    "db_read_lock_hold_total_ns".to_string(),
                    "db_read_lock_hold_max_ns".to_string(),
                    "db_write_lock_acquisitions".to_string(),
                    "db_write_lock_wait_total_ns".to_string(),
                    "db_write_lock_wait_max_ns".to_string(),
                    "db_write_lock_hold_total_ns".to_string(),
                    "db_write_lock_hold_max_ns".to_string(),
                    "max_pending_accepts".to_string(),
                    "max_queued_queries".to_string(),
                    "queued_queries".to_string(),
                    "queued_queries_max".to_string(),
                    "max_active_reads".to_string(),
                    "active_reads".to_string(),
                    "peak_active_reads".to_string(),
                    "max_queued_reads".to_string(),
                    "queued_reads".to_string(),
                    "queued_reads_max".to_string(),
                    "max_active_writes".to_string(),
                    "active_writes".to_string(),
                    "peak_active_writes".to_string(),
                    "queued_writes".to_string(),
                    "queued_writes_max".to_string(),
                    "query_queue_wait_p50_ns".to_string(),
                    "query_queue_wait_p95_ns".to_string(),
                    "query_queue_wait_p99_ns".to_string(),
                    "rows_streamed".to_string(),
                    "bytes_streamed".to_string(),
                    "cursor_count".to_string(),
                    "cursor_memory_bytes".to_string(),
                    "spilled_to_disk_bytes".to_string(),
                ],
                vec![vec![
                    SqlValue::Int(stats.max_connections as i64),
                    SqlValue::Int(stats.active_connections as i64),
                    SqlValue::Int(stats.peak_active_connections as i64),
                    SqlValue::Int(stats.total_connections as i64),
                    SqlValue::Int(stats.rejected_connections as i64),
                    SqlValue::Int(stats.max_active_queries as i64),
                    SqlValue::Int(stats.active_queries as i64),
                    SqlValue::Int(stats.peak_active_queries as i64),
                    SqlValue::Int(stats.rejected_queries as i64),
                    SqlValue::Int(stats.queries_executed as i64),
                    SqlValue::Int(stats.failed_queries as i64),
                    SqlValue::Int(stats.canceled_queries as i64),
                    SqlValue::Int(stats.timed_out_queries as i64),
                    stats
                        .last_cancel_at
                        .map(SqlValue::Int)
                        .unwrap_or(SqlValue::Null),
                    stats
                        .last_cancel_connection_id
                        .map(|id| SqlValue::Int(id as i64))
                        .unwrap_or(SqlValue::Null),
                    stats
                        .last_cancel_reason
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                    stats
                        .last_cancel_sqlstate
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                    SqlValue::Int(stats.uptime_seconds),
                    SqlValue::Int(stats.db_size_bytes as i64),
                    SqlValue::Int(stats.memory_estimate_bytes as i64),
                    stats
                        .last_checkpoint
                        .map(SqlValue::Int)
                        .unwrap_or(SqlValue::Null),
                    SqlValue::Int(stats.writes_executed as i64),
                    SqlValue::Int(stats.proc_neword as i64),
                    SqlValue::Int(stats.proc_payment as i64),
                    SqlValue::Int(stats.proc_delivery as i64),
                    SqlValue::Int(stats.proc_orderstatus as i64),
                    SqlValue::Int(stats.proc_stocklevel as i64),
                    SqlValue::Int(stats.routine_serialization_failure as i64),
                    SqlValue::Int(stats.routine_deadlock_detected as i64),
                    SqlValue::Int(stats.routine_no_data_found as i64),
                    SqlValue::Int(stats.routine_other as i64),
                    SqlValue::Int(stats.wal_written_seq as i64),
                    SqlValue::Int(stats.wal_write_calls as i64),
                    SqlValue::Int(stats.wal_sync_calls as i64),
                    SqlValue::Int(stats.wal_bytes_written as i64),
                    SqlValue::Int(stats.wal_commits_written as i64),
                    SqlValue::Int(stats.wal_max_batch_commits as i64),
                    SqlValue::Int(stats.max_queued_writes as i64),
                    SqlValue::Int(stats.write_queue_depth as i64),
                    SqlValue::Int(stats.write_queue_depth_max as i64),
                    SqlValue::Int(stats.write_wait_total_ns as i64),
                    SqlValue::Int(stats.write_wait_max_ns as i64),
                    SqlValue::Int(stats.write_execution_total_ns as i64),
                    SqlValue::Int(stats.write_execution_max_ns as i64),
                    SqlValue::Int(stats.write_rejected_count as i64),
                    SqlValue::Int(stats.write_timed_out_count as i64),
                    SqlValue::Int(stats.db_lock_acquisitions as i64),
                    SqlValue::Int(stats.db_lock_wait_total_ns as i64),
                    SqlValue::Int(stats.db_lock_wait_max_ns as i64),
                    SqlValue::Int(stats.db_lock_hold_total_ns as i64),
                    SqlValue::Int(stats.db_lock_hold_max_ns as i64),
                    SqlValue::Int(stats.db_read_lock_acquisitions as i64),
                    SqlValue::Int(stats.db_read_lock_wait_total_ns as i64),
                    SqlValue::Int(stats.db_read_lock_wait_max_ns as i64),
                    SqlValue::Int(stats.db_read_lock_hold_total_ns as i64),
                    SqlValue::Int(stats.db_read_lock_hold_max_ns as i64),
                    SqlValue::Int(stats.db_write_lock_acquisitions as i64),
                    SqlValue::Int(stats.db_write_lock_wait_total_ns as i64),
                    SqlValue::Int(stats.db_write_lock_wait_max_ns as i64),
                    SqlValue::Int(stats.db_write_lock_hold_total_ns as i64),
                    SqlValue::Int(stats.db_write_lock_hold_max_ns as i64),
                    SqlValue::Int(stats.max_pending_accepts as i64),
                    SqlValue::Int(stats.max_queued_queries as i64),
                    SqlValue::Int(stats.queued_queries as i64),
                    SqlValue::Int(stats.queued_queries_max as i64),
                    SqlValue::Int(stats.max_active_reads as i64),
                    SqlValue::Int(stats.active_reads as i64),
                    SqlValue::Int(stats.peak_active_reads as i64),
                    SqlValue::Int(stats.max_queued_reads as i64),
                    SqlValue::Int(stats.queued_reads as i64),
                    SqlValue::Int(stats.queued_reads_max as i64),
                    SqlValue::Int(stats.max_active_writes as i64),
                    SqlValue::Int(stats.active_writes as i64),
                    SqlValue::Int(stats.peak_active_writes as i64),
                    SqlValue::Int(stats.queued_writes as i64),
                    SqlValue::Int(stats.queued_writes_max as i64),
                    SqlValue::Int(stats.query_queue_wait_p50_ns as i64),
                    SqlValue::Int(stats.query_queue_wait_p95_ns as i64),
                    SqlValue::Int(stats.query_queue_wait_p99_ns as i64),
                    SqlValue::Int(stats.rows_streamed as i64),
                    SqlValue::Int(stats.bytes_streamed as i64),
                    SqlValue::Int(stats.cursor_count as i64),
                    SqlValue::Int(stats.cursor_memory_bytes as i64),
                    SqlValue::Int(stats.spilled_to_disk_bytes as i64),
                ]],
            )))
        }
        "select * from bicdb_ha_status" => {
            let db = server.read_db()?;
            let status = db.ha_status()?;
            Ok(Some(SqlResult::new(
                vec![
                    "role".to_string(),
                    "read_only".to_string(),
                    "ready".to_string(),
                    "source_path".to_string(),
                    "source_checkpoint_bytes".to_string(),
                    "applied_checkpoint_bytes".to_string(),
                    "lag_bytes".to_string(),
                    "last_apply_at".to_string(),
                    "last_apply_error".to_string(),
                    "promoted_at".to_string(),
                    "last_durable_checkpoint_bytes".to_string(),
                ],
                vec![vec![
                    SqlValue::String(format!("{:?}", status.role).to_lowercase()),
                    SqlValue::Bool(status.read_only),
                    SqlValue::Bool(status.ready),
                    status
                        .source_path
                        .map(|path| SqlValue::String(path.display().to_string()))
                        .unwrap_or(SqlValue::Null),
                    SqlValue::Int(status.source_checkpoint_bytes as i64),
                    SqlValue::Int(status.applied_checkpoint_bytes as i64),
                    SqlValue::Int(status.lag_bytes as i64),
                    status
                        .last_apply_at
                        .map(SqlValue::Int)
                        .unwrap_or(SqlValue::Null),
                    status
                        .last_apply_error
                        .map(SqlValue::String)
                        .unwrap_or(SqlValue::Null),
                    status
                        .promoted_at
                        .map(SqlValue::Int)
                        .unwrap_or(SqlValue::Null),
                    SqlValue::Int(status.last_durable_checkpoint_bytes as i64),
                ]],
            )))
        }
        _ => Ok(None),
    }
}

pub(crate) fn should_buffer_in_transaction(normalized: &str) -> bool {
    normalized.starts_with("insert ")
        || normalized.starts_with("update ")
        || normalized.starts_with("delete ")
        || normalized.starts_with("create ")
        || normalized.starts_with("drop ")
        || normalized.starts_with("alter ")
}

pub(crate) fn buffer_transaction_sql(
    state: &mut ConnectionState,
    sql: &str,
) -> Option<&'static str> {
    let statements = split_sql_statements(sql);
    if statements.is_empty() {
        return None;
    }

    let mut buffered = Vec::with_capacity(statements.len());
    let mut write_tables = Vec::new();
    let mut command_tag = "OK";
    for statement in statements {
        let executable = strip_sql_comments(&statement);
        let normalized = normalize_sql(&executable);
        if !should_buffer_in_transaction(&normalized) {
            return None;
        }
        if let Some(table) = write_table_for_buffered_sql(&normalized) {
            write_tables.push(table);
        }
        command_tag = command_tag_for_buffered_sql(&normalized);
        buffered.push(TxBufferedOperation::Sql(
            executable.trim().trim_end_matches(';').to_string(),
        ));
    }

    state.tx_write_tables.extend(write_tables);
    state.tx_statements.extend(buffered);
    Some(command_tag)
}

pub(crate) fn is_transaction_control_candidate(normalized: &str) -> bool {
    normalized == "begin"
        || normalized.starts_with("begin ")
        || normalized == "start transaction"
        || normalized.starts_with("start transaction ")
        || normalized == "set transaction"
        || normalized.starts_with("set transaction ")
        || normalized.starts_with("set session characteristics as transaction")
}

pub(crate) fn begin_buffered_transaction(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
) -> Result<SqlResult> {
    if state.in_transaction {
        Err(PgWireError::Server(
            "nested transactions are not supported".to_string(),
        ))
    } else {
        let tx = server
            .read_db()?
            .begin_transaction_after(state.last_commit_seq)?;
        state.session_state.begin_transaction();
        state.in_transaction = true;
        state.failed_transaction = false;
        state.tx_statements.clear();
        state.tx = Some(tx);
        state.tx_ddl_undo = SqlSessionDdlUndoLog::default();
        state.tx_collection_generations = server.read_db()?.collection_generations();
        state.tx_write_tables.clear();
        state.tx_shared_role_ddl.clear();
        state.committed_shared_role_ddl.clear();
        state.savepoints.clear();
        Ok(SqlResult::command("BEGIN"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SavepointCommand {
    Create(String),
    RollbackTo(String),
    Release(String),
}

pub(crate) fn parse_savepoint_command(normalized: &str) -> Option<SavepointCommand> {
    if !(normalized.starts_with("savepoint ")
        || normalized.starts_with("rollback to ")
        || normalized.starts_with("release "))
    {
        return None;
    }
    let parts = normalized.split_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        ["savepoint", name] => Some(SavepointCommand::Create((*name).to_string())),
        ["rollback", "to", name] => Some(SavepointCommand::RollbackTo((*name).to_string())),
        ["rollback", "to", "savepoint", name] => {
            Some(SavepointCommand::RollbackTo((*name).to_string()))
        }
        ["release", name] => Some(SavepointCommand::Release((*name).to_string())),
        ["release", "savepoint", name] => Some(SavepointCommand::Release((*name).to_string())),
        _ => None,
    }
}

pub(crate) fn execute_savepoint_command(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    command: SavepointCommand,
) -> Result<SqlResult> {
    match command {
        SavepointCommand::Create(name) => {
            if state.failed_transaction {
                return Err(PgWireError::InFailedTransaction);
            }
            if !state.in_transaction {
                return Err(SqlError::InvalidTransactionState {
                    message: "SAVEPOINT can only be used in transaction blocks".to_string(),
                }
                .into());
            }
            state.savepoints.push(SavepointMark {
                name,
                statement_len: state.tx_statements.len(),
                write_len: state.tx.as_ref().map(Transaction::write_len).unwrap_or(0),
                lock_len: state.tx.as_ref().map(Transaction::lock_len).unwrap_or(0),
                ddl_undo_len: state.tx_ddl_undo.len(),
                shared_role_ddl_len: state.tx_shared_role_ddl.len(),
                session_state: state.session_state.clone(),
            });
            if let Err(error) = enforce_connection_memory(state, &server.config) {
                state.savepoints.pop();
                return Err(error);
            }
            Ok(SqlResult::command("SAVEPOINT"))
        }
        SavepointCommand::RollbackTo(name) => {
            if !state.in_transaction {
                return Err(SqlError::InvalidTransactionState {
                    message: "ROLLBACK TO SAVEPOINT can only be used in transaction blocks"
                        .to_string(),
                }
                .into());
            }
            let Some(position) = state
                .savepoints
                .iter()
                .rposition(|savepoint| savepoint.name == name)
            else {
                return Err(SqlError::InvalidSavepoint { name }.into());
            };
            let statement_len = state.savepoints[position].statement_len;
            let ddl_undo_len = state.savepoints[position].ddl_undo_len;
            let shared_role_ddl_len = state.savepoints[position].shared_role_ddl_len;
            let session_state = state.savepoints[position].session_state.clone();
            state.tx_statements.truncate(statement_len);
            if let Some(tx) = state.tx.as_mut() {
                tx.truncate_writes_and_locks(
                    state.savepoints[position].write_len,
                    state.savepoints[position].lock_len,
                )?;
            }
            rollback_transaction_ddl_to_len(server, state, ddl_undo_len)?;
            state.tx_shared_role_ddl.truncate(shared_role_ddl_len);
            state.session_state.restore_savepoint(&session_state);
            state.tx_write_tables = write_tables_for_buffered_statements(&state.tx_statements);
            state.savepoints.truncate(position + 1);
            state.failed_transaction = false;
            Ok(SqlResult::command("ROLLBACK"))
        }
        SavepointCommand::Release(name) => {
            if state.failed_transaction {
                return Err(PgWireError::InFailedTransaction);
            }
            if !state.in_transaction {
                return Err(SqlError::InvalidTransactionState {
                    message: "RELEASE SAVEPOINT can only be used in transaction blocks".to_string(),
                }
                .into());
            }
            let Some(position) = state
                .savepoints
                .iter()
                .rposition(|savepoint| savepoint.name == name)
            else {
                return Err(SqlError::InvalidSavepoint { name }.into());
            };
            state.savepoints.truncate(position);
            Ok(SqlResult::command("RELEASE"))
        }
    }
}

pub(crate) fn command_tag_for_buffered_sql(normalized: &str) -> &'static str {
    if normalized.starts_with("insert ") {
        "INSERT 0 1"
    } else if normalized.starts_with("update ") {
        "UPDATE 0"
    } else if normalized.starts_with("delete ") {
        "DELETE 0"
    } else if normalized.starts_with("create ") {
        "CREATE"
    } else if normalized.starts_with("drop ") {
        "DROP"
    } else if normalized.starts_with("alter ") {
        "ALTER"
    } else {
        "OK"
    }
}

pub(crate) fn write_tables_for_buffered_statements(
    statements: &[TxBufferedOperation],
) -> FxHashSet<String> {
    statements
        .iter()
        .filter_map(TxBufferedOperation::write_table)
        .collect()
}

pub(crate) fn write_table_for_buffered_sql(normalized: &str) -> Option<String> {
    let table = if let Some(rest) = normalized.strip_prefix("insert into ") {
        rest.split_whitespace().next()
    } else if let Some(rest) = normalized.strip_prefix("update ") {
        rest.split_whitespace().next()
    } else if let Some(rest) = normalized.strip_prefix("delete from ") {
        rest.split_whitespace().next()
    } else {
        None
    }?;
    Some(table.trim_matches('"').trim_end_matches(';').to_string())
}

pub(crate) fn is_write_sql(normalized: &str) -> bool {
    normalized.starts_with("insert ")
        || normalized.starts_with("update ")
        || normalized.starts_with("delete ")
        || is_mutating_with_sql(normalized)
        || normalized.starts_with("create ")
        || normalized.starts_with("drop ")
        || normalized.starts_with("alter ")
        || normalized.starts_with("copy ")
        || normalized == "commit"
}

pub(crate) fn query_kind_for_sql(sql: &str) -> QueryKind {
    query_kind_for_normalized_sql(&normalize_executable_sql(sql))
}

pub(crate) fn query_kind_for_normalized_sql(normalized: &str) -> QueryKind {
    if is_write_sql(normalized) {
        QueryKind::Write
    } else {
        QueryKind::Read
    }
}

pub(crate) fn is_server_status_query(sql: &str) -> bool {
    is_server_status_query_normalized(&normalize_executable_sql(sql))
}

pub(crate) fn is_server_status_query_normalized(normalized: &str) -> bool {
    matches!(
        normalized,
        "select sum(xact_commit + xact_rollback) from pg_stat_database"
            | "select sum(xact_commit + xact_rollback) from pg_catalog.pg_stat_database"
            | "select * from bicdb_server_connections"
            | "select * from bicdb_server_stats"
            | "select * from bicdb_ha_status"
    )
}

pub(crate) fn is_read_only_sql_for_shared_execution(sql: &str) -> bool {
    is_read_only_sql_for_shared_execution_normalized(&normalize_executable_sql(sql))
}

pub(crate) fn is_read_only_sql_for_shared_execution_normalized(normalized: &str) -> bool {
    if normalized.contains("nextval")
        || normalized.contains("currval")
        || normalized.contains("setval")
        || normalized.contains("set_config")
    {
        return false;
    }
    normalized == "select 1"
        || normalized.starts_with("select ")
        || (normalized.starts_with("with ") && !is_mutating_with_sql(normalized))
        || normalized.starts_with("show ")
        || normalized.starts_with("explain select ")
}

pub(crate) fn is_mutating_with_sql(normalized: &str) -> bool {
    normalized.starts_with("with ")
        && [
            ") insert ",
            ") update ",
            ") delete ",
            ") merge ",
            " insert into ",
            " update ",
            " delete from ",
            " merge into ",
        ]
        .iter()
        .any(|needle| normalized.contains(needle))
}

pub(crate) fn parse_copy_statement(sql: &str) -> Result<Option<CopyStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !trimmed
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("copy"))
        || !trimmed
            .get(4..5)
            .is_some_and(|next| next.chars().all(char::is_whitespace))
    {
        return Ok(None);
    }
    if contains_word(trimmed, "binary") {
        return Err(PgWireError::Server(
            "COPY BINARY is not supported; use text or CSV COPY".to_string(),
        ));
    }
    if contains_word(trimmed, "program") || contains_word(trimmed, "freeze") {
        return Err(PgWireError::Server(
            "this COPY variant is not supported by BicDB".to_string(),
        ));
    }

    let body = trimmed[4..].trim();
    let (target, direction, after_direction) = split_copy_direction(body)?;
    let format = if contains_word(after_direction, "csv") {
        CopyFormat::Csv
    } else {
        CopyFormat::Text
    };
    let header = contains_word(after_direction, "header");

    match direction {
        CopyDirection::FromStdin => {
            if !after_direction
                .trim_start()
                .to_ascii_lowercase()
                .starts_with("stdin")
            {
                return Err(PgWireError::Server(
                    "COPY FROM supports only STDIN".to_string(),
                ));
            }
            let (relation, columns) = parse_copy_table_target(target)?;
            Ok(Some(CopyStatement {
                relation: Some(relation),
                columns,
                direction,
                format,
                query: None,
                header,
            }))
        }
        CopyDirection::ToStdout => {
            if !after_direction
                .trim_start()
                .to_ascii_lowercase()
                .starts_with("stdout")
            {
                return Err(PgWireError::Server(
                    "COPY TO supports only STDOUT".to_string(),
                ));
            }
            if target.starts_with('(') && target.ends_with(')') {
                Ok(Some(CopyStatement {
                    relation: None,
                    columns: Vec::new(),
                    direction,
                    format,
                    query: Some(target[1..target.len() - 1].trim().to_string()),
                    header,
                }))
            } else {
                let (relation, columns) = parse_copy_table_target(target)?;
                Ok(Some(CopyStatement {
                    relation: Some(relation),
                    columns,
                    direction,
                    format,
                    query: None,
                    header,
                }))
            }
        }
    }
}

pub(crate) fn split_copy_direction(body: &str) -> Result<(&str, CopyDirection, &str)> {
    let mut depth = 0_i32;
    for (idx, ch) in body.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            if word_at(body, idx, "from") {
                return Ok((
                    body[..idx].trim(),
                    CopyDirection::FromStdin,
                    body[idx + 4..].trim(),
                ));
            }
            if word_at(body, idx, "to") {
                return Ok((
                    body[..idx].trim(),
                    CopyDirection::ToStdout,
                    body[idx + 2..].trim(),
                ));
            }
        }
    }
    Err(PgWireError::Server(
        "COPY requires FROM STDIN or TO STDOUT".to_string(),
    ))
}

pub(crate) fn parse_copy_table_target(target: &str) -> Result<(String, Vec<String>)> {
    let target = target.trim();
    if target.is_empty() || target.starts_with('(') {
        return Err(PgWireError::Server(
            "COPY FROM STDIN requires a table name".to_string(),
        ));
    }
    if let Some(open) = target.find('(') {
        let close = target
            .rfind(')')
            .ok_or_else(|| PgWireError::Server("COPY column list is missing ')'".to_string()))?;
        let relation = normalize_qualified_relation_identifier(target[..open].trim());
        let columns = target[open + 1..close]
            .split(',')
            .map(|column| normalize_identifier(column.trim()))
            .filter(|column| !column.is_empty())
            .collect::<Vec<_>>();
        if columns.is_empty() {
            return Err(PgWireError::Server(
                "COPY column list must not be empty".to_string(),
            ));
        }
        Ok((relation, columns))
    } else {
        Ok((normalize_qualified_relation_identifier(target), Vec::new()))
    }
}

pub(crate) fn execute_copy_query(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    copy: &CopyStatement,
) -> Result<SqlResult> {
    let sql = if let Some(query) = &copy.query {
        query.clone()
    } else {
        let relation = copy
            .relation
            .as_ref()
            .ok_or_else(|| PgWireError::Server("COPY TO missing relation".to_string()))?;
        if copy.columns.is_empty() {
            format!("SELECT * FROM {relation}")
        } else {
            format!("SELECT {} FROM {relation}", copy.columns.join(", "))
        }
    };
    execute_server_sql_with_options(server, state, &sql, false)
}

pub(crate) fn copy_in_data(state: &mut ConnectionState, payload: &[u8]) -> Result<()> {
    let input =
        std::str::from_utf8(payload).map_err(|error| PgWireError::Protocol(error.to_string()))?;
    let copy = state
        .copy_in
        .as_mut()
        .ok_or_else(|| PgWireError::Protocol("COPY FROM STDIN is not active".to_string()))?;
    copy.pending.push_str(input);
    while let Some(newline) = copy.pending.find('\n') {
        let mut line = copy.pending[..newline].to_string();
        if line.ends_with('\r') {
            line.pop();
        }
        copy.pending.drain(..=newline);
        copy_in_line(copy, &line)?;
        spill_copy_rows_if_needed(copy)?;
    }
    Ok(())
}

pub(crate) fn finish_copy_in(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
) -> Result<usize> {
    let started_guc_transaction = state
        .copy_in
        .as_ref()
        .is_some_and(|copy| copy.started_guc_transaction);
    let result = delegation::validate_delegation(server, state)
        .and_then(|_| finish_copy_in_inner(server, state));
    if result.is_err() {
        state.copy_in = None;
    }
    if started_guc_transaction {
        if result.is_ok() {
            state.session_state.commit_transaction();
        } else {
            state.session_state.rollback_transaction();
        }
    }
    if let Err(memory_error) = enforce_persistent_connection_memory(state, &server.config) {
        state.session_state.rollback_transaction();
        let memory_error =
            classify_memory_error_after_transaction_cleanup(state, &server.config, memory_error);
        if result.is_ok() || memory_error.closes_connection() {
            return Err(memory_error);
        }
    }
    result
}

pub(crate) fn abort_copy_in(state: &mut ConnectionState) {
    let started_guc_transaction = state
        .copy_in
        .take()
        .is_some_and(|copy| copy.started_guc_transaction);
    if started_guc_transaction {
        state.session_state.rollback_transaction();
    }
}

pub(crate) fn finish_copy_in_inner(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
) -> Result<usize> {
    ensure_server_writable(server)?;
    let _permit = server.acquire_query(QueryKind::Write)?;
    let deadline = (!server.config.query_timeout.is_zero())
        .then(|| Instant::now() + server.config.query_timeout);
    let cancellation = CancellationToken::new(state.cancel_token.clone(), deadline);
    cancellation
        .check()
        .map_err(|error| classify_bicdb_interrupt(server, state, error))?;
    let mut copy = state
        .copy_in
        .take()
        .ok_or_else(|| PgWireError::Protocol("COPY FROM STDIN is not active".to_string()))?;
    if !copy.pending.is_empty() {
        let line = std::mem::take(&mut copy.pending);
        copy_in_line(&mut copy, line.trim_end_matches('\r'))?;
        spill_copy_rows_if_needed(&mut copy)?;
    }
    if let Some(spill) = copy.spill.as_mut() {
        spill.writer.flush()?;
    }
    let relation = copy
        .spec
        .relation
        .clone()
        .ok_or_else(|| PgWireError::Server("COPY FROM STDIN missing relation".to_string()))?;
    let columns = if copy.spec.columns.is_empty() {
        let result = execute_server_sql_for_describe(server, &format!("SELECT * FROM {relation}"))?;
        result.columns
    } else {
        copy.spec.columns.clone()
    };
    if columns.is_empty() {
        return Err(PgWireError::Server(
            "COPY FROM STDIN requires known target columns".to_string(),
        ));
    }

    let mut copied_rows = copy.inserted_rows;
    if state.in_transaction {
        copied_rows += finish_transactional_copy_in(
            server,
            state,
            &mut copy,
            &relation,
            &columns,
            &cancellation,
        )?;
    } else {
        copied_rows +=
            finish_direct_copy_in(server, state, &mut copy, &relation, &columns, &cancellation)?;
    }

    state.queries_executed += 1;
    server.queries_executed.fetch_add(1, Ordering::SeqCst);
    server.writes_executed.fetch_add(1, Ordering::SeqCst);
    server
        .connections
        .lock()
        .unwrap()
        .insert(state.connection_id, state.snapshot());
    Ok(copied_rows)
}

pub(crate) fn spill_copy_rows_if_needed(copy: &mut CopyInState) -> Result<()> {
    if copy.rows.len() < COPY_IN_FLUSH_ROWS {
        return Ok(());
    }
    spill_copy_rows(copy)
}

pub(crate) fn spill_copy_rows(copy: &mut CopyInState) -> Result<()> {
    if copy.rows.is_empty() {
        return Ok(());
    }
    if copy.spill.is_none() {
        copy.spill = Some(CopySpillState::new()?);
    }
    let spill = copy
        .spill
        .as_mut()
        .ok_or_else(|| PgWireError::Server("COPY spill was not initialized".to_string()))?;
    for row in copy.rows.drain(..) {
        serde_json::to_writer(&mut spill.writer, &row)
            .map_err(|error| PgWireError::Server(error.to_string()))?;
        spill.writer.write_all(b"\n")?;
        spill.rows += 1;
    }
    Ok(())
}

pub(crate) fn finish_direct_copy_in(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    copy: &mut CopyInState,
    relation: &str,
    columns: &[String],
    cancellation: &CancellationToken,
) -> Result<usize> {
    // COPY is one statement even when its input was spilled/replayed in
    // batches. A later trigger or constraint failure must undo earlier batches.
    execute_server_sql(server, state, "BEGIN")?;
    let result = finish_transactional_copy_in(server, state, copy, relation, columns, cancellation)
        .and_then(|count| {
            execute_server_sql(server, state, "COMMIT")?;
            Ok(count)
        });
    if result.is_err() {
        rollback_open_transaction(server, state)?;
    }
    result
}

pub(crate) fn finish_transactional_copy_in(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    copy: &mut CopyInState,
    relation: &str,
    columns: &[String],
    cancellation: &CancellationToken,
) -> Result<usize> {
    let mut copied_rows = 0;
    copied_rows += replay_spilled_copy_rows(
        copy,
        COPY_IN_TRANSACTION_BATCH_ROWS,
        |rows| append_copy_insert_statements(server, state, relation, columns, rows, cancellation),
        cancellation,
    )?;
    if !copy.rows.is_empty() {
        let rows = std::mem::take(&mut copy.rows);
        copied_rows +=
            append_copy_insert_statements(server, state, relation, columns, rows, cancellation)?;
    }
    Ok(copied_rows)
}

pub(crate) fn replay_spilled_copy_rows<F>(
    copy: &mut CopyInState,
    batch_rows: usize,
    mut consume_batch: F,
    cancellation: &CancellationToken,
) -> Result<usize>
where
    F: FnMut(Vec<Vec<Option<String>>>) -> Result<usize>,
{
    let Some(spill) = copy.spill.as_mut() else {
        return Ok(0);
    };
    spill.writer.flush()?;
    let file = File::open(&spill.path)?;
    let reader = BufReader::new(file);
    let mut batch = Vec::with_capacity(batch_rows);
    let mut copied_rows = 0;
    for line in reader.lines() {
        cancellation.check().map_err(PgWireError::from)?;
        let line = line?;
        let row = serde_json::from_str::<Vec<Option<String>>>(&line)
            .map_err(|error| PgWireError::Server(error.to_string()))?;
        batch.push(row);
        if batch.len() >= batch_rows {
            copied_rows += consume_batch(std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        copied_rows += consume_batch(batch)?;
    }
    Ok(copied_rows)
}

pub(crate) fn append_copy_insert_statements(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    relation: &str,
    columns: &[String],
    rows: Vec<Vec<Option<String>>>,
    cancellation: &CancellationToken,
) -> Result<usize> {
    for (idx, row) in rows.iter().enumerate() {
        if idx % 1024 == 0 {
            cancellation
                .check()
                .map_err(|error| classify_bicdb_interrupt(server, state, error))?;
        }
        if row.len() != columns.len() {
            return Err(PgWireError::Server(format!(
                "COPY expected {} columns, got {}",
                columns.len(),
                row.len()
            )));
        }
    }
    let count = rows.len();
    state.tx_write_tables.insert(relation.to_string());
    // Defaults and trigger bodies can allocate sequences or change catalogs.
    // Keep COPY's existing exclusive batch execution, but retain one transaction
    // across batches. Admission is acquired before taking connection state.
    ensure_server_writable(server)?;
    let admission = server.try_admit_write()?;
    let mut db = server.write_db_with_admission(&admission)?;
    let Some(tx) = state.tx.take() else {
        return Err(PgWireError::Server(
            "transaction is open but no transaction state is available".to_string(),
        ));
    };
    let catalog_cache = std::mem::take(&mut state.catalog_cache);
    let ddl_undo_log = std::mem::take(&mut state.tx_ddl_undo);
    let mut session =
        with_connection_guc_state(sql_session_for_server(server, &mut db), server, state)
            .with_catalog_cache(catalog_cache)
            .with_ddl_undo_log(ddl_undo_log)
            .with_cancellation(cancellation.clone())
            .with_pending_transaction(tx);
    let started = Instant::now();
    let result = session.copy_insert_rows(relation, columns, rows);
    capture_connection_guc_state(state, &session);
    state.tx = session.take_pending_transaction();
    state.tx_ddl_undo = session.take_ddl_undo_log();
    state.catalog_cache = session.into_catalog_cache();
    server.record_write_execution(duration_nanos_u64(started.elapsed()));
    result.map_err(PgWireError::from)?;
    Ok(count)
}

pub(crate) fn copy_in_line(copy: &mut CopyInState, line: &str) -> Result<()> {
    if line.is_empty() {
        return Ok(());
    }
    if copy.spec.format == CopyFormat::Text && line == "\\." {
        return Ok(());
    }
    if copy.spec.header && !copy.header_seen {
        copy.header_seen = true;
        return Ok(());
    }
    let row = match copy.spec.format {
        CopyFormat::Text => parse_copy_text_row(line),
        CopyFormat::Csv => parse_copy_csv_row(line),
    }?;
    copy.rows.push(row);
    Ok(())
}

pub(crate) fn parse_copy_text_row(line: &str) -> Result<Vec<Option<String>>> {
    line.split('\t')
        .map(|field| {
            if field == "\\N" {
                Ok(None)
            } else {
                Ok(Some(unescape_copy_text(field)?))
            }
        })
        .collect()
}

pub(crate) fn unescape_copy_text(field: &str) -> Result<String> {
    let mut output = String::with_capacity(field.len());
    let mut chars = field.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => output.push('\n'),
            Some('r') => output.push('\r'),
            Some('t') => output.push('\t'),
            Some('\\') => output.push('\\'),
            Some(other) => output.push(other),
            None => output.push('\\'),
        }
    }
    Ok(output)
}

pub(crate) fn parse_copy_csv_row(line: &str) -> Result<Vec<Option<String>>> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut quoted = false;
    let mut field_was_quoted = false;
    while let Some(ch) = chars.next() {
        if quoted {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    quoted = false;
                }
            } else {
                field.push(ch);
            }
        } else if ch == '"' && field.is_empty() {
            quoted = true;
            field_was_quoted = true;
        } else if ch == ',' {
            fields.push(csv_field_value(&field, field_was_quoted));
            field.clear();
            field_was_quoted = false;
        } else {
            field.push(ch);
        }
    }
    if quoted {
        return Err(PgWireError::Server(
            "unterminated quoted CSV field in COPY data".to_string(),
        ));
    }
    fields.push(csv_field_value(&field, field_was_quoted));
    Ok(fields)
}

pub(crate) fn csv_field_value(field: &str, quoted: bool) -> Option<String> {
    if !quoted && field.is_empty() {
        None
    } else {
        Some(field.to_string())
    }
}

pub(crate) fn render_copy_row(
    db: &BicDb,
    row: &[SqlValue],
    column_types: &[i32],
    format: CopyFormat,
) -> Result<Vec<u8>> {
    let mut output = String::new();
    for (idx, value) in row.iter().enumerate() {
        if idx > 0 {
            output.push(match format {
                CopyFormat::Text => '\t',
                CopyFormat::Csv => ',',
            });
        }
        let rendered = if matches!(value, SqlValue::Null) {
            None
        } else {
            let oid = column_types.get(idx).copied().unwrap_or(25);
            Some(
                String::from_utf8(encode_result_value_with_db(db, value, oid, 0)?)
                    .map_err(|error| PgWireError::Protocol(error.to_string()))?,
            )
        };
        match format {
            CopyFormat::Text => output.push_str(&render_copy_text_value(rendered.as_deref())),
            CopyFormat::Csv => output.push_str(&render_copy_csv_value(rendered.as_deref())),
        }
    }
    output.push('\n');
    Ok(output.into_bytes())
}

pub(crate) fn render_copy_text_value(value: Option<&str>) -> String {
    let Some(value) = value else {
        return "\\N".to_string();
    };
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

pub(crate) fn render_copy_csv_value(value: Option<&str>) -> String {
    let Some(value) = value else {
        return String::new();
    };
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

pub(crate) fn copy_in_response(
    stream: &mut ClientStream,
    server: &Arc<PgWireServer>,
    copy: &CopyStatement,
) -> Result<()> {
    let column_count = if copy.columns.is_empty() {
        let relation = copy
            .relation
            .as_ref()
            .ok_or_else(|| PgWireError::Server("COPY FROM STDIN missing relation".to_string()))?;
        execute_server_sql_for_describe(server, &format!("SELECT * FROM {relation}"))?
            .columns
            .len()
    } else {
        copy.columns.len()
    };
    let mut payload = Vec::new();
    payload.push(0);
    put_i16(&mut payload, column_count as i16);
    for _ in 0..column_count {
        put_i16(&mut payload, 0);
    }
    write_message(stream, b'G', &payload)
}

pub(crate) fn copy_out_response(stream: &mut ClientStream, result: &SqlResult) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(0);
    put_i16(&mut payload, result.columns.len() as i16);
    for _ in &result.columns {
        put_i16(&mut payload, 0);
    }
    write_message(stream, b'H', &payload)
}

pub(crate) fn copy_data(stream: &mut ClientStream, payload: &[u8]) -> Result<()> {
    write_message(stream, b'd', payload)
}

pub(crate) fn contains_word(input: &str, word: &str) -> bool {
    input
        .char_indices()
        .any(|(idx, _)| word_at(input, idx, word))
}

pub(crate) fn word_at(input: &str, idx: usize, word: &str) -> bool {
    let end = idx + word.len();
    if end > input.len() {
        return false;
    }
    input
        .get(idx..end)
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(word))
        && input[..idx]
            .chars()
            .next_back()
            .is_none_or(|ch| !is_identifier_char(ch))
        && input[end..]
            .chars()
            .next()
            .is_none_or(|ch| !is_identifier_char(ch))
}

pub(crate) fn is_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

pub(crate) fn normalize_identifier(value: &str) -> String {
    let value = value.trim();
    if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
        value[1..value.len() - 1].replace("\"\"", "\"")
    } else {
        value.to_string()
    }
}

/// Normalize each component of a possibly schema-qualified relation while
/// KEEPING the qualifier: `Carrier_Private."Ctx" -> carrier_private.Ctx`.
/// COPY must not collapse `schema.table` to `table` — that silently
/// retargeted `COPY carrier_private.context_nonces` at `public`.
pub(crate) fn normalize_qualified_relation_identifier(value: &str) -> String {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut quoted = false;
    let mut chars = value.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if ch == '"' {
            if quoted && chars.peek().is_some_and(|(_, next)| *next == '"') {
                chars.next();
            } else {
                quoted = !quoted;
            }
        } else if ch == '.' && !quoted {
            parts.push(normalize_identifier(value[start..idx].trim()));
            start = idx + 1;
        }
    }
    parts.push(normalize_identifier(value[start..].trim()));
    parts.retain(|part| !part.is_empty());
    parts.join(".")
}

#[allow(dead_code)]
pub(crate) fn normalize_relation_identifier(value: &str) -> String {
    let mut quoted = false;
    let mut last_separator = None;
    let mut chars = value.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if ch == '"' {
            if quoted && chars.peek().is_some_and(|(_, next)| *next == '"') {
                chars.next();
            } else {
                quoted = !quoted;
            }
        } else if ch == '.' && !quoted {
            last_separator = Some(idx);
        }
    }
    normalize_identifier(last_separator.map_or(value, |idx| &value[idx + 1..]))
}

pub(crate) fn parse_message(
    payload: &[u8],
    server: &Arc<PgWireServer>,
) -> Result<ParsedStatementMessage> {
    let mut idx = 0;
    let name = read_cstr(payload, &mut idx)?;
    let sql = read_cstr(payload, &mut idx)?;
    let param_count = read_nonnegative_i16_count(payload, &mut idx, "parameter type")?;
    let mut param_type_oids = Vec::with_capacity(param_count);
    for _ in 0..param_count {
        param_type_oids.push(read_i32(payload, &mut idx)?);
    }
    let db = server.read_db()?;
    let inferred = infer_parameter_types(&db, &sql)?;
    param_type_oids = resolve_parameter_type_oids(&db, param_type_oids, inferred)?;
    Ok(ParsedStatementMessage {
        name,
        statement: PreparedStatement {
            sql,
            param_type_oids,
        },
    })
}

pub(crate) fn bind_message(
    payload: &[u8],
    prepared: &FxHashMap<String, PreparedStatement>,
    server: &Arc<PgWireServer>,
) -> Result<(String, Portal)> {
    let mut idx = 0;
    let portal_name = read_cstr(payload, &mut idx)?;
    let statement_name = read_cstr(payload, &mut idx)?;
    let statement = prepared.get(&statement_name).ok_or_else(|| {
        PgWireError::Protocol(format!("prepared statement {statement_name:?} not found"))
    })?;

    let format_count = read_nonnegative_i16_count(payload, &mut idx, "parameter format")?;
    let mut param_formats = Vec::with_capacity(format_count);
    for _ in 0..format_count {
        param_formats.push(read_i16(payload, &mut idx)?);
    }
    validate_format_codes(&param_formats, "parameter")?;

    let param_count = read_nonnegative_i16_count(payload, &mut idx, "Bind parameter")?;
    if param_count != statement.param_type_oids.len() {
        return Err(PgWireError::Protocol(format!(
            "Bind supplies {param_count} parameters, but prepared statement requires {}",
            statement.param_type_oids.len()
        )));
    }
    validate_format_arity(&param_formats, param_count, "parameter")?;
    let db = server.read_db()?;
    let mut params = Vec::with_capacity(param_count);
    for param_idx in 0..param_count {
        let len = read_i32(payload, &mut idx)?;
        let format = if param_formats.len() == 1 {
            param_formats[0]
        } else {
            param_formats.get(param_idx).copied().unwrap_or(0)
        };
        let oid = statement
            .param_type_oids
            .get(param_idx)
            .copied()
            .unwrap_or_default();
        if len < 0 {
            params.push("NULL".to_string());
            continue;
        }
        let len = len as usize;
        if idx + len > payload.len() {
            return Err(PgWireError::Protocol(
                "Bind parameter length exceeds payload".to_string(),
            ));
        }
        params.push(decode_parameter_with_db(
            &db,
            &payload[idx..idx + len],
            format,
            oid,
        )?);
        idx += len;
    }

    let result_format_count = read_nonnegative_i16_count(payload, &mut idx, "result format")?;
    let mut result_formats = Vec::with_capacity(result_format_count);
    for _ in 0..result_format_count {
        result_formats.push(read_i16(payload, &mut idx)?);
    }
    validate_format_codes(&result_formats, "result")?;

    Ok((
        portal_name,
        Portal {
            sql: substitute_parameters(&statement.sql, &params)?,
            source_sql: statement.sql.clone(),
            result_formats,
            stream: None,
            registered_stream_memory: 0,
            predescribed: None,
        },
    ))
}

pub(crate) fn describe_message(
    stream: &mut ClientStream,
    payload: &[u8],
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
) -> Result<()> {
    let mut idx = 0;
    let target = *payload
        .get(idx)
        .ok_or_else(|| PgWireError::Protocol("Describe message missing target".to_string()))?;
    idx += 1;
    let name = read_cstr(payload, &mut idx)?;
    match target {
        b'S' => {
            let statement = state.prepared.get(&name).ok_or_else(|| {
                PgWireError::Protocol(format!("prepared statement {name:?} not found"))
            })?;
            parameter_description(stream, &statement.param_type_oids)?;
            if let Some(result) = returning_describe_result(server, &statement.sql)? {
                describe_result(stream, server, &result, &[], Some(&statement.sql))?;
            } else if is_describable_query(&statement.sql) && !statement.sql.contains('$') {
                let result = execute_server_sql_for_describe_with_session(
                    server,
                    &statement.sql,
                    &state.session_state,
                    state.security_context.as_ref(),
                )?;
                describe_result(stream, server, &result, &[], Some(&statement.sql))?;
            } else if let Some(result) = placeholder_cast_describe_result(&statement.sql) {
                describe_result(stream, server, &result, &[], Some(&statement.sql))?;
            } else if is_describable_query(&statement.sql) {
                let null_params = vec!["NULL".to_string(); statement.param_type_oids.len()];
                let describe_sql = substitute_parameters(&statement.sql, &null_params)?;
                let result = execute_server_sql_for_describe_with_session(
                    server,
                    &describe_sql,
                    &state.session_state,
                    state.security_context.as_ref(),
                )?;
                describe_result(stream, server, &result, &[], Some(&statement.sql))?;
            } else {
                no_data(stream)?;
            }
        }
        b'P' => {
            let (portal_sql, portal_source_sql, portal_result_formats) = {
                let portal = state
                    .portals
                    .get(&name)
                    .ok_or_else(|| PgWireError::Protocol(format!("portal {name:?} not found")))?;
                (
                    portal.sql.clone(),
                    portal.source_sql.clone(),
                    portal.result_formats.clone(),
                )
            };
            if let Some(result) = returning_describe_result(server, &portal_sql)? {
                describe_result(
                    stream,
                    server,
                    &result,
                    &portal_result_formats,
                    Some(&portal_source_sql),
                )?;
            } else if is_describable_query(&portal_sql) {
                let result = execute_server_sql_for_describe_with_session(
                    server,
                    &portal_sql,
                    &state.session_state,
                    state.security_context.as_ref(),
                )?;
                describe_result(
                    stream,
                    server,
                    &result,
                    &portal_result_formats,
                    Some(&portal_source_sql),
                )?;
            } else {
                let result = execute_server_sql_with_options(server, state, &portal_sql, true)?;
                describe_result(
                    stream,
                    server,
                    &result,
                    &portal_result_formats,
                    Some(&portal_source_sql),
                )?;
                if let Some(portal) = state.portals.get_mut(&name) {
                    portal.predescribed = Some(result);
                }
            }
        }
        other => {
            return Err(PgWireError::Protocol(format!(
                "unknown Describe target {}",
                other as char
            )));
        }
    }
    Ok(())
}

pub(crate) struct PortalExecuteResult {
    pub(crate) result: SqlResult,
    pub(crate) result_formats: Vec<i16>,
    pub(crate) source_sql: String,
    pub(crate) suspended: bool,
}

pub(crate) fn portal_source_sql_from_execute_payload(
    payload: &[u8],
    state: &ConnectionState,
) -> Option<String> {
    let mut idx = 0;
    let portal_name = read_cstr(payload, &mut idx).ok()?;
    state
        .portals
        .get(&portal_name)
        .map(|portal| portal.source_sql.clone())
}

pub(crate) fn execute_message(
    payload: &[u8],
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
) -> Result<PortalExecuteResult> {
    let source = portal_source_sql_from_execute_payload(payload, state).unwrap_or_default();
    let normalized = normalize_executable_sql(&source);
    let ending = delegation::transaction_end(&source, &normalized)?;
    if !matches!(ending, Some(delegation::TransactionEnd::Rollback { .. }))
        && !(ending.is_some() && state.failed_transaction)
    {
        delegation::validate_delegation(server, state)?;
    }
    let mut idx = 0;
    let portal_name = read_cstr(payload, &mut idx)?;
    let max_rows = read_i32(payload, &mut idx)?;
    if max_rows < 0 {
        return Err(PgWireError::Protocol(
            "Execute max_rows must not be negative".to_string(),
        ));
    }
    let max_rows = max_rows as usize;
    let streaming_fetch = max_rows > 0;

    if let Some(portal) = state.portals.get_mut(&portal_name) {
        if let Some(result) = portal.predescribed.take() {
            let result_formats = portal.result_formats.clone();
            let source_sql = portal.source_sql.clone();
            return Ok(PortalExecuteResult {
                result,
                result_formats,
                source_sql,
                suspended: false,
            });
        }
    }

    {
        let portal = state
            .portals
            .get(&portal_name)
            .ok_or_else(|| PgWireError::Protocol(format!("portal {portal_name:?} not found")))?;
        if portal.stream.is_some() {
            return execute_portal_stream_batch(server, state, &portal_name, max_rows);
        }
    }

    let (sql, result_formats, source_sql) = {
        let portal = state
            .portals
            .get(&portal_name)
            .ok_or_else(|| PgWireError::Protocol(format!("portal {portal_name:?} not found")))?;
        (
            portal.sql.clone(),
            portal.result_formats.clone(),
            portal.source_sql.clone(),
        )
    };

    let result = execute_server_sql_with_options(server, state, &sql, !streaming_fetch)?;
    if !streaming_fetch || result.columns.is_empty() {
        return Ok(PortalExecuteResult {
            result,
            result_formats,
            source_sql,
            suspended: false,
        });
    }

    let mut stream = result.into_stream();
    let columns = stream.columns().to_vec();
    let column_types = stream.column_types().to_vec();
    let column_metadata = stream.column_metadata().to_vec();
    let batch = stream.next_batch(max_rows);
    let suspended = !stream.is_done();
    let command_tag = stream.command_complete_tag();
    if suspended {
        let registered_memory = stream.memory_estimate();
        server.register_cursor_memory(registered_memory);
        {
            let portal = state.portals.get_mut(&portal_name).ok_or_else(|| {
                PgWireError::Protocol(format!("portal {portal_name:?} not found"))
            })?;
            portal.stream = Some(stream);
            portal.registered_stream_memory = registered_memory;
        }
        if let Err(error) = enforce_connection_memory(state, &server.config) {
            if let Some(portal) = state.portals.get_mut(&portal_name) {
                portal.stream = None;
                portal.registered_stream_memory = 0;
            }
            server.unregister_cursor_memory(registered_memory);
            return Err(error);
        }
    }

    Ok(PortalExecuteResult {
        result: SqlResult {
            columns,
            rows: batch,
            command_tag: Some(command_tag),
            column_types,
            column_metadata,
        },
        result_formats,
        source_sql,
        suspended,
    })
}

pub(crate) fn execute_portal_stream_batch(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    portal_name: &str,
    max_rows: usize,
) -> Result<PortalExecuteResult> {
    let portal = state
        .portals
        .get_mut(portal_name)
        .ok_or_else(|| PgWireError::Protocol(format!("portal {portal_name:?} not found")))?;
    let stream = portal
        .stream
        .as_mut()
        .ok_or_else(|| PgWireError::Protocol(format!("portal {portal_name:?} has no stream")))?;
    let columns = stream.columns().to_vec();
    let column_types = stream.column_types().to_vec();
    let column_metadata = stream.column_metadata().to_vec();
    let batch = stream.next_batch(max_rows);
    let suspended = !stream.is_done();
    let command_tag = stream.command_complete_tag();
    let result_formats = portal.result_formats.clone();
    let source_sql = portal.source_sql.clone();
    let previous_memory = portal.registered_stream_memory;
    let current_memory = if suspended {
        stream.memory_estimate()
    } else {
        0
    };
    server.adjust_cursor_memory(previous_memory, current_memory);
    portal.registered_stream_memory = current_memory;
    if !suspended {
        portal.stream = None;
        server.active_cursors.fetch_sub(1, Ordering::SeqCst);
    }

    Ok(PortalExecuteResult {
        result: SqlResult {
            columns,
            rows: batch,
            command_tag: Some(command_tag),
            column_types,
            column_metadata,
        },
        result_formats,
        source_sql,
        suspended,
    })
}

pub(crate) fn close_message_with_server(
    payload: &[u8],
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
) -> Result<()> {
    let mut idx = 0;
    let target = *payload
        .get(idx)
        .ok_or_else(|| PgWireError::Protocol("Close message missing target".to_string()))?;
    idx += 1;
    let name = read_cstr(payload, &mut idx)?;
    match target {
        b'S' => {
            state.prepared.remove(&name);
        }
        b'P' => {
            if let Some(portal) = state.portals.remove(&name) {
                if portal.stream.is_some() {
                    server.unregister_cursor_memory(portal.registered_stream_memory);
                }
            }
        }
        other => {
            return Err(PgWireError::Protocol(format!(
                "unknown Close target {}",
                other as char
            )));
        }
    }
    Ok(())
}

pub(crate) fn is_describable_query(sql: &str) -> bool {
    let normalized = normalize_executable_sql(sql);
    normalized.starts_with("select ")
        || normalized == "select"
        || normalized.starts_with("show ")
        || normalized == "show"
        || (normalized.starts_with("with ") && !is_mutating_with_sql(&normalized))
}

pub(crate) fn returning_describe_result(
    server: &Arc<PgWireServer>,
    sql: &str,
) -> Result<Option<SqlResult>> {
    let normalized = sql.trim_start().to_ascii_lowercase();
    if !(normalized.starts_with("insert")
        || normalized.starts_with("update")
        || normalized.starts_with("delete"))
    {
        return Ok(None);
    }
    let Some(returning_idx) = find_top_level_keyword(sql, "returning") else {
        return Ok(None);
    };
    let returning_sql = strip_sql_comments(&sql[returning_idx + "returning".len()..]);
    let returning = returning_sql.trim().trim_end_matches(';').trim();
    let expressions = split_top_level_commas(returning);
    if expressions
        .iter()
        .any(|expression| *expression == "*" || expression.trim_end().ends_with(".*"))
    {
        let Some(table) = dml_target_relation(sql) else {
            return Ok(Some(SqlResult::empty(vec!["?column?".to_string()])));
        };
        let select_list = if expressions.len() == 1 {
            "*".to_string()
        } else {
            expressions.join(", ")
        };
        return execute_server_sql_for_describe(
            server,
            &format!("SELECT {select_list} FROM {table} LIMIT 0"),
        )
        .map(Some);
    }
    let columns = expressions
        .iter()
        .copied()
        .map(returning_column_name)
        .collect::<Vec<_>>();
    if columns.is_empty() {
        Ok(None)
    } else if let Some(table) = dml_target_relation(sql) {
        let select_list = expressions.join(", ");
        match execute_server_sql_for_describe(
            server,
            &format!("SELECT {select_list} FROM {table} LIMIT 0"),
        ) {
            Ok(mut result) if result.columns.len() == columns.len() => {
                result.columns = columns;
                Ok(Some(result))
            }
            _ => {
                let column_types = expressions
                    .iter()
                    .map(|expression| {
                        execute_server_sql_for_describe(
                            server,
                            &format!("SELECT {expression} FROM {table} LIMIT 0"),
                        )
                        .ok()
                        .and_then(|result| result.column_types.into_iter().next().flatten())
                    })
                    .collect::<Vec<_>>();
                Ok(Some(
                    SqlResult::empty(columns).with_column_types(column_types),
                ))
            }
        }
    } else {
        Ok(Some(SqlResult::empty(columns)))
    }
}

pub(crate) fn has_dml_returning(sql: &str) -> bool {
    let normalized = sql.trim_start().to_ascii_lowercase();
    (normalized.starts_with("insert")
        || normalized.starts_with("update")
        || normalized.starts_with("delete"))
        && find_top_level_keyword(sql, "returning").is_some()
}

pub(crate) fn dml_target_relation(sql: &str) -> Option<String> {
    dml_target_relation_from_ast(sql).or_else(|| dml_target_relation_fallback(sql))
}

pub(crate) fn dml_target_relation_from_ast(sql: &str) -> Option<String> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).ok()?;
    if statements.len() != 1 {
        return None;
    }
    match statements.pop()? {
        Statement::Insert(insert) => {
            let TableObject::TableName(name) = insert.table else {
                return None;
            };
            let mut relation = name.to_string();
            if let Some(alias) = insert.table_alias {
                relation.push_str(" AS ");
                relation.push_str(&alias.alias.to_string());
            }
            Some(relation)
        }
        Statement::Update(update) => dml_table_factor_relation(&update.table.relation),
        Statement::Delete(delete) => {
            let tables = match delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            dml_table_factor_relation(&tables.first()?.relation)
        }
        _ => None,
    }
}

pub(crate) fn dml_table_factor_relation(factor: &TableFactor) -> Option<String> {
    let TableFactor::Table {
        name,
        alias,
        args: None,
        ..
    } = factor
    else {
        return None;
    };
    let mut relation = name.to_string();
    if let Some(alias) = alias {
        relation.push_str(" AS ");
        relation.push_str(&alias.name.to_string());
    }
    Some(relation)
}

pub(crate) fn dml_target_relation_fallback(sql: &str) -> Option<String> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    let input = if lower.starts_with("insert into ") {
        &trimmed["insert into ".len()..]
    } else if lower.starts_with("update ") {
        &trimmed["update ".len()..]
    } else if lower.starts_with("delete from ") {
        &trimmed["delete from ".len()..]
    } else {
        return None;
    };
    let name = input
        .trim_start()
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '.' | '"'))
        .collect::<String>();
    (!name.is_empty()).then(|| name.trim_matches('"').to_string())
}

pub(crate) fn returning_column_name(expression: &str) -> String {
    let expression = expression.trim();
    if expression.is_empty() {
        return "?column?".to_string();
    }
    if let Some(as_idx) = find_top_level_keyword(expression, "as") {
        return clean_identifier(&expression[as_idx + 2..]);
    }
    if expression
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '.' | '"'))
    {
        return expression
            .rsplit('.')
            .next()
            .map(clean_identifier)
            .unwrap_or_else(|| "?column?".to_string());
    }
    "?column?".to_string()
}

pub(crate) fn clean_identifier(identifier: &str) -> String {
    identifier
        .split_whitespace()
        .next()
        .unwrap_or("?column?")
        .trim_matches('"')
        .to_string()
}

/// The text without `--` / `/* */` comments (quoted text kept verbatim).
/// Borrowed when the text cannot contain a comment at all — the case for
/// every benchmark-shaped statement — so the callers that normalize each
/// query pay one scan and no copy.
pub(crate) fn strip_sql_comments(sql: &str) -> std::borrow::Cow<'_, str> {
    let bytes = sql.as_bytes();
    if !bytes.iter().any(|byte| *byte == b'-' || *byte == b'/') {
        return std::borrow::Cow::Borrowed(sql);
    }
    std::borrow::Cow::Owned(strip_sql_comments_owned(sql))
}

fn strip_sql_comments_owned(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut stripped = String::with_capacity(sql.len());
    // Copy runs of kept bytes as slices: comment delimiters are ASCII, so
    // every cut lands on a char boundary and non-ASCII text survives intact.
    let mut idx = 0usize;
    let mut run_start = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if in_single {
            if byte == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 1;
                } else {
                    in_single = false;
                }
            }
            idx += 1;
            continue;
        }
        if in_double {
            if byte == b'"' {
                in_double = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => {
                in_single = true;
                idx += 1;
            }
            b'"' => {
                in_double = true;
                idx += 1;
            }
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                stripped.push_str(&sql[run_start..idx]);
                idx += 2;
                while idx < bytes.len() && bytes[idx] != b'\n' {
                    idx += 1;
                }
                run_start = idx;
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                stripped.push_str(&sql[run_start..idx]);
                idx += 2;
                while idx + 1 < bytes.len() && !(bytes[idx] == b'*' && bytes[idx + 1] == b'/') {
                    idx += 1;
                }
                idx = (idx + 2).min(bytes.len());
                run_start = idx;
            }
            _ => idx += 1,
        }
    }
    stripped.push_str(&sql[run_start..]);
    stripped
}

pub(crate) fn find_top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let keyword = keyword.as_bytes();
    let mut idx = 0usize;
    let mut paren_depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if in_single {
            if byte == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                in_single = false;
            }
            idx += 1;
            continue;
        }
        if in_double {
            if byte == b'"' {
                in_double = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => {
                in_single = true;
                idx += 1;
                continue;
            }
            b'"' => {
                in_double = true;
                idx += 1;
                continue;
            }
            b'(' => {
                paren_depth += 1;
                idx += 1;
                continue;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                idx += 1;
                continue;
            }
            _ => {}
        }
        if paren_depth == 0 && keyword_matches_at(bytes, keyword, idx) {
            return Some(idx);
        }
        idx += 1;
    }
    None
}

pub(crate) fn keyword_matches_at(bytes: &[u8], keyword: &[u8], idx: usize) -> bool {
    let end = idx.saturating_add(keyword.len());
    if end > bytes.len() {
        return false;
    }
    let before_ok = idx == 0 || !is_identifier_byte(bytes[idx - 1]);
    let after_ok = end == bytes.len() || !is_identifier_byte(bytes[end]);
    before_ok
        && after_ok
        && bytes[idx..end]
            .iter()
            .zip(keyword.iter())
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

pub(crate) fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
}

pub(crate) fn placeholder_cast_describe_result(sql: &str) -> Option<SqlResult> {
    if !is_describable_query(sql) || !sql.contains('$') {
        return None;
    }
    let column_count = infer_select_cast_oids(sql, usize::MAX)?.len();
    if column_count == 0 {
        return None;
    }
    Some(SqlResult::empty(vec!["?column?".to_string(); column_count]))
}

pub(crate) fn resolve_parameter_type_oids(
    db: &BicDb,
    supplied: Vec<i32>,
    inferred: Vec<Option<String>>,
) -> Result<Vec<i32>> {
    let max_len = supplied.len().max(inferred.len());
    let mut resolved = Vec::with_capacity(max_len);
    for idx in 0..max_len {
        let supplied_oid = supplied.get(idx).copied().filter(|oid| *oid != 0);
        let inferred_oid = match inferred.get(idx).and_then(Option::as_deref) {
            Some(type_name) => oid_for_type_name_with_db(db, type_name)?,
            None => None,
        };
        resolved.push(supplied_oid.or(inferred_oid).unwrap_or(25));
    }
    Ok(resolved)
}

pub(crate) fn decode_parameter_with_db(
    db: &BicDb,
    bytes: &[u8],
    format: i16,
    oid: i32,
) -> Result<String> {
    match format {
        0 => {
            let value = std::str::from_utf8(bytes)
                .map_err(|error| PgWireError::Protocol(error.to_string()))?;
            sql_literal_for_text_parameter_with_db(db, value, oid)
        }
        1 => decode_binary_parameter_with_db(db, bytes, oid),
        other => Err(PgWireError::Protocol(format!(
            "unsupported parameter format code {other}"
        ))),
    }
}

pub(crate) fn decode_binary_parameter_with_db(
    db: &BicDb,
    bytes: &[u8],
    oid: i32,
) -> Result<String> {
    if matches!(oid, 22 | 30) {
        return decode_binary_catalog_vector_parameter(bytes, oid, Some(db));
    }
    if let Some(element_oid) = array_element_oid_with_db(db, oid)? {
        return decode_binary_array_parameter_inner(bytes, element_oid, Some(db));
    }
    if let Some(info) = bicdb_sql::pg_user_range_type_info(db, oid)? {
        return if info.multirange {
            decode_binary_multirange_parameter_inner(
                bytes,
                info.multirange_oid,
                info.range_oid,
                info.subtype_oid,
                Some(db),
            )
            .map(|value| quote_sql_string(&value))
        } else {
            decode_binary_range_parameter_inner(bytes, info.range_oid, info.subtype_oid, Some(db))
                .map(|value| quote_sql_string(&value))
        };
    }
    if let Some(base_oid) = bicdb_sql::pg_user_type_binary_base_oid(db, oid)? {
        return decode_binary_parameter_with_db(db, bytes, base_oid);
    }
    if oid == 2249 || bicdb_sql::pg_is_table_row_type_oid(db, oid)? {
        return decode_binary_composite_parameter(db, bytes, oid);
    }
    if bicdb_sql::pg_is_user_type_oid(db, oid)? {
        return std::str::from_utf8(bytes)
            .map(quote_sql_string)
            .map_err(|error| PgWireError::Protocol(error.to_string()));
    }
    decode_binary_parameter(bytes, oid)
}

pub(crate) fn decode_binary_parameter(bytes: &[u8], oid: i32) -> Result<String> {
    if matches!(oid, 22 | 30) {
        return decode_binary_catalog_vector_parameter(bytes, oid, None);
    }
    if let Some(element_oid) = array_element_oid(oid) {
        return decode_binary_array_parameter(bytes, element_oid);
    }
    if bicdb_sql::pg_type_spec_by_oid(oid).and_then(|spec| spec.binary_codec())
        == Some(PgBinaryCodec::TextPayload)
    {
        return decode_binary_text_parameter(bytes);
    }

    match oid {
        16 => {
            expect_binary_len(bytes, oid, 1)?;
            Ok((bytes[0] != 0).to_string())
        }
        17 => Ok(quote_sql_string(&format!("\\x{}", hex::encode(bytes)))),
        18 => {
            expect_binary_len(bytes, oid, 1)?;
            let value = i8::from_ne_bytes([bytes[0]]);
            Ok(if value < 0 {
                format!("({value})")
            } else {
                value.to_string()
            })
        }
        20 => {
            expect_binary_len(bytes, oid, 8)?;
            Ok(i64::from_be_bytes(bytes.try_into().unwrap()).to_string())
        }
        21 => {
            expect_binary_len(bytes, oid, 2)?;
            Ok(i16::from_be_bytes(bytes.try_into().unwrap()).to_string())
        }
        23 => {
            expect_binary_len(bytes, oid, 4)?;
            Ok(i32::from_be_bytes(bytes.try_into().unwrap()).to_string())
        }
        27 => {
            expect_binary_len(bytes, oid, 6)?;
            Ok(quote_sql_string(
                &bicdb_sql::PgTupleId {
                    block: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
                    offset: u16::from_be_bytes(bytes[4..6].try_into().unwrap()),
                }
                .to_postgres_text(),
            ))
        }
        28 | 29 => {
            expect_binary_len(bytes, oid, 4)?;
            Ok(quote_sql_string(
                &u32::from_be_bytes(bytes.try_into().unwrap()).to_string(),
            ))
        }
        5069 => {
            expect_binary_len(bytes, oid, 8)?;
            Ok(quote_sql_string(
                &u64::from_be_bytes(bytes.try_into().unwrap()).to_string(),
            ))
        }
        790 => {
            expect_binary_len(bytes, oid, 8)?;
            Ok(quote_sql_string(&bicdb_sql::pg_money_text_from_cents(
                i64::from_be_bytes(bytes.try_into().unwrap()),
            )))
        }
        1700 => decode_binary_numeric_parameter(bytes).map(|value| quote_sql_string(&value)),
        114 => decode_binary_text_parameter(bytes),
        600 | 601 | 602 | 603 | 604 | 628 | 718 => decode_binary_geometric_parameter(bytes, oid),
        650 | 869 => decode_binary_network_parameter(bytes, oid),
        774 | 829 => decode_binary_mac_parameter(bytes, oid),
        700 | 701 => Ok(sql_float_parameter_literal(
            &decode_binary_float_parameter_text(bytes, oid)?,
            oid,
        )),
        1082 => {
            expect_binary_len(bytes, oid, 4)?;
            let days = i32::from_be_bytes(bytes.try_into().unwrap());
            let date = match days {
                i32::MAX => PgDate::PositiveInfinity,
                i32::MIN => PgDate::NegativeInfinity,
                days => PgDate::from_epoch_days(i64::from(days))
                    .map_err(|error| temporal_binary_error(oid, error))?,
            };
            Ok(quote_sql_string(&date.to_iso_text()))
        }
        1083 => {
            expect_binary_len(bytes, oid, 8)?;
            let time =
                PgTime::from_micros_since_midnight(i64::from_be_bytes(bytes.try_into().unwrap()))
                    .map_err(|error| temporal_binary_error(oid, error))?;
            Ok(quote_sql_string(&time.to_iso_text()))
        }
        1114 | 1184 => {
            expect_binary_len(bytes, oid, 8)?;
            let micros = i64::from_be_bytes(bytes.try_into().unwrap());
            let timestamp = match micros {
                i64::MAX => PgTimestamp::PositiveInfinity,
                i64::MIN => PgTimestamp::NegativeInfinity,
                micros => PgTimestamp::Finite(micros),
            };
            Ok(quote_sql_string(&timestamp.to_iso_text(oid == 1184)))
        }
        1186 => {
            expect_binary_len(bytes, oid, 16)?;
            let interval = PgInterval {
                micros: i64::from_be_bytes(bytes[0..8].try_into().unwrap()),
                days: i32::from_be_bytes(bytes[8..12].try_into().unwrap()),
                months: i32::from_be_bytes(bytes[12..16].try_into().unwrap()),
            };
            Ok(quote_sql_string(&interval.to_postgres_text()))
        }
        1266 => {
            expect_binary_len(bytes, oid, 12)?;
            let time = PgTime::from_micros_since_midnight(i64::from_be_bytes(
                bytes[0..8].try_into().unwrap(),
            ))
            .map_err(|error| temporal_binary_error(oid, error))?;
            let seconds_west = i32::from_be_bytes(bytes[8..12].try_into().unwrap());
            let offset = seconds_west
                .checked_neg()
                .ok_or_else(|| PgWireError::Protocol("timetz zone is out of range".to_string()))?;
            let timetz =
                PgTimeTz::new(time, offset).map_err(|error| temporal_binary_error(oid, error))?;
            Ok(quote_sql_string(&timetz.to_iso_text()))
        }
        1560 | 1562 => decode_binary_bit_parameter(bytes, oid),
        2950 => {
            expect_binary_len(bytes, oid, 16)?;
            Ok(quote_sql_string(
                &Uuid::from_bytes(bytes.try_into().unwrap()).to_string(),
            ))
        }
        3802 => {
            let (version, json) = bytes.split_first().ok_or_else(|| {
                PgWireError::Protocol("jsonb binary parameter is missing version byte".to_string())
            })?;
            if *version != 1 {
                return Err(PgWireError::Protocol(format!(
                    "unsupported jsonb binary version {version}"
                )));
            }
            let value = std::str::from_utf8(json)
                .map_err(|error| PgWireError::Protocol(error.to_string()))?;
            Ok(quote_sql_string(value))
        }
        4072 => decode_versioned_text_parameter(bytes, oid, "jsonpath"),
        3220 => {
            expect_binary_len(bytes, oid, 8)?;
            Ok(quote_sql_string(&format_pg_lsn(u64::from_be_bytes(
                bytes.try_into().unwrap(),
            ))))
        }
        3904 | 3906 | 3908 | 3910 | 3912 | 3926 => {
            decode_binary_range_parameter(bytes, oid).map(|value| quote_sql_string(&value))
        }
        4451 | 4532 | 4533 | 4534 | 4535 | 4536 => {
            decode_binary_multirange_parameter(bytes, oid).map(|value| quote_sql_string(&value))
        }
        2970 | 5038 => decode_binary_snapshot_parameter(bytes, oid),
        3614 => PgTsVector::from_postgres_binary(bytes)
            .map(|value| quote_sql_string(&value.to_postgres_text()))
            .map_err(|error| PgWireError::Protocol(error.to_string())),
        3615 => PgTsQuery::from_postgres_binary(bytes)
            .map(|value| quote_sql_string(&value.to_postgres_text()))
            .map_err(|error| PgWireError::Protocol(error.to_string())),
        380_200 => decode_binary_vector_parameter(bytes),
        oid if is_oid_alias_oid(oid) => {
            expect_binary_len(bytes, oid, 4)?;
            Ok(u32::from_be_bytes(bytes.try_into().unwrap()).to_string())
        }
        _ => Err(PgWireError::Protocol(format!(
            "unsupported binary parameter oid {oid}"
        ))),
    }
}

#[cfg(test)]
mod advisory_lock_parse_tests {
    use super::*;

    #[test]
    fn advisory_names_in_anonymous_blocks_are_not_top_level_lock_calls() {
        for sql in [
            "DO $q$ BEGIN PERFORM pg_advisory_xact_lock(1); END $q$;",
            "/* provisioning */ DO $q$ DECLARE r RECORD; BEGIN FOR r IN SELECT 'pg_advisory_xact_lock' AS name LOOP NULL; END LOOP; END $q$;",
            "CREATE FUNCTION sample() RETURNS void AS $$ BEGIN PERFORM pg_advisory_unlock(1); END $$ LANGUAGE plpgsql",
        ] {
            assert!(parse_advisory_lock_call(sql).unwrap().is_none(), "{sql}");
        }
    }

    #[test]
    fn dollar_quoted_advisory_unlock_does_not_parse_the_migration_batch_as_a_lock_call() {
        let sql = r#"
            CREATE EXTENSION IF NOT EXISTS pgcrypto;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public
              REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
            CREATE OR REPLACE FUNCTION carrier_private.clear_context()
            RETURNS VOID
            LANGUAGE plpgsql
            AS $carrier_clear$
            BEGIN
              PERFORM pg_advisory_unlock(1, 2);
            END
            $carrier_clear$;
        "#;

        assert!(parse_advisory_lock_call(sql).unwrap().is_none());
    }
}

#[cfg(test)]
mod query_text_fast_path_tests {
    use super::*;

    /// The previous comment stripper (byte-at-a-time), kept as the oracle.
    fn strip_sql_comments_reference(sql: &str) -> String {
        let bytes = sql.as_bytes();
        let mut stripped = Vec::with_capacity(sql.len());
        let mut idx = 0usize;
        let (mut in_single, mut in_double) = (false, false);
        while idx < bytes.len() {
            let byte = bytes[idx];
            if in_single {
                stripped.push(byte);
                if byte == b'\'' {
                    if bytes.get(idx + 1) == Some(&b'\'') {
                        idx += 1;
                        stripped.push(bytes[idx]);
                    } else {
                        in_single = false;
                    }
                }
                idx += 1;
                continue;
            }
            if in_double {
                stripped.push(byte);
                if byte == b'"' {
                    in_double = false;
                }
                idx += 1;
                continue;
            }
            match byte {
                b'\'' => {
                    in_single = true;
                    stripped.push(byte);
                    idx += 1;
                }
                b'"' => {
                    in_double = true;
                    stripped.push(byte);
                    idx += 1;
                }
                b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                    idx += 2;
                    while idx < bytes.len() && bytes[idx] != b'\n' {
                        idx += 1;
                    }
                }
                b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                    idx += 2;
                    while idx + 1 < bytes.len() && !(bytes[idx] == b'*' && bytes[idx + 1] == b'/') {
                        idx += 1;
                    }
                    idx = (idx + 2).min(bytes.len());
                }
                _ => {
                    stripped.push(byte);
                    idx += 1;
                }
            }
        }
        String::from_utf8(stripped).unwrap()
    }

    fn normalize_sql_reference(sql: &str) -> String {
        sql.trim()
            .trim_end_matches(';')
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase()
    }

    const SAMPLES: &[&str] = &[
        "call neword(6,16,7,89,6,0.0,'','',0.0,0.0,0,TO_TIMESTAMP('20260903180000','YYYYMMDDHH24MISS')::timestamp without time zone)",
        "SELECT 1",
        "  SELECT  a,\tb\nFROM t ;;  ",
        "select 'it''s -- not a comment' -- but this is\nfrom t",
        "select /* block */ 1 /* unterminated",
        "select \"Quoted -- Name\" from t /**/",
        "select 'ünïcödé' -- ok\n, 'naïve /* x */'",
        "-- only a comment",
        "/* only */ ;",
        "",
        "   ;  \n ",
        "select -1, 2/3",
        "insert into t values ('a--b', 'c/*d*/e')",
    ];

    #[test]
    fn comment_stripping_matches_the_reference_and_borrows_when_it_can() {
        for sql in SAMPLES {
            let stripped = strip_sql_comments(sql);
            assert_eq!(&*stripped, strip_sql_comments_reference(sql), "{sql:?}");
            let can_borrow = !sql.contains('-') && !sql.contains('/');
            assert_eq!(
                matches!(stripped, std::borrow::Cow::Borrowed(_)),
                can_borrow,
                "{sql:?}"
            );
        }
    }

    #[test]
    fn normalization_matches_the_reference() {
        for sql in SAMPLES {
            assert_eq!(normalize_sql(sql), normalize_sql_reference(sql), "{sql:?}");
            assert_eq!(
                normalize_executable_sql(sql),
                normalize_sql_reference(&strip_sql_comments_reference(sql)),
                "{sql:?}"
            );
        }
    }

    #[test]
    fn blank_text_detection_matches_statement_splitting() {
        for sql in SAMPLES {
            assert_eq!(
                sql_is_blank(sql),
                split_sql_statements(sql).is_empty(),
                "{sql:?}"
            );
        }
    }

    #[test]
    fn savepoint_and_advisory_probes_keep_their_answers() {
        assert!(matches!(
            parse_savepoint_command("savepoint sp1"),
            Some(SavepointCommand::Create(name)) if name == "sp1"
        ));
        assert!(matches!(
            parse_savepoint_command("rollback to savepoint sp1"),
            Some(SavepointCommand::RollbackTo(name)) if name == "sp1"
        ));
        assert!(matches!(
            parse_savepoint_command("release savepoint sp1"),
            Some(SavepointCommand::Release(name)) if name == "sp1"
        ));
        assert!(parse_savepoint_command("rollback").is_none());
        assert!(parse_savepoint_command("select 1").is_none());
        assert!(contains_advisory_lock_function(
            "SELECT PG_ADVISORY_LOCK(1)"
        ));
        assert!(contains_advisory_lock_function(
            "select pg_try_Advisory_xact_lock(1)"
        ));
        assert!(!contains_advisory_lock_function(
            "select advisory_board from t"
        ));
        assert!(!contains_advisory_lock_function("call neword(1,2,3)"));
    }
}
