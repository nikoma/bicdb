//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn maybe_log_slow_query(
    server: &PgWireServer,
    sql: &str,
    rows: usize,
    elapsed: Duration,
) {
    let Some(path) = &server.config.slow_query_log else {
        return;
    };
    if elapsed < server.config.slow_query_threshold {
        return;
    }
    let entry = SlowQueryLogEntry::new(
        elapsed.as_millis() as u64,
        rows,
        sql,
        &[],
        &server.config.slow_query_redaction,
    );
    if let Err(error) = append_slow_query_log(path, &entry) {
        eprintln!("bicdb slow query log error: {error}");
    }
}

pub(crate) fn proc_mix_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_PROC_MIX_TRACE")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

pub(crate) fn pgwire_query_queue_wait_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_QUERY_QUEUE_WAIT_TRACE")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

pub(crate) fn record_proc_mix_if_enabled(server: &PgWireServer, normalized: &str) {
    if !proc_mix_trace_enabled() {
        return;
    }
    let Some(name) = top_level_proc_name(normalized) else {
        return;
    };
    match name {
        "neword" => {
            server.proc_neword.fetch_add(1, Ordering::Relaxed);
        }
        "payment" => {
            server.proc_payment.fetch_add(1, Ordering::Relaxed);
        }
        "delivery" => {
            server.proc_delivery.fetch_add(1, Ordering::Relaxed);
        }
        "ostat" => {
            server.proc_orderstatus.fetch_add(1, Ordering::Relaxed);
        }
        "slev" => {
            server.proc_stocklevel.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

pub(crate) fn top_level_proc_name(normalized: &str) -> Option<&str> {
    let rest = normalized
        .strip_prefix("call ")
        .or_else(|| normalized.strip_prefix("select "))?
        .trim_start();
    let name_end = rest
        .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'))
        .unwrap_or(rest.len());
    let name = rest[..name_end]
        .rsplit('.')
        .next()
        .unwrap_or(&rest[..name_end]);
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

pub(crate) fn log_failed_query(
    server: &PgWireServer,
    state: &ConnectionState,
    sql: Option<&str>,
    error: &PgWireError,
) {
    log_operational_event(
        "query.failed",
        "warn",
        json!({
            "connection_id": state.connection_id,
            "sqlstate": pgwire_error_sqlstate(error),
            "error": error.to_string(),
            "query": sql
                .map(|sql| redact_query_text(sql, &server.config.slow_query_redaction))
                .unwrap_or_else(|| "<unknown>".to_string()),
        }),
    );
}

pub(crate) fn set_active_query(server: &PgWireServer, state: &mut ConnectionState, sql: &str) {
    state.active_query = Some(redact_query_text(sql, &server.config.slow_query_redaction));
    server
        .connections
        .lock()
        .unwrap()
        .insert(state.connection_id, state.snapshot());
}

pub(crate) fn clear_active_query(server: &PgWireServer, state: &mut ConnectionState) {
    state.active_query = None;
    server
        .connections
        .lock()
        .unwrap()
        .insert(state.connection_id, state.snapshot());
}

pub(crate) fn pgwire_error_sqlstate(error: &PgWireError) -> String {
    match error {
        PgWireError::Sql(error) => error.sqlstate().to_string(),
        PgWireError::Authentication => "28P01".to_string(),
        PgWireError::DatabaseNotFound(_) => "3D000".to_string(),
        PgWireError::DatabaseAlreadyExists(_) => "42P04".to_string(),
        PgWireError::Protocol(_) => "08P01".to_string(),
        PgWireError::PersistentConnectionMemoryLimit(_) => "53200".to_string(),
        PgWireError::QueryRejected(_) => "53300".to_string(),
        PgWireError::QueryCanceled | PgWireError::QueryTimedOut => "57014".to_string(),
        PgWireError::InFailedTransaction => "25P02".to_string(),
        PgWireError::BicDb(error) => bicdb_error_sqlstate(error).to_string(),
        _ => "XX000".to_string(),
    }
}

pub(crate) fn log_operational_event(event: &str, severity: &str, fields: serde_json::Value) {
    match operational_event_json(event, severity, fields) {
        Ok(line) => eprintln!("{line}"),
        Err(error) => eprintln!(
            "{{\"event\":\"observability.log_error\",\"severity\":\"error\",\"error\":\"{}\"}}",
            error
        ),
    }
}

pub(crate) fn classify_query_interrupt(
    server: &Arc<PgWireServer>,
    state: &ConnectionState,
    error: PgWireError,
) -> Result<SqlResult> {
    match error {
        PgWireError::Sql(SqlError::BicDb(BicDbError::QueryCanceled))
        | PgWireError::BicDb(BicDbError::QueryCanceled)
        | PgWireError::QueryCanceled => {
            server.canceled_queries.fetch_add(1, Ordering::SeqCst);
            state.take_cancel_request();
            server.record_cancel_metadata(state.connection_id, "cancel", "57014");
            Err(PgWireError::QueryCanceled)
        }
        PgWireError::Sql(SqlError::BicDb(BicDbError::QueryTimedOut))
        | PgWireError::BicDb(BicDbError::QueryTimedOut)
        | PgWireError::QueryTimedOut => {
            server.timed_out_queries.fetch_add(1, Ordering::SeqCst);
            state.take_cancel_request();
            server.record_cancel_metadata(state.connection_id, "timeout", "57014");
            Err(PgWireError::QueryTimedOut)
        }
        other => Err(other),
    }
}

pub(crate) fn classify_bicdb_interrupt(
    server: &Arc<PgWireServer>,
    state: &ConnectionState,
    error: BicDbError,
) -> PgWireError {
    match error {
        BicDbError::QueryCanceled => {
            server.canceled_queries.fetch_add(1, Ordering::SeqCst);
            state.take_cancel_request();
            server.record_cancel_metadata(state.connection_id, "cancel", "57014");
            PgWireError::QueryCanceled
        }
        BicDbError::QueryTimedOut => {
            server.timed_out_queries.fetch_add(1, Ordering::SeqCst);
            state.take_cancel_request();
            server.record_cancel_metadata(state.connection_id, "timeout", "57014");
            PgWireError::QueryTimedOut
        }
        other => PgWireError::BicDb(other),
    }
}

pub(crate) fn execute_server_virtual_query_cancellable(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    sql: &str,
    normalized: &str,
    cancellation: &CancellationToken,
) -> Result<Option<SqlResult>> {
    let mut arguments = Vec::new();
    if parse_advisory_lock_call_with(sql, |expr| {
        arguments.push(expr.clone());
        Ok(None)
    })?
    .is_some()
    {
        // Evaluate all keys once, together, in the caller's transaction. Even
        // outside BEGIN, a write-capable key function and lock acquisition
        // belong to one implicit transaction; an argument error rolls it back.
        let owns_transaction = !state.in_transaction;
        if owns_transaction {
            begin_buffered_transaction(server, state)?;
        }
        let result = (|| {
            let values = if arguments.is_empty() {
                Vec::new()
            } else {
                let query = format!(
                    "SELECT {}",
                    arguments
                        .iter()
                        .map(|expr| format!("({expr})::bigint"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let normalized = normalize_executable_sql(&query);
                let result = execute_server_transaction_sql(
                    server,
                    state,
                    &query,
                    &normalized,
                    cancellation,
                )?;
                let [row] = result.rows.as_slice() else {
                    return Err(
                        SqlError::InvalidSql("advisory lock keys must be scalar".into()).into(),
                    );
                };
                row.clone()
            };
            let mut values = values.into_iter();
            let call = parse_advisory_lock_call_with(sql, |_| match values.next() {
                Some(SqlValue::Null) => Ok(None),
                Some(SqlValue::Int(value)) => Ok(Some(value)),
                _ => {
                    Err(SqlError::InvalidSql("advisory lock key must be an integer".into()).into())
                }
            })?
            .expect("same parsed advisory statement");
            execute_advisory_lock_call(server, state, call, cancellation)
        })();
        if owns_transaction {
            match result {
                Ok(result) => {
                    if let Err(error) = commit_buffered_transaction(server, state, cancellation) {
                        rollback_open_transaction(server, state)?;
                        return Err(error);
                    }
                    return Ok(Some(result));
                }
                Err(error) => {
                    rollback_open_transaction(server, state)?;
                    return Err(error);
                }
            }
        }
        return result.map(Some);
    }
    if let Some(duration) = parse_pg_sleep_normalized(normalized) {
        let started = Instant::now();
        while started.elapsed() < duration {
            match cancellation.check() {
                Ok(()) => {}
                // Return the interrupt and let the single classification site
                // account for it. Counting here as WELL as there charged one
                // canceled query to the meter twice, which is how the
                // certification saw two cancellations against one failed
                // query.
                Err(error) => return Err(PgWireError::BicDb(error)),
            }
            let remaining = duration.saturating_sub(started.elapsed());
            thread::sleep(remaining.min(Duration::from_millis(10)));
        }
        return Ok(Some(SqlResult::new(
            vec!["pg_sleep".to_string()],
            vec![vec![SqlValue::Null]],
        )));
    }
    execute_server_virtual_query_for_normalized(
        server,
        sql,
        normalized,
        Some(VirtualQueryCaller {
            connection_id: state.connection_id,
            user: state.user.as_str(),
        }),
    )
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum AdvisoryLockOperation {
    Acquire {
        mode: AdvisoryLockMode,
        scope: AdvisoryLockScope,
        try_only: bool,
    },
    Unlock {
        mode: AdvisoryLockMode,
    },
    UnlockAll,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AdvisoryLockCall {
    pub(crate) operation: AdvisoryLockOperation,
    pub(crate) key: Option<AdvisoryLockKey>,
    pub(crate) result_name: &'static str,
}

impl AdvisoryLockCall {
    pub(crate) fn result_type(self) -> &'static str {
        match self.operation {
            AdvisoryLockOperation::Acquire { try_only: true, .. }
            | AdvisoryLockOperation::Unlock { .. } => "bool",
            AdvisoryLockOperation::Acquire {
                try_only: false, ..
            }
            | AdvisoryLockOperation::UnlockAll => "void",
        }
    }
}

pub(crate) fn execute_advisory_lock_call(
    server: &PgWireServer,
    state: &ConnectionState,
    call: AdvisoryLockCall,
    cancellation: &CancellationToken,
) -> Result<SqlResult> {
    let value = match (call.operation, call.key) {
        (AdvisoryLockOperation::UnlockAll, _) => {
            server
                .advisory_locks
                .unlock_all_session(state.connection_id);
            SqlValue::Null
        }
        (_, None) => SqlValue::Null,
        (
            AdvisoryLockOperation::Acquire {
                mode,
                scope,
                try_only,
            },
            Some(key),
        ) => {
            let acquired = server.advisory_locks.acquire(
                state.connection_id,
                key,
                mode,
                scope,
                try_only,
                cancellation,
            )?;
            if scope == AdvisoryLockScope::Transaction && !state.in_transaction {
                server
                    .advisory_locks
                    .release_transaction(state.connection_id);
            }
            if try_only {
                SqlValue::Bool(acquired)
            } else {
                SqlValue::Null
            }
        }
        (AdvisoryLockOperation::Unlock { mode }, Some(key)) => {
            SqlValue::Bool(server.advisory_locks.unlock(state.connection_id, key, mode))
        }
    };
    Ok(
        SqlResult::new(vec![call.result_name.to_string()], vec![vec![value]])
            .with_column_types(vec![Some(call.result_type().to_string())]),
    )
}
