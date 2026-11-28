//! Server side of two-phase commit. The connection's open transaction is
//! prepared, or a prepared transaction is finished, through the core; see
//! docs/postgresql-two-phase-commit.md.
use super::*;
use bicdb_sql::two_phase::{cannot_prepare, two_phase_identity, TwoPhaseCommand};

fn requester_identity(
    server: &PgWireServer,
    state: &ConnectionState,
) -> Result<(String, bool, String)> {
    let db = server.read_db()?;
    Ok(two_phase_identity(&db, state.session_state.session_gucs())?)
}

/// Why this connection's transaction cannot be prepared, if it cannot.
fn prepare_refusal(server: &PgWireServer, state: &ConnectionState) -> Option<SqlError> {
    if !state.tx_notifications.is_empty() || state.tx_listen_changed {
        return Some(cannot_prepare("has executed LISTEN, UNLISTEN, or NOTIFY"));
    }
    if state.session_state.has_transaction_hold_cursor() {
        return Some(cannot_prepare("has created a cursor WITH HOLD"));
    }
    if state.tx.is_none()
        || !state.tx_statements.is_empty()
        || !state.tx_ddl_undo.is_empty()
        || !state.tx_shared_role_ddl.is_empty()
    {
        return Some(cannot_prepare(
            "has changed the catalog (two-phase commit of DDL is not supported)",
        ));
    }
    let advisory = server.advisory_locks.state.lock().unwrap();
    let mut transaction_level = false;
    for entry in advisory.locks.values() {
        for holders in [&entry.exclusive, &entry.shared] {
            if let Some(counts) = holders.get(&state.connection_id) {
                if counts.transaction > 0 {
                    transaction_level = true;
                }
            }
        }
        let mine = [&entry.exclusive, &entry.shared]
            .into_iter()
            .filter_map(|holders| holders.get(&state.connection_id))
            .fold((0, 0), |(session, transaction), counts| {
                (session + counts.session, transaction + counts.transaction)
            });
        if mine.0 > 0 && mine.1 > 0 {
            return Some(SqlError::DataException {
                sqlstate: "0A000",
                message: "cannot PREPARE while holding both session-level and transaction-level locks on the same object".into(),
                data_type: None,
            });
        }
    }
    if transaction_level {
        return Some(cannot_prepare(
            "holds transaction-level advisory locks (not supported)",
        ));
    }
    None
}

fn end_failed_prepare(server: &Arc<PgWireServer>, state: &mut ConnectionState) {
    let _ = rollback_open_transaction(server, state);
}

pub(crate) fn execute(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    command: TwoPhaseCommand,
    cancellation: &CancellationToken,
) -> Result<SqlResult> {
    match command {
        TwoPhaseCommand::Prepare(gid) => prepare(server, state, &gid, cancellation),
        other if state.in_transaction => Err(SqlError::DataException {
            sqlstate: "25001",
            message: format!(
                "{} cannot run inside a transaction block",
                other.command_tag()
            ),
            data_type: None,
        }
        .into()),
        other => finish(server, state, other),
    }
}

fn prepare(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    gid: &str,
    cancellation: &CancellationToken,
) -> Result<SqlResult> {
    if !state.in_transaction {
        let mut result = SqlResult::command("ROLLBACK");
        result.notices.push(no_explicit_transaction_warning());
        return Ok(result);
    }
    if state.failed_transaction {
        // PostgreSQL: PREPARE TRANSACTION in an aborted block rolls it back.
        rollback_open_transaction(server, state)?;
        return Ok(SqlResult::command("ROLLBACK"));
    }
    if let Some(refusal) = prepare_refusal(server, state) {
        end_failed_prepare(server, state);
        return Err(refusal.into());
    }
    let identity = match requester_identity(server, state) {
        Ok(identity) => identity,
        Err(error) => {
            end_failed_prepare(server, state);
            return Err(error);
        }
    };
    let tx = state.tx.take().expect("checked open transaction");
    // Deferred constraints are checked at PREPARE, as at COMMIT.
    let mut tx = match drain_deferred_trigger_hooks(server, state, cancellation, tx) {
        Ok(tx) => tx,
        Err(error) => {
            end_failed_prepare(server, state);
            return Err(error);
        }
    };
    let prepared = (|| {
        let _admission = server.try_admit_write()?;
        let db = server.read_db_for_transaction_progress()?;
        Ok::<_, PgWireError>(db.prepare_transaction(&mut tx, gid, &identity.0, &identity.2)?)
    })();
    match prepared {
        Ok(sequence) => {
            let implicit_block = state.implicit_simple_transaction;
            drop(tx);
            state.session_state.commit_transaction();
            state.tx_shared_role_ddl.clear();
            clear_open_transaction_state(server, state);
            state.last_commit_seq = state.last_commit_seq.max(sequence);
            server.sync_quorum.wait_for(sequence)?;
            let mut result = SqlResult::command("PREPARE TRANSACTION");
            if implicit_block {
                result.notices.push(no_explicit_transaction_warning());
            }
            Ok(result)
        }
        Err(error) => {
            let _ = tx.rollback();
            end_failed_prepare(server, state);
            Err(error)
        }
    }
}

fn finish(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    command: TwoPhaseCommand,
) -> Result<SqlResult> {
    ensure_server_writable(server)?;
    let (user, superuser, database) = requester_identity(server, state)?;
    let requester = bicdb_core::PreparedTransactionRequester {
        user: &user,
        superuser,
        database: &database,
    };
    let sequence = {
        let _admission = server.try_admit_write()?;
        let db = server.read_db_for_transaction_progress()?;
        match &command {
            TwoPhaseCommand::CommitPrepared(gid) => db.commit_prepared(gid, requester)?,
            TwoPhaseCommand::RollbackPrepared(gid) => db.rollback_prepared(gid, requester)?,
            TwoPhaseCommand::Prepare(_) => unreachable!("handled by prepare"),
        }
    };
    state.last_commit_seq = state.last_commit_seq.max(sequence);
    server.sync_quorum.wait_for(sequence)?;
    Ok(SqlResult::command(command.command_tag()))
}
