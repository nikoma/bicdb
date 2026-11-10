//! PostgreSQL FunctionCall ('F') / FunctionCallResponse ('V').
//! https://www.postgresql.org/docs/16/protocol-message-formats.html
//! All execution goes through the normal admitted SQL statement path.
use super::*;

fn protocol(message: &str) -> PgWireError {
    PgWireError::Protocol(message.into())
}

fn execute(
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let mut idx = 0;
    let oid = read_i32(payload, &mut idx)? as u32;
    let count = read_nonnegative_i16_count(payload, &mut idx, "FunctionCall argument format")?;
    let mut formats = Vec::with_capacity(count);
    for _ in 0..count {
        formats.push(read_i16(payload, &mut idx)?);
    }
    validate_format_codes(&formats, "FunctionCall argument")?;
    let count = read_nonnegative_i16_count(payload, &mut idx, "FunctionCall argument")?;
    validate_format_arity(&formats, count, "FunctionCall argument")?;
    if state.failed_transaction {
        return Err(PgWireError::InFailedTransaction);
    }
    let db = server.read_db_for_transaction_progress()?;
    let signature = bicdb_sql::postgres_function_call_signature(&db, oid)?.ok_or_else(|| {
        PgWireError::Sql(SqlError::DataException {
            sqlstate: "42883",
            message: format!("function with OID {oid} does not exist"),
            data_type: None,
        })
    })?;
    if count != signature.arguments.len() {
        return Err(protocol(
            "FunctionCall argument count does not match function signature",
        ));
    }
    let mut arguments = Vec::with_capacity(count);
    for (arg, (oid, ty)) in signature.arguments.iter().enumerate() {
        let length = read_i32(payload, &mut idx)?;
        let literal = if length == -1 {
            "NULL".to_string()
        } else {
            if length < -1 {
                return Err(protocol("invalid FunctionCall argument length"));
            }
            let end = idx
                .checked_add(length as usize)
                .filter(|end| *end <= payload.len())
                .ok_or_else(|| protocol("FunctionCall argument length exceeds payload"))?;
            let format = format_for_column(&formats, arg)?;
            let literal = decode_parameter_with_db(&db, &payload[idx..end], format, *oid)?;
            idx = end;
            literal
        };
        let variadic = if signature.variadic && arg + 1 == count {
            "VARIADIC "
        } else {
            ""
        };
        arguments.push(format!("{variadic}({literal})::{ty}"));
    }
    let format = read_i16(payload, &mut idx)?;
    validate_format_code(format, "FunctionCall result")?;
    if idx != payload.len() {
        return Err(protocol("trailing bytes in FunctionCall payload"));
    }
    let sql = format!("SELECT {}({})", signature.sql_name, arguments.join(","));
    drop(db);
    state.active_request_bytes = state.active_request_bytes.saturating_add(sql.len());
    enforce_connection_memory(state, &server.config)?;
    let _ = bicdb_core::take_thread_commit_seq();
    let result = execute_server_sql(server, state, &sql)?;
    if let Some(sequence) = bicdb_core::take_thread_commit_seq() {
        server.sync_quorum.wait_for(sequence)?;
    }
    let [row] = result.rows.as_slice() else {
        return Err(protocol("FunctionCall did not return one scalar row"));
    };
    let [value] = row.as_slice() else {
        return Err(protocol("FunctionCall did not return one scalar column"));
    };
    let mut response = Vec::new();
    if matches!(value, SqlValue::Null) {
        put_i32(&mut response, -1);
    } else {
        let bytes = if signature.result_oid == 2278 {
            Vec::new()
        } else {
            let db = server.read_db_for_transaction_progress()?;
            encode_result_value_with_db(&db, value, signature.result_oid, format)?
        };
        let length = i32::try_from(bytes.len())
            .map_err(|_| protocol("FunctionCall result exceeds protocol length"))?;
        put_i32(&mut response, length);
        response.extend_from_slice(&bytes);
    }
    Ok(response)
}

pub(super) fn handle(
    stream: &mut ClientStream,
    server: &Arc<PgWireServer>,
    state: &mut ConnectionState,
    payload: &[u8],
) -> Result<bool> {
    match execute(server, state, payload) {
        Ok(response) => {
            write_message(stream, b'V', &response)?;
            server.record_streamed_rows(1, response.len() as u64);
        }
        Err(error) => {
            state.failed_queries += 1;
            server.failed_queries.fetch_add(1, Ordering::SeqCst);
            state.record_query_error(&server.config.sql_profile);
            log_failed_query(server, state, None, &error);
            error_response_for_error(stream, &error)?;
            if error.closes_connection() {
                return Ok(false);
            }
        }
    }
    write_session_parameter_status(stream, state)?;
    flush_notifications(stream, server, state.connection_id)?;
    ready_for_query_status(stream, state.transaction_status())?;
    Ok(true)
}
