//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn command_complete(stream: &mut ClientStream, tag: &str) -> Result<()> {
    let mut payload = Vec::new();
    cstr(&mut payload, tag);
    write_message(stream, b'C', &payload)
}

pub(crate) fn parse_complete(stream: &mut ClientStream) -> Result<()> {
    write_message(stream, b'1', &[])
}

pub(crate) fn bind_complete(stream: &mut ClientStream) -> Result<()> {
    write_message(stream, b'2', &[])
}

pub(crate) fn portal_suspended(stream: &mut ClientStream) -> Result<()> {
    write_message(stream, b's', &[])
}

pub(crate) fn close_complete(stream: &mut ClientStream) -> Result<()> {
    write_message(stream, b'3', &[])
}

pub(crate) fn no_data(stream: &mut ClientStream) -> Result<()> {
    write_message(stream, b'n', &[])
}

pub(crate) fn parameter_description(stream: &mut ClientStream, oids: &[i32]) -> Result<()> {
    let mut payload = Vec::new();
    put_i16(&mut payload, oids.len() as i16);
    for oid in oids {
        put_i32(&mut payload, *oid);
    }
    write_message(stream, b't', &payload)
}

pub(crate) fn authentication_ok(stream: &mut ClientStream) -> Result<()> {
    let mut payload = Vec::new();
    put_i32(&mut payload, 0);
    write_message(stream, b'R', &payload)
}

pub(crate) fn authentication_cleartext_password(stream: &mut ClientStream) -> Result<()> {
    let mut payload = Vec::new();
    put_i32(&mut payload, 3);
    write_message(stream, b'R', &payload)
}

pub(crate) fn authentication_sasl(
    stream: &mut ClientStream,
    offer_binding: bool,
    allow_plain: bool,
) -> Result<()> {
    let mut payload = Vec::new();
    put_i32(&mut payload, 10);
    if offer_binding {
        cstr(&mut payload, "SCRAM-SHA-256-PLUS");
    }
    if allow_plain {
        cstr(&mut payload, "SCRAM-SHA-256");
    }
    payload.push(0);
    write_message(stream, b'R', &payload)
}

pub(crate) fn authentication_sasl_continue(stream: &mut ClientStream, message: &str) -> Result<()> {
    let mut payload = Vec::new();
    put_i32(&mut payload, 11);
    payload.extend_from_slice(message.as_bytes());
    write_message(stream, b'R', &payload)
}

pub(crate) fn authentication_sasl_final(stream: &mut ClientStream, message: &str) -> Result<()> {
    let mut payload = Vec::new();
    put_i32(&mut payload, 12);
    payload.extend_from_slice(message.as_bytes());
    write_message(stream, b'R', &payload)
}

pub(crate) fn read_password_message(
    stream: &mut ClientStream,
    max_request_bytes: usize,
) -> Result<String> {
    let (tag, payload) = match read_frontend_message(stream, max_request_bytes)? {
        FrontendMessageRead::Message(tag, payload) => (tag, payload),
        FrontendMessageRead::Eof => {
            return Err(PgWireError::Protocol(
                "connection closed before password message".to_string(),
            ));
        }
        FrontendMessageRead::Timeout => {
            return Err(PgWireError::Protocol(
                "timed out waiting for password message".to_string(),
            ));
        }
    };
    if tag != b'p' {
        return Err(PgWireError::Protocol(
            "expected PasswordMessage during authentication".to_string(),
        ));
    }
    cstring_payload(&payload).map(str::to_string)
}

pub(crate) fn authenticate_scram_sha256(
    stream: &mut ClientStream,
    server: &PgWireServer,
    user: &str,
) -> Result<()> {
    let (verifier, user_exists) = scram_verifier_for_user(&server.auth_path, user)?;
    if !stream.is_tls() && server.config.tls_cert.is_some() {
        return Err(PgWireError::Server(
            "SCRAM authentication must use TLS when this server offers TLS".to_string(),
        ));
    }
    let policy = server.config.effective_channel_binding_policy();
    let channel_binding = if stream.is_tls() && policy != ChannelBindingPolicy::Disable {
        let tls = server.tls_config.as_ref().ok_or_else(|| {
            PgWireError::Server("TLS stream has no loaded TLS configuration".into())
        })?;
        match &tls.channel_binding {
            Ok(binding) => Some(binding.as_slice()),
            Err(_) if policy == ChannelBindingPolicy::Prefer => None,
            Err(error) => return Err(PgWireError::Server(error.clone())),
        }
    } else {
        None
    };
    if policy == ChannelBindingPolicy::Require && channel_binding.is_none() {
        return Err(PgWireError::Protocol(
            "SCRAM-PLUS requires TLS channel binding".into(),
        ));
    }
    let offer_binding = channel_binding.is_some();
    let allow_plain = policy != ChannelBindingPolicy::Require;
    authentication_sasl(stream, offer_binding, allow_plain)?;
    let initial = read_sasl_initial_response(stream, server.config.max_request_bytes)?;
    let use_binding = match initial.mechanism.as_str() {
        "SCRAM-SHA-256-PLUS" if offer_binding => true,
        "SCRAM-SHA-256" if allow_plain => false,
        _ => {
            return Err(PgWireError::Protocol(format!(
                "unsupported SASL mechanism {}",
                initial.mechanism
            )))
        }
    };
    let client_first = std::str::from_utf8(&initial.data)
        .map_err(|error| PgWireError::Protocol(error.to_string()))?;
    let (client_first_bare, gs2_header) = scram_client_first_bare(client_first, use_binding)?;
    // GS2 'y' says the client supports binding but believes the server does
    // not. If we advertised PLUS, that discrepancy signals a downgrade.
    if !use_binding && offer_binding && gs2_header == "y,," {
        return Err(PgWireError::Protocol(
            "SCRAM channel-binding downgrade detected".into(),
        ));
    }
    // Pin the chosen mechanism for this exchange. A failed PLUS proof or
    // binding never retries plain SCRAM.
    let channel_binding = if use_binding { channel_binding } else { None };
    let client_nonce = scram_attribute(client_first_bare, "r")?;
    let server_nonce = format!("{client_nonce}{}", BASE64.encode(Uuid::new_v4().as_bytes()));
    let server_first = format!(
        "r={server_nonce},s={},i={}",
        BASE64.encode(&verifier.salt),
        verifier.iterations
    );
    authentication_sasl_continue(stream, &server_first)?;

    let final_message = read_sasl_response(stream, server.config.max_request_bytes)?;
    let final_text = std::str::from_utf8(&final_message)
        .map_err(|error| PgWireError::Protocol(error.to_string()))?;
    let final_nonce = scram_attribute(final_text, "r")?;
    if final_nonce != server_nonce {
        return Err(PgWireError::Protocol(
            "SCRAM client nonce did not match server nonce".to_string(),
        ));
    }
    let supplied_binding = BASE64
        .decode(scram_attribute(final_text, "c")?.as_bytes())
        .map_err(|error| PgWireError::Protocol(error.to_string()))?;
    let mut expected_binding = gs2_header.as_bytes().to_vec();
    if let Some(binding) = &channel_binding {
        expected_binding.extend_from_slice(binding);
    }
    if !bool::from(supplied_binding.as_slice().ct_eq(&expected_binding)) {
        return Err(PgWireError::Protocol(
            "SCRAM channel binding did not match this TLS session".to_string(),
        ));
    }
    let proof = scram_attribute(final_text, "p")?;
    let proof_bytes = BASE64
        .decode(proof.as_bytes())
        .map_err(|error| PgWireError::Protocol(error.to_string()))?;
    let client_final_without_proof = final_text
        .rsplit_once(",p=")
        .map(|(message, _)| message)
        .ok_or_else(|| PgWireError::Protocol("SCRAM proof is missing".to_string()))?;
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");

    let client_signature = hmac_sha256(&verifier.stored_key, auth_message.as_bytes())?;
    if proof_bytes.len() != client_signature.len() {
        return Err(PgWireError::Server(
            "SCRAM proof length did not match".to_string(),
        ));
    }
    let client_key = proof_bytes
        .iter()
        .zip(client_signature.iter())
        .map(|(proof, signature)| proof ^ signature)
        .collect::<Vec<_>>();
    let computed_stored_key = Sha256::digest(&client_key);
    if !bool::from(computed_stored_key.as_slice().ct_eq(&verifier.stored_key)) || !user_exists {
        return Err(PgWireError::Server(
            "SCRAM proof verification failed".to_string(),
        ));
    }

    let server_signature = hmac_sha256(&verifier.server_key, auth_message.as_bytes())?;
    authentication_sasl_final(stream, &format!("v={}", BASE64.encode(server_signature)))?;
    Ok(())
}

pub(crate) struct SaslInitialResponse {
    pub(crate) mechanism: String,
    pub(crate) data: Vec<u8>,
}

pub(crate) fn read_sasl_initial_response(
    stream: &mut ClientStream,
    max_request_bytes: usize,
) -> Result<SaslInitialResponse> {
    let (tag, payload) = read_password_payload(stream, max_request_bytes)?;
    if tag != b'p' {
        return Err(PgWireError::Protocol(
            "expected SASLInitialResponse during authentication".to_string(),
        ));
    }
    let mut idx = 0;
    let mechanism = read_cstr(&payload, &mut idx)?;
    let len = read_i32(&payload, &mut idx)?;
    let data = if len < 0 {
        Vec::new()
    } else {
        let len = len as usize;
        if idx + len > payload.len() {
            return Err(PgWireError::Protocol(
                "SASL initial response was truncated".to_string(),
            ));
        }
        payload[idx..idx + len].to_vec()
    };
    Ok(SaslInitialResponse { mechanism, data })
}

pub(crate) fn read_sasl_response(
    stream: &mut ClientStream,
    max_request_bytes: usize,
) -> Result<Vec<u8>> {
    let (tag, payload) = read_password_payload(stream, max_request_bytes)?;
    if tag != b'p' {
        return Err(PgWireError::Protocol(
            "expected SASLResponse during authentication".to_string(),
        ));
    }
    Ok(payload)
}

pub(crate) fn read_password_payload(
    stream: &mut ClientStream,
    max_request_bytes: usize,
) -> Result<(u8, Vec<u8>)> {
    match read_frontend_message(stream, max_request_bytes)? {
        FrontendMessageRead::Message(tag, payload) => Ok((tag, payload)),
        FrontendMessageRead::Eof => Err(PgWireError::Protocol(
            "connection closed during authentication".to_string(),
        )),
        FrontendMessageRead::Timeout => Err(PgWireError::Protocol(
            "timed out waiting for authentication message".to_string(),
        )),
    }
}

pub(crate) fn scram_client_first_bare(
    client_first: &str,
    require_binding: bool,
) -> Result<(&str, &str)> {
    if require_binding {
        return client_first
            .strip_prefix("p=tls-server-end-point,,")
            .map(|bare| (bare, "p=tls-server-end-point,,"))
            .ok_or_else(|| {
                PgWireError::Protocol("SCRAM-PLUS channel binding is required".to_string())
            });
    }
    if let Some(rest) = client_first.strip_prefix("n,,") {
        return Ok((rest, "n,,"));
    }
    if let Some(rest) = client_first.strip_prefix("y,,") {
        return Ok((rest, "y,,"));
    }
    Err(PgWireError::Protocol(
        "SCRAM channel binding was not selected for this exchange".to_string(),
    ))
}

pub(crate) fn scram_attribute<'a>(message: &'a str, key: &str) -> Result<&'a str> {
    message
        .split(',')
        .find_map(|part| part.strip_prefix(&format!("{key}=")))
        .ok_or_else(|| PgWireError::Protocol(format!("SCRAM attribute {key} is missing")))
}

pub(crate) fn parameter_status(stream: &mut ClientStream, name: &str, value: &str) -> Result<()> {
    let mut payload = Vec::new();
    cstr(&mut payload, name);
    cstr(&mut payload, value);
    write_message(stream, b'S', &payload)
}

pub(crate) fn backend_key_data(
    stream: &mut ClientStream,
    process_id: i32,
    secret_key: i32,
) -> Result<()> {
    let mut payload = Vec::new();
    put_i32(&mut payload, process_id);
    put_i32(&mut payload, secret_key);
    write_message(stream, b'K', &payload)
}

pub(crate) fn negotiate_protocol_version(
    stream: &mut ClientStream,
    protocol_version: i32,
    unsupported_options: &[String],
) -> Result<()> {
    let mut payload = Vec::new();
    put_i32(&mut payload, protocol_version);
    put_i32(&mut payload, unsupported_options.len() as i32);
    for option in unsupported_options {
        cstr(&mut payload, option);
    }
    write_message(stream, b'v', &payload)
}

pub(crate) fn ready_for_query(stream: &mut ClientStream) -> Result<()> {
    ready_for_query_status(stream, b'I')
}

pub(crate) fn ready_for_query_status(stream: &mut ClientStream, status: u8) -> Result<()> {
    write_message(stream, b'Z', &[status])
}

pub(crate) fn error_response(stream: &mut ClientStream, message: &str) -> Result<()> {
    error_response_with_fields(stream, "ERROR", "XX000", message, &[])
}

pub(crate) fn error_response_for_error(
    stream: &mut ClientStream,
    error: &PgWireError,
) -> Result<()> {
    match error {
        PgWireError::Sql(error) => error_response_with_fields(
            stream,
            "ERROR",
            error.sqlstate(),
            &error.to_string(),
            &error.fields(),
        ),
        PgWireError::Authentication => {
            error_response_with_fields(stream, "FATAL", "28P01", &error.to_string(), &[])
        }
        PgWireError::DatabaseNotFound(_) => {
            error_response_with_fields(stream, "FATAL", "3D000", &error.to_string(), &[])
        }
        PgWireError::DatabaseAlreadyExists(_) => {
            error_response_with_fields(stream, "ERROR", "42P04", &error.to_string(), &[])
        }
        PgWireError::Protocol(_) => {
            error_response_with_fields(stream, "ERROR", "08P01", &error.to_string(), &[])
        }
        PgWireError::PersistentConnectionMemoryLimit(_) => {
            error_response_with_fields(stream, "FATAL", "53200", &error.to_string(), &[])
        }
        PgWireError::QueryRejected(_) => {
            error_response_with_fields(stream, "ERROR", "53300", &error.to_string(), &[])
        }
        PgWireError::QueryCanceled => {
            error_response_with_fields(stream, "ERROR", "57014", &error.to_string(), &[])
        }
        PgWireError::QueryTimedOut => {
            error_response_with_fields(stream, "ERROR", "57014", &error.to_string(), &[])
        }
        PgWireError::InFailedTransaction => {
            error_response_with_fields(stream, "ERROR", "25P02", &error.to_string(), &[])
        }
        PgWireError::BicDb(error) => {
            let fields = bicdb_error_fields(error);
            error_response_with_fields(
                stream,
                "ERROR",
                bicdb_error_sqlstate(error),
                &error.to_string(),
                &fields,
            )
        }
        _ => error_response(stream, &error.to_string()),
    }
}

pub(crate) fn bicdb_error_sqlstate(error: &BicDbError) -> &'static str {
    match error {
        BicDbError::CollectionNotFound(_) => "42P01",
        BicDbError::CollectionAlreadyExists(_) => "42P07",
        BicDbError::InvalidCollectionName(_) => "42602",
        BicDbError::TransactionConflict(_) => "40001",
        BicDbError::TransactionNotPending => "25P01",
        BicDbError::QueryCanceled | BicDbError::QueryTimedOut => "57014",
        BicDbError::EmptyRecordId
        | BicDbError::EmptyVector
        | BicDbError::NonFiniteVectorValue
        | BicDbError::InvalidTopK
        | BicDbError::DimensionMismatch { .. }
        | BicDbError::InvalidTimeRange { .. } => "22000",
        _ => "XX000",
    }
}

pub(crate) fn bicdb_error_fields(error: &BicDbError) -> Vec<SqlErrorField> {
    match error {
        BicDbError::CollectionNotFound(collection) => {
            vec![SqlErrorField::Table(collection.clone())]
        }
        _ => Vec::new(),
    }
}

pub(crate) fn startup_protocol_error_response(
    stream: &mut ClientStream,
    message: &str,
) -> Result<()> {
    error_response_with_fields(stream, "FATAL", "08P01", message, &[])
}

pub(crate) fn error_response_with_fields(
    stream: &mut ClientStream,
    severity: &str,
    code: &str,
    message: &str,
    fields: &[SqlErrorField],
) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(b'S');
    cstr(&mut payload, severity);
    payload.push(b'V');
    cstr(&mut payload, severity);
    payload.push(b'C');
    cstr(&mut payload, code);
    payload.push(b'M');
    cstr(&mut payload, message);
    for field in fields {
        match field {
            SqlErrorField::Detail(value) => {
                payload.push(b'D');
                cstr(&mut payload, value);
            }
            SqlErrorField::Schema(value) => {
                payload.push(b's');
                cstr(&mut payload, value);
            }
            SqlErrorField::Table(value) => {
                payload.push(b't');
                cstr(&mut payload, value);
            }
            SqlErrorField::Column(value) => {
                payload.push(b'c');
                cstr(&mut payload, value);
            }
            SqlErrorField::DataType(value) => {
                payload.push(b'd');
                cstr(&mut payload, value);
            }
            SqlErrorField::Constraint(value) => {
                payload.push(b'n');
                cstr(&mut payload, value);
            }
        }
    }
    payload.push(0);
    write_message(stream, b'E', &payload)
}

pub(crate) fn write_message(stream: &mut ClientStream, tag: u8, payload: &[u8]) -> Result<()> {
    // Appends to the per-connection output buffer; the framed message is sent to
    // the socket when the buffer is flushed (before the next read / at request
    // handoff / past the size threshold), batching a whole reply into one write.
    stream.write_all(&[tag])?;
    stream.write_all(&((payload.len() as i32) + 4).to_be_bytes())?;
    stream.write_all(payload)?;
    Ok(())
}

pub(crate) fn type_oid(result: &SqlResult, column: usize) -> i32 {
    // Value-INDEPENDENT fallback for columns the planner could not type from the
    // schema/expression. Each `SqlValue` kind maps to one fixed OID regardless of
    // the runtime value, so the same column never flips OID based on its contents.
    // Integers always report int8 (20): it can never truncate, and an untyped
    // (dynamic/schemaless) integer column must report a single stable type. A
    // value that happens to fit in int4 is NOT reported as int4 — that was the
    // arbitrariness this fix removes.
    match first_non_null(result, column) {
        Some(SqlValue::Bool(_)) => 16,
        Some(SqlValue::Int(_)) => 20,
        Some(SqlValue::Float(_)) => 701,
        Some(SqlValue::JsonText(_)) => 114,
        Some(SqlValue::Json(JsonValue::Array(_))) => 1009,
        Some(SqlValue::Json(_)) => 114,
        Some(SqlValue::Geometry(_)) => 25,
        Some(SqlValue::TsQuery(_)) => 3615,
        Some(SqlValue::Composite(value)) => value
            .type_oid
            .and_then(|oid| i32::try_from(oid).ok())
            .unwrap_or(2249),
        Some(SqlValue::String(_)) | Some(SqlValue::Null) | None => 25,
    }
}

pub(crate) fn type_size_for_oid(result: &SqlResult, column: usize, oid: i32) -> i16 {
    if let Some(spec) = bicdb_sql::pg_type_spec_by_oid(oid) {
        return spec.len;
    }
    if let Some(size) = temporal_type_size(oid) {
        return size;
    }
    match oid {
        16 => 1,
        20 | 701 => 8,
        21 => 2,
        23 | 700 => 4,
        2950 => 16,
        _ => match first_non_null(result, column) {
            Some(SqlValue::Bool(_)) => 1,
            Some(SqlValue::Int(_)) => 8,
            Some(SqlValue::Float(_)) => 8,
            Some(SqlValue::Geometry(_)) => -1,
            _ => -1,
        },
    }
}

pub(crate) fn type_size_for_oid_with_db(
    db: &BicDb,
    result: &SqlResult,
    column: usize,
    oid: i32,
) -> Result<i16> {
    if bicdb_sql::pg_user_type_array_element_oid(db, oid)?.is_some()
        || bicdb_sql::pg_table_row_array_element_oid(db, oid)?.is_some()
    {
        return Ok(-1);
    }
    if oid == 2249 || bicdb_sql::pg_is_table_row_type_oid(db, oid)? {
        return Ok(-1);
    }
    if bicdb_sql::pg_user_range_type_info(db, oid)?.is_some() {
        return Ok(-1);
    }
    if let Some(base_oid) = bicdb_sql::pg_user_type_binary_base_oid(db, oid)? {
        return Ok(type_size_for_oid(result, column, base_oid));
    }
    if bicdb_sql::pg_is_user_type_oid(db, oid)? {
        return Ok(4);
    }
    Ok(type_size_for_oid(result, column, oid))
}

#[cfg(test)]
pub(crate) fn result_column_types(
    result: &SqlResult,
    source_sql: Option<&str>,
) -> Result<Vec<i32>> {
    result_column_types_inner(None, result, source_sql)
}

pub(crate) fn result_column_types_with_db(
    db: &BicDb,
    result: &SqlResult,
    source_sql: Option<&str>,
) -> Result<Vec<i32>> {
    result_column_types_inner(Some(db), result, source_sql)
}

pub(crate) fn result_column_types_inner(
    db: Option<&BicDb>,
    result: &SqlResult,
    source_sql: Option<&str>,
) -> Result<Vec<i32>> {
    let dml_returning = source_sql.is_some_and(has_dml_returning);
    let dml_inferred = source_sql
        .filter(|_| dml_returning)
        .and_then(infer_dml_returning_oids)
        .unwrap_or_default();
    let inferred = source_sql
        .and_then(|sql| match db {
            Some(db) => infer_select_cast_oids_with_db(db, sql, result.columns.len()),
            None => infer_select_cast_oids(sql, result.columns.len()),
        })
        .unwrap_or_default();
    let query_inferred = source_sql
        .and_then(|sql| db.and_then(|db| infer_query_result_types(db, sql).ok().flatten()))
        .filter(|types| types.len() == result.columns.len())
        .unwrap_or_default();
    (0..result.columns.len())
        .map(|idx| {
            // DML RETURNING is predescribed through a synthetic SELECT that can
            // fall back to text when the expression depends on mutation-only
            // context. An explicit type in the original DML is authoritative.
            if dml_returning {
                if let Some(oid) = dml_inferred.get(idx).copied().flatten() {
                    return Ok(oid);
                }
            }
            if let Some(Some(type_name)) = query_inferred.get(idx) {
                return match db {
                    Some(db) => require_type_oid_with_db(db, type_name),
                    None => require_type_oid(type_name),
                };
            }
            // The planner's schema/expression type is authoritative. SQL-text
            // inference exists only for expressions the planner cannot yet type.
            if let Some(Some(type_name)) = result.column_types.get(idx) {
                return match db {
                    Some(db) => require_type_oid_with_db(db, type_name),
                    None => require_type_oid(type_name),
                };
            }
            if let Some(oid) = inferred.get(idx).copied() {
                return Ok(oid);
            }
            if dml_returning {
                return Ok(25);
            }
            Ok(type_oid(result, idx))
        })
        .collect()
}

pub(crate) fn infer_select_cast_oids(sql: &str, expected_columns: usize) -> Option<Vec<i32>> {
    infer_select_cast_oids_inner(None, sql, expected_columns)
}

pub(crate) fn infer_select_cast_oids_with_db(
    db: &BicDb,
    sql: &str,
    expected_columns: usize,
) -> Option<Vec<i32>> {
    infer_select_cast_oids_inner(Some(db), sql, expected_columns)
}

pub(crate) fn infer_select_cast_oids_inner(
    db: Option<&BicDb>,
    sql: &str,
    expected_columns: usize,
) -> Option<Vec<i32>> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let Statement::Query(query) = statements.pop()? else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if !select.from.is_empty()
        || (expected_columns != usize::MAX && select.projection.len() != expected_columns)
    {
        return None;
    }
    select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                infer_select_expression_oid_inner(db, expr)
            }
            _ => None,
        })
        .collect()
}

pub(crate) fn infer_dml_returning_oids(sql: &str) -> Option<Vec<Option<i32>>> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let statement = statements.pop()?;
    let projection = match &statement {
        Statement::Insert(insert) => insert.returning.as_ref()?,
        Statement::Update(update) => update.returning.as_ref()?,
        Statement::Delete(delete) => delete.returning.as_ref()?,
        _ => return None,
    };
    Some(
        projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    infer_select_expression_oid(expr)
                }
                _ => None,
            })
            .collect(),
    )
}

pub(crate) fn infer_select_expression_oid(expr: &Expr) -> Option<i32> {
    infer_select_expression_oid_inner(None, expr)
}

pub(crate) fn infer_select_expression_oid_inner(db: Option<&BicDb>, expr: &Expr) -> Option<i32> {
    match expr {
        Expr::Value(value) => match value.value {
            Value::Boolean(_) => Some(16),
            _ => None,
        },
        Expr::Cast { data_type, .. } => {
            let type_name = pg_type_from_data_type(data_type)
                .ok()
                .map(|(type_name, _)| type_name)
                .unwrap_or_else(|| data_type.to_string());
            oid_for_type_name(&type_name).or_else(|| {
                db.and_then(|db| {
                    bicdb_sql::pg_user_type_oid_by_name(db, &type_name)
                        .ok()
                        .flatten()
                })
            })
        }
        Expr::Nested(expr) => infer_select_expression_oid_inner(db, expr),
        Expr::Extract { .. } => Some(1700),
        Expr::BinaryOp { left, op, .. } => match op {
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::AtArrow
            | BinaryOperator::ArrowAt
            | BinaryOperator::Question
            | BinaryOperator::QuestionAnd
            | BinaryOperator::QuestionPipe => Some(16),
            BinaryOperator::LongArrow | BinaryOperator::HashLongArrow => Some(25),
            BinaryOperator::Arrow
            | BinaryOperator::HashArrow
            | BinaryOperator::HashMinus
            | BinaryOperator::Minus
            | BinaryOperator::StringConcat => match infer_select_expression_oid_inner(db, left) {
                Some(114) => Some(114),
                Some(3802) => Some(3802),
                _ => None,
            },
            _ => None,
        },
        Expr::Function(function) => {
            let name = function.name.to_string().to_ascii_lowercase();
            if matches!(name.as_str(), "coalesce" | "pg_catalog.coalesce") {
                let FunctionArguments::List(args) = &function.args else {
                    return None;
                };
                return args.args.iter().find_map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        infer_select_expression_oid_inner(db, expr)
                    }
                    _ => None,
                });
            }
            match name.as_str() {
                "current_schemas" | "pg_catalog.current_schemas" => Some(NAME_ARRAY_OID),
                "json_build_object"
                | "pg_catalog.json_build_object"
                | "json_build_array"
                | "pg_catalog.json_build_array"
                | "to_json"
                | "pg_catalog.to_json" => Some(114),
                "jsonb_build_object"
                | "pg_catalog.jsonb_build_object"
                | "jsonb_build_array"
                | "pg_catalog.jsonb_build_array"
                | "to_jsonb"
                | "pg_catalog.to_jsonb"
                | "jsonb_set"
                | "pg_catalog.jsonb_set"
                | "jsonb_strip_nulls"
                | "pg_catalog.jsonb_strip_nulls" => Some(3802),
                "json_array_length"
                | "pg_catalog.json_array_length"
                | "jsonb_array_length"
                | "pg_catalog.jsonb_array_length" => Some(23),
                "jsonb_pretty" | "pg_catalog.jsonb_pretty" => Some(25),
                "json_typeof"
                | "pg_catalog.json_typeof"
                | "jsonb_typeof"
                | "pg_catalog.jsonb_typeof" => Some(25),
                _ => None,
            }
        }
        _ => None,
    }
}

pub(crate) fn split_top_level_commas(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut in_string = false;
    let bytes = input.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'\'' => {
                in_string = !in_string;
                if in_string && bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 1;
                }
            }
            b'(' if !in_string => depth += 1,
            b')' if !in_string => depth -= 1,
            b',' if !in_string && depth == 0 => {
                parts.push(input[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
        idx += 1;
    }
    parts.push(input[start..].trim());
    parts
}

pub(crate) fn oid_for_type_name(type_name: &str) -> Option<i32> {
    bicdb_sql::pg_type_oid_by_name(type_name)
}

pub(crate) fn oid_for_type_name_with_db(db: &BicDb, type_name: &str) -> Result<Option<i32>> {
    match oid_for_type_name(type_name) {
        Some(oid) => Ok(Some(oid)),
        None => bicdb_sql::pg_user_type_oid_by_name(db, type_name).map_err(PgWireError::Sql),
    }
}

pub(crate) fn require_type_oid(type_name: &str) -> Result<i32> {
    oid_for_type_name(type_name).ok_or_else(|| {
        PgWireError::Protocol(format!(
            "result or parameter type {type_name:?} is not registered"
        ))
    })
}

pub(crate) fn require_type_oid_with_db(db: &BicDb, type_name: &str) -> Result<i32> {
    oid_for_type_name_with_db(db, type_name)?.ok_or_else(|| {
        PgWireError::Protocol(format!(
            "result or parameter type {type_name:?} is not registered"
        ))
    })
}

pub(crate) fn format_for_column(result_formats: &[i16], column: usize) -> Result<i16> {
    let format = if result_formats.len() == 1 {
        result_formats[0]
    } else {
        result_formats.get(column).copied().unwrap_or(0)
    };
    validate_format_code(format, "result")?;
    Ok(format)
}

pub(crate) fn validate_format_codes(formats: &[i16], label: &str) -> Result<()> {
    for format in formats {
        validate_format_code(*format, label)?;
    }
    Ok(())
}

pub(crate) fn validate_format_arity(formats: &[i16], expected: usize, label: &str) -> Result<()> {
    if formats.len() <= 1 || formats.len() == expected {
        Ok(())
    } else {
        Err(PgWireError::Protocol(format!(
            "{label} format code count {} does not match value count {expected}",
            formats.len()
        )))
    }
}

pub(crate) fn validate_format_code(format: i16, label: &str) -> Result<()> {
    match format {
        0 | 1 => Ok(()),
        other => Err(PgWireError::Protocol(format!(
            "unsupported {label} format code {other}"
        ))),
    }
}

pub(crate) fn encode_result_value_with_db(
    db: &BicDb,
    value: &SqlValue,
    oid: i32,
    format: i16,
) -> Result<Vec<u8>> {
    encode_result_value_with_memo(db, value, oid, format, &mut None)
}

pub(crate) fn encode_result_value_with_memo(
    db: &BicDb,
    value: &SqlValue,
    oid: i32,
    format: i16,
    element_oid_memo: &mut Option<Option<i32>>,
) -> Result<Vec<u8>> {
    match format {
        0 => {
            let element_oid = if oid == 2277 {
                // anyarray carries its element OID per value — never memoized.
                anyarray_element_oid(value)
            } else {
                memoized_element_oid(db, oid, element_oid_memo)?
            };
            if oid == 2277 || element_oid.is_some() {
                let delimiter = match element_oid {
                    Some(element_oid) => array_element_delimiter_with_db(db, element_oid)?,
                    None => bicdb_sql::pg_user_type_array_delimiter(db, oid)?.unwrap_or(','),
                };
                let rendered;
                let value = if let Some(element_type) =
                    element_oid.and_then(bicdb_sql::oid_alias_type_from_oid)
                {
                    rendered = bicdb_sql::render_oid_alias_array_value(db, element_type, value)?;
                    &rendered
                } else {
                    value
                };
                return Ok(postgres_array_text_result_with_delimiter(
                    value,
                    delimiter,
                    element_oid,
                )?
                .into_bytes());
            }
            if let Some(pg_type) = bicdb_sql::oid_alias_type_from_oid(oid) {
                return Ok(bicdb_sql::render_oid_alias_value(db, pg_type, value)?.into_bytes());
            }
            encode_text_result_value(value, oid)
        }
        1 => encode_binary_result_value_with_memo(db, value, oid, element_oid_memo),
        other => Err(PgWireError::Protocol(format!(
            "unsupported result format code {other}"
        ))),
    }
}

pub(crate) fn encode_text_result_value(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    if let Some(element_oid) = array_element_oid(oid) {
        return Ok(postgres_array_text_result(value, element_oid)?.into_bytes());
    }
    if oid == 16 {
        return match value {
            SqlValue::Bool(true) => Ok(b"t".to_vec()),
            SqlValue::Bool(false) => Ok(b"f".to_vec()),
            _ => Ok(value.to_cell().into_bytes()),
        };
    }
    if oid == 3802 {
        return Ok(postgres_jsonb_text_result(value).into_bytes());
    }
    if oid == 380_200 {
        return Ok(postgres_vector_text_result(value)?.into_bytes());
    }
    if matches!(oid, 700 | 701) {
        let value = float_result(value, oid)?;
        let pg_type = if oid == 700 { "float4" } else { "float8" };
        return Ok(bicdb_sql::postgres_float_text(value, pg_type).into_bytes());
    }
    if oid == 790 {
        let cents = bicdb_sql::pg_money_cents_from_text(&value.to_cell()).map_err(|_| {
            PgWireError::Protocol(format!(
                "money result is not a valid PostgreSQL money value: {}",
                value.to_cell()
            ))
        })?;
        return Ok(bicdb_sql::pg_money_display_from_cents(cents).into_bytes());
    }
    if oid == 869 {
        let PgCanonicalValue::Network(network) = canonical_special_result(value, "inet", oid)?
        else {
            return Ok(value.to_cell().into_bytes());
        };
        return Ok(network.to_postgres_output_text().into_bytes());
    }
    Ok(value.to_cell().into_bytes())
}

pub(crate) fn postgres_jsonb_text_result(value: &SqlValue) -> String {
    match value {
        SqlValue::Json(value) => postgres_jsonb_text(value),
        _ => value.to_cell(),
    }
}

pub(crate) fn postgres_vector_values(value: &SqlValue) -> Result<Vec<f32>> {
    let parsed = match value {
        SqlValue::Json(JsonValue::Array(values)) => JsonValue::Array(values.clone()),
        SqlValue::String(value) => serde_json::from_str::<JsonValue>(value).map_err(|error| {
            PgWireError::Protocol(format!("invalid vector result {value:?}: {error}"))
        })?,
        other => {
            return Err(PgWireError::Protocol(format!(
                "vector result is not an array: {}",
                other.to_cell()
            )));
        }
    };
    let JsonValue::Array(values) = parsed else {
        return Err(PgWireError::Protocol(
            "vector result is not an array".to_string(),
        ));
    };
    if values.is_empty() || values.len() > 16_000 {
        return Err(PgWireError::Protocol(format!(
            "vector result has invalid dimension {}",
            values.len()
        )));
    }
    values
        .into_iter()
        .map(|value| {
            let value = value.as_f64().ok_or_else(|| {
                PgWireError::Protocol("vector result contains a non-numeric value".to_string())
            })? as f32;
            value.is_finite().then_some(value).ok_or_else(|| {
                PgWireError::Protocol("vector result contains a non-finite value".to_string())
            })
        })
        .collect()
}

pub(crate) fn postgres_vector_text_result(value: &SqlValue) -> Result<String> {
    let values = postgres_vector_values(value)?;
    Ok(format!(
        "[{}]",
        values
            .into_iter()
            .map(|value| bicdb_sql::postgres_float_text(f64::from(value), "float4"))
            .collect::<Vec<_>>()
            .join(",")
    ))
}

pub(crate) fn encode_binary_vector_result(value: &SqlValue) -> Result<Vec<u8>> {
    let values = postgres_vector_values(value)?;
    let dimensions = i16::try_from(values.len()).map_err(|_| {
        PgWireError::Protocol(format!("vector dimension {} is too large", values.len()))
    })?;
    let mut output = Vec::with_capacity(4 + values.len() * 4);
    output.extend_from_slice(&dimensions.to_be_bytes());
    output.extend_from_slice(&0_i16.to_be_bytes());
    for value in values {
        output.extend_from_slice(&value.to_bits().to_be_bytes());
    }
    Ok(output)
}

pub(crate) fn postgres_array_text_result(value: &SqlValue, element_oid: i32) -> Result<String> {
    postgres_array_text_result_with_delimiter(
        value,
        array_element_delimiter(element_oid),
        Some(element_oid),
    )
}

pub(crate) fn postgres_array_text_result_with_delimiter(
    value: &SqlValue,
    delimiter: char,
    element_oid: Option<i32>,
) -> Result<String> {
    match value {
        SqlValue::Json(value) => {
            let (array, lower_bounds) = if let Some(input) = value.get("$bicdb_array_input") {
                let array = input
                    .get("value")
                    .filter(|value| value.is_array())
                    .ok_or_else(|| {
                        PgWireError::Protocol(
                            "array result envelope has no array value".to_string(),
                        )
                    })?;
                let lower_bounds = input
                    .get("lower_bounds")
                    .and_then(JsonValue::as_array)
                    .ok_or_else(|| {
                        PgWireError::Protocol(
                            "array result envelope has no lower bounds".to_string(),
                        )
                    })?
                    .iter()
                    .map(|value| {
                        value
                            .as_i64()
                            .and_then(|value| i32::try_from(value).ok())
                            .ok_or_else(|| {
                                PgWireError::Protocol(
                                    "array result lower bound is outside int32".to_string(),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                (array, Some(lower_bounds))
            } else if value.is_array() {
                (value, None)
            } else {
                return Err(PgWireError::Protocol(format!(
                    "array result oid cannot encode scalar value {}",
                    value
                )));
            };
            let dimensions = binary_array_json_dimensions(array).ok_or_else(|| {
                PgWireError::Protocol("array result must be rectangular".to_string())
            })?;
            let prefix = if let Some(lower_bounds) = lower_bounds {
                if lower_bounds.len() != dimensions.len() {
                    return Err(PgWireError::Protocol(format!(
                        "array result has {} lower bounds for rank {}",
                        lower_bounds.len(),
                        dimensions.len()
                    )));
                }
                if lower_bounds.iter().any(|lower_bound| *lower_bound != 1) {
                    dimensions
                        .iter()
                        .zip(lower_bounds)
                        .map(|(length, lower)| {
                            format!("[{lower}:{}]", i64::from(lower) + *length as i64 - 1)
                        })
                        .collect::<String>()
                        + "="
                } else {
                    String::new()
                }
            } else {
                String::new()
            };
            Ok(prefix + postgres_array_json_element(array, delimiter, element_oid)?.as_str())
        }
        SqlValue::String(value) => Ok(value.clone()),
        other => Err(PgWireError::Protocol(format!(
            "array result oid cannot encode scalar value {}",
            other.to_cell()
        ))),
    }
}

pub(crate) fn postgres_array_json_element(
    value: &JsonValue,
    delimiter: char,
    element_oid: Option<i32>,
) -> Result<String> {
    if let Some(composite) = bicdb_sql::pg_composite_from_array_json(value) {
        return Ok(postgres_array_text_element(
            &SqlValue::Composite(composite).to_cell(),
            delimiter,
        ));
    }
    Ok(match value {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::String(value) => {
            postgres_array_text_element(&postgres_array_scalar_text(value, element_oid)?, delimiter)
        }
        JsonValue::Array(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|value| postgres_array_json_element(value, delimiter, element_oid))
                .collect::<Result<Vec<_>>>()?
                .join(&delimiter.to_string())
        ),
        JsonValue::Object(_) => postgres_array_text_element(&value.to_string(), delimiter),
    })
}

pub(crate) fn postgres_array_scalar_text(value: &str, element_oid: Option<i32>) -> Result<String> {
    let Some(element_oid) = element_oid else {
        return Ok(value.to_string());
    };
    let pg_type = match element_oid {
        600 => "point",
        601 => "lseg",
        602 => "path",
        603 => "box",
        604 => "polygon",
        628 => "line",
        718 => "circle",
        650 => "cidr",
        774 => "macaddr8",
        829 => "macaddr",
        869 => "inet",
        _ => return Ok(value.to_string()),
    };
    let canonical =
        canonical_special_result(&SqlValue::String(value.to_string()), pg_type, element_oid)?;
    Ok(match canonical {
        PgCanonicalValue::Network(network) => network.to_postgres_output_text(),
        PgCanonicalValue::MacAddress(address) => address.to_postgres_text(),
        PgCanonicalValue::Geometric(value) => value.to_postgres_text(),
        _ => value.to_string(),
    })
}

pub(crate) fn postgres_array_text_element(value: &str, delimiter: char) -> String {
    let needs_quotes = value.is_empty()
        || value.eq_ignore_ascii_case("NULL")
        || value.chars().any(|ch| {
            ch.is_whitespace() || ch == delimiter || matches!(ch, '"' | '\\' | '{' | '}')
        });
    if !needs_quotes {
        return value.to_string();
    }

    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

pub(crate) fn encode_binary_result_value(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    if let Some(element_oid) = array_element_oid(oid) {
        return encode_binary_array_result(value, element_oid);
    }
    if bicdb_sql::pg_type_spec_by_oid(oid).and_then(|spec| spec.binary_codec())
        == Some(PgBinaryCodec::TextPayload)
    {
        return Ok(value.to_cell().into_bytes());
    }
    match oid {
        16 => match value {
            SqlValue::Bool(value) => Ok(vec![u8::from(*value)]),
            _ => unsupported_binary_result(oid, value),
        },
        17 => Ok(decode_bytea_cell(&value.to_cell())),
        18 => bicdb_sql::pg_internal_char_byte(value)
            .or_else(|| match value {
                SqlValue::String(value) if value.len() == 1 => value.as_bytes().first().copied(),
                _ => None,
            })
            .map(|byte| vec![byte])
            .ok_or_else(|| {
                PgWireError::Protocol(format!(
                    "binary result oid {oid} cannot encode internal char {}",
                    value.to_cell()
                ))
            }),
        20 => integer_result(value, oid).map(|value| value.to_be_bytes().to_vec()),
        21 => integer_result(value, oid).and_then(|value| {
            i16::try_from(value)
                .map(|value| value.to_be_bytes().to_vec())
                .map_err(|_| binary_result_range_error(oid, value))
        }),
        23 => integer_result(value, oid).and_then(|value| {
            i32::try_from(value)
                .map(|value| value.to_be_bytes().to_vec())
                .map_err(|_| binary_result_range_error(oid, value))
        }),
        27 => bicdb_sql::PgTupleId::from_postgres_text(&value.to_cell())
            .map(|value| {
                let mut output = Vec::with_capacity(6);
                output.extend_from_slice(&value.block.to_be_bytes());
                output.extend_from_slice(&value.offset.to_be_bytes());
                output
            })
            .map_err(|error| PgWireError::Protocol(error.to_string())),
        28 | 29 => {
            bicdb_sql::parse_pg_xid32(&value.to_cell(), if oid == 28 { "xid" } else { "cid" })
                .map(|value| value.to_be_bytes().to_vec())
                .map_err(|error| PgWireError::Protocol(error.to_string()))
        }
        5069 => bicdb_sql::parse_pg_xid8(&value.to_cell())
            .map(|value| value.to_be_bytes().to_vec())
            .map_err(|error| PgWireError::Protocol(error.to_string())),
        114 => Ok(value.to_cell().into_bytes()),
        380_200 => encode_binary_vector_result(value),
        600 | 601 | 602 | 603 | 604 | 628 | 718 => geometric_result(value, oid),
        650 | 869 => network_result(value, oid),
        774 | 829 => mac_result(value, oid),
        790 => bicdb_sql::pg_money_cents_from_text(&value.to_cell())
            .map(|value| value.to_be_bytes().to_vec())
            .map_err(|_| {
                PgWireError::Protocol(format!(
                    "money result is not a valid PostgreSQL money value: {}",
                    value.to_cell()
                ))
            }),
        1700 => encode_binary_numeric_result(value),
        700 => float_result(value, oid).map(|value| (value as f32).to_be_bytes().to_vec()),
        701 => float_result(value, oid).map(|value| value.to_be_bytes().to_vec()),
        1082 => date_result(value, oid).map(|value| value.to_be_bytes().to_vec()),
        1083 => time_result(value, oid).map(|value| value.to_be_bytes().to_vec()),
        1114 | 1184 => timestamp_result(value, oid).map(|value| value.to_be_bytes().to_vec()),
        1186 => interval_result(value, oid),
        1266 => timetz_result(value, oid),
        1560 | 1562 => bit_string_result(value, oid),
        2950 => match value {
            SqlValue::String(value) => Uuid::parse_str(value)
                .map(|uuid| uuid.as_bytes().to_vec())
                .map_err(|error| PgWireError::Protocol(error.to_string())),
            _ => unsupported_binary_result(oid, value),
        },
        3802 => {
            let value = postgres_jsonb_text_result(value);
            let mut output = Vec::with_capacity(value.len() + 1);
            output.push(1);
            output.extend_from_slice(value.as_bytes());
            Ok(output)
        }
        4072 => versioned_text_result(value),
        3220 => lsn_result(value, oid),
        3904 | 3906 | 3908 | 3910 | 3912 | 3926 => range_result(value, oid),
        4451 | 4532 | 4533 | 4534 | 4535 | 4536 => multirange_result(value, oid),
        2970 | 5038 => snapshot_result(value, oid),
        3614 => PgTsVector::from_postgres_text(&value.to_cell())
            .map(|value| value.to_postgres_binary())
            .map_err(PgWireError::Sql),
        3615 => {
            let query = match value {
                SqlValue::TsQuery(value) => value.clone(),
                _ => PgTsQuery::from_postgres_text(&value.to_cell()).map_err(PgWireError::Sql)?,
            };
            Ok(query.to_postgres_binary())
        }
        oid if is_oid_alias_oid(oid) => oid_alias_result(value, oid),
        _ => unsupported_binary_result(oid, value),
    }
}

pub(crate) fn encode_binary_result_value_with_db(
    db: &BicDb,
    value: &SqlValue,
    oid: i32,
) -> Result<Vec<u8>> {
    encode_binary_result_value_with_memo(db, value, oid, &mut None)
}

pub(crate) fn encode_binary_result_value_with_memo(
    db: &BicDb,
    value: &SqlValue,
    oid: i32,
    element_oid_memo: &mut Option<Option<i32>>,
) -> Result<Vec<u8>> {
    if let Some(pg_type) = bicdb_sql::oid_alias_type_from_oid(oid) {
        return Ok(bicdb_sql::oid_alias_numeric_value(db, pg_type, value)?
            .to_be_bytes()
            .to_vec());
    }
    if matches!(oid, 22 | 30) {
        return encode_binary_catalog_vector_result(value, oid, db);
    }
    if oid == 2277 {
        let element_oid = anyarray_element_oid(value).ok_or_else(|| {
            PgWireError::Protocol("anyarray result is missing its element OID".to_string())
        })?;
        return encode_binary_array_result_inner(value, element_oid, Some(db));
    }
    if let Some(element_oid) = memoized_element_oid(db, oid, element_oid_memo)? {
        return encode_binary_array_result_inner(value, element_oid, Some(db));
    }
    if let Some(info) = bicdb_sql::pg_user_range_type_info(db, oid)? {
        let ranges = bicdb_sql::pg_user_range_values(db, oid, value)?
            .ok_or_else(|| PgWireError::Protocol(format!("unknown user range oid {oid}")))?;
        if info.multirange {
            return encode_pg_multirange_inner(
                &ranges,
                info.multirange_oid,
                info.range_oid,
                info.subtype_oid,
            );
        }
        let range = ranges.first().ok_or_else(|| {
            PgWireError::Protocol(format!("range oid {oid} is missing its scalar value"))
        })?;
        return encode_pg_range_inner(range, info.range_oid, info.subtype_oid);
    }
    if let Some(base_oid) = bicdb_sql::pg_user_type_binary_base_oid(db, oid)? {
        return encode_binary_result_value_with_db(db, value, base_oid);
    }
    if oid == 2249 || bicdb_sql::pg_is_table_row_type_oid(db, oid)? {
        return encode_binary_composite_result(db, value, oid);
    }
    if bicdb_sql::pg_is_user_type_oid(db, oid)? {
        return Ok(value.to_cell().into_bytes());
    }
    encode_binary_result_value(value, oid)
}

pub(crate) fn anyarray_element_oid(value: &SqlValue) -> Option<i32> {
    let SqlValue::Json(value) = value else {
        return None;
    };
    value
        .get("$bicdb_array_input")?
        .get("element_oid")?
        .as_i64()
        .and_then(|oid| i32::try_from(oid).ok())
}

pub(crate) fn encode_binary_composite_result(
    db: &BicDb,
    value: &SqlValue,
    oid: i32,
) -> Result<Vec<u8>> {
    let SqlValue::Composite(composite) = value else {
        return Err(PgWireError::Protocol(format!(
            "binary composite OID {oid} requires a composite SQL value"
        )));
    };
    let definition = bicdb_sql::pg_table_row_type_definition(db, oid)?;
    let mut output = Vec::new();
    output.extend_from_slice(&(composite.fields.len() as i32).to_be_bytes());
    for (index, field) in composite.fields.iter().enumerate() {
        let pg_type = definition
            .as_ref()
            .and_then(|(_, fields)| fields.get(index))
            .map(|(_, pg_type)| pg_type.as_str())
            .unwrap_or(&field.pg_type);
        let field_oid = require_type_oid_with_db(db, pg_type)?;
        output.extend_from_slice(&field_oid.to_be_bytes());
        if matches!(field.value, SqlValue::Null) {
            output.extend_from_slice(&(-1_i32).to_be_bytes());
            continue;
        }
        let encoded = encode_binary_result_value_with_db(db, &field.value, field_oid)?;
        output.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
        output.extend_from_slice(&encoded);
    }
    Ok(output)
}

pub(crate) fn decode_binary_composite_parameter(
    db: &BicDb,
    bytes: &[u8],
    oid: i32,
) -> Result<String> {
    let mut index = 0usize;
    let field_count = read_i32(bytes, &mut index)?;
    if field_count < 0 {
        return Err(PgWireError::Protocol(
            "binary composite field count must not be negative".to_string(),
        ));
    }
    let field_count = bounded_binary_count(
        field_count,
        bytes.len().saturating_sub(index),
        8,
        "binary composite field",
    )?;
    let definition = bicdb_sql::pg_table_row_type_definition(db, oid)?;
    if let Some((_, fields)) = &definition {
        if fields.len() != field_count {
            return Err(PgWireError::Protocol(format!(
                "binary composite has {field_count} fields but type OID {oid} requires {}",
                fields.len()
            )));
        }
    }
    let mut values = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        let field_oid = read_i32(bytes, &mut index)?;
        let length = read_i32(bytes, &mut index)?;
        if length == -1 {
            values.push("NULL".to_string());
            continue;
        }
        let length = usize::try_from(length).map_err(|_| {
            PgWireError::Protocol("binary composite field length must not be negative".to_string())
        })?;
        let end = index.checked_add(length).ok_or_else(|| {
            PgWireError::Protocol("binary composite field length overflow".to_string())
        })?;
        let field = bytes
            .get(index..end)
            .ok_or_else(|| PgWireError::Protocol("truncated binary composite field".to_string()))?;
        index = end;
        values.push(decode_binary_parameter_with_db(db, field, field_oid)?);
    }
    if index != bytes.len() {
        return Err(PgWireError::Protocol(
            "binary composite has trailing bytes".to_string(),
        ));
    }
    let row = format!("ROW({})", values.join(", "));
    Ok(match definition {
        Some((type_name, _)) => format!("{row}::{type_name}"),
        None => row,
    })
}

pub(crate) fn encode_binary_catalog_vector_result(
    value: &SqlValue,
    oid: i32,
    db: &BicDb,
) -> Result<Vec<u8>> {
    let element_oid = match oid {
        22 => 21,
        30 => 26,
        _ => unreachable!(),
    };
    let value = match value {
        SqlValue::String(value) => {
            let values = value
                .split_whitespace()
                .map(|item| {
                    item.parse::<i64>()
                        .map(JsonValue::from)
                        .map_err(|error| PgWireError::Protocol(error.to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            if values.is_empty() {
                let mut output = Vec::with_capacity(20);
                put_i32(&mut output, 1);
                put_i32(&mut output, 0);
                put_i32(&mut output, element_oid);
                put_i32(&mut output, 0);
                put_i32(&mut output, 0);
                return Ok(output);
            }
            SqlValue::Json(serde_json::json!({
                "$bicdb_array_input": {
                    "lower_bounds": [0],
                    "value": values,
                }
            }))
        }
        value => value.clone(),
    };
    encode_binary_array_result_inner(&value, element_oid, Some(db))
}

pub(crate) fn bit_string_result(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    let bits = PgBitString::from_bit_text(&value.to_cell()).map_err(|error| {
        PgWireError::Protocol(format!("invalid bit-string result for oid {oid}: {error}"))
    })?;
    let bit_len = i32::try_from(bits.bit_len()).map_err(|_| {
        PgWireError::Protocol(format!("bit-string result for oid {oid} is too long"))
    })?;
    let mut output = Vec::with_capacity(4 + bits.bytes().len());
    output.extend_from_slice(&bit_len.to_be_bytes());
    output.extend_from_slice(bits.bytes());
    Ok(output)
}

pub(crate) fn versioned_text_result(value: &SqlValue) -> Result<Vec<u8>> {
    let text = value.to_cell();
    let mut output = Vec::with_capacity(text.len() + 1);
    output.push(1);
    output.extend_from_slice(text.as_bytes());
    Ok(output)
}

pub(crate) fn canonical_special_result(
    value: &SqlValue,
    pg_type: &str,
    oid: i32,
) -> Result<PgCanonicalValue> {
    parse_pg_canonical_special(pg_type, &value.to_cell())
        .map_err(|error| PgWireError::Protocol(error.to_string()))?
        .ok_or_else(|| {
            PgWireError::Protocol(format!(
                "binary result oid {oid} has no canonical {pg_type} value"
            ))
        })
}

pub(crate) fn network_result(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    let pg_type = if oid == 650 { "cidr" } else { "inet" };
    let PgCanonicalValue::Network(network) = canonical_special_result(value, pg_type, oid)? else {
        return unsupported_binary_result(oid, value);
    };
    let mut output = Vec::with_capacity(20);
    match network.address {
        PgIpAddress::V4(address) => {
            output.extend([2, network.prefix, u8::from(oid == 650), 4]);
            output.extend(address);
        }
        PgIpAddress::V6(address) => {
            output.extend([3, network.prefix, u8::from(oid == 650), 16]);
            output.extend(address);
        }
    }
    Ok(output)
}

pub(crate) fn mac_result(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    let pg_type = if oid == 774 { "macaddr8" } else { "macaddr" };
    match canonical_special_result(value, pg_type, oid)? {
        PgCanonicalValue::MacAddress(PgMacAddress::Mac48(address)) if oid == 829 => {
            Ok(address.to_vec())
        }
        PgCanonicalValue::MacAddress(PgMacAddress::Mac64(address)) if oid == 774 => {
            Ok(address.to_vec())
        }
        _ => unsupported_binary_result(oid, value),
    }
}

pub(crate) fn geometric_result(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    let pg_type = bicdb_sql::pg_type_spec_by_oid(oid)
        .map(|spec| spec.name)
        .ok_or_else(|| PgWireError::Protocol(format!("unknown geometric oid {oid}")))?;
    let PgCanonicalValue::Geometric(geometry) = canonical_special_result(value, pg_type, oid)?
    else {
        return unsupported_binary_result(oid, value);
    };
    let mut output = Vec::new();
    let push_float = |output: &mut Vec<u8>, value: PgFloat8| {
        output.extend_from_slice(&value.to_value().to_be_bytes());
    };
    let push_point = |output: &mut Vec<u8>, point: PgPoint| {
        push_float(output, point.x);
        push_float(output, point.y);
    };
    match geometry {
        PgGeometric::Point(point) => push_point(&mut output, point),
        PgGeometric::Line { a, b, c } => {
            push_float(&mut output, a);
            push_float(&mut output, b);
            push_float(&mut output, c);
        }
        PgGeometric::LineSegment { start, end } => {
            push_point(&mut output, start);
            push_point(&mut output, end);
        }
        PgGeometric::Box { high, low } => {
            push_point(&mut output, high);
            push_point(&mut output, low);
        }
        PgGeometric::Path { closed, points } => {
            output.push(u8::from(closed));
            output.extend_from_slice(&(points.len() as i32).to_be_bytes());
            for point in points {
                push_point(&mut output, point);
            }
        }
        PgGeometric::Polygon { points } => {
            output.extend_from_slice(&(points.len() as i32).to_be_bytes());
            for point in points {
                push_point(&mut output, point);
            }
        }
        PgGeometric::Circle { center, radius } => {
            push_point(&mut output, center);
            push_float(&mut output, radius);
        }
    }
    Ok(output)
}

pub(crate) fn oid_alias_result(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    let value = integer_result(value, oid)?;
    u32::try_from(value)
        .map(|value| value.to_be_bytes().to_vec())
        .map_err(|_| binary_result_range_error(oid, value))
}

pub(crate) fn lsn_result(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    let PgCanonicalValue::Lsn(value) = canonical_special_result(value, "pg_lsn", oid)? else {
        return unsupported_binary_result(oid, value);
    };
    Ok(value.to_be_bytes().to_vec())
}

pub(crate) fn range_result(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    let pg_type = bicdb_sql::pg_type_spec_by_oid(oid)
        .map(|spec| spec.name)
        .ok_or_else(|| PgWireError::Protocol(format!("unknown range oid {oid}")))?;
    let PgCanonicalValue::Range(range) = canonical_special_result(value, pg_type, oid)? else {
        return unsupported_binary_result(oid, value);
    };
    encode_pg_range(&range, oid)
}

pub(crate) fn encode_pg_range(range: &PgRange, range_oid: i32) -> Result<Vec<u8>> {
    let subtype_oid = range_subtype_oid_for_range_oid(range_oid).ok_or_else(|| {
        PgWireError::Protocol(format!("range oid {range_oid} has no registered subtype"))
    })?;
    encode_pg_range_inner(range, range_oid, subtype_oid)
}

pub(crate) fn encode_pg_range_inner(
    range: &PgRange,
    _range_oid: i32,
    subtype_oid: i32,
) -> Result<Vec<u8>> {
    if range.empty {
        return Ok(vec![RANGE_EMPTY]);
    }
    let mut flags = 0_u8;
    flags |= match &range.lower {
        PgRangeBound::Unbounded => RANGE_LB_INF,
        PgRangeBound::Inclusive(_) => RANGE_LB_INC,
        PgRangeBound::Exclusive(_) => 0,
    };
    flags |= match &range.upper {
        PgRangeBound::Unbounded => RANGE_UB_INF,
        PgRangeBound::Inclusive(_) => RANGE_UB_INC,
        PgRangeBound::Exclusive(_) => 0,
    };
    let mut output = vec![flags];
    for bound in [&range.lower, &range.upper] {
        let value = match bound {
            PgRangeBound::Unbounded => continue,
            PgRangeBound::Inclusive(value) | PgRangeBound::Exclusive(value) => value.as_ref(),
        };
        let encoded = encode_pg_range_bound(value, subtype_oid)?;
        output.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
        output.extend(encoded);
    }
    Ok(output)
}

pub(crate) fn encode_pg_range_bound(value: &PgCanonicalValue, subtype_oid: i32) -> Result<Vec<u8>> {
    match (value, subtype_oid) {
        (PgCanonicalValue::Int4(value), 23) => Ok(value.to_be_bytes().to_vec()),
        (PgCanonicalValue::Int8(value), 20) => Ok(value.to_be_bytes().to_vec()),
        (PgCanonicalValue::Numeric(value), 1700) => {
            encode_binary_numeric_result(&SqlValue::String(value.to_decimal_text()))
        }
        (PgCanonicalValue::Date(value), 1082) => {
            let days = match value {
                PgDate::NegativeInfinity => i32::MIN,
                PgDate::Finite(days) => *days,
                PgDate::PositiveInfinity => i32::MAX,
            };
            Ok(days.to_be_bytes().to_vec())
        }
        (PgCanonicalValue::Timestamp(value), 1114)
        | (PgCanonicalValue::TimestampTz(value), 1184) => {
            let micros = match value {
                PgTimestamp::NegativeInfinity => i64::MIN,
                PgTimestamp::Finite(micros) => *micros,
                PgTimestamp::PositiveInfinity => i64::MAX,
            };
            Ok(micros.to_be_bytes().to_vec())
        }
        _ => Err(PgWireError::Protocol(format!(
            "range subtype oid {subtype_oid} cannot encode {value:?}"
        ))),
    }
}

pub(crate) fn multirange_result(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    let pg_type = bicdb_sql::pg_type_spec_by_oid(oid)
        .map(|spec| spec.name)
        .ok_or_else(|| PgWireError::Protocol(format!("unknown multirange oid {oid}")))?;
    let PgCanonicalValue::Multirange(ranges) = canonical_special_result(value, pg_type, oid)?
    else {
        return unsupported_binary_result(oid, value);
    };
    let range_oid = range_oid_for_multirange_oid(oid)
        .ok_or_else(|| PgWireError::Protocol(format!("multirange oid {oid} has no range type")))?;
    let subtype_oid = range_subtype_oid_for_range_oid(range_oid).ok_or_else(|| {
        PgWireError::Protocol(format!("range oid {range_oid} has no registered subtype"))
    })?;
    encode_pg_multirange_inner(&ranges, oid, range_oid, subtype_oid)
}

pub(crate) fn encode_pg_multirange_inner(
    ranges: &[PgRange],
    multirange_oid: i32,
    range_oid: i32,
    subtype_oid: i32,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let count = i32::try_from(ranges.len()).map_err(|_| {
        PgWireError::Protocol(format!(
            "multirange oid {multirange_oid} has too many component ranges"
        ))
    })?;
    output.extend_from_slice(&count.to_be_bytes());
    for range in ranges {
        let encoded = encode_pg_range_inner(range, range_oid, subtype_oid)?;
        output.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
        output.extend(encoded);
    }
    Ok(output)
}

pub(crate) fn snapshot_result(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    let pg_type = if oid == 2970 {
        "txid_snapshot"
    } else {
        "pg_snapshot"
    };
    let PgCanonicalValue::Snapshot(snapshot) = canonical_special_result(value, pg_type, oid)?
    else {
        return unsupported_binary_result(oid, value);
    };
    let count = i32::try_from(snapshot.in_progress.len()).map_err(|_| {
        PgWireError::Protocol("snapshot transaction count exceeds int32".to_string())
    })?;
    let mut output = Vec::with_capacity(20 + snapshot.in_progress.len() * 8);
    output.extend_from_slice(&count.to_be_bytes());
    output.extend_from_slice(&snapshot.xmin.to_be_bytes());
    output.extend_from_slice(&snapshot.xmax.to_be_bytes());
    for xid in snapshot.in_progress {
        output.extend_from_slice(&xid.to_be_bytes());
    }
    Ok(output)
}

pub(crate) fn encode_binary_array_result(value: &SqlValue, element_oid: i32) -> Result<Vec<u8>> {
    encode_binary_array_result_inner(value, element_oid, None)
}

pub(crate) fn encode_binary_array_result_inner(
    value: &SqlValue,
    element_oid: i32,
    db: Option<&BicDb>,
) -> Result<Vec<u8>> {
    let normalized = match value {
        SqlValue::String(value) => {
            let spec = bicdb_sql::pg_type_spec_by_oid(element_oid).ok_or_else(|| {
                PgWireError::Protocol(format!(
                    "array element oid {element_oid} requires a structured array value"
                ))
            })?;
            let array_oid = spec.array_oid.ok_or_else(|| {
                PgWireError::Protocol(format!("type {} has no array oid", spec.name))
            })?;
            bicdb_sql::pg_scalar_codec(spec)
                .parse_text(value, bicdb_sql::PgCodecContext::new(array_oid, -1))
                .map_err(|error| PgWireError::Protocol(error.to_string()))?
        }
        value => value.clone(),
    };
    let (dimensions, elements) = binary_array_result_parts(&normalized, element_oid)?;
    let has_nulls = elements.iter().any(JsonValue::is_null);

    let mut output = Vec::new();
    put_i32(&mut output, dimensions.len() as i32);
    put_i32(&mut output, i32::from(has_nulls));
    put_i32(&mut output, element_oid);
    for (length, lower_bound) in &dimensions {
        let length = i32::try_from(*length).map_err(|_| {
            PgWireError::Protocol("array dimension length exceeds int32".to_string())
        })?;
        put_i32(&mut output, length);
        put_i32(&mut output, *lower_bound);
    }

    for element in &elements {
        if element.is_null() {
            put_i32(&mut output, -1);
            continue;
        }
        let value = if let Some(composite) = bicdb_sql::pg_composite_from_array_json(element) {
            SqlValue::Composite(composite)
        } else if matches!(element_oid, 114 | 3802) {
            SqlValue::Json(element.clone())
        } else {
            match element {
                JsonValue::Bool(value) => SqlValue::Bool(*value),
                JsonValue::Number(value) if value.is_i64() => {
                    SqlValue::Int(value.as_i64().unwrap())
                }
                JsonValue::Number(value) => SqlValue::Float(value.as_f64().ok_or_else(|| {
                    PgWireError::Protocol(format!(
                        "array element cannot be represented for oid {element_oid}"
                    ))
                })?),
                JsonValue::String(value) => SqlValue::String(value.clone()),
                value => SqlValue::Json(value.clone()),
            }
        };
        let encoded = match db {
            Some(db) => encode_binary_result_value_with_db(db, &value, element_oid)?,
            None => encode_binary_result_value(&value, element_oid)?,
        };
        put_i32(&mut output, encoded.len() as i32);
        output.extend_from_slice(&encoded);
    }
    Ok(output)
}

pub(crate) fn binary_array_result_parts(
    value: &SqlValue,
    element_oid: i32,
) -> Result<(Vec<(usize, i32)>, Vec<JsonValue>)> {
    let SqlValue::Json(value) = value else {
        return Err(PgWireError::Protocol(format!(
            "array result with element oid {element_oid} requires an array value"
        )));
    };
    let (value, lower_bounds) = if let Some(input) = value.get("$bicdb_array_input") {
        let value = input
            .get("value")
            .filter(|value| value.is_array())
            .ok_or_else(|| {
                PgWireError::Protocol("array result envelope has no array value".to_string())
            })?;
        let lower_bounds = input
            .get("lower_bounds")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| {
                PgWireError::Protocol("array result envelope has no lower bounds".to_string())
            })?
            .iter()
            .map(|value| {
                value
                    .as_i64()
                    .and_then(|value| i32::try_from(value).ok())
                    .ok_or_else(|| {
                        PgWireError::Protocol(
                            "array result lower bound is outside int32".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        (value, Some(lower_bounds))
    } else {
        (value, None)
    };
    let lengths = binary_array_json_dimensions(value).ok_or_else(|| {
        PgWireError::Protocol("binary array result must be rectangular".to_string())
    })?;
    if lengths.len() > 6 {
        return Err(PgWireError::Protocol(format!(
            "binary array result rank {} exceeds PostgreSQL's limit of 6",
            lengths.len()
        )));
    }
    let lower_bounds = lower_bounds.unwrap_or_else(|| vec![1; lengths.len()]);
    if lower_bounds.len() != lengths.len() {
        return Err(PgWireError::Protocol(format!(
            "array result has {} lower bounds for rank {}",
            lower_bounds.len(),
            lengths.len()
        )));
    }
    let dimensions = lengths.into_iter().zip(lower_bounds).collect::<Vec<_>>();
    let expected = if dimensions.is_empty() {
        0
    } else {
        dimensions
            .iter()
            .try_fold(1_usize, |count, (length, _)| count.checked_mul(*length))
            .ok_or_else(|| {
                PgWireError::Protocol("array result element count overflow".to_string())
            })?
    };
    let mut elements = Vec::with_capacity(expected);
    if expected != 0 {
        flatten_binary_array_json(value, dimensions.len(), &mut elements)?;
    }
    if elements.len() != expected {
        return Err(PgWireError::Protocol(format!(
            "array result dimensions require {expected} elements but found {}",
            elements.len()
        )));
    }
    Ok((dimensions, elements))
}

pub(crate) fn binary_array_json_dimensions(value: &JsonValue) -> Option<Vec<usize>> {
    let JsonValue::Array(values) = value else {
        return Some(Vec::new());
    };
    if values.is_empty() {
        return Some(Vec::new());
    }
    let child = binary_array_json_dimensions(&values[0])?;
    if values
        .iter()
        .skip(1)
        .any(|value| binary_array_json_dimensions(value).as_ref() != Some(&child))
    {
        return None;
    }
    let mut dimensions = Vec::with_capacity(child.len() + 1);
    dimensions.push(values.len());
    dimensions.extend(child);
    Some(dimensions)
}

pub(crate) fn flatten_binary_array_json(
    value: &JsonValue,
    rank: usize,
    output: &mut Vec<JsonValue>,
) -> Result<()> {
    if rank == 0 {
        output.push(value.clone());
        return Ok(());
    }
    let values = value.as_array().ok_or_else(|| {
        PgWireError::Protocol("binary array result rank does not match its values".to_string())
    })?;
    for value in values {
        flatten_binary_array_json(value, rank - 1, output)?;
    }
    Ok(())
}

pub(crate) fn integer_result(value: &SqlValue, oid: i32) -> Result<i64> {
    match value {
        SqlValue::Int(value) => Ok(*value),
        SqlValue::String(value) => value
            .parse::<i64>()
            .map_err(|error| PgWireError::Protocol(error.to_string())),
        _ => unsupported_binary_result(oid, value),
    }
}

pub(crate) fn float_result(value: &SqlValue, oid: i32) -> Result<f64> {
    match value {
        SqlValue::Float(value) => Ok(*value),
        SqlValue::Int(value) => Ok(*value as f64),
        SqlValue::String(value) => value
            .parse::<f64>()
            .map_err(|error| PgWireError::Protocol(error.to_string())),
        _ => unsupported_binary_result(oid, value),
    }
}

pub(crate) fn timestamp_result(value: &SqlValue, oid: i32) -> Result<i64> {
    let timestamp = PgTimestamp::from_postgres_text(&value.to_cell(), oid == 1184)
        .map_err(|error| temporal_binary_error(oid, error))?;
    Ok(match timestamp {
        PgTimestamp::Finite(micros) => micros,
        PgTimestamp::PositiveInfinity => i64::MAX,
        PgTimestamp::NegativeInfinity => i64::MIN,
    })
}

pub(crate) fn date_result(value: &SqlValue, oid: i32) -> Result<i32> {
    let date = PgDate::from_iso_text(&value.to_cell())
        .map_err(|error| temporal_binary_error(oid, error))?;
    Ok(match date {
        PgDate::Finite(days) => days,
        PgDate::PositiveInfinity => i32::MAX,
        PgDate::NegativeInfinity => i32::MIN,
    })
}

pub(crate) fn time_result(value: &SqlValue, oid: i32) -> Result<i64> {
    PgTime::from_postgres_text(&value.to_cell())
        .map(PgTime::micros_since_midnight)
        .map_err(|error| temporal_binary_error(oid, error))
}

pub(crate) fn timetz_result(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    let value = PgTimeTz::from_postgres_text(&value.to_cell(), 0)
        .map_err(|error| temporal_binary_error(oid, error))?;
    let mut output = Vec::with_capacity(12);
    output.extend_from_slice(&value.time.micros_since_midnight().to_be_bytes());
    output.extend_from_slice(&(-value.utc_offset_seconds).to_be_bytes());
    Ok(output)
}

pub(crate) fn interval_result(value: &SqlValue, oid: i32) -> Result<Vec<u8>> {
    let value = PgInterval::from_postgres_text(&value.to_cell())
        .map_err(|error| temporal_binary_error(oid, error))?;
    let mut output = Vec::with_capacity(16);
    output.extend_from_slice(&value.micros.to_be_bytes());
    output.extend_from_slice(&value.days.to_be_bytes());
    output.extend_from_slice(&value.months.to_be_bytes());
    Ok(output)
}

pub(crate) fn temporal_type_size(oid: i32) -> Option<i16> {
    match oid {
        1082 => Some(4),
        1083 | 1114 | 1184 => Some(8),
        1266 => Some(12),
        1186 => Some(16),
        _ => None,
    }
}

pub(crate) fn decode_bytea_cell(value: &str) -> Vec<u8> {
    value
        .strip_prefix("\\x")
        .and_then(|hex| hex::decode(hex).ok())
        .unwrap_or_else(|| value.as_bytes().to_vec())
}

pub(crate) fn binary_result_range_error(oid: i32, value: i64) -> PgWireError {
    PgWireError::Protocol(format!(
        "binary result oid {oid} cannot encode out-of-range integer {value}"
    ))
}

pub(crate) fn unsupported_binary_result<T>(oid: i32, value: &SqlValue) -> Result<T> {
    Err(PgWireError::Protocol(format!(
        "unsupported binary result oid {oid} for value {}",
        value.to_cell()
    )))
}

pub(crate) fn first_non_null(result: &SqlResult, column: usize) -> Option<&SqlValue> {
    result
        .rows
        .iter()
        .filter_map(|row| row.get(column))
        .find(|value| !matches!(value, SqlValue::Null))
}

pub(crate) fn cstring_payload(payload: &[u8]) -> Result<&str> {
    let end = payload
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(payload.len());
    std::str::from_utf8(&payload[..end]).map_err(|error| PgWireError::Protocol(error.to_string()))
}

pub(crate) fn cstr(payload: &mut Vec<u8>, value: &str) {
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
}

pub(crate) fn put_i16(payload: &mut Vec<u8>, value: i16) {
    payload.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_i32(payload: &mut Vec<u8>, value: i32) {
    payload.extend_from_slice(&value.to_be_bytes());
}

#[derive(Debug)]
pub(crate) enum AutomaticPagedCheckpointTick {
    Idle,
    Paused { operation_id: Uuid },
    Advanced(PagedCheckpointScheduleAdvance),
}
