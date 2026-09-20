//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn carrier_workflow_service_run_arguments(
    workflow: &str,
    payload: &serde_json::Map<String, Value>,
    operation: &str,
) -> Result<Vec<(Option<String>, Value)>> {
    let mut arguments = vec![(None, Value::String(workflow.to_string()))];
    arguments.extend(carrier_workflow_service_run_id(payload, operation)?);
    Ok(arguments)
}

pub(crate) fn expect_carrier_handle(value: HostValue) -> Result<HostHandle> {
    match value {
        HostValue::Handle(handle) => Ok(handle),
        other => Err(AppRuntimeError::Invocation(format!(
            "BicDB application capability host returned {other:?}, expected a handle"
        ))),
    }
}

pub(crate) fn expect_carrier_rows(value: HostValue) -> Result<Vec<Value>> {
    match value {
        HostValue::Rows(rows) => Ok(rows),
        other => Err(AppRuntimeError::Invocation(format!(
            "BicDB application capability host returned {other:?}, expected rows"
        ))),
    }
}

pub(crate) fn shape_vector_match(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    method: &str,
    row: Value,
) -> Result<Value> {
    let mut row = row.as_object().cloned().ok_or_else(|| {
        AppRuntimeError::Invocation("vector search returned a non-object row".to_string())
    })?;
    let score = row.remove("_score").and_then(|value| value.as_f64());
    let vector_score = row.remove("_vector_score").and_then(|value| value.as_f64());
    let text_score = row.remove("_text_score").and_then(|value| value.as_f64());
    let doc = redact_resource(host, contract, Value::Object(row))?;
    match method {
        "similar" => Ok(doc),
        "similar_with_scores" => Ok(json!({
            "doc": doc,
            "score": score.ok_or_else(|| AppRuntimeError::Invocation(
                "scored vector search omitted its score".to_string()
            ))?,
        })),
        "hybrid_search" => Ok(json!({
            "doc": doc,
            "score": score.ok_or_else(|| AppRuntimeError::Invocation(
                "hybrid search omitted its combined score".to_string()
            ))?,
            "vector_score": vector_score.ok_or_else(|| AppRuntimeError::Invocation(
                "hybrid search omitted its vector score".to_string()
            ))?,
            "text_score": text_score.ok_or_else(|| AppRuntimeError::Invocation(
                "hybrid search omitted its text score".to_string()
            ))?,
        })),
        _ => Err(AppRuntimeError::InvalidPackage(format!(
            "unknown BicDB application vector model method `{method}`"
        ))),
    }
}

pub(crate) fn expect_carrier_u64(value: HostValue) -> Result<u64> {
    match value {
        HostValue::U64(value) => Ok(value),
        other => Err(AppRuntimeError::Invocation(format!(
            "BicDB application capability host returned {other:?}, expected an unsigned integer"
        ))),
    }
}

pub(crate) fn workflow_name_and_run_id(
    arguments: &[(Option<String>, Value)],
    target: &str,
    workflow_index: usize,
) -> Result<(String, String)> {
    if arguments.iter().any(|(name, _)| name.is_some()) {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "builtin `{target}` accepts positional arguments only"
        )));
    }
    let workflow = carrier_string(
        arguments
            .get(workflow_index)
            .map(|(_, value)| value.clone())
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest(format!("builtin `{target}` lacks a workflow name"))
            })?,
        target,
    )?;
    let run_id = carrier_string(
        arguments
            .get(workflow_index + 1)
            .map(|(_, value)| value.clone())
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest(format!("builtin `{target}` lacks a run id"))
            })?,
        target,
    )?;
    uuid::Uuid::parse_str(&run_id).map_err(|_| {
        AppRuntimeError::InvalidRequest(format!("builtin `{target}` has an invalid run id"))
    })?;
    Ok((workflow, run_id))
}

pub(crate) fn carrier_argument(
    arguments: &[(Option<String>, Value)],
    index: usize,
    name: Option<&str>,
    target: &str,
) -> Result<Value> {
    name.and_then(|name| {
        arguments
            .iter()
            .find(|(argument_name, _)| argument_name.as_deref() == Some(name))
            .map(|(_, value)| value.clone())
    })
    .or_else(|| arguments.get(index).map(|(_, value)| value.clone()))
    .ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!("builtin `{target}` requires argument {index}"))
    })
}

pub(crate) fn carrier_string(value: Value, target: &str) -> Result<String> {
    value.as_str().map(str::to_string).ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!("builtin `{target}` requires a string"))
    })
}

pub(crate) fn validate_carrier_virtual_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > 1_024
        || path.contains('\\')
        || path.split('/').any(|part| part == "..")
        || path.chars().any(char::is_control)
    {
        return Err(AppRuntimeError::InvalidRequest(
            "BicDB application virtual file path is empty, oversized, or contains traversal"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_blob_download_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 255
        || name.contains(['/', '\\', '"'])
        || name.chars().any(char::is_control)
    {
        return Err(AppRuntimeError::InvalidRequest(
            "blob download_name is unsafe for Content-Disposition".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn carrier_blob_metadata_value(key: &str, metadata: &BlobMetadata) -> Value {
    json!({
        "key": key,
        "size_bytes": i64::try_from(metadata.size).unwrap_or(i64::MAX),
        "content_type": metadata.content_type.clone().unwrap_or_else(|| {
            mime_guess::from_path(key)
                .first_raw()
                .unwrap_or("application/octet-stream")
                .to_string()
        }),
        "sha256": metadata.sha256,
        "etag": metadata.sha256,
        "last_modified": metadata.last_modified,
    })
}

pub(crate) fn carrier_password_policy(password: &str) -> Value {
    let length = password.chars().count() as i64;
    let has_uppercase = password
        .chars()
        .any(|character| character.is_ascii_uppercase());
    let has_lowercase = password
        .chars()
        .any(|character| character.is_ascii_lowercase());
    let has_number = password.chars().any(|character| character.is_ascii_digit());
    let has_symbol = password
        .chars()
        .any(|character| character.is_ascii_punctuation());
    json!({
        "valid": length >= 12 && has_uppercase && has_lowercase && has_number && has_symbol,
        "has_uppercase": has_uppercase,
        "has_lowercase": has_lowercase,
        "has_number": has_number,
        "has_symbol": has_symbol,
        "min_length": 12,
        "length": length,
    })
}

pub(crate) fn carrier_validate_password(password: &str) -> Result<()> {
    if carrier_password_policy(password)
        .get("valid")
        .and_then(Value::as_bool)
        == Some(true)
    {
        Ok(())
    } else {
        Err(AppRuntimeError::InvalidRequest(
            "password must contain at least 12 characters with uppercase, lowercase, number, and symbol characters"
                .to_string(),
        ))
    }
}

pub(crate) fn carrier_verify_password(password_hash: &str, password: &str) -> Result<bool> {
    let parsed = PasswordHash::new(password_hash).map_err(|error| {
        AppRuntimeError::InvalidRequest(format!("invalid password hash: {error}"))
    })?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}

pub(crate) fn carrier_base32_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut output = String::new();
    let mut buffer = 0_u16;
    let mut bits = 0_u8;
    for byte in bytes {
        buffer = (buffer << 8) | u16::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            output.push(ALPHABET[((buffer >> bits) & 0x1f) as usize] as char);
        }
    }
    if bits > 0 {
        output.push(ALPHABET[((buffer << (5 - bits)) & 0x1f) as usize] as char);
    }
    output
}

pub(crate) fn carrier_base32_decode(value: &str) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut buffer = 0_u32;
    let mut bits = 0_u8;
    for character in value
        .chars()
        .filter(|character| !matches!(character, ' ' | '-'))
    {
        let digit = match character {
            'A'..='Z' => character as u8 - b'A',
            'a'..='z' => character as u8 - b'a',
            '2'..='7' => character as u8 - b'2' + 26,
            '=' => continue,
            _ => {
                return Err(AppRuntimeError::InvalidRequest(
                    "TOTP secret must be valid base32".to_string(),
                ));
            }
        };
        buffer = (buffer << 5) | u32::from(digit);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            output.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    if output.is_empty() {
        return Err(AppRuntimeError::InvalidRequest(
            "TOTP secret must be valid base32".to_string(),
        ));
    }
    Ok(output)
}

pub(crate) fn carrier_validate_totp(period: i64, digits: i64) -> Result<()> {
    if period <= 0 || !(6..=8).contains(&digits) {
        return Err(AppRuntimeError::InvalidRequest(
            "TOTP period must be positive and digits must be between 6 and 8".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn carrier_totp_code(
    secret: &str,
    now: i64,
    period: i64,
    digits: i64,
) -> Result<String> {
    carrier_validate_totp(period, digits)?;
    let secret = carrier_base32_decode(secret)?;
    let counter = now.div_euclid(period) as u64;
    let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(&secret)
        .map_err(|error| AppRuntimeError::Invocation(error.to_string()))?;
    mac.update(&counter.to_be_bytes());
    let digest = mac.finalize().into_bytes();
    let offset = usize::from(digest[digest.len() - 1] & 0x0f);
    let binary = ((u32::from(digest[offset]) & 0x7f) << 24)
        | (u32::from(digest[offset + 1]) << 16)
        | (u32::from(digest[offset + 2]) << 8)
        | u32::from(digest[offset + 3]);
    let modulo = 10_u32.pow(digits as u32);
    Ok(format!(
        "{:0width$}",
        binary % modulo,
        width = digits as usize
    ))
}

pub(crate) fn carrier_constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

pub(crate) fn carrier_time_string(epoch_seconds: i64) -> Result<String> {
    DateTime::<Utc>::from_timestamp(epoch_seconds, 0)
        .map(|value| value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .ok_or_else(|| AppRuntimeError::InvalidRequest("invalid security timestamp".to_string()))
}

pub(crate) fn carrier_append_oauth_params(
    query: &mut url::form_urlencoded::Serializer<'_, url::UrlQuery<'_>>,
    value: &Value,
) -> Result<()> {
    let fields = value.as_object().ok_or_else(|| {
        AppRuntimeError::InvalidRequest(
            "auth.oauth_authorize extra_params must be an object".to_string(),
        )
    })?;
    for (name, value) in fields {
        match value {
            Value::String(value) => {
                query.append_pair(name, value);
            }
            Value::Bool(value) => {
                query.append_pair(name, if *value { "true" } else { "false" });
            }
            Value::Number(value) => {
                query.append_pair(name, &value.to_string());
            }
            Value::Array(values) => {
                for value in values {
                    let value = value.as_str().ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "OAuth extra parameter arrays must contain strings".to_string(),
                        )
                    })?;
                    query.append_pair(name, value);
                }
            }
            _ => {
                return Err(AppRuntimeError::InvalidRequest(
                    "OAuth extra parameter values must be scalar or string arrays".to_string(),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn carrier_integer(value: Value, target: &str) -> Result<i64> {
    value.as_i64().ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!("builtin `{target}` requires an integer"))
    })
}

pub(crate) fn carrier_number(value: Value, target: &str) -> Result<f64> {
    value.as_f64().ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!("builtin `{target}` requires a number"))
    })
}

pub(crate) fn carrier_model_limit(
    named: &mut BTreeMap<String, Value>,
    method: &str,
) -> Result<u32> {
    let limit = named
        .remove("limit")
        .and_then(|value| value.as_i64())
        .ok_or_else(|| {
            AppRuntimeError::InvalidRequest(format!(
                "BicDB application model.{method} requires an integer limit"
            ))
        })?;
    if !(1..=10_000).contains(&limit) {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "BicDB application {method} limit must be in 1..=10000"
        )));
    }
    Ok(limit as u32)
}

pub(crate) fn carrier_spatial_field(
    contract: &ResourceContractV1,
    expected: ApplicationGeometryTypeV1,
) -> Result<String> {
    let mut fields = contract
        .fields
        .iter()
        .filter_map(|field| match field.field_type {
            FieldType::Geometry {
                geometry_type: Some(actual),
                ..
            } if actual == expected => Some(field.name.clone()),
            _ => None,
        });
    let field = fields.next().ok_or_else(|| {
        AppRuntimeError::InvalidPackage(format!(
            "BicDB application model `{}` has no signed {expected:?} field",
            contract.name
        ))
    })?;
    if fields.next().is_some() {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "BicDB application model `{}` has multiple signed {expected:?} fields",
            contract.name
        )));
    }
    Ok(field)
}

pub(crate) fn carrier_vector(value: Value, target: &str) -> Result<Vec<f32>> {
    value
        .as_array()
        .ok_or_else(|| {
            AppRuntimeError::InvalidRequest(format!(
                "BicDB application model `{target}` requires a vector array"
            ))
        })?
        .iter()
        .map(|value| {
            let value = value.as_f64().ok_or_else(|| {
                AppRuntimeError::InvalidRequest(format!(
                    "BicDB application model `{target}` vector contains a non-number"
                ))
            })?;
            let value = value as f32;
            value.is_finite().then_some(value).ok_or_else(|| {
                AppRuntimeError::InvalidRequest(format!(
                    "BicDB application model `{target}` vector contains a non-finite number"
                ))
            })
        })
        .collect()
}

pub(crate) fn carrier_optional_weight(
    value: Option<Value>,
    default: f64,
    target: &str,
) -> Result<f64> {
    match value {
        None => Ok(default),
        Some(value) => value
            .as_f64()
            .filter(|value| value.is_finite())
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest(format!(
                    "BicDB application model `{target}` requires a finite Float"
                ))
            }),
    }
}

pub(crate) fn carrier_positive_size(value: Value, target: &str) -> Result<usize> {
    let value = carrier_integer(value, target)?;
    if value <= 0 || value > 65_536 {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "builtin `{target}` size must be between 1 and 65536"
        )));
    }
    Ok(value as usize)
}

pub(crate) fn carrier_optional_object(
    value: Option<Value>,
    target: &str,
) -> Result<BTreeMap<String, Value>> {
    match value {
        None | Some(Value::Null) => Ok(BTreeMap::new()),
        Some(Value::Object(value)) => Ok(value.into_iter().collect()),
        Some(_) => Err(AppRuntimeError::InvalidRequest(format!(
            "builtin `{target}` metadata must be an object"
        ))),
    }
}

pub(crate) fn carrier_display(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

pub(crate) fn carrier_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn carrier_host_type_error<T>(
    target: &str,
    expected: &str,
    value: HostValue,
) -> Result<T> {
    Err(AppRuntimeError::Invocation(format!(
        "BicDB application builtin `{target}` received {value:?} from BicDB, expected {expected}"
    )))
}

pub(crate) fn carrier_host_error(error: bicdb_extension::abi_v2::HostError) -> AppRuntimeError {
    if error.class == ErrorClass::InvalidRequest && error.code != "invalid_request" {
        return AppRuntimeError::ApplicationFailure {
            code: error.code,
            message: error.message,
            retryable: error.retryable,
        };
    }
    let detail = format!(
        "{} ({:?}, trace {}): {}",
        error.code, error.class, error.trace_id, error.message
    );
    match error.class {
        ErrorClass::Unauthenticated => AppRuntimeError::Authentication(detail),
        ErrorClass::Unauthorized | ErrorClass::PolicyDenied | ErrorClass::MutationGrantDenied => {
            AppRuntimeError::CapabilityDenied(detail)
        }
        ErrorClass::NotFound => AppRuntimeError::NotFound(detail),
        ErrorClass::Conflict | ErrorClass::CommitValidation => AppRuntimeError::Conflict(detail),
        ErrorClass::OptimisticConflict => AppRuntimeError::OptimisticConflict(detail),
        ErrorClass::Timeout => AppRuntimeError::Timeout(detail),
        ErrorClass::Cancelled => AppRuntimeError::Cancelled(detail),
        ErrorClass::ResourceExhausted => AppRuntimeError::ResourceExhausted(detail),
        ErrorClass::RateLimited => AppRuntimeError::RateLimited(detail),
        ErrorClass::Provider => AppRuntimeError::Provider(detail),
        ErrorClass::DependencyUnavailable | ErrorClass::DependencyIncompatible => {
            AppRuntimeError::NotReady(detail)
        }
        ErrorClass::Package | ErrorClass::Migration | ErrorClass::Activation => {
            AppRuntimeError::InvalidPackage(detail)
        }
        ErrorClass::InvalidRequest | ErrorClass::Constraint | ErrorClass::Internal => {
            AppRuntimeError::Invocation(detail)
        }
    }
}

pub(crate) fn carrier_client_url(
    client: &str,
    base_url: &str,
    path: &str,
    query: Option<Value>,
) -> Result<String> {
    let mut base = Url::parse(base_url).map_err(|error| {
        AppRuntimeError::InvalidPackage(format!(
            "BicDB application client `{client}` has an invalid signed base URL: {error}"
        ))
    })?;
    if !base.path().ends_with('/') {
        base.set_path(&format!("{}/", base.path()));
    }
    let mut joined = base.join(path.trim_start_matches('/')).map_err(|error| {
        AppRuntimeError::InvalidRequest(format!(
            "BicDB application client `{client}` path is invalid: {error}"
        ))
    })?;
    if joined.scheme() != base.scheme()
        || joined.host_str() != base.host_str()
        || joined.port_or_known_default() != base.port_or_known_default()
        || !joined.path().starts_with(base.path())
    {
        return Err(AppRuntimeError::CapabilityDenied(format!(
            "BicDB application client `{client}` path escaped its signed base URL"
        )));
    }
    if let Some(query) = query {
        let object = query.as_object().ok_or_else(|| {
            AppRuntimeError::InvalidRequest(format!(
                "BicDB application client `{client}` query must be an object"
            ))
        })?;
        let mut fields = object.iter().collect::<Vec<_>>();
        fields.sort_by(|(left, _), (right, _)| left.cmp(right));
        let mut pairs = joined.query_pairs_mut();
        for (name, value) in fields {
            match value {
                Value::Null => {}
                Value::Array(items) => {
                    for item in items {
                        pairs.append_pair(name, &carrier_query_scalar(client, item)?);
                    }
                }
                value => {
                    pairs.append_pair(name, &carrier_query_scalar(client, value)?);
                }
            }
        }
    }
    Ok(joined.to_string())
}

pub(crate) fn carrier_client_relative_url(
    client: &str,
    path: &str,
    query: Option<Value>,
) -> Result<String> {
    if path.starts_with("//") || path.contains('\\') || path.chars().any(char::is_control) {
        return Err(AppRuntimeError::CapabilityDenied(format!(
            "BicDB application client `{client}` path is not a safe provider-relative path"
        )));
    }
    const ROOT: &str = "/__carrier_provider_root__/";
    let joined = carrier_client_url(
        client,
        "https://carrier-provider.invalid/__carrier_provider_root__/",
        path,
        query,
    )?;
    let parsed = Url::parse(&joined).map_err(|_| {
        AppRuntimeError::InvalidRequest(format!(
            "BicDB application client `{client}` path is invalid"
        ))
    })?;
    let suffix = parsed.path().strip_prefix(ROOT).ok_or_else(|| {
        AppRuntimeError::CapabilityDenied(format!(
            "BicDB application client `{client}` path escaped its provider root"
        ))
    })?;
    let mut relative = format!("/{suffix}");
    if let Some(query) = parsed.query() {
        relative.push('?');
        relative.push_str(query);
    }
    Ok(relative)
}

pub(crate) fn carrier_query_scalar(client: &str, value: &Value) -> Result<String> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Bool(_) | Value::Number(_) => Ok(value.to_string()),
        _ => Err(AppRuntimeError::InvalidRequest(format!(
            "BicDB application client `{client}` query values must be scalar or arrays of scalars"
        ))),
    }
}

pub(crate) fn carrier_resource_request(operation: ResourceOperation) -> ResourceRequest {
    ResourceRequest {
        operation,
        id: None,
        body: Value::Null,
        filters: Vec::new(),
        relation_filters: BTreeMap::new(),
        sort: Vec::new(),
        search: None,
        limit: 50,
        offset: 0,
        scope: ResourceRecordScope::Active,
        expected_version: None,
        idempotency_key: None,
        include_total: false,
        internal_model_call: false,
    }
}

pub(crate) fn carrier_resource_total(response: &ResourceResponse) -> Result<u64> {
    response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-bicdb-result-total"))
        .map(|(_, value)| {
            value.parse::<u64>().map_err(|_| {
                AppRuntimeError::InvalidPackage(
                    "BicDB application model operation returned an invalid total".to_string(),
                )
            })
        })
        .transpose()?
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "BicDB application model operation did not return an exact total".to_string(),
            )
        })
}

pub(crate) fn carrier_scalar_id(value: Value, target: &str, method: &str) -> Result<String> {
    match value {
        Value::String(value) => Ok(value),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(AppRuntimeError::InvalidRequest(format!(
            "BicDB application model id for `{target}.{method}` must be a string or number"
        ))),
    }
}

pub(crate) fn carrier_canonical_json(value: &Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(name, value)| (name.clone(), carrier_canonical_json(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(carrier_canonical_json).collect()),
        other => other.clone(),
    }
}

pub(crate) fn carrier_idempotency_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn carrier_workflow_plan_sha256(
    definition: &ApplicationWorkflowDefinitionV1,
) -> Result<String> {
    Ok(Sha256::digest(serde_json::to_vec(&json!({
        "queue": definition.queue,
        "worker_export": definition.worker_export,
        "timeout_ms": definition.timeout_ms,
        "max_retries": definition.max_retries,
        "max_parallelism": definition.max_parallelism,
        "graph_execution": definition.graph_execution,
        "return_step": definition.return_step,
        "steps": definition.steps,
        "slas": definition.slas,
        "invariants": definition.invariants,
    }))?)
    .iter()
    .map(|byte| format!("{byte:02x}"))
    .collect())
}

pub(crate) fn carrier_flag_bucket(name: &str, group: &str) -> u8 {
    let digest = Sha256::digest(format!("{name}\0{group}").as_bytes());
    (u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) % 100) as u8
}

pub(crate) fn carrier_feature_flag_enabled(
    name: &str,
    definition: &ApplicationFlagDefinitionV1,
    actor: &ActorContext,
) -> bool {
    let mut resolved = definition.default;
    for rule in &definition.rules {
        match rule {
            ApplicationFlagRuleV1::TenantIn { tenants, value }
                if actor
                    .tenant_id
                    .as_ref()
                    .is_some_and(|tenant| tenants.contains(tenant)) =>
            {
                resolved = *value;
                break;
            }
            ApplicationFlagRuleV1::Percentage {
                percent,
                grouped_by,
                value,
            } => {
                let group = match grouped_by {
                    ApplicationFlagGroupV1::UserId => actor.user_id.as_ref(),
                    ApplicationFlagGroupV1::TenantId => actor.tenant_id.as_ref(),
                    ApplicationFlagGroupV1::WorkspaceId => actor.workspace_id.as_ref(),
                };
                if group.is_some_and(|group| carrier_flag_bucket(name, group) < *percent) {
                    resolved = *value;
                    break;
                }
            }
            ApplicationFlagRuleV1::TenantIn { .. } => {}
        }
    }
    resolved
}

pub(crate) fn take_u64(
    values: &mut BTreeMap<String, Value>,
    name: &str,
    default: u64,
) -> Result<u64> {
    values
        .remove(name)
        .filter(|value| !value.is_null())
        .map(|value| {
            value.as_u64().ok_or_else(|| {
                AppRuntimeError::InvalidRequest(format!(
                    "BicDB application `{name}` must be a positive integer"
                ))
            })
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

pub(crate) fn carrier_evaluation_actor(
    program: Option<&ApplicationProgramV1>,
    auth: Option<&ApplicationEvaluationAuthV1>,
    operator: &ActorContext,
) -> Result<ActorContext> {
    let Some(auth) = auth else {
        return Ok(operator.clone());
    };
    let evaluate = |expression| evaluate_carrier_expression(program, expression, BTreeMap::new());
    let id = evaluate(&auth.id)?
        .as_i64()
        .map(|value| value.to_string())
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "BicDB application evaluation auth id did not evaluate to Int".to_string(),
            )
        })?;
    let email = evaluate(&auth.email)?
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "BicDB application evaluation auth email did not evaluate to a non-empty String"
                    .to_string(),
            )
        })?;
    let name = evaluate(&auth.name)?
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "BicDB application evaluation auth name did not evaluate to a non-empty String"
                    .to_string(),
            )
        })?;
    let roles = evaluate(&auth.roles)?
        .as_array()
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "BicDB application evaluation auth roles did not evaluate to String[]".to_string(),
            )
        })?
        .iter()
        .map(|role| {
            role.as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(
                        "BicDB application evaluation auth roles contain a non-string or empty role"
                            .to_string(),
                    )
                })
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let tenant_id = auth
        .tenant_id
        .as_ref()
        .map(|expression| evaluate(expression))
        .transpose()?
        .map(|value| match value {
            Value::Null => Ok(None),
            Value::String(value) if !value.is_empty() => Ok(Some(value)),
            _ => Err(AppRuntimeError::InvalidPackage(
                "BicDB application evaluation auth tenant_id did not evaluate to String?"
                    .to_string(),
            )),
        })
        .transpose()?
        .flatten();
    let mut actor = operator.clone();
    actor.user_id = Some(id);
    actor.service_id = None;
    actor.client_id = None;
    actor.acting_client_id = None;
    actor.authentication_method = Some("carrier_evaluation".to_string());
    actor.roles = roles;
    actor.scopes.clear();
    actor.tenant_id = tenant_id;
    actor.workspace_id = None;
    actor.organization_id = None;
    actor.session_id = None;
    actor.delegation_chain.clear();
    actor.assurance_level = None;
    actor.policy_attributes.insert("email".to_string(), email);
    actor.policy_attributes.insert("name".to_string(), name);
    actor.validate()?;
    Ok(actor)
}

pub(crate) fn carrier_test_case(
    value_type: &ApplicationRouteParameterTypeV1,
    seed: u64,
    index: u32,
) -> Value {
    fn mix(mut value: u64) -> u64 {
        value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn generate(value_type: &ApplicationRouteParameterTypeV1, state: u64, depth: usize) -> Value {
        if depth > 16 {
            return Value::Null;
        }
        let choice = mix(state);
        match value_type {
            ApplicationRouteParameterTypeV1::String => Value::String(
                ["", "carrier", "héllo 世界", "line\nbreak", "  spaced  "][(choice as usize) % 5]
                    .to_string(),
            ),
            ApplicationRouteParameterTypeV1::Int => {
                Value::from([i64::MIN + 1, -1, 0, 1, i64::MAX][(choice as usize) % 5])
            }
            ApplicationRouteParameterTypeV1::Float => {
                Value::from([-1_000_000.25, -0.0, 0.0, 1.5, 1_000_000.25][(choice as usize) % 5])
            }
            ApplicationRouteParameterTypeV1::Decimal => Value::String(
                ["-1000000.25", "-0.01", "0", "1.2300", "999999.99"][(choice as usize) % 5]
                    .to_string(),
            ),
            ApplicationRouteParameterTypeV1::Bool => Value::Bool(choice & 1 == 1),
            ApplicationRouteParameterTypeV1::Json => {
                json!({"case": choice % 17, "enabled": choice & 1 == 1})
            }
            ApplicationRouteParameterTypeV1::Timestamp => {
                Value::String("2024-02-29T12:34:56Z".to_string())
            }
            ApplicationRouteParameterTypeV1::Date => Value::String("2024-02-29".to_string()),
            ApplicationRouteParameterTypeV1::LocalDateTime => {
                Value::String("2024-02-29T12:34:56".to_string())
            }
            ApplicationRouteParameterTypeV1::TimeZone => {
                Value::String("America/Los_Angeles".to_string())
            }
            ApplicationRouteParameterTypeV1::Uuid => Value::String(
                uuid::Uuid::from_u128((u128::from(choice) << 64) | u128::from(mix(choice ^ 0x74)))
                    .to_string(),
            ),
            ApplicationRouteParameterTypeV1::Enum { values } => values
                .iter()
                .nth((choice as usize) % values.len().max(1))
                .cloned()
                .map(Value::String)
                .unwrap_or(Value::Null),
            ApplicationRouteParameterTypeV1::List { element }
            | ApplicationRouteParameterTypeV1::Set { element } => Value::Array(
                (0..(choice % 4))
                    .map(|item| generate(element, mix(choice ^ item), depth + 1))
                    .collect(),
            ),
            ApplicationRouteParameterTypeV1::Optional { value } => {
                if choice % 4 == 0 {
                    Value::Null
                } else {
                    generate(value, mix(choice), depth + 1)
                }
            }
            ApplicationRouteParameterTypeV1::Object { fields } => Value::Object(
                fields
                    .iter()
                    .enumerate()
                    .filter_map(|(field_index, field)| {
                        let field_state = mix(choice ^ field_index as u64);
                        if field.optional && field_state % 4 == 0 {
                            None
                        } else {
                            Some((
                                field.name.clone(),
                                generate(&field.value_type, field_state, depth + 1),
                            ))
                        }
                    })
                    .collect(),
            ),
            ApplicationRouteParameterTypeV1::Map { value, .. } => Value::Object(
                (0..(choice % 3))
                    .map(|item| {
                        (
                            format!("key_{item}"),
                            generate(value, mix(choice ^ item), depth + 1),
                        )
                    })
                    .collect(),
            ),
            ApplicationRouteParameterTypeV1::Vector { dimensions } => Value::Array(
                (0..*dimensions)
                    .map(|item| {
                        Value::from(
                            ((mix(choice ^ item as u64) % 2_001) as f64 - 1_000.0) / 1_000.0,
                        )
                    })
                    .collect(),
            ),
            ApplicationRouteParameterTypeV1::Point => {
                json!({"type": "Point", "coordinates": [12.5, 41.9]})
            }
            ApplicationRouteParameterTypeV1::LineString => json!({
                "type": "LineString",
                "coordinates": [[12.5, 41.9], [12.6, 42.0]]
            }),
            ApplicationRouteParameterTypeV1::Polygon => json!({
                "type": "Polygon",
                "coordinates": [[[12.5, 41.9], [12.6, 41.9], [12.6, 42.0], [12.5, 41.9]]]
            }),
            ApplicationRouteParameterTypeV1::Null => Value::Null,
        }
    }

    generate(value_type, mix(seed ^ u64::from(index)), 0)
}

pub(crate) fn carrier_test_http_call(
    runtime: &ApplicationRuntime,
    actor: Option<ActorContext>,
    target: &str,
    arguments: Vec<(Option<String>, Value)>,
) -> Result<Value> {
    fn argument(
        arguments: &[(Option<String>, Value)],
        name: &str,
        positional: usize,
    ) -> Option<Value> {
        arguments
            .iter()
            .find(|(candidate, _)| candidate.as_deref() == Some(name))
            .or_else(|| {
                arguments
                    .iter()
                    .filter(|(candidate, _)| candidate.is_none())
                    .nth(positional)
            })
            .map(|(_, value)| value.clone())
    }

    let positional_offset = usize::from(target == "http.request_as");
    let method = argument(&arguments, "method", positional_offset)
        .and_then(|value| value.as_str().map(str::to_ascii_uppercase))
        .ok_or_else(|| {
            AppRuntimeError::InvalidRequest(format!("{target} requires a String method"))
        })?;
    let method = match method.as_str() {
        "GET" => HttpMethod::Get,
        "POST" => HttpMethod::Post,
        "PUT" => HttpMethod::Put,
        "PATCH" => HttpMethod::Patch,
        "DELETE" => HttpMethod::Delete,
        "HEAD" => HttpMethod::Head,
        "OPTIONS" => HttpMethod::Options,
        _ => {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "{target} uses unsupported method `{method}`"
            )))
        }
    };
    let raw_path = argument(&arguments, "path", positional_offset + 1)
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| {
            AppRuntimeError::InvalidRequest(format!("{target} requires a String path"))
        })?;
    let (path, query) = raw_path
        .split_once('?')
        .map(|(path, query)| {
            (
                path.to_string(),
                url::form_urlencoded::parse(query.as_bytes())
                    .map(|(name, value)| (name.into_owned(), value.into_owned()))
                    .collect::<Vec<_>>(),
            )
        })
        .unwrap_or_else(|| (raw_path, Vec::new()));
    let headers = match argument(&arguments, "headers", positional_offset + 3) {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Object(headers)) => headers
            .into_iter()
            .map(|(name, value)| {
                let value = value
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| value.to_string());
                (name, value)
            })
            .collect(),
        Some(_) => {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "{target} headers must be a Json object"
            )))
        }
    };
    let body = argument(&arguments, "body", positional_offset + 2)
        .map(HttpRequestBodyV2::Json)
        .unwrap_or(HttpRequestBodyV2::Empty);
    let request = crate::http::ApplicationHttpRequest {
        method,
        path,
        query,
        headers,
        body,
        peer_address: Some("carrier-test-runner".to_string()),
    };
    let dispatched = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                runtime.dispatch_carrier_test_http(
                    &crate::http::HttpHostPolicy::default(),
                    request,
                    actor,
                )
            })
            .join()
    })
    .map_err(|_| {
        AppRuntimeError::Invocation("BicDB application test HTTP dispatcher panicked".to_string())
    })?;
    let (status, headers, body) = match dispatched {
        Ok(response) => {
            let body = match response.body {
                HttpResponseBodyV2::Empty => Value::Null,
                HttpResponseBodyV2::Json(value) => value,
                HttpResponseBodyV2::Text(value) => Value::String(value),
                HttpResponseBodyV2::Binary(value) => {
                    Value::String(base64::engine::general_purpose::STANDARD.encode(value))
                }
                HttpResponseBodyV2::Error(value) => serde_json::to_value(value)?,
                HttpResponseBodyV2::Stream(_)
                | HttpResponseBodyV2::Sse(_)
                | HttpResponseBodyV2::WebSocket(_) => {
                    return Err(AppRuntimeError::InvalidRequest(
                        "BicDB application scenario tests cannot buffer a streaming response"
                            .to_string(),
                    ))
                }
            };
            (response.status, response.headers, body)
        }
        Err(error) => {
            let (status, code, message) = match &error {
                AppRuntimeError::ApplicationFailure { code, message, .. } => {
                    (400, code.as_str(), message.clone())
                }
                AppRuntimeError::Authentication(_) => (401, "unauthenticated", error.to_string()),
                AppRuntimeError::CapabilityDenied(_) => (403, "forbidden", error.to_string()),
                AppRuntimeError::NotFound(_) => (404, "not_found", error.to_string()),
                AppRuntimeError::Conflict(_) | AppRuntimeError::OptimisticConflict(_) => {
                    (409, "conflict", error.to_string())
                }
                AppRuntimeError::Timeout(_) | AppRuntimeError::ResilienceTimeout(_) => {
                    (504, "timeout", error.to_string())
                }
                AppRuntimeError::RateLimited(_) => (429, "rate_limited", error.to_string()),
                AppRuntimeError::NotReady(_) | AppRuntimeError::ResourceExhausted(_) => {
                    (503, "not_ready", error.to_string())
                }
                AppRuntimeError::InvalidRequest(_)
                | AppRuntimeError::MissingIdempotencyKey(_)
                | AppRuntimeError::Invocation(_) => (400, "invalid_request", error.to_string()),
                _ => (500, "internal", error.to_string()),
            };
            (
                status,
                vec![(
                    "content-type".to_string(),
                    "application/problem+json".to_string(),
                )],
                json!({"code": code, "message": message}),
            )
        }
    };
    if target == "http.request_as" {
        if !(200..300).contains(&status) {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "expected 2xx response, got status {status} with body {body}"
            )));
        }
        return Ok(body);
    }
    let headers = headers
        .into_iter()
        .fold(serde_json::Map::new(), |mut output, (name, value)| {
            output.insert(name.to_ascii_lowercase(), Value::String(value));
            output
        });
    Ok(json!({
        "status": status,
        "headers": headers,
        "body": body,
    }))
}

impl Drop for ApplicationRuntime {
    fn drop(&mut self) {
        let supervisors = std::mem::take(
            self.supervisors
                .get_mut()
                .expect("application supervisors poisoned"),
        );
        for set in supervisors.into_values() {
            set.stop();
        }
    }
}

impl ApplicationRuntime {}

pub(crate) fn validate_service_outcome(
    result: Result<Value>,
    method: &ServiceMethod,
) -> Result<Value> {
    match result {
        Ok(value) => {
            validate_service_response(&value, method)?;
            Ok(value)
        }
        Err(AppRuntimeError::ApplicationFailure { code, message, .. })
            if method.errors.contains(&code) || method.allows_undeclared_errors =>
        {
            Err(AppRuntimeError::ApplicationFailure {
                retryable: method.retryable_errors.contains(&code),
                code,
                message,
            })
        }
        Err(AppRuntimeError::ApplicationFailure { code, .. }) => {
            Err(AppRuntimeError::Provider(format!(
                "service method `{}` returned undeclared authored failure `{code}`",
                method.name
            )))
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn validate_service_request(payload: &Value, method: &ServiceMethod) -> Result<()> {
    validate_service_object(payload, &method.request, "request", &method.name)
        .map_err(AppRuntimeError::InvalidRequest)
}

pub(crate) fn validate_service_response(value: &Value, method: &ServiceMethod) -> Result<()> {
    let valid = if method.response.len() == 1 && method.response[0].name == "value" {
        service_field_value_matches(value, &method.response[0])
    } else {
        validate_service_object(value, &method.response, "response", &method.name).is_ok()
    };
    if valid {
        Ok(())
    } else {
        Err(AppRuntimeError::Provider(format!(
            "service method `{}` returned a value outside its signed response contract",
            method.name
        )))
    }
}

pub(crate) fn validate_service_object(
    value: &Value,
    fields: &[ContractField],
    direction: &str,
    method: &str,
) -> std::result::Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("service method `{method}` {direction} must be an object"))?;
    if object.len() != fields.len() || fields.iter().any(|field| !object.contains_key(&field.name))
    {
        return Err(format!(
            "service method `{method}` {direction} fields differ from its signed contract"
        ));
    }
    for field in fields {
        if !service_field_value_matches(&object[&field.name], field) {
            return Err(format!(
                "service method `{method}` {direction} field `{}` does not match its signed type",
                field.name
            ));
        }
    }
    Ok(())
}

pub(crate) fn service_field_value_matches(value: &Value, field: &ContractField) -> bool {
    if value.is_null() {
        return field.nullable;
    }
    if let Some(value_type) = &field.value_type {
        return crate::http::normalize_carrier_json_value(
            value,
            value_type,
            &format!("service.{}", field.name),
        )
        .is_ok();
    }
    match &field.field_type {
        FieldType::Bool => value.is_boolean(),
        FieldType::Int64 => value.as_i64().is_some() || value.as_u64().is_some(),
        FieldType::Float64 => value.as_f64().is_some(),
        FieldType::Decimal => {
            value.is_number()
                || value
                    .as_str()
                    .is_some_and(|value| value.parse::<rust_decimal::Decimal>().is_ok())
        }
        FieldType::String | FieldType::Timestamp | FieldType::Date => value.is_string(),
        FieldType::Bytes => value.is_string() || value.is_array(),
        FieldType::Uuid => value
            .as_str()
            .is_some_and(|value| uuid::Uuid::parse_str(value).is_ok()),
        FieldType::Json => true,
        FieldType::Vector { dimensions } => value.as_array().is_some_and(|values| {
            values.len() == *dimensions as usize
                && values.iter().all(|value| value.as_f64().is_some())
        }),
        FieldType::Geometry { .. } => value.is_object() || value.is_array() || value.is_string(),
    }
}

pub(crate) fn execute_carrier_callable_with_host(
    database: &BicDb,
    manifest: Arc<bicdb_extension::ExtensionManifest>,
    actor: ActorContext,
    services: InvocationServices,
    callable: &str,
    globals: BTreeMap<String, Value>,
    arguments: Vec<(Option<String>, Value)>,
) -> Result<Value> {
    let started = std::time::Instant::now();
    let trace_actor = actor.clone();
    let observability = services.observability.clone();
    let application = manifest
        .application
        .as_deref()
        .expect("BicDB application package application manifest");
    let program = application.application_program.as_ref().ok_or_else(|| {
        AppRuntimeError::InvalidPackage(format!(
            "application `{}` has no BicDB application behavior program",
            manifest.identity.name
        ))
    })?;
    if !program.callables.contains_key(callable) {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "application `{}` has no BicDB application callable `{callable}`",
            manifest.identity.name
        )));
    }
    let mut capability_host = CapabilityHost::new(database, manifest.clone(), actor, services)?;
    let mut host = CapabilityApplicationProgramHost::new(
        &mut capability_host,
        application.resources.clone(),
        program.service_bindings.clone(),
        program.client_bindings.clone(),
        program.secret_bindings.clone(),
        program.event_bindings.clone(),
        program.realtime_bindings.clone(),
        program.workflow_bindings.clone(),
    );
    let result = execute_application_program(program, callable, globals, arguments, &mut host);
    if actor_trace_sampled(&trace_actor) {
        observability.record(crate::ObservabilityEvent::Trace {
            actor: trace_actor,
            name: "bicdb.carrier_callable".to_string(),
            fields: BTreeMap::from([
                (
                    "application".to_string(),
                    Value::String(manifest.identity.name.clone()),
                ),
                ("callable".to_string(), Value::String(callable.to_string())),
                (
                    "elapsed_us".to_string(),
                    Value::from(started.elapsed().as_micros() as u64),
                ),
                ("success".to_string(), Value::Bool(result.is_ok())),
            ]),
        });
    }
    result
}

pub(crate) fn execute_carrier_mutation_bindings(
    host: &mut CapabilityHost,
    transaction: HostHandle,
    contract: &ResourceContractV1,
    mutation: &ResourceMutation,
) -> Result<()> {
    let application = host.application_manifest().clone();
    let Some(program) = application.application_program.clone() else {
        return Ok(());
    };
    let operation = match mutation.operation {
        ResourceOperation::Create => ApplicationMutationOperationV1::Create,
        ResourceOperation::Update => ApplicationMutationOperationV1::Update,
        ResourceOperation::Delete => ApplicationMutationOperationV1::Delete,
        ResourceOperation::Restore => ApplicationMutationOperationV1::Restore,
        ResourceOperation::List | ResourceOperation::Get | ResourceOperation::Upsert => {
            return Ok(())
        }
        ResourceOperation::Action => return Ok(()),
    };
    let bindings = program
        .mutation_bindings
        .iter()
        .filter(|binding| binding.resource == contract.name && binding.operation == operation)
        .cloned()
        .collect::<Vec<_>>();
    for binding in bindings {
        let callable = program.callables.get(&binding.callable).ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application mutation binding references absent callable `{}`",
                binding.callable
            ))
        })?;
        let argument_value = |name: &str| match name {
            "old" => Some(mutation.before.clone()),
            "new" => Some(mutation.after.clone()),
            "value" => Some(
                if matches!(operation, ApplicationMutationOperationV1::Delete) {
                    mutation.before.clone()
                } else {
                    mutation.after.clone()
                },
            ),
            _ => None,
        };
        let arguments = callable
            .parameters
            .iter()
            .map(|name| {
                argument_value(name)
                    .map(|value| (Some(name.clone()), value))
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidPackage(format!(
                            "BicDB application mutation callable `{}` has unknown parameter `{name}`",
                            binding.callable
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let globals = BTreeMap::from([
            ("old".to_string(), mutation.before.clone()),
            ("new".to_string(), mutation.after.clone()),
            (
                "value".to_string(),
                if matches!(operation, ApplicationMutationOperationV1::Delete) {
                    mutation.before.clone()
                } else {
                    mutation.after.clone()
                },
            ),
        ]);
        match binding.kind {
            ApplicationMutationBindingKindV1::InsideTrigger => {
                let mut adapter = CapabilityApplicationProgramHost::new(
                    host,
                    application.resources.clone(),
                    program.service_bindings.clone(),
                    program.client_bindings.clone(),
                    program.secret_bindings.clone(),
                    program.event_bindings.clone(),
                    program.realtime_bindings.clone(),
                    program.workflow_bindings.clone(),
                );
                adapter
                    .transactions
                    .push(ApplicationTransactionFrame::Transaction(transaction));
                execute_application_program(
                    &program,
                    &binding.callable,
                    globals,
                    arguments,
                    &mut adapter,
                )?;
            }
            ApplicationMutationBindingKindV1::Watch => {
                let event = binding.event.as_deref().ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(
                        "BicDB application watch has no event".to_string(),
                    )
                })?;
                let mut adapter = CapabilityApplicationProgramHost::new(
                    host,
                    application.resources.clone(),
                    program.service_bindings.clone(),
                    program.client_bindings.clone(),
                    program.secret_bindings.clone(),
                    program.event_bindings.clone(),
                    program.realtime_bindings.clone(),
                    program.workflow_bindings.clone(),
                );
                adapter
                    .transactions
                    .push(ApplicationTransactionFrame::Transaction(transaction));
                let payload = execute_application_program(
                    &program,
                    &binding.callable,
                    globals,
                    arguments,
                    &mut adapter,
                )?;
                adapter.emit(event, payload)?;
            }
            ApplicationMutationBindingKindV1::PostCommitTrigger => {
                let queue = binding.queue.clone().ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(
                        "BicDB application post-commit trigger has no queue".to_string(),
                    )
                })?;
                let payload = json!({
                    "old": mutation.before,
                    "new": mutation.after,
                    "value": if matches!(operation, ApplicationMutationOperationV1::Delete) {
                        mutation.before.clone()
                    } else {
                        mutation.after.clone()
                    },
                });
                let result = host.call(HostCall {
                    request_id: carrier_program_request_id(),
                    request: HostRequest::Broker(BrokerRequest::PublishOnCommit {
                        transaction,
                        queue,
                        payload,
                        headers: BTreeMap::from([
                            ("carrier_trigger".to_string(), binding.callable.clone()),
                            ("carrier_resource".to_string(), contract.name.clone()),
                        ]),
                        idempotency_key: None,
                        delay_ms: None,
                    }),
                });
                if let Some(error) = result.error {
                    return Err(AppRuntimeError::Invocation(format!(
                        "{} ({:?}, trace {}): {}",
                        error.code, error.class, error.trace_id, error.message
                    )));
                }
                if !matches!(result.value, Some(HostValue::String(_))) {
                    return Err(AppRuntimeError::Invocation(
                        "BicDB application post-commit trigger publish returned no receipt"
                            .to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

#[derive(Default)]
pub(crate) struct RuntimeServiceDispatcher {
    pub(crate) runtime: RwLock<Weak<ApplicationRuntime>>,
}

impl RuntimeServiceDispatcher {
    pub(crate) fn bind(&self, runtime: &Arc<ApplicationRuntime>) {
        *self.runtime.write() = Arc::downgrade(runtime);
    }
}

impl PluginServiceDispatcher for RuntimeServiceDispatcher {
    fn call(&self, call: PluginServiceCall) -> Result<Value> {
        self.runtime
            .read()
            .upgrade()
            .ok_or_else(|| {
                AppRuntimeError::NotReady("application runtime is shutting down".to_string())
            })?
            .invoke_plugin_service(call)
    }
}

pub(crate) enum ApplicationWorkflowDelivery {
    Ack,
    Retry { delay_ms: u64, error: String },
}

pub(crate) fn workflow_globals(state: &ApplicationWorkflowStateV1) -> BTreeMap<String, Value> {
    let mut globals = state.step_results.clone();
    globals.insert("input".to_string(), state.input.clone());
    globals.insert("baggage".to_string(), state.baggage.clone());
    globals
}

pub(crate) fn workflow_step_done(state: &ApplicationWorkflowStateV1, step: &str) -> bool {
    state.step_results.contains_key(step) || state.skipped_steps.contains(step)
}

#[derive(Clone, Debug)]
pub(crate) struct ApplicationWorkflowSlaTimer {
    pub(crate) kind: &'static str,
    pub(crate) sla: String,
    pub(crate) delay_ms: u64,
}

pub(crate) fn workflow_nonterminal_status(state: &ApplicationWorkflowStateV1) -> String {
    if !state.waiting_steps.is_empty()
        && state.scheduled_steps.len() == state.waiting_steps.len()
        && state.active_steps.is_empty()
    {
        "waiting".to_string()
    } else {
        "running".to_string()
    }
}

pub(crate) fn ready_workflow_step_names(
    definition: &ApplicationWorkflowDefinitionV1,
    state: &ApplicationWorkflowStateV1,
) -> Vec<String> {
    let mut ready = definition
        .steps
        .iter()
        .filter(|step| {
            !workflow_step_done(state, &step.name)
                && !state.scheduled_steps.contains(&step.name)
                && step
                    .dependencies
                    .iter()
                    .all(|dependency| workflow_step_done(state, dependency))
        })
        .map(|step| step.name.clone())
        .collect::<Vec<_>>();
    if !definition.graph_execution {
        ready.truncate(1);
    }
    let executing = state
        .scheduled_steps
        .len()
        .saturating_sub(state.waiting_steps.len());
    let available = usize::from(definition.max_parallelism).saturating_sub(executing);
    ready.truncate(available);
    ready
}

pub(crate) fn schedule_ready_workflow_steps(
    definition: &ApplicationWorkflowDefinitionV1,
    state: &mut ApplicationWorkflowStateV1,
) -> Vec<String> {
    let ready = ready_workflow_step_names(definition, state);
    for step in &ready {
        state.scheduled_steps.insert(step.clone());
        append_workflow_evidence(state, "step_scheduled", Some(step), None, None);
    }
    ready
}

pub(crate) fn sla_condition_matches(
    condition: &bicdb_extension::abi_v2::ApplicationWorkflowSlaConditionV1,
    event: &str,
    value: Option<&str>,
) -> bool {
    match condition.kind {
        ApplicationWorkflowSlaConditionKindV1::Event => {
            event == "event" && value == Some(&condition.value)
        }
        ApplicationWorkflowSlaConditionKindV1::Status => {
            event == "status" && value == Some(&condition.value)
        }
        ApplicationWorkflowSlaConditionKindV1::StepStarted => {
            event == "step_started" && value == Some(&condition.value)
        }
        ApplicationWorkflowSlaConditionKindV1::StepCompleted => {
            event == "step_completed" && value == Some(&condition.value)
        }
    }
}

pub(crate) fn update_workflow_slas(
    definition: &ApplicationWorkflowDefinitionV1,
    state: &mut ApplicationWorkflowStateV1,
    event: &str,
    value: Option<&str>,
) -> Vec<ApplicationWorkflowSlaTimer> {
    let now = crate::host::now_ms();
    let mut timers = Vec::new();
    for sla in &definition.slas {
        let excluded = sla.exclusions.iter().any(|exclusion| match exclusion.kind {
            ApplicationWorkflowSlaExclusionKindV1::Event => {
                event == "event" && exclusion.event.as_deref() == value
            }
            ApplicationWorkflowSlaExclusionKindV1::FieldEqualsBool => {
                exclusion
                    .field
                    .as_deref()
                    .and_then(|field| state.input.get(field))
                    .and_then(Value::as_bool)
                    == exclusion.value
            }
        });
        if excluded {
            let current = state.sla_states.entry(sla.name.clone()).or_default();
            if !current.excluded {
                current.excluded = true;
                append_workflow_evidence(state, "sla_excluded", None, Some(&sla.name), None);
            }
            continue;
        }
        let current = state.sla_states.entry(sla.name.clone()).or_default();
        if current.excluded || current.ended_at_ms.is_some() {
            continue;
        }
        let starts = current.started_at_ms.is_none()
            && sla
                .starts_when
                .as_ref()
                .map(|condition| sla_condition_matches(condition, event, value))
                .unwrap_or(event == "workflow_started");
        if starts {
            current.started_at_ms = Some(now);
            let warning_delay = sla
                .deadline_ms
                .saturating_mul(u64::from(sla.warning_at_basis_points))
                / 10_000;
            let breach_delay = sla
                .deadline_ms
                .saturating_mul(u64::from(sla.breach_at_basis_points))
                / 10_000;
            current.warning_at_ms =
                Some(now.saturating_add(i64::try_from(warning_delay).unwrap_or(i64::MAX)));
            current.breach_at_ms =
                Some(now.saturating_add(i64::try_from(breach_delay).unwrap_or(i64::MAX)));
            timers.extend([
                ApplicationWorkflowSlaTimer {
                    kind: "sla_warning",
                    sla: sla.name.clone(),
                    delay_ms: warning_delay,
                },
                ApplicationWorkflowSlaTimer {
                    kind: "sla_breach",
                    sla: sla.name.clone(),
                    delay_ms: breach_delay,
                },
            ]);
            append_workflow_evidence(state, "sla_started", None, Some(&sla.name), None);
        }
        let current = state
            .sla_states
            .get_mut(&sla.name)
            .expect("SLA state exists");
        let ends = current.started_at_ms.is_some()
            && current.ended_at_ms.is_none()
            && sla
                .ends_when
                .as_ref()
                .map(|condition| sla_condition_matches(condition, event, value))
                .unwrap_or(event == "status" && value == Some("completed"));
        if ends {
            current.ended_at_ms = Some(now);
            append_workflow_evidence(state, "sla_attained", None, Some(&sla.name), None);
        }
    }
    timers
}

pub(crate) fn publish_scheduled_workflow_messages(
    host: &mut CapabilityApplicationProgramHost<'_>,
    definition: &ApplicationWorkflowDefinitionV1,
    run_id: &str,
    revision: u64,
    steps: &[String],
    timers: &[ApplicationWorkflowSlaTimer],
) -> Result<()> {
    for step in steps {
        host.publish_workflow_message(
            definition,
            run_id,
            "step",
            Some(step),
            None,
            revision,
            None,
        )?;
    }
    for timer in timers {
        host.publish_workflow_message(
            definition,
            run_id,
            timer.kind,
            None,
            Some(&timer.sla),
            revision,
            Some(timer.delay_ms),
        )?;
    }
    Ok(())
}

pub(crate) fn next_compensation_step<'a>(
    definition: &'a ApplicationWorkflowDefinitionV1,
    state: &ApplicationWorkflowStateV1,
) -> Option<&'a ApplicationWorkflowStepV1> {
    state
        .active_step
        .as_ref()
        .filter(|step| !state.compensated_steps.contains(*step))
        .and_then(|active| {
            definition
                .steps
                .iter()
                .find(|step| &step.name == active && step.compensation_callable.is_some())
        })
        .or_else(|| {
            state
                .completed_step_order
                .iter()
                .rev()
                .filter(|step| !state.compensated_steps.contains(*step))
                .find_map(|completed| {
                    definition.steps.iter().find(|step| {
                        &step.name == completed && step.compensation_callable.is_some()
                    })
                })
        })
}

pub(crate) fn execute_carrier_workflow_delivery(
    database: &BicDb,
    manifest: Arc<bicdb_extension::ExtensionManifest>,
    actor: ActorContext,
    services: InvocationServices,
    workflow_name: &str,
    run_id: &str,
    delivery_kind: &str,
    requested_step: Option<&str>,
    requested_sla: Option<&str>,
) -> Result<ApplicationWorkflowDelivery> {
    let application = manifest
        .application
        .as_deref()
        .expect("BicDB application workflow package application manifest");
    let program = application.application_program.clone().ok_or_else(|| {
        AppRuntimeError::InvalidPackage(
            "BicDB application workflow package has no behavior program".into(),
        )
    })?;
    let definition = program
        .workflow_bindings
        .get(workflow_name)
        .cloned()
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application workflow binding `{workflow_name}` is absent"
            ))
        })?;
    let mut capability_host = CapabilityHost::new(database, manifest.clone(), actor, services)?;
    let mut host = CapabilityApplicationProgramHost::new(
        &mut capability_host,
        application.resources.clone(),
        program.service_bindings.clone(),
        program.client_bindings.clone(),
        program.secret_bindings.clone(),
        program.event_bindings.clone(),
        program.realtime_bindings.clone(),
        program.workflow_bindings.clone(),
    );
    let state = host.load_workflow_state(workflow_name, run_id)?;
    if state.plan_sha256.is_some()
        && definition.plan_sha256.is_some()
        && state.plan_sha256 != definition.plan_sha256
    {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "BicDB application workflow run `{run_id}` was created under incompatible plan {}",
            state.plan_sha256.as_deref().unwrap_or("unknown")
        )));
    }
    if state.terminal() {
        return Ok(ApplicationWorkflowDelivery::Ack);
    }
    if delivery_kind == "workflow_timeout" {
        if let Some(deadline) = state.timeout_at_ms {
            let now = crate::host::now_ms();
            if deadline > now {
                return Ok(ApplicationWorkflowDelivery::Retry {
                    delay_ms: u64::try_from(deadline - now).unwrap_or(1),
                    error: "workflow timeout timer delivered early".to_string(),
                });
            }
        }
    }
    if matches!(delivery_kind, "sla_warning" | "sla_breach") {
        return execute_carrier_workflow_sla_timer(
            &definition,
            &mut host,
            workflow_name,
            run_id,
            delivery_kind,
            requested_sla.ok_or_else(|| {
                AppRuntimeError::InvalidRequest("workflow SLA timer payload lacks sla".to_string())
            })?,
        );
    }
    if state
        .timeout_at_ms
        .is_some_and(|deadline| deadline <= crate::host::now_ms())
    {
        host.begin_transaction("read_committed")?;
        let outcome: Result<()> = (|| {
            let mut state = host.load_workflow_state(workflow_name, run_id)?;
            if !state.terminal() {
                let expected_revision = state.revision;
                state.status = "timed_out".to_string();
                state.active_step = None;
                state.scheduled_steps.clear();
                state.active_steps.clear();
                state.waiting_steps.clear();
                state.last_error = Some("workflow timed out".to_string());
                state.updated_at_ms = crate::host::now_ms();
                state.finished_at_ms = Some(state.updated_at_ms);
                append_workflow_evidence(&mut state, "workflow_timed_out", None, None, None);
                let status = state.status.clone();
                let timers = update_workflow_slas(&definition, &mut state, "status", Some(&status));
                state.revision = state.revision.saturating_add(1);
                host.store_workflow_state(run_id, &mut state, Some(expected_revision))?;
                publish_scheduled_workflow_messages(
                    &mut host,
                    &definition,
                    run_id,
                    state.revision,
                    &[],
                    &timers,
                )?;
            }
            Ok(())
        })();
        if outcome.is_ok() {
            host.commit_transaction()?;
        } else {
            let _ = host.rollback_transaction();
            outcome?;
        }
        return Ok(ApplicationWorkflowDelivery::Ack);
    }
    if state.status == "compensating" {
        return if delivery_kind == "drive" {
            execute_carrier_workflow_compensation(
                &program,
                &definition,
                &mut host,
                workflow_name,
                run_id,
            )
        } else {
            Ok(ApplicationWorkflowDelivery::Ack)
        };
    }
    if delivery_kind == "drive" {
        return drive_carrier_workflow(&definition, &mut host, workflow_name, run_id);
    }
    let step_name = requested_step.ok_or_else(|| {
        AppRuntimeError::InvalidRequest("workflow step payload lacks step".to_string())
    })?;
    let step = definition
        .steps
        .iter()
        .find(|step| step.name == step_name)
        .cloned()
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "workflow delivery references absent step `{step_name}`"
            ))
        })?;
    if step.wait.is_some() {
        return execute_carrier_workflow_wait_delivery(
            &definition,
            &mut host,
            workflow_name,
            run_id,
            &step,
            delivery_kind,
        );
    }
    host.begin_transaction("read_committed")?;
    let execution: Result<()> = (|| {
        let mut state = host.load_workflow_state(workflow_name, run_id)?;
        if state.terminal() || workflow_step_done(&state, &step.name) {
            return Ok(());
        }
        if !state.scheduled_steps.contains(&step.name) {
            return Err(AppRuntimeError::Conflict(format!(
                "workflow step `{}` is not durably scheduled",
                step.name
            )));
        }
        if definition.graph_execution
            && !step
                .dependencies
                .iter()
                .all(|dependency| workflow_step_done(&state, dependency))
        {
            return Err(AppRuntimeError::Conflict(format!(
                "workflow step `{}` dependencies changed while claiming work",
                step.name
            )));
        }
        state.active_steps.insert(step.name.clone());
        append_workflow_evidence(&mut state, "step_started", Some(&step.name), None, None);
        let mut sla_timers =
            update_workflow_slas(&definition, &mut state, "step_started", Some(&step.name));
        let globals = workflow_globals(&state);
        if let Some(condition) = &step.condition_callable {
            let value = execute_application_program(
                &program,
                condition,
                globals.clone(),
                Vec::new(),
                &mut host,
            )?;
            let enabled = value.as_bool().ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "BicDB application workflow condition `{condition}` did not return Bool"
                ))
            })?;
            if !enabled {
                let expected_revision = state.revision;
                state.skipped_steps.insert(step.name.clone());
                state.scheduled_steps.remove(&step.name);
                state.active_steps.remove(&step.name);
                state.active_step = Some(step.name.clone());
                let ready = schedule_ready_workflow_steps(&definition, &mut state);
                state.status = workflow_nonterminal_status(&state);
                state.last_error = None;
                state.updated_at_ms = crate::host::now_ms();
                append_workflow_evidence(&mut state, "step_skipped", Some(&step.name), None, None);
                sla_timers.extend(update_workflow_slas(
                    &definition,
                    &mut state,
                    "step_completed",
                    Some(&step.name),
                ));
                let status = state.status.clone();
                sla_timers.extend(update_workflow_slas(
                    &definition,
                    &mut state,
                    "status",
                    Some(&status),
                ));
                state.revision = state.revision.saturating_add(1);
                host.store_workflow_state(run_id, &mut state, Some(expected_revision))?;
                publish_scheduled_workflow_messages(
                    &mut host,
                    &definition,
                    run_id,
                    state.revision,
                    &ready,
                    &sla_timers,
                )?;
                return Ok(());
            }
        }
        let output =
            execute_application_program(&program, &step.callable, globals, Vec::new(), &mut host)?;
        let expected_revision = state.revision;
        state.step_results.insert(step.name.clone(), output.clone());
        state.completed_step_order.push(step.name.clone());
        state.scheduled_steps.remove(&step.name);
        state.active_steps.remove(&step.name);
        state.active_step = Some(step.name.clone());
        state.last_error = None;
        state.updated_at_ms = crate::host::now_ms();
        append_workflow_evidence(&mut state, "step_completed", Some(&step.name), None, None);
        sla_timers.extend(update_workflow_slas(
            &definition,
            &mut state,
            "step_completed",
            Some(&step.name),
        ));
        let ready;
        if step.name == definition.return_step {
            state.status = "completed".to_string();
            state.output = Some(output);
            state.finished_at_ms = Some(state.updated_at_ms);
            append_workflow_evidence(&mut state, "workflow_completed", None, None, None);
            sla_timers.extend(update_workflow_slas(
                &definition,
                &mut state,
                "status",
                Some("completed"),
            ));
            ready = Vec::new();
        } else {
            ready = schedule_ready_workflow_steps(&definition, &mut state);
            state.status = workflow_nonterminal_status(&state);
            let status = state.status.clone();
            sla_timers.extend(update_workflow_slas(
                &definition,
                &mut state,
                "status",
                Some(&status),
            ));
        }
        state.revision = state.revision.saturating_add(1);
        host.store_workflow_state(run_id, &mut state, Some(expected_revision))?;
        publish_scheduled_workflow_messages(
            &mut host,
            &definition,
            run_id,
            state.revision,
            &ready,
            &sla_timers,
        )?;
        Ok(())
    })();
    match execution {
        Ok(()) => {
            host.commit_transaction()?;
            Ok(ApplicationWorkflowDelivery::Ack)
        }
        Err(AppRuntimeError::Conflict(error) | AppRuntimeError::OptimisticConflict(error)) => {
            let _ = host.rollback_transaction();
            Ok(ApplicationWorkflowDelivery::Retry { delay_ms: 1, error })
        }
        Err(error) => {
            let _ = host.rollback_transaction();
            record_carrier_workflow_step_failure(
                &definition,
                &mut host,
                workflow_name,
                run_id,
                &step,
                error.to_string(),
            )
        }
    }
}

pub(crate) fn drive_carrier_workflow(
    definition: &ApplicationWorkflowDefinitionV1,
    host: &mut CapabilityApplicationProgramHost<'_>,
    workflow_name: &str,
    run_id: &str,
) -> Result<ApplicationWorkflowDelivery> {
    host.begin_transaction("read_committed")?;
    let outcome: Result<Option<Vec<String>>> = (|| {
        let mut state = host.load_workflow_state(workflow_name, run_id)?;
        if state.terminal() || !state.scheduled_steps.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let expected_revision = state.revision;
        let ready = schedule_ready_workflow_steps(definition, &mut state);
        if ready.is_empty() {
            return Ok(None);
        }
        state.status = workflow_nonterminal_status(&state);
        state.updated_at_ms = crate::host::now_ms();
        let status = state.status.clone();
        let timers = update_workflow_slas(definition, &mut state, "status", Some(&status));
        state.revision = state.revision.saturating_add(1);
        host.store_workflow_state(run_id, &mut state, Some(expected_revision))?;
        publish_scheduled_workflow_messages(
            host,
            definition,
            run_id,
            state.revision,
            &ready,
            &timers,
        )?;
        Ok(Some(ready))
    })();
    match outcome {
        Ok(Some(_)) => {
            host.commit_transaction()?;
            Ok(ApplicationWorkflowDelivery::Ack)
        }
        Ok(None) => {
            host.rollback_transaction()?;
            let state = host.load_workflow_state(workflow_name, run_id)?;
            if !state.waiting_steps.is_empty() || !state.scheduled_steps.is_empty() {
                Ok(ApplicationWorkflowDelivery::Ack)
            } else {
                finish_stalled_carrier_workflow(definition, host, workflow_name, run_id)
            }
        }
        Err(error) => {
            let _ = host.rollback_transaction();
            Err(error)
        }
    }
}

pub(crate) fn execute_carrier_workflow_wait_delivery(
    definition: &ApplicationWorkflowDefinitionV1,
    host: &mut CapabilityApplicationProgramHost<'_>,
    workflow_name: &str,
    run_id: &str,
    step: &ApplicationWorkflowStepV1,
    delivery_kind: &str,
) -> Result<ApplicationWorkflowDelivery> {
    let wait = step.wait.as_ref().ok_or_else(|| {
        AppRuntimeError::InvalidPackage(format!("workflow step `{}` is not a wait", step.name))
    })?;
    let state = host.load_workflow_state(workflow_name, run_id)?;
    if state.terminal() || workflow_step_done(&state, &step.name) {
        return Ok(ApplicationWorkflowDelivery::Ack);
    }
    if delivery_kind == "step" {
        if state.waiting_steps.contains_key(&step.name) {
            return Ok(ApplicationWorkflowDelivery::Ack);
        }
        host.begin_transaction("read_committed")?;
        let outcome: Result<()> = (|| {
            let mut state = host.load_workflow_state(workflow_name, run_id)?;
            if state.terminal() || workflow_step_done(&state, &step.name) {
                return Ok(());
            }
            if !state.scheduled_steps.contains(&step.name)
                || !step
                    .dependencies
                    .iter()
                    .all(|dependency| workflow_step_done(&state, dependency))
            {
                return Err(AppRuntimeError::Conflict(format!(
                    "workflow wait `{}` is not dependency-ready",
                    step.name
                )));
            }
            let now = crate::host::now_ms();
            let (kind, signal, delay_ms, timer_kind) = match wait.kind {
                ApplicationWorkflowWaitKindV1::Delay => (
                    "delay",
                    None,
                    wait.delay_ms.expect("validated delay wait"),
                    "wait_delay",
                ),
                ApplicationWorkflowWaitKindV1::Signal => (
                    "signal",
                    wait.signal.clone(),
                    wait.timeout_ms.unwrap_or(0),
                    "wait_timeout",
                ),
            };
            let expected_revision = state.revision;
            state.waiting_steps.insert(
                step.name.clone(),
                ApplicationWorkflowWaitStateV1 {
                    kind: kind.to_string(),
                    signal,
                    due_at_ms: (delay_ms > 0)
                        .then(|| now.saturating_add(i64::try_from(delay_ms).unwrap_or(i64::MAX))),
                },
            );
            let ready = schedule_ready_workflow_steps(definition, &mut state);
            state.status = workflow_nonterminal_status(&state);
            state.active_step = Some(step.name.clone());
            state.updated_at_ms = now;
            append_workflow_evidence(&mut state, "step_started", Some(&step.name), None, None);
            let mut sla_timers =
                update_workflow_slas(definition, &mut state, "step_started", Some(&step.name));
            let status = state.status.clone();
            sla_timers.extend(update_workflow_slas(
                definition,
                &mut state,
                "status",
                Some(&status),
            ));
            append_workflow_evidence(
                &mut state,
                "wait_started",
                Some(&step.name),
                None,
                Some(json!({"kind": kind, "signal": wait.signal})),
            );
            state.revision = state.revision.saturating_add(1);
            host.store_workflow_state(run_id, &mut state, Some(expected_revision))?;
            if delay_ms > 0 {
                host.publish_workflow_message(
                    definition,
                    run_id,
                    timer_kind,
                    Some(&step.name),
                    None,
                    state.revision,
                    Some(delay_ms),
                )?;
            }
            publish_scheduled_workflow_messages(
                host,
                definition,
                run_id,
                state.revision,
                &ready,
                &sla_timers,
            )?;
            Ok(())
        })();
        if outcome.is_ok() {
            host.commit_transaction()?;
            return Ok(ApplicationWorkflowDelivery::Ack);
        }
        let _ = host.rollback_transaction();
        outcome?;
        unreachable!();
    }
    let Some(wait_state) = state.waiting_steps.get(&step.name) else {
        return Ok(ApplicationWorkflowDelivery::Ack);
    };
    let now = crate::host::now_ms();
    if let Some(due_at_ms) = wait_state.due_at_ms {
        if now < due_at_ms {
            return Ok(ApplicationWorkflowDelivery::Retry {
                delay_ms: u64::try_from(due_at_ms - now).unwrap_or(1),
                error: "workflow wait timer delivered early".to_string(),
            });
        }
    }
    if delivery_kind == "wait_timeout" {
        host.begin_transaction("read_committed")?;
        let outcome: Result<()> = (|| {
            let mut state = host.load_workflow_state(workflow_name, run_id)?;
            if !state.waiting_steps.contains_key(&step.name) {
                return Ok(());
            }
            let expected_revision = state.revision;
            state.waiting_steps.remove(&step.name);
            state.scheduled_steps.remove(&step.name);
            state.last_error = Some(format!("workflow signal wait `{}` timed out", step.name));
            state.updated_at_ms = crate::host::now_ms();
            append_workflow_evidence(&mut state, "wait_timed_out", Some(&step.name), None, None);
            if let Some(compensation) = next_compensation_step(definition, &state) {
                state.status = "compensating".to_string();
                state.active_step = Some(compensation.name.clone());
            } else {
                state.status = "failed".to_string();
                state.finished_at_ms = Some(state.updated_at_ms);
            }
            let status = state.status.clone();
            let timers = update_workflow_slas(definition, &mut state, "status", Some(&status));
            state.revision = state.revision.saturating_add(1);
            host.store_workflow_state(run_id, &mut state, Some(expected_revision))?;
            if state.status == "compensating" {
                host.publish_workflow_continuation(definition, run_id, state.revision)?;
            }
            publish_scheduled_workflow_messages(
                host,
                definition,
                run_id,
                state.revision,
                &[],
                &timers,
            )?;
            Ok(())
        })();
        if outcome.is_ok() {
            host.commit_transaction()?;
            return Ok(ApplicationWorkflowDelivery::Ack);
        }
        let _ = host.rollback_transaction();
        outcome?;
        unreachable!();
    }
    host.begin_transaction("read_committed")?;
    let outcome: Result<()> = (|| {
        let mut state = host.load_workflow_state(workflow_name, run_id)?;
        if !state.waiting_steps.contains_key(&step.name) {
            return Ok(());
        }
        let expected_revision = state.revision;
        state.waiting_steps.remove(&step.name);
        state.scheduled_steps.remove(&step.name);
        state.step_results.insert(step.name.clone(), Value::Null);
        state.completed_step_order.push(step.name.clone());
        state.updated_at_ms = crate::host::now_ms();
        append_workflow_evidence(&mut state, "wait_completed", Some(&step.name), None, None);
        append_workflow_evidence(&mut state, "step_completed", Some(&step.name), None, None);
        let mut timers =
            update_workflow_slas(definition, &mut state, "step_completed", Some(&step.name));
        let ready = if step.name == definition.return_step {
            state.status = "completed".to_string();
            state.output = Some(Value::Null);
            state.finished_at_ms = Some(state.updated_at_ms);
            append_workflow_evidence(&mut state, "workflow_completed", None, None, None);
            timers.extend(update_workflow_slas(
                definition,
                &mut state,
                "status",
                Some("completed"),
            ));
            Vec::new()
        } else {
            let ready = schedule_ready_workflow_steps(definition, &mut state);
            state.status = workflow_nonterminal_status(&state);
            let status = state.status.clone();
            timers.extend(update_workflow_slas(
                definition,
                &mut state,
                "status",
                Some(&status),
            ));
            ready
        };
        state.revision = state.revision.saturating_add(1);
        host.store_workflow_state(run_id, &mut state, Some(expected_revision))?;
        publish_scheduled_workflow_messages(
            host,
            definition,
            run_id,
            state.revision,
            &ready,
            &timers,
        )
    })();
    if outcome.is_ok() {
        host.commit_transaction()?;
        Ok(ApplicationWorkflowDelivery::Ack)
    } else {
        let _ = host.rollback_transaction();
        outcome?;
        unreachable!()
    }
}

pub(crate) fn execute_carrier_workflow_sla_timer(
    definition: &ApplicationWorkflowDefinitionV1,
    host: &mut CapabilityApplicationProgramHost<'_>,
    workflow_name: &str,
    run_id: &str,
    delivery_kind: &str,
    sla_name: &str,
) -> Result<ApplicationWorkflowDelivery> {
    let sla = definition
        .slas
        .iter()
        .find(|sla| sla.name == sla_name)
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "workflow SLA timer references absent SLA `{sla_name}`"
            ))
        })?;
    let state = host.load_workflow_state(workflow_name, run_id)?;
    if let Some(sla_state) = state.sla_states.get(sla_name) {
        let due_at_ms = if delivery_kind == "sla_warning" {
            sla_state.warning_at_ms
        } else {
            sla_state.breach_at_ms
        };
        let now = crate::host::now_ms();
        if due_at_ms.is_some_and(|due| due > now) {
            return Ok(ApplicationWorkflowDelivery::Retry {
                delay_ms: u64::try_from(due_at_ms.expect("checked SLA due time") - now)
                    .unwrap_or(1),
                error: "workflow SLA timer delivered early".to_string(),
            });
        }
    }
    host.begin_transaction("read_committed")?;
    let outcome: Result<()> = (|| {
        let mut state = host.load_workflow_state(workflow_name, run_id)?;
        let Some(sla_state) = state.sla_states.get(sla_name) else {
            return Ok(());
        };
        if sla_state.ended_at_ms.is_some()
            || delivery_kind == "sla_warning" && sla_state.warned_at_ms.is_some()
            || delivery_kind == "sla_breach" && sla_state.breached_at_ms.is_some()
        {
            return Ok(());
        }
        let expected_revision = state.revision;
        let now = crate::host::now_ms();
        let event = if delivery_kind == "sla_warning" {
            state
                .sla_states
                .get_mut(sla_name)
                .expect("SLA state exists")
                .warned_at_ms = Some(now);
            "sla_warning"
        } else {
            state
                .sla_states
                .get_mut(sla_name)
                .expect("SLA state exists")
                .breached_at_ms = Some(now);
            "sla_breached"
        };
        append_workflow_evidence(&mut state, event, None, Some(sla_name), None);
        let escalation_trigger = if delivery_kind == "sla_warning" {
            bicdb_extension::abi_v2::ApplicationWorkflowSlaEscalationTriggerV1::BreachImminent
        } else {
            bicdb_extension::abi_v2::ApplicationWorkflowSlaEscalationTriggerV1::Breached
        };
        for escalation in sla
            .escalations
            .iter()
            .filter(|escalation| escalation.trigger == escalation_trigger)
        {
            append_workflow_evidence(
                &mut state,
                "sla_escalation_requested",
                None,
                Some(sla_name),
                Some(json!({
                    "target_kind": escalation.target_kind,
                    "target": escalation.target,
                })),
            );
        }
        state.updated_at_ms = now;
        state.revision = state.revision.saturating_add(1);
        host.store_workflow_state(run_id, &mut state, Some(expected_revision))
    })();
    if outcome.is_ok() {
        host.commit_transaction()?;
        Ok(ApplicationWorkflowDelivery::Ack)
    } else {
        let _ = host.rollback_transaction();
        outcome?;
        unreachable!()
    }
}

pub(crate) fn finish_stalled_carrier_workflow(
    definition: &ApplicationWorkflowDefinitionV1,
    host: &mut CapabilityApplicationProgramHost<'_>,
    workflow_name: &str,
    run_id: &str,
) -> Result<ApplicationWorkflowDelivery> {
    host.begin_transaction("read_committed")?;
    let outcome: Result<()> = (|| {
        let mut state = host.load_workflow_state(workflow_name, run_id)?;
        if state.terminal() {
            return Ok(());
        }
        let expected_revision = state.revision;
        if let Some(output) = state.step_results.get(&definition.return_step).cloned() {
            state.status = "completed".to_string();
            state.output = Some(output);
            state.last_error = None;
            append_workflow_evidence(&mut state, "workflow_completed", None, None, None);
        } else {
            state.status = "failed".to_string();
            state.last_error = Some(
                "workflow has no runnable step and its return step did not complete".to_string(),
            );
            let last_error = state.last_error.clone().map(Value::String);
            append_workflow_evidence(&mut state, "workflow_failed", None, None, last_error);
        }
        state.active_step = None;
        state.updated_at_ms = crate::host::now_ms();
        state.finished_at_ms = Some(state.updated_at_ms);
        let status = state.status.clone();
        let timers = update_workflow_slas(definition, &mut state, "status", Some(&status));
        state.revision = state.revision.saturating_add(1);
        host.store_workflow_state(run_id, &mut state, Some(expected_revision))?;
        publish_scheduled_workflow_messages(host, definition, run_id, state.revision, &[], &timers)
    })();
    if outcome.is_ok() {
        host.commit_transaction()?;
        Ok(ApplicationWorkflowDelivery::Ack)
    } else {
        let _ = host.rollback_transaction();
        outcome?;
        unreachable!()
    }
}

pub(crate) fn record_carrier_workflow_step_failure(
    definition: &ApplicationWorkflowDefinitionV1,
    host: &mut CapabilityApplicationProgramHost<'_>,
    workflow_name: &str,
    run_id: &str,
    step: &ApplicationWorkflowStepV1,
    error: String,
) -> Result<ApplicationWorkflowDelivery> {
    host.begin_transaction("read_committed")?;
    let outcome = (|| {
        let mut state = host.load_workflow_state(workflow_name, run_id)?;
        if state.terminal() || workflow_step_done(&state, &step.name) {
            return Ok(ApplicationWorkflowDelivery::Ack);
        }
        let failures = state.attempts.entry(step.name.clone()).or_default();
        *failures = failures.saturating_add(1);
        let retry = *failures <= step.max_retries;
        let failure_count = *failures;
        let expected_revision = state.revision;
        state.active_step = Some(step.name.clone());
        state.active_steps.remove(&step.name);
        state.last_error = Some(error.clone());
        state.updated_at_ms = crate::host::now_ms();
        if retry {
            state.status = "retry".to_string();
            append_workflow_evidence(
                &mut state,
                "step_retry",
                Some(&step.name),
                None,
                Some(json!({"error": error.clone(), "attempt": failure_count})),
            );
        } else if let Some(compensation) = next_compensation_step(definition, &state) {
            state.status = "compensating".to_string();
            state.active_step = Some(compensation.name.clone());
            state.scheduled_steps.clear();
            state.waiting_steps.clear();
            append_workflow_evidence(
                &mut state,
                "compensation_started",
                Some(&compensation.name),
                None,
                Some(json!({"failed_step": step.name, "error": error.clone()})),
            );
        } else {
            state.status = "failed".to_string();
            state.scheduled_steps.clear();
            state.waiting_steps.clear();
            state.finished_at_ms = Some(state.updated_at_ms);
            append_workflow_evidence(
                &mut state,
                "workflow_failed",
                Some(&step.name),
                None,
                Some(json!({"error": error.clone()})),
            );
        }
        let status = state.status.clone();
        let timers = update_workflow_slas(definition, &mut state, "status", Some(&status));
        state.revision = state.revision.saturating_add(1);
        host.store_workflow_state(run_id, &mut state, Some(expected_revision))?;
        if state.status == "compensating" {
            host.publish_workflow_continuation(definition, run_id, state.revision)?;
        }
        publish_scheduled_workflow_messages(
            host,
            definition,
            run_id,
            state.revision,
            &[],
            &timers,
        )?;
        Ok(if retry {
            ApplicationWorkflowDelivery::Retry {
                delay_ms: step.retry_delay_ms,
                error,
            }
        } else {
            ApplicationWorkflowDelivery::Ack
        })
    })();
    if outcome.is_ok() {
        host.commit_transaction()?;
    } else {
        let _ = host.rollback_transaction();
    }
    outcome
}

pub(crate) fn execute_carrier_workflow_compensation(
    program: &bicdb_extension::abi_v2::ApplicationProgramV1,
    definition: &ApplicationWorkflowDefinitionV1,
    host: &mut CapabilityApplicationProgramHost<'_>,
    workflow_name: &str,
    run_id: &str,
) -> Result<ApplicationWorkflowDelivery> {
    let state = host.load_workflow_state(workflow_name, run_id)?;
    let Some(step) = next_compensation_step(definition, &state).cloned() else {
        host.begin_transaction("read_committed")?;
        let outcome: Result<()> = (|| {
            let mut state = host.load_workflow_state(workflow_name, run_id)?;
            let expected_revision = state.revision;
            state.status = if state.cancel_requested {
                "cancelled".to_string()
            } else {
                "compensated".to_string()
            };
            state.active_step = None;
            state.updated_at_ms = crate::host::now_ms();
            state.finished_at_ms = Some(state.updated_at_ms);
            let event = if state.cancel_requested {
                "workflow_cancelled"
            } else {
                "workflow_compensated"
            };
            append_workflow_evidence(&mut state, event, None, None, None);
            let status = state.status.clone();
            let timers = update_workflow_slas(definition, &mut state, "status", Some(&status));
            state.revision = state.revision.saturating_add(1);
            host.store_workflow_state(run_id, &mut state, Some(expected_revision))?;
            publish_scheduled_workflow_messages(
                host,
                definition,
                run_id,
                state.revision,
                &[],
                &timers,
            )
        })();
        if outcome.is_ok() {
            host.commit_transaction()?;
            return Ok(ApplicationWorkflowDelivery::Ack);
        }
        let _ = host.rollback_transaction();
        outcome?;
        unreachable!();
    };
    let callable = step.compensation_callable.clone().ok_or_else(|| {
        AppRuntimeError::InvalidPackage(format!(
            "workflow compensation step `{}` has no callable",
            step.name
        ))
    })?;
    host.begin_transaction("read_committed")?;
    let execution: Result<()> = (|| {
        let mut state = host.load_workflow_state(workflow_name, run_id)?;
        if state.compensated_steps.contains(&step.name) {
            return Ok(());
        }
        execute_application_program(
            program,
            &callable,
            workflow_globals(&state),
            Vec::new(),
            host,
        )?;
        let expected_revision = state.revision;
        state.compensated_steps.insert(step.name.clone());
        state.compensation_attempts.remove(&step.name);
        state.updated_at_ms = crate::host::now_ms();
        append_workflow_evidence(&mut state, "step_compensated", Some(&step.name), None, None);
        let next = next_compensation_step(definition, &state).map(|step| step.name.clone());
        state.active_step = next.clone();
        if next.is_some() {
            state.status = "compensating".to_string();
        } else {
            state.status = if state.cancel_requested {
                "cancelled".to_string()
            } else {
                "compensated".to_string()
            };
            state.finished_at_ms = Some(state.updated_at_ms);
            let event = if state.cancel_requested {
                "workflow_cancelled"
            } else {
                "workflow_compensated"
            };
            append_workflow_evidence(&mut state, event, None, None, None);
        }
        let status = state.status.clone();
        let timers = update_workflow_slas(definition, &mut state, "status", Some(&status));
        state.revision = state.revision.saturating_add(1);
        host.store_workflow_state(run_id, &mut state, Some(expected_revision))?;
        if next.is_some() {
            host.publish_workflow_continuation(definition, run_id, state.revision)?;
        }
        publish_scheduled_workflow_messages(
            host,
            definition,
            run_id,
            state.revision,
            &[],
            &timers,
        )?;
        Ok(())
    })();
    match execution {
        Ok(()) => {
            host.commit_transaction()?;
            Ok(ApplicationWorkflowDelivery::Ack)
        }
        Err(error) => {
            let _ = host.rollback_transaction();
            record_carrier_workflow_compensation_failure(
                definition,
                host,
                workflow_name,
                run_id,
                &step,
                error.to_string(),
            )
        }
    }
}

pub(crate) fn record_carrier_workflow_compensation_failure(
    definition: &ApplicationWorkflowDefinitionV1,
    host: &mut CapabilityApplicationProgramHost<'_>,
    workflow_name: &str,
    run_id: &str,
    step: &ApplicationWorkflowStepV1,
    error: String,
) -> Result<ApplicationWorkflowDelivery> {
    host.begin_transaction("read_committed")?;
    let outcome = (|| {
        let mut state = host.load_workflow_state(workflow_name, run_id)?;
        let failures = state
            .compensation_attempts
            .entry(step.name.clone())
            .or_default();
        *failures = failures.saturating_add(1);
        let retry = *failures <= definition.max_retries;
        let failure_count = *failures;
        let expected_revision = state.revision;
        state.status = if retry {
            "compensating".to_string()
        } else {
            "compensation_failed".to_string()
        };
        state.active_step = Some(step.name.clone());
        state.last_error = Some(error.clone());
        state.updated_at_ms = crate::host::now_ms();
        state.finished_at_ms = (!retry).then_some(state.updated_at_ms);
        append_workflow_evidence(
            &mut state,
            if retry {
                "compensation_retry"
            } else {
                "compensation_failed"
            },
            Some(&step.name),
            None,
            Some(json!({"error": error.clone(), "attempt": failure_count})),
        );
        let status = state.status.clone();
        let timers = update_workflow_slas(definition, &mut state, "status", Some(&status));
        state.revision = state.revision.saturating_add(1);
        host.store_workflow_state(run_id, &mut state, Some(expected_revision))?;
        publish_scheduled_workflow_messages(
            host,
            definition,
            run_id,
            state.revision,
            &[],
            &timers,
        )?;
        Ok(if retry {
            ApplicationWorkflowDelivery::Retry {
                delay_ms: step.retry_delay_ms,
                error,
            }
        } else {
            ApplicationWorkflowDelivery::Ack
        })
    })();
    if outcome.is_ok() {
        host.commit_transaction()?;
    } else {
        let _ = host.rollback_transaction();
    }
    outcome
}

pub(crate) fn start_worker(
    db: Arc<RwLock<BicDb>>,
    module: Arc<WasmExtension>,
    manifest: Arc<bicdb_extension::ExtensionManifest>,
    services: InvocationServices,
    worker: bicdb_extension::abi_v2::WorkerDefinition,
    node_id: String,
) -> Result<SupervisorControl> {
    let stop = Arc::new(AtomicBool::new(false));
    let active = Arc::new(AtomicBool::new(false));
    let healthy = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let thread_active = active.clone();
    let thread_healthy = healthy.clone();
    let consumer = format!("{}-{}-{}", manifest.identity.name, node_id, worker.name);
    let task = std::thread::Builder::new()
        .name(format!("bicdb-worker-{}", worker.name))
        .spawn(move || {
            thread_healthy.store(true, Ordering::Release);
            while !thread_stop.load(Ordering::Acquire) {
                if !thread_active.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                let messages = {
                    let db = db.read();
                    db.with_broker(|broker| {
                        broker.consume(
                            &worker.queue,
                            &worker.group,
                            &consumer,
                            ConsumeOptions {
                                max_messages: 1,
                                visibility_timeout_ms: worker.visibility_timeout_ms,
                            },
                        )
                    })
                };
                let messages = match messages {
                    Ok(messages) => messages,
                    Err(error) => {
                        thread_healthy.store(false, Ordering::Release);
                        eprintln!("bicdb: worker `{}` consume failed: {error}", worker.name);
                        std::thread::sleep(Duration::from_millis(100));
                        thread_healthy.store(true, Ordering::Release);
                        continue;
                    }
                };
                if messages.is_empty() {
                    std::thread::sleep(Duration::from_millis(25));
                    continue;
                }
                for message in messages {
                    if worker.message.as_ref().is_some_and(|expected| {
                        message
                            .headers
                            .get("bicdb_application")
                            .and_then(|headers| headers.get("carrier_message"))
                            .and_then(Value::as_str)
                            != Some(expected)
                    }) {
                        let db = db.read();
                        if let Err(error) = db.with_broker(|broker| {
                            broker.ack(&worker.queue, &worker.group, &consumer, message.message_id)
                        }) {
                            eprintln!(
                                "bicdb: worker `{}` filtered-message ack failed: {error}",
                                worker.name
                            );
                        }
                        continue;
                    }
                    let mut actor =
                        worker_actor(&manifest.identity.name, &worker.name, &message.headers);
                    if let Some(parent) = actor.causation_id.take() {
                        actor
                            .policy_attributes
                            .insert("carrier.parent_causation_id".to_string(), parent);
                    }
                    actor.causation_id = Some(message.message_id.to_string());
                    let invocation = ExtensionInvocation {
                        id: message.message_id.to_string(),
                        kind: InvocationKind::QueueEvent,
                        target: worker.export.clone(),
                        payload: json!({
                            "message_id": message.message_id,
                            "sequence": message.sequence,
                            "attempts": message.attempts,
                            "payload": message.payload,
                            "headers": message.headers,
                        }),
                        context: actor_invocation_context(&actor),
                    };
                    let carrier_callable = manifest
                        .application
                        .as_deref()
                        .and_then(|application| application.application_program.as_ref())
                        .is_some_and(|program| program.callables.contains_key(&worker.export));
                    let carrier_workflow = manifest
                        .application
                        .as_deref()
                        .and_then(|application| application.application_program.as_ref())
                        .and_then(|program| {
                            program
                                .workflow_bindings
                                .iter()
                                .find(|(_, definition)| {
                                    definition.worker_export == worker.export
                                })
                                .map(|(name, _)| name.clone())
                        });
                    let result: Result<ExtensionInvocationResult> = {
                        let db = db.read();
                        if let Some(workflow_name) = carrier_workflow {
                            let run_id = message
                                .payload
                                .get("run_id")
                                .and_then(Value::as_str)
                                .ok_or_else(|| {
                                    AppRuntimeError::InvalidRequest(format!(
                                        "workflow worker `{}` payload lacks run_id",
                                        worker.name
                                    ))
                                });
                            run_id.and_then(|run_id| {
                                let delivery_kind = message
                                    .payload
                                    .get("kind")
                                    .and_then(Value::as_str)
                                    .unwrap_or("drive");
                                let requested_step = message
                                    .payload
                                    .get("step")
                                    .and_then(Value::as_str);
                                let requested_sla =
                                    message.payload.get("sla").and_then(Value::as_str);
                                execute_carrier_workflow_delivery(
                                    &db,
                                    manifest.clone(),
                                    actor,
                                    services.clone(),
                                    &workflow_name,
                                    run_id,
                                    delivery_kind,
                                    requested_step,
                                    requested_sla,
                                )
                            })
                            .map(|outcome| match outcome {
                                ApplicationWorkflowDelivery::Ack => ExtensionInvocationResult {
                                    status: 200,
                                    headers: BTreeMap::new(),
                                    body: Value::Null,
                                    ack: true,
                                    retry_after_ms: None,
                                    error: None,
                                },
                                ApplicationWorkflowDelivery::Retry { delay_ms, error } => {
                                    ExtensionInvocationResult {
                                        status: 500,
                                        headers: BTreeMap::new(),
                                        body: Value::Null,
                                        ack: false,
                                        retry_after_ms: Some(delay_ms),
                                        error: Some(error),
                                    }
                                }
                            })
                        } else if carrier_callable {
                            (|| {
                                let arguments = if worker.payload_arguments.is_empty() {
                                    vec![(None, message.payload.clone())]
                                } else {
                                    worker
                                        .payload_arguments
                                        .iter()
                                        .map(|name| {
                                            message
                                                .payload
                                                .get(name)
                                                .cloned()
                                                .map(|value| (Some(name.clone()), value))
                                                .ok_or_else(|| {
                                                    AppRuntimeError::InvalidRequest(format!(
                                                        "worker `{}` payload lacks argument `{name}`",
                                                        worker.name
                                                    ))
                                                })
                                        })
                                        .collect::<Result<Vec<_>>>()?
                                };
                                let baggage = message
                                    .headers
                                    .get("bicdb_application")
                                    .and_then(|headers| headers.get("carrier_baggage"))
                                    .and_then(Value::as_str)
                                    .and_then(|value| serde_json::from_str(value).ok())
                                    .unwrap_or_else(|| json!({}));
                                execute_carrier_callable_with_host(
                                    &db,
                                    manifest.clone(),
                                    actor,
                                    services.clone(),
                                    &worker.export,
                                    BTreeMap::from([
                                        ("input".to_string(), message.payload.clone()),
                                        ("message".to_string(), invocation.payload.clone()),
                                        ("baggage".to_string(), baggage),
                                    ]),
                                    arguments,
                                )
                                .map(|body| ExtensionInvocationResult {
                                    status: 200,
                                    headers: BTreeMap::new(),
                                    body,
                                    ack: true,
                                    retry_after_ms: None,
                                    error: None,
                                })
                            })()
                        } else {
                            CapabilityHost::new(&db, manifest.clone(), actor, services.clone())
                                .and_then(|host| {
                                    module
                                        .invoke_with_host(&invocation, Box::new(host))
                                        .map_err(Into::into)
                                })
                        }
                    };
                    let db = db.read();
                    match result {
                        Ok(result) if result.ack && result.error.is_none() => {
                            if let Err(error) = db.with_broker(|broker| {
                                broker.ack(
                                    &worker.queue,
                                    &worker.group,
                                    &consumer,
                                    message.message_id,
                                )
                            }) {
                                eprintln!("bicdb: worker `{}` ack failed: {error}", worker.name);
                            }
                        }
                        result => {
                            let attempts_remain = message.attempts < worker.max_attempts;
                            let (retry, delay, error) = match result {
                                Ok(result) => {
                                    let retryable_status = result.status >= 500
                                        || matches!(result.status, 408 | 409 | 425 | 429);
                                    (
                                        attempts_remain && retryable_status,
                                        result.retry_after_ms,
                                        result.error.or_else(|| {
                                            Some(
                                                if retryable_status {
                                                    "worker requested retry"
                                                } else {
                                                    "worker rejected non-retryable message"
                                                }
                                                .to_string(),
                                            )
                                        }),
                                    )
                                }
                                Err(error) => (
                                    attempts_remain,
                                    (worker.retry_delay_ms > 0).then_some(worker.retry_delay_ms),
                                    Some(error.to_string()),
                                ),
                            };
                            eprintln!(
                                "bicdb: worker `{}` delivery failed: message_id={}, attempt={}/{}, retry={}, retry_delay_ms={}, error={}",
                                worker.name,
                                message.message_id,
                                message.attempts,
                                worker.max_attempts,
                                retry,
                                delay.unwrap_or_default(),
                                error.as_deref().unwrap_or("unspecified worker failure"),
                            );
                            let settle = db.with_broker(|broker| {
                                if !retry && worker.dead_letter_queue.is_some() {
                                    let dead_letter_queue = worker
                                        .dead_letter_queue
                                        .as_ref()
                                        .expect("checked dead-letter queue");
                                    let mut headers =
                                        message.headers.as_object().cloned().unwrap_or_default();
                                    headers.insert(
                                        "carrier_dead_letter_source".to_string(),
                                        Value::String(worker.queue.clone()),
                                    );
                                    headers.insert(
                                        "carrier_dead_letter_worker".to_string(),
                                        Value::String(worker.name.clone()),
                                    );
                                    if let Some(error) = &error {
                                        headers.insert(
                                            "carrier_dead_letter_error".to_string(),
                                            Value::String(error.clone()),
                                        );
                                    }
                                    broker.publish_with(
                                        dead_letter_queue,
                                        message.payload.clone(),
                                        PublishOptions {
                                            headers: Value::Object(headers),
                                            idempotency_key: Some(format!(
                                                "carrier-dead-letter:{}:{}",
                                                worker.name, message.message_id
                                            )),
                                            delay_ms: None,
                                            max_attempts: Some(1),
                                        },
                                    )?;
                                    broker.ack(
                                        &worker.queue,
                                        &worker.group,
                                        &consumer,
                                        message.message_id,
                                    )
                                } else {
                                    broker.nack(
                                        &worker.queue,
                                        &worker.group,
                                        &consumer,
                                        message.message_id,
                                        NackOptions {
                                            requeue: retry,
                                            delay_ms: delay,
                                            error,
                                        },
                                    )
                                }
                            });
                            if let Err(error) = settle {
                                eprintln!("bicdb: worker `{}` nack failed: {error}", worker.name);
                            }
                        }
                    }
                }
            }
            thread_healthy.store(false, Ordering::Release);
        })
        .map_err(|error| AppRuntimeError::Provider(error.to_string()))?;
    healthy.store(true, Ordering::Release);
    Ok(SupervisorControl {
        stop,
        active,
        healthy,
        task: Some(task),
    })
}

pub(crate) const SCHEDULE_STATE_RELATION: &str = "__bicdb_app_schedules";
pub(crate) const SCHEDULE_MISFIRE_GRACE_MS: i64 = 500;
pub(crate) const MAX_SCHEDULE_STATE_RETRIES: usize = 16;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableScheduleOccurrence {
    pub(crate) scheduled_for_ms: i64,
    pub(crate) attempt: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableRunningScheduleOccurrence {
    pub(crate) scheduled_for_ms: i64,
    pub(crate) attempt: u32,
    pub(crate) node_id: String,
    pub(crate) instance_id: String,
    pub(crate) lease_until_ms: i64,
    pub(crate) started_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableScheduleState {
    pub(crate) application: String,
    pub(crate) schedule: String,
    pub(crate) contract_sha256: String,
    pub(crate) revision: u64,
    pub(crate) cursor_unix_ms: i64,
    pub(crate) pending: VecDeque<DurableScheduleOccurrence>,
    pub(crate) running: Vec<DurableRunningScheduleOccurrence>,
    pub(crate) completed_runs: u64,
    pub(crate) failed_runs: u64,
    pub(crate) last_completed_at_ms: Option<i64>,
    pub(crate) last_error: Option<String>,
    pub(crate) created_at_ms: i64,
    pub(crate) updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScheduleClaim {
    pub(crate) scheduled_for_ms: i64,
    pub(crate) attempt: u32,
    pub(crate) node_id: String,
    pub(crate) instance_id: String,
}
