//! Schema-bound generic BicDB application resource operations.
//!
//! BicDB application emits signed [`ResourceContractV1`] values. This interpreter owns
//! HTTP/resource semantics while every data access, transaction, mutation
//! grant, audit, and outbox operation still crosses the capability boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use base64::Engine;
use bicdb_extension::abi_v2::{
    AggregateSpec, ApplicationExpressionV1, ApplicationRouteParameterTypeV1, CryptoRequest,
    DatabaseRequest, ErrorClass, FieldType, FilterExpression, HostCall, HostHandle, HostRequest,
    HostValue, MutationGrantRequest, QuerySpec, ResourceContractV1, ResourceOperation,
    ResourcePolicyRoleMatchV1, ResourcePolicyRuleV1, ResourceRecordScope, SecretRequest, SortField,
    TransactionRequest, ValidationRuleKind,
};
use bicdb_extension::host::ApplicationHost;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::host::CapabilityHost;
use crate::http::normalize_carrier_json_value;
use crate::program::evaluate_carrier_expression;
use crate::{AppRuntimeError, Result};

static EMAIL_VALIDATION_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
        .expect("BicDB application email regex compiles")
});

pub(crate) fn resource_search_index_name(contract: &ResourceContractV1) -> String {
    format!("{}_search", contract.relation)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResourceRequest {
    pub operation: ResourceOperation,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub body: Value,
    #[serde(default)]
    pub filters: Vec<FilterExpression>,
    #[serde(default)]
    pub relation_filters: BTreeMap<String, Value>,
    #[serde(default)]
    pub sort: Vec<SortField>,
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default = "default_page_size")]
    pub limit: u32,
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub scope: ResourceRecordScope,
    #[serde(default)]
    pub expected_version: Option<u64>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// Internal BicDB application model helpers require the exact page total. Native
    /// resource HTTP remains backward compatible and omits the extra count.
    #[serde(default)]
    pub include_total: bool,
    /// Compiler-emitted model helpers may filter any signed model field even
    /// when that field is intentionally absent from the public CRUD query
    /// vocabulary. HTTP-created requests always leave this false.
    #[serde(default)]
    pub internal_model_call: bool,
}

fn default_page_size() -> u32 {
    50
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResourceResponse {
    pub status: u16,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    pub body: Value,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceLifecycleContract {
    version: u32,
    name: String,
    stage_field: String,
    stages: Vec<ResourceLifecycleStage>,
    transitions: Vec<ResourceLifecycleTransition>,
    #[serde(default)]
    journal: Option<ResourceLifecycleJournal>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceLifecycleStage {
    name: String,
    #[serde(default)]
    stamp_field: Option<String>,
    #[serde(default)]
    actor: Option<String>,
    #[serde(default)]
    terminal: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceLifecycleTransition {
    #[serde(default)]
    from: Option<String>,
    to: String,
    #[serde(default)]
    actor: Option<String>,
    #[serde(default)]
    reason_required: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceLifecycleJournal {
    resource: String,
    relation: String,
}

#[derive(Debug)]
struct PreparedLifecycleTransition {
    prior_stage: String,
    new_stage: String,
    reason: Option<String>,
    occurred_at: String,
    organization_id: Option<Value>,
    journal: Option<ResourceLifecycleJournal>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResourceMutation {
    pub operation: ResourceOperation,
    pub before: Value,
    pub after: Value,
}

struct ResourceExecution {
    response: ResourceResponse,
    mutation: Option<ResourceMutation>,
}

impl ResourceExecution {
    fn read(response: ResourceResponse) -> Self {
        Self {
            response,
            mutation: None,
        }
    }
}

/// Execute one contract operation using only ABI-v2 host capabilities.
pub fn execute_resource_operation(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: ResourceRequest,
) -> Result<ResourceResponse> {
    execute_resource_operation_with_mutation_hook(host, contract, request, |_, _, _| Ok(()))
}

pub(crate) fn execute_resource_operation_with_mutation_hook(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: ResourceRequest,
    mut hook: impl FnMut(&mut CapabilityHost, HostHandle, &ResourceMutation) -> Result<()>,
) -> Result<ResourceResponse> {
    authorize(host, contract, &request)?;
    validate_request(contract, &request)?;
    let transaction = begin(host)?;
    let idempotency = match begin_idempotency(host, contract, &request, transaction) {
        Ok(IdempotencyStart::None) => None,
        Ok(IdempotencyStart::Reserved(reservation)) => Some(reservation),
        Ok(IdempotencyStart::Replay(mut response)) => {
            call(
                host,
                HostRequest::Transaction(TransactionRequest::Commit { transaction }),
            )?;
            response
                .headers
                .push(("idempotency-replayed".to_string(), "true".to_string()));
            return Ok(response);
        }
        Err(error) => {
            let _ = call(
                host,
                HostRequest::Transaction(TransactionRequest::Rollback { transaction }),
            );
            return Err(error);
        }
    };
    let result = execute_in_transaction(host, contract, &request, transaction);
    match result {
        Ok(mut execution) => {
            if let Some(mutation) = &execution.mutation {
                if let Err(error) = hook(host, transaction, mutation) {
                    let _ = call(
                        host,
                        HostRequest::Transaction(TransactionRequest::Rollback { transaction }),
                    );
                    return Err(error);
                }
            }
            if let Some(reservation) = idempotency {
                host.complete_idempotency(
                    transaction,
                    &reservation.key,
                    &reservation.request_sha256,
                    reservation.expires_at_ms,
                    serde_json::to_value(&execution.response)?,
                )?;
            }
            call(
                host,
                HostRequest::Transaction(TransactionRequest::Commit { transaction }),
            )?;
            execution.response.headers.push((
                "x-bicdb-contract-version".to_string(),
                contract.version.to_string(),
            ));
            apply_cache_headers(contract, request.operation, &mut execution.response);
            Ok(execution.response)
        }
        Err(error) => {
            let _ = call(
                host,
                HostRequest::Transaction(TransactionRequest::Rollback { transaction }),
            );
            Err(error)
        }
    }
}

/// Execute a resource operation inside a transaction already owned by a
/// BicDB application program. The caller owns commit/rollback; validation, authorization,
/// mutation grants, audit, and publish-on-commit semantics remain identical to
/// the ordinary resource path.
pub(crate) fn execute_resource_operation_in_transaction(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: ResourceRequest,
    transaction: HostHandle,
) -> Result<ResourceResponse> {
    execute_resource_operation_in_transaction_with_mutation_hook(
        host,
        contract,
        request,
        transaction,
        |_, _, _| Ok(()),
    )
}

pub(crate) fn execute_resource_operation_in_transaction_with_mutation_hook(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: ResourceRequest,
    transaction: HostHandle,
    mut hook: impl FnMut(&mut CapabilityHost, HostHandle, &ResourceMutation) -> Result<()>,
) -> Result<ResourceResponse> {
    authorize(host, contract, &request)?;
    validate_request(contract, &request)?;
    if contract.idempotency.is_some()
        && matches!(
            request.operation,
            ResourceOperation::Create | ResourceOperation::Action
        )
    {
        return Err(AppRuntimeError::CapabilityDenied(
            "idempotent resource mutations cannot join an enclosing BicDB application transaction"
                .to_string(),
        ));
    }
    let execution = execute_in_transaction(host, contract, &request, transaction)?;
    if let Some(mutation) = &execution.mutation {
        hook(host, transaction, mutation)?;
    }
    let mut response = execution.response;
    response.headers.push((
        "x-bicdb-contract-version".to_string(),
        contract.version.to_string(),
    ));
    apply_cache_headers(contract, request.operation, &mut response);
    Ok(response)
}

struct IdempotencyReservation {
    key: String,
    request_sha256: String,
    expires_at_ms: i64,
}

enum IdempotencyStart {
    None,
    Reserved(IdempotencyReservation),
    Replay(ResourceResponse),
}

fn begin_idempotency(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: &ResourceRequest,
    transaction: HostHandle,
) -> Result<IdempotencyStart> {
    let Some(configuration) = contract.idempotency.as_ref() else {
        return Ok(IdempotencyStart::None);
    };
    if !matches!(
        request.operation,
        ResourceOperation::Create | ResourceOperation::Action
    ) {
        return Ok(IdempotencyStart::None);
    }
    let raw_key = request
        .idempotency_key
        .as_deref()
        .expect("validated idempotency key");
    let principal = host
        .actor()
        .user_id
        .as_deref()
        .or(host.actor().service_id.as_deref())
        .or(host.actor().client_id.as_deref())
        .unwrap_or_default();
    let identity = format!(
        "{}\0{}\0{}\0{}\0{}\0{}",
        host.application_name(),
        contract.name,
        principal,
        host.actor().tenant_id.as_deref().unwrap_or_default(),
        host.actor().workspace_id.as_deref().unwrap_or_default(),
        raw_key,
    );
    let key = hex_sha256(identity.as_bytes());
    let request_sha256 = hex_sha256(&serde_json::to_vec(request)?);
    let expires_at_ms = crate::host::now_ms()
        .saturating_add((configuration.ttl_seconds.saturating_mul(1000)) as i64);
    match host.begin_idempotency(transaction, &key, &request_sha256, expires_at_ms)? {
        Some(value) => Ok(IdempotencyStart::Replay(serde_json::from_value(value)?)),
        None => Ok(IdempotencyStart::Reserved(IdempotencyReservation {
            key,
            request_sha256,
            expires_at_ms,
        })),
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn execute_in_transaction(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: &ResourceRequest,
    transaction: HostHandle,
) -> Result<ResourceExecution> {
    match request.operation {
        ResourceOperation::List => {
            list(host, contract, request, transaction).map(ResourceExecution::read)
        }
        ResourceOperation::Get => {
            get(host, contract, request, transaction).map(ResourceExecution::read)
        }
        ResourceOperation::Create => create(host, contract, request, transaction, false),
        ResourceOperation::Upsert => upsert(host, contract, request, transaction),
        ResourceOperation::Update => update(host, contract, request, transaction, false, false),
        ResourceOperation::Delete => delete(host, contract, request, transaction),
        ResourceOperation::Restore => update(host, contract, request, transaction, true, false),
        ResourceOperation::Action => Err(AppRuntimeError::CapabilityDenied(
            "custom actions must invoke a declared carrier-program service".to_string(),
        )),
    }
}

fn begin(host: &mut CapabilityHost) -> Result<HostHandle> {
    expect_handle(call(
        host,
        HostRequest::Transaction(TransactionRequest::Begin {
            isolation: bicdb_extension::abi_v2::IsolationLevel::ReadCommitted,
        }),
    )?)
}

fn list(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: &ResourceRequest,
    transaction: HostHandle,
) -> Result<ResourceResponse> {
    let mut filters = request.filters.clone();
    filters.extend(resolve_relation_filters(
        host,
        contract,
        request,
        transaction,
    )?);
    filters.extend(scope_filters(contract, request.scope));
    let policy_filtered = contract.policy.is_some();
    let (rows, total) = if let Some(search) = request.search.as_deref() {
        const MAX_EXACT_SEARCH_ROWS: u32 = 10_000_000;
        let matches = expect_rows(call(
            host,
            HostRequest::Database(DatabaseRequest::FullTextSearch {
                transaction,
                relation: contract.relation.clone(),
                index: resource_search_index_name(contract),
                query: search.to_string(),
                limit: MAX_EXACT_SEARCH_ROWS.saturating_add(1),
            }),
        )?)?
        .into_iter()
        .filter(|row| filters.iter().all(|filter| matches_filter(row, filter)))
        .collect::<Vec<_>>();
        let mut matches = filter_policy_rows(host, contract, matches, transaction)?;
        if matches.len() > MAX_EXACT_SEARCH_ROWS as usize {
            return Err(AppRuntimeError::InvalidRequest(
                "BicDB application exact search pagination exceeds 10000000 matching rows"
                    .to_string(),
            ));
        }
        let total = request.include_total.then_some(matches.len() as u64);
        if !request.sort.is_empty() {
            sort_rows(&mut matches, &request.sort);
        }
        let rows = matches
            .into_iter()
            .skip(request.offset as usize)
            .take(request.limit as usize)
            .collect();
        (rows, total)
    } else if policy_filtered {
        const MAX_POLICY_ROWS: u32 = 10_000_000;
        // Policy filtering must see every candidate row, and the ABI query is
        // capped at 10_000 rows per statement, so the candidate scan goes
        // through the trusted single-statement host path instead: one scan at
        // one snapshot. Paging the ABI query by offset would rescan the
        // collection once per chunk and — because a read-committed transaction
        // refreshes its snapshot before every statement — page over a moving
        // row set, silently skipping or double-counting candidates.
        let rows = host.policy_candidate_rows(transaction, &contract.relation, &filters)?;
        if rows.len() > MAX_POLICY_ROWS as usize {
            return Err(AppRuntimeError::InvalidRequest(
                "BicDB application policy evaluation exceeds 10000000 candidate rows".to_string(),
            ));
        }
        let mut rows = filter_policy_rows(host, contract, rows, transaction)?;
        let total = request.include_total.then_some(rows.len() as u64);
        if !request.sort.is_empty() {
            sort_rows(&mut rows, &request.sort);
        }
        let rows = rows
            .into_iter()
            .skip(request.offset as usize)
            .take(request.limit as usize)
            .collect();
        (rows, total)
    } else {
        let total = if request.include_total {
            Some(expect_u64(call(
                host,
                HostRequest::Database(DatabaseRequest::Aggregate {
                    transaction,
                    relation: contract.relation.clone(),
                    aggregate: AggregateSpec::Count,
                    filters: filters.clone(),
                }),
            )?)?)
        } else {
            None
        };
        let rows = expect_rows(call(
            host,
            HostRequest::Database(DatabaseRequest::Query {
                transaction,
                query: QuerySpec {
                    relation: contract.relation.clone(),
                    filters,
                    sort: request.sort.clone(),
                    columns: Vec::new(),
                    limit: request.limit,
                    offset: request.offset,
                    cursor: None,
                },
            }),
        )?)?;
        (rows, total)
    };
    let rows = rows
        .into_iter()
        .map(|row| redact(host, contract, row))
        .collect::<Result<Vec<_>>>()?;
    let mut headers = vec![("x-bicdb-result-count".to_string(), rows.len().to_string())];
    if let Some(total) = total {
        headers.push(("x-bicdb-result-total".to_string(), total.to_string()));
    }
    // Internal model calls consume rows positionally and predate the paged
    // envelope; the HTTP surface serves the same {items, page_info} envelope
    // as every other BicDB application target.
    if request.internal_model_call {
        return Ok(ResourceResponse {
            status: 200,
            headers,
            body: Value::Array(rows),
        });
    }
    let page = if request.limit > 0 {
        (request.offset / u64::from(request.limit)) + 1
    } else {
        1
    };
    let has_more = match total {
        Some(total) => request.offset + (rows.len() as u64) < total,
        None => rows.len() as u32 == request.limit,
    };
    let mut page_info = serde_json::Map::new();
    page_info.insert("page".to_string(), Value::from(page));
    page_info.insert("per_page".to_string(), Value::from(request.limit));
    if let Some(total) = total {
        page_info.insert("total".to_string(), Value::from(total));
    }
    page_info.insert("has_more".to_string(), Value::Bool(has_more));
    page_info.insert("total_is_exact".to_string(), Value::Bool(total.is_some()));
    Ok(ResourceResponse {
        status: 200,
        headers,
        body: serde_json::json!({
            "items": rows,
            "page_info": Value::Object(page_info),
        }),
    })
}

fn get(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: &ResourceRequest,
    transaction: HostHandle,
) -> Result<ResourceResponse> {
    let id = required_id(request)?;
    let value = expect_json(call(
        host,
        HostRequest::Database(DatabaseRequest::SelectPrimaryKey {
            transaction,
            relation: contract.relation.clone(),
            id: id.to_string(),
            columns: Vec::new(),
        }),
    )?)?;
    if value.is_null()
        || !visible_in_scope(contract, &value, request.scope)
        || !read_policy_allows(host, contract, &value, Some(transaction), 0)?
    {
        return Err(AppRuntimeError::NotFound(format!(
            "resource `{}/{id}` was not found",
            contract.name
        )));
    }
    Ok(ResourceResponse {
        status: 200,
        headers: etag(contract, &value),
        body: redact(host, contract, value)?,
    })
}

fn create(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: &ResourceRequest,
    transaction: HostHandle,
    upsert: bool,
) -> Result<ResourceExecution> {
    let mut object = object_body(request)?;
    reject_unwritable_fields(host, contract, request, &object, &contract.create_fields)?;
    // Runtime-owned timestamps and declared defaults satisfy required fields
    // exactly as a column DEFAULT satisfies NOT NULL: they are applied before
    // absence is judged. Every other BicDB application runtime stamps created_at /
    // updated_at server-side; a client cannot be required to invent them.
    apply_timestamp_conventions(contract, &mut object);
    apply_defaults(contract, &mut object)?;
    // Scope fields (tenant/workspace) are runtime-owned like timestamps: the
    // trusted actor stamps them, so they too are applied before absence is
    // judged.
    apply_actor_scope(host, contract, &mut object)?;
    apply_actor_audit_create(host, contract, &mut object)?;
    for field in &contract.required_create_fields {
        if object.get(field).is_none_or(Value::is_null) {
            return Err(AppRuntimeError::Invocation(format!(
                "required field `{field}` is absent"
            )));
        }
    }
    let id = object
        .get(&contract.primary_key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    object.insert(contract.primary_key.clone(), Value::String(id.clone()));
    if let Some(field) = &contract.soft_delete_field {
        object.insert(
            field.clone(),
            contract.restore_value.clone().unwrap_or(Value::Null),
        );
    }
    if let Some(field) = &contract.version_field {
        object.insert(field.clone(), Value::from(1_u64));
    }
    apply_generated_fields(host, contract, &mut object)?;
    normalize_object(contract, &mut object)?;
    validate_object(contract, &object)?;
    validate_checks(host, contract, &object)?;
    validate_relation_references(host, contract, &object, transaction)?;
    let before = Value::Null;
    let after = Value::Object(object.clone());
    enforce_write_policy(host, contract, &after, Some(transaction))?;
    let stored_after = encrypt_resource_value(host, contract, &after)?;
    let grant = issue_grant(
        host,
        contract,
        transaction,
        if upsert {
            ResourceOperation::Upsert
        } else {
            ResourceOperation::Create
        },
        Some(id.clone()),
        object.keys().cloned().collect(),
        None,
    )?;
    let database = if upsert {
        DatabaseRequest::Upsert {
            transaction,
            grant,
            relation: contract.relation.clone(),
            record: stored_after.clone(),
            expected_version: None,
        }
    } else {
        DatabaseRequest::Insert {
            transaction,
            grant,
            relation: contract.relation.clone(),
            record: stored_after,
        }
    };
    call(host, HostRequest::Database(database))?;
    let mut side_effect_request = request.clone();
    if upsert {
        side_effect_request.operation = ResourceOperation::Create;
    }
    register_side_effects(
        host,
        contract,
        &side_effect_request,
        transaction,
        &before,
        &after,
    )?;
    Ok(ResourceExecution {
        response: ResourceResponse {
            status: 201,
            headers: etag(contract, &after),
            body: redact_plain(host, contract, after.clone())?,
        },
        mutation: Some(ResourceMutation {
            operation: ResourceOperation::Create,
            before,
            after,
        }),
    })
}

fn update(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: &ResourceRequest,
    transaction: HostHandle,
    restore: bool,
    upsert: bool,
) -> Result<ResourceExecution> {
    let id = required_id(request)?.to_string();
    let before = load_required(host, contract, transaction, &id)?;
    enforce_scope(host, contract, &before)?;
    enforce_write_policy(host, contract, &before, Some(transaction))?;
    let soft_deleted = is_soft_deleted(contract, &before);
    if !restore && soft_deleted {
        return Err(AppRuntimeError::NotFound(format!(
            "resource `{}/{id}` was not found",
            contract.name
        )));
    }
    if restore && !soft_deleted {
        return Err(AppRuntimeError::Conflict(format!(
            "resource `{}/{id}` is not deleted",
            contract.name
        )));
    }
    let mut patch = if restore {
        Map::new()
    } else {
        object_body(request)?
    };
    let transition_reason = patch.remove("_transition_reason");
    let body_expected_version = take_body_expected_version(contract, &mut patch)?;
    reject_unwritable_fields(host, contract, request, &patch, &contract.update_fields)?;
    if restore {
        let field = contract.soft_delete_field.as_ref().ok_or_else(|| {
            AppRuntimeError::Invocation("resource does not support restore".to_string())
        })?;
        patch.insert(
            field.clone(),
            contract.restore_value.clone().unwrap_or(Value::Null),
        );
    }
    for immutable in &contract.immutable_fields {
        if patch.contains_key(immutable) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "field `{immutable}` is immutable"
            )));
        }
    }
    apply_actor_audit_update(host, contract, &mut patch)?;
    let lifecycle_transition =
        prepare_lifecycle_transition(host, contract, &before, &mut patch, transition_reason)?;
    let expected_version = expected_version(contract, request, &before, body_expected_version)?;
    if let Some(field) = &contract.version_field {
        patch.insert(
            field.clone(),
            Value::from(expected_version.unwrap_or(0) + 1),
        );
    }
    let mut after = before.as_object().cloned().ok_or_else(|| {
        AppRuntimeError::Invocation("stored resource is not an object".to_string())
    })?;
    for (field, value) in &patch {
        after.insert(field.clone(), value.clone());
    }
    apply_generated_fields(host, contract, &mut after)?;
    normalize_object(contract, &mut after)?;
    for field in patch.keys().cloned().collect::<Vec<_>>() {
        if let Some(value) = after.get(&field) {
            patch.insert(field, value.clone());
        }
    }
    for field in contract.fields.iter().filter(|field| field.generated) {
        if before.get(&field.name) != after.get(&field.name) {
            patch.insert(
                field.name.clone(),
                after.get(&field.name).cloned().unwrap_or(Value::Null),
            );
        }
    }
    validate_object(contract, &after)?;
    validate_checks(host, contract, &after)?;
    validate_relation_references(host, contract, &after, transaction)?;
    enforce_write_policy(
        host,
        contract,
        &Value::Object(after.clone()),
        Some(transaction),
    )?;
    let operation = if restore {
        ResourceOperation::Restore
    } else if upsert {
        ResourceOperation::Upsert
    } else {
        ResourceOperation::Update
    };
    let grant = issue_grant(
        host,
        contract,
        transaction,
        operation,
        Some(id.clone()),
        patch.keys().cloned().collect(),
        expected_version,
    )?;
    let stored_patch = encrypt_resource_value(host, contract, &Value::Object(patch))?;
    let stored_patch = stored_patch
        .as_object()
        .cloned()
        .expect("resource encryption preserves objects");
    let database = if upsert {
        DatabaseRequest::Upsert {
            transaction,
            grant,
            relation: contract.relation.clone(),
            record: encrypt_resource_value(host, contract, &Value::Object(after.clone()))?,
            expected_version,
        }
    } else {
        DatabaseRequest::Update {
            transaction,
            grant,
            relation: contract.relation.clone(),
            id: id.clone(),
            patch: Value::Object(stored_patch),
            expected_version,
        }
    };
    call(host, HostRequest::Database(database))?;
    if let Some(transition) = lifecycle_transition {
        insert_lifecycle_journal(host, contract, transaction, &id, transition)?;
    }
    let after = Value::Object(after);
    let mut side_effect_request = request.clone();
    if upsert {
        side_effect_request.operation = ResourceOperation::Update;
    }
    register_side_effects(
        host,
        contract,
        &side_effect_request,
        transaction,
        &before,
        &after,
    )?;
    Ok(ResourceExecution {
        response: ResourceResponse {
            status: 200,
            headers: etag(contract, &after),
            body: redact_plain(host, contract, after.clone())?,
        },
        mutation: Some(ResourceMutation {
            operation: if restore {
                ResourceOperation::Restore
            } else {
                ResourceOperation::Update
            },
            before,
            after,
        }),
    })
}

fn upsert(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: &ResourceRequest,
    transaction: HostHandle,
) -> Result<ResourceExecution> {
    let mut request = request.clone();
    let mut body = object_body(&request)?;
    let id = request.id.clone().or_else(|| {
        body.get(&contract.primary_key)
            .and_then(Value::as_str)
            .map(str::to_string)
    });
    let Some(id) = id else {
        return create(host, contract, &request, transaction, true);
    };
    let existing = expect_json(call(
        host,
        HostRequest::Database(DatabaseRequest::SelectPrimaryKey {
            transaction,
            relation: contract.relation.clone(),
            id: id.clone(),
            columns: Vec::new(),
        }),
    )?)?;
    if existing.is_null() {
        body.insert(contract.primary_key.clone(), Value::String(id));
        request.body = Value::Object(body);
        create(host, contract, &request, transaction, true)
    } else {
        body.remove(&contract.primary_key);
        request.id = Some(id);
        request.body = Value::Object(body);
        update(host, contract, &request, transaction, false, true)
    }
}

fn delete(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: &ResourceRequest,
    transaction: HostHandle,
) -> Result<ResourceExecution> {
    let id = required_id(request)?.to_string();
    let before = load_required(host, contract, transaction, &id)?;
    enforce_scope(host, contract, &before)?;
    enforce_write_policy(host, contract, &before, Some(transaction))?;
    if is_soft_deleted(contract, &before) {
        return Err(AppRuntimeError::NotFound(format!(
            "resource `{}/{id}` was not found",
            contract.name
        )));
    }
    let expected = expected_version(contract, request, &before, None)?;
    let after = if let Some(field) = &contract.soft_delete_field {
        let deleted_value = match &contract.soft_delete_value {
            Some(value) => value.clone(),
            None => Value::String(
                chrono::DateTime::<chrono::Utc>::from_timestamp_millis(crate::host::now_ms())
                    .ok_or_else(|| {
                        AppRuntimeError::Invocation(
                            "soft-delete timestamp is outside RFC3339 range".to_string(),
                        )
                    })?
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            ),
        };
        let mut patch = Map::from_iter([(field.clone(), deleted_value)]);
        if let Some(version_field) = &contract.version_field {
            patch.insert(
                version_field.clone(),
                Value::from(expected.unwrap_or(0).saturating_add(1)),
            );
        }
        let mut after = before.as_object().cloned().unwrap_or_default();
        after.extend(patch.clone());
        apply_generated_fields(host, contract, &mut after)?;
        for generated in contract.fields.iter().filter(|field| field.generated) {
            if before.get(&generated.name) != after.get(&generated.name) {
                patch.insert(
                    generated.name.clone(),
                    after.get(&generated.name).cloned().unwrap_or(Value::Null),
                );
            }
        }
        validate_object(contract, &after)?;
        validate_checks(host, contract, &after)?;
        validate_relation_references(host, contract, &after, transaction)?;
        let grant = issue_grant(
            host,
            contract,
            transaction,
            ResourceOperation::Delete,
            Some(id.clone()),
            patch.keys().cloned().collect(),
            expected,
        )?;
        call(
            host,
            HostRequest::Database(DatabaseRequest::Update {
                transaction,
                grant,
                relation: contract.relation.clone(),
                id,
                patch: Value::Object(patch),
                expected_version: expected,
            }),
        )?;
        let after = Value::Object(after);
        register_side_effects(host, contract, request, transaction, &before, &after)?;
        after
    } else {
        ensure_relation_delete_is_unreferenced(host, contract, &before, transaction)?;
        let grant = issue_grant(
            host,
            contract,
            transaction,
            ResourceOperation::Delete,
            Some(id.clone()),
            BTreeSet::new(),
            expected,
        )?;
        call(
            host,
            HostRequest::Database(DatabaseRequest::Delete {
                transaction,
                grant,
                relation: contract.relation.clone(),
                id,
            }),
        )?;
        register_side_effects(host, contract, request, transaction, &before, &Value::Null)?;
        Value::Null
    };
    Ok(ResourceExecution {
        response: ResourceResponse {
            status: 204,
            headers: Vec::new(),
            body: Value::Null,
        },
        mutation: Some(ResourceMutation {
            operation: ResourceOperation::Delete,
            before,
            after,
        }),
    })
}

fn validate_relation_references(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    object: &Map<String, Value>,
    transaction: HostHandle,
) -> Result<()> {
    for relation in &contract.relations {
        let Some(value) = object
            .get(&relation.source_field)
            .filter(|value| !value.is_null())
        else {
            continue;
        };
        let target = host
            .application_manifest()
            .resources
            .iter()
            .find(|resource| resource.name == relation.target_resource)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "relation target resource `{}` is absent",
                    relation.target_resource
                ))
            })?;
        let key_type = target
            .fields
            .iter()
            .find(|field| field.name == relation.target_field)
            .and_then(|field| carrier_filter_type_for_field(&field.field_type))
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "relation target `{}.{}` has an unsupported key type",
                    relation.target_resource, relation.target_field
                ))
            })?;
        let rows = expect_rows(call(
            host,
            HostRequest::Database(DatabaseRequest::Query {
                transaction,
                query: QuerySpec {
                    relation: target.relation,
                    filters: vec![FilterExpression::TypedEq {
                        field: relation.target_field.clone(),
                        value: value.clone(),
                        value_type: key_type,
                    }],
                    sort: Vec::new(),
                    columns: vec![relation.target_field.clone()],
                    limit: 1,
                    offset: 0,
                    cursor: None,
                },
            }),
        )?)?;
        if rows.is_empty() {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "relation field `{}` references an absent `{}`",
                relation.source_field, relation.target_resource
            )));
        }
    }
    for foreign_key in &contract.foreign_keys {
        let values = foreign_key
            .fields
            .iter()
            .map(|field| object.get(field))
            .collect::<Option<Vec<_>>>();
        let Some(values) = values.filter(|values| values.iter().all(|value| !value.is_null()))
        else {
            // BicDB application/PostgreSQL composite foreign keys use MATCH SIMPLE:
            // any nullable component disables the row-level reference check.
            continue;
        };
        let target = host
            .application_manifest()
            .resources
            .iter()
            .find(|resource| resource.name == foreign_key.target_resource)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "foreign key target resource `{}` is absent",
                    foreign_key.target_resource
                ))
            })?;
        let filters = foreign_key
            .target_fields
            .iter()
            .zip(values)
            .map(|(field_name, value)| {
                let value_type = target
                    .fields
                    .iter()
                    .find(|field| field.name == *field_name)
                    .and_then(|field| carrier_filter_type_for_field(&field.field_type))
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidPackage(format!(
                            "foreign key target `{}.{field_name}` has an unsupported key type",
                            foreign_key.target_resource
                        ))
                    })?;
                Ok(FilterExpression::TypedEq {
                    field: field_name.clone(),
                    value: value.clone(),
                    value_type,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let rows = expect_rows(call(
            host,
            HostRequest::Database(DatabaseRequest::Query {
                transaction,
                query: QuerySpec {
                    relation: target.relation,
                    filters,
                    sort: Vec::new(),
                    columns: foreign_key.target_fields.clone(),
                    limit: 1,
                    offset: 0,
                    cursor: None,
                },
            }),
        )?)?;
        if rows.is_empty() {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "foreign key `{}` references an absent `{}`",
                foreign_key.name, foreign_key.target_resource
            )));
        }
    }
    Ok(())
}

fn ensure_relation_delete_is_unreferenced(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    object: &Value,
    transaction: HostHandle,
) -> Result<()> {
    let references = host
        .application_manifest()
        .resources
        .iter()
        .flat_map(|source| {
            source.relations.iter().filter_map(|relation| {
                (relation.target_resource == contract.name).then_some((
                    source.name.clone(),
                    source.relation.clone(),
                    relation.source_field.clone(),
                    relation.target_field.clone(),
                ))
            })
        })
        .collect::<BTreeSet<_>>();
    for (source_name, source_relation, source_field, target_field) in references {
        let Some(target_value) = object.get(&target_field).filter(|value| !value.is_null()) else {
            continue;
        };
        let key_type = contract
            .fields
            .iter()
            .find(|field| field.name == target_field)
            .and_then(|field| carrier_filter_type_for_field(&field.field_type))
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "resource `{}.{target_field}` has an unsupported relation key type",
                    contract.name
                ))
            })?;
        let rows = expect_rows(call(
            host,
            HostRequest::Database(DatabaseRequest::Query {
                transaction,
                query: QuerySpec {
                    relation: source_relation,
                    filters: vec![FilterExpression::TypedEq {
                        field: source_field.clone(),
                        value: target_value.clone(),
                        value_type: key_type,
                    }],
                    sort: Vec::new(),
                    columns: vec![source_field],
                    limit: 1,
                    offset: 0,
                    cursor: None,
                },
            }),
        )?)?;
        if !rows.is_empty() {
            return Err(AppRuntimeError::Conflict(format!(
                "resource `{}` is still referenced by `{source_name}`",
                contract.name
            )));
        }
    }
    let composite_references = host
        .application_manifest()
        .resources
        .iter()
        .flat_map(|source| {
            source.foreign_keys.iter().filter_map(|foreign_key| {
                (foreign_key.target_resource == contract.name).then_some((
                    source.name.clone(),
                    source.relation.clone(),
                    foreign_key.clone(),
                ))
            })
        })
        .collect::<Vec<_>>();
    for (source_name, source_relation, foreign_key) in composite_references {
        let target_values = foreign_key
            .target_fields
            .iter()
            .map(|field| object.get(field))
            .collect::<Option<Vec<_>>>();
        let Some(target_values) =
            target_values.filter(|values| values.iter().all(|value| !value.is_null()))
        else {
            continue;
        };
        let filters = foreign_key
            .fields
            .iter()
            .zip(&foreign_key.target_fields)
            .zip(target_values)
            .map(|((source_field, target_field), value)| {
                let value_type = contract
                    .fields
                    .iter()
                    .find(|field| field.name == *target_field)
                    .and_then(|field| carrier_filter_type_for_field(&field.field_type))
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidPackage(format!(
                            "resource `{}.{target_field}` has an unsupported foreign-key type",
                            contract.name
                        ))
                    })?;
                Ok(FilterExpression::TypedEq {
                    field: source_field.clone(),
                    value: value.clone(),
                    value_type,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let rows = expect_rows(call(
            host,
            HostRequest::Database(DatabaseRequest::Query {
                transaction,
                query: QuerySpec {
                    relation: source_relation,
                    filters,
                    sort: Vec::new(),
                    columns: foreign_key.fields.clone(),
                    limit: 1,
                    offset: 0,
                    cursor: None,
                },
            }),
        )?)?;
        if !rows.is_empty() {
            return Err(AppRuntimeError::Conflict(format!(
                "resource `{}` is still referenced by `{source_name}` through `{}`",
                contract.name, foreign_key.name
            )));
        }
    }
    Ok(())
}

fn carrier_filter_type_for_field(
    field_type: &FieldType,
) -> Option<ApplicationRouteParameterTypeV1> {
    match field_type {
        FieldType::String => Some(ApplicationRouteParameterTypeV1::String),
        FieldType::Int64 => Some(ApplicationRouteParameterTypeV1::Int),
        FieldType::Float64 => Some(ApplicationRouteParameterTypeV1::Float),
        FieldType::Decimal => Some(ApplicationRouteParameterTypeV1::Decimal),
        FieldType::Bool => Some(ApplicationRouteParameterTypeV1::Bool),
        FieldType::Uuid => Some(ApplicationRouteParameterTypeV1::Uuid),
        FieldType::Timestamp => Some(ApplicationRouteParameterTypeV1::Timestamp),
        FieldType::Date => Some(ApplicationRouteParameterTypeV1::Date),
        _ => None,
    }
}

fn issue_grant(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    transaction: HostHandle,
    operation: ResourceOperation,
    record_id: Option<String>,
    columns: BTreeSet<String>,
    expected_version: Option<u64>,
) -> Result<HostHandle> {
    expect_handle(call(
        host,
        HostRequest::MutationGrant(MutationGrantRequest {
            transaction,
            resource: contract.name.clone(),
            operation,
            relation: contract.relation.clone(),
            record_id,
            predicate: None,
            expected_version,
            columns,
            bulk: false,
            max_rows: 1,
            statement_budget: 1,
            audit_metadata: BTreeMap::from([
                ("resource".to_string(), contract.name.clone()),
                (
                    "contract_hash".to_string(),
                    contract.contract_sha256.clone(),
                ),
            ]),
        }),
    )?)
}

fn resource_lifecycle(contract: &ResourceContractV1) -> Result<Option<ResourceLifecycleContract>> {
    let Some(value) = contract
        .openapi
        .get("x-bicdb-lifecycle")
        .or_else(|| contract.openapi.get("x-carrier-lifecycle"))
    else {
        return Ok(None);
    };
    let lifecycle: ResourceLifecycleContract =
        serde_json::from_value(value.clone()).map_err(|error| {
            AppRuntimeError::InvalidPackage(format!(
                "resource `{}` has an invalid BicDB application lifecycle contract: {error}",
                contract.name
            ))
        })?;
    if lifecycle.version != 1 || lifecycle.name.trim().is_empty() {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "resource `{}` has an unsupported BicDB application lifecycle contract",
            contract.name
        )));
    }
    if !contract
        .fields
        .iter()
        .any(|field| field.name == lifecycle.stage_field)
    {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "resource `{}` lifecycle stage field `{}` does not exist",
            contract.name, lifecycle.stage_field
        )));
    }
    for stage in &lifecycle.stages {
        if let Some(stamp_field) = &stage.stamp_field {
            if !contract
                .fields
                .iter()
                .any(|field| field.name == *stamp_field)
            {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "resource `{}` lifecycle stamp field `{stamp_field}` does not exist",
                    contract.name
                )));
            }
        }
        let _ = (&stage.actor, stage.terminal);
    }
    Ok(Some(lifecycle))
}

fn lifecycle_actor_value(
    host: &CapabilityHost,
    contract: &ResourceContractV1,
    field_name: &str,
) -> Result<Option<Value>> {
    let Some(field) = contract
        .fields
        .iter()
        .find(|field| field.name == field_name)
    else {
        return Ok(None);
    };
    let actor = audit_actor_id(host)?;
    let value = match field.field_type {
        FieldType::Int64 => actor.parse::<i64>().ok().map(Value::from),
        FieldType::Uuid => Uuid::parse_str(&actor)
            .ok()
            .map(|value| Value::String(value.to_string())),
        FieldType::String => Some(Value::String(actor)),
        _ => None,
    };
    Ok(value)
}

fn prepare_lifecycle_transition(
    host: &CapabilityHost,
    contract: &ResourceContractV1,
    before: &Value,
    patch: &mut Map<String, Value>,
    transition_reason: Option<Value>,
) -> Result<Option<PreparedLifecycleTransition>> {
    let Some(lifecycle) = resource_lifecycle(contract)? else {
        if transition_reason.is_some() {
            return Err(AppRuntimeError::InvalidRequest(
                "_transition_reason is valid only for lifecycle transitions".to_string(),
            ));
        }
        return Ok(None);
    };
    let prior = before
        .get(&lifecycle.stage_field)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "stored resource `{}` has no lifecycle stage",
                contract.name
            ))
        })?
        .to_string();
    let Some(target) = patch
        .get(&lifecycle.stage_field)
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        if transition_reason.is_some() {
            return Err(AppRuntimeError::InvalidRequest(
                "_transition_reason requires a lifecycle stage change".to_string(),
            ));
        }
        let mutable_stage = lifecycle.stages.first().map(|stage| stage.name.as_str());
        if Some(prior.as_str()) != mutable_stage {
            return Err(AppRuntimeError::Conflict(format!(
                "resource `{}` is immutable in lifecycle stage `{prior}`; use an explicit transition or amendment path",
                contract.name
            )));
        }
        return Ok(None);
    };
    if prior == target {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "resource `{}` is already in lifecycle stage `{target}`",
            contract.name
        )));
    }
    let transition = lifecycle
        .transitions
        .iter()
        .find(|transition| {
            transition.to == target && transition.from.as_deref().is_none_or(|from| from == prior)
        })
        .ok_or_else(|| {
            AppRuntimeError::InvalidRequest(format!(
                "lifecycle transition `{prior}` -> `{target}` is not allowed for `{}`",
                contract.name
            ))
        })?;
    if let Some(role) = transition.actor.as_deref() {
        if !host.actor().roles.contains(role)
            && !host.actor().roles.contains("owner")
            && !host.actor().roles.contains("admin")
        {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "lifecycle transition `{prior}` -> `{target}` requires role `{role}`"
            )));
        }
    }
    let reason = match transition_reason {
        None | Some(Value::Null) => None,
        Some(Value::String(reason)) if !reason.trim().is_empty() => Some(reason),
        Some(Value::String(_)) => None,
        Some(_) => {
            return Err(AppRuntimeError::InvalidRequest(
                "_transition_reason must be a string".to_string(),
            ));
        }
    };
    if transition.reason_required && reason.is_none() {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "lifecycle transition `{prior}` -> `{target}` requires a reason"
        )));
    }
    let stage = lifecycle
        .stages
        .iter()
        .find(|stage| stage.name == target)
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "resource `{}` lifecycle transition targets undeclared stage `{target}`",
                contract.name
            ))
        })?;
    let occurred_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    if let Some(stamp_field) = &stage.stamp_field {
        patch.insert(stamp_field.clone(), Value::String(occurred_at.clone()));
    }
    let actor_field = format!("{target}_by");
    if let Some(actor) = lifecycle_actor_value(host, contract, &actor_field)? {
        patch.insert(actor_field, actor);
    }
    if target == "cancelled"
        && contract
            .fields
            .iter()
            .any(|field| field.name == "cancellation_reason")
    {
        patch.insert(
            "cancellation_reason".to_string(),
            reason.clone().map(Value::String).unwrap_or(Value::Null),
        );
    }
    Ok(Some(PreparedLifecycleTransition {
        prior_stage: prior,
        new_stage: target,
        reason,
        occurred_at,
        organization_id: contract
            .tenant_field
            .as_deref()
            .and_then(|field| before.get(field).cloned()),
        journal: lifecycle.journal,
    }))
}

fn insert_lifecycle_journal(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    transaction: HostHandle,
    document_id: &str,
    transition: PreparedLifecycleTransition,
) -> Result<()> {
    let Some(journal) = transition.journal else {
        return Ok(());
    };
    let record = Map::from_iter([
        ("id".to_string(), Value::String(Uuid::new_v4().to_string())),
        (
            "organization_id".to_string(),
            transition.organization_id.unwrap_or(Value::Null),
        ),
        (
            "document_id".to_string(),
            Value::String(document_id.to_string()),
        ),
        (
            "prior_stage".to_string(),
            Value::String(transition.prior_stage),
        ),
        ("new_stage".to_string(), Value::String(transition.new_stage)),
        (
            "reason".to_string(),
            transition.reason.map(Value::String).unwrap_or(Value::Null),
        ),
        ("actor_id".to_string(), Value::Null),
        (
            "occurred_at".to_string(),
            Value::String(transition.occurred_at),
        ),
    ]);
    let columns = record.keys().cloned().collect::<BTreeSet<_>>();
    let grant = expect_handle(call(
        host,
        HostRequest::MutationGrant(MutationGrantRequest {
            transaction,
            resource: journal.resource.clone(),
            operation: ResourceOperation::Create,
            relation: journal.relation.clone(),
            record_id: record.get("id").and_then(Value::as_str).map(str::to_string),
            predicate: None,
            expected_version: None,
            columns,
            bulk: false,
            max_rows: 1,
            statement_budget: 1,
            audit_metadata: BTreeMap::from([
                ("resource".to_string(), journal.resource.clone()),
                ("lifecycle_source".to_string(), contract.name.clone()),
            ]),
        }),
    )?)?;
    call(
        host,
        HostRequest::Database(DatabaseRequest::Insert {
            transaction,
            grant,
            relation: journal.relation,
            record: Value::Object(record),
        }),
    )?;
    Ok(())
}

fn register_side_effects(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: &ResourceRequest,
    transaction: HostHandle,
    before: &Value,
    after: &Value,
) -> Result<()> {
    if contract.audit.required {
        let fields = BTreeMap::from([
            ("resource".to_string(), Value::String(contract.name.clone())),
            (
                "operation".to_string(),
                serde_json::to_value(request.operation)?,
            ),
            (
                "before".to_string(),
                project_fields(
                    before,
                    &contract.audit.include_fields,
                    &contract.audit.redact_fields,
                ),
            ),
            (
                "after".to_string(),
                project_fields(
                    after,
                    &contract.audit.include_fields,
                    &contract.audit.redact_fields,
                ),
            ),
        ]);
        host.register_durable_audit(
            transaction,
            format!("resource.{:?}", request.operation).to_ascii_lowercase(),
            request.id.clone().unwrap_or_else(|| contract.name.clone()),
            fields,
        )?;
    }
    for event in contract
        .events
        .iter()
        .filter(|event| event.operation == request.operation)
    {
        host.publish_resource_event_on_commit(
            transaction,
            &event.queue,
            json!({
                "resource": contract.name,
                "operation": request.operation,
                "before": project_event_fields(before, &event.before_fields),
                "after": project_event_fields(after, &event.after_fields),
                "contract_version": contract.version,
                "schema_version": contract.schema_version,
                "event_schema_version": event.schema_version,
            }),
            request.idempotency_key.clone(),
            &contract.name,
            &format!("{:?}", request.operation).to_ascii_lowercase(),
            contract.schema_version,
            contract.version,
            event.schema_version,
        )?;
    }
    Ok(())
}

pub(crate) fn authorize(
    host: &CapabilityHost,
    contract: &ResourceContractV1,
    request: &ResourceRequest,
) -> Result<()> {
    if !contract.operations.contains(&request.operation) {
        return Err(AppRuntimeError::CapabilityDenied(format!(
            "operation {:?} is not declared by resource `{}`",
            request.operation, contract.name
        )));
    }
    let actor = host.actor();
    if !contract.required_roles.is_subset(&actor.roles)
        || !contract.required_scopes.is_subset(&actor.scopes)
    {
        return Err(AppRuntimeError::CapabilityDenied(format!(
            "actor lacks resource `{}` roles or scopes",
            contract.name
        )));
    }
    for (name, expected) in &contract.policy_attributes {
        if actor.policy_attributes.get(name) != Some(expected) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "actor does not satisfy resource `{}` policy attribute `{name}`",
                contract.name
            )));
        }
    }
    if actor.tenant_id.is_none() && contract.tenant_field.is_some() {
        return Err(AppRuntimeError::CapabilityDenied(
            "tenant-scoped resource requires a trusted tenant".to_string(),
        ));
    }
    if actor.workspace_id.is_none() && contract.workspace_field.is_some() {
        return Err(AppRuntimeError::CapabilityDenied(
            "workspace-scoped resource requires a trusted workspace".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_request(
    contract: &ResourceContractV1,
    request: &ResourceRequest,
) -> Result<()> {
    if request.limit == 0 || request.limit > 1_000 || request.offset > 10_000_000 {
        return Err(AppRuntimeError::Invocation(
            "resource pagination exceeds contract runtime limits".to_string(),
        ));
    }
    for filter in &request.filters {
        let field = filter_field(filter);
        let internal_field = request.internal_model_call
            && contract
                .fields
                .iter()
                .any(|candidate| candidate.name == field);
        if !contract.filters.contains(field) && !is_json_path_filter(filter) && !internal_field {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "filter `{field}` is undeclared"
            )));
        }
        if is_json_path_filter(filter) && !contract.json_path_filters.contains(field) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "JSON-path filter `{field}` is undeclared"
            )));
        }
    }
    for query_name in request.relation_filters.keys() {
        if !contract.filter_contracts.iter().any(|filter| {
            filter.query_name == *query_name
                && matches!(
                    filter.filter,
                    bicdb_extension::abi_v2::ResourceFilterKind::RelationExact { .. }
                        | bicdb_extension::abi_v2::ResourceFilterKind::RelationContains { .. }
                        | bicdb_extension::abi_v2::ResourceFilterKind::RelationMinimum { .. }
                )
        }) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "relation filter `{query_name}` is undeclared"
            )));
        }
    }
    for sort in &request.sort {
        if !contract.sort_fields.contains(&sort.field) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "sort `{}` is undeclared",
                sort.field
            )));
        }
    }
    if request.search.is_some() && contract.search_fields.is_empty() {
        return Err(AppRuntimeError::CapabilityDenied(
            "search is undeclared".to_string(),
        ));
    }
    if let Some(idempotency) = &contract.idempotency {
        if matches!(
            request.operation,
            ResourceOperation::Create | ResourceOperation::Action
        ) {
            let key = request.idempotency_key.as_deref().ok_or_else(|| {
                AppRuntimeError::Invocation(format!(
                    "idempotency key from `{}` is required",
                    idempotency.header
                ))
            })?;
            if key.is_empty() || key.len() > idempotency.max_key_bytes as usize {
                return Err(AppRuntimeError::Invocation(
                    "invalid idempotency key length".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn resolve_relation_filters(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    request: &ResourceRequest,
    transaction: HostHandle,
) -> Result<Vec<FilterExpression>> {
    let mut resolved = Vec::new();
    for (query_name, value) in &request.relation_filters {
        let signed = contract
            .filter_contracts
            .iter()
            .find(|filter| filter.query_name == *query_name)
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "relation filter `{query_name}` has no signed contract"
                ))
            })?;
        let (source_field, target_resource, target_field, predicate) = match &signed.filter {
            bicdb_extension::abi_v2::ResourceFilterKind::RelationExact {
                field,
                target_resource,
                target_field,
                value_type,
            } => (
                field,
                target_resource,
                target_field,
                FilterExpression::TypedEq {
                    field: target_field.clone(),
                    value: value.clone(),
                    value_type: value_type.clone(),
                },
            ),
            bicdb_extension::abi_v2::ResourceFilterKind::RelationContains {
                field,
                target_resource,
                target_field,
                ..
            } => (
                field,
                target_resource,
                target_field,
                FilterExpression::Contains {
                    field: target_field.clone(),
                    value: value
                        .as_str()
                        .ok_or_else(|| {
                            AppRuntimeError::InvalidRequest(format!(
                                "relation filter `{query_name}` requires a string"
                            ))
                        })?
                        .to_string(),
                },
            ),
            bicdb_extension::abi_v2::ResourceFilterKind::RelationMinimum {
                field,
                target_resource,
                target_field,
                value_type,
            } => (
                field,
                target_resource,
                target_field,
                FilterExpression::TypedGe {
                    field: target_field.clone(),
                    value: value.clone(),
                    value_type: value_type.clone(),
                },
            ),
            _ => {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "filter `{query_name}` is not a relation filter"
                )));
            }
        };
        let target = host
            .application_manifest()
            .resources
            .iter()
            .find(|resource| resource.name == *target_resource)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "relation filter target resource `{target_resource}` is absent"
                ))
            })?;
        if !target
            .fields
            .iter()
            .any(|field| field.name == *target_field)
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "relation filter target field `{target_resource}.{target_field}` is absent"
            )));
        }
        let mut ids = Vec::new();
        let mut offset = 0_u64;
        loop {
            let rows = expect_rows(call(
                host,
                HostRequest::Database(DatabaseRequest::Query {
                    transaction,
                    query: QuerySpec {
                        relation: target.relation.clone(),
                        filters: vec![predicate.clone()],
                        sort: Vec::new(),
                        columns: vec![target.primary_key.clone()],
                        limit: 10_000,
                        offset,
                        cursor: None,
                    },
                }),
            )?)?;
            let count = rows.len();
            for row in rows {
                ids.push(row.get(&target.primary_key).cloned().ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(format!(
                        "relation filter target `{target_resource}` omitted its primary key"
                    ))
                })?);
            }
            if count < 10_000 {
                break;
            }
            offset = offset.saturating_add(10_000);
            if offset > 10_000_000 {
                return Err(AppRuntimeError::ResourceExhausted(format!(
                    "relation filter `{query_name}` exceeds the host join bound"
                )));
            }
        }
        resolved.push(FilterExpression::In {
            field: source_field.clone(),
            values: ids,
        });
    }
    Ok(resolved)
}

fn apply_actor_scope(
    host: &CapabilityHost,
    contract: &ResourceContractV1,
    object: &mut Map<String, Value>,
) -> Result<()> {
    if let Some(field) = &contract.tenant_field {
        let trusted = host.actor().tenant_id.as_ref().unwrap();
        if object
            .get(field)
            .and_then(Value::as_str)
            .is_some_and(|claimed| claimed != trusted)
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "request attempted to forge tenant scope".to_string(),
            ));
        }
        object.insert(field.clone(), Value::String(trusted.clone()));
    }
    if let Some(field) = &contract.workspace_field {
        let trusted = host.actor().workspace_id.as_ref().unwrap();
        if object
            .get(field)
            .and_then(Value::as_str)
            .is_some_and(|claimed| claimed != trusted)
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "request attempted to forge workspace scope".to_string(),
            ));
        }
        object.insert(field.clone(), Value::String(trusted.clone()));
    }
    Ok(())
}

fn has_runtime_audit_contract(contract: &ResourceContractV1) -> Result<bool> {
    let expected = [
        ("created_by", FieldType::String),
        ("updated_by", FieldType::String),
        ("created_at", FieldType::Timestamp),
        ("updated_at", FieldType::Timestamp),
    ];
    if !expected
        .iter()
        .all(|(name, _)| contract.fields.iter().any(|field| field.name == *name))
    {
        return Ok(false);
    }
    for (name, field_type) in expected {
        let field = contract
            .fields
            .iter()
            .find(|field| field.name == name)
            .expect("all runtime audit fields were checked above");
        if field.field_type != field_type {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "runtime audit field `{}.{name}` must have type {:?}",
                contract.name, field_type
            )));
        }
    }
    Ok(true)
}

fn audit_actor_id(host: &CapabilityHost) -> Result<String> {
    host.actor()
        .user_id
        .as_ref()
        .or(host.actor().service_id.as_ref())
        .cloned()
        .ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(
                "an authenticated user or service actor is required for audited writes".to_string(),
            )
        })
}

fn apply_actor_audit_create(
    host: &CapabilityHost,
    contract: &ResourceContractV1,
    object: &mut Map<String, Value>,
) -> Result<()> {
    if !has_runtime_audit_contract(contract)? {
        return Ok(());
    }
    let actor_id = Value::String(audit_actor_id(host)?);
    object.insert("created_by".to_string(), actor_id.clone());
    object.insert("updated_by".to_string(), actor_id);
    Ok(())
}

fn apply_actor_audit_update(
    host: &CapabilityHost,
    contract: &ResourceContractV1,
    patch: &mut Map<String, Value>,
) -> Result<()> {
    if !has_runtime_audit_contract(contract)? {
        return Ok(());
    }
    patch.insert(
        "updated_by".to_string(),
        Value::String(audit_actor_id(host)?),
    );
    patch.insert(
        "updated_at".to_string(),
        Value::String(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
    );
    Ok(())
}

fn enforce_scope(
    host: &CapabilityHost,
    contract: &ResourceContractV1,
    record: &Value,
) -> Result<()> {
    for (field, trusted, label) in [
        (
            contract.tenant_field.as_ref(),
            host.actor().tenant_id.as_ref(),
            "tenant",
        ),
        (
            contract.workspace_field.as_ref(),
            host.actor().workspace_id.as_ref(),
            "workspace",
        ),
    ] {
        if let Some(field) = field {
            if record.get(field).and_then(Value::as_str) != trusted.map(String::as_str) {
                return Err(AppRuntimeError::CapabilityDenied(format!(
                    "cross-{label} resource access denied"
                )));
            }
        }
    }
    Ok(())
}

fn load_required(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    transaction: HostHandle,
    id: &str,
) -> Result<Value> {
    let value = expect_json(call(
        host,
        HostRequest::Database(DatabaseRequest::SelectPrimaryKey {
            transaction,
            relation: contract.relation.clone(),
            id: id.to_string(),
            columns: Vec::new(),
        }),
    )?)?;
    if value.is_null() {
        Err(AppRuntimeError::NotFound(format!(
            "resource `{}/{id}` was not found",
            contract.name
        )))
    } else {
        decrypt_resource_value(host, contract, value)
    }
}

fn take_body_expected_version(
    contract: &ResourceContractV1,
    patch: &mut Map<String, Value>,
) -> Result<Option<u64>> {
    let Some(field) = &contract.version_field else {
        return Ok(None);
    };
    let Some(value) = patch.remove(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value.as_u64().map(Some).ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!(
            "optimistic version field `{field}` must be a non-negative integer"
        ))
    })
}

fn expected_version(
    contract: &ResourceContractV1,
    request: &ResourceRequest,
    before: &Value,
    body_expected_version: Option<u64>,
) -> Result<Option<u64>> {
    let Some(field) = &contract.version_field else {
        return Ok(None);
    };
    let expected = match (request.expected_version, body_expected_version) {
        (Some(header), Some(body)) if header != body => {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "If-Match version {header} does not match body version {body}"
            )));
        }
        (Some(header), _) => Some(header),
        (None, body) => body,
    }
    .ok_or_else(|| {
        AppRuntimeError::Invocation(format!(
            "resource `{}` requires an optimistic version",
            contract.name
        ))
    })?;
    let actual = before.get(field).and_then(Value::as_u64).ok_or_else(|| {
        AppRuntimeError::Invocation(format!("stored version field `{field}` is invalid"))
    })?;
    if expected != actual {
        return Err(AppRuntimeError::OptimisticConflict(format!(
            "optimistic version conflict: expected {expected}, actual {actual}"
        )));
    }
    Ok(Some(expected))
}

/// Stamp the runtime-owned timestamp columns when the caller omitted them,
/// matching the Node and Rust runtimes' convention: `created_at` and
/// `updated_at` of timestamp type belong to the runtime, not the request body.
fn apply_timestamp_conventions(contract: &ResourceContractV1, object: &mut Map<String, Value>) {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    for field in &contract.fields {
        if !matches!(field.name.as_str(), "created_at" | "updated_at") {
            continue;
        }
        if !matches!(field.field_type, FieldType::Timestamp) {
            continue;
        }
        if object.get(&field.name).is_none_or(Value::is_null) {
            object.insert(field.name.clone(), Value::String(now.clone()));
        }
    }
}

fn apply_defaults(contract: &ResourceContractV1, object: &mut Map<String, Value>) -> Result<()> {
    for field in &contract.fields {
        if !object.contains_key(&field.name) {
            if let Some(default) = &field.default_json {
                object.insert(field.name.clone(), serde_json::from_str(default)?);
            }
        }
    }
    Ok(())
}

fn schema_expression_globals(object: &Map<String, Value>) -> BTreeMap<String, Value> {
    let mut globals = object
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    globals.insert("source".to_string(), Value::Object(object.clone()));
    globals.insert("subject".to_string(), Value::Object(object.clone()));
    globals
}

fn apply_generated_fields(
    host: &CapabilityHost,
    contract: &ResourceContractV1,
    object: &mut Map<String, Value>,
) -> Result<()> {
    let program = host.application_manifest().application_program.as_ref();
    for field in contract.fields.iter().filter(|field| field.generated) {
        let expression = field.generated_expression.as_ref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "generated field `{}.{}` has no expression",
                contract.name, field.name
            ))
        })?;
        let value =
            evaluate_carrier_expression(program, expression, schema_expression_globals(object))?;
        object.insert(field.name.clone(), value);
    }
    Ok(())
}

fn validate_checks(
    host: &CapabilityHost,
    contract: &ResourceContractV1,
    object: &Map<String, Value>,
) -> Result<()> {
    let program = host.application_manifest().application_program.as_ref();
    for check in &contract.checks {
        let value = evaluate_carrier_expression(
            program,
            &check.expression,
            schema_expression_globals(object),
        )?;
        match value.as_bool() {
            Some(true) => {}
            Some(false) => {
                return Err(AppRuntimeError::InvalidRequest(format!(
                    "resource `{}` violates check `{}`",
                    contract.name, check.name
                )));
            }
            None => {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "resource `{}` check `{}` did not evaluate to Bool",
                    contract.name, check.name
                )));
            }
        }
    }
    Ok(())
}

fn reject_unknown_fields(
    contract: &ResourceContractV1,
    object: &Map<String, Value>,
    allowed: &BTreeSet<String>,
    internal_model_call: bool,
) -> Result<()> {
    for field in object.keys() {
        let signed_internal_field = internal_model_call
            && contract
                .fields
                .iter()
                .any(|candidate| candidate.name == *field && !candidate.generated);
        if field != &contract.primary_key && !allowed.contains(field) && !signed_internal_field {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "field `{field}` is not writable"
            )));
        }
    }
    Ok(())
}

fn reject_unwritable_fields(
    host: &CapabilityHost,
    contract: &ResourceContractV1,
    request: &ResourceRequest,
    object: &Map<String, Value>,
    public_allowed: &BTreeSet<String>,
) -> Result<()> {
    if !request.internal_model_call {
        return reject_unknown_fields(contract, object, public_allowed, false);
    }

    let permission = host
        .application_manifest()
        .permission_for(&contract.relation)
        .ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(format!(
                "relation `{}` is undeclared",
                contract.relation
            ))
        })?;
    let signed_model_fields = contract
        .fields
        .iter()
        .filter(|field| !field.generated)
        .map(|field| field.name.clone())
        .collect::<BTreeSet<_>>();
    let writable_fields = if permission.writable_columns.is_empty() {
        // Legacy packages predate exact column authority. Their internal
        // helpers remain compatible, but only for non-generated fields in
        // the signed resource schema. The host mutation grant remains the
        // final authority.
        signed_model_fields
    } else {
        permission
            .writable_columns
            .intersection(&signed_model_fields)
            .cloned()
            .collect()
    };
    reject_unknown_fields(contract, object, &writable_fields, false)
}

pub(crate) fn scope_filters(
    contract: &ResourceContractV1,
    scope: ResourceRecordScope,
) -> Vec<FilterExpression> {
    let Some(field) = &contract.soft_delete_field else {
        return Vec::new();
    };
    match (scope, &contract.soft_delete_value) {
        (ResourceRecordScope::All, _) => Vec::new(),
        (ResourceRecordScope::Active, Some(value)) => vec![FilterExpression::Ne {
            field: field.clone(),
            value: value.clone(),
        }],
        (ResourceRecordScope::Deleted, Some(value)) => vec![FilterExpression::Eq {
            field: field.clone(),
            value: value.clone(),
        }],
        (ResourceRecordScope::Active, None) => vec![FilterExpression::Eq {
            field: field.clone(),
            value: Value::Null,
        }],
        (ResourceRecordScope::Deleted, None) => vec![FilterExpression::Ne {
            field: field.clone(),
            value: Value::Null,
        }],
    }
}

fn is_soft_deleted(contract: &ResourceContractV1, value: &Value) -> bool {
    let Some(field) = &contract.soft_delete_field else {
        return false;
    };
    match &contract.soft_delete_value {
        Some(deleted) => value.get(field) == Some(deleted),
        None => value.get(field).is_some_and(|value| !value.is_null()),
    }
}

fn visible_in_scope(
    contract: &ResourceContractV1,
    value: &Value,
    scope: ResourceRecordScope,
) -> bool {
    match scope {
        ResourceRecordScope::Active => !is_soft_deleted(contract, value),
        ResourceRecordScope::All => true,
        ResourceRecordScope::Deleted => {
            contract.soft_delete_field.is_none() || is_soft_deleted(contract, value)
        }
    }
}

fn filter_policy_rows(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    rows: Vec<Value>,
    transaction: HostHandle,
) -> Result<Vec<Value>> {
    let mut visible = Vec::with_capacity(rows.len());
    for row in rows {
        if read_policy_allows(host, contract, &row, Some(transaction), 0)? {
            visible.push(row);
        }
    }
    Ok(visible)
}

fn policy_auth_value(host: &CapabilityHost) -> Value {
    let actor = host.actor();
    let id = actor
        .user_id
        .as_deref()
        .and_then(|value| value.parse::<i64>().ok())
        .map(Value::from)
        .unwrap_or_else(|| {
            actor
                .user_id
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null)
        });
    json!({
        "id": id,
        "email": actor.policy_attributes.get("email").cloned().unwrap_or_default(),
        "name": actor.policy_attributes.get("name").cloned().unwrap_or_default(),
        "roles": actor.roles.iter().cloned().collect::<Vec<_>>(),
        "scopes": actor.scopes.iter().cloned().collect::<Vec<_>>(),
        "tenant_id": actor.tenant_id,
    })
}

fn resolve_policy_relation_expressions(
    host: &mut CapabilityHost,
    expression: &ApplicationExpressionV1,
    globals: &BTreeMap<String, Value>,
    transaction: Option<HostHandle>,
    depth: usize,
) -> Result<ApplicationExpressionV1> {
    if depth > 64 {
        return Err(AppRuntimeError::InvalidPackage(
            "resource policy relation evaluation exceeds maximum depth".to_string(),
        ));
    }
    if let ApplicationExpressionV1::Exists {
        binding,
        resource,
        condition,
    } = expression
    {
        let transaction = transaction.ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "resource policy EXISTS requires an active transaction".to_string(),
            )
        })?;
        let bound_contract = host
            .application_manifest()
            .resources
            .iter()
            .find(|candidate| candidate.name == *resource)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "resource policy EXISTS references absent resource `{resource}`"
                ))
            })?;
        let rows = host.policy_candidate_rows(transaction, &bound_contract.relation, &[])?;
        if rows.len() > 10_000_000 {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "resource policy EXISTS on `{resource}` exceeds 10000000 candidate rows"
            )));
        }
        for stored_row in rows {
            if !read_policy_allows(
                host,
                &bound_contract,
                &stored_row,
                Some(transaction),
                depth + 1,
            )? {
                continue;
            }
            let bound_row = decrypt_resource_value(host, &bound_contract, stored_row)?;
            let mut nested_globals = globals.clone();
            nested_globals.insert(binding.clone(), bound_row);
            let resolved = resolve_policy_relation_expressions(
                host,
                condition,
                &nested_globals,
                Some(transaction),
                depth + 1,
            )?;
            let matched = evaluate_carrier_expression(
                host.application_manifest().application_program.as_ref(),
                &resolved,
                nested_globals,
            )?
            .as_bool()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "resource policy EXISTS condition for `{resource}` did not return Bool"
                ))
            })?;
            if matched {
                return Ok(ApplicationExpressionV1::Literal {
                    value: Value::Bool(true),
                });
            }
        }
        return Ok(ApplicationExpressionV1::Literal {
            value: Value::Bool(false),
        });
    }

    let mut resolved = expression.clone();
    match &mut resolved {
        ApplicationExpressionV1::Unary { value, .. } => {
            **value =
                resolve_policy_relation_expressions(host, value, globals, transaction, depth + 1)?;
        }
        ApplicationExpressionV1::Binary { left, right, .. } => {
            **left =
                resolve_policy_relation_expressions(host, left, globals, transaction, depth + 1)?;
            **right =
                resolve_policy_relation_expressions(host, right, globals, transaction, depth + 1)?;
        }
        ApplicationExpressionV1::Array { items } => {
            for item in items {
                *item = resolve_policy_relation_expressions(
                    host,
                    item,
                    globals,
                    transaction,
                    depth + 1,
                )?;
            }
        }
        ApplicationExpressionV1::Call { arguments, .. } => {
            for argument in arguments {
                argument.value = resolve_policy_relation_expressions(
                    host,
                    &argument.value,
                    globals,
                    transaction,
                    depth + 1,
                )?;
            }
        }
        _ => {}
    }
    Ok(resolved)
}

fn evaluate_policy_expression(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    expression: &ApplicationExpressionV1,
    row: &Value,
    transaction: Option<HostHandle>,
    depth: usize,
) -> Result<bool> {
    let mut globals = row
        .as_object()
        .map(|object| {
            object
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_else(BTreeMap::new);
    globals.insert("auth".to_string(), policy_auth_value(host));
    let expression =
        resolve_policy_relation_expressions(host, expression, &globals, transaction, depth)?;
    evaluate_carrier_expression(
        host.application_manifest().application_program.as_ref(),
        &expression,
        globals,
    )?
    .as_bool()
    .ok_or_else(|| {
        AppRuntimeError::InvalidPackage(format!(
            "resource `{}` policy expression did not return Bool",
            contract.name
        ))
    })
}

fn policy_rule_allows(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    rule: &ResourcePolicyRuleV1,
    row: &Value,
    transaction: Option<HostHandle>,
    depth: usize,
) -> Result<bool> {
    let roles_allow = if rule.roles.is_empty() {
        true
    } else {
        match rule.role_match {
            ResourcePolicyRoleMatchV1::Any => !rule.roles.is_disjoint(&host.actor().roles),
            ResourcePolicyRoleMatchV1::All => rule.roles.is_subset(&host.actor().roles),
        }
    };
    if !roles_allow {
        return Ok(false);
    }
    match &rule.expression {
        Some(expression) => {
            evaluate_policy_expression(host, contract, expression, row, transaction, depth)
        }
        None => Ok(true),
    }
}

fn base_policy_allows(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    row: &Value,
    transaction: Option<HostHandle>,
    depth: usize,
) -> Result<bool> {
    let Some(policy) = &contract.policy else {
        return Ok(true);
    };
    match &policy.tenant_expression {
        Some(expression) => {
            evaluate_policy_expression(host, contract, expression, row, transaction, depth)
        }
        None => Ok(true),
    }
}

fn read_policy_allows(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    stored_row: &Value,
    transaction: Option<HostHandle>,
    depth: usize,
) -> Result<bool> {
    let Some(policy) = &contract.policy else {
        return Ok(true);
    };
    let row = decrypt_resource_value(host, contract, stored_row.clone())?;
    if !base_policy_allows(host, contract, &row, transaction, depth)? {
        return Ok(false);
    }
    let Some(read) = &policy.read else {
        return Ok(false);
    };
    if !policy_rule_allows(host, contract, read, &row, transaction, depth)? {
        return Ok(false);
    }
    if is_soft_deleted(contract, &row) {
        let Some(deleted) = &policy.deleted_read else {
            return Ok(false);
        };
        return policy_rule_allows(host, contract, deleted, &row, transaction, depth);
    }
    Ok(true)
}

fn enforce_write_policy(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    row: &Value,
    transaction: Option<HostHandle>,
) -> Result<()> {
    let Some(policy) = &contract.policy else {
        return Ok(());
    };
    let allowed = if base_policy_allows(host, contract, row, transaction, 0)? {
        match &policy.write {
            Some(rule) => policy_rule_allows(host, contract, rule, row, transaction, 0)?,
            None => false,
        }
    } else {
        false
    };
    if allowed {
        Ok(())
    } else {
        Err(AppRuntimeError::CapabilityDenied(format!(
            "actor is denied by resource `{}` write policy",
            contract.name
        )))
    }
}

pub(crate) fn validate_object(
    contract: &ResourceContractV1,
    object: &Map<String, Value>,
) -> Result<()> {
    for field in &contract.fields {
        let Some(value) = object.get(&field.name) else {
            if !field.nullable {
                return Err(AppRuntimeError::Invocation(format!(
                    "required field `{}` is absent",
                    field.name
                )));
            }
            continue;
        };
        if value.is_null() {
            if !field.nullable {
                return Err(AppRuntimeError::Invocation(format!(
                    "field `{}` cannot be null",
                    field.name
                )));
            }
            continue;
        }
        let valid = if field.value_type.is_some() {
            true
        } else {
            match field.field_type {
                FieldType::Bool => value.is_boolean(),
                FieldType::Int64 => value.as_i64().is_some() || value.as_u64().is_some(),
                FieldType::Float64 | FieldType::Decimal => value.is_number(),
                FieldType::String | FieldType::Timestamp | FieldType::Date => value.is_string(),
                FieldType::Bytes => value.is_string() || value.is_array(),
                FieldType::Uuid => value
                    .as_str()
                    .is_some_and(|value| Uuid::parse_str(value).is_ok()),
                FieldType::Json | FieldType::Geometry { .. } => true,
                FieldType::Vector { dimensions } => value.as_array().is_some_and(|values| {
                    values.len() == dimensions as usize && values.iter().all(Value::is_number)
                }),
            }
        };
        if !valid {
            return Err(AppRuntimeError::Invocation(format!(
                "field `{}` does not match {:?}",
                field.name, field.field_type
            )));
        }
    }
    for validation in &contract.validation {
        let Some(value) = object.get(&validation.field) else {
            continue;
        };
        // SQL CHECK semantics: a rule over NULL is not a violation. Whether
        // NULL is allowed at all is the field's nullability, judged above.
        if value.is_null() {
            continue;
        }
        let valid = match &validation.rule {
            ValidationRuleKind::MinLength { value: minimum } => {
                value.as_str().is_some_and(|value| value.len() >= *minimum)
            }
            ValidationRuleKind::MaxLength { value: maximum } => {
                value.as_str().is_some_and(|value| value.len() <= *maximum)
            }
            ValidationRuleKind::Minimum { value: minimum } => {
                numeric_validation_order(value, minimum)
                    .is_some_and(|order| order != std::cmp::Ordering::Less)
            }
            ValidationRuleKind::Maximum { value: maximum } => {
                numeric_validation_order(value, maximum)
                    .is_some_and(|order| order != std::cmp::Ordering::Greater)
            }
            ValidationRuleKind::Range { minimum, maximum } => {
                numeric_validation_order(value, minimum)
                    .is_some_and(|order| order != std::cmp::Ordering::Less)
                    && numeric_validation_order(value, maximum)
                        .is_some_and(|order| order != std::cmp::Ordering::Greater)
            }
            ValidationRuleKind::Email => value
                .as_str()
                .is_some_and(|value| EMAIL_VALIDATION_RE.is_match(value)),
            ValidationRuleKind::Pattern { expression } => value.as_str().is_some_and(|value| {
                regex::Regex::new(expression).is_ok_and(|pattern| pattern.is_match(value))
            }),
            ValidationRuleKind::OneOf { values } => values.contains(value),
        };
        if !valid {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "resource `{}` field `{}` violates its signed validation rule ({:?}); got {}",
                contract.name, validation.field, validation.rule, value,
            )));
        }
    }
    Ok(())
}

fn numeric_validation_order(value: &Value, boundary: &str) -> Option<std::cmp::Ordering> {
    if let Some(value) = value.as_str() {
        return value
            .parse::<Decimal>()
            .ok()?
            .partial_cmp(&boundary.parse::<Decimal>().ok()?);
    }
    value
        .as_f64()
        .filter(|value| value.is_finite())?
        .partial_cmp(
            &boundary
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite())?,
        )
}

pub(crate) fn normalize_object(
    contract: &ResourceContractV1,
    object: &mut Map<String, Value>,
) -> Result<()> {
    for field in &contract.fields {
        let (Some(value_type), Some(value)) = (&field.value_type, object.get_mut(&field.name))
        else {
            continue;
        };
        *value = normalize_carrier_json_value(
            value,
            value_type,
            &format!("resource.{}.{}", contract.name, field.name),
        )?;
    }
    Ok(())
}

fn apply_cache_headers(
    contract: &ResourceContractV1,
    operation: ResourceOperation,
    response: &mut ResourceResponse,
) {
    if !matches!(operation, ResourceOperation::List | ResourceOperation::Get) {
        response
            .headers
            .push(("cache-control".to_string(), "no-store".to_string()));
        return;
    }
    let Some(cache) = &contract.cache else {
        response
            .headers
            .push(("cache-control".to_string(), "no-store".to_string()));
        return;
    };
    let visibility = if cache.private { "private" } else { "public" };
    response.headers.push((
        "cache-control".to_string(),
        format!("{visibility}, max-age={}", cache.max_age_seconds),
    ));
    if !cache.vary.is_empty() {
        response.headers.push((
            "vary".to_string(),
            cache.vary.iter().cloned().collect::<Vec<_>>().join(", "),
        ));
    }
}

pub(crate) fn redact(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    value: Value,
) -> Result<Value> {
    let value = decrypt_resource_value(host, contract, value)?;
    redact_plain(host, contract, value)
}

fn redact_plain(
    host: &CapabilityHost,
    contract: &ResourceContractV1,
    value: Value,
) -> Result<Value> {
    let Value::Object(mut object) = value else {
        return Ok(value);
    };
    normalize_object(contract, &mut object)?;
    let declared = contract
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    object.retain(|field, _| declared.contains(field.as_str()));
    for field in &contract.redacted_fields {
        object.remove(field);
    }
    for (field, roles) in &contract.read_roles {
        if !roles.is_subset(&host.actor().roles) {
            object.remove(field);
        }
    }
    Ok(Value::Object(object))
}

pub(crate) fn encrypt_resource_value(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    value: &Value,
) -> Result<Value> {
    crypt_resource_value(host, contract, value, true)
}

pub(crate) fn decrypt_resource_value(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    value: Value,
) -> Result<Value> {
    crypt_resource_value(host, contract, &value, false)
}

fn crypt_resource_value(
    host: &mut CapabilityHost,
    contract: &ResourceContractV1,
    value: &Value,
    encrypt: bool,
) -> Result<Value> {
    if contract.encrypted_fields.is_empty() || value.is_null() {
        return Ok(value.clone());
    }
    let Value::Object(mut object) = value.clone() else {
        return Err(AppRuntimeError::Invocation(format!(
            "resource `{}` encrypted value is not an object",
            contract.name
        )));
    };
    for (field, secret_name) in &contract.encrypted_fields {
        let Some(value) = object.get(field).cloned() else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        let plaintext_or_ciphertext = value.as_str().ok_or_else(|| {
            AppRuntimeError::Invocation(format!(
                "resource `{}` encrypted field `{field}` must be a string",
                contract.name
            ))
        })?;
        let (secret_version, algorithm) = if !encrypt {
            if let Some(encoded) = plaintext_or_ciphertext.strip_prefix("enc:v2:") {
                let (encoded_version, _) = encoded.split_once(':').ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(
                        "BicDB application encrypted field v2 envelope is malformed".to_string(),
                    )
                })?;
                let version = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(encoded_version)
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
                    .filter(|version| !version.is_empty())
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "BicDB application encrypted field v2 key version is invalid"
                                .to_string(),
                        )
                    })?;
                (Some(version), "bicdb-aes-256-gcm-v2")
            } else {
                (None, "bicdb-aes-256-gcm-v1")
            }
        } else {
            (None, "bicdb-aes-256-gcm-v1")
        };
        let secret = expect_handle(call(
            host,
            HostRequest::Secret(SecretRequest::Open {
                name: secret_name.clone(),
                version: secret_version,
            }),
        )?)?;
        let request = if encrypt {
            CryptoRequest::Encrypt {
                secret,
                algorithm: algorithm.to_string(),
                plaintext: plaintext_or_ciphertext.as_bytes().to_vec(),
                associated_data: Vec::new(),
            }
        } else {
            CryptoRequest::Decrypt {
                secret,
                algorithm: algorithm.to_string(),
                ciphertext: plaintext_or_ciphertext.as_bytes().to_vec(),
                associated_data: Vec::new(),
            }
        };
        let bytes = match call(host, HostRequest::Crypto(request))? {
            HostValue::Bytes(bytes) => bytes,
            other => {
                return Err(AppRuntimeError::Invocation(format!(
                    "resource `{}` crypto host returned {other:?}, expected bytes",
                    contract.name
                )));
            }
        };
        let value = String::from_utf8(bytes).map_err(|_| {
            AppRuntimeError::Invocation(format!(
                "resource `{}` crypto host returned invalid UTF-8",
                contract.name
            ))
        })?;
        object.insert(field.clone(), Value::String(value));
    }
    Ok(Value::Object(object))
}

fn project_fields(value: &Value, include: &BTreeSet<String>, redact: &BTreeSet<String>) -> Value {
    let Some(object) = value.as_object() else {
        return value.clone();
    };
    Value::Object(
        object
            .iter()
            .filter(|(field, _)| {
                (include.is_empty() || include.contains(*field)) && !redact.contains(*field)
            })
            .map(|(field, value)| (field.clone(), value.clone()))
            .collect(),
    )
}

fn project_event_fields(value: &Value, include: &BTreeSet<String>) -> Value {
    let Some(object) = value.as_object() else {
        return value.clone();
    };
    Value::Object(
        object
            .iter()
            .filter(|(field, _)| include.contains(*field))
            .map(|(field, value)| (field.clone(), value.clone()))
            .collect(),
    )
}

fn etag(contract: &ResourceContractV1, value: &Value) -> Vec<(String, String)> {
    contract
        .version_field
        .as_ref()
        .and_then(|field| value.get(field))
        .and_then(Value::as_u64)
        .map(|version| vec![("etag".to_string(), format!("\"{version}\""))])
        .unwrap_or_default()
}

fn required_id(request: &ResourceRequest) -> Result<&str> {
    request.id.as_deref().ok_or_else(|| {
        AppRuntimeError::Invocation("resource item operation requires an id".to_string())
    })
}

fn object_body(request: &ResourceRequest) -> Result<Map<String, Value>> {
    request.body.as_object().cloned().ok_or_else(|| {
        AppRuntimeError::Invocation("resource body must be a JSON object".to_string())
    })
}

fn call(host: &mut CapabilityHost, request: HostRequest) -> Result<HostValue> {
    let result = host.call(HostCall {
        request_id: rand_request_id(),
        request,
    });
    if let Some(error) = result.error {
        let detail = format!(
            "{} ({:?}, trace {}): {}",
            error.code, error.class, error.trace_id, error.message
        );
        return Err(match error.class {
            ErrorClass::Unauthenticated => AppRuntimeError::Authentication(detail),
            ErrorClass::Unauthorized
            | ErrorClass::PolicyDenied
            | ErrorClass::MutationGrantDenied => AppRuntimeError::CapabilityDenied(detail),
            ErrorClass::NotFound => AppRuntimeError::NotFound(detail),
            ErrorClass::Conflict | ErrorClass::CommitValidation => {
                AppRuntimeError::Conflict(detail)
            }
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
        });
    }
    result.value.ok_or_else(|| {
        AppRuntimeError::Invocation("host returned neither a value nor an error".to_string())
    })
}

fn rand_request_id() -> u64 {
    let bytes = Uuid::new_v4().into_bytes();
    u64::from_le_bytes(bytes[..8].try_into().unwrap())
}

fn expect_handle(value: HostValue) -> Result<HostHandle> {
    match value {
        HostValue::Handle(handle) => Ok(handle),
        other => Err(AppRuntimeError::Invocation(format!(
            "host returned {other:?}, expected handle"
        ))),
    }
}

fn expect_json(value: HostValue) -> Result<Value> {
    match value {
        HostValue::Json(value) => Ok(value),
        other => Err(AppRuntimeError::Invocation(format!(
            "host returned {other:?}, expected JSON"
        ))),
    }
}

fn expect_rows(value: HostValue) -> Result<Vec<Value>> {
    match value {
        HostValue::Rows(rows) => Ok(rows),
        other => Err(AppRuntimeError::Invocation(format!(
            "host returned {other:?}, expected rows"
        ))),
    }
}

fn expect_u64(value: HostValue) -> Result<u64> {
    match value {
        HostValue::U64(value) => Ok(value),
        other => Err(AppRuntimeError::Invocation(format!(
            "host returned {other:?}, expected unsigned integer"
        ))),
    }
}

fn filter_field(filter: &FilterExpression) -> &str {
    match filter {
        FilterExpression::Eq { field, .. }
        | FilterExpression::Ne { field, .. }
        | FilterExpression::Lt { field, .. }
        | FilterExpression::Le { field, .. }
        | FilterExpression::Gt { field, .. }
        | FilterExpression::Ge { field, .. }
        | FilterExpression::In { field, .. }
        | FilterExpression::Contains { field, .. }
        | FilterExpression::TypedEq { field, .. }
        | FilterExpression::TypedGe { field, .. }
        | FilterExpression::JsonPathEq { field, .. }
        | FilterExpression::JsonPathGe { field, .. }
        | FilterExpression::JsonPathContains { field, .. }
        | FilterExpression::JsonPathExists { field, .. } => field,
    }
}

fn is_json_path_filter(filter: &FilterExpression) -> bool {
    matches!(
        filter,
        FilterExpression::JsonPathEq { .. }
            | FilterExpression::JsonPathGe { .. }
            | FilterExpression::JsonPathContains { .. }
            | FilterExpression::JsonPathExists { .. }
    )
}

fn matches_filter(row: &Value, filter: &FilterExpression) -> bool {
    let value = row.get(filter_field(filter));
    match filter {
        FilterExpression::Eq {
            value: expected, ..
        } if expected.is_null() => value.is_none_or(Value::is_null),
        FilterExpression::Ne {
            value: expected, ..
        } if expected.is_null() => value.is_some_and(|value| !value.is_null()),
        FilterExpression::Eq {
            value: expected, ..
        } => value == Some(expected),
        FilterExpression::Ne {
            value: expected, ..
        } => value != Some(expected),
        FilterExpression::In { values, .. } => value.is_some_and(|value| values.contains(value)),
        FilterExpression::Contains {
            value: expected, ..
        } => value
            .and_then(Value::as_str)
            .is_some_and(|value| value.to_lowercase().contains(&expected.to_lowercase())),
        FilterExpression::TypedEq {
            value: expected,
            value_type,
            ..
        } => value.is_some_and(|value| typed_json_equal(value, expected, value_type)),
        FilterExpression::TypedGe {
            value: expected,
            value_type,
            ..
        } => value
            .and_then(|value| typed_json_compare(value, expected, value_type))
            .is_some_and(|ordering| !ordering.is_lt()),
        FilterExpression::Lt {
            value: expected, ..
        } => compare(value, Some(expected)).is_lt(),
        FilterExpression::Le {
            value: expected, ..
        } => !compare(value, Some(expected)).is_gt(),
        FilterExpression::Gt {
            value: expected, ..
        } => compare(value, Some(expected)).is_gt(),
        FilterExpression::Ge {
            value: expected, ..
        } => !compare(value, Some(expected)).is_lt(),
        FilterExpression::JsonPathEq {
            path,
            value: expected,
            value_type,
            ..
        } => value
            .and_then(|value| json_path(value, path))
            .is_some_and(|value| match value_type {
                Some(value_type) => typed_json_equal(value, expected, value_type),
                None => value == expected,
            }),
        FilterExpression::JsonPathGe {
            path,
            value: expected,
            value_type,
            ..
        } => value
            .and_then(|value| json_path(value, path))
            .and_then(|value| typed_json_compare(value, expected, value_type))
            .is_some_and(|ordering| !ordering.is_lt()),
        FilterExpression::JsonPathContains {
            path,
            value: expected,
            array,
            ..
        } => value
            .and_then(|value| json_path(value, path))
            .is_some_and(|value| {
                if *array {
                    value.as_array().is_some_and(|values| {
                        values.iter().any(|value| value.as_str() == Some(expected))
                    })
                } else {
                    value.as_str().is_some_and(|value| {
                        value.to_lowercase().contains(&expected.to_lowercase())
                    })
                }
            }),
        FilterExpression::JsonPathExists { path, exists, .. } => {
            value.and_then(|value| json_path(value, path)).is_some() == *exists
        }
    }
}

fn json_path<'a>(mut value: &'a Value, path: &str) -> Option<&'a Value> {
    if path.starts_with('/') {
        return value.pointer(path);
    }
    for segment in path.trim_start_matches("$.").split('.') {
        if segment.is_empty() {
            continue;
        }
        value = value.get(segment)?;
    }
    Some(value)
}

fn typed_json_equal(
    left: &Value,
    right: &Value,
    value_type: &ApplicationRouteParameterTypeV1,
) -> bool {
    typed_json_compare(left, right, value_type).is_some_and(|ordering| ordering.is_eq())
}

fn typed_json_compare(
    left: &Value,
    right: &Value,
    value_type: &ApplicationRouteParameterTypeV1,
) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;

    match value_type {
        ApplicationRouteParameterTypeV1::Int => left.as_i64()?.partial_cmp(&right.as_i64()?),
        ApplicationRouteParameterTypeV1::Float => left.as_f64()?.partial_cmp(&right.as_f64()?),
        ApplicationRouteParameterTypeV1::Decimal => {
            let left = json_decimal(left)?;
            let right = json_decimal(right)?;
            Some(left.cmp(&right))
        }
        ApplicationRouteParameterTypeV1::Bool => left.as_bool()?.partial_cmp(&right.as_bool()?),
        ApplicationRouteParameterTypeV1::Timestamp => {
            let left = chrono::DateTime::parse_from_rfc3339(left.as_str()?).ok()?;
            let right = chrono::DateTime::parse_from_rfc3339(right.as_str()?).ok()?;
            Some(left.cmp(&right))
        }
        ApplicationRouteParameterTypeV1::Date => {
            let left = chrono::NaiveDate::parse_from_str(left.as_str()?, "%Y-%m-%d").ok()?;
            let right = chrono::NaiveDate::parse_from_str(right.as_str()?, "%Y-%m-%d").ok()?;
            Some(left.cmp(&right))
        }
        ApplicationRouteParameterTypeV1::Uuid => {
            let left = Uuid::parse_str(left.as_str()?).ok()?;
            let right = Uuid::parse_str(right.as_str()?).ok()?;
            Some(left.cmp(&right))
        }
        ApplicationRouteParameterTypeV1::String
        | ApplicationRouteParameterTypeV1::LocalDateTime
        | ApplicationRouteParameterTypeV1::TimeZone
        | ApplicationRouteParameterTypeV1::Enum { .. } => Some(left.as_str()?.cmp(right.as_str()?)),
        _ if left == right => Some(Ordering::Equal),
        _ => None,
    }
}

fn json_decimal(value: &Value) -> Option<rust_decimal::Decimal> {
    match value {
        Value::String(value) => value.parse().ok(),
        Value::Number(value) => value.to_string().parse().ok(),
        _ => None,
    }
}

fn compare(left: Option<&Value>, right: Option<&Value>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(Value::Number(left)), Some(Value::Number(right))) => left
            .as_f64()
            .partial_cmp(&right.as_f64())
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(Value::String(left)), Some(Value::String(right))) => left.cmp(right),
        (Some(Value::Bool(left)), Some(Value::Bool(right))) => left.cmp(right),
        (None | Some(Value::Null), None | Some(Value::Null)) => std::cmp::Ordering::Equal,
        (None | Some(Value::Null), _) => std::cmp::Ordering::Less,
        (_, None | Some(Value::Null)) => std::cmp::Ordering::Greater,
        (Some(left), Some(right)) => left.to_string().cmp(&right.to_string()),
    }
}

fn sort_rows(rows: &mut [Value], sort: &[SortField]) {
    rows.sort_by(|left, right| {
        for field in sort {
            let mut ordering = compare(left.get(&field.field), right.get(&field.field));
            if field.descending {
                ordering = ordering.reverse();
            }
            if !ordering.is_eq() {
                return ordering;
            }
        }
        std::cmp::Ordering::Equal
    });
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bicdb_core::{
        BicDb, CollectionPolicy, ConsumeOptions, IndexDefinition, IndexField, IndexKind,
        MutationPolicy, Record, SecurityContext,
    };
    use bicdb_extension::abi_v2::{
        ActorContext, ApplicationFeature, ApplicationManifestV2, ApplicationRouteParameterTypeV1,
        AuditContract, ContractField, CryptoOperation, DatabaseAction, EgressDeclaration,
        EgressResponse, IdempotencyContract, PackageMetadata, RelationPermission,
        ResourceCacheContract, ResourceCheckV1, ResourceEventContract, ResourceForeignKeyV1,
        RouteV2, SecretDeclaration, APPLICATION_COMPATIBILITY_PROFILE,
    };
    use bicdb_extension::{
        ExtensionCapability, ExtensionIdentity, ExtensionLimits, ExtensionManifest,
        ExtensionPermissions, HttpMethod,
    };

    use super::*;
    use crate::{
        EgressProvider, InMemorySecretProvider, InvocationServices, LocalBlobProvider,
        ObservabilityEvent,
    };

    struct DenyEgress;

    impl EgressProvider for DenyEgress {
        fn execute(
            &self,
            _plugin: &str,
            _policy: &EgressDeclaration,
            _mtls_secret: Option<&crate::SecretRecord>,
            _method: &str,
            _url: &str,
            _headers: &[(String, String)],
            _body: &[u8],
            _timeout_ms: u64,
        ) -> Result<EgressResponse> {
            Err(AppRuntimeError::CapabilityDenied(
                "test egress is disabled".to_string(),
            ))
        }
    }

    fn actor(tenant: &str, workspace: &str, roles: &[&str]) -> ActorContext {
        ActorContext {
            user_id: Some("user-1".to_string()),
            service_id: None,
            client_id: Some("carrier-test".to_string()),
            acting_client_id: None,
            authentication_method: Some("test-jwt".to_string()),
            roles: roles.iter().map(|value| (*value).to_string()).collect(),
            scopes: BTreeSet::from(["documents:write".to_string()]),
            tenant_id: Some(tenant.to_string()),
            workspace_id: Some(workspace.to_string()),
            organization_id: None,
            session_id: Some("session-1".to_string()),
            delegation_chain: vec![],
            assurance_level: Some("aal2".to_string()),
            request_origin: Some("test".to_string()),
            trace_id: Uuid::new_v4().to_string(),
            correlation_id: Some("correlation-1".to_string()),
            causation_id: None,
            deadline_unix_ms: crate::host::now_ms() + 60_000,
            policy_attributes: BTreeMap::new(),
        }
    }

    fn contract() -> ResourceContractV1 {
        let mut fields = [
            ("id", FieldType::String, false),
            ("title", FieldType::String, false),
            ("secret", FieldType::String, true),
            ("tenant_id", FieldType::String, false),
            ("workspace_id", FieldType::String, false),
            ("deleted_at", FieldType::Timestamp, true),
            ("version", FieldType::Int64, false),
            ("tenant_code", FieldType::String, true),
            ("region", FieldType::String, true),
            ("created_by", FieldType::String, false),
            ("updated_by", FieldType::String, true),
            ("created_at", FieldType::Timestamp, false),
            ("updated_at", FieldType::Timestamp, false),
        ]
        .into_iter()
        .map(|(name, field_type, nullable)| ContractField {
            name: name.to_string(),
            storage_name: None,
            field_type,
            value_type: None,
            nullable,
            generated: false,
            generated_expression: None,
            default_json: None,
        })
        .collect::<Vec<_>>();
        fields.push(ContractField {
            name: "slug".to_string(),
            storage_name: None,
            field_type: FieldType::String,
            value_type: None,
            nullable: false,
            generated: true,
            generated_expression: Some(
                serde_json::from_value(json!({
                    "op": "call",
                    "kind": "builtin",
                    "target": "string.slug",
                    "result_type": "string",
                    "argument_types": ["string"],
                    "arguments": [{"value": {"op": "variable", "name": "title"}}]
                }))
                .unwrap(),
            ),
            default_json: None,
        });
        ResourceContractV1 {
            version: 1,
            name: "documents".to_string(),
            relation: "documents".to_string(),
            schema_version: 1,
            schema_only: false,
            primary_key: "id".to_string(),
            fields,
            vector_search: None,
            timeseries: None,
            unique_targets: vec![],
            indexes: vec![],
            checks: vec![ResourceCheckV1 {
                name: "title_nonempty".to_string(),
                expression: serde_json::from_value(json!({
                    "op": "binary",
                    "operator": "not_equal",
                    "left": {"op": "variable", "name": "title"},
                    "right": {"op": "literal", "value": ""},
                    "value_type": "bool",
                    "left_type": "string",
                    "right_type": "string"
                }))
                .unwrap(),
            }],
            foreign_keys: vec![ResourceForeignKeyV1 {
                name: "document_tenant_scope".to_string(),
                fields: vec!["tenant_code".to_string(), "region".to_string()],
                target_resource: "TenantIdentity".to_string(),
                target_fields: vec!["tenant_code".to_string(), "region".to_string()],
            }],
            exclusions: vec![],
            reverse_relations: vec![],
            relations: vec![],
            create_fields: BTreeSet::from([
                "title".to_string(),
                "secret".to_string(),
                "tenant_code".to_string(),
                "region".to_string(),
            ]),
            update_fields: BTreeSet::from([
                "title".to_string(),
                "secret".to_string(),
                "tenant_code".to_string(),
                "region".to_string(),
            ]),
            required_create_fields: BTreeSet::from(["title".to_string()]),
            server_managed_fields: BTreeSet::from([
                "created_by".to_string(),
                "updated_by".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
            ]),
            validation: vec![],
            operations: BTreeSet::from([
                ResourceOperation::List,
                ResourceOperation::Get,
                ResourceOperation::Create,
                ResourceOperation::Upsert,
                ResourceOperation::Update,
                ResourceOperation::Delete,
                ResourceOperation::Restore,
            ]),
            filters: BTreeSet::from(["title".to_string()]),
            filter_contracts: Vec::new(),
            relation_filters: BTreeSet::new(),
            json_path_filters: BTreeSet::new(),
            search_fields: BTreeSet::from(["title".to_string()]),
            sort_fields: BTreeSet::from(["title".to_string()]),
            list_defaults: None,
            list_route: "/v1/documents".to_string(),
            item_route: "/v1/documents/{id}".to_string(),
            soft_delete_field: Some("deleted_at".to_string()),
            soft_delete_value: None,
            restore_value: None,
            version_field: Some("version".to_string()),
            tenant_field: Some("tenant_id".to_string()),
            workspace_field: Some("workspace_id".to_string()),
            policy: None,
            required_roles: BTreeSet::from(["editor".to_string()]),
            required_scopes: BTreeSet::from(["documents:write".to_string()]),
            policy_attributes: BTreeMap::new(),
            read_roles: BTreeMap::from([(
                "secret".to_string(),
                BTreeSet::from(["administrator".to_string()]),
            )]),
            redacted_fields: BTreeSet::new(),
            immutable_fields: BTreeSet::from([
                "tenant_id".to_string(),
                "workspace_id".to_string(),
                "created_by".to_string(),
                "created_at".to_string(),
            ]),
            encrypted_fields: BTreeMap::new(),
            privacy: None,
            idempotency: Some(IdempotencyContract {
                header: "idempotency-key".to_string(),
                ttl_seconds: 3_600,
                max_key_bytes: 128,
            }),
            cache: None,
            audit: AuditContract {
                required: true,
                include_fields: BTreeSet::from([
                    "id".to_string(),
                    "title".to_string(),
                    "secret".to_string(),
                ]),
                redact_fields: BTreeSet::from(["secret".to_string()]),
            },
            events: vec![
                ResourceEventContract {
                    operation: ResourceOperation::Create,
                    queue: "document_events".to_string(),
                    schema_version: 1,
                    before_fields: BTreeSet::new(),
                    after_fields: BTreeSet::from(["id".to_string(), "title".to_string()]),
                },
                ResourceEventContract {
                    operation: ResourceOperation::Delete,
                    queue: "document_events".to_string(),
                    schema_version: 1,
                    before_fields: BTreeSet::from(["id".to_string(), "title".to_string()]),
                    after_fields: BTreeSet::from(["id".to_string(), "deleted_at".to_string()]),
                },
            ],
            operation_metadata: vec![],
            contract_sha256: "c".repeat(64),
            openapi: json!({}),
        }
    }

    fn lifecycle_contract() -> ResourceContractV1 {
        let mut contract = contract();
        for (name, field_type, nullable) in [
            ("stage", FieldType::String, false),
            ("submitted_at", FieldType::Timestamp, true),
            ("submitted_by", FieldType::String, true),
            ("cancelled_at", FieldType::Timestamp, true),
            ("cancelled_by", FieldType::String, true),
            ("cancellation_reason", FieldType::String, true),
        ] {
            contract.fields.push(ContractField {
                name: name.to_string(),
                storage_name: None,
                field_type,
                value_type: None,
                nullable,
                generated: false,
                generated_expression: None,
                default_json: None,
            });
        }
        contract
            .update_fields
            .extend(["stage".to_string(), "cancellation_reason".to_string()]);
        contract.server_managed_fields.extend([
            "submitted_at".to_string(),
            "submitted_by".to_string(),
            "cancelled_at".to_string(),
            "cancelled_by".to_string(),
        ]);
        contract.openapi = json!({
            "x-bicdb-lifecycle": {
                "version": 1,
                "name": "DocumentFlow",
                "stage_field": "stage",
                "stages": [
                    {"name": "draft"},
                    {"name": "submitted", "stamp_field": "submitted_at"},
                    {"name": "cancelled", "stamp_field": "cancelled_at", "terminal": true}
                ],
                "transitions": [
                    {"from": "draft", "to": "submitted"},
                    {"from": "submitted", "to": "cancelled", "reason_required": true}
                ]
            }
        });
        contract
    }

    fn tenant_identity_contract() -> ResourceContractV1 {
        serde_json::from_value(json!({
            "version": 1,
            "name": "TenantIdentity",
            "relation": "tenant_identities",
            "schema_version": 1,
            "schema_only": true,
            "primary_key": "id",
            "fields": [
                {"name": "id", "field_type": "string"},
                {"name": "tenant_code", "field_type": "string"},
                {"name": "region", "field_type": "string"}
            ],
            "unique_targets": [{
                "target": "tenant_region",
                "fields": ["tenant_code", "region"]
            }],
            "operations": [],
            "list_route": "/__schema/tenant-identities",
            "item_route": "/__schema/tenant-identities/{id}",
            "contract_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        }))
        .unwrap()
    }

    fn manifest(contract: ResourceContractV1) -> Arc<ExtensionManifest> {
        manifest_with_tenant_identity(contract, tenant_identity_contract())
    }

    fn manifest_with_tenant_identity(
        contract: ResourceContractV1,
        tenant_identity: ResourceContractV1,
    ) -> Arc<ExtensionManifest> {
        let encrypted_fields = contract.encrypted_fields.clone();
        let resource_routes = [
            (
                "documents_list",
                HttpMethod::Get,
                contract.list_route.clone(),
                ResourceOperation::List,
            ),
            (
                "documents_create",
                HttpMethod::Post,
                contract.list_route.clone(),
                ResourceOperation::Create,
            ),
            (
                "documents_update",
                HttpMethod::Patch,
                contract.item_route.clone(),
                ResourceOperation::Update,
            ),
        ]
        .into_iter()
        .map(|(name, method, template, operation)| RouteV2 {
            name: name.to_string(),
            method,
            template,
            export: "carrier_resource_api".to_string(),
            resource: Some(contract.name.clone()),
            operation: Some(operation),
            service_call: None,
            application_request: None,
            application_response: None,
            idempotency: None,
            cache: None,
            telemetry: None,
            response_headers: BTreeMap::new(),
            public: false,
            auth_scheme: None,
            roles: contract.required_roles.clone(),
            scopes: contract.required_scopes.clone(),
            roles_any: false,
            scopes_any: false,
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 4 * 1024 * 1024,
            streaming_request: false,
            streaming_response: false,
            sse: false,
            websocket: false,
        })
        .collect();
        let secrets = encrypted_fields
            .values()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|name| SecretDeclaration {
                name,
                operations: BTreeSet::from([CryptoOperation::Encrypt, CryptoOperation::Decrypt]),
                versions: BTreeSet::new(),
                allow_plaintext_read: false,
            })
            .collect::<Vec<_>>();
        let application_program = (!encrypted_fields.is_empty()).then(|| {
            serde_json::from_value(json!({
                "version": 1,
                "secret_bindings": encrypted_fields
                    .iter()
                    .map(|(field, secret)| (format!("documents.{field}"), secret.clone()))
                    .collect::<BTreeMap<_, _>>()
            }))
            .unwrap()
        });
        let mut capabilities = BTreeSet::from([
            ExtensionCapability::Database,
            ExtensionCapability::Transactions,
            ExtensionCapability::QueueEvents,
            ExtensionCapability::Observability,
        ]);
        let mut required_features = BTreeSet::from([ApplicationFeature::Observability]);
        if !encrypted_fields.is_empty() {
            capabilities.insert(ExtensionCapability::SecretsCrypto);
            required_features.extend([ApplicationFeature::Secrets, ApplicationFeature::Crypto]);
        }
        Arc::new(ExtensionManifest {
            identity: ExtensionIdentity {
                name: "carrier-resource-api".to_string(),
                version: "1.0.0".to_string(),
                abi_version: 2,
                description: "vertical resource test".to_string(),
            },
            dependencies: vec![],
            capabilities,
            permissions: ExtensionPermissions {
                read_relations: BTreeSet::from([
                    "documents".to_string(),
                    "tenant_identities".to_string(),
                ]),
                write_relations: BTreeSet::from(["documents".to_string()]),
                publish_queues: BTreeSet::from(["document_events".to_string()]),
                ..ExtensionPermissions::default()
            },
            limits: ExtensionLimits::default(),
            functions: vec![],
            indexes: vec![],
            storage: vec![],
            routes: vec![],
            subscriptions: vec![],
            observability: vec![],
            application: Some(Box::new(ApplicationManifestV2 {
                abi_version: 2,
                application_profile: APPLICATION_COMPATIBILITY_PROFILE.to_string(),
                package: PackageMetadata {
                    application: "carrier-resource-api".to_string(),
                    version: "1.0.0".to_string(),
                    package_sha256: "a".repeat(64),
                    dependency_lock_sha256: "a".repeat(64),
                    sbom_sha256: "a".repeat(64),
                    provenance_sha256: "a".repeat(64),
                    signature_key_id: "test".to_string(),
                    signature_algorithm: "ed25519".to_string(),
                    signature: "signed-package-value".to_string(),
                },
                relation_permissions: vec![
                    RelationPermission {
                        relation: "documents".to_string(),
                        actions: BTreeSet::from([
                            DatabaseAction::Select,
                            DatabaseAction::Insert,
                            DatabaseAction::Upsert,
                            DatabaseAction::Update,
                            DatabaseAction::Delete,
                            DatabaseAction::Aggregate,
                            DatabaseAction::FullTextSearch,
                        ]),
                        readable_columns: BTreeSet::new(),
                        writable_columns: BTreeSet::new(),
                    },
                    RelationPermission {
                        relation: "tenant_identities".to_string(),
                        actions: BTreeSet::from([DatabaseAction::Select]),
                        readable_columns: BTreeSet::new(),
                        writable_columns: BTreeSet::new(),
                    },
                ],
                raw_sql: vec![],
                service_imports: vec![],
                service_exports: vec![],
                secrets,
                egress: vec![],
                blobs: vec![],
                routes: resource_routes,
                response_headers: BTreeMap::new(),
                auth_schemes: BTreeMap::new(),
                realtime: vec![],
                resources: vec![contract, tenant_identity],
                invariants: vec![],
                migrations: vec![],
                workers: vec![],
                schedules: vec![],
                application_program,
                required_features,
                max_call_depth: 16,
            })),
        })
    }

    fn services(directory: &tempfile::TempDir) -> InvocationServices {
        let secrets = InMemorySecretProvider::default();
        secrets
            .insert(
                "DOCUMENT_SECRET_KEY",
                "1",
                "document-secret-key",
                "aes-256-gcm",
                vec![7; 32],
                true,
            )
            .unwrap();
        InvocationServices::new(
            Arc::new(secrets),
            Arc::new(DenyEgress),
            Arc::new(LocalBlobProvider::open(directory.path().join("blobs"), vec![9; 32]).unwrap()),
            Arc::new(Mutex::<Vec<ObservabilityEvent>>::default()),
        )
    }

    fn request(operation: ResourceOperation) -> ResourceRequest {
        ResourceRequest {
            operation,
            id: None,
            body: Value::Null,
            filters: vec![],
            relation_filters: BTreeMap::new(),
            sort: vec![],
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

    #[test]
    fn search_filters_treat_missing_nullable_fields_as_postgresql_null() {
        let missing = json!({"id": "sparse-row"});
        let explicit_null = json!({"id": "null-row", "deleted_at": null});
        let deleted = json!({"id": "deleted-row", "deleted_at": "2026-08-28T20:00:00Z"});
        let active = FilterExpression::Eq {
            field: "deleted_at".to_string(),
            value: Value::Null,
        };
        let deleted_scope = FilterExpression::Ne {
            field: "deleted_at".to_string(),
            value: Value::Null,
        };

        assert!(matches_filter(&missing, &active));
        assert!(matches_filter(&explicit_null, &active));
        assert!(!matches_filter(&deleted, &active));
        assert!(!matches_filter(&missing, &deleted_scope));
        assert!(!matches_filter(&explicit_null, &deleted_scope));
        assert!(matches_filter(&deleted, &deleted_scope));
    }

    #[test]
    fn lifecycle_transitions_stamp_journal_context_and_lock_submitted_documents() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("lifecycle-db")).unwrap();
        let contract = lifecycle_contract();
        let extension = manifest(contract.clone());
        let host = CapabilityHost::new(
            &mut db,
            extension,
            actor("tenant-a", "workspace-a", &["editor"]),
            services(&directory),
        )
        .unwrap();
        let draft = json!({
            "id": "doc-1",
            "tenant_id": "tenant-a",
            "workspace_id": "workspace-a",
            "stage": "draft"
        });
        let mut submit =
            Map::from_iter([("stage".to_string(), Value::String("submitted".to_string()))]);
        let prepared = prepare_lifecycle_transition(&host, &contract, &draft, &mut submit, None)
            .unwrap()
            .expect("transition prepared");
        assert_eq!(prepared.prior_stage, "draft");
        assert_eq!(prepared.new_stage, "submitted");
        assert_eq!(prepared.organization_id, Some(json!("tenant-a")));
        assert!(submit["submitted_at"].as_str().is_some());
        assert_eq!(submit["submitted_by"], "user-1");

        let submitted = json!({
            "id": "doc-1",
            "tenant_id": "tenant-a",
            "workspace_id": "workspace-a",
            "stage": "submitted"
        });
        let mut ordinary_edit =
            Map::from_iter([("title".to_string(), Value::String("forbidden".to_string()))]);
        assert!(prepare_lifecycle_transition(
            &host,
            &contract,
            &submitted,
            &mut ordinary_edit,
            None,
        )
        .unwrap_err()
        .to_string()
        .contains("immutable"));

        let mut cancel =
            Map::from_iter([("stage".to_string(), Value::String("cancelled".to_string()))]);
        assert!(
            prepare_lifecycle_transition(&host, &contract, &submitted, &mut cancel, None,)
                .unwrap_err()
                .to_string()
                .contains("requires a reason")
        );
        let prepared = prepare_lifecycle_transition(
            &host,
            &contract,
            &submitted,
            &mut cancel,
            Some(json!("Customer request")),
        )
        .unwrap()
        .expect("cancellation prepared");
        assert_eq!(prepared.reason.as_deref(), Some("Customer request"));
        assert!(cancel["cancelled_at"].as_str().is_some());
        assert_eq!(cancel["cancelled_by"], "user-1");
        assert_eq!(cancel["cancellation_reason"], "Customer request");

        let mut reverse =
            Map::from_iter([("stage".to_string(), Value::String("draft".to_string()))]);
        assert!(
            prepare_lifecycle_transition(&host, &contract, &submitted, &mut reverse, None,)
                .unwrap_err()
                .to_string()
                .contains("not allowed")
        );
    }

    #[test]
    fn internal_model_calls_can_filter_signed_fields_without_expanding_public_filters() {
        let contract = contract();
        let mut public = request(ResourceOperation::List);
        public.filters.push(FilterExpression::Eq {
            field: "secret".to_string(),
            value: Value::String("classified".to_string()),
        });
        assert!(validate_request(&contract, &public)
            .unwrap_err()
            .to_string()
            .contains("filter `secret` is undeclared"));

        public.internal_model_call = true;
        validate_request(&contract, &public).unwrap();
        assert!(!contract.filters.contains("secret"));
    }

    #[test]
    fn internal_model_calls_can_write_signed_non_public_fields_only() {
        let contract = contract();
        let lifecycle_body = json!({"created_by": "signed-program-actor"})
            .as_object()
            .cloned()
            .unwrap();
        let generated_body = json!({"slug": "must-remain-runtime-owned"})
            .as_object()
            .cloned()
            .unwrap();

        assert!(
            reject_unknown_fields(&contract, &lifecycle_body, &contract.create_fields, false,)
                .unwrap_err()
                .to_string()
                .contains("field `created_by` is not writable")
        );
        reject_unknown_fields(&contract, &lifecycle_body, &contract.create_fields, true).unwrap();
        assert!(
            reject_unknown_fields(&contract, &generated_body, &contract.create_fields, true,)
                .unwrap_err()
                .to_string()
                .contains("field `slug` is not writable")
        );
    }

    #[test]
    fn internal_model_writes_use_signed_relation_fields_without_expanding_public_crud() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("internal-write-db")).unwrap();
        let contract = lifecycle_contract();
        assert!(!contract.create_fields.contains("stage"));

        let mut extension = manifest(contract.clone());
        let manifest = Arc::get_mut(&mut extension).expect("unshared test manifest");
        let application = manifest.application.as_mut().expect("application manifest");
        let permission = application
            .relation_permissions
            .iter_mut()
            .find(|permission| permission.relation == contract.relation)
            .expect("documents permission");
        permission.writable_columns = contract
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect();

        let host = CapabilityHost::new(
            &mut db,
            extension,
            actor("tenant-a", "workspace-a", &["editor"]),
            services(&directory),
        )
        .unwrap();
        let body = Map::from_iter([("stage".to_string(), json!("draft"))]);

        let public = request(ResourceOperation::Create);
        assert!(reject_unwritable_fields(
            &host,
            &contract,
            &public,
            &body,
            &contract.create_fields,
        )
        .unwrap_err()
        .to_string()
        .contains("stage"));

        let mut internal = public;
        internal.internal_model_call = true;
        reject_unwritable_fields(&host, &contract, &internal, &body, &contract.create_fields)
            .unwrap();

        let unsigned = Map::from_iter([("forged_field".to_string(), json!(true))]);
        assert!(reject_unwritable_fields(
            &host,
            &contract,
            &internal,
            &unsigned,
            &contract.create_fields,
        )
        .unwrap_err()
        .to_string()
        .contains("forged_field"));
    }

    #[test]
    fn exact_resource_fields_normalize_decimal_sets_temporals_and_builtin_objects() {
        let mut contract = contract();
        contract.fields.extend([
            ContractField {
                name: "amount".to_string(),
                storage_name: None,
                field_type: FieldType::Decimal,
                value_type: Some(ApplicationRouteParameterTypeV1::Decimal),
                nullable: false,
                generated: false,
                generated_expression: None,
                default_json: None,
            },
            ContractField {
                name: "labels".to_string(),
                storage_name: None,
                field_type: FieldType::Json,
                value_type: Some(ApplicationRouteParameterTypeV1::Set {
                    element: Box::new(ApplicationRouteParameterTypeV1::String),
                }),
                nullable: false,
                generated: false,
                generated_expression: None,
                default_json: None,
            },
            ContractField {
                name: "zone".to_string(),
                storage_name: None,
                field_type: FieldType::String,
                value_type: Some(ApplicationRouteParameterTypeV1::TimeZone),
                nullable: false,
                generated: false,
                generated_expression: None,
                default_json: None,
            },
            ContractField {
                name: "money".to_string(),
                storage_name: None,
                field_type: FieldType::Json,
                value_type: Some(ApplicationRouteParameterTypeV1::Object {
                    fields: vec![
                        bicdb_extension::abi_v2::ApplicationRouteParameterV1 {
                            name: "amount".to_string(),
                            value_type: ApplicationRouteParameterTypeV1::Decimal,
                            optional: false,
                            default_json: None,
                            validations: vec![],
                        },
                        bicdb_extension::abi_v2::ApplicationRouteParameterV1 {
                            name: "currency".to_string(),
                            value_type: ApplicationRouteParameterTypeV1::String,
                            optional: false,
                            default_json: None,
                            validations: vec![],
                        },
                    ],
                }),
                nullable: false,
                generated: false,
                generated_expression: None,
                default_json: None,
            },
        ]);
        let mut object = json!({
            "id": "doc-rich",
            "title": "Rich",
            "tenant_id": "tenant-a",
            "workspace_id": "workspace-a",
            "version": 1,
            "slug": "rich",
            "created_by": "user-a",
            "updated_by": "user-a",
            "created_at": "2026-08-30T00:00:00.000Z",
            "updated_at": "2026-08-30T00:00:00.000Z",
            "amount": 1.2300,
            "labels": ["alpha", "alpha", "beta"],
            "zone": "US/Pacific",
            "money": {"amount": 10.500, "currency": "USD", "ignored": true}
        })
        .as_object()
        .unwrap()
        .clone();

        normalize_object(&contract, &mut object).unwrap();
        validate_object(&contract, &object).unwrap();
        assert_eq!(object["amount"], json!("1.23"));
        assert_eq!(object["labels"], json!(["alpha", "beta"]));
        assert_eq!(object["zone"], json!("US/Pacific"));
        assert_eq!(object["money"], json!({"amount":"10.5", "currency":"USD"}));

        object.insert("zone".to_string(), json!("Mars/Olympus"));
        assert!(normalize_object(&contract, &mut object).is_err());
    }

    #[test]
    fn vertical_resource_crud_enforces_scope_redaction_idempotency_and_outbox() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("db")).unwrap();
        for collection in [
            "documents",
            "tenant_identities",
            "__bicdb_app_idempotency",
            "__bicdb_app_audit",
        ] {
            db.create_collection(collection).unwrap();
        }
        db.insert(
            "tenant_identities",
            Record::new("tenant-a").with_metadata(json!({
                "id": "tenant-a",
                "tenant_code": "acme",
                "region": "us-west"
            })),
        )
        .unwrap();
        db.set_collection_policy(
            "documents",
            CollectionPolicy::tenant_field("tenant_id")
                .with_read_roles(["editor", "administrator"])
                .with_write_roles(["editor"])
                .with_delete_roles(["editor"]),
        )
        .unwrap();
        db.set_mutation_policy(
            "documents",
            MutationPolicy::grants_required()
                .with_tenant_field("tenant_id")
                .with_workspace_field("workspace_id")
                .with_version_field("version")
                .with_immutable_fields(["tenant_id", "workspace_id"])
                .with_audit(),
        )
        .unwrap();
        db.create_index(IndexDefinition {
            name: "documents_search".to_string(),
            collection: "documents".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["title".to_string()])],
            unique: false,
            kind: IndexKind::FullText,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
        db.set_mutation_policy("__bicdb_app_idempotency", MutationPolicy::grants_required())
            .unwrap();
        db.set_mutation_policy(
            "__bicdb_app_audit",
            MutationPolicy::grants_required().append_only(),
        )
        .unwrap();

        let mut contract = contract();
        contract.cache = Some(ResourceCacheContract {
            max_age_seconds: 30,
            private: true,
            vary: BTreeSet::from(["accept-language".to_string(), "authorization".to_string()]),
        });
        let extension = manifest(contract.clone());
        let invocation_services = services(&directory);
        let mut host = CapabilityHost::new(
            &mut db,
            extension.clone(),
            actor("tenant-a", "workspace-a", &["editor"]),
            invocation_services.clone(),
        )
        .unwrap();
        let mut create = request(ResourceOperation::Create);
        create.body = json!({
            "id": "doc-1",
            "title": "First",
            "secret": "classified"
        });
        create.idempotency_key = Some("create-doc-1".to_string());
        let created = execute_resource_operation(&mut host, &contract, create.clone()).unwrap();
        assert_eq!(created.status, 201);
        assert_eq!(created.body["tenant_id"], "tenant-a");
        assert_eq!(created.body["workspace_id"], "workspace-a");
        assert!(created.body.get("secret").is_none());
        assert_eq!(created.body["version"], 1);
        assert_eq!(created.body["slug"], "first");
        assert_eq!(created.body["created_by"], "user-1");
        assert_eq!(created.body["updated_by"], "user-1");
        assert!(created.body["created_at"].as_str().is_some());
        assert!(created.body["updated_at"].as_str().is_some());
        assert!(created
            .headers
            .iter()
            .any(|(name, value)| name == "cache-control" && value == "no-store"));

        let mut list = request(ResourceOperation::List);
        list.filters.push(FilterExpression::Eq {
            field: "title".to_string(),
            value: Value::String("First".to_string()),
        });
        list.sort.push(SortField {
            field: "title".to_string(),
            descending: true,
        });
        list.limit = 1;
        list.include_total = true;
        let listed = execute_resource_operation(&mut host, &contract, list).unwrap();
        assert_eq!(listed.body["items"].as_array().unwrap().len(), 1);
        assert_eq!(listed.body["page_info"]["total"], 1);
        assert!(listed
            .headers
            .iter()
            .any(|(name, value)| name == "x-bicdb-result-total" && value == "1"));
        assert!(listed
            .headers
            .iter()
            .any(|(name, value)| name == "cache-control" && value == "private, max-age=30"));
        assert!(listed
            .headers
            .iter()
            .any(|(name, value)| { name == "vary" && value == "accept-language, authorization" }));
        let mut search = request(ResourceOperation::List);
        search.search = Some("first".to_string());
        search.include_total = true;
        let searched = execute_resource_operation(&mut host, &contract, search).unwrap();
        assert_eq!(searched.body["items"].as_array().unwrap().len(), 1);
        assert_eq!(searched.body["page_info"]["total"], 1);
        assert!(searched
            .headers
            .iter()
            .any(|(name, value)| name == "x-bicdb-result-total" && value == "1"));

        let openapi = crate::generate_openapi(&extension).unwrap();
        assert_eq!(openapi["openapi"], "3.1.0");
        assert_eq!(
            openapi["components"]["schemas"]["documents"]["properties"]["secret"]
                ["x-bicdb-required-roles"][0],
            "administrator"
        );
        assert_eq!(
            openapi["paths"]["/v1/documents"]["post"]["parameters"][0]["name"],
            "idempotency-key"
        );
        assert_eq!(
            openapi["paths"]["/v1/documents"]["get"]["responses"]["200"]["headers"]
                ["Cache-Control"]["example"],
            "private, max-age=30"
        );
        assert_eq!(
            openapi["paths"]["/v1/documents/{id}"]["patch"]["requestBody"]["content"]
                ["application/json"]["schema"]["properties"]["version"]["type"],
            "integer"
        );
        assert_eq!(
            openapi["paths"]["/v1/documents/{id}"]["patch"]["parameters"][1]["required"],
            false
        );

        let replayed = execute_resource_operation(&mut host, &contract, create).unwrap();
        assert_eq!(replayed.status, 201);
        assert!(replayed
            .headers
            .iter()
            .any(|(name, value)| name == "idempotency-replayed" && value == "true"));

        let mut reused = request(ResourceOperation::Create);
        reused.body = json!({"id": "doc-2", "title": "Different"});
        reused.idempotency_key = Some("create-doc-1".to_string());
        assert!(matches!(
            execute_resource_operation(&mut host, &contract, reused),
            Err(AppRuntimeError::IdempotencyKeyReused(_))
        ));

        let mut forged_audit = request(ResourceOperation::Create);
        forged_audit.body = json!({
            "id": "doc-forged-audit",
            "title": "Forged audit",
            "created_by": "attacker"
        });
        forged_audit.idempotency_key = Some("create-doc-forged-audit".to_string());
        assert!(matches!(
            execute_resource_operation(&mut host, &contract, forged_audit),
            Err(AppRuntimeError::CapabilityDenied(message))
                if message.contains("created_by") && message.contains("not writable")
        ));

        let mut invalid = request(ResourceOperation::Create);
        invalid.body = json!({"id": "doc-invalid", "title": ""});
        invalid.idempotency_key = Some("create-doc-invalid".to_string());
        assert!(execute_resource_operation(&mut host, &contract, invalid)
            .unwrap_err()
            .to_string()
            .contains("title_nonempty"));

        let mut invalid_reference = request(ResourceOperation::Create);
        invalid_reference.body = json!({
            "id": "doc-invalid-fk",
            "title": "Invalid FK",
            "tenant_code": "acme",
            "region": "missing"
        });
        invalid_reference.idempotency_key = Some("create-doc-invalid-fk".to_string());
        assert!(
            execute_resource_operation(&mut host, &contract, invalid_reference)
                .unwrap_err()
                .to_string()
                .contains("document_tenant_scope")
        );

        let mut valid_reference = request(ResourceOperation::Create);
        valid_reference.body = json!({
            "id": "doc-valid-fk",
            "title": "Valid FK",
            "tenant_code": "acme",
            "region": "us-west"
        });
        valid_reference.idempotency_key = Some("create-doc-valid-fk".to_string());
        assert_eq!(
            execute_resource_operation(&mut host, &contract, valid_reference)
                .unwrap()
                .status,
            201
        );

        let mut upsert = request(ResourceOperation::Upsert);
        upsert.id = Some("doc-1".to_string());
        upsert.body = json!({"title": "Upserted"});
        upsert.expected_version = Some(1);
        let upserted = execute_resource_operation(&mut host, &contract, upsert).unwrap();
        assert_eq!(upserted.body["title"], "Upserted");
        assert_eq!(upserted.body["slug"], "upserted");
        assert_eq!(upserted.body["version"], 2);

        let mut stale = request(ResourceOperation::Update);
        stale.id = Some("doc-1".to_string());
        stale.body = json!({"title": "Stale", "version": 1});
        assert!(execute_resource_operation(&mut host, &contract, stale)
            .unwrap_err()
            .to_string()
            .contains("version conflict"));

        let mut update = request(ResourceOperation::Update);
        update.id = Some("doc-1".to_string());
        update.body = json!({"title": "Updated", "version": 2});
        let updated = execute_resource_operation(&mut host, &contract, update).unwrap();
        assert_eq!(updated.body["title"], "Updated");
        assert_eq!(updated.body["slug"], "updated");
        assert_eq!(updated.body["version"], 3);
        assert_eq!(updated.body["created_by"], "user-1");
        assert_eq!(updated.body["updated_by"], "user-1");

        let mut mismatched_version = request(ResourceOperation::Update);
        mismatched_version.id = Some("doc-1".to_string());
        mismatched_version.body = json!({"title": "Mismatch", "version": 2});
        mismatched_version.expected_version = Some(3);
        assert!(
            execute_resource_operation(&mut host, &contract, mismatched_version)
                .unwrap_err()
                .to_string()
                .contains("does not match body version")
        );

        let mut delete = request(ResourceOperation::Delete);
        delete.id = Some("doc-1".to_string());
        delete.expected_version = Some(3);
        assert_eq!(
            execute_resource_operation(&mut host, &contract, delete)
                .unwrap()
                .status,
            204
        );
        let mut get = request(ResourceOperation::Get);
        get.id = Some("doc-1".to_string());
        assert!(execute_resource_operation(&mut host, &contract, get)
            .unwrap_err()
            .to_string()
            .contains("not found"));

        let mut restore = request(ResourceOperation::Restore);
        restore.id = Some("doc-1".to_string());
        restore.expected_version = Some(4);
        let restored = execute_resource_operation(&mut host, &contract, restore).unwrap();
        assert!(restored.body["deleted_at"].is_null());
        assert_eq!(restored.body["version"], 5);
        drop(host);

        let mut other_workspace = CapabilityHost::new(
            &mut db,
            extension,
            actor("tenant-a", "workspace-b", &["editor"]),
            invocation_services,
        )
        .unwrap();
        let mut cross_workspace = request(ResourceOperation::Update);
        cross_workspace.id = Some("doc-1".to_string());
        cross_workspace.body = json!({"title": "Wrong workspace"});
        cross_workspace.expected_version = Some(5);
        let error = execute_resource_operation(&mut other_workspace, &contract, cross_workspace)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("not found"),
            "cross-workspace records must be existence-hidden: {error}"
        );
        drop(other_workspace);

        let security =
            SecurityContext::new("user-1", "tenant-a").with_roles(["editor".to_string()]);
        assert_eq!(
            db.secure(&security)
                .scan_collection("documents")
                .unwrap()
                .len(),
            2
        );
        assert_eq!(db.scan_collection("__bicdb_app_audit").unwrap().len(), 6);
        let events = db
            .broker()
            .consume(
                "document_events",
                "vertical-slice",
                "test-consumer",
                ConsumeOptions {
                    max_messages: 10,
                    visibility_timeout_ms: 30_000,
                },
            )
            .unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].payload["operation"], "create");
        assert_eq!(events[1].payload["operation"], "create");
        assert_eq!(events[2].payload["operation"], "delete");
        let envelope = &events[0].headers["bicdb_application"];
        assert_eq!(envelope["actor_id"], "user-1");
        assert_eq!(envelope["tenant_id"], "tenant-a");
        assert_eq!(envelope["workspace_id"], "workspace-a");
        assert_eq!(envelope["originating_plugin"], "carrier-resource-api");
        assert_eq!(envelope["originating_resource"], "documents");
        assert_eq!(envelope["originating_action"], "create");
        assert_eq!(envelope["contract_version"], "1");
        assert_eq!(envelope["event_schema_version"], "1");
        assert!(envelope["transaction_id"].as_str().is_some());
    }

    #[test]
    fn signed_row_policy_preserves_any_all_predicates_and_fail_closed_rules() {
        use bicdb_extension::abi_v2::{
            ApplicationExpressionTypeV1, ApplicationExpressionV1, ResourcePolicyContractV1,
        };

        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("db")).unwrap();
        let mut contract = contract();
        let title_is_allowed = ApplicationExpressionV1::Binary {
            operator: "equal".to_string(),
            left: Box::new(ApplicationExpressionV1::Variable {
                name: "title".to_string(),
            }),
            right: Box::new(ApplicationExpressionV1::Literal {
                value: json!("allowed"),
            }),
            value_type: Some(ApplicationExpressionTypeV1::Bool),
            left_type: Some(ApplicationExpressionTypeV1::String),
            right_type: Some(ApplicationExpressionTypeV1::String),
        };
        contract.policy = Some(ResourcePolicyContractV1 {
            version: 1,
            tenant_expression: None,
            read: Some(ResourcePolicyRuleV1 {
                roles: BTreeSet::from(["editor".to_string(), "auditor".to_string()]),
                role_match: ResourcePolicyRoleMatchV1::Any,
                expression: Some(title_is_allowed),
            }),
            write: None,
            deleted_read: None,
            sql: None,
        });
        let extension = manifest(contract.clone());
        let mut host = CapabilityHost::new(
            &mut db,
            extension,
            actor("tenant-a", "workspace-a", &["editor"]),
            services(&directory),
        )
        .unwrap();

        assert!(
            read_policy_allows(&mut host, &contract, &json!({"title": "allowed"}), None, 0,)
                .unwrap()
        );
        assert!(
            !read_policy_allows(&mut host, &contract, &json!({"title": "denied"}), None, 0,)
                .unwrap()
        );
        assert!(
            enforce_write_policy(&mut host, &contract, &json!({"title": "allowed"}), None,)
                .is_err()
        );

        contract
            .policy
            .as_mut()
            .unwrap()
            .read
            .as_mut()
            .unwrap()
            .role_match = ResourcePolicyRoleMatchV1::All;
        assert!(
            !read_policy_allows(&mut host, &contract, &json!({"title": "allowed"}), None, 0,)
                .unwrap()
        );
    }

    #[test]
    fn signed_row_policy_resolves_correlated_exists_for_typed_reads_and_writes() {
        use bicdb_extension::abi_v2::ResourcePolicyContractV1;

        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("db")).unwrap();
        for collection in ["documents", "tenant_identities"] {
            db.create_collection(collection).unwrap();
        }
        for (id, title, tenant_id, tenant_code) in [
            ("visible", "Visible", "tenant-a", "acme"),
            ("hidden", "Hidden", "tenant-a", "hidden"),
            ("cross-tenant", "Cross tenant", "tenant-b", "acme"),
        ] {
            db.insert(
                "documents",
                Record::new(id).with_metadata(json!({
                    "id": id,
                    "title": title,
                    "tenant_id": tenant_id,
                    "workspace_id": "workspace-a",
                    "tenant_code": tenant_code,
                    "deleted_at": null,
                    "version": 1,
                    "slug": title.to_lowercase().replace(' ', "-"),
                })),
            )
            .unwrap();
        }
        for (id, tenant_code, region) in [
            ("member-visible", "acme", "clinician@example.test"),
            ("member-hidden", "hidden", "someone-else@example.test"),
        ] {
            db.insert(
                "tenant_identities",
                Record::new(id).with_metadata(json!({
                    "id": id,
                    "tenant_code": tenant_code,
                    "region": region,
                })),
            )
            .unwrap();
        }

        let actor_email_rule: ApplicationExpressionV1 = serde_json::from_value(json!({
            "op": "binary",
            "operator": "equal",
            "left": {"op": "variable", "name": "region"},
            "right": {
                "op": "field",
                "target": {"op": "variable", "name": "auth"},
                "field": "email"
            },
            "value_type": "bool",
            "left_type": "string",
            "right_type": "string"
        }))
        .unwrap();
        let correlated_membership: ApplicationExpressionV1 = serde_json::from_value(json!({
            "op": "exists",
            "binding": "membership",
            "resource": "TenantIdentity",
            "condition": {
                "op": "binary",
                "operator": "and",
                "left": {
                    "op": "binary",
                    "operator": "equal",
                    "left": {
                        "op": "field",
                        "target": {"op": "variable", "name": "membership"},
                        "field": "tenant_code"
                    },
                    "right": {"op": "variable", "name": "tenant_code"},
                    "value_type": "bool",
                    "left_type": "string",
                    "right_type": "string"
                },
                "right": {
                    "op": "binary",
                    "operator": "equal",
                    "left": {
                        "op": "field",
                        "target": {"op": "variable", "name": "membership"},
                        "field": "region"
                    },
                    "right": {
                        "op": "field",
                        "target": {"op": "variable", "name": "auth"},
                        "field": "email"
                    },
                    "value_type": "bool",
                    "left_type": "string",
                    "right_type": "string"
                },
                "value_type": "bool",
                "left_type": "bool",
                "right_type": "bool"
            }
        }))
        .unwrap();
        let tenant_rule: ApplicationExpressionV1 = serde_json::from_value(json!({
            "op": "binary",
            "operator": "equal",
            "left": {"op": "variable", "name": "tenant_id"},
            "right": {
                "op": "field",
                "target": {"op": "variable", "name": "auth"},
                "field": "tenant_id"
            },
            "value_type": "bool",
            "left_type": "string",
            "right_type": "string"
        }))
        .unwrap();
        let allow_editor = |expression| ResourcePolicyRuleV1 {
            roles: BTreeSet::from(["editor".to_string()]),
            role_match: ResourcePolicyRoleMatchV1::Any,
            expression: Some(expression),
        };

        let mut document = contract();
        document.foreign_keys.clear();
        document.events.clear();
        document.audit = AuditContract::default();
        document.idempotency = None;
        document.policy = Some(ResourcePolicyContractV1 {
            version: 1,
            tenant_expression: Some(tenant_rule),
            read: Some(allow_editor(correlated_membership.clone())),
            write: Some(allow_editor(correlated_membership)),
            deleted_read: None,
            sql: None,
        });
        let mut membership = tenant_identity_contract();
        membership.policy = Some(ResourcePolicyContractV1 {
            version: 1,
            tenant_expression: None,
            read: Some(allow_editor(actor_email_rule.clone())),
            write: Some(allow_editor(actor_email_rule)),
            deleted_read: None,
            sql: None,
        });
        let extension = manifest_with_tenant_identity(document.clone(), membership.clone());
        let mut clinician = actor("tenant-a", "workspace-a", &["editor"]);
        clinician
            .policy_attributes
            .insert("email".to_string(), "clinician@example.test".to_string());
        let mut host =
            CapabilityHost::new(&mut db, extension, clinician, services(&directory)).unwrap();

        let diagnostic_transaction = begin(&mut host).unwrap();
        let membership_rows = host
            .policy_candidate_rows(diagnostic_transaction, "tenant_identities", &[])
            .unwrap();
        assert_eq!(membership_rows.len(), 2);
        let visible_membership = membership_rows
            .iter()
            .find(|row| row["id"] == "member-visible")
            .unwrap();
        let hidden_membership = membership_rows
            .iter()
            .find(|row| row["id"] == "member-hidden")
            .unwrap();
        assert!(read_policy_allows(
            &mut host,
            &membership,
            visible_membership,
            Some(diagnostic_transaction),
            0,
        )
        .unwrap());
        assert!(!read_policy_allows(
            &mut host,
            &membership,
            hidden_membership,
            Some(diagnostic_transaction),
            0,
        )
        .unwrap());
        let document_rows = host
            .policy_candidate_rows(diagnostic_transaction, "documents", &[])
            .unwrap();
        assert_eq!(document_rows.len(), 2);
        let visible_document = document_rows
            .iter()
            .find(|row| row["id"] == "visible")
            .unwrap();
        assert!(read_policy_allows(
            &mut host,
            &document,
            visible_document,
            Some(diagnostic_transaction),
            0,
        )
        .unwrap());
        call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Rollback {
                transaction: diagnostic_transaction,
            }),
        )
        .unwrap();

        let listed =
            execute_resource_operation(&mut host, &document, request(ResourceOperation::List))
                .unwrap();
        let items = listed.body["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["id"], "visible");

        let transaction = begin(&mut host).unwrap();
        assert!(enforce_write_policy(
            &mut host,
            &document,
            &json!({
                "tenant_id": "tenant-a",
                "tenant_code": "acme",
            }),
            Some(transaction),
        )
        .is_ok());
        assert!(enforce_write_policy(
            &mut host,
            &document,
            &json!({
                "tenant_id": "tenant-a",
                "tenant_code": "hidden",
            }),
            Some(transaction),
        )
        .is_err());
        call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Rollback { transaction }),
        )
        .unwrap();
    }

    fn policy_scoped_list_case(row_count: usize) {
        use bicdb_extension::abi_v2::ResourcePolicyContractV1;

        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("db")).unwrap();
        let mut contract = contract();
        contract.foreign_keys.clear();
        contract.events.clear();
        contract.audit = AuditContract::default();
        contract.idempotency = None;
        // The policy contract routes every list through the policy-filtered path, whose
        // candidate scan runs through the trusted single-statement host path.
        contract.policy = Some(ResourcePolicyContractV1 {
            version: 1,
            tenant_expression: None,
            read: Some(ResourcePolicyRuleV1 {
                roles: BTreeSet::from(["editor".to_string()]),
                role_match: ResourcePolicyRoleMatchV1::Any,
                expression: None,
            }),
            write: Some(ResourcePolicyRuleV1 {
                roles: BTreeSet::from(["editor".to_string()]),
                role_match: ResourcePolicyRoleMatchV1::Any,
                expression: None,
            }),
            deleted_read: None,
            sql: None,
        });
        for collection in ["documents"] {
            db.create_collection(collection).unwrap();
        }
        // Seed through the core API before policies are installed: the list
        // path is what is under test, and the resource create path cannot
        // seed a beyond-ABI-bound candidate set within one invocation
        // deadline.
        let mut seed = db.begin_transaction().unwrap();
        for index in 0..row_count {
            seed.insert(
                "documents",
                Record::new(format!("policy-doc-{index:07}")).with_metadata(json!({
                    "id": format!("policy-doc-{index:07}"),
                    "title": format!("Doc {index}"),
                    "slug": format!("doc-{index}"),
                    "deleted_at": Value::Null,
                    "tenant_id": "tenant-a",
                    "workspace_id": "workspace-a",
                    "version": 1,
                })),
            )
            .unwrap();
        }
        seed.commit().unwrap();
        db.set_collection_policy(
            "documents",
            CollectionPolicy::tenant_field("tenant_id")
                .with_read_roles(["editor"])
                .with_write_roles(["editor"]),
        )
        .unwrap();
        db.set_mutation_policy(
            "documents",
            MutationPolicy::grants_required()
                .with_tenant_field("tenant_id")
                .with_workspace_field("workspace_id")
                .with_version_field("version")
                .with_immutable_fields(["tenant_id", "workspace_id"]),
        )
        .unwrap();
        let extension = manifest(contract.clone());
        let mut host = CapabilityHost::new(
            &mut db,
            extension,
            actor("tenant-a", "workspace-a", &["editor"]),
            services(&directory),
        )
        .unwrap();
        let mut list = request(ResourceOperation::List);
        list.include_total = true;
        let page_limit = list.limit as usize;
        let listed = execute_resource_operation(&mut host, &contract, list).unwrap();
        assert_eq!(listed.status, 200);
        assert_eq!(
            listed.body["items"].as_array().unwrap().len(),
            row_count.min(page_limit)
        );
        assert_eq!(listed.body["page_info"]["total"], row_count);
        assert!(
            listed
                .headers
                .iter()
                .any(|(name, value)| name == "x-bicdb-result-total"
                    && value == &row_count.to_string())
        );
    }

    #[test]
    fn policy_scoped_list_succeeds_on_policy_bearing_resource() {
        policy_scoped_list_case(3);
    }

    /// The candidate set here is larger than the ABI's 10_000-row per-query
    /// bound. It must still be listable in one statement through the trusted
    /// scan; regressing this path onto the bounded ABI query either fails the
    /// list outright or forces multi-statement paging whose read-committed
    /// snapshot refreshes smear the candidate set.
    #[test]
    fn policy_scoped_list_scans_candidates_beyond_abi_query_bound() {
        policy_scoped_list_case(10_050);
    }

    #[test]
    fn resource_idempotency_survives_restart_and_package_upgrade_then_expires() {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("db");
        let mut contract = contract();
        contract.foreign_keys.clear();
        contract.events.clear();
        contract.audit = AuditContract::default();

        let mut db = BicDb::open(&database_path).unwrap();
        for collection in ["documents", "__bicdb_app_idempotency"] {
            db.create_collection(collection).unwrap();
        }
        db.set_collection_policy(
            "documents",
            CollectionPolicy::tenant_field("tenant_id")
                .with_read_roles(["editor"])
                .with_write_roles(["editor"]),
        )
        .unwrap();
        db.set_mutation_policy(
            "documents",
            MutationPolicy::grants_required()
                .with_tenant_field("tenant_id")
                .with_workspace_field("workspace_id")
                .with_version_field("version")
                .with_immutable_fields(["tenant_id", "workspace_id"]),
        )
        .unwrap();
        db.set_mutation_policy("__bicdb_app_idempotency", MutationPolicy::grants_required())
            .unwrap();

        let mut initial = request(ResourceOperation::Create);
        initial.body = json!({"id": "restart-1", "title": "Persistent"});
        initial.idempotency_key = Some("restart-key".to_string());
        let initial_manifest = manifest(contract.clone());
        let mut host = CapabilityHost::new(
            &mut db,
            initial_manifest,
            actor("tenant-a", "workspace-a", &["editor"]),
            services(&directory),
        )
        .unwrap();
        assert_eq!(
            execute_resource_operation(&mut host, &contract, initial.clone())
                .unwrap()
                .status,
            201
        );
        drop(host);
        drop(db);

        let mut db = BicDb::open(&database_path).unwrap();
        let mut upgraded = manifest(contract.clone());
        let upgraded = Arc::make_mut(&mut upgraded);
        upgraded.identity.version = "1.1.0".to_string();
        let application = upgraded.application.as_mut().unwrap();
        application.package.version = "1.1.0".to_string();
        application.package.package_sha256 = "b".repeat(64);
        let upgraded_manifest = Arc::new(upgraded.clone());
        let mut host = CapabilityHost::new(
            &mut db,
            upgraded_manifest.clone(),
            actor("tenant-a", "workspace-a", &["editor"]),
            services(&directory),
        )
        .unwrap();
        let replayed = execute_resource_operation(&mut host, &contract, initial).unwrap();
        assert!(replayed
            .headers
            .iter()
            .any(|(name, value)| name == "idempotency-replayed" && value == "true"));
        drop(host);

        let mut second_actor = actor("tenant-a", "workspace-a", &["editor"]);
        second_actor.user_id = Some("user-2".to_string());
        let mut host = CapabilityHost::new(
            &mut db,
            upgraded_manifest,
            second_actor,
            services(&directory),
        )
        .unwrap();
        let mut principal_scoped = request(ResourceOperation::Create);
        principal_scoped.body = json!({"id": "restart-2", "title": "Other user"});
        principal_scoped.idempotency_key = Some("restart-key".to_string());
        assert_eq!(
            execute_resource_operation(&mut host, &contract, principal_scoped)
                .unwrap()
                .status,
            201
        );

        let mut expiring_contract = contract.clone();
        expiring_contract.idempotency.as_mut().unwrap().ttl_seconds = 1;
        let mut expiring = request(ResourceOperation::Create);
        expiring.body = json!({"id": "expiry-1", "title": "Before expiry"});
        expiring.idempotency_key = Some("expiry-key".to_string());
        execute_resource_operation(&mut host, &expiring_contract, expiring).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1_100));
        let mut renewed = request(ResourceOperation::Create);
        renewed.body = json!({"id": "expiry-2", "title": "After expiry"});
        renewed.idempotency_key = Some("expiry-key".to_string());
        assert_eq!(
            execute_resource_operation(&mut host, &expiring_contract, renewed)
                .unwrap()
                .status,
            201
        );
    }

    #[test]
    fn encrypted_resource_fields_are_ciphertext_at_rest_and_plaintext_after_authorized_reads() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("db")).unwrap();
        for collection in ["documents", "tenant_identities", "__bicdb_app_audit"] {
            db.create_collection(collection).unwrap();
        }
        db.set_collection_policy(
            "documents",
            CollectionPolicy::tenant_field("tenant_id")
                .with_read_roles(["editor", "administrator"])
                .with_write_roles(["editor"]),
        )
        .unwrap();
        db.set_mutation_policy(
            "documents",
            MutationPolicy::grants_required()
                .with_tenant_field("tenant_id")
                .with_workspace_field("workspace_id")
                .with_version_field("version")
                .with_immutable_fields(["tenant_id", "workspace_id"])
                .with_audit(),
        )
        .unwrap();
        db.set_mutation_policy(
            "__bicdb_app_audit",
            MutationPolicy::grants_required().append_only(),
        )
        .unwrap();

        let mut contract = contract();
        contract.idempotency = None;
        contract
            .encrypted_fields
            .insert("secret".to_string(), "DOCUMENT_SECRET_KEY".to_string());
        contract.privacy = Some(
            serde_json::from_value(json!({
                "fields": {
                    "secret": {
                        "encrypted": true,
                        "classification": "confidential",
                        "presentation": {
                            "list": "hidden",
                            "detail": "masked",
                            "input": "editable",
                            "clipboard": "explicit"
                        }
                    }
                }
            }))
            .unwrap(),
        );
        let extension = manifest(contract.clone());
        let invocation_services = services(&directory);
        let mut host = CapabilityHost::new(
            &mut db,
            extension,
            actor("tenant-a", "workspace-a", &["editor", "administrator"]),
            invocation_services,
        )
        .unwrap();
        let mut create = request(ResourceOperation::Create);
        create.body = json!({
            "id": "encrypted-doc",
            "title": "Private",
            "secret": "classified"
        });
        let created = execute_resource_operation(&mut host, &contract, create).unwrap();
        assert_eq!(created.body["secret"], "classified");

        let mut get = request(ResourceOperation::Get);
        get.id = Some("encrypted-doc".to_string());
        let read = execute_resource_operation(&mut host, &contract, get).unwrap();
        assert_eq!(read.body["secret"], "classified");

        let secret = expect_handle(
            call(
                &mut host,
                HostRequest::Secret(SecretRequest::Open {
                    name: "DOCUMENT_SECRET_KEY".to_string(),
                    version: None,
                }),
            )
            .unwrap(),
        )
        .unwrap();
        let envelope = match call(
            &mut host,
            HostRequest::Crypto(CryptoRequest::Encrypt {
                secret,
                algorithm: "bicdb-aes-256-gcm-v2".to_string(),
                plaintext: b"explicit-bicdb-encryption".to_vec(),
                associated_data: vec![],
            }),
        )
        .unwrap()
        {
            HostValue::Bytes(bytes) => String::from_utf8(bytes).unwrap(),
            other => panic!("expected encrypted bytes, found {other:?}"),
        };
        assert!(envelope.starts_with("enc:v2:"));
        let decrypted =
            decrypt_resource_value(&mut host, &contract, json!({"secret": envelope})).unwrap();
        assert_eq!(decrypted["secret"], "explicit-bicdb-encryption");
        drop(host);

        let security = SecurityContext::new("user-1", "tenant-a")
            .with_workspace_id("workspace-a")
            .with_roles(["editor".to_string(), "administrator".to_string()]);
        let stored = db
            .secure(&security)
            .get("documents", "encrypted-doc")
            .unwrap()
            .unwrap();
        let ciphertext = stored.metadata["secret"].as_str().unwrap();
        assert!(ciphertext.starts_with("enc:v1:"));
        assert!(!ciphertext.contains("classified"));
        let audit =
            serde_json::to_string(&db.scan_collection("__bicdb_app_audit").unwrap()).unwrap();
        assert!(!audit.contains("classified"));
    }
}
