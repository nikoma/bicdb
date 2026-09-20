//! Signed package lifecycle and atomically published application snapshots.

mod application_workflow;
pub(crate) use application_workflow::*;
mod scheduling;
pub(crate) use scheduling::*;
mod schema_defs;
pub(crate) use schema_defs::*;
mod application_runtime;
mod program_host;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use base64::Engine;
use bicdb_core::{
    AuthenticationStrength, BicDb, CollectionPolicy, ConsumeOptions, IndexDefinition,
    IndexExclusion, IndexExclusionElement, IndexField, IndexKind, IndexPredicate,
    IndexPredicateBinaryOperator, IndexPredicateUnaryOperator, IndexPredicateValueType,
    MutationActor, MutationGrantSpec, MutationOperation, MutationPolicy, NackOptions,
    PublishOptions, Record, SecurityContext, TransactionIsolation,
};
use bicdb_extension::abi_v2::{
    ActorContext, AggregateSpec, ApplicationBlobContractV1, ApplicationCallKindV1,
    ApplicationClientBindingV1, ApplicationEvaluationAuthV1, ApplicationExpressionTypeV1,
    ApplicationExpressionV1, ApplicationFlagDefinitionV1, ApplicationFlagGroupV1,
    ApplicationFlagRuleV1, ApplicationGeometryTypeV1, ApplicationLlmBudgetV1,
    ApplicationLlmClientV1, ApplicationLlmOverBudgetV1, ApplicationLlmToolV1,
    ApplicationMutationBindingKindV1, ApplicationMutationOperationV1,
    ApplicationObservabilityContractV1, ApplicationProgramV1, ApplicationRouteParameterTypeV1,
    ApplicationSamplingV1, ApplicationSecurityContractV1, ApplicationServiceBindingV1,
    ApplicationWorkflowDefinitionV1, ApplicationWorkflowSlaConditionKindV1,
    ApplicationWorkflowSlaExclusionKindV1, ApplicationWorkflowStepV1,
    ApplicationWorkflowWaitKindV1, BlobMetadata, BlobRequest, BrokerRequest, ClockRequest,
    ContractField, CryptoRequest, DatabaseAction, DatabaseRequest, EgressRequest, EmailRequest,
    EmbeddingsRequest, ErrorClass, FieldType, FilterExpression, GrpcRequest, HostCall, HostHandle,
    HostRequest, HostValue, HttpRequestBodyV2, HttpResponseBodyV2, IdempotencyContract,
    IsolationLevel, LlmRequest, LogLevel, MetricKind, MigrationStep, ObserveRequest, RandomRequest,
    RedisRequest, RelationQuerySpec, RelationSearchSpec, ResourceContractV1, ResourceFilterKind,
    ResourceIndexKindV1, ResourceOperation, ResourceRecordScope, ResourceReverseRelationV1,
    RouteCacheContract, ScheduleDefinition, ScheduleMisfirePolicy, ScheduleOverlapPolicy,
    ScheduleUpgradePolicy, SecretMetadata, SecretRequest, ServiceMethod, ServiceRequest, SortField,
    SpatialOperation, TokenizerRequest, TransactionRequest,
};
use bicdb_extension::host::{ApplicationHost, WasmEngine, WasmExtension, WasmHostConfig};
use bicdb_extension::{
    resolve_extension_order, ExtensionInstallation, ExtensionInvocation, ExtensionInvocationResult,
    ExtensionManifest, ExtensionState, HttpMethod, InvocationContext, InvocationKind,
};
use bicdb_sql::{
    ensure_embedded_table_schema, reconcile_embedded_table_row_policy, EmbeddedTableColumn,
    EmbeddedTableRowPolicy, EmbeddedTableSchemaReceipt,
};
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use url::Url;

use crate::bounded_sql::analyze_bounded_sql;
use crate::host::{
    application_blob_provider_namespace, lower_carrier_invariants, record_from_resource_json,
    resource_record_json, CapabilityHost, PluginServiceCall, PluginServiceDispatcher,
};
use crate::package::{
    application_module_contract, dependency_lock, embedded_manifest_matches,
    ApplicationModuleContractV1, ApplicationPackage, PackageVerification, PackageVerifier,
};
use crate::program::evaluate_carrier_expression;
use crate::resource::{
    authorize as authorize_resource, decrypt_resource_value, execute_resource_operation,
    execute_resource_operation_in_transaction,
    execute_resource_operation_in_transaction_with_mutation_hook,
    execute_resource_operation_with_mutation_hook, normalize_object as normalize_resource_object,
    redact as redact_resource, resource_search_index_name, scope_filters as resource_scope_filters,
    validate_object as validate_resource_object, validate_request as validate_resource_request,
    ResourceMutation, ResourceRequest, ResourceResponse,
};
use crate::{
    execute_application_program, AppRuntimeError, ApplicationProgramHost, InvocationServices,
    Result,
};

#[derive(Clone, Debug)]
pub struct ApplicationHostConfig {
    pub package_root: PathBuf,
    pub wasm: WasmHostConfig,
    pub max_package_bytes: usize,
    pub node_id: String,
    pub max_history: usize,
    pub required_packages: BTreeSet<String>,
    pub idempotency_entries: usize,
    pub route_cache_entries: usize,
    pub runtime_cache_entries: usize,
    /// Content-addressed compiled Wasm modules retained outside active package
    /// snapshots. Active/staged/history snapshots keep their own `Arc` alive.
    pub module_cache_entries: usize,
}

impl ApplicationHostConfig {
    pub fn new(package_root: impl Into<PathBuf>, node_id: impl Into<String>) -> Self {
        Self {
            package_root: package_root.into(),
            wasm: WasmHostConfig::default(),
            max_package_bytes: 256 * 1024 * 1024,
            node_id: node_id.into(),
            max_history: 8,
            required_packages: BTreeSet::new(),
            idempotency_entries: 100_000,
            route_cache_entries: 100_000,
            runtime_cache_entries: 100_000,
            module_cache_entries: 128,
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.wasm.validate()?;
        if self.max_package_bytes == 0
            || self.node_id.trim().is_empty()
            || self.max_history == 0
            || self.idempotency_entries == 0
            || self.route_cache_entries == 0
            || self.runtime_cache_entries == 0
            || self.module_cache_entries == 0
        {
            return Err(AppRuntimeError::InvalidPackage(
                "application host limits and node identity must be positive/non-empty".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationReadiness {
    pub ready: bool,
    pub signatures_valid: bool,
    pub dependencies_bound: bool,
    pub migrations_complete: bool,
    pub contracts_loaded: bool,
    pub routes_active: bool,
    pub workers_healthy: bool,
    pub schedules_healthy: bool,
    pub providers_available: bool,
    pub schema_compatible: bool,
    #[serde(default)]
    pub issues: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationPackageState {
    Staged,
    Active,
    Disabled,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationEvaluationResult {
    pub evaluation: String,
    pub passed_cases: u64,
    pub total_cases: u64,
    pub pass_rate: f64,
    pub requirement_passed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationTestResult {
    pub test: String,
    pub passed_cases: u64,
    pub total_cases: u64,
    pub seed: u64,
    pub passed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_case: Option<Value>,
}

#[derive(Clone)]
pub struct PackageSnapshot {
    pub generation: u64,
    pub package_hash: String,
    pub package: Arc<ApplicationPackage>,
    pub verification: PackageVerification,
    pub state: ApplicationPackageState,
    pub readiness: ApplicationReadiness,
    pub activated_at_ms: i64,
    modules: BTreeMap<String, Arc<WasmExtension>>,
    manifest: Arc<bicdb_extension::ExtensionManifest>,
    carrier_plan: Option<Arc<ApplicationExecutionPlan>>,
}

#[derive(Clone)]
struct ApplicationExecutionPlan {
    manifest: Arc<bicdb_extension::ExtensionManifest>,
    resources: Arc<Vec<ResourceContractV1>>,
    service_bindings: Arc<BTreeMap<String, ApplicationServiceBindingV1>>,
    client_bindings: Arc<BTreeMap<String, ApplicationClientBindingV1>>,
    secret_bindings: Arc<BTreeMap<String, String>>,
    event_bindings: Arc<BTreeMap<String, String>>,
    realtime_bindings: Arc<BTreeMap<String, BTreeSet<String>>>,
    workflow_bindings: Arc<BTreeMap<String, ApplicationWorkflowDefinitionV1>>,
}

impl ApplicationExecutionPlan {
    fn prepare(manifest: Arc<bicdb_extension::ExtensionManifest>) -> Option<Arc<Self>> {
        let application = manifest.application.as_deref()?;
        let program = application.application_program.as_ref()?;
        Some(Arc::new(Self {
            resources: Arc::new(application.resources.clone()),
            service_bindings: Arc::new(program.service_bindings.clone()),
            client_bindings: Arc::new(program.client_bindings.clone()),
            secret_bindings: Arc::new(program.secret_bindings.clone()),
            event_bindings: Arc::new(program.event_bindings.clone()),
            realtime_bindings: Arc::new(program.realtime_bindings.clone()),
            workflow_bindings: Arc::new(program.workflow_bindings.clone()),
            manifest,
        }))
    }
}

#[derive(Default)]
struct CompiledModuleCache {
    entries: BTreeMap<String, Arc<WasmExtension>>,
    lru: VecDeque<String>,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl CompiledModuleCache {
    fn get(&mut self, sha256: &str) -> Option<Arc<WasmExtension>> {
        let module = self.entries.get(sha256).cloned()?;
        self.hits = self.hits.saturating_add(1);
        self.touch(sha256);
        Some(module)
    }

    fn insert(
        &mut self,
        sha256: String,
        module: Arc<WasmExtension>,
        capacity: usize,
    ) -> Arc<WasmExtension> {
        if let Some(existing) = self.entries.get(&sha256).cloned() {
            self.hits = self.hits.saturating_add(1);
            self.touch(&sha256);
            return existing;
        }
        self.misses = self.misses.saturating_add(1);
        self.entries.insert(sha256.clone(), Arc::clone(&module));
        self.touch(&sha256);
        while self.entries.len() > capacity {
            let Some(evicted) = self.lru.pop_front() else {
                break;
            };
            if self.entries.remove(&evicted).is_some() {
                self.evictions = self.evictions.saturating_add(1);
            }
        }
        module
    }

    fn touch(&mut self, sha256: &str) {
        if let Some(index) = self.lru.iter().position(|entry| entry == sha256) {
            self.lru.remove(index);
        }
        self.lru.push_back(sha256.to_string());
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationPerformanceSnapshot {
    pub active_packages: usize,
    #[serde(alias = "prepared_carrier_programs")]
    pub prepared_application_programs: usize,
    pub compiled_module_cache_entries: usize,
    pub compiled_module_cache_capacity: usize,
    pub compiled_module_cache_hits: u64,
    pub compiled_module_cache_misses: u64,
    pub compiled_module_cache_evictions: u64,
    pub wasm_pool_capacity: u32,
}

#[derive(Debug)]
struct RelationPolicySnapshot {
    relation: String,
    collection: Option<CollectionPolicy>,
    mutation: Option<MutationPolicy>,
}

#[derive(Debug, Default)]
struct SchemaApplicationReceipt {
    created_collections: Vec<String>,
    created_indexes: Vec<String>,
    replaced_indexes: Vec<IndexDefinition>,
    embedded_sql_schemas: Vec<EmbeddedTableSchemaReceipt>,
    policy_snapshots: Vec<RelationPolicySnapshot>,
    migration_records: Vec<String>,
    backfilled_records: Vec<(String, Record)>,
}

impl SchemaApplicationReceipt {
    fn rollback(&self, db: &mut BicDb) -> Result<()> {
        if !self.backfilled_records.is_empty() {
            restore_backfilled_records(db, &self.backfilled_records)?;
        }
        if !self.migration_records.is_empty() {
            remove_applied_migrations(db, &self.migration_records)?;
        }
        for index in self.created_indexes.iter().rev() {
            db.drop_index(index)?;
        }
        for index in self.replaced_indexes.iter().rev() {
            db.drop_index(&index.name)?;
            db.create_index(index.clone())?;
        }
        for schema in self.embedded_sql_schemas.iter().rev() {
            schema.rollback(db).map_err(|error| {
                AppRuntimeError::InvalidPackage(format!(
                    "embedded SQL schema compensation failed: {error}"
                ))
            })?;
        }
        for snapshot in self.policy_snapshots.iter().rev() {
            match &snapshot.collection {
                Some(policy) => db.set_collection_policy(&snapshot.relation, policy.clone())?,
                None => db.clear_collection_policy(&snapshot.relation)?,
            }
            match &snapshot.mutation {
                Some(policy) => db.set_mutation_policy(&snapshot.relation, policy.clone())?,
                None => db.clear_mutation_policy(&snapshot.relation)?,
            }
        }
        for collection in self.created_collections.iter().rev() {
            db.drop_collection(collection)?;
        }
        Ok(())
    }
}

fn embedded_resource_columns(contract: &ResourceContractV1) -> Vec<EmbeddedTableColumn> {
    contract
        .fields
        .iter()
        .map(|field| {
            let (pg_type, vector_dimensions) = match &field.field_type {
                FieldType::Bool => ("bool", None),
                FieldType::Int64 => ("int8", None),
                FieldType::Float64 => ("float8", None),
                FieldType::Decimal => ("numeric", None),
                FieldType::String => ("text", None),
                FieldType::Bytes => ("bytea", None),
                FieldType::Uuid => ("uuid", None),
                FieldType::Timestamp => ("timestamptz", None),
                FieldType::Date => ("date", None),
                FieldType::Json | FieldType::Geometry { .. } => ("jsonb", None),
                FieldType::Vector { dimensions } => ("vector", Some(*dimensions)),
            };
            EmbeddedTableColumn {
                name: field.name.clone(),
                pg_type: pg_type.to_string(),
                nullable: field.nullable,
                primary_key: field.name == contract.primary_key,
                vector_dimensions,
            }
        })
        .collect()
}

fn embedded_resource_row_policy(contract: &ResourceContractV1) -> Option<EmbeddedTableRowPolicy> {
    let policy = contract.policy.as_ref()?.sql.as_ref()?;
    Some(EmbeddedTableRowPolicy {
        select_using: policy.select_using.clone(),
        insert_with_check: policy.insert_with_check.clone(),
        update_using: policy.update_using.clone(),
        update_with_check: policy.update_with_check.clone(),
        delete_using: policy.delete_using.clone(),
    })
}

impl std::fmt::Debug for PackageSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PackageSnapshot")
            .field("generation", &self.generation)
            .field("package_hash", &self.package_hash)
            .field("application", &self.package.manifest.identity.name)
            .field("version", &self.package.manifest.identity.version)
            .field("state", &self.state)
            .field("readiness", &self.readiness)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDiagnostic {
    pub application: String,
    pub version: String,
    pub generation: u64,
    pub state: ApplicationPackageState,
    pub package_hash: String,
    pub readiness: ApplicationReadiness,
    pub routes: Vec<String>,
    pub services: Vec<String>,
    pub workers: Vec<String>,
    pub schedules: Vec<String>,
}

#[derive(Default)]
struct RuntimeCatalog {
    generation: u64,
    packages: BTreeMap<String, Arc<PackageSnapshot>>,
    routes: BTreeMap<(String, String), String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedRuntimeState {
    generation: u64,
    packages: BTreeMap<String, PersistedPackageReference>,
    #[serde(default)]
    staged: BTreeMap<String, PersistedPackageReference>,
    #[serde(default)]
    history: BTreeMap<String, VecDeque<PersistedPackageReference>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPackageReference {
    hash: String,
    generation: u64,
    activated_at_ms: i64,
}

struct SupervisorControl {
    stop: Arc<AtomicBool>,
    active: Arc<AtomicBool>,
    healthy: Arc<AtomicBool>,
    task: Option<JoinHandle<()>>,
}

impl SupervisorControl {
    fn activate(&self) {
        self.active.store(true, Ordering::Release);
    }

    fn stop(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}

#[derive(Default)]
struct SupervisorSet {
    workers: BTreeMap<String, SupervisorControl>,
    schedules: BTreeMap<String, SupervisorControl>,
}

impl SupervisorSet {
    fn activate(&self) {
        for control in self.workers.values().chain(self.schedules.values()) {
            control.activate();
        }
    }

    fn healthy(&self) -> bool {
        self.workers
            .values()
            .chain(self.schedules.values())
            .all(|control| control.healthy.load(Ordering::Acquire))
    }

    fn stop(self) {
        for control in self
            .workers
            .into_values()
            .chain(self.schedules.into_values())
        {
            control.stop();
        }
    }
}

fn stop_supervisor_sets(sets: BTreeMap<String, SupervisorSet>) {
    for set in sets.into_values() {
        set.stop();
    }
}

/// First-party in-process application host.
///
/// Staging validates and compiles all artifacts without touching the active
/// snapshot. Activation publishes one immutable catalog only after schema,
/// dependency, route, provider, worker, and readiness validation succeeds.
/// Existing requests retain their old `Arc`, so failed upgrades never replace
/// a valid application.
pub struct ApplicationRuntime {
    db: Arc<RwLock<BicDb>>,
    config: ApplicationHostConfig,
    verifier: PackageVerifier,
    services: InvocationServices,
    staged: RwLock<BTreeMap<String, Arc<PackageSnapshot>>>,
    active: RwLock<Arc<RuntimeCatalog>>,
    lifecycle: Mutex<()>,
    history: Mutex<BTreeMap<String, VecDeque<Arc<PackageSnapshot>>>>,
    supervisors: Mutex<BTreeMap<String, SupervisorSet>>,
    wasm_engine: WasmEngine,
    module_cache: Mutex<CompiledModuleCache>,
}

impl std::fmt::Debug for ApplicationRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let active = self.active.read();
        formatter
            .debug_struct("ApplicationRuntime")
            .field("node_id", &self.config.node_id)
            .field("generation", &active.generation)
            .field("active_packages", &active.packages.len())
            .finish()
    }
}

#[derive(Clone)]
enum ApplicationTransactionFrame {
    Transaction(HostHandle),
    Savepoint {
        transaction: HostHandle,
        savepoint: HostHandle,
    },
}

#[derive(Clone, Copy)]
enum DeclaredSqlResult {
    List,
    One,
    Scalar,
    Affected,
}

fn carrier_isolation_rank(isolation: IsolationLevel) -> u8 {
    match isolation {
        IsolationLevel::ReadCommitted => 0,
        IsolationLevel::RepeatableRead => 1,
        IsolationLevel::Serializable => 2,
    }
}

fn core_isolation_rank(isolation: TransactionIsolation) -> u8 {
    match isolation {
        TransactionIsolation::ReadCommitted => 0,
        TransactionIsolation::RepeatableRead => 1,
        TransactionIsolation::Serializable => 2,
    }
}

struct CapabilityApplicationProgramHost<'a> {
    host: &'a mut CapabilityHost,
    resources: Arc<Vec<ResourceContractV1>>,
    service_bindings: Arc<BTreeMap<String, ApplicationServiceBindingV1>>,
    client_bindings: Arc<BTreeMap<String, ApplicationClientBindingV1>>,
    secret_bindings: Arc<BTreeMap<String, String>>,
    event_bindings: Arc<BTreeMap<String, String>>,
    realtime_bindings: Arc<BTreeMap<String, BTreeSet<String>>>,
    workflow_bindings: Arc<BTreeMap<String, ApplicationWorkflowDefinitionV1>>,
    secret_handles: BTreeMap<(String, Option<String>), HostHandle>,
    transactions: Vec<ApplicationTransactionFrame>,
    next_savepoint: u64,
    timeout_deadlines: Vec<i64>,
    virtual_files: BTreeMap<String, ApplicationVirtualFile>,
    test_http: Option<&'a ApplicationTestHttpCallback<'a>>,
}

type ApplicationTestHttpCallback<'a> =
    dyn Fn(&str, Vec<(Option<String>, Value)>) -> Result<Value> + Sync + 'a;

struct ApplicationVirtualFile {
    bytes: Vec<u8>,
    content_type: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ApplicationSecurityUserV1 {
    id: i64,
    email: String,
    name: String,
    roles: BTreeSet<String>,
    password_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ApplicationMagicLinkClaimsV1 {
    sub: String,
    email: String,
    exp: i64,
    iat: i64,
    jti: String,
    token_type: String,
    key_version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ApplicationOauthStateV1 {
    pkce_verifier: String,
    nonce: String,
    expires_at: i64,
}

pub(crate) struct IdempotentApplicationExecution {
    pub value: Value,
    pub replayed: bool,
    pub scope: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct ApplicationWorkflowStateV1 {
    application: String,
    workflow: String,
    status: String,
    input: Value,
    baggage: Value,
    step_results: BTreeMap<String, Value>,
    skipped_steps: BTreeSet<String>,
    completed_step_order: Vec<String>,
    attempts: BTreeMap<String, u32>,
    compensation_attempts: BTreeMap<String, u32>,
    compensated_steps: BTreeSet<String>,
    #[serde(default)]
    scheduled_steps: BTreeSet<String>,
    #[serde(default)]
    active_steps: BTreeSet<String>,
    #[serde(default)]
    waiting_steps: BTreeMap<String, ApplicationWorkflowWaitStateV1>,
    #[serde(default)]
    sla_states: BTreeMap<String, ApplicationWorkflowSlaStateV1>,
    #[serde(default)]
    evidence: Vec<ApplicationWorkflowEvidenceV1>,
    #[serde(default)]
    audited_evidence_sequence: u64,
    #[serde(default)]
    cancel_requested: bool,
    #[serde(default)]
    plan_sha256: Option<String>,
    active_step: Option<String>,
    last_error: Option<String>,
    output: Option<Value>,
    timeout_at_ms: Option<i64>,
    created_at_ms: i64,
    updated_at_ms: i64,
    finished_at_ms: Option<i64>,
    tenant_id: Option<String>,
    workspace_id: Option<String>,
    revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct ApplicationWorkflowWaitStateV1 {
    kind: String,
    signal: Option<String>,
    due_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
struct ApplicationWorkflowSlaStateV1 {
    excluded: bool,
    started_at_ms: Option<i64>,
    warning_at_ms: Option<i64>,
    breach_at_ms: Option<i64>,
    ended_at_ms: Option<i64>,
    warned_at_ms: Option<i64>,
    breached_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct ApplicationWorkflowEvidenceV1 {
    sequence: u64,
    kind: String,
    at_ms: i64,
    step: Option<String>,
    sla: Option<String>,
    detail: Option<Value>,
}

impl ApplicationWorkflowStateV1 {
    fn terminal(&self) -> bool {
        matches!(
            self.status.as_str(),
            "completed"
                | "failed"
                | "compensated"
                | "compensation_failed"
                | "timed_out"
                | "cancelled"
        )
    }
}

fn carrier_workflow_concurrency_conflict(error: &AppRuntimeError) -> bool {
    match error {
        AppRuntimeError::OptimisticConflict(_) => true,
        AppRuntimeError::Conflict(detail) => detail.starts_with("transaction conflict:"),
        _ => false,
    }
}

fn append_workflow_evidence(
    state: &mut ApplicationWorkflowStateV1,
    kind: impl Into<String>,
    step: Option<&str>,
    sla: Option<&str>,
    detail: Option<Value>,
) {
    const MAX_EVIDENCE: usize = 2_048;
    let sequence = state
        .evidence
        .last()
        .map(|entry| entry.sequence.saturating_add(1))
        .unwrap_or(1);
    state.evidence.push(ApplicationWorkflowEvidenceV1 {
        sequence,
        kind: kind.into(),
        at_ms: crate::host::now_ms(),
        step: step.map(str::to_string),
        sla: sla.map(str::to_string),
        detail,
    });
    if state.evidence.len() > MAX_EVIDENCE {
        state.evidence.remove(0);
    }
}

impl<'a> CapabilityApplicationProgramHost<'a> {}

impl ApplicationProgramHost for CapabilityApplicationProgramHost<'_> {
    fn check_deadline(&self) -> Result<()> {
        if crate::host::now_ms() >= self.host.actor().deadline_unix_ms {
            return Err(AppRuntimeError::Timeout(
                "BicDB application invocation deadline exceeded".to_string(),
            ));
        }
        Ok(())
    }

    fn call(
        &mut self,
        kind: ApplicationCallKindV1,
        target: &str,
        method: Option<&str>,
        arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value> {
        match kind {
            ApplicationCallKindV1::Action => self.call_service(target, arguments),
            ApplicationCallKindV1::Builtin => self.call_builtin(target, arguments),
            ApplicationCallKindV1::Queue => self.call_queue(
                target,
                method.ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(format!(
                        "BicDB application queue call `{target}` has no method"
                    ))
                })?,
                arguments,
            ),
            ApplicationCallKindV1::Client => self.call_client(
                target,
                method.ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(format!(
                        "BicDB application client call `{target}` has no method"
                    ))
                })?,
                arguments,
            ),
            ApplicationCallKindV1::Model => self.call_model(
                target,
                method.ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(format!(
                        "BicDB application model call `{target}` has no method"
                    ))
                })?,
                arguments,
            ),
            ApplicationCallKindV1::Llm => self.call_llm(
                target,
                method.ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(format!(
                        "BicDB application LLM call `{target}` has no method"
                    ))
                })?,
                arguments,
            ),
            ApplicationCallKindV1::Rag => self.call_rag(
                target,
                method.ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(format!(
                        "BicDB application RAG call `{target}` has no method"
                    ))
                })?,
                arguments,
            ),
            ApplicationCallKindV1::Agent => self.call_agent(
                target,
                method.ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(format!(
                        "BicDB application agent call `{target}` has no method"
                    ))
                })?,
                arguments,
            ),
            _ => Err(AppRuntimeError::CapabilityDenied(format!(
                "BicDB application program call {kind:?} `{target}{}` has no declared capability binding",
                method.map(|value| format!(".{value}")).unwrap_or_default()
            ))),
        }
    }

    fn enter_timeout(&mut self, timeout_ms: u64) -> Result<()> {
        if !self.timeout_deadlines.is_empty() {
            return Err(AppRuntimeError::InvalidPackage(
                "nested with_timeout blocks are not supported".to_string(),
            ));
        }
        let timeout_ms = i64::try_from(timeout_ms).map_err(|_| {
            AppRuntimeError::InvalidRequest(
                "with_timeout timeout_ms exceeds the supported range".to_string(),
            )
        })?;
        let previous = self.host.actor().deadline_unix_ms;
        let deadline = crate::host::now_ms()
            .checked_add(timeout_ms)
            .unwrap_or(i64::MAX)
            .min(previous);
        self.timeout_deadlines.push(previous);
        self.host.replace_actor_deadline_unix_ms(deadline);
        Ok(())
    }

    fn exit_timeout(&mut self) -> Result<()> {
        let previous = self.timeout_deadlines.pop().ok_or_else(|| {
            AppRuntimeError::InvalidPackage("with_timeout host deadline stack is empty".to_string())
        })?;
        self.host.replace_actor_deadline_unix_ms(previous);
        Ok(())
    }

    fn begin_transaction(&mut self, isolation: &str) -> Result<()> {
        let isolation = match isolation {
            "read_committed" => IsolationLevel::ReadCommitted,
            "repeatable_read" => IsolationLevel::RepeatableRead,
            "serializable" => IsolationLevel::Serializable,
            _ => {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "unknown BicDB application transaction isolation `{isolation}`"
                )))
            }
        };
        let transaction = self.current_transaction();
        let frame = if let Some(transaction) = transaction {
            let active = self.host.transaction_isolation(transaction)?;
            if carrier_isolation_rank(isolation) > core_isolation_rank(active) {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "BicDB application transaction isolation {isolation:?} cannot strengthen active {active:?} transaction isolation"
                )));
            }
            self.next_savepoint = self.next_savepoint.saturating_add(1);
            let value =
                self.host_call(HostRequest::Transaction(TransactionRequest::Savepoint {
                    transaction,
                    name: format!("carrier_{}", self.next_savepoint),
                }))?;
            ApplicationTransactionFrame::Savepoint {
                transaction,
                savepoint: expect_carrier_handle(value)?,
            }
        } else {
            let value = self.host_call(HostRequest::Transaction(TransactionRequest::Begin {
                isolation,
            }))?;
            ApplicationTransactionFrame::Transaction(expect_carrier_handle(value)?)
        };
        self.transactions.push(frame);
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<()> {
        let frame = self.transactions.last().cloned().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "BicDB application transaction commit has no active transaction".to_string(),
            )
        })?;
        let owning = matches!(&frame, ApplicationTransactionFrame::Transaction(_));
        let request = match frame {
            ApplicationTransactionFrame::Transaction(transaction) => {
                TransactionRequest::Commit { transaction }
            }
            ApplicationTransactionFrame::Savepoint {
                transaction,
                savepoint,
            } => TransactionRequest::Release {
                transaction,
                savepoint,
            },
        };
        let result = self
            .host_call(HostRequest::Transaction(request))
            .map(|_| ());
        if owning || result.is_ok() {
            // The host consumes an owning transaction handle before commit so
            // even a conflict cannot be rolled back or reused. Savepoints stay
            // on the stack when release itself fails, allowing rollback-to.
            self.transactions.pop();
        }
        result
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        let frame = self.transactions.last().cloned().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "BicDB application transaction rollback has no active transaction".to_string(),
            )
        })?;
        match frame {
            ApplicationTransactionFrame::Transaction(transaction) => {
                self.host_call(HostRequest::Transaction(TransactionRequest::Rollback {
                    transaction,
                }))?;
            }
            ApplicationTransactionFrame::Savepoint {
                transaction,
                savepoint,
            } => {
                self.host_call(HostRequest::Transaction(TransactionRequest::RollbackTo {
                    transaction,
                    savepoint,
                }))?;
                self.host_call(HostRequest::Transaction(TransactionRequest::Release {
                    transaction,
                    savepoint,
                }))?;
            }
        }
        self.transactions.pop();
        Ok(())
    }

    fn emit(&mut self, event: &str, payload: Value) -> Result<()> {
        let primary = self.event_bindings.get(event).cloned().ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(format!(
                "BicDB application event `{event}` has no signed queue binding"
            ))
        })?;
        let mut queues = BTreeSet::from([primary]);
        if let Some(realtime) = self.realtime_bindings.get(event) {
            queues.extend(realtime.iter().cloned());
        }
        for queue in queues {
            let headers = BTreeMap::from([
                ("carrier_event".to_string(), event.to_string()),
                ("carrier_queue".to_string(), queue.clone()),
            ]);
            let request = match self.current_transaction() {
                Some(transaction) => BrokerRequest::PublishOnCommit {
                    transaction,
                    queue,
                    payload: payload.clone(),
                    headers,
                    idempotency_key: None,
                    delay_ms: None,
                },
                None => BrokerRequest::Publish {
                    queue,
                    payload: payload.clone(),
                    headers,
                    idempotency_key: None,
                    delay_ms: None,
                },
            };
            match self.host_call(HostRequest::Broker(request))? {
                HostValue::String(_) => {}
                other => return carrier_host_type_error(event, "event receipt", other),
            }
        }
        Ok(())
    }
}

fn carrier_program_request_id() -> u64 {
    let bytes = uuid::Uuid::new_v4().into_bytes();
    u64::from_le_bytes(bytes[..8].try_into().expect("UUID prefix"))
}

pub(crate) fn actor_trace_sampled(actor: &ActorContext) -> bool {
    actor
        .policy_attributes
        .get("carrier.trace.sampled")
        .is_none_or(|value| value != "false")
}

pub(crate) fn carrier_sampling_decision(
    sampling: ApplicationSamplingV1,
    trace_id: &str,
    parent_sampled: Option<bool>,
) -> bool {
    let ratio = |millionths: u32| {
        if millionths == 0 {
            return false;
        }
        if millionths >= 1_000_000 {
            return true;
        }
        let digest = Sha256::digest(trace_id.as_bytes());
        let bucket =
            u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix")) % 1_000_000;
        bucket < u64::from(millionths)
    };
    match sampling {
        ApplicationSamplingV1::AlwaysOn => true,
        ApplicationSamplingV1::AlwaysOff => false,
        ApplicationSamplingV1::ParentBasedAlwaysOn => parent_sampled.unwrap_or(true),
        ApplicationSamplingV1::ParentBasedAlwaysOff => parent_sampled.unwrap_or(false),
        ApplicationSamplingV1::Ratio { millionths } => ratio(millionths),
        ApplicationSamplingV1::ParentBasedRatio { millionths } => {
            parent_sampled.unwrap_or_else(|| ratio(millionths))
        }
    }
}

fn carrier_traceparent(actor: &ActorContext) -> String {
    let compact = actor
        .trace_id
        .chars()
        .filter(|character| *character != '-')
        .collect::<String>();
    let trace_id = if compact.len() == 32
        && compact.bytes().all(|byte| byte.is_ascii_hexdigit())
        && compact.bytes().any(|byte| byte != b'0')
    {
        compact.to_ascii_lowercase()
    } else {
        carrier_hex(&Sha256::digest(actor.trace_id.as_bytes()))[..32].to_string()
    };
    let span_source = uuid::Uuid::new_v4().simple().to_string();
    let flags = actor
        .policy_attributes
        .get("w3c.trace_flags")
        .map(String::as_str)
        .unwrap_or_else(|| {
            if actor_trace_sampled(actor) {
                "01"
            } else {
                "00"
            }
        });
    format!("00-{trace_id}-{}-{flags}", &span_source[..16])
}

fn carrier_identifier_fragment(value: &str) -> String {
    let mut output = String::new();
    for character in value.chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            output.push(character.to_ascii_lowercase());
        } else if !output.ends_with('_') {
            output.push('_');
        }
    }
    output.trim_matches('_').to_string()
}

fn carrier_federated_workflow_binding_alias(workflow: &str, operation: &str) -> String {
    format!(
        "carrier_federated_workflow_{}_{}",
        carrier_identifier_fragment(workflow),
        carrier_identifier_fragment(operation)
    )
}

fn carrier_workflow_service_method(workflow: &str, operation: &str) -> String {
    format!(
        "carrier_workflow_{}_{}",
        carrier_identifier_fragment(workflow),
        carrier_identifier_fragment(operation)
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ApplicationWorkflowServiceOperation {
    Start,
    Status,
    Result,
    Cancel,
    Signal,
    Evidence,
    RetryCompensation,
}

fn carrier_workflow_service_operation(
    program: &bicdb_extension::abi_v2::ApplicationProgramV1,
    method: &str,
) -> Option<(String, ApplicationWorkflowServiceOperation)> {
    for workflow in program.workflow_bindings.keys() {
        for (operation, kind) in [
            ("start", ApplicationWorkflowServiceOperation::Start),
            ("status", ApplicationWorkflowServiceOperation::Status),
            ("result", ApplicationWorkflowServiceOperation::Result),
            ("cancel", ApplicationWorkflowServiceOperation::Cancel),
            ("signal", ApplicationWorkflowServiceOperation::Signal),
            ("evidence", ApplicationWorkflowServiceOperation::Evidence),
            (
                "retry_compensation",
                ApplicationWorkflowServiceOperation::RetryCompensation,
            ),
        ] {
            if carrier_workflow_service_method(workflow, operation) == method {
                return Some((workflow.clone(), kind));
            }
        }
    }
    None
}

fn carrier_workflow_service_run_id(
    payload: &serde_json::Map<String, Value>,
    operation: &str,
) -> Result<Vec<(Option<String>, Value)>> {
    let run_id = payload.get("run_id").cloned().ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!(
            "BicDB application workflow service `{operation}` lacks `run_id`"
        ))
    })?;
    Ok(vec![(None, run_id)])
}
