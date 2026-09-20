use std::collections::{BTreeMap, BTreeSet};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use aes_gcm::{Aes256Gcm, Nonce as AesNonce};
use base64::Engine;
use bicdb_core::{
    cosine_similarity, dot_product, l2_distance, AuthenticationStrength, BicDb, ConsumeOptions,
    Geometry, MutationActor, MutationGrantId, MutationGrantSpec, MutationOperation, NackOptions,
    NativeCommitValidator, NativeInvariantAggregateFunction, NativeInvariantBinaryOperator,
    NativeInvariantDefinition, NativeInvariantExpression, NativeInvariantKind,
    NativeInvariantUnaryOperator, NativeInvariantValueType, PublishOptions, Record,
    SecurityContext, Transaction, TransactionIsolation,
};
use bicdb_extension::abi_v2::{
    ActorContext, AggregateSpec, ApplicationAggregateFunctionV1, ApplicationCallKindV1,
    ApplicationExpressionTypeV1, ApplicationExpressionV1, ApplicationFeature,
    ApplicationGeometryTypeV1, ApplicationInvariantKindV1, ApplicationObservabilityContractV1,
    ApplicationRouteParameterTypeV1, ApplicationVectorMetricV1, BlobRequest,
    BrokerMessage as AbiBrokerMessage, BrokerRequest, ClockRequest, CommitValidator,
    CommitValidatorKind, ContractField, CryptoOperation, CryptoRequest, DatabaseAction,
    DatabaseRequest, EgressRequest, EmailRequest, EmbeddingsRequest, ErrorClass, FieldType,
    FilterExpression, GrpcRequest, HostCall, HostCallResult, HostError, HostHandle, HostRequest,
    HostValue, IsolationLevel, LlmRequest, ObserveRequest, QuerySpec, RandomRequest,
    RawSqlDeclaration, RedisRequest, RelationQuerySpec, ResourceContractV1, ResourceOperation,
    ResourcePolicyRoleMatchV1, ResourcePolicyRuleV1, ResourceTimeseriesV1, ResourceVectorSearchV1,
    SecretMetadata, SecretRequest, ServiceRequest, SortField, SpatialOperation, StreamRequest,
    TokenizerRequest, TransactionRequest,
};
use bicdb_extension::host::ApplicationHost;
use bicdb_extension::{ExtensionCapability, ExtensionManifest};
use bicdb_sql::{decode_typed_storage_json, SqlSession, SqlValue};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use geo::Contains as _;
use hmac::{Hmac, Mac};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::providers::{
    BlobProvider, BlobRecord, DenyEmailProvider, DenyEmbeddingsProvider, DenyEvaluationProvider,
    DenyGrpcProvider, DenyLlmProvider, DenyRedisProvider, DenyTokenizerProvider, EgressProvider,
    EmailMessage, EmailProvider, EmbeddingsProvider, EvaluationProvider, GrpcProvider,
    HostObservability, LlmProvider, LlmProviderRequest, ObservabilityEvent, RedisProvider,
    SecretProvider, SecretRecord, TokenizerProvider,
};
use crate::{AppRuntimeError, Result};

pub trait PluginServiceDispatcher: Send + Sync {
    fn call(&self, call: PluginServiceCall) -> Result<Value>;
}

#[derive(Clone, Copy)]
pub(crate) struct InvocationDatabase {
    pointer: NonNull<BicDb>,
}

// InvocationDatabase is created only by CapabilityHost and remains valid for
// the duration of the synchronous host call. The dispatcher never stores it.
unsafe impl Send for InvocationDatabase {}

impl InvocationDatabase {
    pub(crate) fn new(database: &BicDb) -> Self {
        Self {
            pointer: NonNull::from(database),
        }
    }

    pub(crate) unsafe fn as_ref<'a>(self) -> &'a BicDb {
        // Safety is required at the call site so this lease cannot accidentally
        // escape the synchronous plugin-service invocation.
        unsafe { self.pointer.as_ptr().as_ref().expect("non-null database") }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PropagatedTransaction {
    pointer: NonNull<Transaction>,
}

// Like InvocationDatabase, this is an invocation-scoped synchronous lease.
unsafe impl Send for PropagatedTransaction {}

impl PropagatedTransaction {
    pub(crate) unsafe fn pointer(self) -> NonNull<Transaction> {
        self.pointer
    }
}

#[derive(Clone)]
pub struct PluginServiceCall {
    pub caller: String,
    /// Effective signed authority of the caller at this hop. This may be a
    /// strict intersection of several package manifests for a nested call.
    pub caller_manifest: Arc<ExtensionManifest>,
    pub dependency: String,
    pub service: String,
    pub method: String,
    pub payload: Value,
    pub actor: ActorContext,
    pub trace: Vec<String>,
    pub deadline_unix_ms: i64,
    pub transaction_requested: bool,
    pub(crate) database: InvocationDatabase,
    pub(crate) transaction: Option<PropagatedTransaction>,
}

impl std::fmt::Debug for PluginServiceCall {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PluginServiceCall")
            .field("caller", &self.caller)
            .field("caller_authority", &self.caller_manifest.identity.name)
            .field("dependency", &self.dependency)
            .field("service", &self.service)
            .field("method", &self.method)
            .field("actor", &self.actor)
            .field("trace", &self.trace)
            .field("deadline_unix_ms", &self.deadline_unix_ms)
            .field("transaction_requested", &self.transaction_requested)
            .finish_non_exhaustive()
    }
}

pub trait RealtimeProvider: Send + Sync {
    fn supports(&self, _kind: bicdb_extension::abi_v2::StreamKind) -> bool {
        false
    }

    fn open(
        &self,
        status: u16,
        headers: &[(String, String)],
        kind: bicdb_extension::abi_v2::StreamKind,
    ) -> Result<u64>;
    fn open_scoped(
        &self,
        status: u16,
        headers: &[(String, String)],
        kind: bicdb_extension::abi_v2::StreamKind,
        _scope: &str,
    ) -> Result<u64> {
        self.open(status, headers, kind)
    }
    fn send(&self, stream: u64, bytes: &[u8]) -> Result<()>;
    fn receive(&self, stream: u64, max_bytes: usize) -> Result<Vec<u8>>;
    fn close(&self, stream: u64, trailers: &[(String, String)]) -> Result<()>;
    fn take_response(&self, _stream: u64, _max_bytes: usize) -> Result<Option<RealtimeResponse>> {
        Ok(None)
    }
    fn register_live_response(
        &self,
        _scope: &str,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<LiveRealtimeResponse>>> {
        Err(AppRuntimeError::Provider(
            "configured realtime provider does not expose live HTTP responses".to_string(),
        ))
    }
    fn cancel_live_response(&self, _scope: &str, _error: AppRuntimeError) {}
}

#[derive(Clone, Debug)]
pub struct RealtimeResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub kind: bicdb_extension::abi_v2::StreamKind,
    pub chunks: Vec<Vec<u8>>,
    pub trailers: Vec<(String, String)>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LiveRealtimeFrame {
    Data(Vec<u8>),
    Trailers(Vec<(String, String)>),
}

#[derive(Debug)]
pub struct LiveRealtimeResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub kind: bicdb_extension::abi_v2::StreamKind,
    pub frames: tokio::sync::mpsc::Receiver<Result<LiveRealtimeFrame>>,
}

#[derive(Clone)]
pub struct InvocationServices {
    pub secrets: Arc<dyn SecretProvider>,
    pub egress: Arc<dyn EgressProvider>,
    pub redis: Arc<dyn RedisProvider>,
    pub email: Arc<dyn EmailProvider>,
    pub grpc: Arc<dyn GrpcProvider>,
    pub tokenizer: Arc<dyn TokenizerProvider>,
    pub embeddings: Arc<dyn EmbeddingsProvider>,
    pub llm: Arc<dyn LlmProvider>,
    pub evaluations: Arc<dyn EvaluationProvider>,
    pub blobs: Arc<dyn BlobProvider>,
    pub observability: Arc<dyn HostObservability>,
    pub services: Arc<dyn PluginServiceDispatcher>,
    pub realtime: Arc<dyn RealtimeProvider>,
    pub max_observability_events: u32,
    pub max_observability_bytes: usize,
    pub max_blob_chunk_bytes: usize,
    pub max_idempotency_entries: usize,
    pub max_route_cache_entries: usize,
    pub max_runtime_cache_entries: usize,
    pub deterministic_clock_ms: Option<Arc<std::sync::atomic::AtomicI64>>,
    pub deterministic_random_seed: Option<Arc<Mutex<u64>>>,
}

impl std::fmt::Debug for InvocationServices {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InvocationServices")
            .field("max_observability_events", &self.max_observability_events)
            .field("max_observability_bytes", &self.max_observability_bytes)
            .field("max_blob_chunk_bytes", &self.max_blob_chunk_bytes)
            .field("max_idempotency_entries", &self.max_idempotency_entries)
            .field("max_route_cache_entries", &self.max_route_cache_entries)
            .field("max_runtime_cache_entries", &self.max_runtime_cache_entries)
            .field(
                "deterministic_clock",
                &self.deterministic_clock_ms.is_some(),
            )
            .field(
                "deterministic_random",
                &self.deterministic_random_seed.is_some(),
            )
            .finish_non_exhaustive()
    }
}

impl InvocationServices {
    pub fn new(
        secrets: Arc<dyn SecretProvider>,
        egress: Arc<dyn EgressProvider>,
        blobs: Arc<dyn BlobProvider>,
        observability: Arc<dyn HostObservability>,
    ) -> Self {
        let denied = Arc::new(DenyServices);
        Self {
            secrets,
            egress,
            redis: Arc::new(DenyRedisProvider),
            email: Arc::new(DenyEmailProvider),
            grpc: Arc::new(DenyGrpcProvider),
            tokenizer: Arc::new(DenyTokenizerProvider),
            embeddings: Arc::new(DenyEmbeddingsProvider),
            llm: Arc::new(DenyLlmProvider),
            evaluations: Arc::new(DenyEvaluationProvider),
            blobs,
            observability,
            services: denied.clone(),
            realtime: denied,
            max_observability_events: 1_000,
            max_observability_bytes: 1024 * 1024,
            max_blob_chunk_bytes: 256 * 1024,
            max_idempotency_entries: 100_000,
            max_route_cache_entries: 100_000,
            max_runtime_cache_entries: 100_000,
            deterministic_clock_ms: None,
            deterministic_random_seed: None,
        }
    }

    pub fn with_realtime(mut self, realtime: Arc<dyn RealtimeProvider>) -> Self {
        self.realtime = realtime;
        self
    }

    pub fn with_redis_provider(mut self, redis: Arc<dyn RedisProvider>) -> Self {
        self.redis = redis;
        self
    }

    pub fn with_email_provider(mut self, email: Arc<dyn EmailProvider>) -> Self {
        self.email = email;
        self
    }

    pub fn with_grpc_provider(mut self, grpc: Arc<dyn GrpcProvider>) -> Self {
        self.grpc = grpc;
        self
    }

    pub fn with_tokenizer_provider(mut self, tokenizer: Arc<dyn TokenizerProvider>) -> Self {
        self.tokenizer = tokenizer;
        self
    }

    pub fn with_embeddings_provider(mut self, embeddings: Arc<dyn EmbeddingsProvider>) -> Self {
        self.embeddings = embeddings;
        self
    }

    pub fn with_llm_provider(mut self, llm: Arc<dyn LlmProvider>) -> Self {
        self.llm = llm;
        self
    }

    pub fn with_evaluation_provider(mut self, evaluations: Arc<dyn EvaluationProvider>) -> Self {
        self.evaluations = evaluations;
        self
    }
}

struct DenyServices;

impl PluginServiceDispatcher for DenyServices {
    fn call(&self, _call: PluginServiceCall) -> Result<Value> {
        Err(AppRuntimeError::CapabilityDenied(
            "plugin service provider is unavailable".to_string(),
        ))
    }
}

impl RealtimeProvider for DenyServices {
    fn open(
        &self,
        _status: u16,
        _headers: &[(String, String)],
        _kind: bicdb_extension::abi_v2::StreamKind,
    ) -> Result<u64> {
        Err(AppRuntimeError::CapabilityDenied(
            "realtime provider is unavailable".to_string(),
        ))
    }
    fn send(&self, _stream: u64, _bytes: &[u8]) -> Result<()> {
        Err(AppRuntimeError::CapabilityDenied(
            "realtime provider is unavailable".to_string(),
        ))
    }
    fn receive(&self, _stream: u64, _max_bytes: usize) -> Result<Vec<u8>> {
        Err(AppRuntimeError::CapabilityDenied(
            "realtime provider is unavailable".to_string(),
        ))
    }
    fn close(&self, _stream: u64, _trailers: &[(String, String)]) -> Result<()> {
        Err(AppRuntimeError::CapabilityDenied(
            "realtime provider is unavailable".to_string(),
        ))
    }
}

#[derive(Clone)]
struct Savepoint {
    transaction: HostHandle,
    write_len: usize,
    lock_len: usize,
    broker_len: usize,
}

#[derive(Clone)]
struct GrantBinding {
    transaction: HostHandle,
    grant: MutationGrantId,
}

#[derive(Clone)]
struct DeliveryBinding {
    queue: String,
    group: String,
    consumer: String,
    message_id: Uuid,
}

struct Upload {
    namespace: String,
    named_key: Option<String>,
    content_type: Option<String>,
    metadata: BTreeMap<String, String>,
    bytes: Vec<u8>,
}

struct BlobRead {
    record: BlobRecord,
    offset: usize,
}

#[derive(Clone)]
struct InvocationRelationScope {
    tenant_field: Option<String>,
    workspace_field: Option<String>,
    tenant_id: Option<String>,
    workspace_id: Option<String>,
    retention_field: Option<String>,
    retention_cutoff_micros: Option<i64>,
}

impl InvocationRelationScope {
    fn allows(&self, record: &Value) -> bool {
        let matches = |field: &Option<String>, expected: &Option<String>| match field {
            Some(field) => expected.as_deref().is_some_and(|expected| {
                record.get(field).and_then(Value::as_str) == Some(expected)
            }),
            None => true,
        };
        let retained = self.retention_field.as_ref().is_none_or(|field| {
            let Some(cutoff) = self.retention_cutoff_micros else {
                return true;
            };
            record
                .get(field)
                .and_then(Value::as_str)
                .and_then(|value| {
                    bicdb_sql::typed_value::PgTimestamp::from_postgres_text(value, true).ok()
                })
                .and_then(bicdb_sql::typed_value::PgTimestamp::finite_micros)
                .is_some_and(|instant| instant >= cutoff)
        });
        matches(&self.tenant_field, &self.tenant_id)
            && matches(&self.workspace_field, &self.workspace_id)
            && retained
    }
}

/// Invocation-local implementation of every ABI-v2 host capability.
pub struct CapabilityHost {
    db: NonNull<BicDb>,
    extension: Arc<ExtensionManifest>,
    actor: ActorContext,
    services: InvocationServices,
    transactions: BTreeMap<HostHandle, Transaction>,
    borrowed_transactions: BTreeMap<HostHandle, NonNull<Transaction>>,
    savepoints: BTreeMap<HostHandle, Savepoint>,
    grants: BTreeMap<HostHandle, GrantBinding>,
    deliveries: BTreeMap<HostHandle, DeliveryBinding>,
    secrets: BTreeMap<HostHandle, SecretRecord>,
    uploads: BTreeMap<HostHandle, Upload>,
    blob_reads: BTreeMap<HostHandle, BlobRead>,
    streams: BTreeMap<HostHandle, u64>,
    llm_response_stream: Option<(String, u64)>,
    service_trace: Vec<String>,
    monotonic_start: Instant,
    observations: u32,
    observation_bytes: usize,
}

// The database pointer is used only synchronously by the invocation thread.
// BicDB transactions are Send and enforce their own commit/record locks.
unsafe impl Send for CapabilityHost {}

impl std::fmt::Debug for CapabilityHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CapabilityHost")
            .field("extension", &self.extension.identity.name)
            .field("trace_id", &self.actor.trace_id)
            .field("transactions", &self.transactions.len())
            .finish_non_exhaustive()
    }
}

impl CapabilityHost {
    pub fn new(
        db: &BicDb,
        extension: Arc<ExtensionManifest>,
        actor: ActorContext,
        services: InvocationServices,
    ) -> Result<Self> {
        let trace = vec![extension.identity.name.to_ascii_lowercase()];
        Self::with_service_trace(db, extension, actor, services, trace)
    }

    /// Constructs an invocation host from an immutable package snapshot that
    /// was already signature- and manifest-validated at activation time.
    pub(crate) fn new_validated(
        db: &BicDb,
        extension: Arc<ExtensionManifest>,
        actor: ActorContext,
        services: InvocationServices,
    ) -> Result<Self> {
        let trace = vec![extension.identity.name.to_ascii_lowercase()];
        Self::with_service_trace_and_transaction_inner(
            db, extension, actor, services, trace, None, false,
        )
        .map(|(host, _)| host)
    }

    pub(crate) fn with_service_trace(
        db: &BicDb,
        extension: Arc<ExtensionManifest>,
        actor: ActorContext,
        services: InvocationServices,
        service_trace: Vec<String>,
    ) -> Result<Self> {
        Self::with_service_trace_and_transaction(
            db,
            extension,
            actor,
            services,
            service_trace,
            None,
        )
        .map(|(host, _)| host)
    }

    pub(crate) fn with_service_trace_and_transaction(
        db: &BicDb,
        extension: Arc<ExtensionManifest>,
        actor: ActorContext,
        services: InvocationServices,
        service_trace: Vec<String>,
        transaction: Option<NonNull<Transaction>>,
    ) -> Result<(Self, Option<HostHandle>)> {
        Self::with_service_trace_and_transaction_inner(
            db,
            extension,
            actor,
            services,
            service_trace,
            transaction,
            true,
        )
    }

    fn with_service_trace_and_transaction_inner(
        db: &BicDb,
        extension: Arc<ExtensionManifest>,
        actor: ActorContext,
        services: InvocationServices,
        service_trace: Vec<String>,
        transaction: Option<NonNull<Transaction>>,
        validate_extension: bool,
    ) -> Result<(Self, Option<HostHandle>)> {
        if validate_extension {
            extension.validate()?;
        }
        actor.validate()?;
        if extension.application.is_none() {
            return Err(AppRuntimeError::InvalidPackage(
                "capability host requires an ABI v2 application manifest".to_string(),
            ));
        }
        let mut host = Self {
            db: NonNull::from(db),
            service_trace,
            extension,
            actor,
            services,
            transactions: BTreeMap::new(),
            borrowed_transactions: BTreeMap::new(),
            savepoints: BTreeMap::new(),
            grants: BTreeMap::new(),
            deliveries: BTreeMap::new(),
            secrets: BTreeMap::new(),
            uploads: BTreeMap::new(),
            blob_reads: BTreeMap::new(),
            streams: BTreeMap::new(),
            llm_response_stream: None,
            monotonic_start: Instant::now(),
            observations: 0,
            observation_bytes: 0,
        };
        let transaction_handle = transaction.map(|transaction| {
            let handle = host.next_transaction_handle();
            host.borrowed_transactions.insert(handle, transaction);
            handle
        });
        if let Some(handle) = transaction_handle {
            for validator in lower_carrier_invariants(host.application())? {
                host.transaction_mut(handle)?
                    .register_commit_validator(validator)?;
            }
        }
        Ok((host, transaction_handle))
    }

    pub(crate) fn application(&self) -> &bicdb_extension::abi_v2::ApplicationManifestV2 {
        self.extension
            .application
            .as_deref()
            .expect("validated ABI v2 manifest")
    }

    /// Trusted immutable identity attached by the host to this invocation.
    pub fn actor(&self) -> &ActorContext {
        &self.actor
    }

    pub(crate) fn replace_actor_deadline_unix_ms(&mut self, deadline_unix_ms: i64) -> i64 {
        std::mem::replace(&mut self.actor.deadline_unix_ms, deadline_unix_ms)
    }

    /// Signed application contract governing this invocation.
    pub fn application_manifest(&self) -> &bicdb_extension::abi_v2::ApplicationManifestV2 {
        self.application()
    }

    pub(crate) fn application_name(&self) -> &str {
        &self.extension.identity.name
    }

    pub(crate) fn begin_idempotency(
        &mut self,
        transaction: HostHandle,
        key: &str,
        request_sha256: &str,
        expires_at_ms: i64,
    ) -> Result<Option<Value>> {
        const RELATION: &str = "__bicdb_app_idempotency";
        let existing = self
            .transaction_mut(transaction)?
            .get_authorized(RELATION, key)?;
        if let Some(existing) = existing {
            let expired = existing
                .metadata
                .get("expires_at_ms")
                .and_then(Value::as_i64)
                .is_some_and(|expiry| expiry <= now_ms());
            if expired {
                self.write_idempotency_record(
                    transaction,
                    key,
                    request_sha256,
                    expires_at_ms,
                    None,
                    MutationOperation::Update,
                )?;
                return Ok(None);
            }
            if existing
                .metadata
                .get("request_sha256")
                .and_then(Value::as_str)
                != Some(request_sha256)
            {
                return Err(AppRuntimeError::IdempotencyKeyReused(
                    "idempotency key was reused with a different request".to_string(),
                ));
            }
            if existing.metadata.get("state").and_then(Value::as_str) == Some("complete") {
                return Ok(existing.metadata.get("response").cloned());
            }
            return Err(AppRuntimeError::Invocation(
                "idempotent operation is already in progress".to_string(),
            ));
        }
        let maximum = self.services.max_idempotency_entries;
        if self
            .transaction_mut(transaction)?
            .scan_collection_authorized(RELATION)?
            .len()
            >= maximum
        {
            return Err(AppRuntimeError::ResourceExhausted(
                "idempotency registry capacity is exhausted".to_string(),
            ));
        }
        self.write_idempotency_record(
            transaction,
            key,
            request_sha256,
            expires_at_ms,
            None,
            MutationOperation::Insert,
        )?;
        Ok(None)
    }

    pub(crate) fn complete_idempotency(
        &mut self,
        transaction: HostHandle,
        key: &str,
        request_sha256: &str,
        expires_at_ms: i64,
        response: Value,
    ) -> Result<()> {
        self.write_idempotency_record(
            transaction,
            key,
            request_sha256,
            expires_at_ms,
            Some(response),
            MutationOperation::Update,
        )
    }

    fn write_idempotency_record(
        &mut self,
        transaction: HostHandle,
        key: &str,
        request_sha256: &str,
        expires_at_ms: i64,
        response: Option<Value>,
        operation: MutationOperation,
    ) -> Result<()> {
        const RELATION: &str = "__bicdb_app_idempotency";
        let state = if response.is_some() {
            "complete"
        } else {
            "pending"
        };
        let record = Record::new(key).with_metadata(json!({
            "state": state,
            "request_sha256": request_sha256,
            "expires_at_ms": expires_at_ms,
            "response": response,
            "application": self.extension.identity.name,
            "tenant_id": self.actor.tenant_id,
            "workspace_id": self.actor.workspace_id,
        }));
        let grant = self
            .transaction_mut(transaction)?
            .issue_mutation_grant(MutationGrantSpec {
                relation: RELATION.to_string(),
                operation,
                record_id: Some(key.to_string()),
                record_id_prefix: None,
                expected_version: None,
                version_field: None,
                allowed_columns: record_columns(&record),
                bulk: false,
                maximum_affected_rows: 1,
                cascade_relations: BTreeSet::new(),
                statement_budget: 1,
                tenant_field: None,
                workspace_field: None,
                audit_metadata: BTreeMap::from([(
                    "kind".to_string(),
                    format!("idempotency-{state}"),
                )]),
            })?;
        match operation {
            MutationOperation::Insert => self
                .transaction_mut(transaction)?
                .insert_with_grant(grant, RELATION, record)?,
            MutationOperation::Update => self
                .transaction_mut(transaction)?
                .update_with_grant(grant, RELATION, record)?,
            _ => unreachable!("idempotency uses insert or update"),
        }
        Ok(())
    }

    pub(crate) fn read_route_cache(
        &mut self,
        transaction: HostHandle,
        key: &str,
        scoped_user_id: Option<&str>,
    ) -> Result<Option<Value>> {
        const RELATION: &str = "__bicdb_app_route_cache";
        let Some(record) = self
            .transaction_mut(transaction)?
            .get_authorized(RELATION, key)?
        else {
            return Ok(None);
        };
        let application = record.metadata.get("application").and_then(Value::as_str);
        let record_user_id = record.metadata.get("user_id").and_then(Value::as_str);
        let tenant_id = record.metadata.get("tenant_id").and_then(Value::as_str);
        let workspace_id = record.metadata.get("workspace_id").and_then(Value::as_str);
        if application != Some(self.extension.identity.name.as_str())
            || record_user_id != scoped_user_id
            || tenant_id != self.actor.tenant_id.as_deref()
            || workspace_id != self.actor.workspace_id.as_deref()
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "route cache entry is outside the signed application/actor boundary".to_string(),
            ));
        }
        if record
            .metadata
            .get("expires_at_ms")
            .and_then(Value::as_i64)
            .is_none_or(|expiry| expiry <= now_ms())
        {
            return Ok(None);
        }
        Ok(record.metadata.get("response").cloned())
    }

    pub(crate) fn write_route_cache(
        &mut self,
        transaction: HostHandle,
        key: &str,
        expires_at_ms: i64,
        response: Value,
        user_id: Option<&str>,
    ) -> Result<()> {
        const RELATION: &str = "__bicdb_app_route_cache";
        let existing = self
            .transaction_mut(transaction)?
            .get_authorized(RELATION, key)?;
        let operation = if existing.is_some() {
            MutationOperation::Update
        } else {
            let records = self
                .transaction_mut(transaction)?
                .scan_collection_authorized(RELATION)?;
            let maximum = self.services.max_route_cache_entries;
            let purge_count = records.len().saturating_add(1).saturating_sub(maximum);
            let expired = records
                .iter()
                .filter(|record| {
                    record
                        .metadata
                        .get("expires_at_ms")
                        .and_then(Value::as_i64)
                        .is_none_or(|expiry| expiry <= now_ms())
                })
                .take(purge_count)
                .map(|record| record.id.clone())
                .collect::<Vec<_>>();
            for expired_key in &expired {
                self.delete_route_cache_record(transaction, expired_key)?;
            }
            if records.len().saturating_sub(expired.len()) >= maximum {
                return Err(AppRuntimeError::ResourceExhausted(
                    "route cache capacity is exhausted".to_string(),
                ));
            }
            MutationOperation::Insert
        };
        let record = Record::new(key).with_metadata(json!({
            "response": response,
            "expires_at_ms": expires_at_ms,
            "application": self.extension.identity.name,
            "user_id": user_id,
            "tenant_id": self.actor.tenant_id,
            "workspace_id": self.actor.workspace_id,
        }));
        let grant = self
            .transaction_mut(transaction)?
            .issue_mutation_grant(MutationGrantSpec {
                relation: RELATION.to_string(),
                operation,
                record_id: Some(key.to_string()),
                record_id_prefix: None,
                expected_version: None,
                version_field: None,
                allowed_columns: record_columns(&record),
                bulk: false,
                maximum_affected_rows: 1,
                cascade_relations: BTreeSet::new(),
                statement_budget: 1,
                tenant_field: None,
                workspace_field: None,
                audit_metadata: BTreeMap::from([(
                    "kind".to_string(),
                    "route-cache-write".to_string(),
                )]),
            })?;
        match operation {
            MutationOperation::Insert => self
                .transaction_mut(transaction)?
                .insert_with_grant(grant, RELATION, record)?,
            MutationOperation::Update => self
                .transaction_mut(transaction)?
                .update_with_grant(grant, RELATION, record)?,
            _ => unreachable!("route cache uses insert or update"),
        }
        Ok(())
    }

    fn delete_route_cache_record(&mut self, transaction: HostHandle, key: &str) -> Result<()> {
        const RELATION: &str = "__bicdb_app_route_cache";
        let grant = self
            .transaction_mut(transaction)?
            .issue_mutation_grant(MutationGrantSpec {
                relation: RELATION.to_string(),
                operation: MutationOperation::Delete,
                record_id: Some(key.to_string()),
                record_id_prefix: None,
                expected_version: None,
                version_field: None,
                allowed_columns: BTreeSet::new(),
                bulk: false,
                maximum_affected_rows: 1,
                cascade_relations: BTreeSet::new(),
                statement_budget: 1,
                tenant_field: None,
                workspace_field: None,
                audit_metadata: BTreeMap::from([(
                    "kind".to_string(),
                    "route-cache-expire".to_string(),
                )]),
            })?;
        self.transaction_mut(transaction)?
            .delete_with_grant(grant, RELATION, key)?;
        Ok(())
    }

    pub(crate) fn read_runtime_cache(
        &mut self,
        transaction: HostHandle,
        key: &str,
    ) -> Result<Option<Value>> {
        const RELATION: &str = "__bicdb_app_runtime_cache";
        let Some(record) = self
            .transaction_mut(transaction)?
            .get_authorized(RELATION, key)?
        else {
            return Ok(None);
        };
        self.validate_runtime_cache_boundary(&record.metadata)?;
        if record
            .metadata
            .get("expires_at_ms")
            .and_then(Value::as_i64)
            .is_some_and(|expiry| expiry <= now_ms())
        {
            return Ok(None);
        }
        Ok(record.metadata.get("value").cloned())
    }

    pub(crate) fn write_runtime_cache(
        &mut self,
        transaction: HostHandle,
        key: &str,
        expires_at_ms: Option<i64>,
        value: Value,
    ) -> Result<()> {
        const RELATION: &str = "__bicdb_app_runtime_cache";
        let existing = self
            .transaction_mut(transaction)?
            .get_authorized(RELATION, key)?;
        if let Some(existing) = &existing {
            self.validate_runtime_cache_boundary(&existing.metadata)?;
        }
        let operation = if existing.is_some() {
            MutationOperation::Update
        } else {
            let records = self
                .transaction_mut(transaction)?
                .scan_collection_authorized(RELATION)?;
            let maximum = self.services.max_runtime_cache_entries;
            let purge_count = records.len().saturating_add(1).saturating_sub(maximum);
            let expired = records
                .iter()
                .filter(|record| {
                    record
                        .metadata
                        .get("expires_at_ms")
                        .and_then(Value::as_i64)
                        .is_some_and(|expiry| expiry <= now_ms())
                })
                .take(purge_count)
                .map(|record| record.id.clone())
                .collect::<Vec<_>>();
            for expired_key in &expired {
                self.delete_runtime_cache_record(transaction, expired_key, "expire")?;
            }
            if records.len().saturating_sub(expired.len()) >= maximum {
                return Err(AppRuntimeError::ResourceExhausted(
                    "runtime cache capacity is exhausted".to_string(),
                ));
            }
            MutationOperation::Insert
        };
        let record = Record::new(key).with_metadata(json!({
            "value": value,
            "expires_at_ms": expires_at_ms,
            "application": self.extension.identity.name,
            "tenant_id": self.actor.tenant_id,
            "workspace_id": self.actor.workspace_id,
        }));
        let grant = self
            .transaction_mut(transaction)?
            .issue_mutation_grant(MutationGrantSpec {
                relation: RELATION.to_string(),
                operation,
                record_id: Some(key.to_string()),
                record_id_prefix: None,
                expected_version: None,
                version_field: None,
                allowed_columns: record_columns(&record),
                bulk: false,
                maximum_affected_rows: 1,
                cascade_relations: BTreeSet::new(),
                statement_budget: 1,
                tenant_field: None,
                workspace_field: None,
                audit_metadata: BTreeMap::from([(
                    "kind".to_string(),
                    "runtime-cache-write".to_string(),
                )]),
            })?;
        match operation {
            MutationOperation::Insert => self
                .transaction_mut(transaction)?
                .insert_with_grant(grant, RELATION, record)?,
            MutationOperation::Update => self
                .transaction_mut(transaction)?
                .update_with_grant(grant, RELATION, record)?,
            _ => unreachable!("runtime cache uses insert or update"),
        }
        Ok(())
    }

    pub(crate) fn delete_runtime_cache(
        &mut self,
        transaction: HostHandle,
        key: &str,
    ) -> Result<bool> {
        const RELATION: &str = "__bicdb_app_runtime_cache";
        let Some(existing) = self
            .transaction_mut(transaction)?
            .get_authorized(RELATION, key)?
        else {
            return Ok(false);
        };
        self.validate_runtime_cache_boundary(&existing.metadata)?;
        self.delete_runtime_cache_record(transaction, key, "delete")?;
        Ok(true)
    }

    fn validate_runtime_cache_boundary(&self, metadata: &Value) -> Result<()> {
        let application = metadata.get("application").and_then(Value::as_str);
        let tenant_id = metadata.get("tenant_id").and_then(Value::as_str);
        let workspace_id = metadata.get("workspace_id").and_then(Value::as_str);
        if application != Some(self.extension.identity.name.as_str())
            || tenant_id != self.actor.tenant_id.as_deref()
            || workspace_id != self.actor.workspace_id.as_deref()
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "runtime cache entry is outside the signed application/tenant/workspace boundary"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn delete_runtime_cache_record(
        &mut self,
        transaction: HostHandle,
        key: &str,
        reason: &str,
    ) -> Result<()> {
        const RELATION: &str = "__bicdb_app_runtime_cache";
        let grant = self
            .transaction_mut(transaction)?
            .issue_mutation_grant(MutationGrantSpec {
                relation: RELATION.to_string(),
                operation: MutationOperation::Delete,
                record_id: Some(key.to_string()),
                record_id_prefix: None,
                expected_version: None,
                version_field: None,
                allowed_columns: BTreeSet::new(),
                bulk: false,
                maximum_affected_rows: 1,
                cascade_relations: BTreeSet::new(),
                statement_budget: 1,
                tenant_field: None,
                workspace_field: None,
                audit_metadata: BTreeMap::from([(
                    "kind".to_string(),
                    format!("runtime-cache-{reason}"),
                )]),
            })?;
        self.transaction_mut(transaction)?
            .delete_with_grant(grant, RELATION, key)?;
        Ok(())
    }

    fn security_storage_key(&self, logical_key: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(b"bicdb-app-security-v1\0");
        digest.update(self.extension.identity.name.as_bytes());
        digest.update(b"\0");
        digest.update(logical_key.as_bytes());
        encode_hex(&digest.finalize())
    }

    /// Read application-global durable security state. The logical key is
    /// hashed with the signed application identity before it reaches BicDB so
    /// one package cannot address another package's auth/session records.
    pub(crate) fn read_security_state(
        &mut self,
        transaction: HostHandle,
        logical_key: &str,
    ) -> Result<Option<Value>> {
        const RELATION: &str = "__bicdb_app_security";
        let key = self.security_storage_key(logical_key);
        let Some(record) = self
            .transaction_mut(transaction)?
            .get_authorized(RELATION, &key)?
        else {
            return Ok(None);
        };
        if record.metadata.get("application").and_then(Value::as_str)
            != Some(self.extension.identity.name.as_str())
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "security state is outside the signed application boundary".to_string(),
            ));
        }
        Ok(record.metadata.get("value").cloned())
    }

    /// Create a security record exactly once. Duplicate logical keys fail at
    /// commit, which supplies the atomic replay/uniqueness boundary used by
    /// registration, OAuth state, magic links, and refresh sessions.
    pub(crate) fn create_security_state(
        &mut self,
        transaction: HostHandle,
        logical_key: &str,
        kind: &str,
        value: Value,
    ) -> Result<()> {
        const RELATION: &str = "__bicdb_app_security";
        let key = self.security_storage_key(logical_key);
        if self
            .transaction_mut(transaction)?
            .get_authorized(RELATION, &key)?
            .is_some()
        {
            return Err(AppRuntimeError::Conflict(
                "security state already exists".to_string(),
            ));
        }
        let record = Record::new(&key).with_metadata(json!({
            "application": self.extension.identity.name,
            "kind": kind,
            "value": value,
            "created_at_ms": now_ms(),
        }));
        let grant = self
            .transaction_mut(transaction)?
            .issue_mutation_grant(MutationGrantSpec {
                relation: RELATION.to_string(),
                operation: MutationOperation::Insert,
                record_id: Some(key),
                record_id_prefix: None,
                expected_version: None,
                version_field: None,
                allowed_columns: record_columns(&record),
                bulk: false,
                maximum_affected_rows: 1,
                cascade_relations: BTreeSet::new(),
                statement_budget: 1,
                tenant_field: None,
                workspace_field: None,
                audit_metadata: BTreeMap::from([(
                    "kind".to_string(),
                    format!("security-{kind}-create"),
                )]),
            })?;
        self.transaction_mut(transaction)?
            .insert_with_grant(grant, RELATION, record)?;
        Ok(())
    }

    /// Atomically consume one-time security state. The deletion participates
    /// in the caller's transaction and therefore survives process restart
    /// without an in-memory replay window.
    pub(crate) fn consume_security_state(
        &mut self,
        transaction: HostHandle,
        logical_key: &str,
        kind: &str,
    ) -> Result<Option<Value>> {
        const RELATION: &str = "__bicdb_app_security";
        let key = self.security_storage_key(logical_key);
        let Some(record) = self
            .transaction_mut(transaction)?
            .get_authorized(RELATION, &key)?
        else {
            return Ok(None);
        };
        if record.metadata.get("application").and_then(Value::as_str)
            != Some(self.extension.identity.name.as_str())
            || record.metadata.get("kind").and_then(Value::as_str) != Some(kind)
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "security state kind or application boundary mismatch".to_string(),
            ));
        }
        let value = record.metadata.get("value").cloned();
        let grant = self
            .transaction_mut(transaction)?
            .issue_mutation_grant(MutationGrantSpec {
                relation: RELATION.to_string(),
                operation: MutationOperation::Delete,
                record_id: Some(key.clone()),
                record_id_prefix: None,
                expected_version: None,
                version_field: None,
                allowed_columns: BTreeSet::new(),
                bulk: false,
                maximum_affected_rows: 1,
                cascade_relations: BTreeSet::new(),
                statement_budget: 1,
                tenant_field: None,
                workspace_field: None,
                audit_metadata: BTreeMap::from([(
                    "kind".to_string(),
                    format!("security-{kind}-consume"),
                )]),
            })?;
        self.transaction_mut(transaction)?
            .delete_with_grant(grant, RELATION, &key)?;
        Ok(value)
    }

    /// Enumerates one signed application's records of a specific security
    /// kind. Storage ids remain opaque and are returned only so the trusted
    /// runtime can revoke matching sessions atomically.
    pub(crate) fn list_security_state(
        &mut self,
        transaction: HostHandle,
        kind: &str,
    ) -> Result<Vec<(String, Value)>> {
        const RELATION: &str = "__bicdb_app_security";
        self.transaction_mut(transaction)?
            .scan_collection_authorized(RELATION)?
            .into_iter()
            .filter(|record| {
                record.metadata.get("application").and_then(Value::as_str)
                    == Some(self.extension.identity.name.as_str())
                    && record.metadata.get("kind").and_then(Value::as_str) == Some(kind)
            })
            .map(|record| {
                let value = record.metadata.get("value").cloned().ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(
                        "durable security record has no value".to_string(),
                    )
                })?;
                Ok((record.id, value))
            })
            .collect()
    }

    pub(crate) fn delete_security_state_record(
        &mut self,
        transaction: HostHandle,
        storage_id: &str,
        kind: &str,
    ) -> Result<bool> {
        const RELATION: &str = "__bicdb_app_security";
        let Some(record) = self
            .transaction_mut(transaction)?
            .get_authorized(RELATION, storage_id)?
        else {
            return Ok(false);
        };
        if record.metadata.get("application").and_then(Value::as_str)
            != Some(self.extension.identity.name.as_str())
            || record.metadata.get("kind").and_then(Value::as_str) != Some(kind)
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "security record revocation crossed a kind or application boundary".to_string(),
            ));
        }
        let grant = self
            .transaction_mut(transaction)?
            .issue_mutation_grant(MutationGrantSpec {
                relation: RELATION.to_string(),
                operation: MutationOperation::Delete,
                record_id: Some(storage_id.to_string()),
                record_id_prefix: None,
                expected_version: None,
                version_field: None,
                allowed_columns: BTreeSet::new(),
                bulk: false,
                maximum_affected_rows: 1,
                cascade_relations: BTreeSet::new(),
                statement_budget: 1,
                tenant_field: None,
                workspace_field: None,
                audit_metadata: BTreeMap::from([(
                    "kind".to_string(),
                    format!("security-{kind}-revoke"),
                )]),
            })?;
        self.transaction_mut(transaction)?
            .delete_with_grant(grant, RELATION, storage_id)?;
        Ok(true)
    }

    pub(crate) fn read_durable_workflow(
        &mut self,
        transaction: HostHandle,
        run_id: &str,
    ) -> Result<Option<Value>> {
        const RELATION: &str = "__bicdb_app_workflows";
        let Some(record) = self
            .transaction_mut(transaction)?
            .get_authorized(RELATION, run_id)?
        else {
            return Ok(None);
        };
        let application = record.metadata.get("application").and_then(Value::as_str);
        let tenant_id = record.metadata.get("tenant_id").and_then(Value::as_str);
        let workspace_id = record.metadata.get("workspace_id").and_then(Value::as_str);
        if application != Some(self.extension.identity.name.as_str())
            || tenant_id != self.actor.tenant_id.as_deref()
            || workspace_id != self.actor.workspace_id.as_deref()
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "workflow run is outside the signed application/tenant/workspace boundary"
                    .to_string(),
            ));
        }
        Ok(Some(record.metadata.clone()))
    }

    pub(crate) fn write_durable_workflow(
        &mut self,
        transaction: HostHandle,
        run_id: &str,
        state: Value,
        expected_revision: Option<u64>,
    ) -> Result<()> {
        const RELATION: &str = "__bicdb_app_workflows";
        if state.get("application").and_then(Value::as_str)
            != Some(self.extension.identity.name.as_str())
            || state.get("tenant_id").and_then(Value::as_str) != self.actor.tenant_id.as_deref()
            || state.get("workspace_id").and_then(Value::as_str)
                != self.actor.workspace_id.as_deref()
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "workflow state cannot cross the signed application/tenant/workspace boundary"
                    .to_string(),
            ));
        }
        let revision = state
            .get("revision")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(
                    "durable workflow state lacks a numeric revision".to_string(),
                )
            })?;
        if expected_revision.is_some_and(|expected| revision != expected.saturating_add(1))
            || expected_revision.is_none() && revision != 1
        {
            return Err(AppRuntimeError::Conflict(
                "durable workflow revision did not advance exactly once".to_string(),
            ));
        }
        let operation = if expected_revision.is_some() {
            MutationOperation::Update
        } else {
            MutationOperation::Insert
        };
        let record = Record::new(run_id).with_metadata(state);
        let grant = self
            .transaction_mut(transaction)?
            .issue_mutation_grant(MutationGrantSpec {
                relation: RELATION.to_string(),
                operation,
                record_id: Some(run_id.to_string()),
                record_id_prefix: None,
                expected_version: expected_revision,
                version_field: Some("revision".to_string()),
                allowed_columns: record_columns(&record),
                bulk: false,
                maximum_affected_rows: 1,
                cascade_relations: BTreeSet::new(),
                statement_budget: 1,
                tenant_field: None,
                workspace_field: None,
                audit_metadata: BTreeMap::from([(
                    "kind".to_string(),
                    if operation == MutationOperation::Insert {
                        "workflow-insert".to_string()
                    } else {
                        "workflow-update".to_string()
                    },
                )]),
            })?;
        match operation {
            MutationOperation::Insert => self
                .transaction_mut(transaction)?
                .insert_with_grant(grant, RELATION, record)?,
            MutationOperation::Update => self
                .transaction_mut(transaction)?
                .update_with_grant(grant, RELATION, record)?,
            _ => unreachable!("workflow state uses insert/update"),
        }
        Ok(())
    }

    pub(crate) fn register_durable_audit(
        &mut self,
        transaction: HostHandle,
        action: String,
        subject: String,
        fields: BTreeMap<String, Value>,
    ) -> Result<()> {
        const RELATION: &str = "__bicdb_app_audit";
        let record = Record::new(Uuid::new_v4().to_string()).with_metadata(json!({
            "kind": "audit",
            "action": action,
            "subject": subject,
            "fields": fields,
            "actor_id": self.actor.user_id.as_ref().or(self.actor.service_id.as_ref()),
            "tenant_id": self.actor.tenant_id,
            "workspace_id": self.actor.workspace_id,
            "plugin": self.extension.identity.name,
            "trace_id": self.actor.trace_id,
            "correlation_id": self.actor.correlation_id,
            "causation_id": self.actor.causation_id,
            "created_at_ms": now_ms(),
        }));
        let grant = self
            .transaction_mut(transaction)?
            .issue_mutation_grant(MutationGrantSpec {
                relation: RELATION.to_string(),
                operation: MutationOperation::Insert,
                record_id: Some(record.id.clone()),
                record_id_prefix: None,
                expected_version: None,
                version_field: None,
                allowed_columns: record_columns(&record),
                bulk: false,
                maximum_affected_rows: 1,
                cascade_relations: BTreeSet::new(),
                statement_budget: 1,
                tenant_field: None,
                workspace_field: None,
                audit_metadata: BTreeMap::from([(
                    "kind".to_string(),
                    "resource-audit".to_string(),
                )]),
            })?;
        self.transaction_mut(transaction)?
            .insert_with_grant(grant, RELATION, record)?;
        self.mirror_audit_to_application_log(transaction, &action, &subject, &fields)?;
        Ok(())
    }

    /// Mirrors a durable audit record into the application's own
    /// `carrier_audit_log` table when the signed manifest declares that
    /// resource, matching what the other BicDB application runtimes write there: the
    /// audit-trail reports in the application read this table.
    fn mirror_audit_to_application_log(
        &mut self,
        transaction: HostHandle,
        action: &str,
        subject: &str,
        fields: &BTreeMap<String, Value>,
    ) -> Result<()> {
        const RELATION: &str = "carrier_audit_log";
        if !self
            .application()
            .resources
            .iter()
            .any(|contract| contract.relation.eq_ignore_ascii_case(RELATION))
        {
            return Ok(());
        }
        let (entity, entity_id) = subject.split_once(':').unwrap_or((subject, ""));
        let now = now_ms();
        let created_at = chrono::DateTime::from_timestamp_millis(now)
            .unwrap_or_default()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let mut digest = Sha256::new();
        digest.update(format!("{action}:{entity}:{entity_id}:{now}").as_bytes());
        let row_hash = format!("{:x}", digest.finalize());
        let mut current_user = serde_json::Map::new();
        if let Some(user_id) = self
            .actor
            .user_id
            .as_ref()
            .or(self.actor.service_id.as_ref())
        {
            current_user.insert("sub".to_string(), Value::String(user_id.clone()));
        }
        for claim in ["email", "name"] {
            if let Some(value) = self.actor.policy_attributes.get(claim) {
                current_user.insert(claim.to_string(), Value::String(value.clone()));
            }
        }
        if !self.actor.roles.is_empty() {
            current_user.insert(
                "roles".to_string(),
                Value::Array(
                    self.actor
                        .roles
                        .iter()
                        .map(|role| Value::String(role.clone()))
                        .collect(),
                ),
            );
        }
        let record_id = Uuid::new_v4().to_string();
        let record = Record::new(record_id.clone()).with_metadata(json!({
            "id": record_id,
            "created_at": created_at,
            "action": action,
            "entity": entity,
            "entity_id": entity_id,
            "current_user": Value::Object(current_user),
            "metadata": fields,
            "row_hash": row_hash,
        }));
        let grant = self
            .transaction_mut(transaction)?
            .issue_mutation_grant(MutationGrantSpec {
                relation: RELATION.to_string(),
                operation: MutationOperation::Insert,
                record_id: Some(record.id.clone()),
                record_id_prefix: None,
                expected_version: None,
                version_field: None,
                allowed_columns: record_columns(&record),
                bulk: false,
                maximum_affected_rows: 1,
                cascade_relations: BTreeSet::new(),
                statement_budget: 1,
                tenant_field: None,
                workspace_field: None,
                audit_metadata: BTreeMap::from([(
                    "kind".to_string(),
                    "resource-audit".to_string(),
                )]),
            })?;
        self.transaction_mut(transaction)?
            .insert_with_grant(grant, RELATION, record)?;
        Ok(())
    }

    pub(crate) fn publish_resource_event_on_commit(
        &mut self,
        transaction: HostHandle,
        queue: &str,
        payload: Value,
        idempotency_key: Option<String>,
        resource: &str,
        action: &str,
        schema_version: u64,
        contract_version: u32,
        event_schema_version: u32,
    ) -> Result<()> {
        self.require_capability(ExtensionCapability::QueueEvents)?;
        if !self.extension.permissions.publish_queues.contains(queue) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "publish queue `{queue}` is undeclared"
            )));
        }
        let transaction_id = self.transaction_mut(transaction)?.id().0;
        let mut headers =
            event_headers(&self.actor, &self.extension.identity.name, BTreeMap::new());
        let trusted = headers
            .get_mut("bicdb_application")
            .and_then(Value::as_object_mut)
            .expect("host event envelope is an object");
        for (name, value) in [
            ("originating_resource", resource.to_string()),
            ("originating_action", action.to_string()),
            ("schema_version", schema_version.to_string()),
            ("contract_version", contract_version.to_string()),
            ("event_schema_version", event_schema_version.to_string()),
            ("transaction_id", transaction_id.to_string()),
        ] {
            trusted.insert(name.to_string(), Value::String(value));
        }
        self.transaction_mut(transaction)?.buffer_broker_publish(
            queue,
            payload,
            PublishOptions {
                headers,
                idempotency_key,
                delay_ms: None,
                max_attempts: None,
            },
        )?;
        Ok(())
    }

    fn ensure_deadline(&self) -> Result<()> {
        if now_ms() >= self.actor.deadline_unix_ms {
            Err(AppRuntimeError::Timeout(
                "invocation deadline exceeded".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn require_capability(&self, capability: ExtensionCapability) -> Result<()> {
        if self.extension.capabilities.contains(&capability) {
            Ok(())
        } else {
            Err(AppRuntimeError::CapabilityDenied(format!(
                "extension `{}` did not declare `{}`",
                self.extension.identity.name,
                capability.as_str()
            )))
        }
    }

    fn permission(
        &self,
        relation: &str,
        action: DatabaseAction,
        columns: impl IntoIterator<Item = String>,
        write: bool,
    ) -> Result<()> {
        self.require_capability(ExtensionCapability::Database)?;
        let permission = self.application().permission_for(relation).ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(format!("relation `{relation}` is undeclared"))
        })?;
        if !permission.actions.contains(&action) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "operation {action:?} is undeclared for `{relation}`"
            )));
        }
        let allowed = if write {
            &permission.writable_columns
        } else {
            &permission.readable_columns
        };
        let exact = self
            .application()
            .required_features
            .contains(&ApplicationFeature::ExactColumnAuthority);
        for column in columns {
            if (exact || !allowed.is_empty()) && !allowed.contains(&column) {
                return Err(AppRuntimeError::CapabilityDenied(format!(
                    "column `{relation}.{column}` is undeclared"
                )));
            }
        }
        Ok(())
    }

    /// Resolve an omitted projection to the columns explicitly signed for the
    /// relation.  ABI-v2 originally used an empty readable set as the legacy
    /// wildcard, so that representation remains unrestricted; once a package
    /// supplies a non-empty set, however, an empty query projection means
    /// "all authorized columns", never "all stored columns".
    fn readable_projection(&self, relation: &str, requested: &[String]) -> Vec<String> {
        if !requested.is_empty() {
            return requested.to_vec();
        }
        self.application()
            .permission_for(relation)
            .filter(|permission| !permission.readable_columns.is_empty())
            .map(|permission| permission.readable_columns.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn project_readable(&self, relation: &str, mut value: Value) -> Value {
        let columns = self.readable_projection(relation, &[]);
        if columns.is_empty()
            && !self
                .application()
                .required_features
                .contains(&ApplicationFeature::ExactColumnAuthority)
        {
            return value;
        }
        if let Some(object) = value.as_object_mut() {
            // Preserve only the score fields synthesized by this host. Stored
            // relation fields may also begin with an underscore and must not
            // escape the package's signed projection.
            object.retain(|name, _| {
                columns.iter().any(|column| column == name)
                    || matches!(name.as_str(), "_score" | "_vector_score" | "_text_score")
            });
        }
        value
    }

    fn relation_scope(&self, relation: &str) -> InvocationRelationScope {
        let contract = self
            .application()
            .resources
            .iter()
            .find(|contract| contract.relation.eq_ignore_ascii_case(relation));
        InvocationRelationScope {
            tenant_field: contract.and_then(|contract| contract.tenant_field.clone()),
            workspace_field: contract.and_then(|contract| contract.workspace_field.clone()),
            tenant_id: self.actor.tenant_id.clone(),
            workspace_id: self.actor.workspace_id.clone(),
            retention_field: contract
                .and_then(|contract| contract.timeseries.as_ref())
                .and_then(|timeseries| {
                    timeseries
                        .retention
                        .as_ref()
                        .map(|_| timeseries.time_field.clone())
                }),
            retention_cutoff_micros: contract
                .and_then(|contract| contract.timeseries.as_ref())
                .and_then(|timeseries| timeseries.retention.as_deref())
                .and_then(carrier_retention_cutoff),
        }
    }

    fn vector_search_contract(&self, relation: &str) -> Result<(String, ResourceVectorSearchV1)> {
        self.application()
            .resources
            .iter()
            .find(|contract| contract.relation.eq_ignore_ascii_case(relation))
            .and_then(|contract| {
                contract
                    .vector_search
                    .clone()
                    .map(|vector| (contract.name.clone(), vector))
            })
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "relation `{relation}` has no signed vector-search contract"
                ))
            })
    }

    fn vector_field(&self, relation: &str) -> Option<String> {
        self.application()
            .resources
            .iter()
            .find(|contract| contract.relation.eq_ignore_ascii_case(relation))
            .and_then(|contract| {
                contract
                    .vector_search
                    .as_ref()
                    .map(|vector| vector.field.clone())
            })
    }

    fn spatial_field_contract(
        &self,
        relation: &str,
        field: &str,
        expected: ApplicationGeometryTypeV1,
    ) -> Result<()> {
        let resource = self
            .application()
            .resources
            .iter()
            .find(|contract| contract.relation.eq_ignore_ascii_case(relation))
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "relation `{relation}` has no signed resource contract"
                ))
            })?;
        let valid = resource.fields.iter().any(|candidate| {
            candidate.name == field
                && matches!(
                    candidate.field_type,
                    bicdb_extension::abi_v2::FieldType::Geometry {
                        geometry_type: Some(actual),
                        ..
                    } if actual == expected
                )
        });
        if !valid {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "relation `{relation}` field `{field}` is not signed as {expected:?}"
            )));
        }
        Ok(())
    }

    fn timeseries_contract(&self, relation: &str) -> Result<ResourceTimeseriesV1> {
        self.application()
            .resources
            .iter()
            .find(|contract| contract.relation.eq_ignore_ascii_case(relation))
            .and_then(|contract| contract.timeseries.clone())
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "relation `{relation}` has no signed timeseries contract"
                ))
            })
    }

    fn transaction_mut(&mut self, handle: HostHandle) -> Result<&mut Transaction> {
        if let Some(transaction) = self.transactions.get_mut(&handle) {
            return Ok(transaction);
        }
        self.borrowed_transactions
            .get(&handle)
            .copied()
            .map(|mut transaction| {
                // Safety: propagated transactions are valid only while the
                // synchronous parent service call is on the stack.
                unsafe { transaction.as_mut() }
            })
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "forged, expired, or cross-invocation transaction handle".to_string(),
                )
            })
    }

    pub(crate) fn transaction_isolation(
        &mut self,
        handle: HostHandle,
    ) -> Result<TransactionIsolation> {
        Ok(self.transaction_mut(handle)?.isolation())
    }

    fn grant(&self, transaction: HostHandle, handle: HostHandle) -> Result<MutationGrantId> {
        self.grants
            .get(&handle)
            .filter(|binding| binding.transaction == transaction)
            .map(|binding| binding.grant)
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "forged, expired, or cross-transaction mutation grant".to_string(),
                )
            })
    }

    fn next_handle<T>(&self, occupied: &BTreeMap<HostHandle, T>) -> HostHandle {
        loop {
            let bytes = Uuid::new_v4().into_bytes();
            let handle = HostHandle(u64::from_le_bytes(bytes[..8].try_into().unwrap()).max(1));
            if !occupied.contains_key(&handle) {
                return handle;
            }
        }
    }

    fn next_transaction_handle(&self) -> HostHandle {
        loop {
            let bytes = Uuid::new_v4().into_bytes();
            let handle = HostHandle(u64::from_le_bytes(bytes[..8].try_into().unwrap()).max(1));
            if !self.transactions.contains_key(&handle)
                && !self.borrowed_transactions.contains_key(&handle)
            {
                return handle;
            }
        }
    }

    fn dispatch(&mut self, request: HostRequest) -> Result<HostValue> {
        self.ensure_deadline()?;
        match request {
            HostRequest::Transaction(request) => self.transaction(request),
            HostRequest::Database(request) => self.database(request),
            HostRequest::MutationGrant(request) => self.mutation_grant(request),
            HostRequest::Broker(request) => self.broker(request),
            HostRequest::Service(request) => self.service(request),
            HostRequest::Clock(request) => self.clock(request),
            HostRequest::Random(request) => self.random(request),
            HostRequest::Secret(request) => self.secret(request),
            HostRequest::Crypto(request) => self.crypto(request),
            HostRequest::Egress(request) => self.egress(request),
            HostRequest::Grpc(request) => self.grpc(request),
            HostRequest::Tokenizer(request) => self.tokenizer(request),
            HostRequest::Embeddings(request) => self.embeddings(request),
            HostRequest::Llm(request) => self.llm(request),
            HostRequest::Redis(request) => self.redis(request),
            HostRequest::Email(request) => self.email(request),
            HostRequest::Blob(request) => self.blob(request),
            HostRequest::Stream(request) => self.stream(request),
            HostRequest::Observe(request) => self.observe(request),
        }
    }

    fn transaction(
        &mut self,
        request: bicdb_extension::abi_v2::TransactionRequest,
    ) -> Result<HostValue> {
        use bicdb_extension::abi_v2::TransactionRequest as Request;
        self.require_capability(ExtensionCapability::Transactions)?;
        match request {
            Request::Begin { isolation } => {
                let isolation = match isolation {
                    IsolationLevel::ReadCommitted => TransactionIsolation::ReadCommitted,
                    IsolationLevel::RepeatableRead => TransactionIsolation::RepeatableRead,
                    IsolationLevel::Serializable => TransactionIsolation::Serializable,
                };
                let actor = MutationActor {
                    actor_id: self
                        .actor
                        .user_id
                        .clone()
                        .or_else(|| self.actor.service_id.clone())
                        .ok_or_else(|| {
                            AppRuntimeError::CapabilityDenied(
                                "actor has no user or service id".to_string(),
                            )
                        })?,
                    roles: self.actor.roles.clone(),
                    scopes: self.actor.scopes.clone(),
                    tenant_id: self.actor.tenant_id.clone(),
                    workspace_id: self.actor.workspace_id.clone(),
                    originating_plugin: self.extension.identity.name.clone(),
                    originating_resource: None,
                    originating_action: None,
                    trace_id: self.actor.trace_id.clone(),
                    deadline_unix_ms: self.actor.deadline_unix_ms,
                };
                // Safety: CapabilityHost is created from a live shared BicDB
                // session and never outlives the synchronous invocation.
                let mut transaction = unsafe { self.db.as_ref() }
                    .begin_application_transaction_with_isolation(actor, isolation)?;
                for validator in lower_carrier_invariants(self.application())? {
                    transaction.register_commit_validator(validator)?;
                }
                let handle = self.next_transaction_handle();
                self.transactions.insert(handle, transaction);
                Ok(HostValue::Handle(handle))
            }
            Request::Commit { transaction } => {
                let transaction = self.transactions.remove(&transaction).ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied(
                        "forged or expired transaction handle".to_string(),
                    )
                })?;
                self.remove_transaction_resources(transaction.id().0);
                transaction.commit()?;
                Ok(HostValue::Unit)
            }
            Request::Rollback { transaction } => {
                let transaction = self.transactions.remove(&transaction).ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied(
                        "forged or expired transaction handle".to_string(),
                    )
                })?;
                self.remove_transaction_resources(transaction.id().0);
                transaction.rollback()?;
                Ok(HostValue::Unit)
            }
            Request::Savepoint { transaction, name } => {
                if name.is_empty() || name.len() > 128 {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "savepoint name must contain 1..=128 bytes".to_string(),
                    ));
                }
                let tx = self.transaction_mut(transaction)?;
                let savepoint = Savepoint {
                    transaction,
                    write_len: tx.write_len(),
                    lock_len: tx.lock_len(),
                    broker_len: tx.broker_publish_len(),
                };
                let handle = self.next_handle(&self.savepoints);
                self.savepoints.insert(handle, savepoint);
                Ok(HostValue::Handle(handle))
            }
            Request::RollbackTo {
                transaction,
                savepoint,
            } => {
                let savepoint = self
                    .savepoints
                    .get(&savepoint)
                    .filter(|savepoint| savepoint.transaction == transaction)
                    .cloned()
                    .ok_or_else(|| {
                        AppRuntimeError::CapabilityDenied(
                            "forged or cross-transaction savepoint".to_string(),
                        )
                    })?;
                let tx = self.transaction_mut(transaction)?;
                tx.truncate_writes_and_locks(savepoint.write_len, savepoint.lock_len)?;
                tx.truncate_broker_publishes(savepoint.broker_len);
                Ok(HostValue::Unit)
            }
            Request::Release {
                transaction,
                savepoint,
            } => {
                if self
                    .savepoints
                    .get(&savepoint)
                    .is_none_or(|savepoint| savepoint.transaction != transaction)
                {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "forged or cross-transaction savepoint".to_string(),
                    ));
                }
                self.savepoints.remove(&savepoint);
                Ok(HostValue::Unit)
            }
            Request::RegisterCommitValidation {
                transaction,
                validator,
            } => {
                let validator = lower_validator(validator)?;
                self.transaction_mut(transaction)?
                    .register_commit_validator(validator)?;
                Ok(HostValue::Unit)
            }
        }
    }

    fn remove_transaction_resources(&mut self, _transaction_id: u64) {
        // Handles are bound by the public transaction handle, which has already
        // been removed. Retaining their tiny map entries would still allow
        // probing, so invalidate every handle whose transaction disappeared.
        let active = self.transactions.keys().copied().collect::<BTreeSet<_>>();
        self.savepoints
            .retain(|_, savepoint| active.contains(&savepoint.transaction));
        self.grants
            .retain(|_, grant| active.contains(&grant.transaction));
    }

    fn mutation_grant(
        &mut self,
        request: bicdb_extension::abi_v2::MutationGrantRequest,
    ) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::Database)?;
        self.require_capability(ExtensionCapability::Transactions)?;
        let contract = self
            .application()
            .resources
            .iter()
            .find(|contract| contract.name == request.resource)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "resource `{}` is undeclared",
                    request.resource
                ))
            })?;
        let mut create_columns = contract.create_fields.clone();
        create_columns.extend(contract.server_managed_fields.iter().cloned());
        create_columns.insert(contract.primary_key.clone());
        create_columns.extend(
            contract
                .fields
                .iter()
                .filter(|field| field.generated || field.default_json.is_some())
                .map(|field| field.name.clone()),
        );
        for managed in [
            contract.tenant_field.as_ref(),
            contract.workspace_field.as_ref(),
            contract.soft_delete_field.as_ref(),
            contract.version_field.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            create_columns.insert(managed.clone());
        }
        let mut update_columns = contract.update_fields.clone();
        update_columns.extend(contract.server_managed_fields.iter().cloned());
        update_columns.extend(
            contract
                .fields
                .iter()
                .filter(|field| field.generated)
                .map(|field| field.name.clone()),
        );
        for managed in [
            contract.soft_delete_field.as_ref(),
            contract.version_field.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            update_columns.insert(managed.clone());
        }
        let upsert_is_update = if request.operation == ResourceOperation::Upsert {
            let id = request.record_id.as_deref().ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "upsert MutationGrant requires a record ID".to_string(),
                )
            })?;
            let scope = self.relation_scope(&request.relation);
            let vector_field = contract
                .vector_search
                .as_ref()
                .map(|vector| vector.field.as_str());
            self.transaction_mut(request.transaction)?
                .get_authorized(&request.relation, id)?
                .as_deref()
                .map(|record| resource_record_json(record, vector_field))
                .is_some_and(|record| scope.allows(&record))
        } else {
            false
        };
        let columns_allowed = match request.operation {
            ResourceOperation::Create => request.columns.is_subset(&create_columns),
            ResourceOperation::Upsert if upsert_is_update => {
                request.columns.is_subset(&update_columns)
            }
            ResourceOperation::Upsert => request.columns.is_subset(&create_columns),
            ResourceOperation::Update | ResourceOperation::Restore => {
                request.columns.is_subset(&update_columns)
            }
            ResourceOperation::Delete if contract.soft_delete_field.is_some() => {
                request.columns.is_subset(&update_columns)
            }
            ResourceOperation::Delete => request.columns.is_empty(),
            _ => request.columns.is_subset(&contract.update_fields),
        };
        if contract.relation != request.relation
            || !contract.operations.contains(&request.operation)
            || !columns_allowed
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "MutationGrant request exceeds the signed resource contract".to_string(),
            ));
        }
        if request.predicate.is_some() {
            return Err(AppRuntimeError::CapabilityDenied(
                "predicate MutationGrants require a native predicate adapter and are unavailable"
                    .to_string(),
            ));
        }
        if request.max_rows == 0
            || request.statement_budget == 0
            || (!request.bulk && request.max_rows != 1)
            || request.max_rows > 10_000
            || request.statement_budget > 10_000
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "MutationGrant row or statement budget exceeds host limits".to_string(),
            ));
        }
        let operation = match request.operation {
            ResourceOperation::Create => MutationOperation::Insert,
            ResourceOperation::Upsert if upsert_is_update => MutationOperation::Update,
            ResourceOperation::Upsert => MutationOperation::Insert,
            ResourceOperation::Update => MutationOperation::Update,
            ResourceOperation::Delete if contract.soft_delete_field.is_some() => {
                MutationOperation::Update
            }
            ResourceOperation::Delete => MutationOperation::Delete,
            ResourceOperation::Restore => MutationOperation::Restore,
            ResourceOperation::Action if request.bulk => MutationOperation::Bulk,
            ResourceOperation::Action => MutationOperation::Update,
            ResourceOperation::List | ResourceOperation::Get => {
                return Err(AppRuntimeError::CapabilityDenied(
                    "read operations cannot request MutationGrants".to_string(),
                ))
            }
        };
        let mut allowed_columns = request.columns;
        if contract.vector_search.as_ref().is_some_and(|vector| {
            vector.field != "vector" && allowed_columns.contains(&vector.field)
        }) {
            allowed_columns.insert("vector".to_string());
        }
        let spec = MutationGrantSpec {
            relation: request.relation,
            operation,
            record_id: request.record_id,
            record_id_prefix: None,
            expected_version: request.expected_version,
            version_field: contract.version_field.clone(),
            allowed_columns,
            bulk: request.bulk,
            maximum_affected_rows: request.max_rows,
            cascade_relations: BTreeSet::new(),
            statement_budget: request.statement_budget,
            tenant_field: contract.tenant_field.clone(),
            workspace_field: contract.workspace_field.clone(),
            audit_metadata: request.audit_metadata,
        };
        let transaction = request.transaction;
        let grant = self
            .transaction_mut(transaction)?
            .issue_mutation_grant(spec)?;
        let handle = self.next_handle(&self.grants);
        self.grants
            .insert(handle, GrantBinding { transaction, grant });
        Ok(HostValue::Handle(handle))
    }

    fn execute_declared_sql(
        &mut self,
        transaction: HostHandle,
        statement: &RawSqlDeclaration,
        parameters: Vec<Value>,
    ) -> Result<HostValue> {
        if parameters.len() != statement.parameters.len() {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "raw SQL `{}` expects {} parameters, found {}",
                statement.id,
                statement.parameters.len(),
                parameters.len()
            )));
        }
        for relation in &statement.relations {
            let contract = self
                .application()
                .resources
                .iter()
                .find(|contract| contract.relation.eq_ignore_ascii_case(relation));
            for action in statement.actions.iter().filter(|action| {
                !matches!(action, DatabaseAction::Execute | DatabaseAction::RawSql)
            }) {
                if let Some(contract) = contract {
                    self.enforce_declared_sql_role_policy(contract, *action)?;
                }
                self.permission(
                    relation,
                    *action,
                    std::iter::empty(),
                    matches!(
                        action,
                        DatabaseAction::Insert
                            | DatabaseAction::Update
                            | DatabaseAction::Delete
                            | DatabaseAction::Upsert
                    ),
                )?;
            }
        }
        self.require_capability(ExtensionCapability::Transactions)?;
        let parameters = parameters
            .into_iter()
            .zip(&statement.parameters)
            .enumerate()
            .map(|(index, (value, field_type))| {
                raw_sql_parameter(value, field_type).map_err(|error| {
                    AppRuntimeError::InvalidRequest(format!(
                        "raw SQL `{}` parameter ${} is invalid: {error}",
                        statement.id,
                        index + 1
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut signed_mutation_grants = BTreeMap::new();
        if statement.actions.iter().any(|action| {
            matches!(
                action,
                DatabaseAction::Insert
                    | DatabaseAction::Update
                    | DatabaseAction::Delete
                    | DatabaseAction::Upsert
            )
        }) {
            for relation in &statement.relations {
                let permission = self
                    .application()
                    .permission_for(relation)
                    .expect("declared SQL relation permission was validated")
                    .clone();
                let contract = self
                    .application()
                    .resources
                    .iter()
                    .find(|contract| contract.relation.eq_ignore_ascii_case(relation))
                    .map(|contract| {
                        (
                            contract.version_field.clone(),
                            contract.tenant_field.clone(),
                            contract.workspace_field.clone(),
                        )
                    })
                    .unwrap_or_default();
                let grant =
                    self.transaction_mut(transaction)?
                        .issue_mutation_grant(MutationGrantSpec {
                            relation: relation.clone(),
                            operation: MutationOperation::Bulk,
                            record_id: None,
                            record_id_prefix: None,
                            expected_version: None,
                            version_field: contract.0,
                            allowed_columns: permission.writable_columns,
                            bulk: true,
                            maximum_affected_rows: u64::from(statement.max_affected_rows),
                            cascade_relations: BTreeSet::new(),
                            statement_budget: statement.max_affected_rows,
                            tenant_field: contract.1,
                            workspace_field: contract.2,
                            audit_metadata: BTreeMap::from([
                                ("kind".to_string(), "signed-raw-sql".to_string()),
                                ("statement_id".to_string(), statement.id.clone()),
                            ]),
                        })?;
                signed_mutation_grants.insert(relation.clone(), grant);
            }
        }

        // SqlSession owns a transaction while it executes. Replace the host's
        // slot with a short-lived pending transaction, run the signed SQL with
        // the real transaction, then restore it before returning. This keeps
        // raw SQL, model calls, nested services, and publish-on-commit on one
        // atomic transaction even when the handle was borrowed from a caller.
        let database_pointer = self.db;
        let database = unsafe {
            // Safety: CapabilityHost is invocation-local and the database
            // outlives every synchronous host call.
            database_pointer.as_ref()
        };
        let placeholder = database.begin_transaction()?;
        let active = std::mem::replace(self.transaction_mut(transaction)?, placeholder);
        let security = raw_sql_security_context(&self.actor);
        let mut session = SqlSession::new_shared_secure(database, security)
            .with_deferred_commit()
            .with_pending_transaction(active)
            .with_positional_parameters(parameters)
            .with_signed_application_security_guc_compatibility()
            .with_signed_mutation_grants(signed_mutation_grants);
        let execution = session.execute(&statement.sql);
        let restored = session.take_pending_transaction().ok_or_else(|| {
            AppRuntimeError::Invocation(format!(
                "raw SQL `{}` consumed its owning transaction",
                statement.id
            ))
        })?;
        let placeholder = std::mem::replace(self.transaction_mut(transaction)?, restored);
        placeholder.rollback()?;
        let result = execution.map_err(|error| {
            AppRuntimeError::Invocation(format!(
                "raw SQL `{}` failed (SQLSTATE {}): {error}",
                statement.id,
                error.sqlstate()
            ))
        })?;
        raw_sql_result(statement, result)
    }

    fn enforce_declared_sql_role_policy(
        &self,
        contract: &ResourceContractV1,
        action: DatabaseAction,
    ) -> Result<()> {
        let Some(policy) = &contract.policy else {
            return Ok(());
        };
        // A signed native projection is installed as FORCE RLS on this exact
        // relation. Let the SQL engine evaluate row expressions, correlated
        // EXISTS, old-row USING, and candidate-row WITH CHECK; a coarse host
        // role precheck would reject valid active rows and cannot be exact.
        if policy.sql.is_some() {
            return Ok(());
        }
        if policy.tenant_expression.is_some() {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "raw SQL cannot enforce expression-based policy for resource `{}`",
                contract.name
            )));
        }
        let allows = |rule: Option<&ResourcePolicyRuleV1>| -> Result<bool> {
            let Some(rule) = rule else {
                return Ok(false);
            };
            if rule.expression.is_some() {
                return Err(AppRuntimeError::CapabilityDenied(format!(
                    "raw SQL cannot enforce row-expression policy for resource `{}`",
                    contract.name
                )));
            }
            Ok(if rule.roles.is_empty() {
                true
            } else {
                match rule.role_match {
                    ResourcePolicyRoleMatchV1::Any => !rule.roles.is_disjoint(&self.actor.roles),
                    ResourcePolicyRoleMatchV1::All => rule.roles.is_subset(&self.actor.roles),
                }
            })
        };
        let allowed = match action {
            DatabaseAction::Select | DatabaseAction::Aggregate => {
                allows(policy.read.as_ref())?
                    && (contract.soft_delete_field.is_none()
                        || allows(policy.deleted_read.as_ref())?)
            }
            DatabaseAction::Insert
            | DatabaseAction::Update
            | DatabaseAction::Delete
            | DatabaseAction::Upsert => allows(policy.write.as_ref())?,
            _ => true,
        };
        if allowed {
            Ok(())
        } else {
            Err(AppRuntimeError::CapabilityDenied(format!(
                "actor is denied by resource `{}` raw SQL policy",
                contract.name
            )))
        }
    }

    fn record_database_read_dependencies(&mut self, request: &DatabaseRequest) -> Result<()> {
        let (transaction, relations) = match request {
            DatabaseRequest::SelectPrimaryKey {
                transaction,
                relation,
                ..
            }
            | DatabaseRequest::Insert {
                transaction,
                relation,
                ..
            }
            | DatabaseRequest::Update {
                transaction,
                relation,
                ..
            }
            | DatabaseRequest::Delete {
                transaction,
                relation,
                ..
            }
            | DatabaseRequest::Upsert {
                transaction,
                relation,
                ..
            }
            | DatabaseRequest::Aggregate {
                transaction,
                relation,
                ..
            }
            | DatabaseRequest::FullTextSearch {
                transaction,
                relation,
                ..
            }
            | DatabaseRequest::VectorSearch {
                transaction,
                relation,
                ..
            }
            | DatabaseRequest::HybridSearch {
                transaction,
                relation,
                ..
            }
            | DatabaseRequest::Spatial {
                transaction,
                relation,
                ..
            }
            | DatabaseRequest::Recent {
                transaction,
                relation,
                ..
            }
            | DatabaseRequest::JsonPath {
                transaction,
                relation,
                ..
            }
            | DatabaseRequest::LockRows {
                transaction,
                relation,
                ..
            } => (*transaction, vec![relation.clone()]),
            DatabaseRequest::Query { transaction, query } => {
                (*transaction, vec![query.relation.clone()])
            }
            DatabaseRequest::RelationQuery { transaction, query }
            | DatabaseRequest::RelationAggregate {
                transaction, query, ..
            } => (
                *transaction,
                vec![query.source_relation.clone(), query.target_relation.clone()],
            ),
            DatabaseRequest::RawSql {
                transaction,
                statement_id,
                ..
            } => (
                *transaction,
                self.application()
                    .raw_sql
                    .iter()
                    .find(|statement| statement.id == *statement_id)
                    .map(|statement| statement.relations.iter().cloned().collect())
                    .unwrap_or_default(),
            ),
        };
        let transaction = self.transaction_mut(transaction)?;
        transaction.refresh_read_committed_snapshot()?;
        for relation in relations {
            transaction.record_serializable_read(&relation)?;
        }
        Ok(())
    }

    fn database(&mut self, request: DatabaseRequest) -> Result<HostValue> {
        self.record_database_read_dependencies(&request)?;
        match request {
            DatabaseRequest::SelectPrimaryKey {
                transaction,
                relation,
                id,
                columns,
            } => {
                let default_projection = columns.is_empty();
                let columns = self.readable_projection(&relation, &columns);
                self.permission(&relation, DatabaseAction::Select, columns.clone(), false)?;
                let scope = self.relation_scope(&relation);
                let vector_field = self.vector_field(&relation);
                let record = self
                    .transaction_mut(transaction)?
                    .get_authorized(&relation, &id)?;
                Ok(HostValue::Json(
                    record
                        .as_deref()
                        .map(|record| resource_record_json(record, vector_field.as_deref()))
                        .filter(|value| scope.allows(value))
                        .map(|value| {
                            if default_projection {
                                self.project_readable(&relation, value)
                            } else {
                                project(value, &columns)
                            }
                        })
                        .unwrap_or(Value::Null),
                ))
            }
            DatabaseRequest::Insert {
                transaction,
                grant,
                relation,
                record,
            } => {
                let vector_field = self.vector_field(&relation);
                let record = record_from_resource_json(record, vector_field.as_deref())?;
                self.permission(
                    &relation,
                    DatabaseAction::Insert,
                    resource_record_columns(&record, vector_field.as_deref()),
                    true,
                )?;
                let grant = self.grant(transaction, grant)?;
                self.transaction_mut(transaction)?
                    .insert_with_grant(grant, &relation, record)?;
                Ok(HostValue::Unit)
            }
            DatabaseRequest::Update {
                transaction,
                grant,
                relation,
                id,
                patch,
                ..
            } => {
                let vector_field = self.vector_field(&relation);
                let patch = patch.as_object().cloned().ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied("update patch must be an object".to_string())
                })?;
                self.permission(
                    &relation,
                    DatabaseAction::Update,
                    patch.keys().cloned(),
                    true,
                )?;
                let mut record = self
                    .transaction_mut(transaction)?
                    .get_authorized(&relation, &id)?
                    .ok_or_else(|| {
                        AppRuntimeError::CapabilityDenied(format!(
                            "record `{relation}/{id}` not found"
                        ))
                    })?
                    .as_ref()
                    .clone();
                merge_resource_record_patch(&mut record, patch, vector_field.as_deref())?;
                let grant = self.grant(transaction, grant)?;
                self.transaction_mut(transaction)?
                    .update_with_grant(grant, &relation, record)?;
                Ok(HostValue::Unit)
            }
            DatabaseRequest::Delete {
                transaction,
                grant,
                relation,
                id,
            } => {
                self.permission(&relation, DatabaseAction::Delete, std::iter::empty(), true)?;
                let grant = self.grant(transaction, grant)?;
                self.transaction_mut(transaction)?
                    .delete_with_grant(grant, &relation, &id)?;
                Ok(HostValue::Unit)
            }
            DatabaseRequest::Upsert {
                transaction,
                grant,
                relation,
                record,
                ..
            } => {
                let vector_field = self.vector_field(&relation);
                let record = record_from_resource_json(record, vector_field.as_deref())?;
                self.permission(
                    &relation,
                    DatabaseAction::Upsert,
                    resource_record_columns(&record, vector_field.as_deref()),
                    true,
                )?;
                let grant = self.grant(transaction, grant)?;
                self.transaction_mut(transaction)?
                    .upsert_with_grant(grant, &relation, record)?;
                Ok(HostValue::Unit)
            }
            DatabaseRequest::Query { transaction, query } => self.query(transaction, query),
            DatabaseRequest::RelationQuery { transaction, query } => {
                self.relation_query(transaction, query)
            }
            DatabaseRequest::RelationAggregate {
                transaction,
                query,
                aggregate,
            } => {
                self.permission(
                    &query.target_relation,
                    DatabaseAction::Aggregate,
                    aggregate_field(&aggregate),
                    false,
                )?;
                let rows = self.relation_rows(transaction, &query)?;
                Ok(aggregate_rows(&rows, aggregate))
            }
            DatabaseRequest::Aggregate {
                transaction,
                relation,
                aggregate,
                filters,
            } => {
                self.permission(
                    &relation,
                    DatabaseAction::Aggregate,
                    aggregate_field(&aggregate),
                    false,
                )?;
                let scope = self.relation_scope(&relation);
                let vector_field = self.vector_field(&relation);
                let rows = self
                    .transaction_mut(transaction)?
                    .scan_collection_authorized(&relation)?
                    .into_iter()
                    .map(|record| resource_record_json(&record, vector_field.as_deref()))
                    .filter(|record| {
                        scope.allows(record)
                            && filters.iter().all(|filter| filter_matches(record, filter))
                    })
                    .collect::<Vec<_>>();
                Ok(aggregate_rows(&rows, aggregate))
            }
            DatabaseRequest::FullTextSearch {
                transaction,
                relation,
                index,
                query,
                limit,
            } => {
                self.permission(
                    &relation,
                    DatabaseAction::FullTextSearch,
                    std::iter::empty(),
                    false,
                )?;
                let scope = self.relation_scope(&relation);
                let vector_field = self.vector_field(&relation);
                let rows = self
                    .transaction_mut(transaction)?
                    .full_text_search_authorized(&relation, &index, &query, limit as usize)?
                    .into_iter()
                    .map(|record| resource_record_json(&record, vector_field.as_deref()))
                    .filter(|record| scope.allows(record))
                    .map(|record| self.project_readable(&relation, record))
                    .collect();
                Ok(HostValue::Rows(rows))
            }
            DatabaseRequest::VectorSearch {
                transaction,
                relation,
                index,
                vector,
                limit,
                filters,
            } => {
                self.permission(
                    &relation,
                    DatabaseAction::VectorSearch,
                    std::iter::empty(),
                    false,
                )?;
                let (_, contract) = self.vector_search_contract(&relation)?;
                if index != contract.index_name {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "vector index `{index}` is not the signed index for `{relation}`"
                    )));
                }
                validate_vector_query(&vector, contract.dimensions, limit)?;
                let scope = self.relation_scope(&relation);
                let records = self
                    .transaction_mut(transaction)?
                    .scan_collection_authorized(&relation)?;
                let mut scored = Vec::new();
                for record in records {
                    let Some(candidate) = record.vector.as_ref() else {
                        continue;
                    };
                    let row = resource_record_json(&record, Some(&contract.field));
                    if !scope.allows(&row)
                        || !filters.iter().all(|filter| filter_matches(&row, filter))
                    {
                        continue;
                    }
                    let score = carrier_vector_score(contract.metric, &vector, candidate)?;
                    scored.push((score, record.id.clone(), row));
                }
                scored.sort_by(|left, right| {
                    right
                        .0
                        .total_cmp(&left.0)
                        .then_with(|| left.1.cmp(&right.1))
                });
                Ok(HostValue::Rows(
                    scored
                        .into_iter()
                        .take(limit as usize)
                        .map(|(score, _, mut row)| {
                            row.as_object_mut()
                                .expect("record JSON is object")
                                .insert("_score".to_string(), Value::from(score));
                            row
                        })
                        .map(|row| self.project_readable(&relation, row))
                        .collect(),
                ))
            }
            DatabaseRequest::HybridSearch {
                transaction,
                relation,
                vector_index,
                text_index,
                query,
                vector,
                vector_weight,
                text_weight,
                limit,
                filters,
            } => {
                self.permission(
                    &relation,
                    DatabaseAction::VectorSearch,
                    std::iter::empty(),
                    false,
                )?;
                self.permission(
                    &relation,
                    DatabaseAction::FullTextSearch,
                    std::iter::empty(),
                    false,
                )?;
                let (resource, contract) = self.vector_search_contract(&relation)?;
                if vector_index != contract.index_name
                    || text_index != format!("{resource}_search")
                    || contract.text_fields.is_empty()
                {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "hybrid search does not match the signed vector/text indexes".to_string(),
                    ));
                }
                validate_vector_query(&vector, contract.dimensions, limit)?;
                let query = query.trim();
                if query.is_empty() || !vector_weight.is_finite() || !text_weight.is_finite() {
                    return Err(AppRuntimeError::InvalidRequest(
                        "hybrid search requires a non-empty query and finite weights".to_string(),
                    ));
                }
                let scope = self.relation_scope(&relation);
                let records = self
                    .transaction_mut(transaction)?
                    .scan_collection_authorized(&relation)?;
                let mut scored = Vec::new();
                for record in records {
                    let Some(candidate) = record.vector.as_ref() else {
                        continue;
                    };
                    let row = resource_record_json(&record, Some(&contract.field));
                    if !scope.allows(&row)
                        || !filters.iter().all(|filter| filter_matches(&row, filter))
                    {
                        continue;
                    }
                    let vector_score = carrier_vector_score(contract.metric, &vector, candidate)?;
                    let text = contract
                        .text_fields
                        .iter()
                        .map(|field| {
                            row.get(field)
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string()
                        })
                        .collect::<Vec<_>>();
                    let text_score = f64::from(bicdb_sql::application_websearch_rank_cd_english(
                        &text, query,
                    ));
                    let score = vector_weight.mul_add(vector_score, text_weight * text_score);
                    if !score.is_finite() {
                        return Err(AppRuntimeError::InvalidRequest(
                            "hybrid search score is not finite".to_string(),
                        ));
                    }
                    scored.push((score, record.id.clone(), vector_score, text_score, row));
                }
                scored.sort_by(|left, right| {
                    right
                        .0
                        .total_cmp(&left.0)
                        .then_with(|| left.1.cmp(&right.1))
                });
                Ok(HostValue::Rows(
                    scored
                        .into_iter()
                        .take(limit as usize)
                        .map(|(score, _, vector_score, text_score, mut row)| {
                            let row = row.as_object_mut().expect("record JSON is object");
                            row.insert("_score".to_string(), Value::from(score));
                            row.insert("_vector_score".to_string(), Value::from(vector_score));
                            row.insert("_text_score".to_string(), Value::from(text_score));
                            Value::Object(row.clone())
                        })
                        .map(|row| self.project_readable(&relation, row))
                        .collect(),
                ))
            }
            DatabaseRequest::Spatial {
                transaction,
                relation,
                operation,
                filters,
            } => {
                let (field, expected_kind, limit) = match &operation {
                    SpatialOperation::Nearest { field, limit, .. }
                    | SpatialOperation::WithinRadius { field, limit, .. } => {
                        (field.as_str(), ApplicationGeometryTypeV1::Point, *limit)
                    }
                    SpatialOperation::Contains { field, limit, .. } => {
                        (field.as_str(), ApplicationGeometryTypeV1::Polygon, *limit)
                    }
                    SpatialOperation::Within { field, limit, .. }
                    | SpatialOperation::Intersects { field, limit, .. } => {
                        let kind = self
                            .application()
                            .resources
                            .iter()
                            .find(|contract| contract.relation.eq_ignore_ascii_case(&relation))
                            .and_then(|contract| {
                                contract.fields.iter().find(|candidate| candidate.name == *field)
                            })
                            .and_then(|candidate| match candidate.field_type {
                                bicdb_extension::abi_v2::FieldType::Geometry {
                                    geometry_type,
                                    ..
                                } => geometry_type,
                                _ => None,
                            })
                            .ok_or_else(|| {
                                AppRuntimeError::CapabilityDenied(format!(
                                    "relation `{relation}` field `{field}` is not an exactly typed geometry"
                                ))
                            })?;
                        (field.as_str(), kind, *limit)
                    }
                };
                if limit == 0 || limit > 10_000 {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "spatial limit must be in 1..=10000".to_string(),
                    ));
                }
                self.permission(
                    &relation,
                    DatabaseAction::Spatial,
                    std::iter::once(field.to_string()).chain(filters.iter().map(filter_field)),
                    false,
                )?;
                self.spatial_field_contract(&relation, field, expected_kind)?;
                let scope = self.relation_scope(&relation);
                let vector_field = self.vector_field(&relation);
                let rows = self
                    .transaction_mut(transaction)?
                    .scan_collection_authorized(&relation)?
                    .into_iter()
                    .map(|record| resource_record_json(&record, vector_field.as_deref()))
                    .filter(|row| {
                        scope.allows(row)
                            && filters.iter().all(|filter| filter_matches(row, filter))
                    })
                    .collect::<Vec<_>>();
                Ok(HostValue::Rows(
                    spatial_rows(rows, operation)?
                        .into_iter()
                        .map(|row| self.project_readable(&relation, row))
                        .collect(),
                ))
            }
            DatabaseRequest::Recent {
                transaction,
                relation,
                time_field,
                window,
                limit,
                filters,
            } => {
                if limit == 0 || limit > 10_000 {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "recent limit must be in 1..=10000".to_string(),
                    ));
                }
                let contract = self.timeseries_contract(&relation)?;
                if contract.time_field != time_field {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "relation `{relation}` recent field `{time_field}` differs from its signed timeseries field"
                    )));
                }
                self.permission(
                    &relation,
                    DatabaseAction::Timeseries,
                    std::iter::once(time_field.clone()).chain(filters.iter().map(filter_field)),
                    false,
                )?;
                let cutoff = carrier_recent_cutoff(&window, contract.retention.as_deref())?;
                let scope = self.relation_scope(&relation);
                let vector_field = self.vector_field(&relation);
                let mut rows = self
                    .transaction_mut(transaction)?
                    .scan_collection_authorized(&relation)?
                    .into_iter()
                    .map(|record| resource_record_json(&record, vector_field.as_deref()))
                    .filter(|row| {
                        scope.allows(row)
                            && filters.iter().all(|filter| filter_matches(row, filter))
                    })
                    .filter_map(|row| {
                        let instant = row
                            .get(&time_field)
                            .and_then(Value::as_str)
                            .and_then(|value| {
                                bicdb_sql::typed_value::PgTimestamp::from_postgres_text(value, true)
                                    .ok()
                            })?
                            .finite_micros()?;
                        (instant >= cutoff).then_some((instant, row))
                    })
                    .collect::<Vec<_>>();
                rows.sort_by(|left, right| {
                    right
                        .0
                        .cmp(&left.0)
                        .then_with(|| carrier_row_id(&left.1).cmp(&carrier_row_id(&right.1)))
                });
                Ok(HostValue::Rows(
                    rows.into_iter()
                        .take(limit as usize)
                        .map(|(_, row)| self.project_readable(&relation, row))
                        .collect(),
                ))
            }
            DatabaseRequest::JsonPath {
                transaction,
                relation,
                path,
                filters,
                limit,
            } => {
                self.permission(
                    &relation,
                    DatabaseAction::JsonPath,
                    filters.iter().map(filter_field),
                    false,
                )?;
                if !path.starts_with('$') || path.len() > 1024 || limit > 10_000 {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "invalid JSON path or limit".to_string(),
                    ));
                }
                let scope = self.relation_scope(&relation);
                let vector_field = self.vector_field(&relation);
                let rows = self
                    .transaction_mut(transaction)?
                    .scan_collection_authorized(&relation)?
                    .into_iter()
                    .map(|record| resource_record_json(&record, vector_field.as_deref()))
                    .filter(|row| {
                        scope.allows(row)
                            && json_path_value(row, &path).is_some()
                            && filters.iter().all(|filter| filter_matches(row, filter))
                    })
                    .take(limit as usize)
                    .map(|row| self.project_readable(&relation, row))
                    .collect();
                Ok(HostValue::Rows(rows))
            }
            DatabaseRequest::RawSql {
                transaction,
                statement_id,
                parameters,
            } => {
                let statement = self
                    .application()
                    .raw_sql
                    .iter()
                    .find(|statement| statement.id == statement_id)
                    .cloned()
                    .ok_or_else(|| {
                        AppRuntimeError::CapabilityDenied(format!(
                            "raw SQL `{statement_id}` is undeclared"
                        ))
                    })?;
                self.execute_declared_sql(transaction, &statement, parameters)
            }
            DatabaseRequest::LockRows {
                transaction,
                relation,
                ids,
            } => {
                self.permission(&relation, DatabaseAction::Lock, std::iter::empty(), false)?;
                let scope = self.relation_scope(&relation);
                let vector_field = self.vector_field(&relation);
                for id in &ids {
                    let record = self
                        .transaction_mut(transaction)?
                        .get_authorized(&relation, id)?
                        .ok_or_else(|| {
                            AppRuntimeError::CapabilityDenied(
                                "row lock target is absent or outside actor scope".to_string(),
                            )
                        })?;
                    if !scope.allows(&resource_record_json(&record, vector_field.as_deref())) {
                        return Err(AppRuntimeError::CapabilityDenied(
                            "row lock target is absent or outside actor scope".to_string(),
                        ));
                    }
                }
                self.transaction_mut(transaction)?
                    .lock_rows_authorized(&relation, &ids)?;
                Ok(HostValue::Unit)
            }
        }
    }

    fn query(&mut self, transaction: HostHandle, query: QuerySpec) -> Result<HostValue> {
        if query.limit == 0 || query.limit > 10_000 || query.offset > 10_000_000 {
            return Err(AppRuntimeError::CapabilityDenied(
                "query pagination exceeds host bounds".to_string(),
            ));
        }
        let default_projection = query.columns.is_empty();
        let columns = self.readable_projection(&query.relation, &query.columns);
        let mut referenced = columns.clone();
        referenced.extend(query.sort.iter().map(|sort| sort.field.clone()));
        referenced.extend(query.filters.iter().map(filter_field));
        self.permission(&query.relation, DatabaseAction::Select, referenced, false)?;
        let scope = self.relation_scope(&query.relation);
        let vector_field = self.vector_field(&query.relation);
        let mut rows = self
            .transaction_mut(transaction)?
            .scan_collection_authorized(&query.relation)?
            .into_iter()
            .map(|record| resource_record_json(&record, vector_field.as_deref()))
            .filter(|row| {
                scope.allows(row)
                    && query
                        .filters
                        .iter()
                        .all(|filter| filter_matches(row, filter))
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| compare_rows(left, right, &query.sort));
        let start = query.offset as usize;
        let rows = rows
            .into_iter()
            .skip(start)
            .take(query.limit as usize)
            .map(|row| {
                if default_projection {
                    self.project_readable(&query.relation, row)
                } else {
                    project(row, &columns)
                }
            })
            .collect();
        Ok(HostValue::Rows(rows))
    }

    /// Trusted single-statement candidate scan for the policy-filtered
    /// resource list path. Applies the same permission, scope, and projection
    /// discipline as [`DatabaseRequest::Query`], but is deliberately not
    /// reachable from the extension ABI and therefore not subject to the
    /// ABI's per-query pagination bound: policy filtering must see every
    /// candidate row, and splitting the scan across bounded ABI queries would
    /// rescan the collection once per chunk and — because a read-committed
    /// transaction refreshes its snapshot before every statement — page over
    /// a moving row set, silently skipping or double-counting candidates.
    /// The caller enforces its own candidate ceiling on the returned rows.
    pub(crate) fn policy_candidate_rows(
        &mut self,
        transaction: HostHandle,
        relation: &str,
        filters: &[FilterExpression],
    ) -> Result<Vec<Value>> {
        let tx = self.transaction_mut(transaction)?;
        tx.refresh_read_committed_snapshot()?;
        tx.record_serializable_read(relation)?;
        let columns = self.readable_projection(relation, &[]);
        let mut referenced = columns;
        referenced.extend(filters.iter().map(filter_field));
        self.permission(relation, DatabaseAction::Select, referenced, false)?;
        let scope = self.relation_scope(relation);
        let vector_field = self.vector_field(relation);
        let rows = self
            .transaction_mut(transaction)?
            .scan_collection_authorized(relation)?
            .into_iter()
            .map(|record| resource_record_json(&record, vector_field.as_deref()))
            .filter(|row| {
                scope.allows(row) && filters.iter().all(|filter| filter_matches(row, filter))
            })
            .map(|row| self.project_readable(relation, row))
            .collect();
        Ok(rows)
    }

    fn relation_query(
        &mut self,
        transaction: HostHandle,
        query: RelationQuerySpec,
    ) -> Result<HostValue> {
        let mut rows = self.relation_rows(transaction, &query)?;
        if !query.target_sort.is_empty() {
            rows.sort_by(|left, right| compare_rows(left, right, &query.target_sort));
        }
        let default_projection = query.target_columns.is_empty();
        let target_columns =
            self.readable_projection(&query.target_relation, &query.target_columns);
        let rows = rows
            .into_iter()
            .skip(query.offset as usize)
            .take(query.limit as usize)
            .map(|row| {
                if default_projection {
                    self.project_readable(&query.target_relation, row)
                } else {
                    project(row, &target_columns)
                }
            })
            .collect();
        Ok(HostValue::Rows(rows))
    }

    fn relation_rows(
        &mut self,
        transaction: HostHandle,
        query: &RelationQuerySpec,
    ) -> Result<Vec<Value>> {
        if query.limit == 0 || query.limit > 1_000 || query.offset > 10_000_000 {
            return Err(AppRuntimeError::CapabilityDenied(
                "relation query pagination exceeds host bounds".to_string(),
            ));
        }
        self.permission(
            &query.source_relation,
            DatabaseAction::Select,
            [query.source_field.clone(), query.join_field.clone()],
            false,
        )?;
        let mut target_columns =
            self.readable_projection(&query.target_relation, &query.target_columns);
        target_columns.push(query.target_field.clone());
        target_columns.extend(query.target_sort.iter().map(|sort| sort.field.clone()));
        target_columns.extend(query.target_filters.iter().map(filter_field));
        self.permission(
            &query.target_relation,
            DatabaseAction::Select,
            target_columns,
            false,
        )?;
        if query.search.is_some() {
            self.permission(
                &query.target_relation,
                DatabaseAction::FullTextSearch,
                std::iter::empty(),
                false,
            )?;
        }
        let source_scope = self.relation_scope(&query.source_relation);
        let target_scope = self.relation_scope(&query.target_relation);
        let source_vector_field = self.vector_field(&query.source_relation);
        let target_vector_field = self.vector_field(&query.target_relation);
        let join_values = self
            .transaction_mut(transaction)?
            .scan_collection_authorized(&query.source_relation)?
            .into_iter()
            .map(|record| resource_record_json(&record, source_vector_field.as_deref()))
            .filter(|row| {
                source_scope.allows(row)
                    && row.get(&query.source_field) == Some(&query.source_value)
            })
            .filter_map(|row| row.get(&query.join_field).cloned())
            .collect::<Vec<_>>();
        if join_values.is_empty() {
            return Ok(Vec::new());
        }
        const MAX_EXACT_SEARCH_ROWS: usize = 10_000_000;
        let target_records = if let Some(search) = &query.search {
            let records = self
                .transaction_mut(transaction)?
                .full_text_search_authorized(
                    &query.target_relation,
                    &search.index,
                    &search.query,
                    MAX_EXACT_SEARCH_ROWS.saturating_add(1),
                )?;
            if records.len() > MAX_EXACT_SEARCH_ROWS {
                return Err(AppRuntimeError::InvalidRequest(
                    "BicDB application exact relation search pagination exceeds 10000000 matching rows"
                        .to_string(),
                ));
            }
            records
        } else {
            self.transaction_mut(transaction)?
                .scan_collection_authorized(&query.target_relation)?
        };
        let mut rows = Vec::new();
        for row in target_records
            .into_iter()
            .map(|record| resource_record_json(&record, target_vector_field.as_deref()))
        {
            if !target_scope.allows(&row)
                || !query
                    .target_filters
                    .iter()
                    .all(|filter| filter_matches(&row, filter))
            {
                continue;
            }
            let Some(value) = row.get(&query.target_field) else {
                continue;
            };
            for _ in 0..join_values
                .iter()
                .filter(|candidate| *candidate == value)
                .count()
            {
                rows.push(row.clone());
            }
        }
        Ok(rows)
    }

    fn broker(&mut self, request: BrokerRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::QueueEvents)?;
        match request {
            BrokerRequest::Publish {
                queue,
                payload,
                headers,
                idempotency_key,
                delay_ms,
            } => {
                if !self.extension.permissions.publish_queues.contains(&queue) {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "publish queue `{queue}` is undeclared"
                    )));
                }
                let headers = event_headers(&self.actor, &self.extension.identity.name, headers);
                let receipt = unsafe { self.db.as_ref() }.with_broker(|broker| {
                    broker.publish_with(
                        &queue,
                        payload,
                        PublishOptions {
                            headers,
                            idempotency_key,
                            delay_ms,
                            max_attempts: None,
                        },
                    )
                })?;
                Ok(HostValue::String(receipt.message_id.to_string()))
            }
            BrokerRequest::PublishOnCommit {
                transaction,
                queue,
                payload,
                headers,
                idempotency_key,
                delay_ms,
            } => {
                if !self.extension.permissions.publish_queues.contains(&queue) {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "publish queue `{queue}` is undeclared"
                    )));
                }
                let transaction_id = self.transaction_mut(transaction)?.id().0;
                let mut headers =
                    event_headers(&self.actor, &self.extension.identity.name, headers);
                if let Some(values) = headers
                    .get_mut("bicdb_application")
                    .and_then(Value::as_object_mut)
                {
                    values.insert(
                        "transaction_id".to_string(),
                        Value::String(transaction_id.to_string()),
                    );
                }
                let id = self.transaction_mut(transaction)?.buffer_broker_publish(
                    &queue,
                    payload,
                    PublishOptions {
                        headers,
                        idempotency_key,
                        delay_ms,
                        max_attempts: None,
                    },
                )?;
                Ok(HostValue::String(id.to_string()))
            }
            BrokerRequest::Consume {
                queue,
                group,
                consumer,
                max_messages,
                visibility_timeout_ms,
            } => {
                if !self.extension.permissions.consume_queues.contains(&queue) {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "consume queue `{queue}` is undeclared"
                    )));
                }
                let messages = unsafe { self.db.as_ref() }.with_broker(|broker| {
                    broker.consume(
                        &queue,
                        &group,
                        &consumer,
                        ConsumeOptions {
                            max_messages: max_messages.min(100) as usize,
                            visibility_timeout_ms,
                        },
                    )
                })?;
                let mut output = Vec::with_capacity(messages.len());
                for message in messages {
                    let handle = self.next_handle(&self.deliveries);
                    self.deliveries.insert(
                        handle,
                        DeliveryBinding {
                            queue: queue.clone(),
                            group: group.clone(),
                            consumer: consumer.clone(),
                            message_id: message.message_id,
                        },
                    );
                    output.push(AbiBrokerMessage {
                        delivery: handle,
                        message_id: message.message_id.to_string(),
                        payload: message.payload,
                        headers: string_headers(&message.headers),
                        attempts: message.attempts,
                        trace_id: system_header(&message.headers, "trace_id")
                            .unwrap_or_else(|| self.actor.trace_id.clone()),
                        correlation_id: system_header(&message.headers, "correlation_id"),
                        causation_id: system_header(&message.headers, "causation_id"),
                        actor_id: system_header(&message.headers, "actor_id"),
                        tenant_id: system_header(&message.headers, "tenant_id"),
                        workspace_id: system_header(&message.headers, "workspace_id"),
                        organization_id: system_header(&message.headers, "organization_id"),
                        originating_plugin: system_header(&message.headers, "originating_plugin")
                            .unwrap_or_default(),
                        originating_resource: system_header(
                            &message.headers,
                            "originating_resource",
                        ),
                        originating_action: system_header(&message.headers, "originating_action"),
                        schema_version: system_header(&message.headers, "schema_version")
                            .and_then(|value| value.parse().ok())
                            .unwrap_or(1),
                        contract_version: system_header(&message.headers, "contract_version")
                            .and_then(|value| value.parse().ok()),
                        event_schema_version: system_header(
                            &message.headers,
                            "event_schema_version",
                        )
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(1),
                        transaction_id: system_header(&message.headers, "transaction_id")
                            .and_then(|value| value.parse().ok()),
                        commit_sequence: Some(message.sequence),
                    });
                }
                Ok(HostValue::Messages(output))
            }
            BrokerRequest::Ack { delivery } => {
                let binding = self.deliveries.remove(&delivery).ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied(
                        "forged or expired delivery handle".to_string(),
                    )
                })?;
                unsafe { self.db.as_ref() }.with_broker(|broker| {
                    broker.ack(
                        &binding.queue,
                        &binding.group,
                        &binding.consumer,
                        binding.message_id,
                    )
                })?;
                Ok(HostValue::Unit)
            }
            BrokerRequest::Nack {
                delivery,
                retry,
                delay_ms,
                error_code,
            } => {
                let binding = self.deliveries.remove(&delivery).ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied(
                        "forged or expired delivery handle".to_string(),
                    )
                })?;
                unsafe { self.db.as_ref() }.with_broker(|broker| {
                    broker.nack(
                        &binding.queue,
                        &binding.group,
                        &binding.consumer,
                        binding.message_id,
                        NackOptions {
                            requeue: retry,
                            delay_ms,
                            error: error_code,
                        },
                    )
                })?;
                Ok(HostValue::Unit)
            }
        }
    }

    fn service(&mut self, request: ServiceRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::PluginServices)?;
        let ServiceRequest::Call {
            dependency,
            service,
            method,
            payload,
            transaction,
            deadline_unix_ms,
        } = request;
        let import = self
            .application()
            .service_imports
            .iter()
            .find(|import| import.name == dependency && import.service == service)
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "plugin dependency `{dependency}` service `{service}` is undeclared"
                ))
            })?;
        if transaction.is_some() && !import.propagate_transaction {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "plugin dependency `{dependency}` did not declare transaction propagation"
            )));
        }
        if deadline_unix_ms > self.actor.deadline_unix_ms {
            return Err(AppRuntimeError::CapabilityDenied(
                "service call attempted to extend its deadline".to_string(),
            ));
        }
        if deadline_unix_ms <= now_ms() {
            return Err(AppRuntimeError::Timeout(
                "service-call deadline has elapsed".to_string(),
            ));
        }
        if self.service_trace.len() >= self.application().max_call_depth as usize {
            return Err(AppRuntimeError::CapabilityDenied(
                "plugin call-depth limit exceeded".to_string(),
            ));
        }
        if self
            .service_trace
            .iter()
            .any(|entry| entry.eq_ignore_ascii_case(&dependency))
            && !import.allow_reentrant
        {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "plugin dependency cycle or forbidden reentrancy through `{dependency}`"
            )));
        }
        let mut trace = self.service_trace.clone();
        trace.push(dependency.clone());
        let propagated_transaction = transaction
            .map(|handle| {
                self.transaction_mut(handle)
                    .map(|transaction| PropagatedTransaction {
                        pointer: NonNull::from(transaction),
                    })
            })
            .transpose()?;
        let result = self.services.services.call(PluginServiceCall {
            caller: self.extension.identity.name.clone(),
            caller_manifest: self.extension.clone(),
            dependency,
            service,
            method,
            payload,
            actor: self.actor.clone(),
            trace,
            deadline_unix_ms,
            transaction_requested: transaction.is_some(),
            database: InvocationDatabase { pointer: self.db },
            transaction: propagated_transaction,
        })?;
        Ok(HostValue::Json(result))
    }

    fn clock(&self, request: ClockRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::Clock)?;
        let value = match request {
            ClockRequest::WallTime => self
                .services
                .deterministic_clock_ms
                .as_ref()
                .map(|clock| clock.load(std::sync::atomic::Ordering::SeqCst))
                .unwrap_or_else(now_ms),
            ClockRequest::MonotonicTime => self.monotonic_start.elapsed().as_nanos() as i64,
        };
        Ok(HostValue::I64(value))
    }

    fn random(&self, request: RandomRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::Random)?;
        match request {
            RandomRequest::Bytes { len } => {
                if len > 64 * 1024 {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "random request exceeds 64 KiB".to_string(),
                    ));
                }
                Ok(HostValue::Bytes(self.random_bytes(len as usize)))
            }
            RandomRequest::Uuid => {
                let bytes: [u8; 16] = self.random_bytes(16).try_into().unwrap();
                Ok(HostValue::String(Uuid::from_bytes(bytes).to_string()))
            }
        }
    }

    fn random_bytes(&self, len: usize) -> Vec<u8> {
        if let Some(seed) = &self.services.deterministic_random_seed {
            let mut state = seed.lock().expect("deterministic RNG poisoned");
            let mut bytes = Vec::with_capacity(len);
            while bytes.len() < len {
                *state ^= *state << 13;
                *state ^= *state >> 7;
                *state ^= *state << 17;
                bytes.extend_from_slice(&state.to_le_bytes());
            }
            bytes.truncate(len);
            return bytes;
        }
        let mut bytes = Vec::with_capacity(len);
        while bytes.len() < len {
            bytes.extend_from_slice(Uuid::new_v4().as_bytes());
        }
        bytes.truncate(len);
        bytes
    }

    fn secret(&mut self, request: SecretRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::SecretsCrypto)?;
        match request {
            SecretRequest::Open { name, version } => {
                let declaration = self
                    .application()
                    .secrets
                    .iter()
                    .find(|secret| secret.name == name)
                    .ok_or_else(|| {
                        AppRuntimeError::CapabilityDenied(format!("secret `{name}` is undeclared"))
                    })?;
                if version.as_ref().is_some_and(|version| {
                    !declaration.versions.is_empty() && !declaration.versions.contains(version)
                }) {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "secret `{name}` version is undeclared"
                    )));
                }
                let secret = self.services.secrets.open(&name, version.as_deref())?;
                let handle = self.next_handle(&self.secrets);
                self.secrets.insert(handle, secret);
                Ok(HostValue::Handle(handle))
            }
            SecretRequest::Metadata { secret } => {
                let secret = self.secret_record(secret, CryptoOperation::Metadata)?;
                Ok(HostValue::SecretMetadata(SecretMetadata {
                    name: secret.name.clone(),
                    key_id: secret.key_id.clone(),
                    version: secret.version.clone(),
                    algorithm: secret.algorithm.clone(),
                }))
            }
            SecretRequest::ReadPlaintext { secret } => {
                let secret = self.secret_record(secret, CryptoOperation::PlaintextRead)?;
                Ok(HostValue::Bytes(secret.material.to_vec()))
            }
        }
    }

    fn secret_record(
        &self,
        handle: HostHandle,
        operation: CryptoOperation,
    ) -> Result<&SecretRecord> {
        let secret = self.secrets.get(&handle).ok_or_else(|| {
            AppRuntimeError::CapabilityDenied("forged or expired secret handle".to_string())
        })?;
        let declaration = self
            .application()
            .secrets
            .iter()
            .find(|declaration| declaration.name == secret.name)
            .filter(|declaration| declaration.operations.contains(&operation))
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "secret `{}` does not allow {operation:?}",
                    secret.name
                ))
            })?;
        if operation == CryptoOperation::PlaintextRead && !declaration.allow_plaintext_read {
            return Err(AppRuntimeError::CapabilityDenied(
                "secret plaintext is host-only".to_string(),
            ));
        }
        Ok(secret)
    }

    fn crypto(&self, request: CryptoRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::SecretsCrypto)?;
        match request {
            CryptoRequest::Sign {
                secret,
                algorithm,
                message,
            } => {
                let secret = self.secret_record(secret, CryptoOperation::Sign)?;
                let signature = match algorithm.as_str() {
                    "ed25519" => {
                        let key: [u8; 32] = secret.material.as_ref().try_into().map_err(|_| {
                            AppRuntimeError::Provider(
                                "Ed25519 signing key must contain 32 bytes".to_string(),
                            )
                        })?;
                        SigningKey::from_bytes(&key)
                            .sign(&message)
                            .to_bytes()
                            .to_vec()
                    }
                    "hmac-sha256" => hmac_sha256(&secret.material, &message)?,
                    _ => {
                        return Err(AppRuntimeError::CapabilityDenied(format!(
                            "signing algorithm `{algorithm}` is unsupported"
                        )))
                    }
                };
                Ok(HostValue::Bytes(signature))
            }
            CryptoRequest::Verify {
                secret,
                algorithm,
                message,
                signature,
            } => {
                let secret = self.secret_record(secret, CryptoOperation::Verify)?;
                let valid = match algorithm.as_str() {
                    "ed25519" => {
                        let key: [u8; 32] = secret.material.as_ref().try_into().map_err(|_| {
                            AppRuntimeError::Provider(
                                "Ed25519 verifying key must contain 32 bytes".to_string(),
                            )
                        })?;
                        let verifying = VerifyingKey::from_bytes(&key)
                            .map_err(|error| AppRuntimeError::Provider(error.to_string()))?;
                        Signature::from_slice(&signature)
                            .ok()
                            .is_some_and(|signature| verifying.verify(&message, &signature).is_ok())
                    }
                    "hmac-sha256" => {
                        let expected = hmac_sha256(&secret.material, &message)?;
                        constant_time_eq(&expected, &signature)
                    }
                    _ => false,
                };
                Ok(HostValue::Bool(valid))
            }
            CryptoRequest::Hmac {
                secret,
                algorithm,
                message,
            } => {
                let secret = self.secret_record(secret, CryptoOperation::Hmac)?;
                if algorithm != "hmac-sha256" {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "only hmac-sha256 is supported".to_string(),
                    ));
                }
                Ok(HostValue::Bytes(hmac_sha256(&secret.material, &message)?))
            }
            CryptoRequest::Encrypt {
                secret,
                algorithm,
                plaintext,
                associated_data,
            } => {
                let secret = self.secret_record(secret, CryptoOperation::Encrypt)?;
                if matches!(
                    algorithm.as_str(),
                    "bicdb-aes-256-gcm-v1"
                        | "bicdb-aes-256-gcm-v2"
                        | "carrier-aes-256-gcm-v1"
                        | "carrier-aes-256-gcm-v2"
                ) {
                    let key = encryption_key(&secret.material);
                    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| {
                        AppRuntimeError::Provider(
                            "invalid BicDB application encryption key".to_string(),
                        )
                    })?;
                    let nonce = self.random_bytes(12);
                    let ciphertext = cipher
                        .encrypt(
                            AesNonce::from_slice(&nonce),
                            Payload {
                                msg: &plaintext,
                                aad: &associated_data,
                            },
                        )
                        .map_err(|_| AppRuntimeError::Provider("encryption failed".to_string()))?;
                    let envelope = if matches!(
                        algorithm.as_str(),
                        "bicdb-aes-256-gcm-v2" | "carrier-aes-256-gcm-v2"
                    ) {
                        format!(
                            "enc:v2:{}:{}:{}",
                            base64::engine::general_purpose::URL_SAFE_NO_PAD
                                .encode(secret.version.as_bytes()),
                            encode_hex(&nonce),
                            encode_hex(&ciphertext)
                        )
                    } else {
                        format!("enc:v1:{}:{}", encode_hex(&nonce), encode_hex(&ciphertext))
                    };
                    return Ok(HostValue::Bytes(envelope.into_bytes()));
                }
                if algorithm != "xchacha20-poly1305" {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "unsupported encryption algorithm".to_string(),
                    ));
                }
                let key = encryption_key(&secret.material);
                let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
                let nonce = self.random_bytes(24);
                let mut ciphertext = nonce.clone();
                ciphertext.extend_from_slice(
                    &cipher
                        .encrypt(
                            XNonce::from_slice(&nonce),
                            Payload {
                                msg: &plaintext,
                                aad: &associated_data,
                            },
                        )
                        .map_err(|_| AppRuntimeError::Provider("encryption failed".to_string()))?,
                );
                Ok(HostValue::Bytes(ciphertext))
            }
            CryptoRequest::Decrypt {
                secret,
                algorithm,
                ciphertext,
                associated_data,
            } => {
                let secret = self.secret_record(secret, CryptoOperation::Decrypt)?;
                if matches!(
                    algorithm.as_str(),
                    "bicdb-aes-256-gcm-v1"
                        | "bicdb-aes-256-gcm-v2"
                        | "carrier-aes-256-gcm-v1"
                        | "carrier-aes-256-gcm-v2"
                ) {
                    let envelope = std::str::from_utf8(&ciphertext).map_err(|_| {
                        AppRuntimeError::InvalidRequest(
                            "BicDB application encrypted field is not UTF-8".to_string(),
                        )
                    })?;
                    let encoded = if matches!(
                        algorithm.as_str(),
                        "bicdb-aes-256-gcm-v2" | "carrier-aes-256-gcm-v2"
                    ) {
                        envelope.strip_prefix("enc:v2:").and_then(|encoded| {
                            let (version, rest) = encoded.split_once(':')?;
                            let version = base64::engine::general_purpose::URL_SAFE_NO_PAD
                                .decode(version)
                                .ok()
                                .and_then(|version| String::from_utf8(version).ok())?;
                            (version == secret.version).then_some(rest)
                        })
                    } else {
                        envelope.strip_prefix("enc:v1:")
                    }
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "BicDB application encrypted field envelope does not match its algorithm"
                                .to_string(),
                        )
                    })?;
                    let (nonce, ciphertext) = encoded.split_once(':').ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "BicDB application encrypted field envelope is malformed".to_string(),
                        )
                    })?;
                    let nonce = decode_hex(nonce)?;
                    let ciphertext = decode_hex(ciphertext)?;
                    if nonce.len() != 12 {
                        return Err(AppRuntimeError::InvalidRequest(
                            "BicDB application encrypted field nonce has the wrong length"
                                .to_string(),
                        ));
                    }
                    let key = encryption_key(&secret.material);
                    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| {
                        AppRuntimeError::Provider(
                            "invalid BicDB application encryption key".to_string(),
                        )
                    })?;
                    let plaintext = cipher
                        .decrypt(
                            AesNonce::from_slice(&nonce),
                            Payload {
                                msg: &ciphertext,
                                aad: &associated_data,
                            },
                        )
                        .map_err(|_| AppRuntimeError::Provider("decryption failed".to_string()))?;
                    return Ok(HostValue::Bytes(plaintext));
                }
                if algorithm != "xchacha20-poly1305" || ciphertext.len() < 24 {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "invalid encrypted payload or algorithm".to_string(),
                    ));
                }
                let key = encryption_key(&secret.material);
                let cipher = XChaCha20Poly1305::new(Key::from_slice(&key));
                let plaintext = cipher
                    .decrypt(
                        XNonce::from_slice(&ciphertext[..24]),
                        Payload {
                            msg: &ciphertext[24..],
                            aad: &associated_data,
                        },
                    )
                    .map_err(|_| AppRuntimeError::Provider("decryption failed".to_string()))?;
                Ok(HostValue::Bytes(plaintext))
            }
            CryptoRequest::Derive {
                secret,
                algorithm,
                context,
                len,
            } => {
                let secret = self.secret_record(secret, CryptoOperation::Derive)?;
                if algorithm != "hmac-sha256" || len == 0 || len > 1024 {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "invalid key derivation request".to_string(),
                    ));
                }
                let mut output = Vec::with_capacity(len as usize);
                let mut counter = 0u32;
                while output.len() < len as usize {
                    let mut input = context.clone();
                    input.extend_from_slice(&counter.to_be_bytes());
                    output.extend_from_slice(&hmac_sha256(&secret.material, &input)?);
                    counter += 1;
                }
                output.truncate(len as usize);
                Ok(HostValue::Bytes(output))
            }
        }
    }

    fn egress(&self, request: EgressRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::NetworkEgress)?;
        let EgressRequest::Http {
            policy,
            method,
            url,
            headers,
            body,
            deadline_unix_ms,
        } = request;
        if deadline_unix_ms > self.actor.deadline_unix_ms {
            return Err(AppRuntimeError::CapabilityDenied(
                "egress request attempted to extend deadline".to_string(),
            ));
        }
        if deadline_unix_ms <= now_ms() {
            return Err(AppRuntimeError::Timeout(
                "egress deadline has elapsed".to_string(),
            ));
        }
        let declaration = self
            .application()
            .egress
            .iter()
            .find(|declaration| declaration.name == policy)
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!("egress policy `{policy}` is undeclared"))
            })?;
        let mtls_secret = declaration
            .mtls_secret
            .as_deref()
            .map(|name| self.services.secrets.open(name, None))
            .transpose()?;
        let remaining = deadline_unix_ms.saturating_sub(now_ms()).max(1) as u64;
        Ok(HostValue::EgressResponse(self.services.egress.execute(
            &self.extension.identity.name,
            declaration,
            mtls_secret.as_ref(),
            &method,
            &url,
            &headers,
            &body,
            remaining,
        )?))
    }

    fn grpc(&mut self, request: GrpcRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::NetworkEgress)?;
        self.require_capability(ExtensionCapability::Observability)?;
        let contract = self
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.grpc.as_ref())
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied("gRPC client authority is undeclared".to_string())
            })?;
        let GrpcRequest::Unary {
            client,
            method,
            payload,
            deadline_unix_ms,
        } = request;
        if deadline_unix_ms > self.actor.deadline_unix_ms || deadline_unix_ms <= now_ms() {
            return Err(AppRuntimeError::CapabilityDenied(
                "gRPC request exceeds the invocation deadline".to_string(),
            ));
        }
        let client_contract = contract.clients.get(&client).cloned().ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(format!(
                "gRPC client `{client}` is outside signed authority"
            ))
        })?;
        let method_contract = client_contract.methods.get(&method).ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(format!(
                "gRPC method `{client}.{method}` is outside signed authority"
            ))
        })?;
        let json_size = serde_json::to_vec(&payload)?.len() as u64;
        if json_size > client_contract.max_request_bytes {
            return Err(AppRuntimeError::CapabilityDenied(
                "gRPC request exceeds its signed size bound".to_string(),
            ));
        }
        let remaining = deadline_unix_ms.saturating_sub(now_ms()).max(1) as u64;
        let deadline_ms = remaining.min(method_contract.deadline_ms);
        let application = self.application_name().to_string();
        let mut evidence = ObserveRequest::Evidence {
            control: "grpc.unary".to_string(),
            outcome: "success".to_string(),
            fields: BTreeMap::from([
                (
                    "application".to_string(),
                    Value::String(application.clone()),
                ),
                (
                    "provider".to_string(),
                    Value::String(client_contract.provider.clone()),
                ),
                ("client".to_string(), Value::String(client.clone())),
                ("method".to_string(), Value::String(method.clone())),
            ]),
        };
        self.reserve_observation(&evidence)?;
        let result = self
            .services
            .grpc
            .unary(
                &application,
                &client_contract.provider,
                &method,
                &client_contract,
                payload,
                deadline_ms,
                &self.actor.trace_id,
            )
            .and_then(|value| {
                if serde_json::to_vec(&value)?.len() as u64 > client_contract.max_response_bytes {
                    return Err(AppRuntimeError::Provider(
                        "gRPC provider returned a response above the signed size bound".to_string(),
                    ));
                }
                Ok(value)
            });
        if result.is_err() {
            let ObserveRequest::Evidence { outcome, .. } = &mut evidence else {
                unreachable!("gRPC evidence is an evidence event")
            };
            *outcome = "error".to_string();
        }
        self.record_observation(evidence);
        result.map(HostValue::Json)
    }

    fn tokenizer(&mut self, request: TokenizerRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::AiInference)?;
        self.require_capability(ExtensionCapability::Observability)?;
        let TokenizerRequest::Count { provider, text } = request;
        let contract = self
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.tokenizer.as_ref())
            .and_then(|contract| contract.providers.get(&provider))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "tokenizer provider `{provider}` is outside signed authority"
                ))
            })?;
        if text.len() as u64 > contract.max_input_bytes {
            return Err(AppRuntimeError::CapabilityDenied(
                "tokenizer input exceeds its signed size bound".to_string(),
            ));
        }
        let application = self.application_name().to_string();
        let mut evidence = ObserveRequest::Evidence {
            control: "tokenizer.count".to_string(),
            outcome: "success".to_string(),
            fields: BTreeMap::from([
                (
                    "application".to_string(),
                    Value::String(application.clone()),
                ),
                ("provider".to_string(), Value::String(provider.clone())),
            ]),
        };
        self.reserve_observation(&evidence)?;
        let result = self
            .services
            .tokenizer
            .count(&application, &provider, &text)
            .and_then(|tokens| {
                if tokens > contract.max_tokens {
                    return Err(AppRuntimeError::Provider(
                        "tokenizer result exceeds its signed token bound".to_string(),
                    ));
                }
                Ok(tokens)
            });
        if result.is_err() {
            let ObserveRequest::Evidence { outcome, .. } = &mut evidence else {
                unreachable!("tokenizer evidence is an evidence event")
            };
            *outcome = "error".to_string();
        }
        self.record_observation(evidence);
        result.map(HostValue::U64)
    }

    fn embeddings(&mut self, request: EmbeddingsRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::AiInference)?;
        self.require_capability(ExtensionCapability::Observability)?;
        let EmbeddingsRequest::Embed {
            provider,
            text,
            dimensions,
        } = request;
        let contract = self
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.embeddings.as_ref())
            .and_then(|contract| contract.providers.get(&provider))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "embeddings provider `{provider}` is outside signed authority"
                ))
            })?;
        if dimensions != contract.dimensions || text.len() as u64 > contract.max_input_bytes {
            return Err(AppRuntimeError::CapabilityDenied(
                "embedding request exceeds its signed dimension or size authority".to_string(),
            ));
        }
        let application = self.application_name().to_string();
        let mut evidence = ObserveRequest::Evidence {
            control: "embeddings.embed".to_string(),
            outcome: "success".to_string(),
            fields: BTreeMap::from([
                (
                    "application".to_string(),
                    Value::String(application.clone()),
                ),
                ("provider".to_string(), Value::String(provider.clone())),
                ("dimensions".to_string(), Value::from(dimensions)),
            ]),
        };
        self.reserve_observation(&evidence)?;
        let result = self
            .services
            .embeddings
            .embed(&application, &provider, &text, dimensions)
            .and_then(|embedding| {
                if embedding.len() != dimensions as usize
                    || embedding.iter().any(|value| !value.is_finite())
                {
                    return Err(AppRuntimeError::Provider(
                        "embedding provider returned an invalid vector".to_string(),
                    ));
                }
                serde_json::to_value(embedding).map_err(AppRuntimeError::from)
            });
        if result.is_err() {
            let ObserveRequest::Evidence { outcome, .. } = &mut evidence else {
                unreachable!("embedding evidence is an evidence event")
            };
            *outcome = "error".to_string();
        }
        self.record_observation(evidence);
        result.map(HostValue::Json)
    }

    fn llm(&mut self, request: LlmRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::AiInference)?;
        self.require_capability(ExtensionCapability::NetworkEgress)?;
        self.require_capability(ExtensionCapability::Observability)?;
        if let LlmRequest::Estimate {
            client,
            input_tokens,
            output_tokens,
        } = &request
        {
            let contract = self
                .application()
                .application_program
                .as_ref()
                .and_then(|program| program.llm.as_ref())
                .and_then(|contract| contract.clients.get(client))
                .ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied(format!(
                        "LLM client `{client}` is outside signed authority"
                    ))
                })?;
            if *input_tokens > 1_000_000_000 || *output_tokens > contract.max_output_tokens {
                return Err(AppRuntimeError::CapabilityDenied(
                    "LLM estimate exceeds signed token bounds".to_string(),
                ));
            }
            return self
                .services
                .llm
                .estimate_microusd(
                    self.application_name(),
                    &contract.provider,
                    *input_tokens,
                    *output_tokens,
                )
                .map(HostValue::U64);
        }
        let LlmRequest::Complete {
            client,
            method,
            user_prompt,
            continuation,
            history,
            conversation_id,
            output_type,
            allowed_tools,
            deadline_unix_ms,
        } = request
        else {
            unreachable!("LLM estimate returned above")
        };
        if deadline_unix_ms > self.actor.deadline_unix_ms || deadline_unix_ms <= now_ms() {
            return Err(AppRuntimeError::CapabilityDenied(
                "LLM request exceeds the invocation deadline".to_string(),
            ));
        }
        let program = self
            .application()
            .application_program
            .as_ref()
            .ok_or_else(|| AppRuntimeError::CapabilityDenied("LLM authority is absent".into()))?;
        let contract = program
            .llm
            .as_ref()
            .and_then(|contract| contract.clients.get(&client))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "LLM client `{client}` is outside signed authority"
                ))
            })?;
        if !contract.methods.contains(&method) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "LLM method `{client}.{method}` is outside signed authority"
            )));
        }
        let tools = match allowed_tools {
            Some(allowed) => {
                if allowed
                    .iter()
                    .any(|name| !contract.tools.contains_key(name))
                {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "LLM tool filter exceeds signed client authority".to_string(),
                    ));
                }
                contract
                    .tools
                    .iter()
                    .filter(|(name, _)| allowed.contains(*name))
                    .map(|(name, tool)| (name.clone(), tool.clone()))
                    .collect::<BTreeMap<_, _>>()
            }
            None => contract.tools.clone(),
        };
        let output_schema = match (method.as_str(), output_type.as_deref()) {
            ("respond" | "stream" | "stream_response", None) => None,
            ("respond_as", Some(output)) => Some(
                contract
                    .structured_outputs
                    .get(output)
                    .cloned()
                    .ok_or_else(|| {
                        AppRuntimeError::CapabilityDenied(format!(
                            "LLM structured output `{client}.{output}` is outside signed authority"
                        ))
                    })?,
            ),
            _ => {
                return Err(AppRuntimeError::CapabilityDenied(
                    "LLM method and structured-output authority do not match".to_string(),
                ));
            }
        };
        if (!continuation && user_prompt.is_empty())
            || (continuation && !user_prompt.is_empty())
            || history.len() > contract.max_history_messages as usize
            || user_prompt.len() as u64 > contract.max_prompt_bytes
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "LLM prompt or history exceeds signed bounds".to_string(),
            ));
        }
        let mut messages = Vec::new();
        if let Some(system_prompt) = &contract.system_prompt {
            messages.push(json!({"role": "system", "content": system_prompt}));
        }
        for message in history {
            let object = message.as_object().ok_or_else(|| {
                AppRuntimeError::InvalidRequest("LLM history entry must be an object".to_string())
            })?;
            let role = object.get("role").and_then(Value::as_str);
            let valid = match role {
                Some("user") => {
                    object.len() == 2
                        && object
                            .get("content")
                            .and_then(Value::as_str)
                            .is_some_and(|value| !value.is_empty())
                }
                Some("assistant") if object.contains_key("tool_calls") => {
                    object.len() == 3
                        && object
                            .get("content")
                            .is_some_and(|value| value.is_null() || value.as_str().is_some())
                        && object.get("tool_calls").is_some_and(Value::is_array)
                }
                Some("assistant") => {
                    object.len() == 2
                        && object
                            .get("content")
                            .and_then(Value::as_str)
                            .is_some_and(|value| !value.is_empty())
                }
                Some("tool") => {
                    object.len() == 4
                        && object
                            .get("tool_call_id")
                            .and_then(Value::as_str)
                            .is_some_and(|value| !value.is_empty())
                        && object
                            .get("name")
                            .and_then(Value::as_str)
                            .is_some_and(|value| tools.contains_key(value))
                        && object.get("content").and_then(Value::as_str).is_some()
                }
                _ => false,
            };
            if !valid {
                return Err(AppRuntimeError::InvalidRequest(
                    "LLM history entry is outside the bounded conversation schema".to_string(),
                ));
            }
            messages.push(message);
        }
        if !continuation {
            messages.push(json!({"role": "user", "content": user_prompt}));
        }
        let encoded_messages = serde_json::to_vec(&messages)?;
        if encoded_messages.len() as u64 > contract.max_prompt_bytes {
            return Err(AppRuntimeError::CapabilityDenied(
                "LLM messages exceed the signed prompt bound".to_string(),
            ));
        }
        let tokenizer_contract = program
            .tokenizer
            .as_ref()
            .and_then(|tokenizer| tokenizer.providers.get(&contract.tokenizer_provider))
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "LLM tokenizer authority is absent from the signed program".to_string(),
                )
            })?;
        let tokenizer_text = messages
            .iter()
            .filter_map(|message| message.get("content").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        if tokenizer_text.len() as u64 > tokenizer_contract.max_input_bytes {
            return Err(AppRuntimeError::CapabilityDenied(
                "LLM tokenizer input exceeds its signed size bound".to_string(),
            ));
        }
        let application = self.application_name().to_string();
        let input_tokens = self.services.tokenizer.count(
            &application,
            &contract.tokenizer_provider,
            &tokenizer_text,
        )?;
        if input_tokens > tokenizer_contract.max_tokens {
            return Err(AppRuntimeError::CapabilityDenied(
                "LLM prompt exceeds its signed token bound".to_string(),
            ));
        }
        let remaining = deadline_unix_ms.saturating_sub(now_ms()).max(1) as u64;
        let mut evidence = ObserveRequest::Evidence {
            control: "llm.complete".to_string(),
            outcome: "success".to_string(),
            fields: BTreeMap::from([
                (
                    "application".to_string(),
                    Value::String(application.clone()),
                ),
                (
                    "provider".to_string(),
                    Value::String(contract.provider.clone()),
                ),
                ("client".to_string(), Value::String(client.clone())),
                ("method".to_string(), Value::String(method.clone())),
                ("input_tokens".to_string(), Value::from(input_tokens)),
            ]),
        };
        self.reserve_observation(&evidence)?;
        let stream_provider = if method == "stream_response" {
            self.require_capability(ExtensionCapability::Streaming)?;
            let scope = self
                .actor
                .correlation_id
                .as_deref()
                .unwrap_or(&self.actor.trace_id)
                .to_string();
            match self.llm_response_stream.as_ref() {
                Some((active_scope, stream)) if active_scope == &scope => Some(*stream),
                Some(_) => {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "one invocation cannot open multiple live LLM response scopes".to_string(),
                    ));
                }
                None => {
                    let stream = self.services.realtime.open_scoped(
                        200,
                        &[],
                        bicdb_extension::abi_v2::StreamKind::ServerSentEvents,
                        &scope,
                    )?;
                    self.llm_response_stream = Some((scope, stream));
                    Some(stream)
                }
            }
        } else {
            None
        };
        let provider_request = LlmProviderRequest {
            messages,
            output_type: output_type.clone(),
            output_schema: output_schema.clone(),
            tools: tools.clone(),
            deadline_ms: remaining,
            trace_id: self.actor.trace_id.clone(),
        };
        let provider_result = if let Some(stream) = stream_provider {
            let realtime = Arc::clone(&self.services.realtime);
            let mut emit = |bytes: &[u8]| realtime.send(stream, bytes);
            self.services.llm.stream(
                &application,
                &contract.provider,
                &contract,
                provider_request,
                &mut emit,
            )
        } else {
            self.services.llm.complete(
                &application,
                &contract.provider,
                &contract,
                provider_request,
            )
        };
        let mut result = provider_result.and_then(|response| {
            if response.provider != contract.provider
                || response.model.is_empty()
                || contract
                    .model
                    .as_ref()
                    .is_some_and(|model| model != &response.model)
                || response.text.len() as u64 > contract.max_response_bytes
                || response.structured_output.as_ref().is_some_and(|value| {
                    serde_json::to_vec(value)
                        .map(|value| value.len() as u64 > contract.max_response_bytes)
                        .unwrap_or(true)
                })
            {
                return Err(AppRuntimeError::Provider(
                    "LLM provider returned an invalid or oversized response".to_string(),
                ));
            }
            if response.tool_calls.len() > 128 {
                return Err(AppRuntimeError::Provider(
                    "LLM provider returned too many tool calls".to_string(),
                ));
            }
            let mut tool_ids = BTreeSet::new();
            let mut tool_requests = Vec::new();
            let mut assistant_tool_calls = Vec::new();
            for call in &response.tool_calls {
                let tool = tools.get(&call.name).ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied(format!(
                        "LLM requested undeclared tool `{}`",
                        call.name
                    ))
                })?;
                if call.id.len() > 1_024 || !tool_ids.insert(call.id.as_str()) {
                    return Err(AppRuntimeError::Provider(
                        "LLM provider returned an invalid tool call id".to_string(),
                    ));
                }
                let supplied = call.arguments.as_object().ok_or_else(|| {
                    AppRuntimeError::Provider("LLM tool arguments must be an object".to_string())
                })?;
                if supplied.keys().any(|name| {
                    !tool
                        .parameters
                        .iter()
                        .any(|parameter| parameter.name == *name)
                }) {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "LLM tool `{}` received an undeclared argument",
                        call.name
                    )));
                }
                let mut arguments = Map::new();
                for parameter in &tool.parameters {
                    let value = match supplied.get(&parameter.name) {
                        Some(value) => crate::http::coerce_carrier_parameter_value(
                            value,
                            &parameter.value_type,
                            "llm_tool",
                            &parameter.name,
                        )?,
                        None if parameter.optional => Value::Null,
                        None => {
                            return Err(AppRuntimeError::CapabilityDenied(format!(
                                "LLM tool `{}` omitted required argument `{}`",
                                call.name, parameter.name
                            )));
                        }
                    };
                    arguments.insert(parameter.name.clone(), value);
                }
                let arguments = Value::Object(arguments);
                let encoded_arguments = serde_json::to_string(&arguments)?;
                tool_requests.push(json!({
                    "id": call.id,
                    "name": call.name,
                    "arguments": arguments,
                }));
                assistant_tool_calls.push(json!({
                    "id": call.id,
                    "type": "function",
                    "function": {
                        "name": call.name,
                        "arguments": encoded_arguments,
                    }
                }));
            }
            let structured_output = match (output_schema.as_ref(), response.structured_output) {
                (Some(schema), Some(value)) => Some(crate::http::coerce_carrier_parameter_value(
                    &value,
                    schema,
                    "llm",
                    output_type.as_deref().unwrap_or("structured_output"),
                )?),
                (Some(_), None) if !response.tool_calls.is_empty() => None,
                (Some(_), None) => {
                    return Err(AppRuntimeError::Provider(
                        "LLM provider omitted required structured output".to_string(),
                    ));
                }
                (None, _) => None,
            };
            for usage in [
                response.input_tokens,
                response.output_tokens,
                response.total_tokens,
            ]
            .into_iter()
            .flatten()
            {
                if usage < 0 {
                    return Err(AppRuntimeError::Provider(
                        "LLM provider returned invalid token usage".to_string(),
                    ));
                }
            }
            if response.output_tokens.is_some_and(|tokens| {
                u64::try_from(tokens)
                    .map(|tokens| tokens > contract.max_output_tokens)
                    .unwrap_or(true)
            }) || response
                .input_tokens
                .zip(response.output_tokens)
                .zip(response.total_tokens)
                .is_some_and(|((input, output), total)| {
                    input.checked_add(output).is_none_or(|sum| total < sum)
                })
            {
                return Err(AppRuntimeError::Provider(
                    "LLM provider returned token usage outside signed bounds".to_string(),
                ));
            }
            let assistant_message = if assistant_tool_calls.is_empty() {
                json!({"role": "assistant", "content": response.text})
            } else {
                json!({
                    "role": "assistant",
                    "content": if response.text.is_empty() {
                        Value::Null
                    } else {
                        Value::String(response.text.clone())
                    },
                    "tool_calls": assistant_tool_calls,
                })
            };
            Ok(json!({
                "text": response.text,
                "provider": response.provider,
                "model": response.model,
                "conversation_id": conversation_id,
                "tool_calls": tool_requests.len(),
                "tool_requests": tool_requests,
                "assistant_message": assistant_message,
                "input_tokens": response.input_tokens,
                "output_tokens": response.output_tokens,
                "total_tokens": response.total_tokens,
                "stream_path": Value::Null,
                "structured_output": structured_output,
            }))
        });
        if let Some(stream) = stream_provider {
            if let Ok(value) = result.as_mut() {
                value["__carrier_stream_handle"] = Value::from(stream);
            }
            let final_turn = result
                .as_ref()
                .map(|value| {
                    value
                        .get("tool_requests")
                        .and_then(Value::as_array)
                        .is_none_or(Vec::is_empty)
                })
                .unwrap_or(true);
            if final_turn {
                if let Err(error) = &result {
                    let envelope = serde_json::to_vec(&json!({
                        "error": {
                            "type": "bicdb_stream_error",
                            "code": "llm_stream_failed",
                            "message": error.to_string(),
                        }
                    }))?;
                    let mut frame = b"data: ".to_vec();
                    frame.extend_from_slice(&envelope);
                    frame.extend_from_slice(b"\n\ndata: [DONE]\n\n");
                    let _ = self.services.realtime.send(stream, &frame);
                }
                let _ = self.services.realtime.close(stream, &[]);
                self.llm_response_stream = None;
            }
        }
        if result.is_err() {
            let ObserveRequest::Evidence { outcome, .. } = &mut evidence else {
                unreachable!("LLM evidence is an evidence event")
            };
            *outcome = "error".to_string();
        }
        self.record_observation(evidence);
        result.map(HostValue::Json)
    }

    fn redis(&mut self, request: RedisRequest) -> Result<HostValue> {
        enum Operation {
            Publish {
                provider: String,
                channel: String,
                message: String,
            },
            Incr {
                provider: String,
                key: String,
            },
        }

        self.require_capability(ExtensionCapability::NetworkEgress)?;
        self.require_capability(ExtensionCapability::Observability)?;
        let contract = self
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.redis.as_ref())
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "Redis-compatible helper authority is undeclared".to_string(),
                )
            })?;
        let application = self.application_name().to_string();
        let (helper, subject, operation) = match request {
            RedisRequest::Publish {
                provider,
                channel,
                message,
            } => {
                if provider != contract.provider
                    || !contract.helpers.contains("redis.publish")
                    || channel.is_empty()
                    || channel.len() > contract.max_channel_bytes as usize
                    || message.len() > contract.max_message_bytes as usize
                {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "Redis-compatible publish request exceeds signed authority".to_string(),
                    ));
                }
                let subject = redis_subject_hash(b"channel", channel.as_bytes());
                (
                    "redis.publish",
                    subject,
                    Operation::Publish {
                        provider,
                        channel,
                        message,
                    },
                )
            }
            RedisRequest::Incr { provider, key } => {
                if provider != contract.provider
                    || !contract.helpers.contains("redis.incr")
                    || key.is_empty()
                    || key.len() > contract.max_key_bytes as usize
                {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "Redis-compatible increment request exceeds signed authority".to_string(),
                    ));
                }
                let subject = redis_subject_hash(b"key", key.as_bytes());
                ("redis.incr", subject, Operation::Incr { provider, key })
            }
        };
        let mut evidence = ObserveRequest::Evidence {
            control: helper.to_string(),
            // Reserve for the longer final outcome before the external side
            // effect. This prevents evidence quota from turning a successful
            // non-idempotent increment into a reported failure.
            outcome: "success".to_string(),
            fields: BTreeMap::from([
                (
                    "application".to_string(),
                    Value::String(application.clone()),
                ),
                (
                    "provider".to_string(),
                    Value::String(contract.provider.clone()),
                ),
                ("subject_sha256".to_string(), Value::String(subject)),
            ]),
        };
        self.reserve_observation(&evidence)?;
        let result = match operation {
            Operation::Publish {
                provider,
                channel,
                message,
            } => self
                .services
                .redis
                .publish(&application, &provider, &channel, &message),
            Operation::Incr { provider, key } => self.services.redis.incr(
                &application,
                &provider,
                self.actor.tenant_id.as_deref(),
                &key,
            ),
        };
        if result.is_err() {
            let ObserveRequest::Evidence { outcome, .. } = &mut evidence else {
                unreachable!("Redis evidence is an evidence event")
            };
            *outcome = "error".to_string();
        }
        self.record_observation(evidence);
        result.map(HostValue::I64)
    }

    fn email(&mut self, request: EmailRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::NetworkEgress)?;
        self.require_capability(ExtensionCapability::Observability)?;
        let contract = self
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.email.as_ref())
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "email helper authority is undeclared".to_string(),
                )
            })?;
        let EmailRequest::Send {
            provider,
            from,
            to,
            cc,
            bcc,
            reply_to,
            subject,
            text,
            html,
        } = request;
        let recipient_count = to.len().saturating_add(cc.len()).saturating_add(bcc.len());
        let valid_address = |value: &str| {
            !value.is_empty()
                && value.len() <= contract.max_address_bytes as usize
                && !value.chars().any(char::is_control)
        };
        let body_bytes = text
            .as_ref()
            .map_or(0, String::len)
            .saturating_add(html.as_ref().map_or(0, String::len));
        if provider != contract.provider
            || contract.helper != "email.send"
            || recipient_count == 0
            || recipient_count > contract.max_recipients as usize
            || !valid_address(&from)
            || to
                .iter()
                .chain(&cc)
                .chain(&bcc)
                .any(|value| !valid_address(value))
            || reply_to
                .as_deref()
                .is_some_and(|value| !valid_address(value))
            || subject.len() > contract.max_subject_bytes as usize
            || subject.chars().any(char::is_control)
            || body_bytes == 0
            || body_bytes > contract.max_body_bytes as usize
            || text
                .as_ref()
                .is_some_and(|value| value.chars().any(|value| value == '\0'))
            || html
                .as_ref()
                .is_some_and(|value| value.chars().any(|value| value == '\0'))
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "email request exceeds signed authority".to_string(),
            ));
        }
        let recipient_subject = serde_json::to_vec(&(&from, &to, &cc, &bcc, &reply_to))?;
        let application = self.application_name().to_string();
        let mut evidence = ObserveRequest::Evidence {
            control: "email.send".to_string(),
            outcome: "success".to_string(),
            fields: BTreeMap::from([
                (
                    "application".to_string(),
                    Value::String(application.clone()),
                ),
                (
                    "provider".to_string(),
                    Value::String(contract.provider.clone()),
                ),
                (
                    "recipient_count".to_string(),
                    Value::from(recipient_count as u64),
                ),
                (
                    "recipient_set_sha256".to_string(),
                    Value::String(email_subject_hash(&recipient_subject)),
                ),
            ]),
        };
        self.reserve_observation(&evidence)?;
        let result = self
            .services
            .email
            .send(
                &application,
                &provider,
                EmailMessage {
                    from,
                    to,
                    cc,
                    bcc,
                    reply_to,
                    subject,
                    text,
                    html,
                },
            )
            .and_then(|delivery| {
                if !delivery.accepted
                    || delivery.delivery_id.is_empty()
                    || delivery.delivery_id.len() > 512
                    || delivery.status.is_empty()
                    || delivery.status.len() > 128
                    || delivery.transport.is_empty()
                    || delivery.transport.len() > 128
                    || delivery.delivery_id.chars().any(char::is_control)
                    || delivery.status.chars().any(char::is_control)
                    || delivery.transport.chars().any(char::is_control)
                {
                    return Err(AppRuntimeError::Provider(
                        "email provider returned an invalid delivery receipt".to_string(),
                    ));
                }
                Ok(HostValue::Json(serde_json::json!({
                    "accepted": delivery.accepted,
                    "delivery_id": delivery.delivery_id,
                    "status": delivery.status,
                    "transport": delivery.transport,
                })))
            });
        if result.is_err() {
            let ObserveRequest::Evidence { outcome, .. } = &mut evidence else {
                unreachable!("email evidence is an evidence event")
            };
            *outcome = "error".to_string();
        }
        self.record_observation(evidence);
        result
    }

    fn blob(&mut self, request: BlobRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::Blobs)?;
        match request {
            BlobRequest::CreateUpload {
                namespace,
                content_type,
                metadata,
            } => {
                let declaration = self.blob_declaration(&namespace)?;
                if content_type.as_ref().is_some_and(|content_type| {
                    !declaration.content_types.is_empty()
                        && !declaration.content_types.contains(content_type)
                }) {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "content type is undeclared for blob namespace `{namespace}`"
                    )));
                }
                let handle = self.next_handle(&self.uploads);
                self.uploads.insert(
                    handle,
                    Upload {
                        namespace,
                        named_key: None,
                        content_type,
                        metadata,
                        bytes: Vec::new(),
                    },
                );
                Ok(HostValue::Handle(handle))
            }
            BlobRequest::Write { upload, bytes } => {
                if bytes.len() > self.services.max_blob_chunk_bytes {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "blob chunk exceeds host limit".to_string(),
                    ));
                }
                let (namespace, current_len) = self
                    .uploads
                    .get(&upload)
                    .map(|upload| (upload.namespace.clone(), upload.bytes.len()))
                    .ok_or_else(|| {
                        AppRuntimeError::CapabilityDenied(
                            "forged or expired upload handle".to_string(),
                        )
                    })?;
                let maximum = self.blob_declaration(&namespace)?.max_blob_bytes as usize;
                if current_len.saturating_add(bytes.len()) > maximum {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "blob upload exceeds declared size".to_string(),
                    ));
                }
                self.uploads
                    .get_mut(&upload)
                    .expect("upload validated above")
                    .bytes
                    .extend_from_slice(&bytes);
                Ok(HostValue::U64(bytes.len() as u64))
            }
            BlobRequest::Finish { upload } => {
                let upload = self.uploads.remove(&upload).ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied("forged or expired upload handle".to_string())
                })?;
                let require_scan = self.blob_declaration(&upload.namespace)?.require_scan;
                let provider_namespace = self.provider_blob_namespace(&upload.namespace);
                let metadata = match upload.named_key {
                    Some(key) => self.services.blobs.put_named(
                        &provider_namespace,
                        &key,
                        &upload.bytes,
                        upload.content_type.as_deref(),
                        &upload.metadata,
                        require_scan,
                    )?,
                    None => self.services.blobs.put(
                        &provider_namespace,
                        &upload.bytes,
                        upload.content_type.as_deref(),
                        &upload.metadata,
                        require_scan,
                    )?,
                };
                Ok(HostValue::BlobMetadata(
                    self.logical_blob_metadata(metadata, &upload.namespace),
                ))
            }
            BlobRequest::OpenRead { namespace, blob_id } => {
                self.blob_declaration(&namespace)?;
                let provider_namespace = self.provider_blob_namespace(&namespace);
                let record = self
                    .services
                    .blobs
                    .get(&provider_namespace, &blob_id)?
                    .ok_or_else(|| AppRuntimeError::Provider("blob not found".to_string()))?;
                let handle = self.next_handle(&self.blob_reads);
                self.blob_reads
                    .insert(handle, BlobRead { record, offset: 0 });
                Ok(HostValue::Handle(handle))
            }
            BlobRequest::Read { blob, max_bytes } => {
                let limit = (max_bytes as usize).min(self.services.max_blob_chunk_bytes);
                let read = self.blob_reads.get_mut(&blob).ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied(
                        "forged or expired blob read handle".to_string(),
                    )
                })?;
                let end = read
                    .offset
                    .saturating_add(limit)
                    .min(read.record.bytes.len());
                let bytes = read.record.bytes[read.offset..end].to_vec();
                read.offset = end;
                Ok(HostValue::Bytes(bytes))
            }
            BlobRequest::Delete { namespace, blob_id } => {
                self.blob_declaration(&namespace)?;
                let provider_namespace = self.provider_blob_namespace(&namespace);
                Ok(HostValue::Bool(
                    self.services.blobs.delete(&provider_namespace, &blob_id)?,
                ))
            }
            BlobRequest::Metadata { namespace, blob_id } => {
                self.blob_declaration(&namespace)?;
                let provider_namespace = self.provider_blob_namespace(&namespace);
                let metadata = self
                    .services
                    .blobs
                    .get(&provider_namespace, &blob_id)?
                    .map(|record| record.metadata)
                    .ok_or_else(|| AppRuntimeError::Provider("blob not found".to_string()))?;
                Ok(HostValue::BlobMetadata(
                    self.logical_blob_metadata(metadata, &namespace),
                ))
            }
            BlobRequest::SignedUrl {
                namespace,
                blob_id,
                expires_seconds,
            } => {
                let declaration = self.blob_declaration(&namespace)?;
                if !declaration.allow_signed_urls {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "signed URLs are undeclared for this blob namespace".to_string(),
                    ));
                }
                let provider_namespace = self.provider_blob_namespace(&namespace);
                Ok(HostValue::String(self.services.blobs.signed_url(
                    &provider_namespace,
                    &blob_id,
                    expires_seconds,
                )?))
            }
            BlobRequest::Attach {
                transaction,
                grant,
                namespace,
                blob_id,
                relation,
                record_id,
                field,
            } => {
                self.blob_declaration(&namespace)?;
                let provider_namespace = self.provider_blob_namespace(&namespace);
                let metadata = self
                    .services
                    .blobs
                    .get(&provider_namespace, &blob_id)?
                    .map(|record| record.metadata)
                    .ok_or_else(|| AppRuntimeError::Provider("blob not found".to_string()))?;
                let mut record = self
                    .transaction_mut(transaction)?
                    .get_authorized(&relation, &record_id)?
                    .ok_or_else(|| {
                        AppRuntimeError::Provider("attachment row not found".to_string())
                    })?
                    .as_ref()
                    .clone();
                let object = record.metadata.as_object_mut().ok_or_else(|| {
                    AppRuntimeError::Provider("record metadata is not an object".to_string())
                })?;
                object.insert(
                    field,
                    json!({
                        "namespace": namespace,
                        "blob_id": blob_id,
                        "size": metadata.size,
                        "sha256": metadata.sha256,
                        "content_type": metadata.content_type,
                    }),
                );
                let grant = self.grant(transaction, grant)?;
                self.transaction_mut(transaction)?
                    .update_with_grant(grant, &relation, record)?;
                Ok(HostValue::Unit)
            }
            BlobRequest::CreateNamedUpload {
                namespace,
                key,
                content_type,
                metadata,
            } => {
                let declaration = self.blob_declaration(&namespace)?;
                if content_type.as_ref().is_some_and(|content_type| {
                    !declaration.content_types.is_empty()
                        && !declaration.content_types.contains(content_type)
                }) {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "content type is undeclared for blob namespace `{namespace}`"
                    )));
                }
                let handle = self.next_handle(&self.uploads);
                self.uploads.insert(
                    handle,
                    Upload {
                        namespace,
                        named_key: Some(key),
                        content_type,
                        metadata,
                        bytes: Vec::new(),
                    },
                );
                Ok(HostValue::Handle(handle))
            }
            BlobRequest::OpenNamedRead { namespace, key } => {
                self.blob_declaration(&namespace)?;
                let provider_namespace = self.provider_blob_namespace(&namespace);
                let mut record = self
                    .services
                    .blobs
                    .get_named(&provider_namespace, &key)?
                    .ok_or_else(|| AppRuntimeError::NotFound("blob not found".to_string()))?;
                record.metadata = self.logical_blob_metadata(record.metadata, &namespace);
                let handle = self.next_handle(&self.blob_reads);
                self.blob_reads
                    .insert(handle, BlobRead { record, offset: 0 });
                Ok(HostValue::Handle(handle))
            }
            BlobRequest::DeleteNamed { namespace, key } => {
                self.blob_declaration(&namespace)?;
                let provider_namespace = self.provider_blob_namespace(&namespace);
                Ok(HostValue::Bool(
                    self.services
                        .blobs
                        .delete_named(&provider_namespace, &key)?,
                ))
            }
            BlobRequest::NamedMetadata { namespace, key } => {
                self.blob_declaration(&namespace)?;
                let provider_namespace = self.provider_blob_namespace(&namespace);
                let metadata = self
                    .services
                    .blobs
                    .get_named(&provider_namespace, &key)?
                    .map(|record| record.metadata)
                    .ok_or_else(|| AppRuntimeError::NotFound("blob not found".to_string()))?;
                Ok(HostValue::BlobMetadata(
                    self.logical_blob_metadata(metadata, &namespace),
                ))
            }
            BlobRequest::NamedSignedUrl {
                namespace,
                key,
                expires_seconds,
                method,
                download_name,
            } => {
                let declaration = self.blob_declaration(&namespace)?;
                if !declaration.allow_signed_urls {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "signed URLs are undeclared for this blob namespace".to_string(),
                    ));
                }
                let provider_namespace = self.provider_blob_namespace(&namespace);
                Ok(HostValue::String(self.services.blobs.signed_url_named(
                    &provider_namespace,
                    &key,
                    expires_seconds,
                    &method,
                    download_name.as_deref(),
                )?))
            }
        }
    }

    fn provider_blob_namespace(&self, namespace: &str) -> String {
        application_blob_provider_namespace(&self.extension.identity.name, namespace)
    }

    fn logical_blob_metadata(
        &self,
        mut metadata: bicdb_extension::abi_v2::BlobMetadata,
        namespace: &str,
    ) -> bicdb_extension::abi_v2::BlobMetadata {
        metadata.namespace = namespace.to_string();
        metadata
    }

    fn blob_declaration(
        &self,
        namespace: &str,
    ) -> Result<&bicdb_extension::abi_v2::BlobDeclaration> {
        self.application()
            .blobs
            .iter()
            .find(|declaration| declaration.namespace == namespace)
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "blob namespace `{namespace}` is undeclared"
                ))
            })
    }

    fn stream(&mut self, request: StreamRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::Streaming)?;
        match request {
            StreamRequest::OpenResponse {
                status,
                headers,
                kind,
            } => {
                let provider = self.services.realtime.open(status, &headers, kind)?;
                let handle = HostHandle(provider);
                if provider == 0 || self.streams.contains_key(&handle) {
                    let _ = self.services.realtime.close(provider, &[]);
                    return Err(AppRuntimeError::Provider(
                        "realtime provider returned an invalid stream identity".to_string(),
                    ));
                }
                self.streams.insert(handle, provider);
                Ok(HostValue::Handle(handle))
            }
            StreamRequest::Send { stream, bytes } => {
                let provider = *self.streams.get(&stream).ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied("forged or closed stream handle".to_string())
                })?;
                self.services.realtime.send(provider, &bytes)?;
                Ok(HostValue::Unit)
            }
            StreamRequest::SendEvent { stream, event } => {
                let provider = *self.streams.get(&stream).ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied("forged or closed stream handle".to_string())
                })?;
                let mut encoded = String::new();
                if let Some(id) = event.id {
                    encoded.push_str("id: ");
                    encoded.push_str(&id.replace(['\r', '\n'], ""));
                    encoded.push('\n');
                }
                if let Some(kind) = event.event {
                    encoded.push_str("event: ");
                    encoded.push_str(&kind.replace(['\r', '\n'], ""));
                    encoded.push('\n');
                }
                if let Some(retry) = event.retry_ms {
                    encoded.push_str(&format!("retry: {retry}\n"));
                }
                for line in event.data.lines() {
                    encoded.push_str("data: ");
                    encoded.push_str(line);
                    encoded.push('\n');
                }
                encoded.push('\n');
                self.services.realtime.send(provider, encoded.as_bytes())?;
                Ok(HostValue::Unit)
            }
            StreamRequest::Close { stream, trailers } => {
                let provider = self.streams.remove(&stream).ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied("forged or closed stream handle".to_string())
                })?;
                self.services.realtime.close(provider, &trailers)?;
                Ok(HostValue::Unit)
            }
            StreamRequest::Receive { stream, max_bytes } => {
                let provider = *self.streams.get(&stream).ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied("forged or closed stream handle".to_string())
                })?;
                Ok(HostValue::Bytes(self.services.realtime.receive(
                    provider,
                    (max_bytes as usize).min(1024 * 1024),
                )?))
            }
        }
    }

    fn observe(&mut self, request: ObserveRequest) -> Result<HostValue> {
        self.require_capability(ExtensionCapability::Observability)?;
        self.reserve_observation(&request)?;
        let request = self.sanitize_observation(request)?;
        self.record_observation(request);
        Ok(HostValue::Unit)
    }

    pub(crate) fn observe_carrier_audit(
        &mut self,
        transaction: Option<HostHandle>,
        action: String,
        subject: String,
        fields: BTreeMap<String, Value>,
    ) -> Result<()> {
        self.require_capability(ExtensionCapability::Observability)?;
        let contract = self
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.observability.as_ref())
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "BicDB application audit lacks a signed observability contract".to_string(),
                )
            })?;
        if !contract.helpers.contains("audit.record")
            || (!contract.dynamic_audit_actions && !contract.audit_actions.contains(&action))
            || !contract.durable_audit
        {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "BicDB application audit action `{action}` is outside signed durable-audit authority"
            )));
        }
        let request = ObserveRequest::Audit {
            action,
            subject,
            fields,
        };
        self.reserve_observation(&request)?;
        let request = self.sanitize_observation_with_contract(request, &contract)?;
        let ObserveRequest::Audit {
            action,
            subject,
            fields,
        } = request
        else {
            unreachable!("BicDB application audit sanitization preserves its request kind")
        };

        let owned = transaction.is_none();
        let transaction = match transaction {
            Some(transaction) => transaction,
            None => match self.transaction(TransactionRequest::Begin {
                isolation: IsolationLevel::ReadCommitted,
            })? {
                HostValue::Handle(transaction) => transaction,
                _ => unreachable!("transaction begin returns a handle"),
            },
        };
        let durable = self.register_durable_audit(
            transaction,
            action.clone(),
            subject.clone(),
            fields.clone(),
        );
        if owned {
            match durable {
                Ok(()) => {
                    self.transaction(TransactionRequest::Commit { transaction })?;
                }
                Err(error) => {
                    let _ = self.transaction(TransactionRequest::Rollback { transaction });
                    return Err(error);
                }
            }
        } else {
            durable?;
        }
        self.record_observation(ObserveRequest::Audit {
            action,
            subject,
            fields,
        });
        Ok(())
    }

    fn reserve_observation(&mut self, request: &ObserveRequest) -> Result<()> {
        let bytes = serde_json::to_vec(&request)?.len();
        if self
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.observability.as_ref())
            .is_some_and(|contract| bytes as u64 > contract.max_field_bytes)
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "BicDB application observation exceeds its signed field-byte limit".to_string(),
            ));
        }
        if self.observations >= self.services.max_observability_events
            || self.observation_bytes.saturating_add(bytes) > self.services.max_observability_bytes
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "observability quota exceeded".to_string(),
            ));
        }
        self.observations += 1;
        self.observation_bytes += bytes;
        Ok(())
    }

    fn sanitize_observation(&self, request: ObserveRequest) -> Result<ObserveRequest> {
        let Some(contract) = self
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.observability.as_ref())
        else {
            return Ok(request);
        };
        self.sanitize_observation_with_contract(request, contract)
    }

    fn sanitize_observation_with_contract(
        &self,
        request: ObserveRequest,
        contract: &ApplicationObservabilityContractV1,
    ) -> Result<ObserveRequest> {
        let redacted = contract
            .redacted_keys
            .iter()
            .map(|key| key.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let fields = |fields: BTreeMap<String, Value>| {
            redact_observation_fields(fields, &redacted, contract.max_field_depth)
        };
        Ok(match request {
            ObserveRequest::Log {
                level,
                message,
                fields: values,
            } => ObserveRequest::Log {
                level,
                message,
                fields: fields(values),
            },
            ObserveRequest::TraceEvent {
                name,
                fields: values,
            } => ObserveRequest::TraceEvent {
                name,
                fields: fields(values),
            },
            ObserveRequest::Metric {
                name,
                kind,
                value,
                labels,
            } => ObserveRequest::Metric {
                name,
                kind,
                value,
                labels: labels
                    .into_iter()
                    .map(|(key, value)| {
                        if redacted.contains(&key.to_ascii_lowercase()) {
                            (key, "[REDACTED]".to_string())
                        } else {
                            (key, value)
                        }
                    })
                    .collect(),
            },
            ObserveRequest::Audit {
                action,
                subject,
                fields: values,
            } => ObserveRequest::Audit {
                action,
                subject,
                fields: fields(values),
            },
            ObserveRequest::Evidence {
                control,
                outcome,
                fields: values,
            } => ObserveRequest::Evidence {
                control,
                outcome,
                fields: fields(values),
            },
        })
    }

    fn record_observation(&self, request: ObserveRequest) {
        let application = self.application_name().to_string();
        let service_name = self
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.observability.as_ref())
            .map(|contract| contract.service_name.clone())
            .unwrap_or_else(|| application.clone());
        let enrich = |mut fields: BTreeMap<String, Value>| {
            fields
                .entry("application".to_string())
                .or_insert_with(|| Value::String(application.clone()));
            fields
                .entry("service.name".to_string())
                .or_insert_with(|| Value::String(service_name.clone()));
            fields
        };
        let event = match request {
            ObserveRequest::Log {
                level,
                message,
                fields,
            } => ObservabilityEvent::Log {
                actor: self.actor.clone(),
                level,
                message,
                fields: enrich(fields),
            },
            ObserveRequest::TraceEvent { name, fields } => ObservabilityEvent::Trace {
                actor: self.actor.clone(),
                name,
                fields: enrich(fields),
            },
            ObserveRequest::Metric {
                name,
                kind,
                value,
                labels,
            } => {
                let mut labels = labels;
                labels
                    .entry("bicdb.application".to_string())
                    .or_insert_with(|| application.clone());
                labels
                    .entry("service.name".to_string())
                    .or_insert_with(|| service_name.clone());
                ObservabilityEvent::Metric {
                    actor: self.actor.clone(),
                    name,
                    kind,
                    value,
                    labels,
                }
            }
            ObserveRequest::Audit {
                action,
                subject,
                fields,
            } => ObservabilityEvent::Audit {
                actor: self.actor.clone(),
                action,
                subject,
                fields: enrich(fields),
            },
            ObserveRequest::Evidence {
                control,
                outcome,
                fields,
            } => ObservabilityEvent::Evidence {
                actor: self.actor.clone(),
                control,
                outcome,
                fields: enrich(fields),
            },
        };
        self.services.observability.record(event);
    }
}

fn redact_observation_fields(
    fields: BTreeMap<String, Value>,
    redacted_keys: &BTreeSet<String>,
    max_depth: u16,
) -> BTreeMap<String, Value> {
    fields
        .into_iter()
        .map(|(key, value)| {
            let value = if redacted_keys.contains(&key.to_ascii_lowercase()) {
                Value::String("[REDACTED]".to_string())
            } else {
                redact_observation_value(value, redacted_keys, max_depth, 1)
            };
            (key, value)
        })
        .collect()
}

fn redact_observation_value(
    value: Value,
    redacted_keys: &BTreeSet<String>,
    max_depth: u16,
    depth: u16,
) -> Value {
    if depth >= max_depth && matches!(value, Value::Array(_) | Value::Object(_)) {
        return Value::String("[TRUNCATED]".to_string());
    }
    match value {
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| {
                    let value = if redacted_keys.contains(&key.to_ascii_lowercase()) {
                        Value::String("[REDACTED]".to_string())
                    } else {
                        redact_observation_value(
                            value,
                            redacted_keys,
                            max_depth,
                            depth.saturating_add(1),
                        )
                    };
                    (key, value)
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|value| {
                    redact_observation_value(
                        value,
                        redacted_keys,
                        max_depth,
                        depth.saturating_add(1),
                    )
                })
                .collect(),
        ),
        value => value,
    }
}

pub(crate) fn application_blob_provider_namespace(application: &str, namespace: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bicdb-app-blob-v1\0");
    digest.update(application.as_bytes());
    format!("{}-{namespace}", &encode_hex(&digest.finalize())[..24])
}

impl ApplicationHost for CapabilityHost {
    fn call(&mut self, call: HostCall) -> HostCallResult {
        let request_id = call.request_id;
        let parent_causation = self
            .actor
            .causation_id
            .replace(format!("host:{request_id}"));
        let previous_parent = self
            .actor
            .policy_attributes
            .get("carrier.parent_causation_id")
            .cloned();
        if let Some(parent) = &parent_causation {
            self.actor
                .policy_attributes
                .insert("carrier.parent_causation_id".to_string(), parent.clone());
        } else {
            self.actor
                .policy_attributes
                .remove("carrier.parent_causation_id");
        }
        let capability = match &call.request {
            HostRequest::Transaction(_) => "transaction",
            HostRequest::Database(_) => "database",
            HostRequest::MutationGrant(_) => "mutation_grant",
            HostRequest::Broker(_) => "broker",
            HostRequest::Service(_) => "plugin_service",
            HostRequest::Clock(_) => "clock",
            HostRequest::Random(_) => "random",
            HostRequest::Secret(_) => "secret",
            HostRequest::Crypto(_) => "crypto",
            HostRequest::Egress(_) => "egress",
            HostRequest::Grpc(_) => "grpc",
            HostRequest::Tokenizer(_) => "tokenizer",
            HostRequest::Embeddings(_) => "embeddings",
            HostRequest::Llm(_) => "llm",
            HostRequest::Redis(_) => "redis",
            HostRequest::Email(_) => "email",
            HostRequest::Blob(_) => "blob",
            HostRequest::Stream(_) => "stream",
            HostRequest::Observe(_) => "observability",
        };
        let started = Instant::now();
        let result = self.dispatch(call.request);
        if crate::runtime::actor_trace_sampled(&self.actor) {
            self.services
                .observability
                .record(ObservabilityEvent::Trace {
                    actor: self.actor.clone(),
                    name: "bicdb.host_call".to_string(),
                    fields: BTreeMap::from([
                        (
                            "application".to_string(),
                            Value::String(self.application_name().to_string()),
                        ),
                        (
                            "capability".to_string(),
                            Value::String(capability.to_string()),
                        ),
                        (
                            "elapsed_us".to_string(),
                            Value::from(started.elapsed().as_micros() as u64),
                        ),
                        ("success".to_string(), Value::Bool(result.is_ok())),
                    ]),
                });
        }
        self.actor.causation_id = parent_causation;
        match previous_parent {
            Some(value) => {
                self.actor
                    .policy_attributes
                    .insert("carrier.parent_causation_id".to_string(), value);
            }
            None => {
                self.actor
                    .policy_attributes
                    .remove("carrier.parent_causation_id");
            }
        }
        match result {
            Ok(value) => HostCallResult::success(request_id, value),
            Err(error) => {
                eprintln!(
                    "bicdb application host call failed: trace_id={} application={} capability={} error={error}",
                    self.actor.trace_id,
                    self.application_name(),
                    capability,
                );
                HostCallResult::failure(request_id, host_error(&self.actor.trace_id, error))
            }
        }
    }
}

impl Drop for CapabilityHost {
    fn drop(&mut self) {
        // Dropping a pending Transaction rolls it back logically and releases
        // every record/snapshot lock. No transaction handle survives return.
        self.transactions.clear();
        for (_, provider) in std::mem::take(&mut self.streams) {
            let _ = self.services.realtime.close(provider, &[]);
        }
        if let Some((_, provider)) = self.llm_response_stream.take() {
            let _ = self.services.realtime.close(provider, &[]);
        }
    }
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn raw_sql_security_context(actor: &ActorContext) -> SecurityContext {
    let subject = actor
        .user_id
        .as_deref()
        .or(actor.service_id.as_deref())
        .map(str::to_string)
        .unwrap_or_else(|| format!("anonymous:{}", actor.trace_id));
    let mut context = SecurityContext::new(subject, actor.tenant_id.clone().unwrap_or_default())
        .with_roles(actor.roles.iter().cloned())
        .with_scopes(actor.scopes.iter().cloned())
        .with_policy_attributes(actor.policy_attributes.clone());
    if let Some(client_id) = &actor.client_id {
        context = context.with_client_id(client_id.clone());
    }
    if let Some(workspace_id) = &actor.workspace_id {
        context = context.with_workspace_id(workspace_id.clone());
    }
    // The strength reaches RLS policies through the trusted
    // `carrier.authentication_strength` GUC and gates the cross-tenant
    // internal-admin path, so an unrecognised or absent method must resolve to
    // the WEAKEST value, not the strongest. `Internal` is both the strongest
    // variant and the enum's `#[default]`, so the old catch-all arm stamped an
    // actor with no authentication method — an anonymous caller on a public
    // route — as the most privileged principal in the system. A bearer token
    // identifying a SERVICE rather than a user missed the `user_id` guard and
    // landed in the same arm; a bearer is a bearer either way.
    let strength = match actor
        .authentication_method
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "password" => AuthenticationStrength::Password,
        "scram" | "scram_sha_256" => AuthenticationStrength::ScramSha256,
        "oidc" => AuthenticationStrength::Oidc,
        "mtls" => AuthenticationStrength::Mtls,
        "step_up" => AuthenticationStrength::StepUp,
        "jwt" | "bearer" => AuthenticationStrength::Jwt,
        "internal" => AuthenticationStrength::Internal,
        _ => AuthenticationStrength::Unauthenticated,
    };
    context.with_authenticated_session(
        actor
            .session_id
            .clone()
            .unwrap_or_else(|| actor.trace_id.clone()),
        strength,
    )
}

fn raw_sql_parameter(value: Value, field_type: &FieldType) -> Result<SqlValue> {
    if value.is_null() {
        return Ok(SqlValue::Null);
    }
    let invalid = || {
        AppRuntimeError::InvalidRequest(format!("value does not match signed type {field_type:?}"))
    };
    match field_type {
        FieldType::Bool => value.as_bool().map(SqlValue::Bool).ok_or_else(invalid),
        FieldType::Int64 => value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
            .map(SqlValue::Int)
            .ok_or_else(invalid),
        FieldType::Float64 => value.as_f64().map(SqlValue::Float).ok_or_else(invalid),
        FieldType::Decimal => match value {
            Value::Number(value) => Ok(SqlValue::String(value.to_string())),
            Value::String(value) => Ok(SqlValue::String(value)),
            _ => Err(invalid()),
        },
        FieldType::String | FieldType::Timestamp | FieldType::Date => value
            .as_str()
            .map(|value| SqlValue::String(value.to_string()))
            .ok_or_else(invalid),
        FieldType::Uuid => value
            .as_str()
            .filter(|value| uuid::Uuid::parse_str(value).is_ok())
            .map(|value| SqlValue::String(value.to_string()))
            .ok_or_else(invalid),
        FieldType::Bytes => match value {
            Value::String(value) => Ok(SqlValue::String(value)),
            Value::Array(_) => Ok(SqlValue::Json(value)),
            _ => Err(invalid()),
        },
        FieldType::Json | FieldType::Vector { .. } | FieldType::Geometry { .. } => {
            if raw_sql_json_matches(&value, field_type, true) {
                Ok(SqlValue::Json(value))
            } else {
                Err(invalid())
            }
        }
    }
}

fn raw_sql_result(
    statement: &RawSqlDeclaration,
    result: bicdb_sql::SqlResult,
) -> Result<HostValue> {
    let carrier_scalar_result = statement.result.len() == 1
        && statement.result[0].name == "value"
        && result.columns.len() == 1;
    if !statement.result.is_empty() {
        if result.columns.len() != statement.result.len()
            || !carrier_scalar_result
                && !result
                    .columns
                    .iter()
                    .zip(&statement.result)
                    .all(|(actual, expected)| actual.eq_ignore_ascii_case(&expected.name))
        {
            return Err(AppRuntimeError::Invocation(format!(
                "raw SQL `{}` returned columns {:?}, expected {:?}",
                statement.id,
                result.columns,
                statement
                    .result
                    .iter()
                    .map(|field| field.name.as_str())
                    .collect::<Vec<_>>()
            )));
        }
    }
    let affected_command = result.command_tag.as_deref().is_some_and(|tag| {
        ["INSERT ", "UPDATE ", "DELETE "]
            .iter()
            .any(|prefix| tag.starts_with(prefix))
    });
    if !result.columns.is_empty()
        || !statement.result.is_empty()
        || statement.actions.contains(&DatabaseAction::Select) && !affected_command
    {
        let rows = result
            .rows
            .into_iter()
            .map(|row| {
                if row.len() != result.columns.len() {
                    return Err(AppRuntimeError::Invocation(format!(
                        "raw SQL `{}` returned a malformed row",
                        statement.id
                    )));
                }
                let mut object = Map::new();
                for (index, (name, value)) in result.columns.iter().zip(row).enumerate() {
                    let value = raw_sql_value_json(value)?;
                    if let Some(contract) = statement.result.get(index) {
                        // BicDB application's PostgreSQL Node runtime returns SQL NULL from
                        // `sql.scalar_as` unchanged, even though BicDB application signs the
                        // synthetic scalar column (`value`) as non-nullable. Match
                        // that observable PostgreSQL behavior while retaining
                        // strict nullability enforcement for structured rows.
                        if !(carrier_scalar_result && value.is_null())
                            && !raw_sql_contract_value_matches(&value, contract)
                        {
                            return Err(AppRuntimeError::Invocation(format!(
                                "raw SQL `{}` column `{}` violates its signed result type",
                                statement.id, contract.name
                            )));
                        }
                    }
                    let output_name = statement
                        .result
                        .get(index)
                        .map_or_else(|| name.clone(), |field| field.name.clone());
                    object.insert(output_name, value);
                }
                Ok(Value::Object(object))
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(HostValue::Rows(rows));
    }
    Ok(HostValue::U64(
        result
            .command_tag
            .as_deref()
            .and_then(|tag| tag.rsplit(' ').next())
            .and_then(|count| count.parse().ok())
            .unwrap_or(0),
    ))
}

fn raw_sql_contract_value_matches(value: &Value, contract: &ContractField) -> bool {
    value.is_null() && contract.nullable
        || !value.is_null() && raw_sql_json_matches(value, &contract.field_type, false)
}

fn raw_sql_json_matches(value: &Value, field_type: &FieldType, parameter: bool) -> bool {
    match field_type {
        FieldType::Bool => value.is_boolean(),
        FieldType::Int64 => value.as_i64().is_some() || value.as_u64().is_some(),
        FieldType::Float64 => value.as_f64().is_some(),
        FieldType::Decimal => {
            value.is_number()
                || value
                    .as_str()
                    .is_some_and(|value| value.parse::<rust_decimal::Decimal>().is_ok())
        }
        FieldType::String | FieldType::Timestamp | FieldType::Date | FieldType::Bytes => {
            value.is_string() || matches!(field_type, FieldType::Bytes) && value.is_array()
        }
        FieldType::Uuid => value
            .as_str()
            .is_some_and(|value| uuid::Uuid::parse_str(value).is_ok()),
        FieldType::Json => true,
        FieldType::Vector { dimensions } => value.as_array().is_some_and(|values| {
            values.len() == *dimensions as usize
                && values.iter().all(|value| value.as_f64().is_some())
        }),
        FieldType::Geometry { .. } => {
            parameter && (value.is_object() || value.is_array())
                || !parameter && (value.is_object() || value.is_array() || value.is_string())
        }
    }
}

fn raw_sql_value_json(value: SqlValue) -> Result<Value> {
    Ok(match value {
        SqlValue::Null => Value::Null,
        SqlValue::Bool(value) => Value::Bool(value),
        SqlValue::Int(value) => Value::from(value),
        SqlValue::Float(value) => serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| {
                AppRuntimeError::Invocation("SQL returned a non-finite float".to_string())
            })?,
        SqlValue::String(value) => Value::String(value),
        SqlValue::JsonText(value) => value.parsed().clone(),
        SqlValue::Json(value) => value,
        SqlValue::Geometry(value) => serde_json::to_value(value)?,
        SqlValue::TsQuery(value) => serde_json::to_value(value)?,
        SqlValue::Composite(value) => serde_json::to_value(value)?,
    })
}

fn host_error(trace_id: &str, error: AppRuntimeError) -> HostError {
    let error = match error {
        AppRuntimeError::ApplicationFailure {
            code,
            message,
            retryable,
        } => {
            return HostError {
                code,
                class: ErrorClass::InvalidRequest,
                message,
                retryable,
                retry_after_ms: None,
                trace_id: trace_id.to_string(),
            };
        }
        error => error,
    };
    let (code, class, retryable) = match &error {
        AppRuntimeError::ApplicationFailure { .. } => {
            unreachable!(
                "BicDB application failures return before generic host error classification"
            )
        }
        AppRuntimeError::Authentication(_) => {
            ("unauthenticated", ErrorClass::Unauthenticated, false)
        }
        AppRuntimeError::InvalidRequest(_) | AppRuntimeError::MissingIdempotencyKey(_) => {
            ("invalid_request", ErrorClass::InvalidRequest, false)
        }
        AppRuntimeError::CapabilityDenied(_) => {
            ("capability_denied", ErrorClass::Unauthorized, false)
        }
        AppRuntimeError::NotFound(_) => ("not_found", ErrorClass::NotFound, false),
        AppRuntimeError::Conflict(_) | AppRuntimeError::IdempotencyKeyReused(_) => {
            ("conflict", ErrorClass::Conflict, true)
        }
        AppRuntimeError::OptimisticConflict(_) => {
            ("optimistic_conflict", ErrorClass::OptimisticConflict, false)
        }
        AppRuntimeError::Timeout(_) => ("deadline_exceeded", ErrorClass::Timeout, true),
        AppRuntimeError::ResilienceTimeout(_) => ("timeout", ErrorClass::Timeout, true),
        AppRuntimeError::Cancelled(_) => ("cancelled", ErrorClass::Cancelled, true),
        AppRuntimeError::ResourceExhausted(_) => {
            ("resource_exhausted", ErrorClass::ResourceExhausted, true)
        }
        AppRuntimeError::RateLimited(_) => ("rate_limited", ErrorClass::RateLimited, true),
        AppRuntimeError::Provider(_) => ("provider_failure", ErrorClass::Provider, true),
        AppRuntimeError::InvalidPackage(_) | AppRuntimeError::Signature(_) => {
            ("invalid_package", ErrorClass::Package, false)
        }
        AppRuntimeError::NotReady(_) => (
            "dependency_unavailable",
            ErrorClass::DependencyUnavailable,
            true,
        ),
        AppRuntimeError::CircuitOpen(_) => {
            ("circuit_open", ErrorClass::DependencyUnavailable, true)
        }
        AppRuntimeError::Invocation(_) => ("invalid_request", ErrorClass::InvalidRequest, false),
        AppRuntimeError::BicDb(bicdb_core::BicDbError::MutationDenied(_)) => (
            "mutation_grant_denied",
            ErrorClass::MutationGrantDenied,
            false,
        ),
        AppRuntimeError::BicDb(bicdb_core::BicDbError::Authorization(_)) => {
            ("policy_denied", ErrorClass::PolicyDenied, false)
        }
        AppRuntimeError::BicDb(bicdb_core::BicDbError::TransactionConflict(_)) => {
            ("conflict", ErrorClass::Conflict, true)
        }
        AppRuntimeError::BicDb(bicdb_core::BicDbError::Index(message))
            if message.contains("unique index") && message.contains("duplicate keys") =>
        {
            ("conflict", ErrorClass::Conflict, false)
        }
        AppRuntimeError::BicDb(bicdb_core::BicDbError::Index(message))
            if message.contains("exclusion constraint") =>
        {
            (
                "conflict",
                ErrorClass::Conflict,
                message.contains("in-flight write"),
            )
        }
        AppRuntimeError::BicDb(bicdb_core::BicDbError::QueryTimedOut) => {
            ("deadline_exceeded", ErrorClass::Timeout, true)
        }
        AppRuntimeError::BicDb(bicdb_core::BicDbError::QueryCanceled) => {
            ("cancelled", ErrorClass::Cancelled, true)
        }
        AppRuntimeError::BicDb(bicdb_core::BicDbError::CommitValidation(_)) => {
            ("commit_validation", ErrorClass::CommitValidation, false)
        }
        AppRuntimeError::BicDb(_) => ("database_error", ErrorClass::Internal, false),
        AppRuntimeError::Extension(_) | AppRuntimeError::Json(_) | AppRuntimeError::Io(_) => {
            ("internal", ErrorClass::Internal, false)
        }
    };
    HostError {
        code: code.to_string(),
        class,
        // Provider/SQL internals are deliberately not returned to the guest.
        message: match class {
            ErrorClass::Internal | ErrorClass::Provider => code.to_string(),
            _ => error.to_string(),
        },
        retryable,
        retry_after_ms: None,
        trace_id: trace_id.to_string(),
    }
}

fn native_invariant_value_type(
    value_type: Option<ApplicationExpressionTypeV1>,
) -> NativeInvariantValueType {
    match value_type.unwrap_or(ApplicationExpressionTypeV1::Json) {
        ApplicationExpressionTypeV1::Bool => NativeInvariantValueType::Bool,
        ApplicationExpressionTypeV1::Int => NativeInvariantValueType::Int64,
        ApplicationExpressionTypeV1::Float => NativeInvariantValueType::Float64,
        ApplicationExpressionTypeV1::Decimal | ApplicationExpressionTypeV1::Money => {
            NativeInvariantValueType::Decimal
        }
        ApplicationExpressionTypeV1::String | ApplicationExpressionTypeV1::TimeZone => {
            NativeInvariantValueType::String
        }
        ApplicationExpressionTypeV1::Uuid => NativeInvariantValueType::Uuid,
        ApplicationExpressionTypeV1::Timestamp
        | ApplicationExpressionTypeV1::LocalDateTime
        | ApplicationExpressionTypeV1::ZonedDateTime => NativeInvariantValueType::Timestamp,
        ApplicationExpressionTypeV1::Date => NativeInvariantValueType::Date,
        ApplicationExpressionTypeV1::Json | ApplicationExpressionTypeV1::Other => {
            NativeInvariantValueType::Json
        }
    }
}

fn lower_carrier_invariant_expression(
    application: &bicdb_extension::abi_v2::ApplicationManifestV2,
    subject_resource: &str,
    bindings: &BTreeMap<String, String>,
    expression: &ApplicationExpressionV1,
) -> Result<NativeInvariantExpression> {
    let subject = application
        .resources
        .iter()
        .find(|resource| resource.name == subject_resource)
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "invariant subject resource `{subject_resource}` is unavailable"
            ))
        })?;
    let resource_relation = |resource: &str| {
        application
            .resources
            .iter()
            .find(|candidate| candidate.name == resource)
            .map(|candidate| candidate.relation.clone())
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "invariant dependency resource `{resource}` is unavailable"
                ))
            })
    };
    Ok(match expression {
        ApplicationExpressionV1::Variable { name }
            if subject.fields.iter().any(|field| field.name == *name) =>
        {
            NativeInvariantExpression::SubjectField {
                field: name.clone(),
            }
        }
        ApplicationExpressionV1::Literal { value } => NativeInvariantExpression::Literal {
            value: value.clone(),
        },
        ApplicationExpressionV1::Field { target, field } => {
            let ApplicationExpressionV1::Variable { name } = target.as_ref() else {
                return Err(AppRuntimeError::InvalidPackage(
                    "invariant field target is not a binding".to_string(),
                ));
            };
            if name == "subject" {
                NativeInvariantExpression::SubjectField {
                    field: field.clone(),
                }
            } else if bindings.contains_key(name) {
                NativeInvariantExpression::BindingField {
                    binding: name.clone(),
                    field: field.clone(),
                }
            } else {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "invariant field target `{name}` is not bound"
                )));
            }
        }
        ApplicationExpressionV1::Unary {
            operator,
            value,
            value_type,
            operand_type,
        } => NativeInvariantExpression::Unary {
            operator: match operator.as_str() {
                "not" => NativeInvariantUnaryOperator::Not,
                "negate" => NativeInvariantUnaryOperator::Negate,
                _ => {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "unsupported invariant unary operator `{operator}`"
                    )));
                }
            },
            value: Box::new(lower_carrier_invariant_expression(
                application,
                subject_resource,
                bindings,
                value,
            )?),
            value_type: native_invariant_value_type((*operand_type).or(*value_type)),
        },
        ApplicationExpressionV1::Binary {
            operator,
            left,
            right,
            value_type,
            left_type,
            right_type,
        } => NativeInvariantExpression::Binary {
            operator: match operator.as_str() {
                "add" => NativeInvariantBinaryOperator::Add,
                "subtract" => NativeInvariantBinaryOperator::Subtract,
                "multiply" => NativeInvariantBinaryOperator::Multiply,
                "divide" => NativeInvariantBinaryOperator::Divide,
                "and" => NativeInvariantBinaryOperator::And,
                "or" => NativeInvariantBinaryOperator::Or,
                "implies" => NativeInvariantBinaryOperator::Implies,
                "contains" => NativeInvariantBinaryOperator::Contains,
                "equal" => NativeInvariantBinaryOperator::Equal,
                "not_equal" => NativeInvariantBinaryOperator::NotEqual,
                "greater" => NativeInvariantBinaryOperator::Greater,
                "greater_equal" => NativeInvariantBinaryOperator::GreaterEqual,
                "less" => NativeInvariantBinaryOperator::Less,
                "less_equal" => NativeInvariantBinaryOperator::LessEqual,
                _ => {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "unsupported invariant binary operator `{operator}`"
                    )));
                }
            },
            left: Box::new(lower_carrier_invariant_expression(
                application,
                subject_resource,
                bindings,
                left,
            )?),
            right: Box::new(lower_carrier_invariant_expression(
                application,
                subject_resource,
                bindings,
                right,
            )?),
            value_type: native_invariant_value_type((*left_type).or(*right_type).or(*value_type)),
        },
        ApplicationExpressionV1::Call {
            kind,
            target,
            arguments,
            argument_types,
            ..
        } if *kind == ApplicationCallKindV1::Builtin && target == "overlaps" => {
            NativeInvariantExpression::Overlaps {
                values: arguments
                    .iter()
                    .map(|argument| {
                        lower_carrier_invariant_expression(
                            application,
                            subject_resource,
                            bindings,
                            &argument.value,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
                value_type: native_invariant_value_type(argument_types.first().copied()),
            }
        }
        ApplicationExpressionV1::Exists {
            binding,
            resource,
            condition,
        } => {
            let mut nested = bindings.clone();
            nested.insert(binding.clone(), resource.clone());
            NativeInvariantExpression::Exists {
                binding: binding.clone(),
                relation: resource_relation(resource)?,
                condition: Box::new(lower_carrier_invariant_expression(
                    application,
                    subject_resource,
                    &nested,
                    condition,
                )?),
            }
        }
        ApplicationExpressionV1::Aggregate {
            function,
            binding,
            resource,
            condition,
            field,
            value_type,
        } => {
            let mut nested = bindings.clone();
            nested.insert(binding.clone(), resource.clone());
            NativeInvariantExpression::Aggregate {
                function: match function {
                    ApplicationAggregateFunctionV1::Count => {
                        NativeInvariantAggregateFunction::Count
                    }
                    ApplicationAggregateFunctionV1::Sum => NativeInvariantAggregateFunction::Sum,
                },
                binding: binding.clone(),
                relation: resource_relation(resource)?,
                condition: Box::new(lower_carrier_invariant_expression(
                    application,
                    subject_resource,
                    &nested,
                    condition,
                )?),
                field: field.clone(),
                value_type: native_invariant_value_type(*value_type),
            }
        }
        _ => {
            return Err(AppRuntimeError::InvalidPackage(
                "invariant expression escaped ABI validation".to_string(),
            ));
        }
    })
}

pub(crate) fn lower_carrier_invariants(
    application: &bicdb_extension::abi_v2::ApplicationManifestV2,
) -> Result<Vec<NativeCommitValidator>> {
    application
        .invariants
        .iter()
        .map(|invariant| {
            let subject_relation = application
                .resources
                .iter()
                .find(|resource| resource.name == invariant.subject_resource)
                .map(|resource| resource.relation.clone())
                .ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(format!(
                        "invariant `{}` has no subject resource",
                        invariant.name
                    ))
                })?;
            let dependency_relations = invariant
                .dependency_resources
                .iter()
                .map(|resource| {
                    application
                        .resources
                        .iter()
                        .find(|candidate| candidate.name == *resource)
                        .map(|candidate| candidate.relation.clone())
                        .ok_or_else(|| {
                            AppRuntimeError::InvalidPackage(format!(
                                "invariant `{}` dependency `{resource}` is unavailable",
                                invariant.name
                            ))
                        })
                })
                .collect::<Result<BTreeSet<_>>>()?;
            Ok(NativeCommitValidator::ApplicationInvariant {
                definition: NativeInvariantDefinition {
                    name: invariant.name.clone(),
                    subject_relation,
                    kind: match invariant.kind {
                        ApplicationInvariantKindV1::MustAlways => NativeInvariantKind::MustAlways,
                        ApplicationInvariantKindV1::MustNever => NativeInvariantKind::MustNever,
                    },
                    expression: lower_carrier_invariant_expression(
                        application,
                        &invariant.subject_resource,
                        &BTreeMap::new(),
                        &invariant.expression,
                    )?,
                    dependency_relations,
                    source: invariant.source.clone(),
                },
            })
        })
        .collect()
}

fn lower_validator(validator: CommitValidator) -> Result<NativeCommitValidator> {
    let relation =
        validator.relations.iter().next().cloned().ok_or_else(|| {
            AppRuntimeError::CapabilityDenied("validator lacks relation".to_string())
        })?;
    let string = |name: &str| {
        validator
            .parameters
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| AppRuntimeError::CapabilityDenied(format!("validator lacks `{name}`")))
    };
    Ok(match validator.kind {
        CommitValidatorKind::AppendOnly => NativeCommitValidator::AppendOnly { relation },
        CommitValidatorKind::ImmutableFields => NativeCommitValidator::ImmutableFields {
            relation,
            fields: validator
                .parameters
                .get("fields")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
        },
        CommitValidatorKind::TenantWorkspaceImmutable => {
            NativeCommitValidator::TenantWorkspaceImmutable {
                relation,
                tenant_field: validator
                    .parameters
                    .get("tenant_field")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                workspace_field: validator
                    .parameters
                    .get("workspace_field")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            }
        }
        CommitValidatorKind::OptimisticVersion => NativeCommitValidator::OptimisticVersion {
            relation,
            field: string("field")?,
        },
        CommitValidatorKind::LedgerBalanced => NativeCommitValidator::LedgerBalanced {
            relation,
            subject_field: string("subject_field")?,
            debit_field: string("debit_field")?,
            credit_field: string("credit_field")?,
        },
        CommitValidatorKind::AggregateInvariant => NativeCommitValidator::AggregateInvariant {
            relation,
            subject_field: string("subject_field")?,
            value_field: string("value_field")?,
            minimum: validator
                .parameters
                .get("minimum")
                .and_then(Value::as_f64)
                .ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied("validator lacks minimum".to_string())
                })?,
            maximum: validator
                .parameters
                .get("maximum")
                .and_then(Value::as_f64)
                .ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied("validator lacks maximum".to_string())
                })?,
        },
        CommitValidatorKind::FinanceGuard => NativeCommitValidator::FinanceGuard {
            relation,
            amount_field: string("amount_field")?,
            maximum_absolute_amount: validator
                .parameters
                .get("maximum_absolute_amount")
                .and_then(Value::as_f64)
                .ok_or_else(|| {
                    AppRuntimeError::CapabilityDenied("validator lacks maximum".to_string())
                })?,
        },
        CommitValidatorKind::CustomDeclared => {
            return Err(AppRuntimeError::CapabilityDenied(
                "custom commit validators require a separately declared native provider"
                    .to_string(),
            ))
        }
    })
}

fn record_from_flat_json(value: Value) -> Result<Record> {
    let mut object = value.as_object().cloned().ok_or_else(|| {
        AppRuntimeError::CapabilityDenied("database record must be an object".to_string())
    })?;
    let id = object
        .remove("id")
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| AppRuntimeError::CapabilityDenied("record lacks string id".to_string()))?;
    let vector = object
        .remove("vector")
        .map(serde_json::from_value)
        .transpose()?;
    let geometry = object
        .remove("geometry")
        .map(serde_json::from_value)
        .transpose()?;
    let timestamp = object
        .remove("timestamp")
        .map(|value| {
            value.as_i64().ok_or_else(|| {
                AppRuntimeError::CapabilityDenied("timestamp must be an integer".to_string())
            })
        })
        .transpose()?;
    let mut record = Record::new(id).with_metadata(Value::Object(object));
    record.vector = vector;
    record.geometry = geometry;
    record.timestamp = timestamp;
    Ok(record)
}

pub(crate) fn record_from_resource_json(
    mut value: Value,
    vector_field: Option<&str>,
) -> Result<Record> {
    if let Some(vector_field) = vector_field.filter(|field| *field != "vector") {
        let vector = value.get(vector_field).cloned();
        if let (Some(vector), Some(object)) = (vector, value.as_object_mut()) {
            object.insert("vector".to_string(), vector);
        }
    }
    record_from_flat_json(value)
}

pub(crate) fn record_json(record: &Record) -> Value {
    let mut object = record.metadata.as_object().cloned().unwrap_or_default();
    for value in object.values_mut() {
        if let Some(decoded) = decode_typed_storage_json(value) {
            *value = decoded;
        }
    }
    object
        .entry("id".to_string())
        .or_insert_with(|| Value::String(record.id.clone()));
    if let Some(vector) = &record.vector {
        object.insert("vector".to_string(), json!(vector));
    }
    if let Some(geometry) = &record.geometry {
        if let Ok(value) = serde_json::to_value(geometry) {
            object.insert("geometry".to_string(), value);
        }
    }
    if let Some(timestamp) = record.timestamp {
        object.insert("timestamp".to_string(), Value::from(timestamp));
    }
    Value::Object(object)
}

pub(crate) fn resource_record_json(record: &Record, vector_field: Option<&str>) -> Value {
    let mut value = record_json(record);
    if let Some(vector_field) = vector_field.filter(|field| *field != "vector") {
        if let Some(object) = value.as_object_mut() {
            let vector = object.remove("vector");
            if !object.contains_key(vector_field) {
                if let Some(vector) = vector {
                    object.insert(vector_field.to_string(), vector);
                }
            }
        }
    }
    value
}

fn merge_resource_record_patch(
    record: &mut Record,
    mut patch: Map<String, Value>,
    vector_field: Option<&str>,
) -> Result<()> {
    if let Some(vector_field) = vector_field.filter(|field| *field != "vector") {
        if let Some(vector) = patch.get(vector_field).cloned() {
            patch.insert("vector".to_string(), vector);
        }
    }
    merge_record_patch(record, patch)
}

fn resource_record_columns(record: &Record, vector_field: Option<&str>) -> BTreeSet<String> {
    let mut columns = record_columns(record);
    if let Some(vector_field) = vector_field.filter(|field| *field != "vector") {
        columns.remove("vector");
        columns.insert(vector_field.to_string());
    }
    columns
}

fn validate_vector_query(vector: &[f32], dimensions: u32, limit: u32) -> Result<()> {
    if vector.len() != dimensions as usize {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "vector query expects {dimensions} dimensions, found {}",
            vector.len()
        )));
    }
    if vector.is_empty()
        || vector.iter().any(|value| !value.is_finite())
        || limit == 0
        || limit > 10_000
    {
        return Err(AppRuntimeError::InvalidRequest(
            "vector query requires finite values and a limit in 1..=10000".to_string(),
        ));
    }
    Ok(())
}

fn carrier_vector_score(
    metric: ApplicationVectorMetricV1,
    query: &[f32],
    candidate: &[f32],
) -> Result<f64> {
    let score = match metric {
        ApplicationVectorMetricV1::Cosine => f64::from(cosine_similarity(query, candidate)?),
        ApplicationVectorMetricV1::Euclidean => {
            1.0 / (1.0 + f64::from(l2_distance(query, candidate)?))
        }
        ApplicationVectorMetricV1::InnerProduct => f64::from(dot_product(query, candidate)?),
    };
    if !score.is_finite() {
        return Err(AppRuntimeError::InvalidRequest(
            "vector similarity score is not finite".to_string(),
        ));
    }
    Ok(score)
}

fn merge_record_patch(record: &mut Record, patch: Map<String, Value>) -> Result<()> {
    if !record.metadata.is_object() {
        return Err(AppRuntimeError::CapabilityDenied(
            "record metadata is not an object".to_string(),
        ));
    }
    for (field, value) in patch {
        if field == "id" {
            if value.as_str() != Some(&record.id) {
                return Err(AppRuntimeError::CapabilityDenied(
                    "record id is immutable".to_string(),
                ));
            }
        } else {
            match field.as_str() {
                "vector" => record.vector = serde_json::from_value(value)?,
                "geometry" => record.geometry = serde_json::from_value(value)?,
                "timestamp" => {
                    record.timestamp = Some(value.as_i64().ok_or_else(|| {
                        AppRuntimeError::CapabilityDenied(
                            "timestamp must be an integer".to_string(),
                        )
                    })?)
                }
                _ => {
                    record
                        .metadata
                        .as_object_mut()
                        .expect("validated object")
                        .insert(field, value);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    /// `Internal` is the strongest value in the lattice and gates cross-tenant
    /// support access, so it must be reachable only by naming it. Every
    /// constructor that does not name it yields the weakest value — an omitted
    /// strength is not evidence of trust.
    #[test]
    fn only_an_explicit_constructor_yields_an_internal_principal() {
        use bicdb_core::SecurityContext;

        assert_eq!(
            SecurityContext::new("u", "t").authentication_strength,
            AuthenticationStrength::Unauthenticated,
            "the quiet constructor must not mint an internal principal"
        );
        assert_eq!(
            SecurityContext::authenticated("u", "t", AuthenticationStrength::Jwt)
                .authentication_strength,
            AuthenticationStrength::Jwt,
            "a delegated principal keeps the strength it was authenticated with"
        );
        assert_eq!(
            SecurityContext::trusted_internal("host", "t").authentication_strength,
            AuthenticationStrength::Internal,
            "a trusted system principal says so explicitly"
        );
    }

    /// `authentication_strength` is `#[serde(default)]`, so a SecurityContext
    /// deserialized without the field takes the enum's default — which was
    /// `Internal`, the strongest variant and the one the cross-tenant
    /// internal-admin gate requires. An omitted field must not mint the most
    /// privileged principal in the system.
    #[test]
    fn an_omitted_authentication_strength_is_the_weakest_not_the_strongest() {
        let context: bicdb_core::SecurityContext =
            serde_json::from_str(r#"{"user_id":"u","tenant_id":"t"}"#).unwrap();
        assert_eq!(
            context.authentication_strength,
            AuthenticationStrength::Unauthenticated
        );
        assert_eq!(
            AuthenticationStrength::default(),
            AuthenticationStrength::Unauthenticated
        );
    }

    /// `raw_sql_security_context` maps an actor's authentication method onto a
    /// strength that RLS policies read through the trusted
    /// `carrier.authentication_strength` GUC. The fallback arm resolved to
    /// `Internal` — simultaneously the STRONGEST variant and the one the
    /// cross-tenant internal-admin gate requires — so an actor with no
    /// authentication method at all (anonymous on a public route) or a bearer
    /// token carrying only a service id was stamped as the strongest principal
    /// in the system.
    #[test]
    fn an_unauthenticated_actor_is_not_stamped_with_the_strongest_strength() {
        let anonymous = ActorContext::default();
        assert_eq!(anonymous.authentication_method, None);
        assert_ne!(
            raw_sql_security_context(&anonymous).authentication_strength,
            AuthenticationStrength::Internal,
            "an actor with no authentication method must not become an internal principal"
        );

        // A bearer token that identifies a SERVICE rather than a user missed
        // the `user_id.is_some()` guard and fell through to the same arm.
        let mut service = ActorContext::default();
        service.authentication_method = Some("bearer".to_string());
        service.service_id = Some("svc-1".to_string());
        assert_ne!(
            raw_sql_security_context(&service).authentication_strength,
            AuthenticationStrength::Internal,
            "a bearer-authenticated service must not become an internal principal"
        );

        // Recognised methods are unchanged.
        let mut user = ActorContext::default();
        user.authentication_method = Some("bearer".to_string());
        user.user_id = Some("u-1".to_string());
        assert_eq!(
            raw_sql_security_context(&user).authentication_strength,
            AuthenticationStrength::Jwt
        );
        let mut mtls = ActorContext::default();
        mtls.authentication_method = Some("mtls".to_string());
        assert_eq!(
            raw_sql_security_context(&mtls).authentication_strength,
            AuthenticationStrength::Mtls
        );
    }
    use super::*;
    use bicdb_core::{CollectionPolicy, MutationPolicy};
    use bicdb_extension::abi_v2::{
        ApplicationFeature, ApplicationManifestV2, PackageMetadata, RawSqlDeclaration,
        RelationPermission, ServiceImport, TransactionRequest, APPLICATION_COMPATIBILITY_PROFILE,
    };
    use bicdb_extension::host::ApplicationHost;
    use bicdb_extension::{
        ExtensionIdentity, ExtensionLimits, ExtensionPermissions, EXTENSION_ABI_V2,
    };
    use std::path::Path;

    use crate::{
        InMemorySecretProvider, LocalBlobProvider, ObservabilityEvent, ProductionEgressProvider,
        RedisProvider,
    };

    fn hash() -> String {
        "a".repeat(64)
    }

    #[test]
    fn unique_index_violations_cross_the_host_as_conflicts() {
        let error = host_error(
            "trace",
            AppRuntimeError::BicDb(bicdb_core::BicDbError::Index(
                "unique index `Doctor_external_id` has duplicate keys".to_string(),
            )),
        );
        assert_eq!(error.code, "conflict");
        assert_eq!(error.class, ErrorClass::Conflict);
        assert!(!error.retryable);
    }

    #[test]
    fn carrier_scalar_sql_preserves_postgresql_null_results() {
        let scalar_statement = RawSqlDeclaration {
            id: "nullable_scalar".to_string(),
            sql: "SELECT NULLIF($1, '')".to_string(),
            sha256: hash(),
            relations: BTreeSet::new(),
            routines: BTreeSet::from(["nullif".to_string()]),
            actions: BTreeSet::from([DatabaseAction::Select]),
            parameters: vec![FieldType::String],
            result: vec![ContractField {
                name: "value".to_string(),
                storage_name: None,
                field_type: FieldType::String,
                value_type: None,
                nullable: false,
                generated: false,
                generated_expression: None,
                default_json: None,
            }],
            max_affected_rows: 10_000,
        };
        let null_result = bicdb_sql::SqlResult {
            columns: vec!["?column?".to_string()],
            rows: vec![vec![SqlValue::Null]],
            command_tag: Some("SELECT 1".to_string()),
            column_types: vec![Some("text".to_string())],
            column_metadata: vec![],
        };

        assert_eq!(
            raw_sql_result(&scalar_statement, null_result.clone()).unwrap(),
            HostValue::Rows(vec![json!({"value": null})])
        );

        let mut structured_statement = scalar_statement;
        structured_statement.result[0].name = "note".to_string();
        assert!(raw_sql_result(&structured_statement, null_result).is_err());
    }

    #[test]
    fn carrier_cte_insert_sql_reports_postgresql_affected_rows() {
        let statement = RawSqlDeclaration {
            id: "cte_insert_audit".to_string(),
            sql: "WITH previous AS (SELECT row_hash FROM carrier_audit_log LIMIT 1) \
                  INSERT INTO carrier_audit_log (row_hash) SELECT row_hash FROM previous"
                .to_string(),
            sha256: hash(),
            relations: BTreeSet::from(["carrier_audit_log".to_string()]),
            routines: BTreeSet::new(),
            actions: BTreeSet::from([DatabaseAction::Select, DatabaseAction::Insert]),
            parameters: vec![],
            result: vec![],
            max_affected_rows: 10_000,
        };

        assert_eq!(
            raw_sql_result(&statement, bicdb_sql::SqlResult::command("INSERT 0 1")).unwrap(),
            HostValue::U64(1)
        );
    }

    #[test]
    fn sparse_nullable_fields_match_postgresql_null_resource_filters() {
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

        assert!(filter_matches(&missing, &active));
        assert!(filter_matches(&explicit_null, &active));
        assert!(!filter_matches(&deleted, &active));
        assert!(!filter_matches(&missing, &deleted_scope));
        assert!(!filter_matches(&explicit_null, &deleted_scope));
        assert!(filter_matches(&deleted, &deleted_scope));
    }

    #[test]
    fn declared_sql_enforces_role_only_resource_policies() {
        let directory = tempfile::tempdir().unwrap();
        let db = BicDb::open(directory.path().join("raw-sql-policy-db")).unwrap();
        let mut request_actor = actor(now_ms() + 30_000);
        request_actor.roles.insert("editor".to_string());
        let mut host =
            CapabilityHost::new(&db, manifest(), request_actor, services(directory.path()))
                .unwrap();
        let contract: ResourceContractV1 = serde_json::from_value(json!({
            "version": 1,
            "name": "PolicyDocument",
            "relation": "policy_documents",
            "schema_version": 1,
            "schema_only": true,
            "primary_key": "id",
            "fields": [
                {"name": "id", "field_type": "string"},
                {"name": "deleted_at", "field_type": "timestamp", "nullable": true}
            ],
            "list_route": "/__schema/policy-documents",
            "item_route": "/__schema/policy-documents/{id}",
            "soft_delete_field": "deleted_at",
            "policy": {
                "version": 1,
                "read": {"roles": ["editor"], "role_match": "any"},
                "write": {"roles": ["org_admin"], "role_match": "any"},
                "deleted_read": {"roles": ["auditor"], "role_match": "any"}
            },
            "contract_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        }))
        .unwrap();

        assert!(host
            .enforce_declared_sql_role_policy(&contract, DatabaseAction::Select)
            .is_err());
        assert!(host
            .enforce_declared_sql_role_policy(&contract, DatabaseAction::Insert)
            .is_err());

        host.actor.roles.insert("auditor".to_string());
        host.enforce_declared_sql_role_policy(&contract, DatabaseAction::Select)
            .unwrap();
        host.actor.roles.insert("org_admin".to_string());
        host.enforce_declared_sql_role_policy(&contract, DatabaseAction::Insert)
            .unwrap();
    }

    #[test]
    fn carrier_named_vectors_round_trip_and_all_signed_metrics_score() {
        let record = record_from_resource_json(
            json!({
                "id": "doc-1",
                "title": "BicDB application",
                "embedding": [1.0, 0.0, 0.0]
            }),
            Some("embedding"),
        )
        .unwrap();
        assert_eq!(record.vector.as_deref(), Some(&[1.0, 0.0, 0.0][..]));
        let round_trip = resource_record_json(&record, Some("embedding"));
        assert_eq!(round_trip["embedding"], json!([1.0, 0.0, 0.0]));
        assert!(round_trip.get("vector").is_none());

        validate_vector_query(&[1.0, 0.0, 0.0], 3, 5).unwrap();
        assert!(validate_vector_query(&[1.0, 0.0], 3, 5).is_err());
        assert_eq!(
            carrier_vector_score(ApplicationVectorMetricV1::Cosine, &[1.0, 0.0], &[1.0, 0.0])
                .unwrap(),
            1.0
        );
        assert_eq!(
            carrier_vector_score(
                ApplicationVectorMetricV1::Euclidean,
                &[1.0, 0.0],
                &[4.0, 0.0]
            )
            .unwrap(),
            0.25
        );
        assert_eq!(
            carrier_vector_score(
                ApplicationVectorMetricV1::InnerProduct,
                &[1.0, 2.0],
                &[3.0, 4.0]
            )
            .unwrap(),
            11.0
        );
    }

    #[test]
    fn carrier_resource_projection_uses_postgresql_uuid_primary_keys() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path()).unwrap();
        {
            let mut session = SqlSession::new(&mut db);
            session
                .execute("CREATE TABLE typed_resources (id UUID PRIMARY KEY, name TEXT NOT NULL)")
                .unwrap();
            session
                .execute("INSERT INTO typed_resources (id, name) VALUES ('01130c7d-7adc-884e-f1a4-651c7c042046', 'typed')")
                .unwrap();
        }
        let record = db
            .scan_collection_unchecked("typed_resources")
            .unwrap()
            .remove(0);
        assert_eq!(record.id, "01130c7d-7adc-884e-f1a4-651c7c042046");
        assert_eq!(
            resource_record_json(&record, None)["id"],
            json!("01130c7d-7adc-884e-f1a4-651c7c042046")
        );
    }

    #[test]
    fn carrier_resource_metadata_field_is_not_an_internal_record_envelope() {
        let record = record_from_resource_json(
            json!({
                "id": "venue-1",
                "name": "Origin",
                "metadata": {"classification": {"category": "urban"}}
            }),
            None,
        )
        .unwrap();
        assert_eq!(
            record.metadata["metadata"]["classification"]["category"],
            "urban"
        );
        assert_eq!(
            resource_record_json(&record, None)["metadata"]["classification"]["category"],
            "urban"
        );
        assert_eq!(
            resource_record_columns(&record, None),
            BTreeSet::from(["id".to_string(), "metadata".to_string(), "name".to_string()])
        );
    }

    #[test]
    fn carrier_spatial_helpers_are_exact_bounded_and_deterministic() {
        let point_rows = vec![
            json!({"id":"b","location":{"type":"Point","coordinates":[1.0,0.0]}}),
            json!({"id":"a","location":{"type":"Point","coordinates":[0.0,0.0]}}),
            json!({"id":"c","location":{"type":"Point","coordinates":[4.0,0.0]}}),
        ];
        let point = json!({"type":"Point","coordinates":[0.0,0.0]});
        let nearest = spatial_rows(
            point_rows.clone(),
            SpatialOperation::Nearest {
                field: "location".to_string(),
                point: point.clone(),
                limit: 2,
            },
        )
        .unwrap();
        assert_eq!(
            nearest
                .iter()
                .map(|row| row["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        let within = spatial_rows(
            point_rows,
            SpatialOperation::WithinRadius {
                field: "location".to_string(),
                point: point.clone(),
                radius: 1.0,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(
            within
                .iter()
                .map(|row| row["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        let contained = spatial_rows(
            vec![
                json!({
                    "id":"inside",
                    "area":{"type":"Polygon","coordinates":[[[ -1.0,-1.0],[1.0,-1.0],[1.0,1.0],[-1.0,1.0],[-1.0,-1.0]]]}
                }),
                json!({
                    "id":"outside",
                    "area":{"type":"Polygon","coordinates":[[[ 10.0,10.0],[12.0,10.0],[12.0,12.0],[10.0,12.0],[10.0,10.0]]]}
                }),
            ],
            SpatialOperation::Contains {
                field: "area".to_string(),
                point,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(contained.len(), 1);
        assert_eq!(contained[0]["id"], "inside");
        assert!(spatial_rows(
            Vec::new(),
            SpatialOperation::WithinRadius {
                field: "location".to_string(),
                point: json!({"type":"Point","coordinates":[0.0,0.0]}),
                radius: -1.0,
                limit: 10,
            }
        )
        .is_err());
    }

    fn manifest() -> Arc<ExtensionManifest> {
        Arc::new(ExtensionManifest {
            identity: ExtensionIdentity {
                name: "security_test".to_string(),
                version: "1.0.0".to_string(),
                abi_version: EXTENSION_ABI_V2,
                description: String::new(),
            },
            dependencies: vec![],
            capabilities: BTreeSet::from([
                ExtensionCapability::Database,
                ExtensionCapability::Transactions,
                ExtensionCapability::PluginServices,
                ExtensionCapability::SecretsCrypto,
                ExtensionCapability::QueueEvents,
            ]),
            permissions: ExtensionPermissions {
                read_relations: BTreeSet::from(["allowed".to_string()]),
                publish_queues: BTreeSet::from(["security_events".to_string()]),
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
                    application: "security_test".to_string(),
                    version: "1.0.0".to_string(),
                    package_sha256: hash(),
                    dependency_lock_sha256: hash(),
                    sbom_sha256: hash(),
                    provenance_sha256: hash(),
                    signature_key_id: "test".to_string(),
                    signature_algorithm: "ed25519".to_string(),
                    signature: "test-signature-value".to_string(),
                },
                relation_permissions: vec![RelationPermission {
                    relation: "allowed".to_string(),
                    actions: BTreeSet::from([DatabaseAction::Select]),
                    readable_columns: BTreeSet::new(),
                    writable_columns: BTreeSet::new(),
                }],
                raw_sql: vec![RawSqlDeclaration {
                    id: "allowed_scan".to_string(),
                    sql: "SELECT id, value FROM allowed".to_string(),
                    sha256: hash(),
                    relations: BTreeSet::from(["allowed".to_string()]),
                    routines: BTreeSet::new(),
                    actions: BTreeSet::from([DatabaseAction::Select]),
                    parameters: vec![],
                    result: vec![
                        ContractField {
                            name: "id".to_string(),
                            storage_name: None,
                            field_type: FieldType::String,
                            value_type: None,
                            nullable: false,
                            generated: false,
                            generated_expression: None,
                            default_json: None,
                        },
                        ContractField {
                            name: "value".to_string(),
                            storage_name: None,
                            field_type: FieldType::Int64,
                            value_type: None,
                            nullable: false,
                            generated: false,
                            generated_expression: None,
                            default_json: None,
                        },
                    ],
                    max_affected_rows: 10_000,
                }],
                service_imports: vec![ServiceImport {
                    name: "dependency".to_string(),
                    service: "example".to_string(),
                    version: "^1".to_string(),
                    contract_sha256: hash(),
                    optional: false,
                    propagate_transaction: false,
                    allow_reentrant: false,
                    delegated_authority: false,
                }],
                service_exports: vec![],
                secrets: vec![],
                egress: vec![],
                blobs: vec![],
                routes: vec![],
                response_headers: BTreeMap::new(),
                auth_schemes: BTreeMap::new(),
                realtime: vec![],
                resources: vec![],
                invariants: vec![],
                migrations: vec![],
                workers: vec![],
                schedules: vec![],
                application_program: None,
                required_features: BTreeSet::<ApplicationFeature>::new(),
                max_call_depth: 4,
            })),
        })
    }

    fn actor(deadline_unix_ms: i64) -> ActorContext {
        ActorContext {
            user_id: Some("user-1".to_string()),
            service_id: None,
            client_id: Some("test".to_string()),
            acting_client_id: None,
            authentication_method: Some("test".to_string()),
            roles: BTreeSet::new(),
            scopes: BTreeSet::new(),
            tenant_id: Some("tenant-1".to_string()),
            workspace_id: Some("workspace-1".to_string()),
            organization_id: Some("organization-1".to_string()),
            session_id: Some("session-1".to_string()),
            delegation_chain: vec![],
            assurance_level: Some("aal2".to_string()),
            request_origin: Some("test".to_string()),
            trace_id: "trace-1".to_string(),
            correlation_id: Some("correlation-1".to_string()),
            causation_id: None,
            deadline_unix_ms,
            policy_attributes: BTreeMap::new(),
        }
    }

    fn services(root: &Path) -> InvocationServices {
        InvocationServices::new(
            Arc::new(InMemorySecretProvider::default()),
            Arc::new(ProductionEgressProvider::new().unwrap()),
            Arc::new(LocalBlobProvider::open(root.join("blobs"), vec![7; 32]).unwrap()),
            Arc::new(Mutex::<Vec<ObservabilityEvent>>::default()),
        )
    }

    #[test]
    fn carrier_audit_is_transactional_durable_and_redacted() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("audit-db")).unwrap();
        db.create_collection("__bicdb_app_audit").unwrap();
        db.set_mutation_policy(
            "__bicdb_app_audit",
            MutationPolicy::grants_required().append_only(),
        )
        .unwrap();

        let mut extension = Arc::try_unwrap(manifest()).expect("unique test manifest");
        extension
            .capabilities
            .insert(ExtensionCapability::Observability);
        let application = extension.application.as_deref_mut().unwrap();
        application.required_features.extend([
            ApplicationFeature::Observability,
            ApplicationFeature::ReadCommitted,
        ]);
        application.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "max_call_depth": 4,
                "callables": {
                    "audit": {
                        "parameters": [],
                        "body": [{"op": "return", "value": {
                            "op": "call", "kind": "builtin", "target": "audit.record",
                            "arguments": [
                                {"value": {"op": "literal", "value": "patient.read"}},
                                {"value": {"op": "literal", "value": "Patient"}},
                                {"value": {"op": "literal", "value": "patient-1"}}
                            ]
                        }}]
                    }
                },
                "observability": {
                    "version": 1,
                    "provider": "bicdb",
                    "protocol": "host",
                    "service_name": "security-test",
                    "sampling": {"kind": "always_on"},
                    "helpers": ["audit.record"],
                    "redacted_keys": ["email", "password"],
                    "metric_names": [],
                    "audit_actions": ["patient.read"],
                    "dynamic_metric_names": false,
                    "dynamic_audit_actions": false,
                    "max_field_depth": 8,
                    "max_field_bytes": 4096,
                    "durable_audit": true,
                    "propagate_w3c": true
                }
            }))
            .unwrap(),
        );
        let extension = Arc::new(extension);
        extension.validate().unwrap();

        let observations = Arc::new(Mutex::<Vec<ObservabilityEvent>>::default());
        let mut invocation_services = services(directory.path());
        invocation_services.observability = observations.clone();
        let mut audit_actor = actor(now_ms() + 30_000);
        audit_actor.causation_id = Some("request-1".to_string());
        let mut host =
            CapabilityHost::new(&db, extension, audit_actor, invocation_services).unwrap();
        host.observe_carrier_audit(
            None,
            "patient.read".to_string(),
            "Patient:patient-1".to_string(),
            BTreeMap::from([
                ("email".to_string(), json!("private@example.test")),
                (
                    "nested".to_string(),
                    json!({"password": "super-secret", "safe": true}),
                ),
            ]),
        )
        .unwrap();

        let durable = db.scan_collection("__bicdb_app_audit").unwrap();
        assert_eq!(durable.len(), 1);
        assert_eq!(durable[0].metadata["fields"]["email"], "[REDACTED]");
        assert_eq!(
            durable[0].metadata["fields"]["nested"]["password"],
            "[REDACTED]"
        );
        assert_eq!(durable[0].metadata["causation_id"], "request-1");
        let events = observations.lock().unwrap();
        assert!(matches!(
            &events[0],
            ObservabilityEvent::Audit { fields, .. }
                if fields["email"] == "[REDACTED]"
                    && fields["nested"]["password"] == "[REDACTED]"
        ));
        drop(events);
        assert!(host
            .observe_carrier_audit(
                None,
                "forged.action".to_string(),
                "Patient:patient-1".to_string(),
                BTreeMap::new(),
            )
            .unwrap_err()
            .to_string()
            .contains("outside signed durable-audit authority"));
    }

    #[derive(Default)]
    struct RecordingRedis {
        increments: Mutex<Vec<(String, String, Option<String>, String)>>,
    }

    impl RedisProvider for RecordingRedis {
        fn available(&self, _application: &str, _provider: &str) -> Result<()> {
            Ok(())
        }

        fn publish(
            &self,
            _application: &str,
            _provider: &str,
            _channel: &str,
            _message: &str,
        ) -> Result<i64> {
            Ok(0)
        }

        fn incr(
            &self,
            application: &str,
            provider: &str,
            tenant: Option<&str>,
            key: &str,
        ) -> Result<i64> {
            let mut increments = self.increments.lock().unwrap();
            increments.push((
                application.to_string(),
                provider.to_string(),
                tenant.map(str::to_string),
                key.to_string(),
            ));
            Ok(increments.len() as i64)
        }
    }

    #[derive(Default)]
    struct RecordingEmail {
        deliveries: Mutex<Vec<(String, String, EmailMessage)>>,
    }

    impl EmailProvider for RecordingEmail {
        fn available(&self, _application: &str, _provider: &str) -> Result<()> {
            Ok(())
        }

        fn send(
            &self,
            application: &str,
            provider: &str,
            message: EmailMessage,
        ) -> Result<crate::EmailDelivery> {
            self.deliveries.lock().unwrap().push((
                application.to_string(),
                provider.to_string(),
                message,
            ));
            Ok(crate::EmailDelivery {
                accepted: true,
                delivery_id: "delivery-1".to_string(),
                status: "sent".to_string(),
                transport: "recording".to_string(),
            })
        }
    }

    #[derive(Default)]
    struct RecordingGrpc {
        calls: Mutex<Vec<(String, String, String, Value, u64, String)>>,
    }

    impl GrpcProvider for RecordingGrpc {
        fn available(&self, _application: &str, _provider: &str) -> Result<()> {
            Ok(())
        }

        fn unary(
            &self,
            application: &str,
            provider: &str,
            method: &str,
            _contract: &bicdb_extension::abi_v2::ApplicationGrpcClientV1,
            payload: Value,
            deadline_ms: u64,
            trace_id: &str,
        ) -> Result<Value> {
            self.calls.lock().unwrap().push((
                application.to_string(),
                provider.to_string(),
                method.to_string(),
                payload,
                deadline_ms,
                trace_id.to_string(),
            ));
            Ok(json!({"accepted": true, "reservation_id": "reservation-1"}))
        }
    }

    fn redis_manifest() -> Arc<ExtensionManifest> {
        let mut extension = Arc::try_unwrap(manifest()).expect("test manifest is uniquely owned");
        extension.capabilities.extend([
            ExtensionCapability::NetworkEgress,
            ExtensionCapability::Observability,
        ]);
        let application = extension.application.as_deref_mut().unwrap();
        application.required_features.extend([
            ApplicationFeature::Egress,
            ApplicationFeature::Observability,
        ]);
        application.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "max_steps": 100,
                "max_call_depth": 4,
                "callables": {
                    "route": {
                        "parameters": [],
                        "body": [{
                            "op": "expr",
                            "value": {
                                "op": "call",
                                "kind": "builtin",
                                "target": "redis.incr",
                                "method": null,
                                "result_type": "int",
                                "argument_types": ["string"],
                                "argument_item_types": [null],
                                "result_projection": null,
                                "arguments": [{
                                    "name": null,
                                    "value": {"op": "literal", "value": "counter"}
                                }]
                            }
                        }]
                    }
                },
                "redis": {
                    "version": 1,
                    "provider": "default",
                    "helpers": ["redis.incr"],
                    "max_key_bytes": 16384,
                    "max_channel_bytes": 16384,
                    "max_message_bytes": 16777216,
                    "tenant_scoped_keys": true,
                    "application_scoped_channels": true,
                    "emit_evidence": true
                }
            }))
            .unwrap(),
        );
        Arc::new(extension)
    }

    fn email_manifest() -> Arc<ExtensionManifest> {
        let mut extension = Arc::try_unwrap(manifest()).expect("test manifest is uniquely owned");
        extension.capabilities.extend([
            ExtensionCapability::NetworkEgress,
            ExtensionCapability::Observability,
        ]);
        let application = extension.application.as_deref_mut().unwrap();
        application.required_features.extend([
            ApplicationFeature::Egress,
            ApplicationFeature::Observability,
        ]);
        application.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "max_steps": 100,
                "max_call_depth": 4,
                "callables": {
                    "send": {
                        "parameters": ["to", "subject", "text"],
                        "body": [{
                            "op": "expr",
                            "value": {
                                "op": "call",
                                "kind": "builtin",
                                "target": "email.send",
                                "arguments": [
                                    {"name": "from", "value": {"op": "literal", "value": "care@example.test"}},
                                    {"name": "to", "value": {"op": "variable", "name": "to"}},
                                    {"name": "subject", "value": {"op": "variable", "name": "subject"}},
                                    {"name": "text", "value": {"op": "variable", "name": "text"}}
                                ]
                            }
                        }]
                    }
                },
                "email": {
                    "version": 1,
                    "provider": "default",
                    "helper": "email.send",
                    "max_recipients": 10,
                    "max_address_bytes": 512,
                    "max_subject_bytes": 8192,
                    "max_body_bytes": 1048576,
                    "emit_evidence": true
                }
            }))
            .unwrap(),
        );
        Arc::new(extension)
    }

    fn grpc_manifest() -> Arc<ExtensionManifest> {
        let mut extension = Arc::try_unwrap(manifest()).expect("test manifest is uniquely owned");
        extension.capabilities.extend([
            ExtensionCapability::NetworkEgress,
            ExtensionCapability::Observability,
        ]);
        let application = extension.application.as_deref_mut().unwrap();
        application.required_features.extend([
            ApplicationFeature::Egress,
            ApplicationFeature::Observability,
        ]);
        application.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "max_steps": 100,
                "max_call_depth": 4,
                "callables": {
                    "reserve": {
                        "parameters": ["request"],
                        "body": [{
                            "op": "return",
                            "value": {
                                "op": "call",
                                "kind": "client",
                                "target": "InventoryGrpc",
                                "method": "Reserve",
                                "arguments": [{
                                    "value": {"op": "variable", "name": "request"}
                                }]
                            }
                        }]
                    }
                },
                "grpc": {
                    "version": 1,
                    "clients": {
                        "InventoryGrpc": {
                            "provider": "inventory",
                            "service": "Inventory",
                            "methods": {
                                "Reserve": {
                                    "path": "/carrier.inventory.v1.Inventory/Reserve",
                                    "request_type": "Request",
                                    "response_type": "Response",
                                    "deadline_ms": 2000,
                                    "retries": 1
                                }
                            },
                            "messages": {
                                "Request": {
                                    "proto_name": "ReserveRequest",
                                    "fields": [{
                                        "name": "sku",
                                        "proto_name": "sku",
                                        "wire_type": "string",
                                        "tag": 1,
                                        "repeated": false,
                                        "optional": false
                                    }]
                                },
                                "Response": {
                                    "proto_name": "ReserveResponse",
                                    "fields": [
                                        {
                                            "name": "accepted",
                                            "proto_name": "accepted",
                                            "wire_type": "bool",
                                            "tag": 1,
                                            "repeated": false,
                                            "optional": false
                                        },
                                        {
                                            "name": "reservation_id",
                                            "proto_name": "reservation_id",
                                            "wire_type": "string",
                                            "tag": 2,
                                            "repeated": false,
                                            "optional": false
                                        }
                                    ]
                                }
                            },
                            "max_request_bytes": 1024,
                            "max_response_bytes": 1024,
                            "emit_evidence": true
                        }
                    }
                }
            }))
            .unwrap(),
        );
        Arc::new(extension)
    }

    fn call(host: &mut CapabilityHost, request: HostRequest) -> HostCallResult {
        host.call(HostCall {
            request_id: 7,
            request,
        })
    }

    #[test]
    fn redis_host_reserves_evidence_before_tenant_scoped_side_effects() {
        let directory = tempfile::tempdir().unwrap();
        let database = BicDb::open(directory.path().join("redis-host")).unwrap();
        let provider = Arc::new(RecordingRedis::default());
        let observations = Arc::new(Mutex::<Vec<ObservabilityEvent>>::default());
        let build_services = |maximum_events| {
            let mut services = InvocationServices::new(
                Arc::new(InMemorySecretProvider::default()),
                Arc::new(ProductionEgressProvider::new().unwrap()),
                Arc::new(
                    LocalBlobProvider::open(directory.path().join("blobs"), vec![7; 32]).unwrap(),
                ),
                observations.clone(),
            )
            .with_redis_provider(provider.clone());
            services.max_observability_events = maximum_events;
            services
        };

        let mut denied = CapabilityHost::new(
            &database,
            redis_manifest(),
            actor(now_ms() + 10_000),
            build_services(0),
        )
        .unwrap();
        let denied = call(
            &mut denied,
            HostRequest::Redis(RedisRequest::Incr {
                provider: "default".to_string(),
                key: "counter".to_string(),
            }),
        );
        assert_eq!(denied.error.unwrap().class, ErrorClass::Unauthorized);
        assert!(provider.increments.lock().unwrap().is_empty());

        let mut host = CapabilityHost::new(
            &database,
            redis_manifest(),
            actor(now_ms() + 10_000),
            build_services(10),
        )
        .unwrap();
        let result = call(
            &mut host,
            HostRequest::Redis(RedisRequest::Incr {
                provider: "default".to_string(),
                key: "counter".to_string(),
            }),
        );
        assert_eq!(result.value, Some(HostValue::I64(1)));
        assert_eq!(
            provider.increments.lock().unwrap().as_slice(),
            &[(
                "security_test".to_string(),
                "default".to_string(),
                Some("tenant-1".to_string()),
                "counter".to_string(),
            )]
        );
        let events = observations.lock().unwrap();
        let evidence = events.iter().find_map(|event| match event {
            ObservabilityEvent::Evidence {
                control,
                outcome,
                fields,
                ..
            } if control == "redis.incr" => Some((outcome, fields)),
            _ => None,
        });
        let (outcome, fields) = evidence.expect("Redis evidence is recorded");
        assert_eq!(outcome, "success");
        assert_eq!(fields["provider"], "default");
        assert_eq!(fields["subject_sha256"].as_str().unwrap().len(), 64);
        assert_ne!(fields["subject_sha256"], "counter");
    }

    #[test]
    fn email_host_reserves_redacted_evidence_before_delivery() {
        let directory = tempfile::tempdir().unwrap();
        let database = BicDb::open(directory.path().join("email-host")).unwrap();
        let provider = Arc::new(RecordingEmail::default());
        let observations = Arc::new(Mutex::<Vec<ObservabilityEvent>>::default());
        let build_services = |maximum_events| {
            let mut services = InvocationServices::new(
                Arc::new(InMemorySecretProvider::default()),
                Arc::new(ProductionEgressProvider::new().unwrap()),
                Arc::new(
                    LocalBlobProvider::open(directory.path().join("blobs"), vec![7; 32]).unwrap(),
                ),
                observations.clone(),
            )
            .with_email_provider(provider.clone());
            services.max_observability_events = maximum_events;
            services
        };
        let request = || {
            HostRequest::Email(EmailRequest::Send {
                provider: "default".to_string(),
                from: "care@example.test".to_string(),
                to: vec!["ada@example.test".to_string()],
                cc: Vec::new(),
                bcc: Vec::new(),
                reply_to: None,
                subject: "Appointment".to_string(),
                text: Some("Your appointment is confirmed.".to_string()),
                html: None,
            })
        };

        let mut denied = CapabilityHost::new(
            &database,
            email_manifest(),
            actor(now_ms() + 10_000),
            build_services(0),
        )
        .unwrap();
        assert_eq!(
            call(&mut denied, request()).error.unwrap().class,
            ErrorClass::Unauthorized
        );
        assert!(provider.deliveries.lock().unwrap().is_empty());

        let mut host = CapabilityHost::new(
            &database,
            email_manifest(),
            actor(now_ms() + 10_000),
            build_services(10),
        )
        .unwrap();
        let result = call(&mut host, request());
        assert_eq!(
            result.value,
            Some(HostValue::Json(json!({
                "accepted": true,
                "delivery_id": "delivery-1",
                "status": "sent",
                "transport": "recording"
            })))
        );
        let deliveries = provider.deliveries.lock().unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].0, "security_test");
        assert_eq!(deliveries[0].1, "default");
        assert_eq!(deliveries[0].2.to, ["ada@example.test"]);
        drop(deliveries);

        let events = observations.lock().unwrap();
        let (_, fields) = events
            .iter()
            .find_map(|event| match event {
                ObservabilityEvent::Evidence {
                    control,
                    outcome,
                    fields,
                    ..
                } if control == "email.send" => Some((outcome, fields)),
                _ => None,
            })
            .expect("email evidence is recorded");
        assert_eq!(fields["recipient_count"], 1);
        assert_eq!(fields["recipient_set_sha256"].as_str().unwrap().len(), 64);
        let encoded = serde_json::to_string(fields).unwrap();
        assert!(!encoded.contains("ada@example.test"));
        assert!(!encoded.contains("appointment is confirmed"));
    }

    #[test]
    fn grpc_host_enforces_signed_method_and_reserves_redacted_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let database = BicDb::open(directory.path().join("grpc-host")).unwrap();
        let provider = Arc::new(RecordingGrpc::default());
        let observations = Arc::new(Mutex::<Vec<ObservabilityEvent>>::default());
        let build_services = |maximum_events| {
            let mut services = InvocationServices::new(
                Arc::new(InMemorySecretProvider::default()),
                Arc::new(ProductionEgressProvider::new().unwrap()),
                Arc::new(
                    LocalBlobProvider::open(directory.path().join("blobs"), vec![7; 32]).unwrap(),
                ),
                observations.clone(),
            )
            .with_grpc_provider(provider.clone());
            services.max_observability_events = maximum_events;
            services
        };

        let mut quota_denied = CapabilityHost::new(
            &database,
            grpc_manifest(),
            actor(now_ms() + 10_000),
            build_services(0),
        )
        .unwrap();
        let denied = call(
            &mut quota_denied,
            HostRequest::Grpc(GrpcRequest::Unary {
                client: "InventoryGrpc".to_string(),
                method: "Reserve".to_string(),
                payload: json!({"sku": "secret-sku"}),
                deadline_unix_ms: now_ms() + 1_000,
            }),
        );
        assert_eq!(denied.error.unwrap().class, ErrorClass::Unauthorized);
        assert!(provider.calls.lock().unwrap().is_empty());

        let mut host = CapabilityHost::new(
            &database,
            grpc_manifest(),
            actor(now_ms() + 10_000),
            build_services(10),
        )
        .unwrap();
        let result = call(
            &mut host,
            HostRequest::Grpc(GrpcRequest::Unary {
                client: "InventoryGrpc".to_string(),
                method: "Reserve".to_string(),
                payload: json!({"sku": "secret-sku"}),
                deadline_unix_ms: now_ms() + 1_000,
            }),
        );
        assert_eq!(
            result.value,
            Some(HostValue::Json(json!({
                "accepted": true,
                "reservation_id": "reservation-1"
            })))
        );
        let calls = provider.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "security_test");
        assert_eq!(calls[0].1, "inventory");
        assert_eq!(calls[0].2, "Reserve");
        assert_eq!(calls[0].5, "trace-1");
        drop(calls);

        let rejected = call(
            &mut host,
            HostRequest::Grpc(GrpcRequest::Unary {
                client: "InventoryGrpc".to_string(),
                method: "DeleteEverything".to_string(),
                payload: json!({}),
                deadline_unix_ms: now_ms() + 1_000,
            }),
        );
        assert_eq!(rejected.error.unwrap().class, ErrorClass::Unauthorized);
        assert_eq!(provider.calls.lock().unwrap().len(), 1);

        let evidence = observations
            .lock()
            .unwrap()
            .iter()
            .find_map(|event| match event {
                ObservabilityEvent::Evidence {
                    control, fields, ..
                } if control == "grpc.unary" => Some(fields.clone()),
                _ => None,
            })
            .expect("gRPC evidence is recorded");
        assert_eq!(evidence["method"], "Reserve");
        assert!(!serde_json::to_string(&evidence)
            .unwrap()
            .contains("secret-sku"));
    }

    fn handle(value: HostCallResult) -> HostHandle {
        match value.value {
            Some(HostValue::Handle(handle)) => handle,
            _ => panic!("expected handle, got {value:?}"),
        }
    }

    #[test]
    fn invocation_handles_deadlines_and_declarations_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("db")).unwrap();
        db.create_collection("allowed").unwrap();
        bicdb_sql::ensure_embedded_table_schema(
            &mut db,
            "allowed",
            &[
                bicdb_sql::EmbeddedTableColumn {
                    name: "id".to_string(),
                    pg_type: "text".to_string(),
                    nullable: false,
                    primary_key: true,
                    vector_dimensions: None,
                },
                bicdb_sql::EmbeddedTableColumn {
                    name: "value".to_string(),
                    pg_type: "int8".to_string(),
                    nullable: false,
                    primary_key: false,
                    vector_dimensions: None,
                },
            ],
        )
        .unwrap();
        db.insert(
            "allowed",
            Record::new("row-1").with_metadata(json!({"value": 7})),
        )
        .unwrap();
        let mut host = CapabilityHost::new(
            &db,
            manifest(),
            actor(now_ms() + 60_000),
            services(directory.path()),
        )
        .unwrap();

        let first = handle(call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Begin {
                isolation: IsolationLevel::ReadCommitted,
            }),
        ));
        let second = handle(call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Begin {
                isolation: IsolationLevel::ReadCommitted,
            }),
        ));
        let savepoint = handle(call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Savepoint {
                transaction: first,
                name: "before".to_string(),
            }),
        ));
        let cross_transaction = call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::RollbackTo {
                transaction: second,
                savepoint,
            }),
        );
        assert_eq!(
            cross_transaction.error.unwrap().class,
            ErrorClass::Unauthorized
        );

        let forged = call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Commit {
                transaction: HostHandle(u64::MAX),
            }),
        );
        assert_eq!(forged.error.unwrap().class, ErrorClass::Unauthorized);

        let undeclared_relation = call(
            &mut host,
            HostRequest::Database(DatabaseRequest::Query {
                transaction: first,
                query: QuerySpec {
                    relation: "not_declared".to_string(),
                    filters: vec![],
                    sort: vec![],
                    columns: vec![],
                    limit: 1,
                    offset: 0,
                    cursor: None,
                },
            }),
        );
        assert_eq!(
            undeclared_relation.error.unwrap().class,
            ErrorClass::Unauthorized
        );

        let undeclared_sql = call(
            &mut host,
            HostRequest::Database(DatabaseRequest::RawSql {
                transaction: first,
                statement_id: "not_declared".to_string(),
                parameters: vec![],
            }),
        );
        assert_eq!(
            undeclared_sql.error.unwrap().class,
            ErrorClass::Unauthorized
        );
        let declared_sql = call(
            &mut host,
            HostRequest::Database(DatabaseRequest::RawSql {
                transaction: first,
                statement_id: "allowed_scan".to_string(),
                parameters: vec![],
            }),
        );
        assert_eq!(
            declared_sql.value,
            Some(HostValue::Rows(vec![json!({"id": "row-1", "value": 7})]))
        );

        let undeclared_secret = call(
            &mut host,
            HostRequest::Secret(SecretRequest::Open {
                name: "not_declared".to_string(),
                version: None,
            }),
        );
        assert_eq!(
            undeclared_secret.error.unwrap().class,
            ErrorClass::Unauthorized
        );
        let publish = call(
            &mut host,
            HostRequest::Broker(BrokerRequest::Publish {
                queue: "security_events".to_string(),
                payload: json!({"ok": true}),
                headers: BTreeMap::from([
                    ("actor_id".to_string(), "forged-user".to_string()),
                    ("tenant_id".to_string(), "forged-tenant".to_string()),
                    (
                        "originating_resource".to_string(),
                        "forged-resource".to_string(),
                    ),
                ]),
                idempotency_key: None,
                delay_ms: None,
            }),
        );
        assert!(publish.error.is_none());

        assert!(call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Commit { transaction: first }),
        )
        .error
        .is_none());
        let reused = call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Commit { transaction: first }),
        );
        assert_eq!(reused.error.unwrap().class, ErrorClass::Unauthorized);
        assert!(call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Rollback {
                transaction: second
            }),
        )
        .error
        .is_none());

        let mut expired = CapabilityHost::new(
            &db,
            manifest(),
            actor(now_ms() - 1),
            services(directory.path()),
        )
        .unwrap();
        let timeout = call(&mut expired, HostRequest::Clock(ClockRequest::WallTime));
        assert_eq!(timeout.error.unwrap().class, ErrorClass::Timeout);
        drop(expired);
        drop(host);
        let event = db
            .with_broker(|broker| {
                broker.consume(
                    "security_events",
                    "test",
                    "test",
                    ConsumeOptions {
                        max_messages: 1,
                        visibility_timeout_ms: 1_000,
                    },
                )
            })
            .unwrap()
            .pop()
            .unwrap();
        let headers = &event.headers["bicdb_application"];
        assert_eq!(headers["actor_id"], "user-1");
        assert_eq!(headers["tenant_id"], "tenant-1");
        assert!(headers.get("originating_resource").is_none());
    }

    #[test]
    fn declared_sql_insert_is_visible_to_resource_read_in_same_transaction() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("raw-sql-read-your-writes-db")).unwrap();
        db.create_collection("allowed").unwrap();
        bicdb_sql::ensure_embedded_table_schema(
            &mut db,
            "allowed",
            &[
                bicdb_sql::EmbeddedTableColumn {
                    name: "id".to_string(),
                    pg_type: "uuid".to_string(),
                    nullable: false,
                    primary_key: true,
                    vector_dimensions: None,
                },
                bicdb_sql::EmbeddedTableColumn {
                    name: "value".to_string(),
                    pg_type: "int8".to_string(),
                    nullable: false,
                    primary_key: false,
                    vector_dimensions: None,
                },
                bicdb_sql::EmbeddedTableColumn {
                    name: "tenant_id".to_string(),
                    pg_type: "text".to_string(),
                    nullable: false,
                    primary_key: false,
                    vector_dimensions: None,
                },
            ],
        )
        .unwrap();
        db.set_collection_policy(
            "allowed",
            CollectionPolicy::tenant_field("tenant_id")
                .with_read_roles(["editor"])
                .with_write_roles(["editor"]),
        )
        .unwrap();
        db.set_mutation_policy(
            "allowed",
            MutationPolicy::grants_required().with_tenant_field("tenant_id"),
        )
        .unwrap();

        let mut extension = Arc::try_unwrap(manifest()).expect("unique test manifest");
        extension
            .permissions
            .write_relations
            .insert("allowed".to_string());
        let application = extension.application.as_deref_mut().unwrap();
        let permission = application
            .relation_permissions
            .iter_mut()
            .find(|permission| permission.relation == "allowed")
            .unwrap();
        permission.actions.insert(DatabaseAction::Insert);
        permission.writable_columns.extend([
            "id".to_string(),
            "value".to_string(),
            "tenant_id".to_string(),
        ]);
        application.raw_sql.push(RawSqlDeclaration {
            id: "allowed_insert".to_string(),
            sql: "INSERT INTO allowed (id, value, tenant_id) VALUES ($1, $2, $3)".to_string(),
            sha256: hash(),
            relations: BTreeSet::from(["allowed".to_string()]),
            routines: BTreeSet::new(),
            actions: BTreeSet::from([DatabaseAction::Insert]),
            parameters: vec![FieldType::Uuid, FieldType::Int64, FieldType::String],
            result: vec![],
            max_affected_rows: 10,
        });

        let mut request_actor = actor(now_ms() + 60_000);
        request_actor.roles.insert("editor".to_string());
        let mut host = CapabilityHost::new(
            &db,
            Arc::new(extension),
            request_actor,
            services(directory.path()),
        )
        .unwrap();
        let transaction = handle(call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Begin {
                isolation: IsolationLevel::ReadCommitted,
            }),
        ));
        let savepoint = handle(call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Savepoint {
                transaction,
                name: "carrier_1".to_string(),
            }),
        ));
        assert_eq!(
            call(
                &mut host,
                HostRequest::Database(DatabaseRequest::RawSql {
                    transaction,
                    statement_id: "allowed_insert".to_string(),
                    parameters: vec![
                        json!("25bdbe3e-3616-4a88-aae8-5cf735b0a6a2"),
                        json!(8),
                        json!("tenant-1"),
                    ],
                }),
            )
            .value,
            Some(HostValue::U64(1))
        );
        assert!(call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Release {
                transaction,
                savepoint,
            }),
        )
        .error
        .is_none());
        let pending_rows = host
            .transaction_mut(transaction)
            .unwrap()
            .scan_collection_for_integrity_check("allowed")
            .unwrap();
        assert_eq!(
            pending_rows
                .iter()
                .map(|record| record.id.clone())
                .collect::<Vec<_>>(),
            vec!["25bdbe3e-3616-4a88-aae8-5cf735b0a6a2".to_string()]
        );
        let pending = &pending_rows[0];
        assert_eq!(pending.metadata["tenant_id"], json!("tenant-1"));
        assert_eq!(
            call(
                &mut host,
                HostRequest::Database(DatabaseRequest::SelectPrimaryKey {
                    transaction,
                    relation: "allowed".to_string(),
                    id: "25bdbe3e-3616-4a88-aae8-5cf735b0a6a2".to_string(),
                    columns: vec![],
                }),
            )
            .value,
            Some(HostValue::Json(json!({
                "id": "25bdbe3e-3616-4a88-aae8-5cf735b0a6a2",
                "tenant_id": "tenant-1",
                "value": 8
            })))
        );
        assert!(call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Rollback { transaction }),
        )
        .error
        .is_none());
    }

    #[test]
    fn omitted_projection_returns_only_signed_readable_columns() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("db")).unwrap();
        db.create_collection("allowed").unwrap();
        db.insert(
            "allowed",
            Record::new("row-1").with_metadata(json!({
                "value": 7,
                "private_note": "must-not-leak",
                "_private": "must-not-leak-either"
            })),
        )
        .unwrap();

        let mut limited_manifest = (*manifest()).clone();
        limited_manifest
            .application
            .as_mut()
            .unwrap()
            .relation_permissions[0]
            .readable_columns = BTreeSet::from(["id".to_string(), "value".to_string()]);
        let mut host = CapabilityHost::new(
            &db,
            Arc::new(limited_manifest),
            actor(now_ms() + 60_000),
            services(directory.path()),
        )
        .unwrap();
        let transaction = handle(call(
            &mut host,
            HostRequest::Transaction(TransactionRequest::Begin {
                isolation: IsolationLevel::ReadCommitted,
            }),
        ));

        let default_projection = call(
            &mut host,
            HostRequest::Database(DatabaseRequest::Query {
                transaction,
                query: QuerySpec {
                    relation: "allowed".to_string(),
                    filters: vec![],
                    sort: vec![],
                    columns: vec![],
                    limit: 1,
                    offset: 0,
                    cursor: None,
                },
            }),
        );
        assert_eq!(
            default_projection.value,
            Some(HostValue::Rows(vec![json!({"id": "row-1", "value": 7})]))
        );

        let forged_projection = call(
            &mut host,
            HostRequest::Database(DatabaseRequest::SelectPrimaryKey {
                transaction,
                relation: "allowed".to_string(),
                id: "row-1".to_string(),
                columns: vec!["private_note".to_string()],
            }),
        );
        assert_eq!(
            forged_projection.error.unwrap().class,
            ErrorClass::Unauthorized
        );

        let mut deny_all_manifest = (*manifest()).clone();
        let application = deny_all_manifest.application.as_mut().unwrap();
        application
            .required_features
            .insert(ApplicationFeature::ExactColumnAuthority);
        application.relation_permissions[0].readable_columns.clear();
        let mut deny_all_host = CapabilityHost::new(
            &db,
            Arc::new(deny_all_manifest),
            actor(now_ms() + 60_000),
            services(directory.path()),
        )
        .unwrap();
        let deny_all_transaction = handle(call(
            &mut deny_all_host,
            HostRequest::Transaction(TransactionRequest::Begin {
                isolation: IsolationLevel::ReadCommitted,
            }),
        ));
        let empty_default = call(
            &mut deny_all_host,
            HostRequest::Database(DatabaseRequest::Query {
                transaction: deny_all_transaction,
                query: QuerySpec {
                    relation: "allowed".to_string(),
                    filters: vec![],
                    sort: vec![],
                    columns: vec![],
                    limit: 1,
                    offset: 0,
                    cursor: None,
                },
            }),
        );
        assert_eq!(empty_default.value, Some(HostValue::Rows(vec![json!({})])));
        let explicit_id = call(
            &mut deny_all_host,
            HostRequest::Database(DatabaseRequest::SelectPrimaryKey {
                transaction: deny_all_transaction,
                relation: "allowed".to_string(),
                id: "row-1".to_string(),
                columns: vec!["id".to_string()],
            }),
        );
        assert_eq!(explicit_id.error.unwrap().class, ErrorClass::Unauthorized);
    }

    #[test]
    fn dependency_cycles_fail_closed_and_all_transaction_isolations_are_native() {
        let directory = tempfile::tempdir().unwrap();
        let db = BicDb::open(directory.path().join("db")).unwrap();
        let mut host = CapabilityHost::with_service_trace(
            &db,
            manifest(),
            actor(now_ms() + 60_000),
            services(directory.path()),
            vec!["security_test".to_string(), "dependency".to_string()],
        )
        .unwrap();
        let cycle = call(
            &mut host,
            HostRequest::Service(ServiceRequest::Call {
                dependency: "dependency".to_string(),
                service: "example".to_string(),
                method: "run".to_string(),
                payload: Value::Null,
                transaction: None,
                deadline_unix_ms: now_ms() + 10_000,
            }),
        );
        assert_eq!(cycle.error.unwrap().class, ErrorClass::Unauthorized);

        for isolation in [IsolationLevel::RepeatableRead, IsolationLevel::Serializable] {
            let transaction = handle(call(
                &mut host,
                HostRequest::Transaction(TransactionRequest::Begin { isolation }),
            ));
            assert!(call(
                &mut host,
                HostRequest::Transaction(TransactionRequest::Rollback { transaction }),
            )
            .error
            .is_none());
        }
    }
}

fn record_columns(record: &Record) -> BTreeSet<String> {
    let mut columns: BTreeSet<String> = record
        .metadata
        .as_object()
        .map(|object| object.keys().cloned().collect())
        .unwrap_or_default();
    columns.insert("id".to_string());
    if record.vector.is_some() {
        columns.insert("vector".to_string());
    }
    if record.geometry.is_some() {
        columns.insert("geometry".to_string());
    }
    if record.timestamp.is_some() {
        columns.insert("timestamp".to_string());
    }
    columns
}

fn filter_field(filter: &FilterExpression) -> String {
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
        | FilterExpression::JsonPathExists { field, .. } => field.clone(),
    }
}

fn filter_matches(record: &Value, filter: &FilterExpression) -> bool {
    let field = filter_field(filter);
    let current = record.get(&field);
    match filter {
        // BicDB stores rows sparsely, so an omitted nullable SQL column is not
        // present in record metadata. PostgreSQL still exposes that column as
        // NULL. Resource filters use Eq/Ne with JSON null to model IS
        // NULL/IS NOT NULL (not SQL's three-valued `= NULL`), so missing and
        // explicit-null fields must have identical behavior here.
        FilterExpression::Eq { value, .. } if value.is_null() => current.is_none_or(Value::is_null),
        FilterExpression::Ne { value, .. } if value.is_null() => {
            current.is_some_and(|current| !current.is_null())
        }
        FilterExpression::Eq { value, .. } => current == Some(value),
        FilterExpression::Ne { value, .. } => current != Some(value),
        FilterExpression::Lt { value, .. } => compare_value(current, Some(value)).is_lt(),
        FilterExpression::Le { value, .. } => !compare_value(current, Some(value)).is_gt(),
        FilterExpression::Gt { value, .. } => compare_value(current, Some(value)).is_gt(),
        FilterExpression::Ge { value, .. } => !compare_value(current, Some(value)).is_lt(),
        FilterExpression::In { values, .. } => {
            current.is_some_and(|current| values.contains(current))
        }
        FilterExpression::Contains { value, .. } => current
            .and_then(Value::as_str)
            .is_some_and(|current| current.to_lowercase().contains(&value.to_lowercase())),
        FilterExpression::TypedEq {
            value, value_type, ..
        } => current.is_some_and(|current| typed_json_equal(current, value, value_type)),
        FilterExpression::TypedGe {
            value, value_type, ..
        } => current
            .and_then(|current| typed_json_compare(current, value, value_type))
            .is_some_and(|ordering| !ordering.is_lt()),
        FilterExpression::JsonPathEq {
            path,
            value,
            value_type,
            ..
        } => current
            .and_then(|current| current.pointer(path))
            .is_some_and(|current| match value_type {
                Some(value_type) => typed_json_equal(current, value, value_type),
                None => current == value,
            }),
        FilterExpression::JsonPathGe {
            path,
            value,
            value_type,
            ..
        } => current
            .and_then(|current| current.pointer(path))
            .and_then(|current| typed_json_compare(current, value, value_type))
            .is_some_and(|ordering| !ordering.is_lt()),
        FilterExpression::JsonPathContains {
            path, value, array, ..
        } => current
            .and_then(|current| current.pointer(path))
            .is_some_and(|current| {
                if *array {
                    current.as_array().is_some_and(|values| {
                        values
                            .iter()
                            .any(|candidate| candidate.as_str() == Some(value))
                    })
                } else {
                    current.as_str().is_some_and(|current| {
                        current.to_lowercase().contains(&value.to_lowercase())
                    })
                }
            }),
        FilterExpression::JsonPathExists { path, exists, .. } => {
            current.and_then(|current| current.pointer(path)).is_some() == *exists
        }
    }
}

fn json_path_value<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut value = value;
    for segment in path
        .trim_start_matches('$')
        .trim_start_matches('.')
        .split('.')
    {
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

fn spatial_rows(rows: Vec<Value>, operation: SpatialOperation) -> Result<Vec<Value>> {
    match operation {
        SpatialOperation::Nearest {
            field,
            point,
            limit,
        } => {
            let point = carrier_point(&point, "nearest point")?;
            let mut rows = rows
                .into_iter()
                .filter_map(|row| {
                    let candidate = row
                        .get(&field)
                        .and_then(|value| carrier_point(value, "stored point").ok())?;
                    Some((
                        point_distance(&candidate, &point),
                        carrier_row_id(&row),
                        row,
                    ))
                })
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| {
                left.0
                    .total_cmp(&right.0)
                    .then_with(|| left.1.cmp(&right.1))
            });
            Ok(rows
                .into_iter()
                .take(limit as usize)
                .map(|(_, _, row)| row)
                .collect())
        }
        SpatialOperation::WithinRadius {
            field,
            point,
            radius,
            limit,
        } => {
            if !radius.is_finite() || radius < 0.0 {
                return Err(AppRuntimeError::CapabilityDenied(
                    "within radius must be a finite non-negative number".to_string(),
                ));
            }
            let point = carrier_point(&point, "within point")?;
            let mut rows = rows
                .into_iter()
                .filter_map(|row| {
                    let candidate = row
                        .get(&field)
                        .and_then(|value| carrier_point(value, "stored point").ok())?;
                    (point_distance(&candidate, &point) <= radius)
                        .then_some((carrier_row_id(&row), row))
                })
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| left.0.cmp(&right.0));
            Ok(rows
                .into_iter()
                .take(limit as usize)
                .map(|(_, row)| row)
                .collect())
        }
        SpatialOperation::Contains {
            field,
            point,
            limit,
        } => {
            let point = carrier_point(&point, "contains point")?;
            let mut rows = rows
                .into_iter()
                .filter_map(|row| {
                    let polygon = row.get(&field).and_then(|value| {
                        Geometry::from_geojson_value(value.clone())
                            .ok()
                            .and_then(|geometry| match geometry {
                                Geometry::Polygon(polygon) => Some(polygon),
                                _ => None,
                            })
                    })?;
                    polygon
                        .contains(&point)
                        .then_some((carrier_row_id(&row), row))
                })
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| left.0.cmp(&right.0));
            Ok(rows
                .into_iter()
                .take(limit as usize)
                .map(|(_, row)| row)
                .collect())
        }
        SpatialOperation::Within {
            field,
            geometry,
            limit,
        } => {
            let envelope = geometry_envelope(&geometry)?;
            Ok(rows
                .into_iter()
                .filter(|row| {
                    row.get(&field)
                        .and_then(|value| geometry_envelope(value).ok())
                        .is_some_and(|candidate| envelope_contains(envelope, candidate))
                })
                .take(limit as usize)
                .collect())
        }
        SpatialOperation::Intersects {
            field,
            geometry,
            limit,
        } => {
            let envelope = geometry_envelope(&geometry)?;
            Ok(rows
                .into_iter()
                .filter(|row| {
                    row.get(&field)
                        .and_then(|value| geometry_envelope(value).ok())
                        .is_some_and(|candidate| envelopes_intersect(envelope, candidate))
                })
                .take(limit as usize)
                .collect())
        }
    }
}

fn carrier_point(value: &Value, usage: &str) -> Result<geo::Point<f64>> {
    match Geometry::from_geojson_value(value.clone()) {
        Ok(Geometry::Point(point)) => Ok(point),
        _ => Err(AppRuntimeError::CapabilityDenied(format!(
            "{usage} must be a valid GeoJSON Point"
        ))),
    }
}

fn point_distance(left: &geo::Point<f64>, right: &geo::Point<f64>) -> f64 {
    let dx = left.x() - right.x();
    let dy = left.y() - right.y();
    dx.hypot(dy)
}

fn carrier_row_id(row: &Value) -> String {
    row.get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn envelope_contains(outer: [f64; 4], inner: [f64; 4]) -> bool {
    inner[0] >= outer[0] && inner[1] >= outer[1] && inner[2] <= outer[2] && inner[3] <= outer[3]
}

fn envelopes_intersect(left: [f64; 4], right: [f64; 4]) -> bool {
    left[0] <= right[2] && left[2] >= right[0] && left[1] <= right[3] && left[3] >= right[1]
}

fn carrier_recent_cutoff(window: &str, retention: Option<&str>) -> Result<i64> {
    use bicdb_sql::typed_value::{PgInterval, PgTimestamp};

    let now = PgTimestamp::from_postgres_text(&chrono::Utc::now().to_rfc3339(), true)
        .map_err(|error| AppRuntimeError::Invocation(format!("cannot read host time: {error}")))?;
    let cutoff = |interval: &str| -> Result<i64> {
        let interval = PgInterval::from_postgres_text(interval).map_err(|_| {
            AppRuntimeError::InvalidRequest(format!("invalid PostgreSQL interval `{interval}`"))
        })?;
        now.checked_sub_interval(interval)
            .map_err(|_| {
                AppRuntimeError::InvalidRequest("timeseries interval overflow".to_string())
            })?
            .finite_micros()
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest("timeseries cutoff is not finite".to_string())
            })
    };
    let mut effective = cutoff(window)?;
    if let Some(retention) = retention {
        effective = effective.max(cutoff(retention)?);
    }
    Ok(effective)
}

fn carrier_retention_cutoff(retention: &str) -> Option<i64> {
    carrier_recent_cutoff(retention, None).ok()
}

fn geometry_envelope(geometry: &Value) -> Result<[f64; 4]> {
    if let Some(bbox) = geometry.get("bbox").and_then(Value::as_array) {
        if bbox.len() == 4 {
            let values = [
                bbox[0].as_f64(),
                bbox[1].as_f64(),
                bbox[2].as_f64(),
                bbox[3].as_f64(),
            ];
            if values.iter().all(Option::is_some) {
                return Ok([
                    values[0].unwrap(),
                    values[1].unwrap(),
                    values[2].unwrap(),
                    values[3].unwrap(),
                ]);
            }
        }
    }
    let coordinates = geometry.get("coordinates").unwrap_or(geometry);
    let mut points = Vec::new();
    collect_coordinates(coordinates, &mut points);
    if points.is_empty() {
        return Err(AppRuntimeError::CapabilityDenied(
            "geometry has no finite coordinates".to_string(),
        ));
    }
    let mut envelope = [
        f64::INFINITY,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
    ];
    for (x, y) in points {
        envelope[0] = envelope[0].min(x);
        envelope[1] = envelope[1].min(y);
        envelope[2] = envelope[2].max(x);
        envelope[3] = envelope[3].max(y);
    }
    Ok(envelope)
}

fn collect_coordinates(value: &Value, points: &mut Vec<(f64, f64)>) {
    let Some(values) = value.as_array() else {
        return;
    };
    if values.len() >= 2 {
        if let (Some(x), Some(y)) = (values[0].as_f64(), values[1].as_f64()) {
            if x.is_finite() && y.is_finite() {
                points.push((x, y));
            }
            return;
        }
    }
    for value in values {
        collect_coordinates(value, points);
    }
}

fn compare_value(left: Option<&Value>, right: Option<&Value>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(Value::Number(left)), Some(Value::Number(right))) => left
            .as_f64()
            .partial_cmp(&right.as_f64())
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(Value::String(left)), Some(Value::String(right))) => left.cmp(right),
        (Some(Value::Bool(left)), Some(Value::Bool(right))) => left.cmp(right),
        (None, None) => std::cmp::Ordering::Equal,
        (None, _) => std::cmp::Ordering::Less,
        (_, None) => std::cmp::Ordering::Greater,
        (Some(left), Some(right)) => left.to_string().cmp(&right.to_string()),
    }
}

fn compare_rows(left: &Value, right: &Value, sort: &[SortField]) -> std::cmp::Ordering {
    for field in sort {
        let ordering = compare_value(left.get(&field.field), right.get(&field.field));
        let ordering = if field.descending {
            ordering.reverse()
        } else {
            ordering
        };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    compare_value(left.get("id"), right.get("id"))
}

fn project(mut record: Value, columns: &[String]) -> Value {
    if columns.is_empty() {
        return record;
    }
    if let Some(object) = record.as_object_mut() {
        object.retain(|field, _| columns.iter().any(|column| column == field));
    }
    record
}

fn aggregate_field(aggregate: &AggregateSpec) -> Vec<String> {
    match aggregate {
        AggregateSpec::Count => vec![],
        AggregateSpec::Sum { field }
        | AggregateSpec::Min { field }
        | AggregateSpec::Max { field }
        | AggregateSpec::Average { field } => vec![field.clone()],
    }
}

fn aggregate_rows(rows: &[Value], aggregate: AggregateSpec) -> HostValue {
    if matches!(aggregate, AggregateSpec::Count) {
        return HostValue::U64(rows.len() as u64);
    }
    let field = aggregate_field(&aggregate).pop().unwrap();
    let values = rows
        .iter()
        .filter_map(|row| row.get(&field))
        .filter_map(Value::as_f64)
        .collect::<Vec<_>>();
    match aggregate {
        AggregateSpec::Sum { .. } => HostValue::F64(values.iter().sum()),
        AggregateSpec::Min { .. } => {
            HostValue::F64(values.into_iter().fold(f64::INFINITY, f64::min))
        }
        AggregateSpec::Max { .. } => {
            HostValue::F64(values.into_iter().fold(f64::NEG_INFINITY, f64::max))
        }
        AggregateSpec::Average { .. } => HostValue::F64(if values.is_empty() {
            0.0
        } else {
            values.iter().sum::<f64>() / values.len() as f64
        }),
        AggregateSpec::Count => unreachable!(),
    }
}

fn event_headers(
    actor: &ActorContext,
    plugin: &str,
    mut headers: BTreeMap<String, String>,
) -> Value {
    // Actor, tenancy, origin, and transaction fields are host-owned. Removing
    // them first prevents a module from supplying a trusted-looking value when
    // the real actor field is absent.
    for reserved in [
        "trace_id",
        "correlation_id",
        "causation_id",
        "trace_flags",
        "tracestate",
        "parent_span_id",
        "trace_sampled",
        "actor_id",
        "actor_roles",
        "actor_scopes",
        "tenant_id",
        "workspace_id",
        "organization_id",
        "originating_plugin",
        "originating_resource",
        "originating_action",
        "schema_version",
        "contract_version",
        "event_schema_version",
        "transaction_id",
        "commit_sequence",
    ] {
        headers.remove(reserved);
    }
    headers.insert("trace_id".to_string(), actor.trace_id.clone());
    if let Some(value) = &actor.correlation_id {
        headers.insert("correlation_id".to_string(), value.clone());
    }
    if let Some(value) = &actor.causation_id {
        headers.insert("causation_id".to_string(), value.clone());
    }
    if let Some(value) = actor.policy_attributes.get("w3c.trace_flags") {
        headers.insert("trace_flags".to_string(), value.clone());
    }
    if let Some(value) = actor.policy_attributes.get("w3c.tracestate") {
        headers.insert("tracestate".to_string(), value.clone());
    }
    if let Some(value) = actor.policy_attributes.get("w3c.parent_span_id") {
        headers.insert("parent_span_id".to_string(), value.clone());
    }
    if let Some(value) = actor.policy_attributes.get("carrier.trace.sampled") {
        headers.insert("trace_sampled".to_string(), value.clone());
    }
    if let Some(value) = actor.user_id.as_ref().or(actor.service_id.as_ref()) {
        headers.insert("actor_id".to_string(), value.clone());
    }
    headers.insert(
        "actor_roles".to_string(),
        serde_json::to_string(&actor.roles).expect("roles serialize"),
    );
    headers.insert(
        "actor_scopes".to_string(),
        serde_json::to_string(&actor.scopes).expect("scopes serialize"),
    );
    if let Some(value) = &actor.tenant_id {
        headers.insert("tenant_id".to_string(), value.clone());
    }
    if let Some(value) = &actor.workspace_id {
        headers.insert("workspace_id".to_string(), value.clone());
    }
    if let Some(value) = &actor.organization_id {
        headers.insert("organization_id".to_string(), value.clone());
    }
    headers.insert("originating_plugin".to_string(), plugin.to_string());
    headers.insert("schema_version".to_string(), "1".to_string());
    json!({"bicdb_application": headers})
}

fn string_headers(value: &Value) -> BTreeMap<String, String> {
    value
        .get("bicdb_application")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.to_string())))
        .collect()
}

fn system_header(value: &Value, name: &str) -> Option<String> {
    value
        .get("bicdb_application")
        .and_then(|headers| headers.get(name))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Result<Vec<u8>> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)
        .map_err(|error| AppRuntimeError::Provider(error.to_string()))?;
    mac.update(message);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn encryption_key(material: &[u8]) -> [u8; 32] {
    Sha256::digest(material).into()
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn redis_subject_hash(kind: &[u8], value: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bicdb-carrier-redis-evidence-v1\0");
    digest.update(kind);
    digest.update(b"\0");
    digest.update(value);
    encode_hex(&digest.finalize())
}

fn email_subject_hash(value: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bicdb-carrier-email-evidence-v1\0");
    digest.update(value);
    encode_hex(&digest.finalize())
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    let bytes = value.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(AppRuntimeError::InvalidRequest(
            "hex input must contain an even number of digits".to_string(),
        ));
    }
    bytes
        .chunks_exact(2)
        .map(|digits| {
            let high = hex_nibble(digits[0]);
            let low = hex_nibble(digits[1]);
            match (high, low) {
                (Some(high), Some(low)) => Ok((high << 4) | low),
                _ => Err(AppRuntimeError::InvalidRequest(
                    "hex input contains a non-hexadecimal digit".to_string(),
                )),
            }
        })
        .collect()
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |different, (left, right)| different | (left ^ right))
        == 0
}
