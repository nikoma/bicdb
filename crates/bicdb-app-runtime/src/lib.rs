//! Batteries-included, capability-mediated application runtime for BicDB.
//!
//! The crate is the trusted bridge between ABI-v2 WebAssembly and BicDB. It
//! owns package verification, actor authentication, native transactions,
//! MutationGrants, broker/outbox access, services, secrets, crypto, egress,
//! blobs, observability, and lifecycle/readiness state. WASM receives only the
//! typed `bicdb:app/host.call` import.

mod auth;
mod bounded_sql;
mod host;
mod http;
mod openapi;
mod otlp;
mod package;
mod package_input;
pub use package_input::{encoded_package_byte_limit, MAX_MODULE_NAME_BYTES, MAX_PACKAGE_MODULES};
mod program;
mod providers;
mod resource;
mod runtime;

pub use auth::{JwtAudience, JwtAuthenticator, JwtClaims, JwtConfiguration};
pub use bicdb_extension::abi_v2::{
    ActorContext, ApplicationCallKindV1, ApplicationTelemetryProtocolV1,
};
pub use host::{
    CapabilityHost, InvocationServices, LiveRealtimeFrame, LiveRealtimeResponse, PluginServiceCall,
    PluginServiceDispatcher, RealtimeProvider, RealtimeResponse,
};
pub use http::{
    ApplicationHttpRequest, ApplicationHttpResponse, HttpAdmissionCheck, HttpHostPolicy,
    HttpServerHandle, TrustedHttpHandler,
};
pub use openapi::generate_openapi;
pub use otlp::{OtlpObservability, OtlpObservabilityConfig};
pub use package::{
    canonical_signing_payload, ApplicationCapability, ApplicationComponent,
    ApplicationComponentKind, ApplicationDataClass, ApplicationDatabaseFeature,
    ApplicationExecutionScope, ApplicationPackage, FrontendAsset, PackageVerification,
    PackageVerifier, TrustedSigningKeys,
};
pub use program::{
    execute_application_program, ApplicationProgramHost, DenyApplicationProgramHost,
};
pub use providers::{
    BlobProvider, BlobRecord, BoundedObservability, BufferedRealtimeProvider,
    CompositeObservability, DenyBlobProvider, DenyEgressProvider, DenyEmailProvider,
    DenyEmbeddingsProvider, DenyEvaluationProvider, DenyGrpcProvider, DenyLlmProvider,
    DenyRedisProvider, DenyTokenizerProvider, EgressProvider, EmailDelivery, EmailMessage,
    EmailProvider, EmbeddingsProvider, EvaluationProvider, GrpcProvider, HostObservability,
    HttpEgressProviderConfig, InMemoryEvaluationProvider, InMemorySecretProvider,
    JsonlObservability, LlmProvider, LlmProviderRequest, LlmProviderResponse, LlmProviderToolCall,
    LocalBlobProvider, ObservabilityEvent, ProductionEgressProvider, RedisProvider, SecretProvider,
    SecretRecord, TokenizerProvider,
};
pub use resource::{execute_resource_operation, ResourceRequest, ResourceResponse};
pub use runtime::{
    ApplicationEvaluationResult, ApplicationHostConfig, ApplicationPackageState,
    ApplicationPerformanceSnapshot, ApplicationReadiness, ApplicationRuntime,
    ApplicationTestResult, PackageSnapshot, RuntimeDiagnostic,
};

use thiserror::Error;

pub type Result<T> = std::result::Result<T, AppRuntimeError>;

#[derive(Debug, Error)]
pub enum AppRuntimeError {
    #[error("invalid application package: {0}")]
    InvalidPackage(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("{message}")]
    ApplicationFailure {
        code: String,
        message: String,
        retryable: bool,
    },
    #[error("missing idempotency key: {0}")]
    MissingIdempotencyKey(String),
    #[error("application package signature failed: {0}")]
    Signature(String),
    #[error("authentication failed: {0}")]
    Authentication(String),
    #[error("application capability denied: {0}")]
    CapabilityDenied(String),
    #[error("application invocation failed: {0}")]
    Invocation(String),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("operation conflict: {0}")]
    Conflict(String),
    #[error("idempotency key reused: {0}")]
    IdempotencyKeyReused(String),
    #[error("optimistic concurrency conflict: {0}")]
    OptimisticConflict(String),
    #[error("operation timed out: {0}")]
    Timeout(String),
    #[error("resilience block timed out: {0}")]
    ResilienceTimeout(String),
    #[error("operation cancelled: {0}")]
    Cancelled(String),
    #[error("resource limit exceeded: {0}")]
    ResourceExhausted(String),
    #[error("rate limit exceeded: {0}")]
    RateLimited(String),
    #[error("application provider failed: {0}")]
    Provider(String),
    #[error("application is not ready: {0}")]
    NotReady(String),
    #[error("circuit breaker is open: {0}")]
    CircuitOpen(String),
    #[error(transparent)]
    BicDb(#[from] bicdb_core::BicDbError),
    #[error(transparent)]
    Extension(#[from] bicdb_extension::ExtensionError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
