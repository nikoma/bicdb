use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::TcpListener as StdTcpListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use base64::Engine;
use bicdb_app_runtime::{
    ActorContext, ApplicationHostConfig, ApplicationPackage, ApplicationRuntime,
    ApplicationTelemetryProtocolV1, BlobProvider, BoundedObservability, CompositeObservability,
    HostObservability, HttpEgressProviderConfig, HttpHostPolicy, InMemoryEvaluationProvider,
    InMemorySecretProvider, InvocationServices, JsonlObservability, JwtAuthenticator,
    JwtConfiguration, LocalBlobProvider, OtlpObservability, OtlpObservabilityConfig,
    PackageVerifier, ProductionEgressProvider, TrustedSigningKeys,
};
use bicdb_blob_s3::{S3BlobProvider, S3BlobProviderConfig};
use bicdb_core::BicDb;
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use clap::Subcommand;
use parking_lot::RwLock;
use runtime_email_provider::{EmailProviderConfig, ProductionEmailProvider};
use runtime_embeddings_provider::{EmbeddingsProviderConfig, ProductionEmbeddingsProvider};
use runtime_grpc_provider::{GrpcProviderConfig, ProductionGrpcProvider};
use runtime_llm_provider::{LlmProviderConfig, ProductionLlmProvider};
use runtime_redis_provider::{ProductionRedisProvider, RedisProviderConfig};
use runtime_tokenizer_provider::{ProductionTokenizerProvider, TokenizerProviderConfig};
use serde::Deserialize;

#[derive(Debug, Subcommand)]
pub enum AppCommand {
    /// Verify, stage, migrate, and atomically activate a package.
    Install {
        package: PathBuf,
    },
    /// Verify and durably stage a package without changing active routes.
    Stage {
        package: PathBuf,
    },
    /// Validate signature, manifest, compatibility profile, and WASM modules.
    Validate {
        package: PathBuf,
    },
    /// Atomically activate a package staged by this or an earlier process.
    Activate {
        application: String,
    },
    /// Atomically activate a dependency-ordered set of staged packages.
    ActivateBatch {
        #[arg(required = true)]
        applications: Vec<String>,
    },
    /// Verify, stage, migrate, and atomically replace an active package.
    Upgrade {
        package: PathBuf,
    },
    /// Restore the prior coherent signed application snapshot.
    Rollback {
        application: String,
    },
    /// Remove an application from the active route and worker catalog.
    Disable {
        application: String,
    },
    /// Remove inactive staged and rollback catalog entries.
    Remove {
        application: String,
    },
    List,
    Inspect {
        application: String,
    },
    /// Read the application's durable, redacted audit observations.
    Observations {
        application: String,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Run a signed BicDB application dataset evaluation with operator-provided cases.
    Evaluate {
        application: String,
        evaluation: String,
        #[arg(long, default_value_t = 300_000)]
        timeout_ms: u64,
    },
    /// Run one compiler-signed BicDB application scenario or property test.
    Test {
        application: String,
        test: String,
        /// Execute one signed case (used by BicDB application's isolated test runner).
        #[arg(long)]
        case_index: Option<u32>,
        #[arg(long, default_value_t = 300_000)]
        timeout_ms: u64,
    },
    VerifySignature {
        package: PathBuf,
    },
    DependencyGraph,
    Routes,
    Services,
    Workers,
    Schedules,
    Doctor,
    /// Report bounded module/program cache and Wasm pool state.
    Performance,
    ExportDiagnostics {
        output: PathBuf,
    },
    /// Run HTTP applications and pgwire over one shared in-process BicDB.
    Serve {
        #[arg(long, default_value = "127.0.0.1")]
        http_host: String,
        #[arg(long, default_value_t = 8080)]
        http_port: u16,
        #[arg(long)]
        http_tls_cert: Option<PathBuf>,
        #[arg(long)]
        http_tls_key: Option<PathBuf>,
        #[arg(long, default_value = "127.0.0.1")]
        pg_host: String,
        #[arg(long, default_value_t = 5433)]
        pg_port: u16,
        /// PostgreSQL compatibility version advertised by the embedded pgwire listener.
        #[arg(long, default_value = bicdb_sql::POSTGRES_COMPATIBILITY_VERSION)]
        pg_server_version: String,
        /// Numeric companion to --pg-server-version; derived automatically when omitted.
        #[arg(long)]
        pg_server_version_num: Option<String>,
        #[arg(long)]
        pg_require_auth: bool,
        #[arg(long)]
        pg_tls_cert: Option<PathBuf>,
        #[arg(long)]
        pg_tls_key: Option<PathBuf>,
        #[arg(long)]
        pg_require_tls: bool,
        #[arg(long)]
        jwt_hs256_key: Option<PathBuf>,
        #[arg(long)]
        oidc_jwks: Option<PathBuf>,
        /// Operator-owned JSON verifier registry for signed per-route schemes.
        #[arg(long)]
        auth_config: Option<PathBuf>,
        #[arg(long)]
        jwt_issuer: Option<String>,
        #[arg(long)]
        jwt_audience: Option<String>,
        #[arg(long, default_value_t = 30_000)]
        request_timeout_ms: u64,
        #[arg(long, default_value_t = 8 * 1024 * 1024)]
        max_request_bytes: usize,
        #[arg(long, default_value_t = 16 * 1024 * 1024)]
        max_response_bytes: usize,
        #[arg(long, default_value_t = 1024)]
        max_concurrent_requests: usize,
        #[arg(long, default_value_t = 1000)]
        rate_limit_per_minute: u32,
    },
}

struct RuntimeBundle {
    runtime: Arc<ApplicationRuntime>,
    db: Arc<RwLock<BicDb>>,
}

pub fn run(
    database_path: PathBuf,
    package_root: Option<PathBuf>,
    trusted_key_specs: Vec<String>,
    secret_specs: Vec<String>,
    blob_config: Option<PathBuf>,
    integration_config: Option<PathBuf>,
    command: AppCommand,
) -> Result<()> {
    let package_root = package_root.unwrap_or_else(|| database_path.join("applications"));
    let bundle = open_runtime(
        &database_path,
        &package_root,
        &trusted_key_specs,
        &secret_specs,
        blob_config.as_deref(),
        integration_config.as_deref(),
    )?;
    match command {
        AppCommand::Install { package } => {
            let package = read_package(&package, &bundle.runtime)?;
            let application = package.manifest.identity.name.clone();
            let verification = bundle.runtime.install(package)?;
            let readiness = bundle.runtime.activate(&application)?;
            print_json(&serde_json::json!({
                "application": application,
                "package_sha256": verification.package_sha256,
                "readiness": readiness,
            }))?;
        }
        AppCommand::Stage { package } => {
            let verification = bundle
                .runtime
                .stage(read_package(&package, &bundle.runtime)?)?;
            print_json(&serde_json::json!({
                "state": "staged",
                "package_sha256": verification.package_sha256,
                "signing_key_id": verification.signing_key_id,
            }))?;
        }
        AppCommand::Validate { package } | AppCommand::VerifySignature { package } => {
            let verification = bundle
                .runtime
                .validate(&read_package(&package, &bundle.runtime)?)?;
            print_json(&serde_json::json!({
                "valid": true,
                "package_sha256": verification.package_sha256,
                "module_sha256": verification.module_sha256,
                "signing_key_id": verification.signing_key_id,
            }))?;
        }
        AppCommand::Activate { application } => {
            print_json(&bundle.runtime.activate(&application)?)?;
        }
        AppCommand::ActivateBatch { applications } => {
            print_json(&bundle.runtime.activate_batch(&applications)?)?;
        }
        AppCommand::Upgrade { package } => {
            print_json(
                &bundle
                    .runtime
                    .upgrade(read_package(&package, &bundle.runtime)?)?,
            )?;
        }
        AppCommand::Rollback { application } => {
            print_json(&bundle.runtime.rollback(&application)?)?;
        }
        AppCommand::Disable { application } => {
            bundle.runtime.disable(&application)?;
            print_json(&serde_json::json!({"application": application, "state": "disabled"}))?;
        }
        AppCommand::Remove { application } => {
            bundle.runtime.remove(&application)?;
            print_json(&serde_json::json!({"application": application, "state": "removed"}))?;
        }
        AppCommand::List => print_json(&bundle.runtime.list())?,
        AppCommand::Inspect { application } => {
            print_json(&bundle.runtime.inspect(&application)?)?;
        }
        AppCommand::Performance => print_json(&bundle.runtime.performance_snapshot())?,
        AppCommand::Observations { application, limit } => {
            print_json(&bundle.runtime.durable_observations(&application, limit)?)?;
        }
        AppCommand::Evaluate {
            application,
            evaluation,
            timeout_ms,
        } => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before the Unix epoch")?
                .as_millis();
            let now = i64::try_from(now).context("system clock exceeds i64 milliseconds")?;
            let timeout_ms = i64::try_from(timeout_ms).context("evaluation timeout exceeds i64")?;
            let actor = ActorContext {
                service_id: Some("bicdb-evaluation-runner".to_string()),
                trace_id: format!("bicdb-eval-{now}"),
                deadline_unix_ms: now.saturating_add(timeout_ms),
                ..ActorContext::default()
            };
            let result = bundle
                .runtime
                .evaluate_application(&application, &evaluation, actor)?;
            print_json(&result)?;
            if !result.requirement_passed {
                bail!("BicDB application evaluation `{evaluation}` did not satisfy its signed requirement");
            }
        }
        AppCommand::Test {
            application,
            test,
            case_index,
            timeout_ms,
        } => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before the Unix epoch")?
                .as_millis();
            let now = i64::try_from(now).context("system clock exceeds i64 milliseconds")?;
            let timeout_ms = i64::try_from(timeout_ms).context("test timeout exceeds i64")?;
            let actor = ActorContext {
                service_id: Some("bicdb-test-runner".to_string()),
                trace_id: format!("bicdb-test-{now}"),
                deadline_unix_ms: now.saturating_add(timeout_ms),
                ..ActorContext::default()
            };
            let result = match case_index {
                Some(index) => {
                    bundle
                        .runtime
                        .test_application_case(&application, &test, actor, index)?
                }
                None => bundle
                    .runtime
                    .test_application(&application, &test, actor)?,
            };
            print_json(&result)?;
            if !result.passed {
                bail!("BicDB application test `{test}` failed");
            }
        }
        AppCommand::DependencyGraph => print_json(&bundle.runtime.dependency_graph())?,
        AppCommand::Routes => print_json(&bundle.runtime.routes())?,
        AppCommand::Services => print_json(&bundle.runtime.services())?,
        AppCommand::Workers => print_json(&bundle.runtime.workers())?,
        AppCommand::Schedules => print_json(&bundle.runtime.schedules())?,
        AppCommand::Doctor => {
            print_json(&serde_json::json!({
                "readiness": bundle.runtime.readiness(),
                "packages": bundle.runtime.doctor(),
            }))?;
        }
        AppCommand::ExportDiagnostics { output } => {
            atomic_write(&output, &bundle.runtime.export_diagnostics()?)?;
            println!("{}", output.display());
        }
        AppCommand::Serve {
            http_host,
            http_port,
            http_tls_cert,
            http_tls_key,
            pg_host,
            pg_port,
            pg_server_version,
            pg_server_version_num,
            pg_require_auth,
            pg_tls_cert,
            pg_tls_key,
            pg_require_tls,
            jwt_hs256_key,
            oidc_jwks,
            auth_config,
            jwt_issuer,
            jwt_audience,
            request_timeout_ms,
            max_request_bytes,
            max_response_bytes,
            max_concurrent_requests,
            rate_limit_per_minute,
        } => serve(
            database_path,
            bundle,
            ServeOptions {
                http_host,
                http_port,
                http_tls_cert,
                http_tls_key,
                pg_host,
                pg_port,
                pg_server_version,
                pg_server_version_num,
                pg_require_auth,
                pg_tls_cert,
                pg_tls_key,
                pg_require_tls,
                jwt_hs256_key,
                oidc_jwks,
                auth_config,
                jwt_issuer,
                jwt_audience,
                request_timeout_ms,
                max_request_bytes,
                max_response_bytes,
                max_concurrent_requests,
                rate_limit_per_minute,
            },
        )?,
    }
    Ok(())
}

fn open_runtime(
    database_path: &Path,
    package_root: &Path,
    trusted_key_specs: &[String],
    secret_specs: &[String],
    blob_config: Option<&Path>,
    integration_config: Option<&Path>,
) -> Result<RuntimeBundle> {
    let mut trusted = TrustedSigningKeys::default();
    for specification in trusted_key_specs {
        let (id, path) = split_once(specification, "trusted key", "KEY_ID=FILE")?;
        let bytes = decode_public_key(&fs::read(path).with_context(|| format!("read {path}"))?)?;
        trusted.insert_ed25519(id, &bytes)?;
    }
    let verifier = PackageVerifier::new(trusted, 256 * 1024 * 1024)?;
    let secret_provider = InMemorySecretProvider::default();
    for specification in secret_specs {
        let mut parts = specification.splitn(3, '=');
        let (Some(name), Some(version), Some(path)) = (parts.next(), parts.next(), parts.next())
        else {
            bail!("secret must use NAME=VERSION=FILE");
        };
        secret_provider.insert(
            name,
            version,
            format!("{name}:{version}"),
            "opaque",
            fs::read(path).with_context(|| format!("read secret file {path}"))?,
            true,
        )?;
    }
    let blob_key = load_or_create_blob_key(package_root)?;
    let blob_provider = open_blob_provider(package_root, blob_config, blob_key)?;
    let providers = open_integration_providers(integration_config)?;
    let observability: Arc<dyn HostObservability> = match &providers.observability {
        Some(config) => {
            let mut sinks = Vec::<Arc<dyn HostObservability>>::new();
            if let Some(path) = &config.jsonl_path {
                sinks.push(Arc::new(JsonlObservability::open(path, config.capacity)?));
            }
            if let Some(otlp) = &config.otlp {
                sinks.push(Arc::new(OtlpObservability::open(otlp.clone())?));
            }
            match sinks.len() {
                0 => bail!("observability config requires jsonl_path, otlp, or both"),
                1 => sinks.pop().expect("one observability sink"),
                _ => Arc::new(CompositeObservability::new(sinks)?),
            }
        }
        None => Arc::new(BoundedObservability::new(100_000)?),
    };
    let services = InvocationServices::new(
        Arc::new(secret_provider),
        Arc::new(providers.egress),
        blob_provider,
        observability,
    )
    .with_redis_provider(Arc::new(providers.redis))
    .with_email_provider(Arc::new(providers.email))
    .with_grpc_provider(Arc::new(providers.grpc))
    .with_tokenizer_provider(Arc::new(providers.tokenizer))
    .with_embeddings_provider(Arc::new(providers.embeddings))
    .with_llm_provider(Arc::new(providers.llm))
    .with_evaluation_provider(Arc::new(providers.evaluations));
    let db = Arc::new(RwLock::new(BicDb::open(database_path)?));
    let runtime = ApplicationRuntime::new_shared(
        db.clone(),
        ApplicationHostConfig::new(package_root, host_node_id()),
        verifier,
        services,
    )?;
    Ok(RuntimeBundle { runtime, db })
}

struct ServeOptions {
    http_host: String,
    http_port: u16,
    http_tls_cert: Option<PathBuf>,
    http_tls_key: Option<PathBuf>,
    pg_host: String,
    pg_port: u16,
    pg_server_version: String,
    pg_server_version_num: Option<String>,
    pg_require_auth: bool,
    pg_tls_cert: Option<PathBuf>,
    pg_tls_key: Option<PathBuf>,
    pg_require_tls: bool,
    jwt_hs256_key: Option<PathBuf>,
    oidc_jwks: Option<PathBuf>,
    auth_config: Option<PathBuf>,
    jwt_issuer: Option<String>,
    jwt_audience: Option<String>,
    request_timeout_ms: u64,
    max_request_bytes: usize,
    max_response_bytes: usize,
    max_concurrent_requests: usize,
    rate_limit_per_minute: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthConfigDocument {
    schemes: Vec<AuthVerifierConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum AuthVerifierConfig {
    JwtHs256 {
        issuer: String,
        audience: String,
        #[serde(default)]
        key_file: Option<PathBuf>,
        #[serde(default)]
        keys: Vec<AuthHs256KeyConfig>,
    },
    OidcEd25519 {
        issuer: String,
        audience: String,
        jwks_file: PathBuf,
    },
    OidcRs256 {
        issuer: String,
        audience: String,
        jwks_file: PathBuf,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthHs256KeyConfig {
    key_id: String,
    key_file: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BlobProviderConfigDocument {
    Local {
        #[serde(default)]
        root: Option<PathBuf>,
    },
    S3Compatible {
        endpoint: String,
        bucket: String,
        #[serde(default)]
        key_prefix: String,
        region: String,
        access_key_id_file: PathBuf,
        secret_access_key_file: PathBuf,
        #[serde(default)]
        session_token_file: Option<PathBuf>,
        #[serde(default)]
        force_path_style: bool,
        #[serde(default)]
        allow_insecure_http: bool,
        #[serde(default = "default_blob_max_object_bytes")]
        max_object_bytes: u64,
        #[serde(default = "default_blob_connect_timeout_ms")]
        connect_timeout_ms: u64,
        #[serde(default = "default_blob_request_timeout_ms")]
        request_timeout_ms: u64,
        #[serde(default)]
        server_side_encryption: Option<String>,
        #[serde(default)]
        sse_kms_key_id: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntegrationProviderConfigDocument {
    version: u32,
    #[serde(default)]
    observability: Option<ObservabilityIntegrationConfig>,
    #[serde(default)]
    http: Vec<HttpIntegrationConfig>,
    #[serde(default)]
    redis: Vec<RedisIntegrationConfig>,
    #[serde(default)]
    email: Vec<EmailIntegrationConfig>,
    #[serde(default)]
    grpc: Vec<GrpcIntegrationConfig>,
    #[serde(default)]
    tokenizer: Vec<TokenizerIntegrationConfig>,
    #[serde(default)]
    embeddings: Vec<EmbeddingsIntegrationConfig>,
    #[serde(default)]
    llm: Vec<LlmIntegrationConfig>,
    #[serde(default)]
    evaluations: Vec<EvaluationIntegrationConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservabilityIntegrationConfig {
    #[serde(default)]
    jsonl_path: Option<PathBuf>,
    #[serde(default = "default_observability_capacity")]
    capacity: usize,
    #[serde(default)]
    otlp: Option<OtlpIntegrationConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OtlpIntegrationConfig {
    endpoint: String,
    protocol: ApplicationTelemetryProtocolV1,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    header_files: BTreeMap<String, PathBuf>,
    #[serde(default)]
    allow_insecure_http: bool,
    #[serde(default = "default_otlp_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "default_observability_capacity")]
    queue_capacity: usize,
    #[serde(default = "default_otlp_export_interval_ms")]
    export_interval_ms: u64,
}

const fn default_observability_capacity() -> usize {
    100_000
}

const fn default_otlp_timeout_ms() -> u64 {
    10_000
}

const fn default_otlp_export_interval_ms() -> u64 {
    1_000
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpIntegrationConfig {
    application: String,
    provider: String,
    base_url: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    header_files: BTreeMap<String, PathBuf>,
    #[serde(default)]
    allow_insecure_http: bool,
    #[serde(default)]
    allow_private_networks: bool,
    healthcheck_path: String,
    #[serde(default = "default_http_healthcheck_statuses")]
    healthcheck_statuses: Vec<u16>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RedisIntegrationConfig {
    application: String,
    provider: String,
    endpoint: String,
    #[serde(default)]
    username_file: Option<PathBuf>,
    #[serde(default)]
    password_file: Option<PathBuf>,
    #[serde(default)]
    database: u32,
    #[serde(default = "default_redis_key_prefix")]
    key_prefix: String,
    #[serde(default = "default_redis_channel_prefix")]
    channel_prefix: String,
    #[serde(default)]
    allow_insecure_redis: bool,
    #[serde(default = "default_redis_pool_size")]
    pool_size: u32,
    #[serde(default = "default_redis_connect_timeout_ms")]
    connect_timeout_ms: u64,
    #[serde(default = "default_redis_request_timeout_ms")]
    request_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmailIntegrationConfig {
    application: String,
    provider: String,
    endpoint: String,
    #[serde(default)]
    username_file: Option<PathBuf>,
    #[serde(default)]
    password_file: Option<PathBuf>,
    allowed_from: Vec<String>,
    #[serde(default)]
    allowed_recipient_domains: Vec<String>,
    #[serde(default)]
    allow_insecure_smtp: bool,
    #[serde(default = "default_email_max_recipients")]
    max_recipients: u32,
    #[serde(default = "default_email_max_message_bytes")]
    max_message_bytes: u64,
    #[serde(default = "default_email_pool_size")]
    pool_size: u32,
    #[serde(default = "default_email_timeout_ms")]
    timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrpcIntegrationConfig {
    application: String,
    provider: String,
    endpoint: String,
    #[serde(default)]
    bearer_token_file: Option<PathBuf>,
    #[serde(default)]
    client_cert_file: Option<PathBuf>,
    #[serde(default)]
    client_key_file: Option<PathBuf>,
    #[serde(default)]
    ca_file: Option<PathBuf>,
    #[serde(default)]
    allowed_methods: Vec<String>,
    #[serde(default)]
    allow_insecure_http: bool,
    #[serde(default = "default_grpc_max_message_bytes")]
    max_request_bytes: u64,
    #[serde(default = "default_grpc_max_message_bytes")]
    max_response_bytes: u64,
    #[serde(default = "default_grpc_connect_timeout_ms")]
    connect_timeout_ms: u64,
    #[serde(default = "default_grpc_request_timeout_ms")]
    request_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenizerIntegrationConfig {
    application: String,
    provider: String,
    tokenizer_file: PathBuf,
    #[serde(default = "default_ai_max_input_bytes")]
    max_input_bytes: u64,
    #[serde(default = "default_tokenizer_max_tokens")]
    max_tokens: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmbeddingsIntegrationConfig {
    application: String,
    provider: String,
    model: String,
    model_dir: PathBuf,
    dimensions: usize,
    #[serde(default = "default_ai_max_input_bytes")]
    max_input_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LlmIntegrationConfig {
    application: String,
    provider: String,
    endpoint: String,
    api_key_file: PathBuf,
    wire_format: String,
    model: String,
    #[serde(default)]
    operator_system_prompt_file: Option<PathBuf>,
    #[serde(default = "default_llm_max_body_bytes")]
    max_request_bytes: u64,
    #[serde(default = "default_llm_max_body_bytes")]
    max_response_bytes: u64,
    #[serde(default = "default_llm_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default)]
    allow_insecure_http: bool,
    #[serde(default)]
    input_microusd_per_million_tokens: u64,
    #[serde(default)]
    output_microusd_per_million_tokens: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvaluationIntegrationConfig {
    application: String,
    provider: String,
    dataset_file: PathBuf,
}

fn default_http_healthcheck_statuses() -> Vec<u16> {
    vec![200]
}

fn default_redis_key_prefix() -> String {
    "carrier-cache".to_string()
}

fn default_redis_channel_prefix() -> String {
    "carrier-events".to_string()
}

const fn default_redis_pool_size() -> u32 {
    16
}

const fn default_redis_connect_timeout_ms() -> u64 {
    5_000
}

const fn default_redis_request_timeout_ms() -> u64 {
    5_000
}

const fn default_email_max_recipients() -> u32 {
    100
}

const fn default_email_max_message_bytes() -> u64 {
    4 * 1024 * 1024
}

const fn default_email_pool_size() -> u32 {
    16
}

const fn default_email_timeout_ms() -> u64 {
    30_000
}

const fn default_grpc_max_message_bytes() -> u64 {
    4 * 1024 * 1024
}

const fn default_grpc_connect_timeout_ms() -> u64 {
    5_000
}

const fn default_grpc_request_timeout_ms() -> u64 {
    30_000
}

const fn default_ai_max_input_bytes() -> u64 {
    4 * 1024 * 1024
}

const fn default_tokenizer_max_tokens() -> u64 {
    1_000_000
}

const fn default_llm_max_body_bytes() -> u64 {
    4 * 1024 * 1024
}

const fn default_llm_request_timeout_ms() -> u64 {
    60_000
}

const fn default_blob_max_object_bytes() -> u64 {
    256 * 1024 * 1024
}

const fn default_blob_connect_timeout_ms() -> u64 {
    5_000
}

const fn default_blob_request_timeout_ms() -> u64 {
    60_000
}

fn open_blob_provider(
    package_root: &Path,
    config_path: Option<&Path>,
    signing_key: Vec<u8>,
) -> Result<Arc<dyn BlobProvider>> {
    let Some(config_path) = config_path else {
        return Ok(Arc::new(LocalBlobProvider::open(
            package_root.join("blobs"),
            signing_key,
        )?));
    };
    let document: BlobProviderConfigDocument = serde_json::from_reader(
        File::open(config_path)
            .with_context(|| format!("open blob provider config {}", config_path.display()))?,
    )
    .with_context(|| format!("decode blob provider config {}", config_path.display()))?;
    let base = config_path.parent().unwrap_or_else(|| Path::new("."));
    match document {
        BlobProviderConfigDocument::Local { root } => {
            let root = root
                .map(|path| resolve_auth_path(base, path))
                .unwrap_or_else(|| package_root.join("blobs"));
            Ok(Arc::new(LocalBlobProvider::open(root, signing_key)?))
        }
        BlobProviderConfigDocument::S3Compatible {
            endpoint,
            bucket,
            key_prefix,
            region,
            access_key_id_file,
            secret_access_key_file,
            session_token_file,
            force_path_style,
            allow_insecure_http,
            max_object_bytes,
            connect_timeout_ms,
            request_timeout_ms,
            server_side_encryption,
            sse_kms_key_id,
        } => {
            let provider = S3BlobProvider::new(
                S3BlobProviderConfig {
                    endpoint: endpoint.parse().context("parse S3 endpoint")?,
                    bucket,
                    key_prefix,
                    region,
                    access_key_id: read_provider_credential(
                        &resolve_auth_path(base, access_key_id_file),
                        "S3 access key id",
                    )?,
                    secret_access_key: read_provider_credential(
                        &resolve_auth_path(base, secret_access_key_file),
                        "S3 secret access key",
                    )?,
                    session_token: session_token_file
                        .map(|path| {
                            read_provider_credential(
                                &resolve_auth_path(base, path),
                                "S3 session token",
                            )
                        })
                        .transpose()?,
                    force_path_style,
                    allow_insecure_http,
                    max_object_bytes,
                    connect_timeout: std::time::Duration::from_millis(connect_timeout_ms),
                    request_timeout: std::time::Duration::from_millis(request_timeout_ms),
                    server_side_encryption,
                    sse_kms_key_id,
                },
                signing_key,
            )?;
            provider.healthcheck()?;
            Ok(Arc::new(provider))
        }
    }
}

struct IntegrationProviders {
    observability: Option<ResolvedObservabilityConfig>,
    egress: ProductionEgressProvider,
    redis: ProductionRedisProvider,
    email: ProductionEmailProvider,
    grpc: ProductionGrpcProvider,
    tokenizer: ProductionTokenizerProvider,
    embeddings: ProductionEmbeddingsProvider,
    llm: ProductionLlmProvider,
    evaluations: InMemoryEvaluationProvider,
}

struct ResolvedObservabilityConfig {
    jsonl_path: Option<PathBuf>,
    capacity: usize,
    otlp: Option<OtlpObservabilityConfig>,
}

fn open_integration_providers(config_path: Option<&Path>) -> Result<IntegrationProviders> {
    let Some(config_path) = config_path else {
        return Ok(IntegrationProviders {
            observability: None,
            egress: ProductionEgressProvider::new()?,
            redis: ProductionRedisProvider::new(Vec::new())?,
            email: ProductionEmailProvider::new(Vec::new())?,
            grpc: ProductionGrpcProvider::new(Vec::new())?,
            tokenizer: ProductionTokenizerProvider::new(Vec::new())?,
            embeddings: ProductionEmbeddingsProvider::new(Vec::new())?,
            llm: ProductionLlmProvider::new(Vec::new())?,
            evaluations: InMemoryEvaluationProvider::default(),
        });
    };
    let document: IntegrationProviderConfigDocument = serde_json::from_reader(
        File::open(config_path)
            .with_context(|| format!("open integration config {}", config_path.display()))?,
    )
    .with_context(|| format!("decode integration config {}", config_path.display()))?;
    if document.version != 1 {
        bail!("integration config requires version 1");
    }
    let base = config_path.parent().unwrap_or_else(|| Path::new("."));
    let observability = document
        .observability
        .map(|config| -> Result<ResolvedObservabilityConfig> {
            let jsonl_path = config.jsonl_path.map(|path| resolve_auth_path(base, path));
            let otlp = config
                .otlp
                .map(|mut otlp| -> Result<OtlpObservabilityConfig> {
                    for (name, path) in otlp.header_files {
                        if otlp.headers.contains_key(&name) {
                            bail!("OTLP header `{name}` has both a literal and file value");
                        }
                        otlp.headers.insert(
                            name.clone(),
                            read_provider_credential(
                                &resolve_auth_path(base, path),
                                &format!("OTLP header {name}"),
                            )?,
                        );
                    }
                    Ok(OtlpObservabilityConfig {
                        endpoint: otlp.endpoint,
                        protocol: otlp.protocol,
                        headers: otlp.headers,
                        allow_insecure_http: otlp.allow_insecure_http,
                        timeout: Duration::from_millis(otlp.timeout_ms),
                        queue_capacity: otlp.queue_capacity,
                        export_interval: Duration::from_millis(otlp.export_interval_ms),
                    })
                })
                .transpose()?;
            Ok(ResolvedObservabilityConfig {
                jsonl_path,
                capacity: config.capacity,
                otlp,
            })
        })
        .transpose()?;
    let mut http = Vec::with_capacity(document.http.len());
    for binding in document.http {
        let mut headers = binding.headers;
        for (name, path) in binding.header_files {
            if headers.contains_key(&name) {
                bail!("HTTP provider header `{name}` has both a literal and file value");
            }
            headers.insert(
                name.clone(),
                read_provider_credential(
                    &resolve_auth_path(base, path),
                    &format!("HTTP provider header {name}"),
                )?,
            );
        }
        http.push(HttpEgressProviderConfig {
            application: binding.application,
            provider: binding.provider,
            base_url: binding
                .base_url
                .parse()
                .context("parse HTTP provider base URL")?,
            headers,
            allow_insecure_http: binding.allow_insecure_http,
            allow_private_networks: binding.allow_private_networks,
            healthcheck_path: binding.healthcheck_path,
            healthcheck_statuses: binding.healthcheck_statuses.into_iter().collect(),
        });
    }
    let mut redis = Vec::with_capacity(document.redis.len());
    for binding in document.redis {
        redis.push(RedisProviderConfig {
            application: binding.application,
            provider: binding.provider,
            endpoint: binding.endpoint.parse().context("parse Redis endpoint")?,
            username: binding
                .username_file
                .map(|path| {
                    read_provider_credential(&resolve_auth_path(base, path), "Redis username")
                })
                .transpose()?,
            password: binding
                .password_file
                .map(|path| {
                    read_provider_credential(&resolve_auth_path(base, path), "Redis password")
                })
                .transpose()?,
            database: binding.database,
            key_prefix: binding.key_prefix,
            channel_prefix: binding.channel_prefix,
            allow_insecure_redis: binding.allow_insecure_redis,
            pool_size: binding.pool_size,
            connect_timeout: std::time::Duration::from_millis(binding.connect_timeout_ms),
            request_timeout: std::time::Duration::from_millis(binding.request_timeout_ms),
        });
    }
    let mut email = Vec::with_capacity(document.email.len());
    for binding in document.email {
        email.push(EmailProviderConfig {
            application: binding.application,
            provider: binding.provider,
            endpoint: binding.endpoint.parse().context("parse SMTP endpoint")?,
            username: binding
                .username_file
                .map(|path| {
                    read_provider_credential(&resolve_auth_path(base, path), "SMTP username")
                })
                .transpose()?,
            password: binding
                .password_file
                .map(|path| {
                    read_provider_credential(&resolve_auth_path(base, path), "SMTP password")
                })
                .transpose()?,
            allowed_from: binding.allowed_from.into_iter().collect::<BTreeSet<_>>(),
            allowed_recipient_domains: binding
                .allowed_recipient_domains
                .into_iter()
                .map(|domain| domain.to_ascii_lowercase())
                .collect::<BTreeSet<_>>(),
            allow_insecure_smtp: binding.allow_insecure_smtp,
            max_recipients: binding.max_recipients,
            max_message_bytes: binding.max_message_bytes,
            pool_size: binding.pool_size,
            timeout: std::time::Duration::from_millis(binding.timeout_ms),
        });
    }
    let mut grpc = Vec::with_capacity(document.grpc.len());
    for binding in document.grpc {
        grpc.push(GrpcProviderConfig {
            application: binding.application,
            provider: binding.provider,
            endpoint: binding.endpoint.parse().context("parse gRPC endpoint")?,
            bearer_token: binding
                .bearer_token_file
                .map(|path| {
                    read_provider_credential(&resolve_auth_path(base, path), "gRPC bearer token")
                })
                .transpose()?,
            client_cert_pem: binding
                .client_cert_file
                .map(|path| {
                    read_provider_secret_bytes(
                        &resolve_auth_path(base, path),
                        "gRPC client certificate",
                    )
                })
                .transpose()?,
            client_key_pem: binding
                .client_key_file
                .map(|path| {
                    read_provider_secret_bytes(&resolve_auth_path(base, path), "gRPC client key")
                })
                .transpose()?,
            ca_pem: binding
                .ca_file
                .map(|path| {
                    read_provider_secret_bytes(
                        &resolve_auth_path(base, path),
                        "gRPC CA certificate",
                    )
                })
                .transpose()?,
            allowed_methods: binding.allowed_methods.into_iter().collect(),
            allow_insecure_http: binding.allow_insecure_http,
            max_request_bytes: binding.max_request_bytes,
            max_response_bytes: binding.max_response_bytes,
            connect_timeout: std::time::Duration::from_millis(binding.connect_timeout_ms),
            request_timeout: std::time::Duration::from_millis(binding.request_timeout_ms),
        });
    }
    let tokenizer = document
        .tokenizer
        .into_iter()
        .map(|binding| TokenizerProviderConfig {
            application: binding.application,
            provider: binding.provider,
            tokenizer_file: resolve_auth_path(base, binding.tokenizer_file),
            max_input_bytes: binding.max_input_bytes,
            max_tokens: binding.max_tokens,
        })
        .collect::<Vec<_>>();
    let embeddings = document
        .embeddings
        .into_iter()
        .map(|binding| {
            let model = bicdb_core::ModelRegistryEntry::local_onnx(
                binding.model,
                resolve_auth_path(base, binding.model_dir),
                binding.dimensions,
            )?;
            Ok(EmbeddingsProviderConfig {
                application: binding.application,
                provider: binding.provider,
                model,
                max_input_bytes: binding.max_input_bytes,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let llm = document
        .llm
        .into_iter()
        .map(|binding| {
            Ok(LlmProviderConfig {
                application: binding.application,
                provider: binding.provider,
                endpoint: binding.endpoint.parse().context("parse LLM endpoint")?,
                api_key: read_provider_credential(
                    &resolve_auth_path(base, binding.api_key_file),
                    "LLM API key",
                )?,
                wire_format: binding.wire_format,
                model: binding.model,
                operator_system_prompt: binding
                    .operator_system_prompt_file
                    .map(|path| {
                        read_provider_text_secret(
                            &resolve_auth_path(base, path),
                            "LLM operator system prompt",
                        )
                    })
                    .transpose()?,
                max_request_bytes: binding.max_request_bytes,
                max_response_bytes: binding.max_response_bytes,
                request_timeout: std::time::Duration::from_millis(binding.request_timeout_ms),
                allow_insecure_http: binding.allow_insecure_http,
                input_microusd_per_million_tokens: binding.input_microusd_per_million_tokens,
                output_microusd_per_million_tokens: binding.output_microusd_per_million_tokens,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let egress = ProductionEgressProvider::with_http_providers(http)?;
    let redis = ProductionRedisProvider::new(redis)?;
    let email = ProductionEmailProvider::new(email)?;
    let grpc = ProductionGrpcProvider::new(grpc)?;
    let tokenizer = ProductionTokenizerProvider::new(tokenizer)?;
    let embeddings = ProductionEmbeddingsProvider::new(embeddings)?;
    let llm = ProductionLlmProvider::new(llm)?;
    let evaluations = InMemoryEvaluationProvider::default();
    for binding in document.evaluations {
        let path = resolve_auth_path(base, binding.dataset_file);
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("read evaluation dataset {}", path.display()))?;
        let cases = contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .with_context(|| format!("decode evaluation dataset {}", path.display()))
            })
            .collect::<Result<Vec<serde_json::Value>>>()?;
        evaluations.insert(binding.application, binding.provider, cases);
    }
    egress.healthcheck_providers()?;
    redis.healthcheck()?;
    email.healthcheck()?;
    grpc.healthcheck()?;
    tokenizer.healthcheck()?;
    embeddings.healthcheck()?;
    llm.healthcheck()?;
    Ok(IntegrationProviders {
        observability,
        egress,
        redis,
        email,
        grpc,
        tokenizer,
        embeddings,
        llm,
        evaluations,
    })
}

fn read_provider_secret_bytes(path: &Path, label: &str) -> Result<Vec<u8>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = fs::metadata(path)
            .with_context(|| format!("inspect {label} file {}", path.display()))?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            bail!(
                "{label} file {} must not be accessible by group or other users",
                path.display()
            );
        }
    }
    let value = fs::read(path).with_context(|| format!("read {label} file {}", path.display()))?;
    if value.is_empty() || value.len() > 1024 * 1024 || value.contains(&0) {
        bail!("{label} file {} is empty or invalid", path.display());
    }
    Ok(value)
}

fn read_provider_credential(path: &Path, label: &str) -> Result<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = fs::metadata(path)
            .with_context(|| format!("inspect {label} file {}", path.display()))?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            bail!(
                "{label} file {} must not be accessible by group or other users",
                path.display()
            );
        }
    }
    let value = fs::read_to_string(path)
        .with_context(|| format!("read {label} file {}", path.display()))?;
    let value = value.trim();
    if value.is_empty()
        || value
            .chars()
            .any(|character| matches!(character, '\r' | '\n'))
    {
        bail!(
            "{label} file {} must contain one non-empty line",
            path.display()
        );
    }
    Ok(value.to_string())
}

fn read_provider_text_secret(path: &Path, label: &str) -> Result<String> {
    let value = String::from_utf8(read_provider_secret_bytes(path, label)?)
        .with_context(|| format!("decode {label} file {} as UTF-8", path.display()))?;
    if value.trim().is_empty()
        || value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        bail!("{label} file {} contains invalid text", path.display());
    }
    Ok(value)
}

fn serve(database_path: PathBuf, bundle: RuntimeBundle, options: ServeOptions) -> Result<()> {
    if !bundle.runtime.readiness().ready {
        bail!(
            "application host is not ready: {}",
            serde_json::to_string(&bundle.runtime.readiness())?
        );
    }
    let authenticator = load_authenticator(&options)?;
    let db = Arc::clone(&bundle.db);
    let policy = HttpHostPolicy {
        max_request_bytes: options.max_request_bytes,
        max_response_bytes: options.max_response_bytes,
        request_timeout_ms: options.request_timeout_ms,
        rate_limit_per_minute: options.rate_limit_per_minute,
        max_concurrent_requests: options.max_concurrent_requests,
        ..HttpHostPolicy::default()
    };
    let pg_listener = StdTcpListener::bind((&*options.pg_host, options.pg_port))
        .context("bind pgwire listener")?;
    let pg_address = pg_listener.local_addr()?;
    let mut pg_config = PgWireConfig::default();
    pg_config.host = options.pg_host.clone();
    pg_config.port = pg_address.port();
    pg_config.postgres_server_version = options.pg_server_version.clone();
    let derived_version_num =
        bicdb_pgwire::derive_postgres_server_version_num(&options.pg_server_version)
            .map_err(anyhow::Error::msg)?;
    pg_config.postgres_server_version_num = match options.pg_server_version_num {
        Some(version_num) if version_num != derived_version_num => {
            bail!(
                "--pg-server-version-num {version_num} does not match \
                 --pg-server-version {} (expected {derived_version_num})",
                options.pg_server_version
            );
        }
        Some(version_num) => version_num,
        None => derived_version_num,
    };
    pg_config.require_auth = options.pg_require_auth;
    pg_config.tls_cert = options.pg_tls_cert;
    pg_config.tls_key = options.pg_tls_key;
    pg_config.require_tls = options.pg_require_tls;
    let pg_server = PgWireServer::open_with_shared(&database_path, pg_config, db)?;
    let pg_for_thread = pg_server.clone();
    let pg_task = std::thread::spawn(move || {
        bicdb_pgwire::serve_existing_listener(pg_for_thread, pg_listener)
    });

    let async_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let shutdown_tx = Mutex::new(Some(shutdown_tx));
    ctrlc::set_handler(move || {
        if let Some(sender) = shutdown_tx.lock().expect("shutdown sender poisoned").take() {
            let _ = sender.send(());
        }
    })?;
    // Keep the final application-runtime owner outside Tokio's async context.
    // Some host providers own blocking runtimes and must be dropped only after
    // this runtime has left `block_on`.
    let application_runtime = Arc::clone(&bundle.runtime);
    async_runtime.block_on(async move {
        let authenticator = Arc::new(authenticator);
        let address = format!("{}:{}", options.http_host, options.http_port)
            .parse::<std::net::SocketAddr>()
            .map_err(|error| {
                bicdb_app_runtime::AppRuntimeError::Provider(format!(
                    "HTTP host must be an IP address: {error}"
                ))
            })?;
        let (http, scheme) = match (options.http_tls_cert, options.http_tls_key) {
            (Some(certificate), Some(private_key)) => (
                application_runtime
                    .serve_http_tls(address, certificate, private_key, authenticator, policy)
                    .await?,
                "https",
            ),
            (None, None) if address.ip().is_loopback() => {
                let listener = tokio::net::TcpListener::bind(address).await?;
                (
                    application_runtime
                        .serve_http(listener, authenticator, policy)
                        .await?,
                    "http",
                )
            }
            (None, None) => {
                return Err(bicdb_app_runtime::AppRuntimeError::Provider(
                    "non-loopback application HTTP requires --http-tls-cert and --http-tls-key"
                        .to_string(),
                ));
            }
            _ => {
                return Err(bicdb_app_runtime::AppRuntimeError::Provider(
                    "HTTP TLS certificate and key must be configured together".to_string(),
                ));
            }
        };
        println!(
            "BicDB application HTTP listening on {scheme}://{}; pgwire listening on {}",
            http.address, pg_address,
        );
        let _ = shutdown_rx.await;
        pg_server.request_shutdown();
        http.shutdown().await
    })?;
    pg_task
        .join()
        .map_err(|_| anyhow::anyhow!("pgwire host thread panicked"))??;
    Ok(())
}

fn load_authenticator(options: &ServeOptions) -> Result<JwtAuthenticator> {
    if let Some(path) = options.auth_config.as_ref() {
        if options.jwt_hs256_key.is_some()
            || options.oidc_jwks.is_some()
            || options.jwt_issuer.is_some()
            || options.jwt_audience.is_some()
        {
            bail!("--auth-config cannot be combined with legacy JWT/OIDC verifier arguments");
        }
        let document: AuthConfigDocument = serde_json::from_slice(&fs::read(path)?)?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        let mut authenticators = Vec::with_capacity(document.schemes.len());
        for scheme in document.schemes {
            let authenticator = match scheme {
                AuthVerifierConfig::JwtHs256 {
                    issuer,
                    audience,
                    key_file,
                    keys,
                } => match (key_file, keys.as_slice()) {
                    (Some(key_file), []) => JwtAuthenticator::hs256(
                        verifier_configuration(issuer, audience, "jwt-hs256"),
                        fs::read(resolve_auth_path(base, key_file))?,
                    )?,
                    (None, keys) if !keys.is_empty() => JwtAuthenticator::hs256_rotating(
                        verifier_configuration(issuer, audience, "jwt-hs256"),
                        keys.iter()
                            .map(|key| {
                                Ok((
                                    key.key_id.clone(),
                                    fs::read(resolve_auth_path(base, key.key_file.clone()))?,
                                ))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )?,
                    _ => bail!(
                        "jwt_hs256 auth config requires exactly one of key_file or non-empty keys"
                    ),
                },
                AuthVerifierConfig::OidcEd25519 {
                    issuer,
                    audience,
                    jwks_file,
                } => JwtAuthenticator::oidc_ed25519(
                    verifier_configuration(issuer, audience, "oidc"),
                    &fs::read(resolve_auth_path(base, jwks_file))?,
                )?,
                AuthVerifierConfig::OidcRs256 {
                    issuer,
                    audience,
                    jwks_file,
                } => JwtAuthenticator::oidc_rs256(
                    verifier_configuration(issuer, audience, "oidc"),
                    &fs::read(resolve_auth_path(base, jwks_file))?,
                )?,
            };
            authenticators.push(authenticator);
        }
        return JwtAuthenticator::multiple(authenticators).map_err(Into::into);
    }

    let issuer = options
        .jwt_issuer
        .clone()
        .context("serve requires --jwt-issuer with a legacy verifier")?;
    let audience = options
        .jwt_audience
        .clone()
        .context("serve requires --jwt-audience with a legacy verifier")?;
    match (&options.jwt_hs256_key, &options.oidc_jwks) {
        (Some(key), None) => JwtAuthenticator::hs256(
            verifier_configuration(issuer, audience, "jwt-hs256"),
            fs::read(key)?,
        )
        .map_err(Into::into),
        (None, Some(jwks)) => JwtAuthenticator::oidc_ed25519(
            verifier_configuration(issuer, audience, "oidc"),
            &fs::read(jwks)?,
        )
        .map_err(Into::into),
        _ => bail!("serve requires --auth-config or exactly one of --jwt-hs256-key/--oidc-jwks"),
    }
}

fn verifier_configuration(
    issuer: String,
    audience: String,
    authentication_method: &str,
) -> JwtConfiguration {
    JwtConfiguration {
        issuer,
        audience,
        authentication_method: authentication_method.to_string(),
        // 24h default; deployments issuing longer-lived service tokens
        // raise it explicitly.
        maximum_lifetime_seconds: std::env::var("BICDB_JWT_MAX_LIFETIME_SECONDS")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(86_400),
        clock_skew_seconds: 30,
    }
}

fn resolve_auth_path(base: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}

fn read_package(path: &Path, runtime: &ApplicationRuntime) -> Result<ApplicationPackage> {
    runtime
        .read_package_file(path)
        .with_context(|| format!("read bounded package {}", path.display()))
}

fn decode_public_key(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() == 32 {
        return Ok(bytes.to_vec());
    }
    let value = std::str::from_utf8(bytes)?.trim();
    if let Ok(decoded) = hex::decode(value) {
        if decoded.len() == 32 {
            return Ok(decoded);
        }
    }
    if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(value) {
        if decoded.len() == 32 {
            return Ok(decoded);
        }
    }
    bail!("trusted Ed25519 public key must be 32 raw, hex, or base64 bytes")
}

fn split_once<'a>(value: &'a str, label: &str, syntax: &str) -> Result<(&'a str, &'a str)> {
    let Some((left, right)) = value.split_once('=') else {
        bail!("{label} must use {syntax}");
    };
    if left.is_empty() || right.is_empty() {
        bail!("{label} must use {syntax}");
    }
    Ok((left, right))
}

fn load_or_create_blob_key(package_root: &Path) -> Result<Vec<u8>> {
    let path = package_root.join("host-blob-signing.key");
    match fs::read(&path) {
        Ok(key) if key.len() == 32 => return Ok(key),
        Ok(_) => bail!("{} does not contain a 32-byte host key", path.display()),
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
        Err(_) => {}
    }
    fs::create_dir_all(package_root)?;
    let mut key = vec![0_u8; 32];
    File::open("/dev/urandom")?.read_exact(&mut key)?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(&path) {
        Ok(mut file) => {
            file.write_all(&key)?;
            file.sync_all()?;
            Ok(key)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let key = fs::read(&path)?;
            if key.len() != 32 {
                bail!("{} does not contain a 32-byte host key", path.display());
            }
            Ok(key)
        }
        Err(error) => Err(error.into()),
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("diagnostic output has no parent"))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".bicdb-diagnostics-{}.tmp", std::process::id()));
    let mut file = File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn host_node_id() -> String {
    std::env::var("BICDB_NODE_ID").unwrap_or_else(|_| format!("bicdb-{}", std::process::id()))
}

fn print_json(value: &impl serde::Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_provider_document_is_exact_and_defaults_are_bounded() {
        let document: BlobProviderConfigDocument = serde_json::from_value(serde_json::json!({
            "kind": "s3_compatible",
            "endpoint": "https://objects.example.test",
            "bucket": "carrier-blobs",
            "region": "us-west-2",
            "access_key_id_file": "access.key",
            "secret_access_key_file": "secret.key"
        }))
        .unwrap();
        match document {
            BlobProviderConfigDocument::S3Compatible {
                key_prefix,
                max_object_bytes,
                connect_timeout_ms,
                request_timeout_ms,
                ..
            } => {
                assert!(key_prefix.is_empty());
                assert_eq!(max_object_bytes, 256 * 1024 * 1024);
                assert_eq!(connect_timeout_ms, 5_000);
                assert_eq!(request_timeout_ms, 60_000);
            }
            BlobProviderConfigDocument::Local { .. } => panic!("wrong provider kind"),
        }
        assert!(
            serde_json::from_value::<BlobProviderConfigDocument>(serde_json::json!({
                "kind": "local",
                "unexpected": true
            }))
            .is_err()
        );
    }

    #[test]
    fn integration_provider_document_is_exact_and_defaults_are_bounded() {
        let document: IntegrationProviderConfigDocument =
            serde_json::from_value(serde_json::json!({
                "version": 1,
                "observability": {
                    "jsonl_path": "observability/events.jsonl",
                    "otlp": {
                        "endpoint": "https://collector.example.test",
                        "protocol": "otlp_grpc",
                        "header_files": {"authorization": "otel.token"}
                    }
                },
                "http": [{
                    "application": "clinic",
                    "provider": "mail",
                    "base_url": "https://api.example.test/v1",
                    "healthcheck_path": "/health"
                }],
                "redis": [{
                    "application": "clinic",
                    "provider": "default",
                    "endpoint": "rediss://cache.example.test:6380"
                }],
                "email": [{
                    "application": "clinic",
                    "provider": "default",
                    "endpoint": "smtps://smtp.example.test:465",
                    "allowed_from": ["care@example.test"]
                }],
                "grpc": [{
                    "application": "clinic",
                    "provider": "InventoryGrpc",
                    "endpoint": "https://inventory.example.test",
                    "allowed_methods": ["/carrier.inventory.v1.Inventory/Reserve"]
                }]
            }))
            .unwrap();
        assert_eq!(document.version, 1);
        let observability = document.observability.unwrap();
        assert_eq!(observability.capacity, 100_000);
        let otlp = observability.otlp.unwrap();
        assert_eq!(otlp.protocol, ApplicationTelemetryProtocolV1::OtlpGrpc);
        assert_eq!(otlp.timeout_ms, 10_000);
        assert_eq!(otlp.queue_capacity, 100_000);
        assert_eq!(otlp.export_interval_ms, 1_000);
        assert_eq!(document.http[0].healthcheck_statuses, vec![200]);
        assert_eq!(document.redis[0].pool_size, 16);
        assert_eq!(document.redis[0].key_prefix, "carrier-cache");
        assert_eq!(document.email[0].max_recipients, 100);
        assert_eq!(document.email[0].max_message_bytes, 4 * 1024 * 1024);
        assert_eq!(document.grpc[0].max_request_bytes, 4 * 1024 * 1024);
        assert_eq!(document.grpc[0].request_timeout_ms, 30_000);
        assert!(serde_json::from_value::<IntegrationProviderConfigDocument>(
            serde_json::json!({"version": 1, "unexpected": true})
        )
        .is_err());
    }

    #[cfg(unix)]
    #[test]
    fn provider_credentials_must_be_private_single_line_files() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credential");
        fs::write(&path, "provider-secret").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            read_provider_credential(&path, "test credential").unwrap(),
            "provider-secret"
        );

        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(read_provider_credential(&path, "test credential").is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&path, "first\nsecond").unwrap();
        assert!(read_provider_credential(&path, "test credential").is_err());
        assert_eq!(
            read_provider_text_secret(&path, "test prompt").unwrap(),
            "first\nsecond"
        );
    }
}
