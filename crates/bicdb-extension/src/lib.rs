//! BicDB's versioned extension contract.
//!
//! Extension authors compile against the manifest types and [`BicDbExtension`]
//! registrar in this crate. Native BicDB hosts enable the `host` feature to
//! load WebAssembly modules through [`host::WasmExtension`]. Browser builds can
//! disable default features and retain only the portable contract types.
//!
//! The public binary boundary is deliberately not a Rust trait object: Rust
//! does not promise a stable trait ABI. The trait in this crate is an authoring
//! convenience that produces a versioned, serialized [`ExtensionManifest`].
//! Dynamic modules cross a small JSON-over-WebAssembly ABI instead.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

pub mod abi_v2;
#[cfg(feature = "host")]
pub mod host;
#[cfg(feature = "http")]
pub mod http;

/// First public BicDB extension ABI.
pub const EXTENSION_ABI_VERSION: u32 = 1;
/// Capability-mediated BicDB application ABI.
pub const EXTENSION_ABI_V2: u32 = abi_v2::APPLICATION_ABI_VERSION;
/// Highest ABI this crate can validate. ABI v1 remains the default for
/// manifests that omit `abi_version`.
pub const LATEST_EXTENSION_ABI_VERSION: u32 = EXTENSION_ABI_V2;

/// Hard limit for a serialized manifest accepted by a host.
// A BicDB application application package's extension manifest scales with the
// application: a real business app (77 models, ~60 routes, signed per-column
// authority for every declared statement) produces a manifest of a few
// megabytes. 16 MiB keeps a hard ceiling against runaway inputs while not
// rejecting legitimate applications at a 1 MiB guess.
pub const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;

/// Errors shared by manifest authors and hosts.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ExtensionError {
    #[error("invalid extension manifest: {0}")]
    InvalidManifest(String),
    #[error("extension ABI {actual} is not supported; host expects {expected}")]
    UnsupportedAbi { expected: u32, actual: u32 },
    #[error("extension capability `{0}` was not declared")]
    MissingCapability(&'static str),
    #[error("extension runtime error: {0}")]
    Runtime(String),
    #[error("extension resource limit exceeded: {0}")]
    ResourceLimit(String),
    #[error("extension payload is invalid: {0}")]
    InvalidPayload(String),
}

pub type Result<T> = std::result::Result<T, ExtensionError>;

/// Stable identity and compatibility information for one extension release.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtensionIdentity {
    pub name: String,
    pub version: String,
    #[serde(default = "current_abi_version")]
    pub abi_version: u32,
    #[serde(default)]
    pub description: String,
}

fn current_abi_version() -> u32 {
    EXTENSION_ABI_VERSION
}

/// Capabilities an extension can register.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionCapability {
    Functions,
    Indexes,
    Storage,
    HttpRoutes,
    DatabaseEvents,
    QueueEvents,
    Database,
    Transactions,
    PluginServices,
    Clock,
    Random,
    SecretsCrypto,
    NetworkEgress,
    Blobs,
    Streaming,
    Jobs,
    Schedules,
    AiInference,
    Observability,
}

impl ExtensionCapability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Functions => "functions",
            Self::Indexes => "indexes",
            Self::Storage => "storage",
            Self::HttpRoutes => "http_routes",
            Self::DatabaseEvents => "database_events",
            Self::QueueEvents => "queue_events",
            Self::Database => "database",
            Self::Transactions => "transactions",
            Self::PluginServices => "plugin_services",
            Self::Clock => "clock",
            Self::Random => "random",
            Self::SecretsCrypto => "secrets_crypto",
            Self::NetworkEgress => "network_egress",
            Self::Blobs => "blobs",
            Self::Streaming => "streaming",
            Self::Jobs => "jobs",
            Self::Schedules => "schedules",
            Self::AiInference => "ai_inference",
            Self::Observability => "observability",
        }
    }
}

/// Permissions requested by a module.
///
/// Declaring a capability does not grant authority. Hosts intersect these
/// requests with their operator policy before activation.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtensionPermissions {
    #[serde(default)]
    pub read_relations: BTreeSet<String>,
    #[serde(default)]
    pub write_relations: BTreeSet<String>,
    #[serde(default)]
    pub publish_queues: BTreeSet<String>,
    #[serde(default)]
    pub consume_queues: BTreeSet<String>,
    #[serde(default)]
    pub network_hosts: BTreeSet<String>,
}

/// Per-invocation bounds requested by the package and capped by the host.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtensionLimits {
    pub memory_bytes: u64,
    pub fuel: u64,
    pub timeout_ms: u64,
    pub max_input_bytes: u64,
    pub max_output_bytes: u64,
    pub max_concurrency: u32,
}

impl Default for ExtensionLimits {
    fn default() -> Self {
        Self {
            memory_bytes: 64 * 1024 * 1024,
            fuel: 10_000_000,
            timeout_ms: 10_000,
            max_input_bytes: 1024 * 1024,
            max_output_bytes: 4 * 1024 * 1024,
            max_concurrency: 4,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FunctionRegistration {
    pub name: String,
    pub export: String,
    #[serde(default)]
    pub arguments: Vec<String>,
    pub returns: String,
    #[serde(default)]
    pub volatility: FunctionVolatility,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FunctionVolatility {
    Immutable,
    Stable,
    #[default]
    Volatile,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexRegistration {
    pub name: String,
    pub access_method: String,
    pub export: String,
    #[serde(default)]
    pub supported_types: Vec<String>,
    pub format_version: u32,
}

/// Reserved contract for trusted native storage engines.
///
/// WebAssembly hosts must not activate storage registrations. Storage changes
/// durability, replication, backup, and recovery and therefore requires a
/// separately trusted native host policy.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageRegistration {
    pub name: String,
    pub export: String,
    pub format_version: u32,
    #[serde(default)]
    pub requires_native_trust: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
    Options,
}

impl fmt::Display for HttpMethod {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
            Self::Head => "HEAD",
            Self::Options => "OPTIONS",
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RouteAuth {
    Public,
    Authenticated,
    #[default]
    RowLevelSecurity,
    Admin,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HttpRouteRegistration {
    pub name: String,
    pub method: HttpMethod,
    pub path: String,
    pub export: String,
    #[serde(default)]
    pub auth: RouteAuth,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventSource {
    Database {
        relation: String,
        operations: BTreeSet<DatabaseOperation>,
    },
    Queue {
        queue: String,
        group: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventSubscriptionRegistration {
    pub name: String,
    pub source: EventSource,
    pub export: String,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_visibility_timeout_ms")]
    pub visibility_timeout_ms: u64,
}

fn default_max_attempts() -> u32 {
    5
}

fn default_visibility_timeout_ms() -> u64 {
    30_000
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObservabilityRegistration {
    pub name: String,
    pub export: String,
    #[serde(default)]
    pub description: String,
}

/// One extension required by another extension.
///
/// Resolution is deliberately catalog-only. A manifest can describe what it
/// needs, but it cannot make the host download code or choose an untrusted
/// package source.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionDependency {
    pub name: String,
    #[serde(default = "any_version")]
    pub version: String,
    #[serde(default = "current_abi_version")]
    pub abi_version: u32,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub capabilities: BTreeSet<ExtensionCapability>,
    #[serde(default)]
    pub module_sha256: Option<String>,
}

fn any_version() -> String {
    "*".to_string()
}

impl ExtensionDependency {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("extension dependency", &self.name)?;
        semver::VersionReq::parse(&self.version).map_err(|error| {
            ExtensionError::InvalidManifest(format!(
                "dependency `{}` has invalid semantic version requirement `{}`: {error}",
                self.name, self.version
            ))
        })?;
        if self.abi_version == 0 {
            return Err(ExtensionError::InvalidManifest(format!(
                "dependency `{}` ABI version must be positive",
                self.name
            )));
        }
        if let Some(module_sha256) = &self.module_sha256 {
            validate_sha256(module_sha256)?;
        }
        Ok(())
    }

    pub fn matches(&self, installation: &ExtensionInstallation) -> Result<()> {
        self.validate()?;
        let identity = &installation.manifest.identity;
        let installed_version = semver::Version::parse(&identity.version).map_err(|error| {
            ExtensionError::InvalidManifest(format!(
                "installed dependency `{}` has invalid semantic version `{}`: {error}",
                identity.name, identity.version
            ))
        })?;
        let requirement = semver::VersionReq::parse(&self.version).map_err(|error| {
            ExtensionError::InvalidManifest(format!(
                "dependency `{}` has invalid semantic version requirement `{}`: {error}",
                self.name, self.version
            ))
        })?;
        if !requirement.matches(&installed_version) {
            return Err(ExtensionError::InvalidManifest(format!(
                "extension dependency `{}` requires version `{}`, but `{}` is installed",
                self.name, self.version, identity.version
            )));
        }
        if identity.abi_version != self.abi_version {
            return Err(ExtensionError::InvalidManifest(format!(
                "extension dependency `{}` requires ABI {}, but ABI {} is installed",
                self.name, self.abi_version, identity.abi_version
            )));
        }
        if !self
            .capabilities
            .is_subset(&installation.manifest.capabilities)
        {
            let missing = self
                .capabilities
                .difference(&installation.manifest.capabilities)
                .map(|capability| capability.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(ExtensionError::InvalidManifest(format!(
                "extension dependency `{}` is missing capabilities: {missing}",
                self.name
            )));
        }
        if self
            .module_sha256
            .as_ref()
            .is_some_and(|expected| expected != &installation.module_sha256)
        {
            return Err(ExtensionError::InvalidManifest(format!(
                "extension dependency `{}` does not match its pinned module SHA-256",
                self.name
            )));
        }
        Ok(())
    }
}

/// Complete declarative registration emitted by an extension.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionManifest {
    pub identity: ExtensionIdentity,
    #[serde(default)]
    pub dependencies: Vec<ExtensionDependency>,
    #[serde(default)]
    pub capabilities: BTreeSet<ExtensionCapability>,
    #[serde(default)]
    pub permissions: ExtensionPermissions,
    #[serde(default)]
    pub limits: ExtensionLimits,
    #[serde(default)]
    pub functions: Vec<FunctionRegistration>,
    #[serde(default)]
    pub indexes: Vec<IndexRegistration>,
    #[serde(default)]
    pub storage: Vec<StorageRegistration>,
    #[serde(default)]
    pub routes: Vec<HttpRouteRegistration>,
    #[serde(default)]
    pub subscriptions: Vec<EventSubscriptionRegistration>,
    #[serde(default)]
    pub observability: Vec<ObservabilityRegistration>,
    /// ABI-v2 application contract. Absent for ABI v1 packages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application: Option<Box<abi_v2::ApplicationManifestV2>>,
}

/// Durable lifecycle state recorded by BicDB's extension catalog.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionState {
    Staged,
    Active,
    Disabled,
    Failed,
}

/// Cluster activation proof. A single-node database uses one ready node and a
/// zero topology generation; clustered activation is published only after the
/// metadata quorum has committed the ready-node set.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtensionActivation {
    pub catalog_generation: u64,
    pub topology_generation: u64,
    pub ready_nodes: BTreeSet<String>,
    pub required_nodes: BTreeSet<String>,
    pub quorum_committed: bool,
    pub activated_at_ms: i64,
}

impl ExtensionActivation {
    pub fn validate(&self) -> Result<()> {
        if self.catalog_generation == 0 {
            return Err(ExtensionError::InvalidManifest(
                "extension activation generation must be positive".to_string(),
            ));
        }
        if self.required_nodes.is_empty() || !self.required_nodes.is_subset(&self.ready_nodes) {
            return Err(ExtensionError::InvalidManifest(
                "every required extension node must report the package ready".to_string(),
            ));
        }
        if self.topology_generation > 0 && !self.quorum_committed {
            return Err(ExtensionError::InvalidManifest(
                "cluster extension activation must be quorum committed".to_string(),
            ));
        }
        Ok(())
    }
}

/// Durable installation record. Module bytes live in the content-addressed
/// package store; the catalog stores their expected SHA-256 and manifest.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtensionInstallation {
    pub manifest: ExtensionManifest,
    pub module_sha256: String,
    pub state: ExtensionState,
    pub installed_at_ms: i64,
    #[serde(default)]
    pub activation: Option<ExtensionActivation>,
    #[serde(default)]
    pub last_error: Option<String>,
}

impl ExtensionInstallation {
    pub fn validate(&self) -> Result<()> {
        self.manifest.validate()?;
        validate_sha256(&self.module_sha256)?;
        match (self.state, &self.activation) {
            (ExtensionState::Active, Some(activation)) => activation.validate(),
            (ExtensionState::Active, None) => Err(ExtensionError::InvalidManifest(
                "active extension is missing an activation proof".to_string(),
            )),
            (_, Some(activation)) => activation.validate(),
            (_, None) => Ok(()),
        }
    }
}

/// Resolve a dependency graph in dependency-first order.
///
/// With `require_active`, required dependencies must be active. Optional
/// dependencies that are absent or inactive are ignored. Without it, every
/// installed optional dependency participates in activation just like a
/// required dependency. The returned names are normalized to lowercase.
pub fn resolve_extension_order(
    installations: &[ExtensionInstallation],
    roots: &[String],
    require_active: bool,
) -> Result<Vec<String>> {
    let mut by_name = BTreeMap::new();
    for installation in installations {
        installation.validate()?;
        let name = installation.manifest.identity.name.to_ascii_lowercase();
        if by_name.insert(name.clone(), installation).is_some() {
            return Err(ExtensionError::InvalidManifest(format!(
                "duplicate installed extension `{name}`"
            )));
        }
    }

    fn visit(
        name: &str,
        by_name: &BTreeMap<String, &ExtensionInstallation>,
        require_active: bool,
        visiting: &mut Vec<String>,
        visited: &mut BTreeSet<String>,
        order: &mut Vec<String>,
    ) -> Result<()> {
        let name = name.to_ascii_lowercase();
        if visited.contains(&name) {
            return Ok(());
        }
        if let Some(start) = visiting.iter().position(|candidate| candidate == &name) {
            let mut cycle = visiting[start..].to_vec();
            cycle.push(name);
            return Err(ExtensionError::InvalidManifest(format!(
                "extension dependency cycle: {}",
                cycle.join(" -> ")
            )));
        }
        let installation = by_name.get(&name).copied().ok_or_else(|| {
            ExtensionError::InvalidManifest(format!(
                "required extension dependency `{name}` is not installed"
            ))
        })?;
        if require_active && installation.state != ExtensionState::Active {
            return Err(ExtensionError::InvalidManifest(format!(
                "required extension dependency `{name}` is not active"
            )));
        }

        visiting.push(name.clone());
        for dependency in &installation.manifest.dependencies {
            let dependency_name = dependency.name.to_ascii_lowercase();
            let Some(dependency_installation) = by_name.get(&dependency_name).copied() else {
                if dependency.optional {
                    continue;
                }
                return Err(ExtensionError::InvalidManifest(format!(
                    "extension `{name}` requires missing dependency `{dependency_name}`"
                )));
            };
            if require_active
                && dependency.optional
                && dependency_installation.state != ExtensionState::Active
            {
                continue;
            }
            dependency.matches(dependency_installation)?;
            visit(
                &dependency_name,
                by_name,
                require_active,
                visiting,
                visited,
                order,
            )?;
        }
        visiting.pop();
        visited.insert(name.clone());
        order.push(name);
        Ok(())
    }

    let mut visiting = Vec::new();
    let mut visited = BTreeSet::new();
    let mut order = Vec::new();
    for root in roots {
        visit(
            root,
            &by_name,
            require_active,
            &mut visiting,
            &mut visited,
            &mut order,
        )?;
    }
    Ok(order)
}

/// Dynamic REST resource owned by an extension such as `instant_rest`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestResourceDefinition {
    pub name: String,
    pub extension: String,
    pub relation: String,
    pub path: String,
    pub export: String,
    pub methods: BTreeSet<HttpMethod>,
    #[serde(default)]
    pub auth: RouteAuth,
    #[serde(default)]
    pub openapi: bool,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl RestResourceDefinition {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("resource", &self.name)?;
        validate_identifier("extension", &self.extension)?;
        validate_qualified_name("relation", &self.relation)?;
        validate_route_path(&self.path)?;
        validate_export(&self.export)?;
        if self.methods.is_empty() {
            return Err(ExtensionError::InvalidManifest(format!(
                "resource `{}` has no HTTP methods",
                self.name
            )));
        }
        Ok(())
    }
}

/// Durable host-owned website route.
///
/// The active and previous version fields are catalog pointers. Website
/// releases are immutable records, so switching either pointer is an atomic
/// publish/rollback operation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WebsiteDefinition {
    pub name: String,
    pub extension: String,
    #[serde(default)]
    pub host: Option<String>,
    pub mount_path: String,
    pub export: String,
    #[serde(default)]
    pub auth: RouteAuth,
    #[serde(default)]
    pub active_version: Option<String>,
    #[serde(default)]
    pub previous_version: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl WebsiteDefinition {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("website", &self.name)?;
        validate_identifier("extension", &self.extension)?;
        validate_website_mount(&self.mount_path)?;
        validate_export(&self.export)?;
        if let Some(host) = &self.host {
            validate_hostname(host)?;
        }
        for version in [&self.active_version, &self.previous_version]
            .into_iter()
            .flatten()
        {
            validate_version(version)?;
        }
        if self.active_version == self.previous_version && self.active_version.is_some() {
            return Err(ExtensionError::InvalidManifest(format!(
                "website `{}` active and previous versions must differ",
                self.name
            )));
        }
        Ok(())
    }
}

/// One immutable website content bundle.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WebsiteRelease {
    pub website: String,
    pub version: String,
    pub content_sha256: String,
    pub content: JsonValue,
    pub created_at_ms: i64,
}

impl WebsiteRelease {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("website", &self.website)?;
        validate_version(&self.version)?;
        validate_sha256(&self.content_sha256)?;
        if !self.content.is_object() {
            return Err(ExtensionError::InvalidManifest(format!(
                "website `{}` release content must be a JSON object",
                self.website
            )));
        }
        Ok(())
    }
}

/// Fully resolved website deployment supplied to a runtime snapshot.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ActiveWebsiteDeployment {
    pub definition: WebsiteDefinition,
    pub release: WebsiteRelease,
}

impl ActiveWebsiteDeployment {
    pub fn validate(&self) -> Result<()> {
        self.definition.validate()?;
        self.release.validate()?;
        if !self
            .definition
            .name
            .eq_ignore_ascii_case(&self.release.website)
        {
            return Err(ExtensionError::InvalidManifest(format!(
                "website `{}` received release for `{}`",
                self.definition.name, self.release.website
            )));
        }
        if self.definition.active_version.as_deref() != Some(self.release.version.as_str()) {
            return Err(ExtensionError::InvalidManifest(format!(
                "website `{}` active version does not match release `{}`",
                self.definition.name, self.release.version
            )));
        }
        Ok(())
    }
}

/// Dynamic database/queue event binding owned by an extension.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventBindingDefinition {
    pub name: String,
    pub extension: String,
    pub source: EventSource,
    /// Durable queue used for database-origin events. Queue-origin bindings use
    /// the queue contained in `source`.
    #[serde(default)]
    pub delivery_queue: Option<String>,
    pub export: String,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_visibility_timeout_ms")]
    pub visibility_timeout_ms: u64,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl EventBindingDefinition {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("event binding", &self.name)?;
        validate_identifier("extension", &self.extension)?;
        validate_export(&self.export)?;
        if self.max_attempts == 0 || self.visibility_timeout_ms == 0 {
            return Err(ExtensionError::InvalidManifest(
                "event delivery limits must be positive".to_string(),
            ));
        }
        match &self.source {
            EventSource::Database {
                relation,
                operations,
            } => {
                validate_qualified_name("relation", relation)?;
                if operations.is_empty() {
                    return Err(ExtensionError::InvalidManifest(
                        "database event binding requires at least one operation".to_string(),
                    ));
                }
                let queue = self.delivery_queue.as_deref().ok_or_else(|| {
                    ExtensionError::InvalidManifest(
                        "database event binding requires a delivery queue".to_string(),
                    )
                })?;
                validate_identifier("queue", queue)
            }
            EventSource::Queue { queue, group } => {
                if self.delivery_queue.is_some() {
                    return Err(ExtensionError::InvalidManifest(
                        "queue event binding must not declare a second delivery queue".to_string(),
                    ));
                }
                validate_identifier("queue", queue)?;
                validate_identifier("group", group)
            }
        }
    }
}

impl ExtensionManifest {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("extension", &self.identity.name)?;
        validate_version(&self.identity.version)?;
        if !matches!(
            self.identity.abi_version,
            EXTENSION_ABI_VERSION | EXTENSION_ABI_V2
        ) {
            return Err(ExtensionError::UnsupportedAbi {
                expected: LATEST_EXTENSION_ABI_VERSION,
                actual: self.identity.abi_version,
            });
        }
        match (self.identity.abi_version, self.application.as_deref()) {
            (EXTENSION_ABI_VERSION, None) => {}
            (EXTENSION_ABI_VERSION, Some(_)) => {
                return Err(ExtensionError::InvalidManifest(
                    "ABI v1 extension cannot contain an ABI v2 application manifest".to_string(),
                ))
            }
            (EXTENSION_ABI_V2, Some(application)) => application.validate(self)?,
            (EXTENSION_ABI_V2, None) => {
                return Err(ExtensionError::InvalidManifest(
                    "ABI v2 extension requires an application manifest".to_string(),
                ))
            }
            _ => unreachable!("supported ABI versions handled above"),
        }
        validate_limits(&self.limits)?;
        let mut dependency_names = BTreeSet::new();
        for dependency in &self.dependencies {
            dependency.validate()?;
            if dependency.name.eq_ignore_ascii_case(&self.identity.name) {
                return Err(ExtensionError::InvalidManifest(format!(
                    "extension `{}` cannot depend on itself",
                    self.identity.name
                )));
            }
            if !dependency_names.insert(dependency.name.to_ascii_lowercase()) {
                return Err(ExtensionError::InvalidManifest(format!(
                    "duplicate extension dependency `{}`",
                    dependency.name
                )));
            }
        }
        require_capability(
            &self.functions,
            &self.capabilities,
            ExtensionCapability::Functions,
        )?;
        require_capability(
            &self.indexes,
            &self.capabilities,
            ExtensionCapability::Indexes,
        )?;
        require_capability(
            &self.storage,
            &self.capabilities,
            ExtensionCapability::Storage,
        )?;
        require_capability(
            &self.routes,
            &self.capabilities,
            ExtensionCapability::HttpRoutes,
        )?;
        require_capability(
            &self.observability,
            &self.capabilities,
            ExtensionCapability::Observability,
        )?;

        let mut names = BTreeSet::new();
        for (kind, name) in self.registration_names() {
            validate_identifier(kind, name)?;
            if !names.insert((kind, name.to_ascii_lowercase())) {
                return Err(ExtensionError::InvalidManifest(format!(
                    "duplicate {kind} registration `{name}`"
                )));
            }
        }
        let mut routes = BTreeSet::new();
        for route in &self.routes {
            validate_route_path(&route.path)?;
            validate_export(&route.export)?;
            if !routes.insert((route.method, route.path.clone())) {
                return Err(ExtensionError::InvalidManifest(format!(
                    "duplicate route {} {}",
                    route.method, route.path
                )));
            }
        }
        for function in &self.functions {
            validate_export(&function.export)?;
        }
        for index in &self.indexes {
            validate_export(&index.export)?;
            if index.format_version == 0 {
                return Err(ExtensionError::InvalidManifest(format!(
                    "index `{}` format_version must be positive",
                    index.name
                )));
            }
        }
        for storage in &self.storage {
            validate_export(&storage.export)?;
            if !storage.requires_native_trust {
                return Err(ExtensionError::InvalidManifest(format!(
                    "storage `{}` must declare requires_native_trust",
                    storage.name
                )));
            }
            if storage.format_version == 0 {
                return Err(ExtensionError::InvalidManifest(format!(
                    "storage `{}` format_version must be positive",
                    storage.name
                )));
            }
        }
        for subscription in &self.subscriptions {
            validate_export(&subscription.export)?;
            if subscription.max_attempts == 0 || subscription.visibility_timeout_ms == 0 {
                return Err(ExtensionError::InvalidManifest(format!(
                    "subscription `{}` requires positive delivery limits",
                    subscription.name
                )));
            }
            match &subscription.source {
                EventSource::Database {
                    relation,
                    operations,
                } => {
                    require_declared(&self.capabilities, ExtensionCapability::DatabaseEvents)?;
                    validate_qualified_name("relation", relation)?;
                    if operations.is_empty() {
                        return Err(ExtensionError::InvalidManifest(format!(
                            "subscription `{}` has no database operations",
                            subscription.name
                        )));
                    }
                }
                EventSource::Queue { queue, group } => {
                    require_declared(&self.capabilities, ExtensionCapability::QueueEvents)?;
                    validate_identifier("queue", queue)?;
                    validate_identifier("group", group)?;
                }
            }
        }
        if !self.permissions.network_hosts.is_empty() {
            require_declared(&self.capabilities, ExtensionCapability::NetworkEgress)?;
        }
        Ok(())
    }

    fn registration_names(&self) -> impl Iterator<Item = (&'static str, &str)> {
        self.functions
            .iter()
            .map(|item| ("function", item.name.as_str()))
            .chain(
                self.indexes
                    .iter()
                    .map(|item| ("index", item.name.as_str())),
            )
            .chain(
                self.storage
                    .iter()
                    .map(|item| ("storage", item.name.as_str())),
            )
            .chain(self.routes.iter().map(|item| ("route", item.name.as_str())))
            .chain(
                self.subscriptions
                    .iter()
                    .map(|item| ("subscription", item.name.as_str())),
            )
            .chain(
                self.observability
                    .iter()
                    .map(|item| ("observability", item.name.as_str())),
            )
    }
}

fn validate_limits(limits: &ExtensionLimits) -> Result<()> {
    if limits.memory_bytes < 64 * 1024 || limits.memory_bytes > 4 * 1024 * 1024 * 1024 {
        return Err(ExtensionError::InvalidManifest(
            "memory_bytes must be between 64 KiB and 4 GiB".to_string(),
        ));
    }
    if limits.fuel == 0
        || limits.timeout_ms == 0
        || limits.max_input_bytes == 0
        || limits.max_output_bytes == 0
        || limits.max_concurrency == 0
    {
        return Err(ExtensionError::InvalidManifest(
            "all execution limits must be positive".to_string(),
        ));
    }
    if limits.max_input_bytes > limits.memory_bytes || limits.max_output_bytes > limits.memory_bytes
    {
        return Err(ExtensionError::InvalidManifest(
            "input and output limits cannot exceed memory_bytes".to_string(),
        ));
    }
    Ok(())
}

fn require_capability<T>(
    registrations: &[T],
    capabilities: &BTreeSet<ExtensionCapability>,
    capability: ExtensionCapability,
) -> Result<()> {
    if !registrations.is_empty() {
        require_declared(capabilities, capability)?;
    }
    Ok(())
}

fn require_declared(
    capabilities: &BTreeSet<ExtensionCapability>,
    capability: ExtensionCapability,
) -> Result<()> {
    if capabilities.contains(&capability) {
        Ok(())
    } else {
        Err(ExtensionError::MissingCapability(capability.as_str()))
    }
}

fn validate_identifier(kind: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 128 {
        return Err(ExtensionError::InvalidManifest(format!(
            "{kind} name must contain 1..=128 bytes"
        )));
    }
    let mut chars = value.chars();
    let valid_first = chars
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_');
    if !valid_first
        || !chars.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
        })
    {
        return Err(ExtensionError::InvalidManifest(format!(
            "invalid {kind} name `{value}`"
        )));
    }
    Ok(())
}

fn validate_qualified_name(kind: &str, value: &str) -> Result<()> {
    for part in value.split('.') {
        validate_identifier(kind, part)?;
    }
    Ok(())
}

fn validate_version(version: &str) -> Result<()> {
    if version.len() > 64 {
        return Err(ExtensionError::InvalidManifest(
            "extension version must contain at most 64 bytes".to_string(),
        ));
    }
    semver::Version::parse(version)
        .map(|_| ())
        .map_err(|error| {
            ExtensionError::InvalidManifest(format!(
                "invalid semantic extension version `{version}`: {error}"
            ))
        })
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ExtensionError::InvalidManifest(
            "module_sha256 must be 64 lowercase hexadecimal characters".to_string(),
        ));
    }
    Ok(())
}

fn validate_export(export: &str) -> Result<()> {
    validate_identifier("WASM export", export)
}

fn validate_route_path(path: &str) -> Result<()> {
    if !path.starts_with('/') || path.len() > 512 || path.contains("..") || path.contains('\0') {
        return Err(ExtensionError::InvalidManifest(format!(
            "invalid HTTP route path `{path}`"
        )));
    }
    Ok(())
}

fn validate_website_mount(path: &str) -> Result<()> {
    validate_route_path(path)?;
    if path.len() > 1 && path.ends_with('/') {
        return Err(ExtensionError::InvalidManifest(format!(
            "website mount path `{path}` must not end in `/`"
        )));
    }
    Ok(())
}

fn validate_hostname(host: &str) -> Result<()> {
    if host.is_empty()
        || host.len() > 253
        || host.starts_with('.')
        || host.ends_with('.')
        || !host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    {
        return Err(ExtensionError::InvalidManifest(format!(
            "invalid website host `{host}`"
        )));
    }
    Ok(())
}

/// Author-facing registrar. Static/native extensions implement
/// [`BicDbExtension`] and populate this value; WASM build tooling serializes
/// the resulting manifest.
#[derive(Clone, Debug)]
pub struct ExtensionRegistrar {
    manifest: ExtensionManifest,
}

impl ExtensionRegistrar {
    pub fn new(identity: ExtensionIdentity) -> Self {
        Self {
            manifest: ExtensionManifest {
                identity,
                dependencies: Vec::new(),
                capabilities: BTreeSet::new(),
                permissions: ExtensionPermissions::default(),
                limits: ExtensionLimits::default(),
                functions: Vec::new(),
                indexes: Vec::new(),
                storage: Vec::new(),
                routes: Vec::new(),
                subscriptions: Vec::new(),
                observability: Vec::new(),
                application: None,
            },
        }
    }

    pub fn declare(&mut self, capability: ExtensionCapability) -> &mut Self {
        self.manifest.capabilities.insert(capability);
        self
    }

    pub fn require_extension(&mut self, dependency: ExtensionDependency) -> &mut Self {
        self.manifest.dependencies.push(dependency);
        self
    }

    pub fn permissions(&mut self, permissions: ExtensionPermissions) -> &mut Self {
        self.manifest.permissions = permissions;
        self
    }

    pub fn limits(&mut self, limits: ExtensionLimits) -> &mut Self {
        self.manifest.limits = limits;
        self
    }

    pub fn register_function(&mut self, registration: FunctionRegistration) -> &mut Self {
        self.manifest.functions.push(registration);
        self
    }

    pub fn register_index(&mut self, registration: IndexRegistration) -> &mut Self {
        self.manifest.indexes.push(registration);
        self
    }

    pub fn register_storage(&mut self, registration: StorageRegistration) -> &mut Self {
        self.manifest.storage.push(registration);
        self
    }

    pub fn register_route(&mut self, registration: HttpRouteRegistration) -> &mut Self {
        self.manifest.routes.push(registration);
        self
    }

    pub fn register_subscription(
        &mut self,
        registration: EventSubscriptionRegistration,
    ) -> &mut Self {
        self.manifest.subscriptions.push(registration);
        self
    }

    pub fn register_observability(&mut self, registration: ObservabilityRegistration) -> &mut Self {
        self.manifest.observability.push(registration);
        self
    }

    pub fn application(&mut self, application: abi_v2::ApplicationManifestV2) -> &mut Self {
        self.manifest.application = Some(Box::new(application));
        self
    }

    pub fn finish(self) -> Result<ExtensionManifest> {
        self.manifest.validate()?;
        Ok(self.manifest)
    }
}

/// Ergonomic static authoring interface.
///
/// This trait is not the dynamic binary ABI. Its result is serialized and
/// crosses the stable ABI described by [`export_bicdb_extension!`].
pub trait BicDbExtension {
    fn identity(&self) -> ExtensionIdentity;

    fn register_dependencies(&self, _registrar: &mut ExtensionRegistrar) -> Result<()> {
        Ok(())
    }

    fn register_functions(&self, _registrar: &mut ExtensionRegistrar) -> Result<()> {
        Ok(())
    }

    fn register_indexes(&self, _registrar: &mut ExtensionRegistrar) -> Result<()> {
        Ok(())
    }

    fn register_storage(&self, _registrar: &mut ExtensionRegistrar) -> Result<()> {
        Ok(())
    }

    fn register_routes(&self, _registrar: &mut ExtensionRegistrar) -> Result<()> {
        Ok(())
    }

    fn register_events(&self, _registrar: &mut ExtensionRegistrar) -> Result<()> {
        Ok(())
    }

    fn register_observability(&self, _registrar: &mut ExtensionRegistrar) -> Result<()> {
        Ok(())
    }

    fn manifest(&self) -> Result<ExtensionManifest> {
        let mut registrar = ExtensionRegistrar::new(self.identity());
        self.register_dependencies(&mut registrar)?;
        self.register_functions(&mut registrar)?;
        self.register_indexes(&mut registrar)?;
        self.register_storage(&mut registrar)?;
        self.register_routes(&mut registrar)?;
        self.register_events(&mut registrar)?;
        self.register_observability(&mut registrar)?;
        registrar.finish()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InvocationKind {
    Function,
    Index,
    Storage,
    HttpRoute,
    DatabaseEvent,
    QueueEvent,
    Observability,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct InvocationContext {
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub tenant: Option<String>,
    #[serde(default)]
    pub deadline_unix_ms: Option<i64>,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExtensionInvocation {
    pub id: String,
    pub kind: InvocationKind,
    pub target: String,
    #[serde(default)]
    pub payload: JsonValue,
    #[serde(default)]
    pub context: InvocationContext,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExtensionInvocationResult {
    #[serde(default = "default_status")]
    pub status: u16,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: JsonValue,
    #[serde(default = "default_true")]
    pub ack: bool,
    #[serde(default)]
    pub retry_after_ms: Option<u64>,
    #[serde(default)]
    pub error: Option<String>,
}

fn default_status() -> u16 {
    200
}

fn default_true() -> bool {
    true
}

/// Export a manifest and JSON invocation handler through ABI v1.
///
/// The module exports no imports by itself. BicDB allocates the invocation in
/// module memory, calls the handler, reads the packed pointer/length response,
/// and then returns both allocations through the module deallocator.
#[macro_export]
macro_rules! export_bicdb_extension {
    ($manifest_json:expr, $handler:path) => {
        #[no_mangle]
        pub extern "C" fn bicdb_extension_abi_version() -> u32 {
            $crate::EXTENSION_ABI_VERSION
        }

        #[no_mangle]
        pub extern "C" fn bicdb_extension_manifest_ptr() -> u32 {
            $manifest_json.as_bytes().as_ptr() as u32
        }

        #[no_mangle]
        pub extern "C" fn bicdb_extension_manifest_len() -> u32 {
            $manifest_json.as_bytes().len() as u32
        }

        #[no_mangle]
        pub extern "C" fn bicdb_extension_alloc(len: u32) -> u32 {
            let mut buffer = Vec::<u8>::with_capacity(len as usize);
            let pointer = buffer.as_mut_ptr();
            std::mem::forget(buffer);
            pointer as u32
        }

        #[no_mangle]
        pub unsafe extern "C" fn bicdb_extension_dealloc(pointer: u32, len: u32) {
            if pointer != 0 {
                let slice = std::ptr::slice_from_raw_parts_mut(pointer as *mut u8, len as usize);
                drop(Box::from_raw(slice));
            }
        }

        #[no_mangle]
        pub unsafe extern "C" fn bicdb_extension_invoke(pointer: u32, len: u32) -> u64 {
            let input = std::slice::from_raw_parts(pointer as *const u8, len as usize);
            // The ABI carries pointer and length, not Vec capacity. Converting
            // to a boxed slice gives the host an exactly-sized allocation that
            // `bicdb_extension_dealloc` can reconstruct without allocator UB.
            let output: Box<[u8]> = $handler(input).into_boxed_slice();
            let output_len = output.len() as u32;
            let output_pointer = output.as_ptr() as u32;
            let _ = Box::into_raw(output);
            ((output_pointer as u64) << 32) | output_len as u64
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Demo;

    impl BicDbExtension for Demo {
        fn identity(&self) -> ExtensionIdentity {
            ExtensionIdentity {
                name: "instant_rest".to_string(),
                version: "1.0.0".to_string(),
                abi_version: EXTENSION_ABI_VERSION,
                description: "REST resources".to_string(),
            }
        }

        fn register_routes(&self, registrar: &mut ExtensionRegistrar) -> Result<()> {
            registrar
                .declare(ExtensionCapability::HttpRoutes)
                .register_route(HttpRouteRegistration {
                    name: "health".to_string(),
                    method: HttpMethod::Get,
                    path: "/health".to_string(),
                    export: "handle_health".to_string(),
                    auth: RouteAuth::Public,
                });
            Ok(())
        }
    }

    #[test]
    fn trait_authoring_builds_a_valid_manifest() {
        let manifest = Demo.manifest().unwrap();
        assert_eq!(manifest.identity.name, "instant_rest");
        assert_eq!(manifest.routes.len(), 1);
        assert!(manifest
            .capabilities
            .contains(&ExtensionCapability::HttpRoutes));
    }

    #[test]
    fn registrations_require_explicit_capabilities() {
        let mut registrar = ExtensionRegistrar::new(Demo.identity());
        registrar.register_route(HttpRouteRegistration {
            name: "health".to_string(),
            method: HttpMethod::Get,
            path: "/health".to_string(),
            export: "handle_health".to_string(),
            auth: RouteAuth::Public,
        });
        assert_eq!(
            registrar.finish().unwrap_err(),
            ExtensionError::MissingCapability("http_routes")
        );
    }

    #[test]
    fn duplicate_route_method_and_path_fail_closed() {
        let mut registrar = ExtensionRegistrar::new(Demo.identity());
        registrar.declare(ExtensionCapability::HttpRoutes);
        for name in ["first", "second"] {
            registrar.register_route(HttpRouteRegistration {
                name: name.to_string(),
                method: HttpMethod::Get,
                path: "/same".to_string(),
                export: format!("handle_{name}"),
                auth: RouteAuth::Public,
            });
        }
        assert!(registrar
            .finish()
            .unwrap_err()
            .to_string()
            .contains("duplicate route GET /same"));
    }

    #[test]
    fn database_subscriptions_require_operations() {
        let mut registrar = ExtensionRegistrar::new(Demo.identity());
        registrar
            .declare(ExtensionCapability::DatabaseEvents)
            .register_subscription(EventSubscriptionRegistration {
                name: "patient_changes".to_string(),
                source: EventSource::Database {
                    relation: "public.patients".to_string(),
                    operations: BTreeSet::new(),
                },
                export: "on_patient_change".to_string(),
                max_attempts: 5,
                visibility_timeout_ms: 30_000,
            });
        assert!(registrar
            .finish()
            .unwrap_err()
            .to_string()
            .contains("has no database operations"));
    }

    fn staged_installation(
        name: &str,
        version: &str,
        dependencies: Vec<ExtensionDependency>,
        capabilities: BTreeSet<ExtensionCapability>,
    ) -> ExtensionInstallation {
        ExtensionInstallation {
            manifest: ExtensionManifest {
                identity: ExtensionIdentity {
                    name: name.to_string(),
                    version: version.to_string(),
                    abi_version: EXTENSION_ABI_VERSION,
                    description: String::new(),
                },
                dependencies,
                capabilities,
                permissions: ExtensionPermissions::default(),
                limits: ExtensionLimits::default(),
                functions: vec![],
                indexes: vec![],
                storage: vec![],
                routes: vec![],
                subscriptions: vec![],
                observability: vec![],
                application: None,
            },
            module_sha256: "0".repeat(64),
            state: ExtensionState::Staged,
            installed_at_ms: 1,
            activation: None,
            last_error: None,
        }
    }

    fn dependency(name: &str, version: &str) -> ExtensionDependency {
        ExtensionDependency {
            name: name.to_string(),
            version: version.to_string(),
            abi_version: EXTENSION_ABI_VERSION,
            optional: false,
            capabilities: BTreeSet::new(),
            module_sha256: None,
        }
    }

    #[test]
    fn dependency_graph_is_semver_checked_and_dependency_first() {
        let installations = vec![
            staged_installation(
                "website",
                "2.0.0",
                vec![dependency("renderer", "^1.2")],
                BTreeSet::new(),
            ),
            staged_installation(
                "renderer",
                "1.4.0",
                vec![dependency("templates", ">=3.0, <4.0")],
                BTreeSet::new(),
            ),
            staged_installation("templates", "3.1.0", vec![], BTreeSet::new()),
        ];
        assert_eq!(
            resolve_extension_order(&installations, &["website".to_string()], false).unwrap(),
            ["templates", "renderer", "website"]
        );

        let mut incompatible = installations;
        incompatible[2].manifest.identity.version = "4.0.0".to_string();
        assert!(
            resolve_extension_order(&incompatible, &["website".to_string()], false)
                .unwrap_err()
                .to_string()
                .contains("requires version")
        );
    }

    #[test]
    fn dependency_cycles_and_unknown_manifest_fields_fail_closed() {
        let installations = vec![
            staged_installation(
                "one",
                "1.0.0",
                vec![dependency("two", "*")],
                BTreeSet::new(),
            ),
            staged_installation(
                "two",
                "1.0.0",
                vec![dependency("one", "*")],
                BTreeSet::new(),
            ),
        ];
        assert!(
            resolve_extension_order(&installations, &["one".to_string()], false)
                .unwrap_err()
                .to_string()
                .contains("one -> two -> one")
        );

        let manifest = serde_json::to_value(Demo.manifest().unwrap()).unwrap();
        let mut object = manifest.as_object().unwrap().clone();
        object.insert("surprise".to_string(), serde_json::json!(true));
        assert!(serde_json::from_value::<ExtensionManifest>(object.into()).is_err());
    }
}
