//! Strongly typed contracts for the capability-mediated BicDB application ABI.
//!
//! The v2 wire encoding is canonical JSON carried through the single
//! `bicdb:app/host.call` import. The enum discriminants and validation in this
//! module are the stable interface; WASM modules never receive native pointers,
//! database objects, sockets, files, environment variables, or WASI.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{ExtensionCapability, ExtensionError, ExtensionManifest, HttpMethod, Result};

pub const APPLICATION_ABI_VERSION: u32 = 2;
pub const APPLICATION_COMPATIBILITY_PROFILE: &str = "bicdb-application-v2";
pub const MAX_CALL_DEPTH: u16 = 32;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ActorContext {
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub service_id: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub acting_client_id: Option<String>,
    #[serde(default)]
    pub authentication_method: Option<String>,
    #[serde(default)]
    pub roles: BTreeSet<String>,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub organization_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub delegation_chain: Vec<DelegationLink>,
    #[serde(default)]
    pub assurance_level: Option<String>,
    #[serde(default)]
    pub request_origin: Option<String>,
    pub trace_id: String,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub causation_id: Option<String>,
    pub deadline_unix_ms: i64,
    #[serde(default)]
    pub policy_attributes: BTreeMap<String, String>,
}

impl ActorContext {
    pub fn validate(&self) -> Result<()> {
        validate_nonempty("trace_id", &self.trace_id, 256)?;
        if self.deadline_unix_ms <= 0 {
            return invalid("actor deadline must be a positive Unix millisecond value");
        }
        if self.user_id.is_none() && self.service_id.is_none() {
            return invalid("actor context requires a user_id or service_id");
        }
        if self.delegation_chain.len() > 16 {
            return invalid("actor delegation chain exceeds 16 links");
        }
        for link in &self.delegation_chain {
            link.validate()?;
        }
        validate_string_set("role", &self.roles)?;
        validate_string_set("scope", &self.scopes)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DelegationLink {
    pub subject: String,
    pub delegated_by: String,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
}

impl DelegationLink {
    fn validate(&self) -> Result<()> {
        validate_nonempty("delegation subject", &self.subject, 256)?;
        validate_nonempty("delegating subject", &self.delegated_by, 256)?;
        validate_string_set("delegated scope", &self.scopes)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseAction {
    Select,
    Insert,
    Update,
    Delete,
    Upsert,
    Aggregate,
    FullTextSearch,
    VectorSearch,
    Spatial,
    Timeseries,
    JsonPath,
    Execute,
    RawSql,
    Lock,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RelationPermission {
    pub relation: String,
    pub actions: BTreeSet<DatabaseAction>,
    #[serde(default)]
    pub readable_columns: BTreeSet<String>,
    #[serde(default)]
    pub writable_columns: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawSqlDeclaration {
    pub id: String,
    pub sql: String,
    pub sha256: String,
    pub relations: BTreeSet<String>,
    #[serde(default)]
    pub routines: BTreeSet<String>,
    pub actions: BTreeSet<DatabaseAction>,
    #[serde(default)]
    pub parameters: Vec<FieldType>,
    #[serde(default)]
    pub result: Vec<ContractField>,
    #[serde(default = "default_raw_sql_max_rows")]
    pub max_affected_rows: u32,
}

const fn default_raw_sql_max_rows() -> u32 {
    10_000
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceImport {
    pub name: String,
    pub service: String,
    pub version: String,
    pub contract_sha256: String,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub propagate_transaction: bool,
    #[serde(default)]
    pub allow_reentrant: bool,
    /// Execute the callee under its own signed implementation authority rather
    /// than the legacy caller/callee intersection. Both sides must opt in.
    #[serde(default)]
    pub delegated_authority: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceExport {
    pub service: String,
    pub version: String,
    pub contract_sha256: String,
    pub export: String,
    #[serde(default)]
    pub methods: Vec<ServiceMethod>,
    #[serde(default)]
    pub allow_reentrant: bool,
    /// Permit exact importers to invoke this service under the callee's signed
    /// implementation authority. The method contract remains the only entry.
    #[serde(default)]
    pub delegated_authority: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceMethod {
    pub name: String,
    #[serde(default)]
    pub request: Vec<ContractField>,
    #[serde(default)]
    pub response: Vec<ContractField>,
    /// Authored, stable failure codes this method may return.
    #[serde(default)]
    pub errors: BTreeSet<String>,
    /// Explicit compatibility escape hatch for BicDB application programs whose failure
    /// code is computed at runtime rather than represented by a static union.
    #[serde(default)]
    pub allows_undeclared_errors: bool,
    /// Declared authored failures for which a caller may safely retry.
    #[serde(default)]
    pub retryable_errors: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SecretDeclaration {
    pub name: String,
    pub operations: BTreeSet<CryptoOperation>,
    #[serde(default)]
    pub versions: BTreeSet<String>,
    #[serde(default)]
    pub allow_plaintext_read: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum CryptoOperation {
    Metadata,
    Sign,
    Verify,
    Hmac,
    Encrypt,
    Decrypt,
    Derive,
    PlaintextRead,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EgressDeclaration {
    pub name: String,
    /// Optional operator-owned provider binding. Provider-bound calls carry
    /// only relative paths; the physical endpoint stays outside the package.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Exact header names whose values the operator provider must supply.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub required_provider_headers: BTreeSet<String>,
    pub schemes: BTreeSet<String>,
    pub hosts: BTreeSet<String>,
    pub ports: BTreeSet<u16>,
    #[serde(default)]
    pub allow_redirects: bool,
    #[serde(default)]
    pub allow_private_networks: bool,
    #[serde(default)]
    pub mtls_secret: Option<String>,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub timeout_ms: u64,
    pub max_concurrency: u32,
    pub requests_per_minute: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlobDeclaration {
    pub namespace: String,
    pub max_blob_bytes: u64,
    #[serde(default)]
    pub content_types: BTreeSet<String>,
    #[serde(default)]
    pub allow_signed_urls: bool,
    #[serde(default)]
    pub require_scan: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkerDefinition {
    pub name: String,
    pub export: String,
    pub queue: String,
    pub group: String,
    pub max_attempts: u32,
    pub visibility_timeout_ms: u64,
    #[serde(default)]
    pub retry_delay_ms: u64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub dead_letter_queue: Option<String>,
    #[serde(default)]
    pub payload_arguments: Vec<String>,
    #[serde(default)]
    pub required: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScheduleDefinition {
    pub name: String,
    pub export: String,
    pub schedule: String,
    pub timezone: String,
    #[serde(default)]
    pub payload: JsonValue,
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "ScheduleMisfirePolicy::is_skip")]
    pub misfire: ScheduleMisfirePolicy,
    #[serde(default, skip_serializing_if = "ScheduleOverlapPolicy::is_skip")]
    pub overlap: ScheduleOverlapPolicy,
    #[serde(
        default = "default_schedule_concurrency",
        skip_serializing_if = "is_one_u16"
    )]
    pub max_concurrency: u16,
    #[serde(
        default = "default_schedule_catch_up_limit",
        skip_serializing_if = "is_default_schedule_catch_up_limit"
    )]
    pub catch_up_limit: u16,
    #[serde(default, skip_serializing_if = "ScheduleUpgradePolicy::is_preserve")]
    pub upgrade: ScheduleUpgradePolicy,
}

const fn default_schedule_concurrency() -> u16 {
    1
}

const fn default_schedule_catch_up_limit() -> u16 {
    100
}

fn is_one_u16(value: &u16) -> bool {
    *value == 1
}

fn is_default_schedule_catch_up_limit(value: &u16) -> bool {
    *value == default_schedule_catch_up_limit()
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleMisfirePolicy {
    #[default]
    Skip,
    FireOnce,
    CatchUp,
}

impl ScheduleMisfirePolicy {
    fn is_skip(value: &Self) -> bool {
        *value == Self::Skip
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleOverlapPolicy {
    #[default]
    Skip,
    Queue,
}

impl ScheduleOverlapPolicy {
    fn is_skip(value: &Self) -> bool {
        *value == Self::Skip
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleUpgradePolicy {
    #[default]
    Preserve,
    Reset,
}

impl ScheduleUpgradePolicy {
    fn is_preserve(value: &Self) -> bool {
        *value == Self::Preserve
    }
}

/// Authentication verifier family selected by a signed BicDB application route.
/// Secrets and key material remain operator-owned and are never packaged.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationAuthKindV1 {
    JwtHs256,
    OidcEd25519,
    OidcRs256,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationAuthSchemeV1 {
    pub kind: ApplicationAuthKindV1,
    pub issuer: String,
    pub audience: String,
}

/// Compiler-signed contract for BicDB application's stateful authentication helpers.
/// Key material remains in the host secret provider; this contract only binds
/// the selected verifier, secret name, lifetimes, and durability/evidence
/// requirements.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationSecurityContractV1 {
    pub version: u32,
    /// Exact BicDB application builtin names reachable from the signed program.
    #[serde(default)]
    pub helpers: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_scheme: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_algorithm: Option<String>,
    #[serde(default = "default_carrier_access_ttl_seconds")]
    pub access_ttl_seconds: u64,
    #[serde(default = "default_carrier_refresh_ttl_seconds")]
    pub refresh_ttl_seconds: u64,
    #[serde(default)]
    pub magic_link_secrets: BTreeSet<String>,
    #[serde(default = "true_value")]
    pub durable_replay: bool,
    #[serde(default = "true_value")]
    pub versioned_keys: bool,
    #[serde(default = "true_value")]
    pub emit_evidence: bool,
}

/// Compiler-signed logical blob namespace used by BicDB application's embedded runtime.
/// Guest paths refer only to invocation-local virtual files; provider keys and
/// bytes stay behind the host capability boundary.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationBlobContractV1 {
    pub version: u32,
    #[serde(default)]
    pub helpers: BTreeSet<String>,
    pub namespace: String,
    #[serde(default)]
    pub signed_methods: BTreeSet<String>,
    pub max_blob_bytes: u64,
    #[serde(default = "true_value")]
    pub virtual_files: bool,
    #[serde(default = "true_value")]
    pub durable_keys: bool,
    #[serde(default = "true_value")]
    pub emit_evidence: bool,
}

fn default_carrier_access_ttl_seconds() -> u64 {
    3_600
}

fn default_carrier_refresh_ttl_seconds() -> u64 {
    604_800
}

fn true_value() -> bool {
    true
}

impl ApplicationBlobContractV1 {
    fn validate(&self, application: &ApplicationManifestV2) -> Result<()> {
        const HELPERS: &[&str] = &[
            "blob.put",
            "blob.get",
            "blob.signed_url",
            "blob.metadata",
            "blob.hash",
        ];
        if self.version != 1 {
            return invalid(format!(
                "BicDB application blob contract requires version 1, found {}",
                self.version
            ));
        }
        if self.helpers.is_empty()
            || self
                .helpers
                .iter()
                .any(|helper| !HELPERS.contains(&helper.as_str()))
        {
            return invalid(
                "BicDB application blob contract has no helpers or names an unsupported helper",
            );
        }
        let program = application.application_program.as_ref().ok_or_else(|| {
            ExtensionError::InvalidManifest(
                "BicDB application blob contract requires a behavior program".to_string(),
            )
        })?;
        let used_helpers = HELPERS
            .iter()
            .filter(|helper| application_program_uses_builtin(program, &[*helper]))
            .map(|helper| (*helper).to_string())
            .collect::<BTreeSet<_>>();
        if used_helpers != self.helpers {
            return invalid(
                "BicDB application blob contract helper set differs from the signed behavior program",
            );
        }
        if application_program_blob_signed_methods(program)? != self.signed_methods {
            return invalid(
                "BicDB application blob signed method set differs from the signed behavior program",
            );
        }
        validate_identifier("BicDB application blob namespace", &self.namespace)?;
        if self.max_blob_bytes == 0
            || !self.virtual_files
            || !self.durable_keys
            || !self.emit_evidence
        {
            return invalid(
                "BicDB application blob contract requires a positive limit, virtual files, durable keys, and evidence",
            );
        }
        if self
            .signed_methods
            .iter()
            .any(|method| !matches!(method.as_str(), "GET" | "PUT"))
            || (self.helpers.contains("blob.signed_url") != !self.signed_methods.is_empty())
        {
            return invalid(
                "BicDB application blob signed methods must be the exact non-empty GET/PUT set",
            );
        }
        let declaration = application
            .blobs
            .iter()
            .find(|blob| blob.namespace == self.namespace)
            .ok_or_else(|| {
                ExtensionError::InvalidManifest(format!(
                    "BicDB application blob namespace `{}` is undeclared",
                    self.namespace
                ))
            })?;
        if declaration.max_blob_bytes != self.max_blob_bytes
            || declaration.allow_signed_urls != !self.signed_methods.is_empty()
        {
            return invalid(
                "BicDB application blob program and namespace size/signed-URL authority disagree",
            );
        }
        Ok(())
    }
}

impl ApplicationRedisContractV1 {
    fn validate(&self, application: &ApplicationManifestV2) -> Result<()> {
        const HELPERS: &[&str] = &["redis.publish", "redis.incr"];
        if self.version != 1
            || self.helpers.is_empty()
            || self
                .helpers
                .iter()
                .any(|helper| !HELPERS.contains(&helper.as_str()))
        {
            return invalid(
                "BicDB application Redis contract requires version 1 and exact supported helpers",
            );
        }
        let program = application.application_program.as_ref().ok_or_else(|| {
            ExtensionError::InvalidManifest(
                "BicDB application Redis contract requires a behavior program".to_string(),
            )
        })?;
        let used = HELPERS
            .iter()
            .filter(|helper| application_program_uses_builtin(program, &[*helper]))
            .map(|helper| (*helper).to_string())
            .collect::<BTreeSet<_>>();
        if used != self.helpers {
            return invalid(
                "BicDB application Redis contract helper set differs from the signed behavior program",
            );
        }
        validate_identifier("BicDB application Redis provider", &self.provider)?;
        if self.max_key_bytes == 0
            || self.max_key_bytes > 16 * 1024
            || self.max_channel_bytes == 0
            || self.max_channel_bytes > 16 * 1024
            || self.max_message_bytes == 0
            || self.max_message_bytes > 16 * 1024 * 1024
            || !self.tenant_scoped_keys
            || !self.application_scoped_channels
            || !self.emit_evidence
        {
            return invalid(
                "BicDB application Redis contract requires bounded inputs, scoped keys/channels, and evidence",
            );
        }
        Ok(())
    }
}

impl ApplicationEmailContractV1 {
    fn validate(&self, application: &ApplicationManifestV2) -> Result<()> {
        if self.version != 1 || self.helper != "email.send" {
            return invalid(
                "BicDB application email contract requires version 1 and helper email.send",
            );
        }
        let program = application.application_program.as_ref().ok_or_else(|| {
            ExtensionError::InvalidManifest(
                "BicDB application email contract requires a behavior program".to_string(),
            )
        })?;
        if !application_program_uses_builtin(program, &["email.send"])
            || application_program_uses_builtin_named_argument(program, "email.send", "smtp_url")
        {
            return invalid(
                "BicDB application email contract must exactly bind email.send without a request-selected SMTP endpoint",
            );
        }
        validate_identifier("BicDB application email provider", &self.provider)?;
        if self.max_recipients == 0
            || self.max_recipients > 1_000
            || self.max_address_bytes == 0
            || self.max_address_bytes > 8 * 1024
            || self.max_subject_bytes == 0
            || self.max_subject_bytes > 64 * 1024
            || self.max_body_bytes == 0
            || self.max_body_bytes > 16 * 1024 * 1024
            || !self.emit_evidence
        {
            return invalid(
                "BicDB application email contract requires bounded inputs and evidence",
            );
        }
        Ok(())
    }
}

impl ApplicationGrpcContractV1 {
    fn validate(&self) -> Result<()> {
        const MAX_MESSAGE_BYTES: u64 = 16 * 1024 * 1024;
        const SCALARS: &[&str] = &[
            "string", "bool", "float", "double", "int32", "sint32", "sfixed32", "fixed32",
            "uint32", "int64", "sint64", "sfixed64", "fixed64", "uint64",
        ];
        if self.version != 1 || self.clients.is_empty() || self.clients.len() > 1_024 {
            return invalid(
                "BicDB application gRPC contract requires version 1 and bounded clients",
            );
        }
        for (client_name, client) in &self.clients {
            validate_identifier("BicDB application gRPC client", client_name)?;
            validate_identifier("BicDB application gRPC provider", &client.provider)?;
            validate_identifier("BicDB application gRPC service", &client.service)?;
            if client.methods.is_empty()
                || client.methods.len() > 4_096
                || client.messages.is_empty()
                || client.messages.len() > 16_384
                || client.max_request_bytes == 0
                || client.max_request_bytes > MAX_MESSAGE_BYTES
                || client.max_response_bytes == 0
                || client.max_response_bytes > MAX_MESSAGE_BYTES
                || !client.emit_evidence
            {
                return invalid(format!(
                    "BicDB application gRPC client `{client_name}` has invalid method, schema, size, or evidence bounds"
                ));
            }
            for (message_name, message) in &client.messages {
                validate_identifier("BicDB application gRPC message", message_name)?;
                validate_identifier("BicDB application gRPC proto message", &message.proto_name)?;
                if message.fields.len() > 16_384 {
                    return invalid(format!(
                        "BicDB application gRPC message `{message_name}` has too many fields"
                    ));
                }
                let mut names = BTreeSet::new();
                let mut tags = BTreeSet::new();
                for field in &message.fields {
                    validate_identifier("BicDB application gRPC field", &field.name)?;
                    validate_identifier("BicDB application gRPC proto field", &field.proto_name)?;
                    if !names.insert(field.name.clone())
                        || !tags.insert(field.tag)
                        || field.tag == 0
                        || field.tag >= (1 << 29)
                        || (19_000..=19_999).contains(&field.tag)
                        || (field.repeated && field.optional)
                    {
                        return invalid(format!(
                            "BicDB application gRPC message `{message_name}` has invalid or duplicate field metadata"
                        ));
                    }
                    match &field.message_type {
                        Some(target) => {
                            if SCALARS.contains(&field.wire_type.as_str())
                                || client
                                    .messages
                                    .get(target)
                                    .is_none_or(|message| message.proto_name != field.wire_type)
                            {
                                return invalid(format!(
                                    "BicDB application gRPC field `{message_name}.{}` has an invalid message reference",
                                    field.name
                                ));
                            }
                        }
                        None if !SCALARS.contains(&field.wire_type.as_str()) => {
                            return invalid(format!(
                                "BicDB application gRPC field `{message_name}.{}` has an unsupported wire type",
                                field.name
                            ));
                        }
                        None => {}
                    }
                }
            }
            for (method_name, method) in &client.methods {
                validate_identifier("BicDB application gRPC method", method_name)?;
                if method.path.len() > 8 * 1024
                    || !method.path.starts_with('/')
                    || method.path.contains("..")
                    || !method.path.ends_with(&format!("/{method_name}"))
                    || !client.messages.contains_key(&method.request_type)
                    || !client.messages.contains_key(&method.response_type)
                    || method.deadline_ms == 0
                    || method.deadline_ms > 60_000
                    || method.retries > 10
                {
                    return invalid(format!(
                        "BicDB application gRPC method `{client_name}.{method_name}` has invalid path, schema, deadline, or retry authority"
                    ));
                }
            }
        }
        Ok(())
    }
}

impl ApplicationTokenizerContractV1 {
    fn validate(&self) -> Result<()> {
        if self.version != 1 || self.providers.is_empty() || self.providers.len() > 1_024 {
            return invalid(
                "BicDB application tokenizer contract requires version 1 and bounded providers",
            );
        }
        for (name, provider) in &self.providers {
            validate_identifier("BicDB application tokenizer provider", name)?;
            if provider.max_input_bytes == 0
                || provider.max_input_bytes > 16 * 1024 * 1024
                || provider.max_tokens == 0
                || provider.max_tokens > 1_000_000
                || !provider.emit_evidence
            {
                return invalid(format!(
                    "BicDB application tokenizer provider `{name}` has invalid bounds or evidence"
                ));
            }
        }
        Ok(())
    }
}

impl ApplicationEmbeddingsContractV1 {
    fn validate(&self) -> Result<()> {
        if self.version != 1 || self.providers.is_empty() || self.providers.len() > 1_024 {
            return invalid(
                "BicDB application embeddings contract requires version 1 and bounded providers",
            );
        }
        for (name, provider) in &self.providers {
            validate_identifier("BicDB application embeddings provider", name)?;
            if provider.dimensions == 0
                || provider.dimensions > 65_535
                || provider.max_input_bytes == 0
                || provider.max_input_bytes > 16 * 1024 * 1024
                || !provider.emit_evidence
            {
                return invalid(format!(
                    "BicDB application embeddings provider `{name}` has invalid dimensions, bounds, or evidence"
                ));
            }
        }
        Ok(())
    }
}

impl ApplicationLlmContractV1 {
    fn validate(&self) -> Result<()> {
        if self.version != 1 || self.clients.is_empty() || self.clients.len() > 1_024 {
            return invalid(
                "BicDB application LLM contract requires version 1 and bounded clients",
            );
        }
        for (name, client) in &self.clients {
            validate_identifier("BicDB application LLM client", name)?;
            validate_identifier("BicDB application LLM provider", &client.provider)?;
            validate_identifier(
                "BicDB application LLM tokenizer provider",
                &client.tokenizer_provider,
            )?;
            if client.methods.is_empty()
                || client.methods.len() > 4
                || client.methods.iter().any(|method| {
                    !matches!(
                        method.as_str(),
                        "respond" | "respond_as" | "stream" | "stream_response"
                    )
                })
                || client.max_prompt_bytes == 0
                || client.max_prompt_bytes > 16 * 1024 * 1024
                || client.max_history_messages == 0
                || client.max_history_messages > 1_024
                || client.max_output_tokens == 0
                || client.max_output_tokens > 1_000_000
                || client.max_turns == 0
                || client.max_turns > 128
                || client.max_response_bytes == 0
                || client.max_response_bytes > 16 * 1024 * 1024
                || client.temperature_millis > 2_000
                || client
                    .wire_format
                    .as_deref()
                    .is_some_and(|wire| !matches!(wire, "openai" | "anthropic"))
                || client
                    .model
                    .as_ref()
                    .is_some_and(|model| model.is_empty() || model.len() > 1_024)
                || client
                    .system_prompt
                    .as_ref()
                    .is_some_and(|prompt| prompt.len() > 1024 * 1024)
                || (client.operator_system_prompt && client.system_prompt.is_some())
                || !client.emit_evidence
            {
                return invalid(format!(
                    "BicDB application LLM client `{name}` has invalid method, provider, model, prompt, or execution bounds"
                ));
            }
            if client.methods.contains("respond_as") != !client.structured_outputs.is_empty() {
                return invalid(format!(
                    "BicDB application LLM client `{name}` structured-output authority does not match its methods"
                ));
            }
            for (output, value_type) in &client.structured_outputs {
                validate_identifier("BicDB application LLM structured output", output)?;
                value_type.validate(0)?;
            }
            for (tool_name, tool) in &client.tools {
                validate_identifier("BicDB application LLM tool", tool_name)?;
                validate_identifier("BicDB application LLM tool callable", &tool.callable)?;
                if tool.parameters.len() > 128
                    || tool
                        .description
                        .as_ref()
                        .is_some_and(|description| description.len() > 64 * 1024)
                {
                    return invalid(format!(
                        "BicDB application LLM tool `{name}.{tool_name}` is oversized"
                    ));
                }
                unique_by(
                    "BicDB application LLM tool parameter",
                    tool.parameters.iter().map(|parameter| &parameter.name),
                )?;
                for parameter in &tool.parameters {
                    validate_identifier("BicDB application LLM tool parameter", &parameter.name)?;
                    parameter.value_type.validate(0)?;
                    for validation in &parameter.validations {
                        validation.validate(name, "LLM tool", &parameter.name)?;
                    }
                }
                tool.output.validate(0)?;
            }
            if let Some(budget) = &client.budget {
                if budget.limit_microusd_per_tenant_day == 0 {
                    return invalid(format!(
                        "BicDB application LLM client `{name}` has an empty budget"
                    ));
                }
                match &budget.over_budget {
                    ApplicationLlmOverBudgetV1::Fail { error_code } => {
                        validate_service_error_code(error_code)?;
                    }
                    ApplicationLlmOverBudgetV1::Downgrade { client } => {
                        validate_identifier("BicDB application LLM budget downgrade", client)?;
                    }
                }
            }
            if let Some(routing) = &client.routing {
                validate_identifier("BicDB application routed LLM primary", &routing.primary)?;
                let mut fallbacks = BTreeSet::new();
                for fallback in &routing.fallbacks {
                    validate_identifier("BicDB application routed LLM fallback", fallback)?;
                    if !fallbacks.insert(fallback) {
                        return invalid(format!(
                            "BicDB application LLM client `{name}` repeats routing fallback `{fallback}`"
                        ));
                    }
                }
                if routing.fallbacks.len() > 16
                    || routing
                        .fallbacks
                        .iter()
                        .any(|target| target == &routing.primary)
                    || (!routing.on_primary_outage
                        && !routing.on_rate_limit
                        && !routing.on_budget_pressure
                        && routing.target_microusd_per_request.is_none())
                {
                    return invalid(format!(
                        "BicDB application LLM client `{name}` has invalid routing"
                    ));
                }
            }
            if let Some(stream) = &client.stream {
                for (label, value) in [
                    ("queue", &stream.queue),
                    ("chunk event", &stream.chunk_event),
                    ("tool event", &stream.tool_event),
                    ("completed event", &stream.completed_event),
                    ("failed event", &stream.failed_event),
                ] {
                    validate_identifier(&format!("BicDB application LLM stream {label}"), value)?;
                }
                if !stream.path.starts_with('/')
                    || stream.path.len() > 8 * 1024
                    || stream.path.contains("..")
                    || !client.methods.contains("stream")
                {
                    return invalid(format!(
                        "BicDB application LLM client `{name}` has invalid streaming authority"
                    ));
                }
            }
        }
        Ok(())
    }
}

impl ApplicationRagContractV1 {
    fn validate(&self) -> Result<()> {
        if self.version != 1 || self.pipelines.is_empty() || self.pipelines.len() > 1_024 {
            return invalid(
                "BicDB application RAG contract requires version 1 and bounded pipelines",
            );
        }
        for (name, pipeline) in &self.pipelines {
            validate_identifier("BicDB application RAG pipeline", name)?;
            validate_identifier("BicDB application RAG retriever", &pipeline.retriever_model)?;
            if let Some(provider) = &pipeline.embedding_provider {
                validate_identifier("BicDB application RAG embedding provider", provider)?;
            }
            if let Some(callable) = &pipeline.embedding_callable {
                validate_identifier("BicDB application RAG embedding callable", callable)?;
            }
            validate_identifier("BicDB application RAG LLM client", &pipeline.llm_client)?;
            if pipeline.dimensions == 0
                || pipeline.dimensions > 65_535
                || pipeline.top_k == 0
                || pipeline.top_k > 1_000
                || pipeline.context_window_tokens == 0
                || pipeline.context_window_tokens > 1_000_000
                || pipeline
                    .score_threshold_millionths
                    .is_some_and(|threshold| !(-1_000_000..=1_000_000).contains(&threshold))
                || !pipeline.emit_evidence
                || pipeline.embedding_provider.is_some() == pipeline.embedding_callable.is_some()
            {
                return invalid(format!(
                    "BicDB application RAG pipeline `{name}` has invalid bounds"
                ));
            }
        }
        Ok(())
    }
}

impl ApplicationAgentContractV1 {
    fn validate(&self) -> Result<()> {
        if self.version != 1 || self.agents.is_empty() || self.agents.len() > 1_024 {
            return invalid(
                "BicDB application agent contract requires version 1 and bounded agents",
            );
        }
        for (name, agent) in &self.agents {
            validate_identifier("BicDB application agent", name)?;
            validate_identifier("BicDB application agent LLM client", &agent.llm_client)?;
            validate_identifier("BicDB application agent callable", &agent.callable)?;
            if let Some(output) = &agent.structured_output {
                validate_identifier("BicDB application agent structured output", output)?;
            }
            validate_string_set("BicDB application agent tool", &agent.tools)?;
            agent.output.validate(0)?;
            if agent.max_iterations == 0
                || agent.max_iterations > 128
                || agent.budget_tokens == 0
                || agent.budget_tokens > 100_000_000
                || agent.timeout_ms == 0
                || agent.timeout_ms > 24 * 60 * 60 * 1_000
                || agent.max_tool_calls == 0
                || agent.max_tool_calls > 10_000
                || agent.retry_attempts > 64
                || agent.retry_backoff_ms > 60 * 60 * 1_000
            {
                return invalid(format!(
                    "BicDB application agent `{name}` has invalid execution bounds"
                ));
            }
            if let Some(fallback) = &agent.fallback_output {
                validate_carrier_expression(fallback)?;
            }
        }
        Ok(())
    }
}

impl ApplicationEvaluationContractV1 {
    fn validate(&self) -> Result<()> {
        if self.version != 1 || self.evaluations.is_empty() || self.evaluations.len() > 1_024 {
            return invalid(
                "BicDB application evaluation contract requires version 1 and bounded evaluations",
            );
        }
        for (name, evaluation) in &self.evaluations {
            validate_identifier("BicDB application evaluation", name)?;
            validate_identifier(
                "BicDB application evaluation provider",
                &evaluation.provider,
            )?;
            validate_identifier(
                "BicDB application evaluation case callable",
                &evaluation.case_callable,
            )?;
            validate_identifier(
                "BicDB application evaluation requirement callable",
                &evaluation.require_callable,
            )?;
            if let Some(auth) = &evaluation.auth {
                for expression in [&auth.id, &auth.email, &auth.name, &auth.roles] {
                    validate_carrier_expression(expression)?;
                    if carrier_expression_call_matches(expression, &|_, _, _, _, _| true) {
                        return invalid(format!(
                            "BicDB application evaluation `{name}` auth expressions must be pure"
                        ));
                    }
                }
                if let Some(tenant_id) = &auth.tenant_id {
                    validate_carrier_expression(tenant_id)?;
                    if carrier_expression_call_matches(tenant_id, &|_, _, _, _, _| true) {
                        return invalid(format!(
                            "BicDB application evaluation `{name}` tenant auth expression must be pure"
                        ));
                    }
                }
            }
            evaluation.case_type.validate(0)?;
            if evaluation.max_cases == 0
                || evaluation.max_cases > 1_000_000
                || evaluation.timeout_ms == 0
                || evaluation.timeout_ms > 24 * 60 * 60 * 1_000
                || !evaluation.emit_evidence
            {
                return invalid(format!(
                    "BicDB application evaluation `{name}` has invalid bounds"
                ));
            }
        }
        Ok(())
    }
}

impl ApplicationTestContractV1 {
    fn validate(&self) -> Result<()> {
        if self.version != 1 || self.tests.is_empty() || self.tests.len() > 1_024 {
            return invalid("BicDB application test contract requires version 1 and bounded tests");
        }
        for (name, test) in &self.tests {
            validate_identifier("BicDB application test", name)?;
            validate_identifier("BicDB application test callable", &test.callable)?;
            if let Some(auth) = &test.auth {
                for expression in [&auth.id, &auth.email, &auth.name, &auth.roles] {
                    validate_carrier_expression(expression)?;
                    if carrier_expression_call_matches(expression, &|_, _, _, _, _| true) {
                        return invalid(format!(
                            "BicDB application test `{name}` auth expressions must be pure"
                        ));
                    }
                }
                if let Some(tenant_id) = &auth.tenant_id {
                    validate_carrier_expression(tenant_id)?;
                    if carrier_expression_call_matches(tenant_id, &|_, _, _, _, _| true) {
                        return invalid(format!(
                            "BicDB application test `{name}` tenant auth expression must be pure"
                        ));
                    }
                }
            }
            if let Some(case_type) = &test.case_type {
                case_type.validate(0)?;
            }
            if test.cases == 0
                || test.cases > 10_000
                || (test.case_type.is_none() && test.cases != 1)
                || test.timeout_ms == 0
                || test.timeout_ms > 24 * 60 * 60 * 1_000
                || !test.emit_evidence
            {
                return invalid(format!(
                    "BicDB application test `{name}` has invalid bounds"
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationRealtimeContractV1 {
    pub name: String,
    pub path: String,
    pub queue: String,
    pub events: BTreeSet<String>,
    pub output: ApplicationRouteParameterTypeV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorize_group: Option<String>,
}

impl ApplicationAuthSchemeV1 {
    fn validate(&self, name: &str) -> Result<()> {
        validate_identifier("authentication scheme", name)?;
        if self.issuer.trim().is_empty() || self.issuer.len() > 2_048 {
            return invalid(format!(
                "authentication scheme `{name}` has an empty or oversized issuer"
            ));
        }
        if self.audience.trim().is_empty() || self.audience.len() > 1_024 {
            return invalid(format!(
                "authentication scheme `{name}` has an empty or oversized audience"
            ));
        }
        Ok(())
    }
}

impl ApplicationSecurityContractV1 {
    fn validate(&self, application: &ApplicationManifestV2) -> Result<()> {
        if self.version != 1 {
            return invalid(format!(
                "BicDB application security contract requires version 1, found {}",
                self.version
            ));
        }
        if self.access_ttl_seconds == 0 || self.refresh_ttl_seconds == 0 {
            return invalid("BicDB application security token lifetimes must be positive");
        }
        if self.access_ttl_seconds > self.refresh_ttl_seconds {
            return invalid("BicDB application security access lifetime exceeds refresh lifetime");
        }
        let supported = BTreeSet::from([
            "auth.register",
            "auth.login",
            "auth.issue_tokens",
            "auth.password_policy",
            "auth.password_hash",
            "auth.password_verify",
            "auth.password_breach_digest",
            "auth.totp_secret",
            "auth.totp_code",
            "auth.totp_verify",
            "auth.totp_uri",
            "auth.magic_link_issue",
            "auth.magic_link_verify",
            "auth.oauth_authorize",
            "auth.oauth_callback",
        ]);
        if self.helpers.is_empty()
            || !self
                .helpers
                .iter()
                .all(|helper| supported.contains(helper.as_str()))
        {
            return invalid(
                "BicDB application security contract has no helpers or names an unsupported helper",
            );
        }
        if !self.durable_replay || !self.versioned_keys || !self.emit_evidence {
            return invalid(
                "BicDB application security v1 requires durable replay, versioned keys, and evidence",
            );
        }
        if self.helpers.contains("auth.issue_tokens")
            && (self.auth_scheme.is_none()
                || self.signing_secret.is_none()
                || self.signing_algorithm.is_none())
        {
            return invalid(
                "BicDB application auth.issue_tokens requires an auth scheme and host signing secret",
            );
        }
        match (
            &self.auth_scheme,
            &self.signing_secret,
            &self.signing_algorithm,
        ) {
            (None, None, None) => {}
            (Some(scheme), None, None) if !self.helpers.contains("auth.issue_tokens") => {
                if !application.auth_schemes.contains_key(scheme) {
                    return invalid(format!(
                        "BicDB application security contract references absent auth scheme `{scheme}`"
                    ));
                }
            }
            (Some(scheme), Some(secret), Some(algorithm)) => {
                let auth = application.auth_schemes.get(scheme).ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "BicDB application security contract references absent auth scheme `{scheme}`"
                    ))
                })?;
                let expected = match auth.kind {
                    ApplicationAuthKindV1::JwtHs256 => "hmac-sha256",
                    ApplicationAuthKindV1::OidcEd25519 => "ed25519",
                    ApplicationAuthKindV1::OidcRs256 => "rsa-sha256",
                };
                if algorithm != expected {
                    return invalid(format!(
                        "BicDB application security scheme `{scheme}` requires `{expected}` signing"
                    ));
                }
                if !application.secrets.iter().any(|candidate| {
                    candidate.name == *secret
                        && candidate.operations.contains(&CryptoOperation::Metadata)
                        && candidate.operations.contains(&CryptoOperation::Sign)
                        && candidate.operations.contains(&CryptoOperation::Verify)
                }) {
                    return invalid(format!(
                        "BicDB application security signing secret `{secret}` is undeclared or lacks metadata/sign/verify authority"
                    ));
                }
            }
            _ => {
                return invalid(
                    "BicDB application security auth scheme, signing secret, and algorithm must be declared together",
                );
            }
        }
        for secret in &self.magic_link_secrets {
            if !application.secrets.iter().any(|candidate| {
                candidate.name == *secret
                    && candidate.operations.contains(&CryptoOperation::Metadata)
                    && candidate.operations.contains(&CryptoOperation::Hmac)
                    && candidate.operations.contains(&CryptoOperation::Verify)
            }) {
                return invalid(format!(
                    "BicDB application magic-link secret `{secret}` is undeclared or lacks metadata/HMAC/verify authority"
                ));
            }
        }
        Ok(())
    }

    fn validate_host_route(&self, route: &RouteV2) -> Result<()> {
        let (method, template, public, required_helpers): (HttpMethod, &str, bool, &[&str]) =
            match route.export.as_str() {
                "__carrier_security_register" => (
                    HttpMethod::Post,
                    "/auth/register",
                    true,
                    &["auth.register", "auth.issue_tokens"],
                ),
                "__carrier_security_login" => (
                    HttpMethod::Post,
                    "/auth/login",
                    true,
                    &["auth.login", "auth.issue_tokens"],
                ),
                "__carrier_security_refresh" => (
                    HttpMethod::Post,
                    "/auth/refresh",
                    true,
                    &["auth.issue_tokens"],
                ),
                "__carrier_security_logout" => (
                    HttpMethod::Post,
                    "/auth/logout",
                    false,
                    &["auth.issue_tokens"],
                ),
                "__carrier_security_sessions" => (
                    HttpMethod::Get,
                    "/auth/sessions",
                    false,
                    &["auth.issue_tokens"],
                ),
                "__carrier_security_session_revoke" => (
                    HttpMethod::Delete,
                    "/auth/sessions/{session_id}",
                    false,
                    &["auth.issue_tokens"],
                ),
                _ => {
                    return invalid(
                        "BicDB application package names an unknown host security route",
                    )
                }
            };
        let expected_scheme = (!public).then(|| self.auth_scheme.clone()).flatten();
        if route.method != method
            || route.template != template
            || route.public != public
            || route.auth_scheme != expected_scheme
            || !required_helpers
                .iter()
                .all(|helper| self.helpers.contains(*helper))
            || route.resource.is_some()
            || route.operation.is_some()
            || route.service_call.is_some()
            || route.idempotency.is_some()
            || route.cache.is_some()
            || !route.roles.is_empty()
            || !route.scopes.is_empty()
            || route.streaming_request
            || route.streaming_response
            || route.sse
            || route.websocket
        {
            return invalid(format!(
                "BicDB application host security route `{}` has a weakened or forged contract",
                route.name
            ));
        }
        Ok(())
    }
}

impl ApplicationRealtimeContractV1 {
    fn validate(
        &self,
        application: &ApplicationManifestV2,
        extension: &ExtensionManifest,
    ) -> Result<()> {
        validate_identifier("BicDB application realtime surface", &self.name)?;
        validate_route_template(&self.path)?;
        if self.path.ends_with('/') {
            return invalid(format!(
                "BicDB application realtime surface `{}` path must not end with a slash",
                self.name
            ));
        }
        validate_identifier("BicDB application realtime queue", &self.queue)?;
        if self.events.is_empty() {
            return invalid(format!(
                "BicDB application realtime surface `{}` has no events",
                self.name
            ));
        }
        validate_string_set("BicDB application realtime event", &self.events)?;
        self.output.validate(0)?;
        let fields = match &self.output {
            ApplicationRouteParameterTypeV1::Object { fields } => fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<BTreeSet<_>>(),
            _ => BTreeSet::new(),
        };
        for (label, field) in [
            ("tenant", self.tenant_field.as_deref()),
            ("workspace", self.workspace_field.as_deref()),
            ("group", self.group_field.as_deref()),
        ] {
            if let Some(field) = field {
                if !fields.contains(field) {
                    return invalid(format!(
                        "BicDB application realtime surface `{}` {label} field `{field}` is absent from its output",
                        self.name
                    ));
                }
            }
        }
        if self.authorize_group.is_some() && self.group_field.is_none() {
            return invalid(format!(
                "BicDB application realtime surface `{}` group authorization requires a group field",
                self.name
            ));
        }
        if let Some(callable) = &self.authorize_group {
            let callable = application
                .application_program
                .as_ref()
                .and_then(|program| program.callables.get(callable))
                .ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "BicDB application realtime surface `{}` references absent authorization callable `{callable}`",
                        self.name
                    ))
                })?;
            if callable.parameters != ["group"] {
                return invalid(format!(
                    "BicDB application realtime surface `{}` authorization callable must accept exactly `group`",
                    self.name
                ));
            }
        }
        if !extension.permissions.publish_queues.contains(&self.queue) {
            return invalid(format!(
                "BicDB application realtime surface `{}` queue `{}` is not declared for publish",
                self.name, self.queue
            ));
        }
        if !extension
            .capabilities
            .contains(&ExtensionCapability::Streaming)
            || !extension
                .capabilities
                .contains(&ExtensionCapability::QueueEvents)
            || !application.required_features.is_superset(&BTreeSet::from([
                ApplicationFeature::Streaming,
                ApplicationFeature::Sse,
                ApplicationFeature::WebSocket,
            ]))
        {
            return invalid(format!(
                "BicDB application realtime surface `{}` lacks signed streaming capabilities",
                self.name
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RouteV2 {
    pub name: String,
    pub method: HttpMethod,
    pub template: String,
    pub export: String,
    #[serde(default)]
    pub resource: Option<String>,
    #[serde(default)]
    pub operation: Option<ResourceOperation>,
    /// Optional direct binding from an HTTP route to a signed plugin service
    /// import. The host performs the in-process call while preserving actor,
    /// deadline, dependency-lock, and cycle enforcement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_call: Option<RouteServiceCall>,
    /// Signed BicDB application-specific coercion and validation schema. Other guests
    /// continue to receive the generic raw HTTP ABI.
    #[serde(
        default,
        alias = "carrier_request",
        skip_serializing_if = "Option::is_none"
    )]
    pub application_request: Option<ApplicationRouteRequestV1>,
    /// Signed BicDB application result schema checked before a response leaves the host.
    #[serde(
        default,
        alias = "carrier_response",
        skip_serializing_if = "Option::is_none"
    )]
    pub application_response: Option<ApplicationRouteParameterTypeV1>,
    /// Database-native replay contract for a custom application route. The
    /// host stores the normalized request and response in the same transaction
    /// as the route's BicDB application-program effects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency: Option<IdempotencyContract>,
    /// Host-managed JSON response cache for a BicDB application GET route.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<RouteCacheContract>,
    /// Compiler-signed trace sampling override for this route. Collection and
    /// export remain host-owned; the application can only narrow the policy
    /// declared by its BicDB application observability contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telemetry: Option<ApplicationRouteTelemetryV1>,
    /// Compiler-signed static success headers (for example BicDB application route
    /// security policy). Dynamic host headers remain host-owned.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub response_headers: BTreeMap<String, String>,
    /// Allows the route to run without a bearer token. A supplied token is
    /// still verified and becomes the actor context.
    #[serde(default)]
    pub public: bool,
    /// Application-local name of the signed verifier contract for a protected
    /// route. Absent only for public routes and legacy single-verifier packages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_scheme: Option<String>,
    #[serde(default)]
    pub roles: BTreeSet<String>,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
    /// Match at least one declared role instead of requiring the full set.
    #[serde(default)]
    pub roles_any: bool,
    /// Match at least one declared scope instead of requiring the full set.
    #[serde(default)]
    pub scopes_any: bool,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    #[serde(default)]
    pub streaming_request: bool,
    #[serde(default)]
    pub streaming_response: bool,
    #[serde(default)]
    pub sse: bool,
    #[serde(default)]
    pub websocket: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RouteServiceCall {
    pub dependency: String,
    pub service: String,
    pub method: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationRouteRequestV1 {
    pub path_parameters: Vec<ApplicationRouteParameterV1>,
    pub query_parameters: Vec<ApplicationRouteParameterV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<ApplicationRouteParameterTypeV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationRouteParameterV1 {
    pub name: String,
    pub value_type: ApplicationRouteParameterTypeV1,
    pub optional: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_json: Option<String>,
    #[serde(default)]
    pub validations: Vec<ApplicationRouteParameterValidationV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApplicationRouteParameterTypeV1 {
    String,
    Int,
    Float,
    Decimal,
    Bool,
    Json,
    Timestamp,
    Date,
    LocalDateTime,
    TimeZone,
    Uuid,
    Enum {
        values: BTreeSet<String>,
    },
    List {
        element: Box<Self>,
    },
    Set {
        element: Box<Self>,
    },
    Optional {
        value: Box<Self>,
    },
    Object {
        fields: Vec<ApplicationRouteParameterV1>,
    },
    Map {
        key: Box<Self>,
        value: Box<Self>,
    },
    Vector {
        dimensions: usize,
    },
    Point,
    LineString,
    Polygon,
    Null,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApplicationRouteParameterValidationV1 {
    Email,
    Length {
        min: Option<usize>,
        max: Option<usize>,
    },
    Range {
        minimum: Option<String>,
        maximum: Option<String>,
    },
    Pattern {
        expression: String,
    },
}

/// Versioned, compiler-emitted BicDB application behavior executed by BicDB's reusable
/// runtime. Applications carry data, not generated web-server source.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationProgramV1 {
    pub version: u32,
    #[serde(default = "default_program_max_steps")]
    pub max_steps: u64,
    /// Maximum nested local BicDB application callable depth. This is separate from the
    /// application-wide service-call depth and is signed with the program so
    /// the interpreter cannot silently choose a weaker bound.
    #[serde(default = "default_program_max_call_depth")]
    pub max_call_depth: u16,
    #[serde(default)]
    pub callables: BTreeMap<String, ApplicationCallableV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<ApplicationBlobContractV1>,
    /// Exact Redis-compatible helper/provider authority used by the program.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redis: Option<ApplicationRedisContractV1>,
    /// Exact operator-owned email provider authority used by `email.send`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<ApplicationEmailContractV1>,
    /// Exact unary gRPC schemas and method authority. Endpoints and transport
    /// credentials are deliberately absent and remain operator provider state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc: Option<ApplicationGrpcContractV1>,
    /// Exact local tokenizer authority used by LLM preflight and explicitly
    /// lowered BicDB-native functions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<ApplicationTokenizerContractV1>,
    /// Exact local embedding model and dimension authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embeddings: Option<ApplicationEmbeddingsContractV1>,
    /// Exact structured-completion authority. Provider endpoints, model
    /// credentials, and operator-selected configuration are never packaged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm: Option<ApplicationLlmContractV1>,
    /// Exact retrieval pipelines composed from signed embeddings, native
    /// vector search, and LLM clients.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rag: Option<ApplicationRagContractV1>,
    /// Durable bounded agent definitions. Tool implementations remain ordinary
    /// signed BicDB application callables and cannot acquire authority from model output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<ApplicationAgentContractV1>,
    /// Operator-dataset evaluation programs compiled into exact callables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluations: Option<ApplicationEvaluationContractV1>,
    /// Signed scenario and property tests executable only through the trusted
    /// operator test path. Test HTTP calls never grant application egress.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tests: Option<ApplicationTestContractV1>,
    /// Exact observability helpers, redaction, sampling, and propagation
    /// behavior emitted by the BicDB application compiler. Export destinations and
    /// credentials are deliberately absent and remain operator state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observability: Option<ApplicationObservabilityContractV1>,
    #[serde(default)]
    pub service_bindings: BTreeMap<String, ApplicationServiceBindingV1>,
    #[serde(default)]
    pub client_bindings: BTreeMap<String, ApplicationClientBindingV1>,
    /// Compiler-signed feature-flag configuration. The guest can select only a
    /// declared literal flag name; actor attributes come from the trusted host.
    #[serde(default)]
    pub flags: BTreeMap<String, ApplicationFlagDefinitionV1>,
    /// Maps compiler-validated literal job names to private durable broker
    /// queues consumed by generic BicDB application workers.
    #[serde(default)]
    pub job_bindings: BTreeMap<String, String>,
    /// Maps BicDB application encrypted field references (for example `User.ssn`) to
    /// signed, host-only secret declarations. The program never receives key
    /// material.
    #[serde(default)]
    pub secret_bindings: BTreeMap<String, String>,
    /// Stateful auth/session and key-rotation behavior selected by the BicDB application
    /// compiler. The runtime rejects security helpers when this exact contract
    /// is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security: Option<ApplicationSecurityContractV1>,
    /// Maps BicDB application event names to private durable broker queues. Publishing
    /// and consuming still require the corresponding signed permissions.
    #[serde(default)]
    pub event_bindings: BTreeMap<String, String>,
    /// Dedicated durable fan-out queues for signed realtime surfaces. Event
    /// publication mirrors into these queues in the owning transaction.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub realtime_bindings: BTreeMap<String, BTreeSet<String>>,
    #[serde(default)]
    pub mutation_bindings: Vec<ApplicationMutationBindingV1>,
    /// Signed durable workflow plans executed by BicDB's generic state-machine
    /// runner. Callables remain in the shared interpreter program.
    #[serde(default)]
    pub workflow_bindings: BTreeMap<String, ApplicationWorkflowDefinitionV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationObservabilityContractV1 {
    pub version: u32,
    pub provider: String,
    pub protocol: ApplicationTelemetryProtocolV1,
    pub service_name: String,
    pub sampling: ApplicationSamplingV1,
    /// Exact effectful BicDB application observability builtins reachable from the
    /// signed program. An empty set is deny-all rather than wildcard access.
    #[serde(default)]
    pub helpers: BTreeSet<String>,
    /// Field names that must be recursively replaced before an event reaches
    /// any operational sink or durable audit record.
    #[serde(default)]
    pub redacted_keys: BTreeSet<String>,
    #[serde(default)]
    pub metric_names: BTreeSet<String>,
    #[serde(default)]
    pub audit_actions: BTreeSet<String>,
    #[serde(default)]
    pub dynamic_metric_names: bool,
    #[serde(default)]
    pub dynamic_audit_actions: bool,
    pub max_field_depth: u16,
    pub max_field_bytes: u64,
    pub durable_audit: bool,
    pub propagate_w3c: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationRouteTelemetryV1 {
    pub sampling: ApplicationSamplingV1,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationTelemetryProtocolV1 {
    Host,
    OtlpHttp,
    OtlpGrpc,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApplicationSamplingV1 {
    AlwaysOn,
    AlwaysOff,
    ParentBasedAlwaysOn,
    ParentBasedAlwaysOff,
    Ratio { millionths: u32 },
    ParentBasedRatio { millionths: u32 },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationRedisContractV1 {
    pub version: u32,
    pub provider: String,
    pub helpers: BTreeSet<String>,
    pub max_key_bytes: u32,
    pub max_channel_bytes: u32,
    pub max_message_bytes: u32,
    pub tenant_scoped_keys: bool,
    pub application_scoped_channels: bool,
    pub emit_evidence: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationEmailContractV1 {
    pub version: u32,
    pub provider: String,
    pub helper: String,
    pub max_recipients: u32,
    pub max_address_bytes: u32,
    pub max_subject_bytes: u32,
    pub max_body_bytes: u32,
    pub emit_evidence: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationGrpcContractV1 {
    pub version: u32,
    pub clients: BTreeMap<String, ApplicationGrpcClientV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationGrpcClientV1 {
    pub provider: String,
    pub service: String,
    pub methods: BTreeMap<String, ApplicationGrpcMethodV1>,
    pub messages: BTreeMap<String, ApplicationGrpcMessageV1>,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub emit_evidence: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationGrpcMethodV1 {
    pub path: String,
    pub request_type: String,
    pub response_type: String,
    pub deadline_ms: u64,
    pub retries: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationGrpcMessageV1 {
    pub proto_name: String,
    pub fields: Vec<ApplicationGrpcFieldV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationGrpcFieldV1 {
    pub name: String,
    pub proto_name: String,
    pub wire_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_type: Option<String>,
    pub tag: u32,
    pub repeated: bool,
    pub optional: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationTokenizerContractV1 {
    pub version: u32,
    pub providers: BTreeMap<String, ApplicationTokenizerProviderV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationTokenizerProviderV1 {
    pub max_input_bytes: u64,
    pub max_tokens: u64,
    pub emit_evidence: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationEmbeddingsContractV1 {
    pub version: u32,
    pub providers: BTreeMap<String, ApplicationEmbeddingsProviderV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationEmbeddingsProviderV1 {
    pub dimensions: u32,
    pub max_input_bytes: u64,
    pub emit_evidence: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationLlmContractV1 {
    pub version: u32,
    pub clients: BTreeMap<String, ApplicationLlmClientV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationLlmClientV1 {
    pub provider: String,
    pub tokenizer_provider: String,
    pub methods: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire_format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub max_prompt_bytes: u64,
    pub max_history_messages: u32,
    pub max_output_tokens: u64,
    #[serde(default = "one_u32", skip_serializing_if = "is_one_u32")]
    pub max_turns: u32,
    pub max_response_bytes: u64,
    pub temperature_millis: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    pub operator_system_prompt: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub structured_outputs: BTreeMap<String, ApplicationRouteParameterTypeV1>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tools: BTreeMap<String, ApplicationLlmToolV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<ApplicationLlmBudgetV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<ApplicationLlmRoutingV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<ApplicationLlmStreamV1>,
    pub emit_evidence: bool,
}

fn one_u32() -> u32 {
    1
}

fn is_one_u32(value: &u32) -> bool {
    *value == 1
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationLlmToolV1 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub parameters: Vec<ApplicationRouteParameterV1>,
    pub output: ApplicationRouteParameterTypeV1,
    pub callable: String,
    pub mutating: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationLlmBudgetV1 {
    pub limit_microusd_per_tenant_day: u64,
    pub over_budget: ApplicationLlmOverBudgetV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApplicationLlmOverBudgetV1 {
    Fail { error_code: String },
    Downgrade { client: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationLlmRoutingV1 {
    pub primary: String,
    #[serde(default)]
    pub fallbacks: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_microusd_per_request: Option<u64>,
    pub on_primary_outage: bool,
    pub on_rate_limit: bool,
    pub on_budget_pressure: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationLlmStreamV1 {
    pub path: String,
    pub queue: String,
    pub chunk_event: String,
    pub tool_event: String,
    pub completed_event: String,
    pub failed_event: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationRagContractV1 {
    pub version: u32,
    pub pipelines: BTreeMap<String, ApplicationRagPipelineV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationRagPipelineV1 {
    pub retriever_model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_callable: Option<String>,
    pub dimensions: u32,
    pub llm_client: String,
    pub top_k: u32,
    pub context_window_tokens: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score_threshold_millionths: Option<i32>,
    pub emit_evidence: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationAgentContractV1 {
    pub version: u32,
    pub agents: BTreeMap<String, ApplicationAgentV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationAgentV1 {
    pub llm_client: String,
    pub callable: String,
    pub output: ApplicationRouteParameterTypeV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_output: Option<String>,
    pub max_iterations: u32,
    pub budget_tokens: u64,
    pub timeout_ms: u64,
    #[serde(default)]
    pub tools: BTreeSet<String>,
    pub require_auth: bool,
    pub require_tenant: bool,
    pub max_tool_calls: u32,
    #[serde(default)]
    pub retry_attempts: u32,
    #[serde(default)]
    pub retry_backoff_ms: u64,
    pub deny_tools_after_output: bool,
    pub output_must_match: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_output: Option<ApplicationExpressionV1>,
    pub emit_tokens: bool,
    pub emit_tool_calls: bool,
    pub emit_guard_failures: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationEvaluationContractV1 {
    pub version: u32,
    pub evaluations: BTreeMap<String, ApplicationEvaluationV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationEvaluationV1 {
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<ApplicationEvaluationAuthV1>,
    pub case_type: ApplicationRouteParameterTypeV1,
    pub case_callable: String,
    pub require_callable: String,
    pub max_cases: u32,
    pub timeout_ms: u64,
    pub emit_evidence: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationEvaluationAuthV1 {
    pub id: ApplicationExpressionV1,
    pub email: ApplicationExpressionV1,
    pub name: ApplicationExpressionV1,
    pub roles: ApplicationExpressionV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<ApplicationExpressionV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationTestContractV1 {
    pub version: u32,
    pub tests: BTreeMap<String, ApplicationTestV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationTestV1 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<ApplicationEvaluationAuthV1>,
    pub callable: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub case_type: Option<ApplicationRouteParameterTypeV1>,
    pub cases: u32,
    pub seed: u64,
    pub timeout_ms: u64,
    pub emit_evidence: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationWorkflowDefinitionV1 {
    pub queue: String,
    pub worker_export: String,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_retries: u32,
    #[serde(default)]
    pub graph_execution: bool,
    #[serde(default = "one_u16")]
    pub max_parallelism: u16,
    /// Hash of the durable execution topology. Active runs may cross package
    /// upgrades only when this contract remains unchanged.
    #[serde(default)]
    pub plan_sha256: Option<String>,
    pub return_step: String,
    #[serde(default)]
    pub steps: Vec<ApplicationWorkflowStepV1>,
    #[serde(default)]
    pub slas: Vec<ApplicationWorkflowSlaV1>,
    /// Pure predicates checked atomically before a durable run is created.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub invariants: Vec<ApplicationWorkflowInvariantV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationWorkflowInvariantV1 {
    pub name: String,
    pub kind: ApplicationInvariantKindV1,
    pub expression: ApplicationExpressionV1,
    pub source: String,
}

fn one_u16() -> u16 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationWorkflowStepV1 {
    pub name: String,
    pub callable: String,
    #[serde(default)]
    pub condition_callable: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub max_retries: u32,
    #[serde(default)]
    pub retry_delay_ms: u64,
    #[serde(default)]
    pub compensation_callable: Option<String>,
    #[serde(default)]
    pub wait: Option<ApplicationWorkflowWaitV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationWorkflowWaitV1 {
    pub kind: ApplicationWorkflowWaitKindV1,
    #[serde(default)]
    pub signal: Option<String>,
    #[serde(default)]
    pub delay_ms: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationWorkflowWaitKindV1 {
    Signal,
    Delay,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationWorkflowSlaV1 {
    pub name: String,
    pub attainment_basis_points: u32,
    pub deadline_ms: u64,
    pub warning_at_basis_points: u32,
    pub breach_at_basis_points: u32,
    #[serde(default)]
    pub starts_when: Option<ApplicationWorkflowSlaConditionV1>,
    #[serde(default)]
    pub ends_when: Option<ApplicationWorkflowSlaConditionV1>,
    #[serde(default)]
    pub attach_to_audit: bool,
    pub scope: String,
    pub measure_by: String,
    #[serde(default)]
    pub reports: Vec<String>,
    #[serde(default)]
    pub exclusions: Vec<ApplicationWorkflowSlaExclusionV1>,
    #[serde(default)]
    pub escalations: Vec<ApplicationWorkflowSlaEscalationV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationWorkflowSlaExclusionV1 {
    pub kind: ApplicationWorkflowSlaExclusionKindV1,
    #[serde(default)]
    pub event: Option<String>,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub value: Option<bool>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationWorkflowSlaExclusionKindV1 {
    Event,
    FieldEqualsBool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationWorkflowSlaConditionV1 {
    pub kind: ApplicationWorkflowSlaConditionKindV1,
    pub value: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationWorkflowSlaConditionKindV1 {
    Event,
    Status,
    StepStarted,
    StepCompleted,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationWorkflowSlaEscalationV1 {
    pub trigger: ApplicationWorkflowSlaEscalationTriggerV1,
    pub target_kind: String,
    pub target: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationWorkflowSlaEscalationTriggerV1 {
    BreachImminent,
    Breached,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationMutationOperationV1 {
    Create,
    Update,
    Delete,
    Restore,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationMutationBindingKindV1 {
    InsideTrigger,
    PostCommitTrigger,
    Watch,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationMutationBindingV1 {
    pub resource: String,
    pub operation: ApplicationMutationOperationV1,
    pub kind: ApplicationMutationBindingKindV1,
    pub callable: String,
    #[serde(default)]
    pub event: Option<String>,
    #[serde(default)]
    pub queue: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationServiceBindingV1 {
    pub dependency: String,
    pub service: String,
    pub method: String,
    #[serde(default)]
    pub parameters: Vec<String>,
    #[serde(default)]
    pub propagate_transaction: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationClientBindingV1 {
    pub policy: String,
    pub base_url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub timeout_ms: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationFlagGroupV1 {
    UserId,
    TenantId,
    WorkspaceId,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApplicationFlagRuleV1 {
    TenantIn {
        tenants: Vec<String>,
        value: bool,
    },
    Percentage {
        percent: u8,
        grouped_by: ApplicationFlagGroupV1,
        value: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationFlagDefinitionV1 {
    pub default: bool,
    #[serde(default)]
    pub rules: Vec<ApplicationFlagRuleV1>,
}

fn default_program_max_steps() -> u64 {
    100_000
}

fn default_program_max_call_depth() -> u16 {
    16
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationCallableV1 {
    #[serde(default)]
    pub parameters: Vec<String>,
    #[serde(default)]
    pub body: Vec<ApplicationStatementV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApplicationStatementV1 {
    Let {
        name: String,
        value: ApplicationExpressionV1,
    },
    Assign {
        name: String,
        value: ApplicationExpressionV1,
    },
    Return {
        value: ApplicationExpressionV1,
    },
    If {
        condition: ApplicationExpressionV1,
        #[serde(default)]
        then_branch: Vec<ApplicationStatementV1>,
        #[serde(default)]
        else_branch: Vec<ApplicationStatementV1>,
    },
    For {
        name: String,
        iterable: ApplicationExpressionV1,
        #[serde(default)]
        body: Vec<ApplicationStatementV1>,
    },
    While {
        condition: ApplicationExpressionV1,
        #[serde(default)]
        body: Vec<ApplicationStatementV1>,
    },
    Transaction {
        isolation: String,
        #[serde(default)]
        body: Vec<ApplicationStatementV1>,
    },
    Break,
    Continue,
    Fail {
        code: ApplicationExpressionV1,
        message: ApplicationExpressionV1,
    },
    Emit {
        event: String,
        value: ApplicationExpressionV1,
    },
    WithTimeout {
        timeout_ms: ApplicationExpressionV1,
        #[serde(default)]
        body: Vec<ApplicationStatementV1>,
    },
    WithRetry {
        attempts: ApplicationExpressionV1,
        backoff_ms: ApplicationExpressionV1,
        #[serde(default)]
        max_backoff_ms: Option<ApplicationExpressionV1>,
        #[serde(default)]
        body: Vec<ApplicationStatementV1>,
    },
    CircuitBreaker {
        name: ApplicationExpressionV1,
        failure_threshold: ApplicationExpressionV1,
        reset_timeout_ms: ApplicationExpressionV1,
        #[serde(default)]
        body: Vec<ApplicationStatementV1>,
    },
    Expr {
        value: ApplicationExpressionV1,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApplicationExpressionV1 {
    Variable {
        name: String,
    },
    Literal {
        value: JsonValue,
    },
    Object {
        #[serde(default)]
        fields: BTreeMap<String, ApplicationExpressionV1>,
    },
    Array {
        #[serde(default)]
        items: Vec<ApplicationExpressionV1>,
    },
    Field {
        target: Box<ApplicationExpressionV1>,
        field: String,
    },
    Index {
        target: Box<ApplicationExpressionV1>,
        index: Box<ApplicationExpressionV1>,
    },
    Unary {
        operator: String,
        value: Box<ApplicationExpressionV1>,
        #[serde(default)]
        value_type: Option<ApplicationExpressionTypeV1>,
        #[serde(default)]
        operand_type: Option<ApplicationExpressionTypeV1>,
    },
    Binary {
        operator: String,
        left: Box<ApplicationExpressionV1>,
        right: Box<ApplicationExpressionV1>,
        #[serde(default)]
        value_type: Option<ApplicationExpressionTypeV1>,
        #[serde(default)]
        left_type: Option<ApplicationExpressionTypeV1>,
        #[serde(default)]
        right_type: Option<ApplicationExpressionTypeV1>,
    },
    Match {
        value: Box<ApplicationExpressionV1>,
        #[serde(default)]
        arms: Vec<ApplicationMatchArmV1>,
    },
    Call {
        kind: ApplicationCallKindV1,
        target: String,
        #[serde(default)]
        method: Option<String>,
        #[serde(default)]
        result_type: Option<ApplicationExpressionTypeV1>,
        #[serde(default)]
        argument_types: Vec<ApplicationExpressionTypeV1>,
        #[serde(default)]
        argument_item_types: Vec<Option<ApplicationExpressionTypeV1>>,
        /// Compiler-signed item shaping for paged BicDB application model helpers. The
        /// resource host returns the full authorized rows; the interpreter
        /// applies this contract before the value can reach application code.
        #[serde(default)]
        result_projection: Option<ApplicationModelProjectionV1>,
        #[serde(default)]
        arguments: Vec<ApplicationArgumentV1>,
    },
    Exists {
        binding: String,
        resource: String,
        condition: Box<ApplicationExpressionV1>,
    },
    Aggregate {
        function: ApplicationAggregateFunctionV1,
        binding: String,
        resource: String,
        condition: Box<ApplicationExpressionV1>,
        #[serde(default)]
        field: Option<String>,
        #[serde(default)]
        value_type: Option<ApplicationExpressionTypeV1>,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationAggregateFunctionV1 {
    Count,
    Sum,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationModelProjectionV1 {
    #[serde(default)]
    pub fields: Vec<ApplicationProjectionFieldV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApplicationProjectionFieldV1 {
    Field {
        name: String,
        field: String,
    },
    Relation {
        name: String,
        source_field: String,
        target: String,
        target_key: String,
        target_field: String,
    },
    Computed {
        name: String,
        expression: Box<ApplicationExpressionV1>,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationExpressionTypeV1 {
    String,
    Int,
    Float,
    Decimal,
    Bool,
    Json,
    Timestamp,
    Date,
    LocalDateTime,
    TimeZone,
    Uuid,
    Money,
    ZonedDateTime,
    Other,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationCallKindV1 {
    Function,
    Action,
    Builtin,
    Model,
    Client,
    Queue,
    Llm,
    Rag,
    Agent,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationArgumentV1 {
    #[serde(default)]
    pub name: Option<String>,
    pub value: ApplicationExpressionV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationMatchArmV1 {
    #[serde(default)]
    pub variant: Option<String>,
    pub value: ApplicationExpressionV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PackageMetadata {
    pub application: String,
    pub version: String,
    pub package_sha256: String,
    pub dependency_lock_sha256: String,
    pub sbom_sha256: String,
    pub provenance_sha256: String,
    pub signature_key_id: String,
    pub signature_algorithm: String,
    pub signature: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationManifestV2 {
    #[serde(default = "application_abi_version")]
    pub abi_version: u32,
    #[serde(alias = "carrier_profile")]
    pub application_profile: String,
    pub package: PackageMetadata,
    #[serde(default)]
    pub relation_permissions: Vec<RelationPermission>,
    #[serde(default)]
    pub raw_sql: Vec<RawSqlDeclaration>,
    #[serde(default)]
    pub service_imports: Vec<ServiceImport>,
    #[serde(default)]
    pub service_exports: Vec<ServiceExport>,
    #[serde(default)]
    pub secrets: Vec<SecretDeclaration>,
    #[serde(default)]
    pub egress: Vec<EgressDeclaration>,
    #[serde(default)]
    pub blobs: Vec<BlobDeclaration>,
    #[serde(default)]
    pub routes: Vec<RouteV2>,
    /// Compiler-signed application-wide success headers inherited by routes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub response_headers: BTreeMap<String, String>,
    /// Public verifier metadata selected by routes. Operator key material is
    /// supplied independently by the trusted host.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub auth_schemes: BTreeMap<String, ApplicationAuthSchemeV1>,
    /// Compiler-signed BicDB application stream/subscription surfaces. The host owns
    /// transport negotiation, durable cursors, filtering, and revocation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub realtime: Vec<ApplicationRealtimeContractV1>,
    #[serde(default)]
    pub resources: Vec<ResourceContractV1>,
    #[serde(default)]
    pub invariants: Vec<ApplicationInvariantV1>,
    #[serde(default)]
    pub migrations: Vec<MigrationPlanV1>,
    #[serde(default)]
    pub workers: Vec<WorkerDefinition>,
    #[serde(default)]
    pub schedules: Vec<ScheduleDefinition>,
    #[serde(
        default,
        alias = "carrier_program",
        skip_serializing_if = "Option::is_none"
    )]
    pub application_program: Option<ApplicationProgramV1>,
    #[serde(default)]
    pub required_features: BTreeSet<ApplicationFeature>,
    #[serde(default = "default_max_call_depth")]
    pub max_call_depth: u16,
}

fn application_abi_version() -> u32 {
    APPLICATION_ABI_VERSION
}

fn default_max_call_depth() -> u16 {
    16
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn readable_column_allowed(
    application: &ApplicationManifestV2,
    permission: &RelationPermission,
    column: &str,
) -> bool {
    (!application
        .required_features
        .contains(&ApplicationFeature::ExactColumnAuthority)
        && permission.readable_columns.is_empty())
        || permission.readable_columns.contains(column)
}

impl ApplicationManifestV2 {
    pub fn validate(&self, extension: &ExtensionManifest) -> Result<()> {
        if self.abi_version != APPLICATION_ABI_VERSION
            || extension.identity.abi_version != APPLICATION_ABI_VERSION
        {
            return invalid("application manifest requires extension ABI v2");
        }
        if self.application_profile != APPLICATION_COMPATIBILITY_PROFILE {
            return invalid(format!(
                "unsupported BicDB application profile `{}`; expected `{APPLICATION_COMPATIBILITY_PROFILE}`",
                self.application_profile
            ));
        }
        if self.max_call_depth == 0 || self.max_call_depth > MAX_CALL_DEPTH {
            return invalid(format!("max_call_depth must be 1..={MAX_CALL_DEPTH}"));
        }
        self.package.validate(&extension.identity.name)?;
        reject_unsupported_features(&self.required_features)?;
        unique_by(
            "relation permission",
            self.relation_permissions.iter().map(|item| &item.relation),
        )?;
        for permission in &self.relation_permissions {
            validate_qualified("relation", &permission.relation)?;
            if permission.actions.is_empty() {
                return invalid(format!(
                    "relation permission `{}` has no operations",
                    permission.relation
                ));
            }
            validate_string_set("readable column", &permission.readable_columns)?;
            validate_string_set("writable column", &permission.writable_columns)?;
            if self
                .required_features
                .contains(&ApplicationFeature::ExactColumnAuthority)
            {
                if let Some(resource) = self
                    .resources
                    .iter()
                    .find(|resource| resource.relation.eq_ignore_ascii_case(&permission.relation))
                {
                    let fields = resource
                        .fields
                        .iter()
                        .map(|field| field.name.as_str())
                        .collect::<BTreeSet<_>>();
                    if permission
                        .readable_columns
                        .iter()
                        .chain(&permission.writable_columns)
                        .any(|column| !fields.contains(column.as_str()))
                    {
                        return invalid(format!(
                            "relation permission `{}` contains a column outside its resource contract",
                            permission.relation
                        ));
                    }
                }
            }
        }
        unique_by("raw SQL", self.raw_sql.iter().map(|item| &item.id))?;
        for statement in &self.raw_sql {
            statement.validate()?;
        }
        unique_by(
            "service import",
            self.service_imports.iter().map(|item| &item.name),
        )?;
        unique_by(
            "service export",
            self.service_exports.iter().map(|item| &item.service),
        )?;
        for service in &self.service_imports {
            service.validate()?;
        }
        for service in &self.service_exports {
            service.validate()?;
        }
        let delegates_service_authority = self
            .service_imports
            .iter()
            .any(|service| service.delegated_authority)
            || self
                .service_exports
                .iter()
                .any(|service| service.delegated_authority);
        if delegates_service_authority
            != self
                .required_features
                .contains(&ApplicationFeature::DelegatedServiceAuthority)
        {
            return invalid(
                "delegated service authority and its required feature must be declared together",
            );
        }
        unique_by("secret", self.secrets.iter().map(|item| &item.name))?;
        for secret in &self.secrets {
            validate_identifier("secret", &secret.name)?;
            if secret.operations.is_empty() {
                return invalid(format!(
                    "secret `{}` has no allowed operations",
                    secret.name
                ));
            }
            if secret.allow_plaintext_read
                != secret.operations.contains(&CryptoOperation::PlaintextRead)
            {
                return invalid(format!(
                    "secret `{}` plaintext policy and operation disagree",
                    secret.name
                ));
            }
        }
        unique_by("egress policy", self.egress.iter().map(|item| &item.name))?;
        for egress in &self.egress {
            egress.validate()?;
            if let Some(secret) = egress.mtls_secret.as_deref() {
                if !self.secrets.iter().any(|declaration| {
                    declaration.name == secret && !declaration.allow_plaintext_read
                }) {
                    return invalid(format!(
                        "egress policy `{}` mTLS secret `{secret}` must be a declared host-only secret",
                        egress.name
                    ));
                }
            }
        }
        unique_by(
            "blob namespace",
            self.blobs.iter().map(|item| &item.namespace),
        )?;
        for blob in &self.blobs {
            validate_identifier("blob namespace", &blob.namespace)?;
            if blob.max_blob_bytes == 0 {
                return invalid(format!(
                    "blob namespace `{}` requires a positive size limit",
                    blob.namespace
                ));
            }
        }
        unique_by("v2 route", self.routes.iter().map(|item| &item.name))?;
        validate_signed_response_headers("application", &self.response_headers)?;
        for (name, scheme) in &self.auth_schemes {
            scheme.validate(name)?;
        }
        unique_by(
            "BicDB application realtime surface",
            self.realtime.iter().map(|item| &item.name),
        )?;
        let mut realtime_paths = BTreeSet::new();
        for realtime in &self.realtime {
            realtime.validate(self, extension)?;
            if !realtime_paths.insert(realtime.path.to_ascii_lowercase()) {
                return invalid(format!(
                    "duplicate BicDB application realtime path `{}`",
                    realtime.path
                ));
            }
            let program = self.application_program.as_ref().ok_or_else(|| {
                ExtensionError::InvalidManifest(format!(
                    "BicDB application realtime surface `{}` has no BicDB application program",
                    realtime.name
                ))
            })?;
            for event in &realtime.events {
                if !program
                    .realtime_bindings
                    .get(event)
                    .is_some_and(|queues| queues.contains(&realtime.queue))
                {
                    return invalid(format!(
                        "BicDB application realtime surface `{}` event `{event}` lacks its exact fan-out queue",
                        realtime.name
                    ));
                }
            }
        }
        let mut route_keys = BTreeSet::new();
        for route in &self.routes {
            route.validate()?;
            if route.export.starts_with("__carrier_security_") {
                self.application_program
                    .as_ref()
                    .and_then(|program| program.security.as_ref())
                    .ok_or_else(|| {
                        ExtensionError::InvalidManifest(format!(
                            "BicDB application host security route `{}` has no signed security contract",
                            route.name
                        ))
                    })?
                    .validate_host_route(route)?;
            }
            match (route.public, route.auth_scheme.as_deref()) {
                (true, Some(_)) => {
                    return invalid(format!(
                        "public route `{}` cannot select an authentication scheme",
                        route.name
                    ));
                }
                (false, Some(scheme)) if !self.auth_schemes.contains_key(scheme) => {
                    return invalid(format!(
                        "route `{}` references absent authentication scheme `{scheme}`",
                        route.name
                    ));
                }
                (false, None) if !self.auth_schemes.is_empty() => {
                    return invalid(format!(
                        "protected route `{}` must select an authentication scheme",
                        route.name
                    ));
                }
                _ => {}
            }
            if let Some(call) = &route.service_call {
                if !self
                    .service_imports
                    .iter()
                    .any(|import| import.name == call.dependency && import.service == call.service)
                {
                    return invalid(format!(
                        "route `{}` references undeclared plugin service `{}/{}`",
                        route.name, call.dependency, call.service
                    ));
                }
            }
            if !route_keys.insert((route.method, route.template.to_ascii_lowercase())) {
                return invalid(format!(
                    "duplicate v2 route {} {}",
                    route.method, route.template
                ));
            }
        }
        if let Some(security) = self
            .application_program
            .as_ref()
            .and_then(|program| program.security.as_ref())
        {
            let mut required_exports = BTreeSet::new();
            if security.helpers.contains("auth.issue_tokens") {
                required_exports.extend([
                    "__carrier_security_refresh",
                    "__carrier_security_logout",
                    "__carrier_security_sessions",
                    "__carrier_security_session_revoke",
                ]);
            }
            if security.helpers.contains("auth.register")
                && security.helpers.contains("auth.issue_tokens")
            {
                required_exports.insert("__carrier_security_register");
            }
            if security.helpers.contains("auth.login")
                && security.helpers.contains("auth.issue_tokens")
            {
                required_exports.insert("__carrier_security_login");
            }
            for export in required_exports {
                if !self.routes.iter().any(|route| route.export == export) {
                    return invalid(format!(
                        "BicDB application security contract lacks required host route `{export}`"
                    ));
                }
            }
        }
        for realtime in &self.realtime {
            for (suffix, streaming_request, streaming_response, sse, websocket) in [
                ("negotiate", false, false, false, false),
                ("poll", false, false, false, false),
                ("sse", false, true, true, false),
                ("ws", true, true, false, true),
            ] {
                let template = format!("{}/{suffix}", realtime.path);
                let route = self
                    .routes
                    .iter()
                    .find(|route| route.method == HttpMethod::Get && route.template == template)
                    .ok_or_else(|| {
                        ExtensionError::InvalidManifest(format!(
                            "BicDB application realtime surface `{}` lacks `{template}`",
                            realtime.name
                        ))
                    })?;
                if route.streaming_request != streaming_request
                    || route.streaming_response != streaming_response
                    || route.sse != sse
                    || route.websocket != websocket
                    || route.resource.is_some()
                    || route.operation.is_some()
                    || route.service_call.is_some()
                    || route.idempotency.is_some()
                    || route.cache.is_some()
                {
                    return invalid(format!(
                        "BicDB application realtime route `{}` has a weakened transport contract",
                        route.name
                    ));
                }
            }
        }
        unique_by("resource", self.resources.iter().map(|item| &item.name))?;
        for resource in &self.resources {
            resource.validate(self)?;
            if !resource.encrypted_fields.is_empty()
                && (!extension
                    .capabilities
                    .contains(&ExtensionCapability::SecretsCrypto)
                    || !self
                        .required_features
                        .contains(&ApplicationFeature::Secrets)
                    || !self.required_features.contains(&ApplicationFeature::Crypto))
            {
                return invalid(format!(
                    "resource `{}` encryption lacks signed secrets/crypto capabilities",
                    resource.name
                ));
            }
            if resource.audit.required
                && (!extension
                    .capabilities
                    .contains(&ExtensionCapability::Observability)
                    || !self
                        .required_features
                        .contains(&ApplicationFeature::Observability))
            {
                return invalid(format!(
                    "resource `{}` audit lacks the signed observability capability",
                    resource.name
                ));
            }
            for event in &resource.events {
                if !extension.permissions.publish_queues.contains(&event.queue) {
                    return invalid(format!(
                        "resource `{}` event queue `{}` is not declared for publish",
                        resource.name, event.queue
                    ));
                }
            }
        }
        for permission in self
            .relation_permissions
            .iter()
            .filter(|permission| permission.actions.contains(&DatabaseAction::VectorSearch))
        {
            if !self
                .required_features
                .contains(&ApplicationFeature::VectorSearch)
            {
                return invalid(format!(
                    "relation `{}` vector authority requires the vector_search feature",
                    permission.relation
                ));
            }
            if !self.resources.iter().any(|resource| {
                resource.relation.eq_ignore_ascii_case(&permission.relation)
                    && resource.vector_search.is_some()
            }) {
                return invalid(format!(
                    "relation `{}` vector authority lacks a signed vector-search contract",
                    permission.relation
                ));
            }
        }
        for permission in self
            .relation_permissions
            .iter()
            .filter(|permission| permission.actions.contains(&DatabaseAction::Spatial))
        {
            if !self
                .required_features
                .contains(&ApplicationFeature::Spatial)
                || !self.resources.iter().any(|resource| {
                    resource.relation.eq_ignore_ascii_case(&permission.relation)
                        && resource.fields.iter().any(|field| {
                            matches!(
                                field.field_type,
                                FieldType::Geometry {
                                    geometry_type: Some(_),
                                    ..
                                }
                            )
                        })
                })
            {
                return invalid(format!(
                    "relation `{}` spatial authority lacks the feature or an exactly typed geometry contract",
                    permission.relation
                ));
            }
        }
        for permission in self
            .relation_permissions
            .iter()
            .filter(|permission| permission.actions.contains(&DatabaseAction::Timeseries))
        {
            if !self
                .required_features
                .contains(&ApplicationFeature::Timeseries)
                || !self.resources.iter().any(|resource| {
                    resource.relation.eq_ignore_ascii_case(&permission.relation)
                        && resource.timeseries.is_some()
                })
            {
                return invalid(format!(
                    "relation `{}` timeseries authority lacks the feature or a signed timeseries contract",
                    permission.relation
                ));
            }
        }
        unique_by(
            "BicDB application invariant",
            self.invariants.iter().map(|item| &item.name),
        )?;
        if !self.invariants.is_empty()
            && !self
                .required_features
                .contains(&ApplicationFeature::CommitValidators)
        {
            return invalid("BicDB application invariants require the commit_validators feature");
        }
        for invariant in &self.invariants {
            invariant.validate(self)?;
        }
        let mut migration_versions = BTreeSet::new();
        for migration in &self.migrations {
            migration.validate()?;
            if !migration_versions.insert(migration.schema_version) {
                return invalid(format!(
                    "duplicate migration schema version {}",
                    migration.schema_version
                ));
            }
            for step in &migration.transformations {
                if let MigrationStep::BackfillField {
                    resource, field, ..
                } = step
                {
                    let Some(contract) = self
                        .resources
                        .iter()
                        .find(|candidate| candidate.name == *resource)
                    else {
                        return invalid(format!(
                            "migration backfill resource `{resource}` is absent"
                        ));
                    };
                    let Some(contract_field) = contract
                        .fields
                        .iter()
                        .find(|candidate| candidate.name == *field)
                    else {
                        return invalid(format!(
                            "migration backfill field `{resource}.{field}` is absent"
                        ));
                    };
                    if contract_field.nullable {
                        return invalid(format!(
                            "migration backfill field `{resource}.{field}` must be required"
                        ));
                    }
                }
            }
        }
        unique_by("worker", self.workers.iter().map(|item| &item.name))?;
        for worker in &self.workers {
            validate_identifier("worker", &worker.name)?;
            validate_identifier("worker export", &worker.export)?;
            validate_identifier("queue", &worker.queue)?;
            validate_identifier("consumer group", &worker.group)?;
            if worker.max_attempts == 0 || worker.visibility_timeout_ms == 0 {
                return invalid(format!("worker `{}` has invalid retry limits", worker.name));
            }
            if let Some(message) = &worker.message {
                validate_identifier("worker message", message)?;
            }
            if let Some(queue) = &worker.dead_letter_queue {
                validate_identifier("worker dead-letter queue", queue)?;
            }
            for argument in &worker.payload_arguments {
                validate_identifier("worker payload argument", argument)?;
            }
        }
        unique_by("schedule", self.schedules.iter().map(|item| &item.name))?;
        for schedule in &self.schedules {
            validate_identifier("schedule", &schedule.name)?;
            validate_identifier("schedule export", &schedule.export)?;
            validate_nonempty("schedule expression", &schedule.schedule, 256)?;
            validate_nonempty("schedule timezone", &schedule.timezone, 128)?;
            if schedule.max_concurrency == 0 || schedule.max_concurrency > 64 {
                return invalid(format!(
                    "schedule `{}` max_concurrency must be in 1..=64",
                    schedule.name
                ));
            }
            if schedule.catch_up_limit == 0 || schedule.catch_up_limit > 10_000 {
                return invalid(format!(
                    "schedule `{}` catch_up_limit must be in 1..=10000",
                    schedule.name
                ));
            }
            let durable_policy = !schedule.timezone.eq_ignore_ascii_case("UTC")
                || schedule.misfire != ScheduleMisfirePolicy::Skip
                || schedule.overlap != ScheduleOverlapPolicy::Skip
                || schedule.max_concurrency != default_schedule_concurrency()
                || schedule.catch_up_limit != default_schedule_catch_up_limit()
                || schedule.upgrade != ScheduleUpgradePolicy::Preserve;
            if durable_policy
                && !self
                    .required_features
                    .contains(&ApplicationFeature::DurableSchedules)
            {
                return invalid(format!(
                    "schedule `{}` uses durable policy without the DurableSchedules feature",
                    schedule.name
                ));
            }
        }
        if self.routes.iter().any(|route| route.cache.is_some())
            && (!extension
                .capabilities
                .contains(&ExtensionCapability::Database)
                || !extension
                    .capabilities
                    .contains(&ExtensionCapability::Transactions))
        {
            return invalid(
                "BicDB application route cache requires signed database and transactions capabilities",
            );
        }
        if self.routes.iter().any(|route| route.telemetry.is_some())
            && self.application_program.is_none()
        {
            return invalid("route telemetry requires a BicDB application behavior program");
        }
        if let Some(program) = &self.application_program {
            program.validate()?;
            let uses_typed_declared_sql_projections =
                application_program_uses_typed_declared_sql_projections(program);
            if uses_typed_declared_sql_projections
                != self
                    .required_features
                    .contains(&ApplicationFeature::TypedDeclaredSqlProjections)
            {
                return invalid(
                    "typed declared SQL result projections and their required feature must be declared together",
                );
            }
            if let Some(blob) = &program.blob {
                blob.validate(self)?;
                for feature in [ApplicationFeature::Blobs, ApplicationFeature::Observability] {
                    if !self.required_features.contains(&feature) {
                        return invalid(format!(
                            "BicDB application blob contract requires the {feature:?} feature"
                        ));
                    }
                }
                for capability in [
                    ExtensionCapability::Blobs,
                    ExtensionCapability::Observability,
                ] {
                    if !extension.capabilities.contains(&capability) {
                        return invalid(format!(
                            "BicDB application blob contract requires the {capability:?} capability"
                        ));
                    }
                }
            }
            if let Some(redis) = &program.redis {
                redis.validate(self)?;
                for feature in [
                    ApplicationFeature::Egress,
                    ApplicationFeature::Observability,
                ] {
                    if !self.required_features.contains(&feature) {
                        return invalid(format!(
                            "BicDB application Redis contract requires the {feature:?} feature"
                        ));
                    }
                }
                for capability in [
                    ExtensionCapability::NetworkEgress,
                    ExtensionCapability::Observability,
                ] {
                    if !extension.capabilities.contains(&capability) {
                        return invalid(format!(
                            "BicDB application Redis contract requires the {capability:?} capability"
                        ));
                    }
                }
            }
            if let Some(email) = &program.email {
                email.validate(self)?;
                for feature in [
                    ApplicationFeature::Egress,
                    ApplicationFeature::Observability,
                ] {
                    if !self.required_features.contains(&feature) {
                        return invalid(format!(
                            "BicDB application email contract requires the {feature:?} feature"
                        ));
                    }
                }
                for capability in [
                    ExtensionCapability::NetworkEgress,
                    ExtensionCapability::Observability,
                ] {
                    if !extension.capabilities.contains(&capability) {
                        return invalid(format!(
                            "BicDB application email contract requires the {capability:?} capability"
                        ));
                    }
                }
            }
            if let Some(grpc) = &program.grpc {
                grpc.validate()?;
                for feature in [
                    ApplicationFeature::Egress,
                    ApplicationFeature::Observability,
                ] {
                    if !self.required_features.contains(&feature) {
                        return invalid(format!(
                            "BicDB application gRPC contract requires the {feature:?} feature"
                        ));
                    }
                }
                for capability in [
                    ExtensionCapability::NetworkEgress,
                    ExtensionCapability::Observability,
                ] {
                    if !extension.capabilities.contains(&capability) {
                        return invalid(format!(
                            "BicDB application gRPC contract requires the {capability:?} capability"
                        ));
                    }
                }
            }
            if let Some(tokenizer) = &program.tokenizer {
                tokenizer.validate()?;
                for feature in [
                    ApplicationFeature::Tokenizers,
                    ApplicationFeature::Observability,
                ] {
                    if !self.required_features.contains(&feature) {
                        return invalid(format!(
                            "BicDB application tokenizer contract requires the {feature:?} feature"
                        ));
                    }
                }
                for capability in [
                    ExtensionCapability::AiInference,
                    ExtensionCapability::Observability,
                ] {
                    if !extension.capabilities.contains(&capability) {
                        return invalid(format!(
                            "BicDB application tokenizer contract requires the {capability:?} capability"
                        ));
                    }
                }
            }
            if let Some(embeddings) = &program.embeddings {
                embeddings.validate()?;
                for feature in [
                    ApplicationFeature::Embeddings,
                    ApplicationFeature::Observability,
                ] {
                    if !self.required_features.contains(&feature) {
                        return invalid(format!(
                            "BicDB application embeddings contract requires the {feature:?} feature"
                        ));
                    }
                }
                for capability in [
                    ExtensionCapability::AiInference,
                    ExtensionCapability::Observability,
                ] {
                    if !extension.capabilities.contains(&capability) {
                        return invalid(format!(
                            "BicDB application embeddings contract requires the {capability:?} capability"
                        ));
                    }
                }
            }
            if let Some(llm) = &program.llm {
                llm.validate()?;
                let tokenizer = program.tokenizer.as_ref().ok_or_else(|| {
                    ExtensionError::InvalidManifest(
                        "BicDB application LLM contract requires a tokenizer contract".to_string(),
                    )
                })?;
                for (name, client) in &llm.clients {
                    if !tokenizer.providers.contains_key(&client.tokenizer_provider) {
                        return invalid(format!(
                            "BicDB application LLM client `{name}` references an absent tokenizer provider"
                        ));
                    }
                }
                for feature in [
                    ApplicationFeature::Llm,
                    ApplicationFeature::Tokenizers,
                    ApplicationFeature::Observability,
                    ApplicationFeature::ReadCommitted,
                ] {
                    if !self.required_features.contains(&feature) {
                        return invalid(format!(
                            "BicDB application LLM contract requires the {feature:?} feature"
                        ));
                    }
                }
                for capability in [
                    ExtensionCapability::AiInference,
                    ExtensionCapability::NetworkEgress,
                    ExtensionCapability::Observability,
                    ExtensionCapability::Database,
                    ExtensionCapability::Transactions,
                ] {
                    if !extension.capabilities.contains(&capability) {
                        return invalid(format!(
                            "BicDB application LLM contract requires the {capability:?} capability"
                        ));
                    }
                }
                if llm.clients.values().any(|client| !client.tools.is_empty())
                    && !self
                        .required_features
                        .contains(&ApplicationFeature::LlmTools)
                {
                    return invalid("BicDB application LLM tools require the llm_tools feature");
                }
                if llm
                    .clients
                    .values()
                    .any(|client| client.methods.contains("stream_response"))
                    && (!self
                        .required_features
                        .contains(&ApplicationFeature::Streaming)
                        || !extension
                            .capabilities
                            .contains(&ExtensionCapability::Streaming))
                {
                    return invalid(
                        "BicDB application live LLM responses require the streaming feature and capability",
                    );
                }
                for (name, client) in &llm.clients {
                    let Some(stream) = &client.stream else {
                        continue;
                    };
                    for feature in [ApplicationFeature::Streaming, ApplicationFeature::Broker] {
                        if !self.required_features.contains(&feature) {
                            return invalid(format!(
                                "BicDB application LLM stream `{name}` requires the {feature:?} feature"
                            ));
                        }
                    }
                    for capability in [
                        ExtensionCapability::Streaming,
                        ExtensionCapability::QueueEvents,
                    ] {
                        if !extension.capabilities.contains(&capability) {
                            return invalid(format!(
                                "BicDB application LLM stream `{name}` requires the {capability:?} capability"
                            ));
                        }
                    }
                    if !self
                        .realtime
                        .iter()
                        .any(|surface| surface.path == stream.path && surface.queue == stream.queue)
                        || [
                            &stream.chunk_event,
                            &stream.tool_event,
                            &stream.completed_event,
                            &stream.failed_event,
                        ]
                        .into_iter()
                        .any(|event| {
                            program
                                .realtime_bindings
                                .get(event)
                                .is_none_or(|queues| !queues.contains(&stream.queue))
                        })
                    {
                        return invalid(format!(
                            "BicDB application LLM stream `{name}` lacks its exact realtime event surface"
                        ));
                    }
                }
            }
            if let Some(rag) = &program.rag {
                rag.validate()?;
                for feature in [
                    ApplicationFeature::Rag,
                    ApplicationFeature::Llm,
                    ApplicationFeature::VectorSearch,
                    ApplicationFeature::ReadCommitted,
                    ApplicationFeature::Observability,
                ] {
                    if !self.required_features.contains(&feature) {
                        return invalid(format!(
                            "BicDB application RAG contract requires the {feature:?} feature"
                        ));
                    }
                }
                if rag
                    .pipelines
                    .values()
                    .any(|pipeline| pipeline.embedding_provider.is_some())
                    && !self
                        .required_features
                        .contains(&ApplicationFeature::Embeddings)
                {
                    return invalid(
                        "BicDB application RAG provider embeddings require the Embeddings feature",
                    );
                }
                for capability in [
                    ExtensionCapability::AiInference,
                    ExtensionCapability::NetworkEgress,
                    ExtensionCapability::Database,
                    ExtensionCapability::Transactions,
                    ExtensionCapability::Observability,
                ] {
                    if !extension.capabilities.contains(&capability) {
                        return invalid(format!(
                            "BicDB application RAG contract requires the {capability:?} capability"
                        ));
                    }
                }
                for (name, pipeline) in &rag.pipelines {
                    let resource = self
                        .resources
                        .iter()
                        .find(|resource| resource.name == pipeline.retriever_model)
                        .ok_or_else(|| {
                            ExtensionError::InvalidManifest(format!(
                                "BicDB application RAG pipeline `{name}` references absent resource"
                            ))
                        })?;
                    if resource.vector_search.is_none()
                        || self
                            .permission_for(&resource.relation)
                            .is_none_or(|permission| {
                                !permission.actions.contains(&DatabaseAction::VectorSearch)
                            })
                    {
                        return invalid(format!(
                            "BicDB application RAG pipeline `{name}` lacks exact native vector authority"
                        ));
                    }
                }
            }
            if let Some(agents) = &program.agents {
                agents.validate()?;
                for feature in [
                    ApplicationFeature::Agents,
                    ApplicationFeature::Llm,
                    ApplicationFeature::Jobs,
                    ApplicationFeature::ReadCommitted,
                    ApplicationFeature::Observability,
                ] {
                    if !self.required_features.contains(&feature) {
                        return invalid(format!(
                            "BicDB application agent contract requires the {feature:?} feature"
                        ));
                    }
                }
                for capability in [
                    ExtensionCapability::AiInference,
                    ExtensionCapability::NetworkEgress,
                    ExtensionCapability::Database,
                    ExtensionCapability::Transactions,
                    ExtensionCapability::QueueEvents,
                    ExtensionCapability::Observability,
                ] {
                    if !extension.capabilities.contains(&capability) {
                        return invalid(format!(
                            "BicDB application agent contract requires the {capability:?} capability"
                        ));
                    }
                }
                for name in agents.agents.keys() {
                    if !program.workflow_bindings.contains_key(name) {
                        return invalid(format!(
                            "BicDB application agent `{name}` lacks a durable workflow binding"
                        ));
                    }
                }
            }
            if let Some(evaluations) = &program.evaluations {
                evaluations.validate()?;
                for feature in [
                    ApplicationFeature::Evaluations,
                    ApplicationFeature::Observability,
                ] {
                    if !self.required_features.contains(&feature) {
                        return invalid(format!(
                            "BicDB application evaluation contract requires the {feature:?} feature"
                        ));
                    }
                }
                if !extension
                    .capabilities
                    .contains(&ExtensionCapability::Observability)
                {
                    return invalid(
                        "BicDB application evaluation contract requires the Observability capability",
                    );
                }
            }
            if let Some(tests) = &program.tests {
                tests.validate()?;
                for feature in [ApplicationFeature::Tests, ApplicationFeature::Observability] {
                    if !self.required_features.contains(&feature) {
                        return invalid(format!(
                            "BicDB application test contract requires the {feature:?} feature"
                        ));
                    }
                }
                if !extension
                    .capabilities
                    .contains(&ExtensionCapability::Observability)
                {
                    return invalid(
                        "BicDB application test contract requires the Observability capability",
                    );
                }
            }
            if let Some(observability) = &program.observability {
                observability.validate(program)?;
                if !self
                    .required_features
                    .contains(&ApplicationFeature::Observability)
                    || !extension
                        .capabilities
                        .contains(&ExtensionCapability::Observability)
                {
                    return invalid(
                        "BicDB application observability contract requires the Observability feature and capability",
                    );
                }
                if observability.durable_audit
                    && (!extension
                        .capabilities
                        .contains(&ExtensionCapability::Database)
                        || !extension
                            .capabilities
                            .contains(&ExtensionCapability::Transactions)
                        || !self
                            .required_features
                            .contains(&ApplicationFeature::ReadCommitted))
                {
                    return invalid(
                        "BicDB application durable audit requires database, transactions, and read_committed authority",
                    );
                }
            }
            for route in &self.routes {
                if let Some(telemetry) = &route.telemetry {
                    telemetry.sampling.validate()?;
                    if program.observability.is_none() {
                        return invalid(format!(
                            "route `{}` telemetry lacks a BicDB application observability contract",
                            route.name
                        ));
                    }
                }
            }
            if let Some(security) = &program.security {
                security.validate(self)?;
                let mut required = BTreeSet::new();
                let stateful = [
                    "auth.register",
                    "auth.login",
                    "auth.issue_tokens",
                    "auth.magic_link_verify",
                    "auth.oauth_authorize",
                    "auth.oauth_callback",
                ];
                if security
                    .helpers
                    .iter()
                    .any(|helper| stateful.contains(&helper.as_str()))
                {
                    required.extend([
                        ExtensionCapability::Database,
                        ExtensionCapability::Transactions,
                        ExtensionCapability::Observability,
                    ]);
                }
                let clocked = [
                    "auth.issue_tokens",
                    "auth.totp_code",
                    "auth.totp_verify",
                    "auth.magic_link_issue",
                    "auth.magic_link_verify",
                    "auth.oauth_authorize",
                    "auth.oauth_callback",
                ];
                if security
                    .helpers
                    .iter()
                    .any(|helper| clocked.contains(&helper.as_str()))
                {
                    required.insert(ExtensionCapability::Clock);
                }
                let randomized = [
                    "auth.register",
                    "auth.issue_tokens",
                    "auth.password_hash",
                    "auth.totp_secret",
                    "auth.magic_link_issue",
                    "auth.oauth_authorize",
                ];
                if security
                    .helpers
                    .iter()
                    .any(|helper| randomized.contains(&helper.as_str()))
                {
                    required.insert(ExtensionCapability::Random);
                }
                let secret_backed = [
                    "auth.issue_tokens",
                    "auth.magic_link_issue",
                    "auth.magic_link_verify",
                ];
                if security
                    .helpers
                    .iter()
                    .any(|helper| secret_backed.contains(&helper.as_str()))
                {
                    required.insert(ExtensionCapability::SecretsCrypto);
                }
                for capability in required {
                    if !extension.capabilities.contains(&capability) {
                        return invalid(format!(
                            "BicDB application security contract requires the {capability:?} capability"
                        ));
                    }
                }
            }
            let transaction_features = application_program_transaction_features(program);
            for feature in &transaction_features {
                if !self.required_features.contains(feature) {
                    return invalid(format!(
                        "BicDB application behavior transaction requires the {feature:?} feature"
                    ));
                }
            }
            if !transaction_features.is_empty()
                && !extension
                    .capabilities
                    .contains(&ExtensionCapability::Transactions)
            {
                return invalid(
                    "BicDB application behavior transactions require the signed transactions capability",
                );
            }
            if application_program_uses_builtin(
                program,
                &[
                    "cache.get_as",
                    "cache.set",
                    "cache.delete",
                    "cache.exists",
                    "memoize.call",
                ],
            ) && (!extension
                .capabilities
                .contains(&ExtensionCapability::Database)
                || !extension
                    .capabilities
                    .contains(&ExtensionCapability::Transactions))
            {
                return invalid(
                    "BicDB application runtime cache requires signed database and transactions capabilities",
                );
            }
            if program.max_call_depth > self.max_call_depth {
                return invalid(format!(
                    "BicDB application behavior max_call_depth {} exceeds application max_call_depth {}",
                    program.max_call_depth, self.max_call_depth
                ));
            }
            for route in self.routes.iter().filter(|route| {
                route.resource.is_none()
                    && route.service_call.is_none()
                    && !is_carrier_realtime_route(&self.realtime, route)
                    && !route.export.starts_with("__carrier_security_")
            }) {
                if !program.callables.contains_key(&route.export) {
                    return invalid(format!(
                        "route `{}` references absent BicDB application program callable `{}`",
                        route.name, route.export
                    ));
                }
            }
            for (alias, binding) in &program.service_bindings {
                let import = self
                    .service_imports
                    .iter()
                    .find(|import| {
                        import.name == binding.dependency && import.service == binding.service
                    })
                    .ok_or_else(|| {
                        ExtensionError::InvalidManifest(format!(
                            "BicDB application service binding `{alias}` references undeclared import `{}/{}`",
                            binding.dependency, binding.service
                        ))
                    })?;
                if binding.propagate_transaction && !import.propagate_transaction {
                    return invalid(format!(
                        "BicDB application service binding `{alias}` requests undeclared transaction propagation"
                    ));
                }
            }
            for (alias, binding) in &program.client_bindings {
                let declaration = self
                    .egress
                    .iter()
                    .find(|policy| policy.name == binding.policy)
                    .ok_or_else(|| {
                        ExtensionError::InvalidManifest(format!(
                            "BicDB application client binding `{alias}` references undeclared egress policy `{}`",
                            binding.policy
                        ))
                    })?;
                if declaration.provider.is_some() != binding.base_url.is_empty() {
                    return invalid(format!(
                        "BicDB application client binding `{alias}` endpoint mode differs from egress policy `{}`",
                        declaration.name
                    ));
                }
                if declaration
                    .required_provider_headers
                    .iter()
                    .any(|required| {
                        binding
                            .headers
                            .keys()
                            .any(|literal| literal.eq_ignore_ascii_case(required))
                    })
                {
                    return invalid(format!(
                        "BicDB application client binding `{alias}` duplicates an operator provider header"
                    ));
                }
            }
            for (field_reference, secret_name) in &program.secret_bindings {
                if !self
                    .secrets
                    .iter()
                    .any(|secret| secret.name == *secret_name)
                {
                    return invalid(format!(
                        "BicDB application encrypted field `{field_reference}` references undeclared secret `{secret_name}`"
                    ));
                }
            }
            for (event, queue) in &program.event_bindings {
                if !extension.permissions.publish_queues.contains(queue)
                    && !extension.permissions.consume_queues.contains(queue)
                {
                    return invalid(format!(
                        "BicDB application event `{event}` queue `{queue}` has no signed publish or consume permission"
                    ));
                }
            }
            for (event, queues) in &program.realtime_bindings {
                for queue in queues {
                    if !extension.permissions.publish_queues.contains(queue) {
                        return invalid(format!(
                            "BicDB application realtime event `{event}` queue `{queue}` has no signed publish permission"
                        ));
                    }
                }
            }
            for binding in &program.mutation_bindings {
                if !self
                    .resources
                    .iter()
                    .any(|resource| resource.name == binding.resource)
                {
                    return invalid(format!(
                        "BicDB application mutation binding references absent resource `{}`",
                        binding.resource
                    ));
                }
                if let Some(queue) = &binding.queue {
                    if !extension.permissions.publish_queues.contains(queue) {
                        return invalid(format!(
                            "BicDB application post-commit trigger queue `{queue}` is not declared for publish"
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn permission_for(&self, relation: &str) -> Option<&RelationPermission> {
        self.relation_permissions
            .iter()
            .find(|permission| permission.relation.eq_ignore_ascii_case(relation))
    }
}

fn is_carrier_realtime_route(contracts: &[ApplicationRealtimeContractV1], route: &RouteV2) -> bool {
    route.method == HttpMethod::Get
        && contracts.iter().any(|contract| {
            ["negotiate", "poll", "sse", "ws"]
                .into_iter()
                .any(|suffix| route.template == format!("{}/{suffix}", contract.path))
        })
}

const CARRIER_OBSERVABILITY_HELPERS: [&str; 8] = [
    "audit.record",
    "logs.debug",
    "logs.error",
    "logs.info",
    "logs.warn",
    "metrics.counter.increment",
    "metrics.gauge.record",
    "trace.annotate",
];

fn carrier_observability_call_name<'a>(
    arguments: &'a [ApplicationArgumentV1],
    named: &str,
) -> Option<&'a ApplicationExpressionV1> {
    arguments
        .iter()
        .find(|argument| argument.name.as_deref() == Some(named))
        .or_else(|| arguments.first())
        .map(|argument| &argument.value)
}

fn carrier_literal_string(expression: &ApplicationExpressionV1) -> Option<&str> {
    match expression {
        ApplicationExpressionV1::Literal {
            value: JsonValue::String(value),
        } => Some(value),
        _ => None,
    }
}

impl ApplicationSamplingV1 {
    fn validate(self) -> Result<()> {
        match self {
            Self::Ratio { millionths } | Self::ParentBasedRatio { millionths }
                if millionths > 1_000_000 =>
            {
                invalid("BicDB application trace sampling ratio exceeds one million millionths")
            }
            _ => Ok(()),
        }
    }
}

impl ApplicationObservabilityContractV1 {
    fn validate(&self, program: &ApplicationProgramV1) -> Result<()> {
        if self.version != 1 {
            return invalid("BicDB application observability contract requires version 1");
        }
        match (self.provider.as_str(), self.protocol) {
            ("bicdb", ApplicationTelemetryProtocolV1::Host)
            | (
                "opentelemetry",
                ApplicationTelemetryProtocolV1::OtlpHttp | ApplicationTelemetryProtocolV1::OtlpGrpc,
            ) => {}
            _ => {
                return invalid(
                    "BicDB application observability provider/protocol binding is not supported",
                );
            }
        }
        validate_nonempty(
            "BicDB application observability service name",
            &self.service_name,
            256,
        )?;
        self.sampling.validate()?;
        if self.max_field_depth == 0 || self.max_field_depth > 64 {
            return invalid("BicDB application observability max_field_depth must be 1..=64");
        }
        if self.max_field_bytes == 0 || self.max_field_bytes > 1024 * 1024 {
            return invalid("BicDB application observability max_field_bytes must be 1..=1048576");
        }
        if !self.propagate_w3c {
            return invalid(
                "BicDB application observability requires W3C trace-context propagation",
            );
        }
        for helper in &self.helpers {
            if !CARRIER_OBSERVABILITY_HELPERS.contains(&helper.as_str()) {
                return invalid(format!(
                    "BicDB application observability contract contains unknown helper `{helper}`"
                ));
            }
        }
        for key in &self.redacted_keys {
            validate_nonempty("BicDB application observability redacted key", key, 256)?;
        }
        for name in &self.metric_names {
            validate_nonempty("BicDB application metric name", name, 256)?;
        }
        for action in &self.audit_actions {
            validate_nonempty("BicDB application audit action", action, 256)?;
        }
        let used = CARRIER_OBSERVABILITY_HELPERS
            .into_iter()
            .filter(|target| application_program_uses_builtin(program, &[*target]))
            .map(str::to_string)
            .collect::<BTreeSet<_>>();
        if self.helpers != used {
            return invalid(format!(
                "BicDB application observability helper authority does not exactly match the program (signed={:?}, used={used:?})",
                self.helpers
            ));
        }
        if self.durable_audit != self.helpers.contains("audit.record") {
            return invalid(
                "BicDB application durable-audit authority must exactly match audit.record usage",
            );
        }
        let metric_targets = ["metrics.counter.increment", "metrics.gauge.record"];
        let has_dynamic_metric =
            application_program_call_matches(program, &|kind, target, _, arguments, _| {
                *kind == ApplicationCallKindV1::Builtin
                    && metric_targets.contains(&target)
                    && carrier_observability_call_name(arguments, "name")
                        .and_then(carrier_literal_string)
                        .is_none()
            });
        if self.dynamic_metric_names != has_dynamic_metric {
            return invalid(
                "BicDB application dynamic metric-name authority does not exactly match the program",
            );
        }
        if application_program_call_matches(program, &|kind, target, _, arguments, _| {
            *kind == ApplicationCallKindV1::Builtin
                && metric_targets.contains(&target)
                && carrier_observability_call_name(arguments, "name")
                    .and_then(carrier_literal_string)
                    .is_some_and(|name| !self.metric_names.contains(name))
        }) || self.metric_names.iter().any(|name| {
            !application_program_call_matches(program, &|kind, target, _, arguments, _| {
                *kind == ApplicationCallKindV1::Builtin
                    && metric_targets.contains(&target)
                    && carrier_observability_call_name(arguments, "name")
                        .and_then(carrier_literal_string)
                        == Some(name.as_str())
            })
        }) {
            return invalid(
                "BicDB application metric-name authority does not exactly match the program",
            );
        }
        let has_dynamic_audit =
            application_program_call_matches(program, &|kind, target, _, arguments, _| {
                *kind == ApplicationCallKindV1::Builtin
                    && target == "audit.record"
                    && carrier_observability_call_name(arguments, "action")
                        .and_then(carrier_literal_string)
                        .is_none()
            });
        if self.dynamic_audit_actions != has_dynamic_audit {
            return invalid(
                "BicDB application dynamic audit-action authority does not exactly match the program",
            );
        }
        if application_program_call_matches(program, &|kind, target, _, arguments, _| {
            *kind == ApplicationCallKindV1::Builtin
                && target == "audit.record"
                && carrier_observability_call_name(arguments, "action")
                    .and_then(carrier_literal_string)
                    .is_some_and(|action| !self.audit_actions.contains(action))
        }) || self.audit_actions.iter().any(|action| {
            !application_program_call_matches(program, &|kind, target, _, arguments, _| {
                *kind == ApplicationCallKindV1::Builtin
                    && target == "audit.record"
                    && carrier_observability_call_name(arguments, "action")
                        .and_then(carrier_literal_string)
                        == Some(action.as_str())
            })
        }) {
            return invalid(
                "BicDB application audit-action authority does not exactly match the program",
            );
        }
        Ok(())
    }
}

impl ApplicationProgramV1 {
    fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return invalid(format!(
                "unsupported BicDB application behavior program version {}",
                self.version
            ));
        }
        if self.max_steps == 0 || self.max_steps > 10_000_000 {
            return invalid("BicDB application behavior max_steps must be 1..=10000000");
        }
        if self.max_call_depth == 0 || self.max_call_depth > MAX_CALL_DEPTH {
            return invalid(format!(
                "BicDB application behavior max_call_depth must be 1..={MAX_CALL_DEPTH}"
            ));
        }
        if self.callables.len() > 16_384 {
            return invalid("BicDB application behavior program has too many callables");
        }
        for (name, callable) in &self.callables {
            validate_identifier("BicDB application callable", name)?;
            if callable.parameters.len() > 1_024 {
                return invalid(format!(
                    "BicDB application callable `{name}` has too many parameters"
                ));
            }
            let mut parameters = BTreeSet::new();
            for parameter in &callable.parameters {
                validate_identifier("BicDB application parameter", parameter)?;
                if !parameters.insert(parameter) {
                    return invalid(format!(
                        "BicDB application callable `{name}` repeats parameter `{parameter}`"
                    ));
                }
            }
            if callable.body.len() > self.max_steps as usize {
                return invalid(format!(
                    "BicDB application callable `{name}` exceeds its program step limit"
                ));
            }
            for statement in &callable.body {
                validate_carrier_statement(statement)?;
            }
        }
        for (alias, binding) in &self.service_bindings {
            validate_identifier("BicDB application service alias", alias)?;
            validate_identifier("BicDB application service dependency", &binding.dependency)?;
            validate_identifier("BicDB application service", &binding.service)?;
            validate_identifier("BicDB application service method", &binding.method)?;
            let mut parameters = BTreeSet::new();
            for parameter in &binding.parameters {
                validate_identifier("BicDB application service parameter", parameter)?;
                if !parameters.insert(parameter) {
                    return invalid(format!(
                        "BicDB application service binding `{alias}` repeats parameter `{parameter}`"
                    ));
                }
            }
        }
        for (alias, binding) in &self.client_bindings {
            validate_identifier("BicDB application client alias", alias)?;
            validate_identifier("BicDB application client egress policy", &binding.policy)?;
            if binding.base_url.len() > 8 * 1024 {
                return invalid(format!(
                    "BicDB application client binding `{alias}` base URL exceeds 8192 bytes"
                ));
            }
            if binding.timeout_ms == 0 {
                return invalid(format!(
                    "BicDB application client binding `{alias}` requires a positive timeout"
                ));
            }
            let mut header_names = BTreeSet::new();
            for (name, value) in &binding.headers {
                validate_http_header_name("BicDB application client header", name)?;
                validate_nonempty("BicDB application client header value", value, 8 * 1024)?;
                if !header_names.insert(name.to_ascii_lowercase()) {
                    return invalid(format!(
                        "BicDB application client binding `{alias}` repeats header `{name}` case-insensitively"
                    ));
                }
                if [
                    "host",
                    "content-length",
                    "connection",
                    "traceparent",
                    "tracestate",
                    "x-carrier-trace-id",
                    "x-correlation-id",
                    "x-causation-id",
                ]
                .iter()
                .any(|reserved| name.eq_ignore_ascii_case(reserved))
                {
                    return invalid(format!(
                        "BicDB application client binding `{alias}` sets host-controlled header `{name}`"
                    ));
                }
            }
        }
        for (name, definition) in &self.flags {
            validate_identifier("BicDB application feature flag", name)?;
            if definition.rules.len() > 1_024 {
                return invalid(format!(
                    "BicDB application feature flag `{name}` has too many rules"
                ));
            }
            for rule in &definition.rules {
                match rule {
                    ApplicationFlagRuleV1::TenantIn { tenants, .. } => {
                        if tenants.len() > 16_384 {
                            return invalid(format!(
                                "BicDB application feature flag `{name}` has too many tenant targets"
                            ));
                        }
                        for tenant in tenants {
                            validate_nonempty(
                                "BicDB application feature-flag tenant",
                                tenant,
                                512,
                            )?;
                        }
                    }
                    ApplicationFlagRuleV1::Percentage { percent, .. } if *percent > 100 => {
                        return invalid(format!(
                            "BicDB application feature flag `{name}` percentage exceeds 100"
                        ));
                    }
                    ApplicationFlagRuleV1::Percentage { .. } => {}
                }
            }
        }
        for (job, queue) in &self.job_bindings {
            validate_identifier("BicDB application job", job)?;
            validate_identifier("BicDB application job queue", queue)?;
            if !self.callables.contains_key(job) {
                return invalid(format!(
                    "BicDB application job binding references absent callable `{job}`"
                ));
            }
        }
        for (field_reference, secret_name) in &self.secret_bindings {
            validate_nonempty(
                "BicDB application encrypted field reference",
                field_reference,
                512,
            )?;
            validate_nonempty("BicDB application encrypted field secret", secret_name, 512)?;
        }
        if let Some(security) = &self.security {
            if security.version != 1 {
                return invalid("BicDB application security contract requires version 1");
            }
        }
        if let Some(blob) = &self.blob {
            if blob.version != 1 {
                return invalid("BicDB application blob contract requires version 1");
            }
        }
        if let Some(redis) = &self.redis {
            if redis.version != 1 {
                return invalid("BicDB application Redis contract requires version 1");
            }
        }
        if let Some(email) = &self.email {
            if email.version != 1 {
                return invalid("BicDB application email contract requires version 1");
            }
        }
        if let Some(grpc) = &self.grpc {
            grpc.validate()?;
            for (client, contract) in &grpc.clients {
                for method in contract.methods.keys() {
                    if !application_program_call_matches(self, &|kind, target, candidate, _, _| {
                        *kind == ApplicationCallKindV1::Client
                            && target == client
                            && candidate == Some(method.as_str())
                    }) {
                        return invalid(format!(
                            "BicDB application gRPC contract grants unused method `{client}.{method}`"
                        ));
                    }
                }
            }
        }
        if let Some(tokenizer) = &self.tokenizer {
            tokenizer.validate()?;
            for provider in tokenizer.providers.keys() {
                let used_by_llm = self.llm.as_ref().is_some_and(|llm| {
                    llm.clients
                        .values()
                        .any(|client| client.tokenizer_provider == *provider)
                });
                let used_directly = provider == "default"
                    && application_program_call_matches(self, &|kind, target, _, _, _| {
                        *kind == ApplicationCallKindV1::Builtin && target == "tokenizer.count"
                    });
                if !used_by_llm && !used_directly {
                    return invalid(format!(
                        "BicDB application tokenizer contract grants unused provider `{provider}`"
                    ));
                }
            }
        }
        if let Some(embeddings) = &self.embeddings {
            embeddings.validate()?;
            for provider in embeddings.providers.keys() {
                let used_directly = provider == "default"
                    && application_program_call_matches(self, &|kind, target, _, _, _| {
                        *kind == ApplicationCallKindV1::Builtin && target == "embeddings.embed"
                    });
                let used_by_rag = self.rag.as_ref().is_some_and(|rag| {
                    rag.pipelines.values().any(|pipeline| {
                        pipeline.embedding_provider.as_deref() == Some(provider.as_str())
                    })
                });
                if !used_directly && !used_by_rag {
                    return invalid(format!(
                        "BicDB application embeddings contract grants unused provider `{provider}`"
                    ));
                }
            }
        }
        if let Some(llm) = &self.llm {
            llm.validate()?;
            let mut used_methods = BTreeSet::<(String, String)>::new();
            let mut used_outputs = BTreeSet::<(String, String)>::new();
            for (client, contract) in &llm.clients {
                for method in &contract.methods {
                    if application_program_call_matches(self, &|kind, target, candidate, _, _| {
                        *kind == ApplicationCallKindV1::Llm
                            && target == client
                            && candidate == Some(method.as_str())
                    }) {
                        used_methods.insert((client.clone(), method.clone()));
                    }
                }
                for output in contract.structured_outputs.keys() {
                    if application_program_call_matches(
                        self,
                        &|kind, target, method, arguments, _| {
                            *kind == ApplicationCallKindV1::Llm
                                && target == client
                                && method == Some("respond_as")
                                && arguments.first().is_some_and(|argument| {
                                    matches!(
                                        &argument.value,
                                        ApplicationExpressionV1::Literal { value }
                                            if value.as_str() == Some(output.as_str())
                                    )
                                })
                        },
                    ) {
                        used_outputs.insert((client.clone(), output.clone()));
                    }
                }
            }
            if let Some(rag) = &self.rag {
                for pipeline in rag.pipelines.values() {
                    used_methods.insert((pipeline.llm_client.clone(), "respond".to_string()));
                }
            }
            if let Some(agents) = &self.agents {
                for agent in agents.agents.values() {
                    let method = if let Some(output) = &agent.structured_output {
                        used_outputs.insert((agent.llm_client.clone(), output.clone()));
                        "respond_as"
                    } else {
                        "respond"
                    };
                    used_methods.insert((agent.llm_client.clone(), method.to_string()));
                }
            }
            loop {
                let mut additions = Vec::new();
                for (client, method) in &used_methods {
                    let Some(contract) = llm.clients.get(client) else {
                        continue;
                    };
                    let mut targets = Vec::new();
                    if let Some(routing) = &contract.routing {
                        targets.push(routing.primary.clone());
                        targets.extend(routing.fallbacks.clone());
                    }
                    if let Some(ApplicationLlmBudgetV1 {
                        over_budget: ApplicationLlmOverBudgetV1::Downgrade { client },
                        ..
                    }) = &contract.budget
                    {
                        targets.push(client.clone());
                    }
                    for target in targets {
                        if !used_methods.contains(&(target.clone(), method.clone())) {
                            additions.push((target, method.clone()));
                        }
                    }
                }
                if additions.is_empty() {
                    break;
                }
                used_methods.extend(additions);
            }
            loop {
                let mut additions = Vec::new();
                for (client, output) in &used_outputs {
                    let Some(contract) = llm.clients.get(client) else {
                        continue;
                    };
                    let mut targets = Vec::new();
                    if let Some(routing) = &contract.routing {
                        targets.push(routing.primary.clone());
                        targets.extend(routing.fallbacks.clone());
                    }
                    if let Some(ApplicationLlmBudgetV1 {
                        over_budget: ApplicationLlmOverBudgetV1::Downgrade { client },
                        ..
                    }) = &contract.budget
                    {
                        targets.push(client.clone());
                    }
                    for target in targets {
                        if !used_outputs.contains(&(target.clone(), output.clone())) {
                            additions.push((target, output.clone()));
                        }
                    }
                }
                if additions.is_empty() {
                    break;
                }
                used_outputs.extend(additions);
            }
            for (client, contract) in &llm.clients {
                for method in &contract.methods {
                    if !used_methods.contains(&(client.clone(), method.clone())) {
                        return invalid(format!(
                            "BicDB application LLM contract grants unused method `{client}.{method}`"
                        ));
                    }
                }
                for output in contract.structured_outputs.keys() {
                    if !used_outputs.contains(&(client.clone(), output.clone())) {
                        return invalid(format!(
                            "BicDB application LLM contract grants unused structured output `{client}.{output}`"
                        ));
                    }
                }
                for (tool_name, tool) in &contract.tools {
                    let callable = self.callables.get(&tool.callable).ok_or_else(|| {
                        ExtensionError::InvalidManifest(format!(
                            "BicDB application LLM tool `{client}.{tool_name}` references absent callable `{}`",
                            tool.callable
                        ))
                    })?;
                    let tool_parameters = tool
                        .parameters
                        .iter()
                        .map(|parameter| parameter.name.as_str())
                        .collect::<Vec<_>>();
                    let callable_parameters = callable
                        .parameters
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>();
                    if tool_parameters != callable_parameters {
                        return invalid(format!(
                            "BicDB application LLM tool `{client}.{tool_name}` does not match callable `{}` parameters",
                            tool.callable
                        ));
                    }
                }
                let mut routed_targets = Vec::new();
                if let Some(routing) = &contract.routing {
                    routed_targets.push(routing.primary.as_str());
                    routed_targets.extend(routing.fallbacks.iter().map(String::as_str));
                }
                if let Some(ApplicationLlmBudgetV1 {
                    over_budget: ApplicationLlmOverBudgetV1::Downgrade { client: downgrade },
                    ..
                }) = &contract.budget
                {
                    routed_targets.push(downgrade);
                }
                for target in routed_targets {
                    let target_contract = llm.clients.get(target);
                    if target == client
                        || target_contract.is_none_or(|target_contract| {
                            target_contract.routing.is_some()
                                || !contract
                                    .methods
                                    .iter()
                                    .all(|method| target_contract.methods.contains(method))
                                || !contract.tools.iter().all(|(name, tool)| {
                                    target_contract.tools.get(name) == Some(tool)
                                })
                                || !contract.structured_outputs.iter().all(|(name, output)| {
                                    target_contract.structured_outputs.get(name) == Some(output)
                                })
                        })
                    {
                        return invalid(format!(
                            "BicDB application LLM client `{client}` routes to incompatible client `{target}`"
                        ));
                    }
                }
            }
        }
        if application_program_call_matches(self, &|kind, target, method, _, _| {
            *kind == ApplicationCallKindV1::Llm
                && self
                    .llm
                    .as_ref()
                    .and_then(|llm| llm.clients.get(target))
                    .zip(method)
                    .is_none_or(|(client, method)| !client.methods.contains(method))
        }) {
            return invalid("BicDB application LLM call is outside its exact signed authority");
        }
        if application_program_call_matches(self, &|kind, target, method, _, _| {
            *kind == ApplicationCallKindV1::Llm
                && method == Some("stream")
                && self
                    .llm
                    .as_ref()
                    .and_then(|llm| llm.clients.get(target))
                    .is_none_or(|client| client.stream.is_none())
        }) {
            return invalid(
                "BicDB application LLM stream call lacks exact signed transport authority",
            );
        }
        if application_program_call_matches(self, &|kind, target, _, _, _| {
            *kind == ApplicationCallKindV1::Builtin
                && ((target == "tokenizer.count"
                    && self
                        .tokenizer
                        .as_ref()
                        .is_none_or(|contract| !contract.providers.contains_key("default")))
                    || (target == "embeddings.embed"
                        && self
                            .embeddings
                            .as_ref()
                            .is_none_or(|contract| !contract.providers.contains_key("default"))))
        }) {
            return invalid(
                "BicDB application AI builtin call is outside its exact signed authority",
            );
        }
        if let Some(rag) = &self.rag {
            rag.validate()?;
            let llm = self.llm.as_ref().ok_or_else(|| {
                ExtensionError::InvalidManifest(
                    "BicDB application RAG contract requires an LLM contract".to_string(),
                )
            })?;
            for (name, pipeline) in &rag.pipelines {
                let embedding_valid = match (
                    pipeline.embedding_provider.as_ref(),
                    pipeline.embedding_callable.as_ref(),
                ) {
                    (Some(provider), None) => self
                        .embeddings
                        .as_ref()
                        .and_then(|embeddings| embeddings.providers.get(provider))
                        .is_some_and(|embedding| embedding.dimensions == pipeline.dimensions),
                    (None, Some(callable)) => self.callables.contains_key(callable),
                    _ => false,
                };
                if !embedding_valid
                    || !llm.clients.contains_key(&pipeline.llm_client)
                    || !application_program_call_matches(self, &|kind, target, method, _, _| {
                        *kind == ApplicationCallKindV1::Rag
                            && target == name
                            && matches!(method, Some("respond" | "stream_response"))
                    })
                {
                    return invalid(format!(
                        "BicDB application RAG pipeline `{name}` has inconsistent or unused exact authority"
                    ));
                }
            }
        }
        if application_program_call_matches(self, &|kind, target, method, _, _| {
            *kind == ApplicationCallKindV1::Rag
                && (!matches!(method, Some("respond" | "stream_response"))
                    || self
                        .rag
                        .as_ref()
                        .is_none_or(|contract| !contract.pipelines.contains_key(target)))
        }) {
            return invalid("BicDB application RAG call is outside its exact signed authority");
        }
        if let Some(agents) = &self.agents {
            agents.validate()?;
            let llm = self.llm.as_ref().ok_or_else(|| {
                ExtensionError::InvalidManifest(
                    "BicDB application agent contract requires an LLM contract".to_string(),
                )
            })?;
            for (name, agent) in &agents.agents {
                let client = llm.clients.get(&agent.llm_client).ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "BicDB application agent `{name}` references absent LLM client"
                    ))
                })?;
                if !self.callables.contains_key(&agent.callable)
                    || !agent
                        .tools
                        .iter()
                        .all(|tool| client.tools.contains_key(tool))
                    || agent.structured_output.as_ref().is_some_and(|output| {
                        client.structured_outputs.get(output) != Some(&agent.output)
                    })
                    || !application_program_call_matches(self, &|kind, target, method, _, _| {
                        *kind == ApplicationCallKindV1::Agent
                            && target == name
                            && method == Some("run")
                    })
                {
                    return invalid(format!(
                        "BicDB application agent `{name}` has inconsistent or unused exact authority"
                    ));
                }
            }
        }
        if application_program_call_matches(self, &|kind, target, method, _, _| {
            *kind == ApplicationCallKindV1::Agent
                && (method != Some("run")
                    || self
                        .agents
                        .as_ref()
                        .is_none_or(|contract| !contract.agents.contains_key(target)))
        }) {
            return invalid("BicDB application agent call is outside its exact signed authority");
        }
        if let Some(evaluations) = &self.evaluations {
            evaluations.validate()?;
            for (name, evaluation) in &evaluations.evaluations {
                for callable in [&evaluation.case_callable, &evaluation.require_callable] {
                    if !self.callables.contains_key(callable) {
                        return invalid(format!(
                            "BicDB application evaluation `{name}` references absent callable `{callable}`"
                        ));
                    }
                }
            }
        }
        if let Some(tests) = &self.tests {
            tests.validate()?;
            for (name, test) in &tests.tests {
                if !self.callables.contains_key(&test.callable) {
                    return invalid(format!(
                        "BicDB application test `{name}` references absent callable `{}`",
                        test.callable
                    ));
                }
            }
        }
        if let Some(observability) = &self.observability {
            observability.validate(self)?;
        } else if CARRIER_OBSERVABILITY_HELPERS
            .into_iter()
            .any(|target| application_program_uses_builtin(self, &[target]))
        {
            return invalid(
                "BicDB application observability builtin call lacks an exact signed observability contract",
            );
        }
        if application_program_call_matches(self, &|kind, target, method, _, _| {
            if *kind != ApplicationCallKindV1::Client {
                return false;
            }
            if self.client_bindings.contains_key(target) {
                return !matches!(method, Some("get" | "post"));
            }
            self.grpc
                .as_ref()
                .and_then(|grpc| grpc.clients.get(target))
                .zip(method)
                .is_none_or(|(client, method)| !client.methods.contains_key(method))
        }) {
            return invalid(
                "BicDB application client call is outside its exact HTTP or gRPC binding authority",
            );
        }
        for (event, queue) in &self.event_bindings {
            validate_identifier("BicDB application event", event)?;
            validate_identifier("BicDB application event queue", queue)?;
        }
        for (event, queues) in &self.realtime_bindings {
            validate_identifier("BicDB application realtime event", event)?;
            if queues.is_empty() {
                return invalid(format!(
                    "BicDB application realtime event `{event}` has no fan-out queues"
                ));
            }
            validate_string_set("BicDB application realtime queue", queues)?;
        }
        for binding in &self.mutation_bindings {
            validate_identifier("BicDB application mutation resource", &binding.resource)?;
            validate_identifier("BicDB application mutation callable", &binding.callable)?;
            if !self.callables.contains_key(&binding.callable) {
                return invalid(format!(
                    "BicDB application mutation binding references absent callable `{}`",
                    binding.callable
                ));
            }
            match binding.kind {
                ApplicationMutationBindingKindV1::InsideTrigger => {
                    if binding.event.is_some() || binding.queue.is_some() {
                        return invalid("inside-transaction trigger has event/queue metadata");
                    }
                }
                ApplicationMutationBindingKindV1::PostCommitTrigger => {
                    if binding.event.is_some() || binding.queue.is_none() {
                        return invalid("post-commit trigger requires only a queue binding");
                    }
                }
                ApplicationMutationBindingKindV1::Watch => {
                    let event = binding.event.as_ref().ok_or_else(|| {
                        ExtensionError::InvalidManifest(
                            "BicDB application watch requires an event binding".to_string(),
                        )
                    })?;
                    if binding.queue.is_some() || !self.event_bindings.contains_key(event) {
                        return invalid(
                            "BicDB application watch references absent event or unexpected queue",
                        );
                    }
                }
            }
        }
        for (workflow_name, workflow) in &self.workflow_bindings {
            validate_identifier("BicDB application workflow", workflow_name)?;
            validate_identifier("BicDB application workflow queue", &workflow.queue)?;
            validate_identifier(
                "BicDB application workflow worker export",
                &workflow.worker_export,
            )?;
            if workflow.timeout_ms == Some(0) {
                return invalid(format!(
                    "BicDB application workflow `{workflow_name}` has a zero timeout"
                ));
            }
            if workflow.steps.is_empty() {
                return invalid(format!(
                    "BicDB application workflow `{workflow_name}` has no steps"
                ));
            }
            if !(1..=64).contains(&workflow.max_parallelism) {
                return invalid(format!(
                    "BicDB application workflow `{workflow_name}` max_parallelism must be between 1 and 64"
                ));
            }
            if let Some(plan_sha256) = &workflow.plan_sha256 {
                validate_sha256("BicDB application workflow plan hash", plan_sha256)?;
            }
            let mut invariant_names = BTreeSet::new();
            for invariant in &workflow.invariants {
                validate_identifier("BicDB application workflow invariant", &invariant.name)?;
                validate_nonempty(
                    "BicDB application workflow invariant source",
                    &invariant.source,
                    16 * 1024,
                )?;
                if !invariant_names.insert(&invariant.name) {
                    return invalid(format!(
                        "BicDB application workflow `{workflow_name}` repeats invariant `{}`",
                        invariant.name
                    ));
                }
                validate_carrier_expression(&invariant.expression)?;
            }
            let mut step_names = BTreeSet::new();
            for step in &workflow.steps {
                validate_identifier("BicDB application workflow step", &step.name)?;
                if !step_names.insert(step.name.clone()) {
                    return invalid(format!(
                        "BicDB application workflow `{workflow_name}` repeats step `{}`",
                        step.name
                    ));
                }
                if let Some(wait) = &step.wait {
                    if !step.callable.is_empty()
                        || step.condition_callable.is_some()
                        || step.compensation_callable.is_some()
                    {
                        return invalid(format!(
                            "BicDB application workflow `{workflow_name}` wait step `{}` cannot declare callables",
                            step.name
                        ));
                    }
                    match wait.kind {
                        ApplicationWorkflowWaitKindV1::Signal
                            if wait
                                .signal
                                .as_deref()
                                .is_some_and(|value| !value.is_empty())
                                && wait.delay_ms.is_none() => {}
                        ApplicationWorkflowWaitKindV1::Delay
                            if wait.signal.is_none()
                                && wait.delay_ms.is_some_and(|value| value > 0)
                                && wait.timeout_ms.is_none() => {}
                        _ => {
                            return invalid(format!(
                                "BicDB application workflow `{workflow_name}` wait step `{}` has an invalid signal/delay contract",
                                step.name
                            ));
                        }
                    }
                    if wait.timeout_ms == Some(0) {
                        return invalid(format!(
                            "BicDB application workflow `{workflow_name}` wait step `{}` has a zero timeout",
                            step.name
                        ));
                    }
                } else {
                    validate_identifier(
                        "BicDB application workflow step callable",
                        &step.callable,
                    )?;
                    if !self.callables.contains_key(&step.callable) {
                        return invalid(format!(
                            "BicDB application workflow `{workflow_name}` references absent step callable `{}`",
                            step.callable
                        ));
                    }
                }
                for callable in [
                    step.condition_callable.as_ref(),
                    step.compensation_callable.as_ref(),
                ]
                .into_iter()
                .flatten()
                {
                    validate_identifier("BicDB application workflow auxiliary callable", callable)?;
                    if !self.callables.contains_key(callable) {
                        return invalid(format!(
                            "BicDB application workflow `{workflow_name}` references absent callable `{callable}`"
                        ));
                    }
                }
                let dependencies = step.dependencies.iter().collect::<BTreeSet<_>>();
                if dependencies.len() != step.dependencies.len()
                    || dependencies.contains(&step.name)
                {
                    return invalid(format!(
                        "BicDB application workflow `{workflow_name}` step `{}` has duplicate/self dependencies",
                        step.name
                    ));
                }
            }
            let mut sla_names = BTreeSet::new();
            for sla in &workflow.slas {
                validate_identifier("BicDB application workflow SLA", &sla.name)?;
                if !sla_names.insert(&sla.name)
                    || sla.attainment_basis_points == 0
                    || sla.attainment_basis_points > 10_000
                    || sla.deadline_ms == 0
                    || sla.warning_at_basis_points == 0
                    || sla.warning_at_basis_points > sla.breach_at_basis_points
                    || sla.breach_at_basis_points > 10_000
                {
                    return invalid(format!(
                        "BicDB application workflow `{workflow_name}` SLA `{}` has invalid thresholds",
                        sla.name
                    ));
                }
                for condition in [sla.starts_when.as_ref(), sla.ends_when.as_ref()]
                    .into_iter()
                    .flatten()
                {
                    validate_nonempty(
                        "BicDB application workflow SLA condition",
                        &condition.value,
                        512,
                    )?;
                    if matches!(
                        condition.kind,
                        ApplicationWorkflowSlaConditionKindV1::StepStarted
                            | ApplicationWorkflowSlaConditionKindV1::StepCompleted
                    ) && !step_names.contains(&condition.value)
                    {
                        return invalid(format!(
                            "BicDB application workflow `{workflow_name}` SLA `{}` references absent step `{}`",
                            sla.name, condition.value
                        ));
                    }
                }
                for escalation in &sla.escalations {
                    validate_nonempty(
                        "BicDB application workflow SLA escalation kind",
                        &escalation.target_kind,
                        64,
                    )?;
                    validate_nonempty(
                        "BicDB application workflow SLA escalation target",
                        &escalation.target,
                        2_048,
                    )?;
                }
                validate_nonempty("BicDB application workflow SLA scope", &sla.scope, 64)?;
                validate_nonempty(
                    "BicDB application workflow SLA measure",
                    &sla.measure_by,
                    64,
                )?;
                for report in &sla.reports {
                    validate_nonempty("BicDB application workflow SLA report", report, 512)?;
                }
                for exclusion in &sla.exclusions {
                    match exclusion.kind {
                        ApplicationWorkflowSlaExclusionKindV1::Event
                            if exclusion
                                .event
                                .as_deref()
                                .is_some_and(|event| !event.is_empty())
                                && exclusion.field.is_none()
                                && exclusion.value.is_none() => {}
                        ApplicationWorkflowSlaExclusionKindV1::FieldEqualsBool
                            if exclusion.event.is_none()
                                && exclusion
                                    .field
                                    .as_deref()
                                    .is_some_and(|field| !field.is_empty())
                                && exclusion.value.is_some() => {}
                        _ => {
                            return invalid(format!(
                                "BicDB application workflow `{workflow_name}` SLA `{}` has an invalid exclusion",
                                sla.name
                            ));
                        }
                    }
                }
            }
            if !step_names.contains(&workflow.return_step) {
                return invalid(format!(
                    "BicDB application workflow `{workflow_name}` return step `{}` is absent",
                    workflow.return_step
                ));
            }
            if let Some((step, dependency)) = workflow.steps.iter().find_map(|step| {
                step.dependencies
                    .iter()
                    .find(|dependency| !step_names.contains(*dependency))
                    .map(|dependency| (&step.name, dependency))
            }) {
                return invalid(format!(
                    "BicDB application workflow `{workflow_name}` step `{step}` depends on absent step `{dependency}`"
                ));
            }
            let mut resolved = BTreeSet::new();
            loop {
                let before = resolved.len();
                for step in &workflow.steps {
                    if step
                        .dependencies
                        .iter()
                        .all(|dependency| resolved.contains(dependency))
                    {
                        resolved.insert(step.name.clone());
                    }
                }
                if resolved.len() == workflow.steps.len() || resolved.len() == before {
                    break;
                }
            }
            if resolved.len() != workflow.steps.len() {
                return invalid(format!(
                    "BicDB application workflow `{workflow_name}` dependency graph contains a cycle"
                ));
            }
        }
        Ok(())
    }
}

fn validate_carrier_statement(statement: &ApplicationStatementV1) -> Result<()> {
    match statement {
        ApplicationStatementV1::Let { value, .. }
        | ApplicationStatementV1::Assign { value, .. }
        | ApplicationStatementV1::Return { value }
        | ApplicationStatementV1::Expr { value } => validate_carrier_expression(value),
        ApplicationStatementV1::If {
            condition,
            then_branch,
            else_branch,
        } => {
            validate_carrier_expression(condition)?;
            for statement in then_branch.iter().chain(else_branch) {
                validate_carrier_statement(statement)?;
            }
            Ok(())
        }
        ApplicationStatementV1::For { iterable, body, .. }
        | ApplicationStatementV1::While {
            condition: iterable,
            body,
        } => {
            validate_carrier_expression(iterable)?;
            for statement in body {
                validate_carrier_statement(statement)?;
            }
            Ok(())
        }
        ApplicationStatementV1::Transaction { isolation, body } => {
            if !matches!(
                isolation.as_str(),
                "read_committed" | "repeatable_read" | "serializable"
            ) {
                return invalid(format!(
                    "unknown BicDB application transaction isolation `{isolation}`"
                ));
            }
            for statement in body {
                validate_carrier_statement(statement)?;
            }
            Ok(())
        }
        ApplicationStatementV1::Fail { code, message } => {
            validate_carrier_expression(code)?;
            validate_carrier_expression(message)
        }
        ApplicationStatementV1::Emit { value, .. } => validate_carrier_expression(value),
        ApplicationStatementV1::WithTimeout { timeout_ms, body } => {
            validate_carrier_expression(timeout_ms)?;
            for statement in body {
                validate_carrier_statement(statement)?;
            }
            Ok(())
        }
        ApplicationStatementV1::WithRetry {
            attempts,
            backoff_ms,
            max_backoff_ms,
            body,
        } => {
            validate_carrier_expression(attempts)?;
            validate_carrier_expression(backoff_ms)?;
            if let Some(maximum) = max_backoff_ms {
                validate_carrier_expression(maximum)?;
            }
            for statement in body {
                validate_carrier_statement(statement)?;
            }
            Ok(())
        }
        ApplicationStatementV1::CircuitBreaker {
            name,
            failure_threshold,
            reset_timeout_ms,
            body,
        } => {
            validate_carrier_expression(name)?;
            validate_carrier_expression(failure_threshold)?;
            validate_carrier_expression(reset_timeout_ms)?;
            for statement in body {
                validate_carrier_statement(statement)?;
            }
            Ok(())
        }
        ApplicationStatementV1::Break | ApplicationStatementV1::Continue => Ok(()),
    }
}

fn application_program_transaction_features(
    program: &ApplicationProgramV1,
) -> BTreeSet<ApplicationFeature> {
    fn collect(statement: &ApplicationStatementV1, features: &mut BTreeSet<ApplicationFeature>) {
        match statement {
            ApplicationStatementV1::Transaction { isolation, body } => {
                features.insert(match isolation.as_str() {
                    "repeatable_read" => ApplicationFeature::RepeatableRead,
                    "serializable" => ApplicationFeature::Serializable,
                    _ => ApplicationFeature::ReadCommitted,
                });
                features.insert(ApplicationFeature::Savepoints);
                for statement in body {
                    collect(statement, features);
                }
            }
            ApplicationStatementV1::If {
                then_branch,
                else_branch,
                ..
            } => {
                for statement in then_branch.iter().chain(else_branch) {
                    collect(statement, features);
                }
            }
            ApplicationStatementV1::For { body, .. }
            | ApplicationStatementV1::While { body, .. }
            | ApplicationStatementV1::WithTimeout { body, .. }
            | ApplicationStatementV1::WithRetry { body, .. }
            | ApplicationStatementV1::CircuitBreaker { body, .. } => {
                for statement in body {
                    collect(statement, features);
                }
            }
            ApplicationStatementV1::Let { .. }
            | ApplicationStatementV1::Assign { .. }
            | ApplicationStatementV1::Return { .. }
            | ApplicationStatementV1::Break
            | ApplicationStatementV1::Continue
            | ApplicationStatementV1::Fail { .. }
            | ApplicationStatementV1::Emit { .. }
            | ApplicationStatementV1::Expr { .. } => {}
        }
    }

    let mut features = BTreeSet::new();
    for callable in program.callables.values() {
        for statement in &callable.body {
            collect(statement, &mut features);
        }
    }
    features
}

fn validate_carrier_expression(expression: &ApplicationExpressionV1) -> Result<()> {
    match expression {
        ApplicationExpressionV1::Variable { .. } | ApplicationExpressionV1::Literal { .. } => {
            Ok(())
        }
        ApplicationExpressionV1::Object { fields } => {
            for value in fields.values() {
                validate_carrier_expression(value)?;
            }
            Ok(())
        }
        ApplicationExpressionV1::Array { items } => {
            for value in items {
                validate_carrier_expression(value)?;
            }
            Ok(())
        }
        ApplicationExpressionV1::Field { target, .. }
        | ApplicationExpressionV1::Unary { value: target, .. } => {
            validate_carrier_expression(target)
        }
        ApplicationExpressionV1::Index { target, index }
        | ApplicationExpressionV1::Binary {
            left: target,
            right: index,
            ..
        } => {
            validate_carrier_expression(target)?;
            validate_carrier_expression(index)
        }
        ApplicationExpressionV1::Match { value, arms } => {
            validate_carrier_expression(value)?;
            for arm in arms {
                validate_carrier_expression(&arm.value)?;
            }
            Ok(())
        }
        ApplicationExpressionV1::Call {
            kind,
            target,
            method,
            result_projection,
            arguments,
            ..
        } => {
            for argument in arguments {
                validate_carrier_expression(&argument.value)?;
            }
            let Some(projection) = result_projection else {
                return Ok(());
            };
            let paged_model_projection = *kind == ApplicationCallKindV1::Model
                && method.as_deref().is_some_and(|method| {
                    method == "list" || method == "search" || method.starts_with("list_by_")
                });
            let typed_declared_sql_projection = *kind == ApplicationCallKindV1::Builtin
                && method.is_none()
                && matches!(
                    target.as_str(),
                    "sql.one_as"
                        | "sql.list_as"
                        | "db.call_as"
                        | "db.fn_one_as"
                        | "db.graph_one_as"
                        | "db.graph_list_as"
                );
            if !paged_model_projection && !typed_declared_sql_projection {
                return invalid(
                    "BicDB application result projection requires a paged model call or typed declared SQL call",
                );
            }
            if projection.fields.len() > 1_024 {
                return invalid("BicDB application model projection has too many fields");
            }
            let mut names = BTreeSet::new();
            for field in &projection.fields {
                let name = match field {
                    ApplicationProjectionFieldV1::Field { name, field } => {
                        validate_identifier("BicDB application projection source field", field)?;
                        name
                    }
                    ApplicationProjectionFieldV1::Relation {
                        name,
                        source_field,
                        target,
                        target_key,
                        target_field,
                    } => {
                        validate_identifier(
                            "BicDB application projection relation source",
                            source_field,
                        )?;
                        validate_identifier(
                            "BicDB application projection relation target",
                            target,
                        )?;
                        validate_identifier(
                            "BicDB application projection relation key",
                            target_key,
                        )?;
                        validate_identifier(
                            "BicDB application projection relation field",
                            target_field,
                        )?;
                        name
                    }
                    ApplicationProjectionFieldV1::Computed { name, expression } => {
                        validate_carrier_expression(expression)?;
                        name
                    }
                };
                validate_identifier("BicDB application projection field", name)?;
                if !names.insert(name.to_ascii_lowercase()) {
                    return invalid(format!(
                        "BicDB application model projection repeats field `{name}`"
                    ));
                }
            }
            Ok(())
        }
        ApplicationExpressionV1::Exists { .. } | ApplicationExpressionV1::Aggregate { .. } => {
            invalid("invariant-only expression escaped into the BicDB application behavior program")
        }
    }
}

fn valid_resource_policy_literal(value: &JsonValue) -> bool {
    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => true,
        JsonValue::Array(items) => items.iter().all(|item| {
            matches!(
                item,
                JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_)
            )
        }),
        JsonValue::Object(_) => false,
    }
}

fn validate_resource_policy_expression(
    application: &ApplicationManifestV2,
    subject: &ResourceContractV1,
    bindings: &BTreeMap<String, String>,
    expression: &ApplicationExpressionV1,
    depth: usize,
) -> Result<()> {
    if depth > 64 {
        return invalid("resource policy expression exceeds maximum depth");
    }
    let recurse = |value: &ApplicationExpressionV1, bindings: &BTreeMap<String, String>| {
        validate_resource_policy_expression(application, subject, bindings, value, depth + 1)
    };
    match expression {
        ApplicationExpressionV1::Variable { name }
            if subject.fields.iter().any(|field| field.name == *name) =>
        {
            Ok(())
        }
        ApplicationExpressionV1::Variable { name } => invalid(format!(
            "resource policy references undeclared field or variable `{name}`"
        )),
        ApplicationExpressionV1::Literal { value } if valid_resource_policy_literal(value) => Ok(()),
        ApplicationExpressionV1::Literal { .. } => {
            invalid("resource policy literals must be scalar or arrays of scalars")
        }
        ApplicationExpressionV1::Field { target, field }
            if matches!(target.as_ref(), ApplicationExpressionV1::Variable { name } if name == "auth")
                && matches!(
                    field.as_str(),
                    "id" | "email" | "name" | "roles" | "scopes" | "tenant_id"
                ) =>
        {
            Ok(())
        }
        ApplicationExpressionV1::Field { target, field } => {
            let ApplicationExpressionV1::Variable { name } = target.as_ref() else {
                return invalid(
                    "resource policy field access requires auth or a relation binding",
                );
            };
            let resource_name = bindings.get(name).ok_or_else(|| {
                ExtensionError::InvalidManifest(format!(
                    "resource policy field access uses unknown binding `{name}`"
                ))
            })?;
            let resource = application
                .resources
                .iter()
                .find(|resource| resource.name == *resource_name)
                .expect("resource policy binding resources are validated");
            if resource.fields.iter().any(|candidate| candidate.name == *field) {
                Ok(())
            } else {
                invalid(format!(
                    "resource policy binding `{name}` references undeclared field `{field}`"
                ))
            }
        }
        ApplicationExpressionV1::Unary { operator, value, .. }
            if matches!(operator.as_str(), "not" | "negate") =>
        {
            recurse(value, bindings)
        }
        ApplicationExpressionV1::Binary {
            operator,
            left,
            right,
            ..
        } if matches!(
            operator.as_str(),
            "and"
                | "or"
                | "implies"
                | "contains"
                | "equal"
                | "not_equal"
                | "greater"
                | "greater_equal"
                | "less"
                | "less_equal"
        ) => {
            recurse(left, bindings)?;
            recurse(right, bindings)
        }
        ApplicationExpressionV1::Array { items } => {
            for item in items {
                recurse(item, bindings)?;
            }
            Ok(())
        }
        ApplicationExpressionV1::Call {
            kind: ApplicationCallKindV1::Builtin,
            target,
            arguments,
            ..
        } if target == "array.contains" => {
            if arguments.len() != 2 {
                return invalid("resource policy array.contains requires two arguments");
            }
            for argument in arguments {
                recurse(&argument.value, bindings)?;
            }
            Ok(())
        }
        ApplicationExpressionV1::Exists {
            binding,
            resource,
            condition,
        } => {
            validate_identifier("resource policy binding", binding)?;
            if binding == "auth"
                || subject
                    .fields
                    .iter()
                    .any(|field| field.name == *binding)
            {
                return invalid(format!(
                    "resource policy binding `{binding}` shadows a trusted or subject value"
                ));
            }
            if bindings.contains_key(binding) {
                return invalid(format!("duplicate resource policy binding `{binding}`"));
            }
            if resource == &subject.name {
                return invalid(format!(
                    "resource `{}` policy cannot correlate EXISTS to itself without an explicit outer alias",
                    subject.name
                ));
            }
            if !application
                .resources
                .iter()
                .any(|candidate| candidate.name == *resource)
            {
                return invalid(format!(
                    "resource policy binding `{binding}` references unknown resource `{resource}`"
                ));
            }
            let mut nested = bindings.clone();
            nested.insert(binding.clone(), resource.clone());
            recurse(condition, &nested)
        }
        _ => invalid(
            "resource policy expressions may use only row fields, trusted auth fields, literals, boolean/comparison operators, array.contains, and bounded relation exists",
        ),
    }
}

fn validate_resource_index_predicate(
    expression: &ApplicationExpressionV1,
    fields: &BTreeSet<&str>,
) -> Result<()> {
    match expression {
        ApplicationExpressionV1::Variable { name } => {
            if fields.contains(name.as_str()) {
                Ok(())
            } else {
                invalid(format!(
                    "partial index predicate references undeclared field `{name}`"
                ))
            }
        }
        ApplicationExpressionV1::Literal { .. } => Ok(()),
        ApplicationExpressionV1::Unary {
            operator, value, ..
        } if matches!(operator.as_str(), "not" | "negate") => {
            validate_resource_index_predicate(value, fields)
        }
        ApplicationExpressionV1::Binary {
            operator,
            left,
            right,
            ..
        } if matches!(
            operator.as_str(),
            "and"
                | "or"
                | "implies"
                | "contains"
                | "equal"
                | "not_equal"
                | "greater"
                | "greater_equal"
                | "less"
                | "less_equal"
        ) => {
            validate_resource_index_predicate(left, fields)?;
            validate_resource_index_predicate(right, fields)
        }
        _ => invalid(
            "partial index predicates only support model fields, literals, boolean operators, comparisons, and contains",
        ),
    }
}

fn application_program_uses_builtin(program: &ApplicationProgramV1, targets: &[&str]) -> bool {
    application_program_call_matches(program, &|kind, target, _, _, _| {
        *kind == ApplicationCallKindV1::Builtin
            && targets.iter().any(|candidate| target == *candidate)
    })
}

fn application_program_uses_builtin_named_argument(
    program: &ApplicationProgramV1,
    target: &str,
    argument: &str,
) -> bool {
    application_program_call_matches(program, &|kind, candidate, _, arguments, _| {
        *kind == ApplicationCallKindV1::Builtin
            && candidate == target
            && arguments
                .iter()
                .any(|candidate| candidate.name.as_deref() == Some(argument))
    })
}

fn application_program_blob_signed_methods(
    program: &ApplicationProgramV1,
) -> Result<BTreeSet<String>> {
    let mut methods = BTreeSet::new();
    for callable in program.callables.values() {
        for statement in &callable.body {
            collect_carrier_blob_signed_methods_statement(statement, &mut methods)?;
        }
    }
    Ok(methods)
}

fn collect_carrier_blob_signed_methods_statement(
    statement: &ApplicationStatementV1,
    methods: &mut BTreeSet<String>,
) -> Result<()> {
    let mut expression = |value: &ApplicationExpressionV1| {
        collect_carrier_blob_signed_methods_expression(value, methods)
    };
    match statement {
        ApplicationStatementV1::Let { value, .. }
        | ApplicationStatementV1::Assign { value, .. }
        | ApplicationStatementV1::Return { value }
        | ApplicationStatementV1::Expr { value }
        | ApplicationStatementV1::Emit { value, .. } => expression(value),
        ApplicationStatementV1::If {
            condition,
            then_branch,
            else_branch,
        } => {
            expression(condition)?;
            for statement in then_branch.iter().chain(else_branch) {
                collect_carrier_blob_signed_methods_statement(statement, methods)?;
            }
            Ok(())
        }
        ApplicationStatementV1::For { iterable, body, .. } => {
            expression(iterable)?;
            for statement in body {
                collect_carrier_blob_signed_methods_statement(statement, methods)?;
            }
            Ok(())
        }
        ApplicationStatementV1::While { condition, body } => {
            expression(condition)?;
            for statement in body {
                collect_carrier_blob_signed_methods_statement(statement, methods)?;
            }
            Ok(())
        }
        ApplicationStatementV1::Transaction { body, .. } => {
            for statement in body {
                collect_carrier_blob_signed_methods_statement(statement, methods)?;
            }
            Ok(())
        }
        ApplicationStatementV1::Fail { code, message } => {
            expression(code)?;
            expression(message)
        }
        ApplicationStatementV1::WithTimeout { timeout_ms, body } => {
            expression(timeout_ms)?;
            for statement in body {
                collect_carrier_blob_signed_methods_statement(statement, methods)?;
            }
            Ok(())
        }
        ApplicationStatementV1::WithRetry {
            attempts,
            backoff_ms,
            max_backoff_ms,
            body,
            ..
        } => {
            expression(attempts)?;
            expression(backoff_ms)?;
            if let Some(max_backoff_ms) = max_backoff_ms {
                expression(max_backoff_ms)?;
            }
            for statement in body {
                collect_carrier_blob_signed_methods_statement(statement, methods)?;
            }
            Ok(())
        }
        ApplicationStatementV1::CircuitBreaker {
            name,
            failure_threshold,
            reset_timeout_ms,
            body,
        } => {
            expression(name)?;
            expression(failure_threshold)?;
            expression(reset_timeout_ms)?;
            for statement in body {
                collect_carrier_blob_signed_methods_statement(statement, methods)?;
            }
            Ok(())
        }
        ApplicationStatementV1::Break | ApplicationStatementV1::Continue => Ok(()),
    }
}

fn collect_carrier_blob_signed_methods_expression(
    expression: &ApplicationExpressionV1,
    methods: &mut BTreeSet<String>,
) -> Result<()> {
    match expression {
        ApplicationExpressionV1::Variable { .. } | ApplicationExpressionV1::Literal { .. } => {
            Ok(())
        }
        ApplicationExpressionV1::Object { fields } => {
            for value in fields.values() {
                collect_carrier_blob_signed_methods_expression(value, methods)?;
            }
            Ok(())
        }
        ApplicationExpressionV1::Array { items } => {
            for value in items {
                collect_carrier_blob_signed_methods_expression(value, methods)?;
            }
            Ok(())
        }
        ApplicationExpressionV1::Field { target, .. }
        | ApplicationExpressionV1::Unary { value: target, .. } => {
            collect_carrier_blob_signed_methods_expression(target, methods)
        }
        ApplicationExpressionV1::Index { target, index }
        | ApplicationExpressionV1::Binary {
            left: target,
            right: index,
            ..
        } => {
            collect_carrier_blob_signed_methods_expression(target, methods)?;
            collect_carrier_blob_signed_methods_expression(index, methods)
        }
        ApplicationExpressionV1::Match { value, arms } => {
            collect_carrier_blob_signed_methods_expression(value, methods)?;
            for arm in arms {
                collect_carrier_blob_signed_methods_expression(&arm.value, methods)?;
            }
            Ok(())
        }
        ApplicationExpressionV1::Call {
            kind,
            target,
            arguments,
            result_projection,
            ..
        } => {
            if *kind == ApplicationCallKindV1::Builtin && target == "blob.signed_url" {
                let method = arguments
                    .iter()
                    .find(|argument| argument.name.as_deref() == Some("method"));
                let method =
                    match method {
                        None => "GET".to_string(),
                        Some(ApplicationArgumentV1 {
                            value:
                                ApplicationExpressionV1::Literal {
                                    value: JsonValue::String(value),
                                },
                            ..
                        }) if matches!(value.to_ascii_uppercase().as_str(), "GET" | "PUT") => {
                            value.to_ascii_uppercase()
                        }
                        _ => return invalid(
                            "BicDB application blob signed URL method must be a literal GET or PUT",
                        ),
                    };
                methods.insert(method);
            }
            for argument in arguments {
                collect_carrier_blob_signed_methods_expression(&argument.value, methods)?;
            }
            if let Some(projection) = result_projection {
                for field in &projection.fields {
                    if let ApplicationProjectionFieldV1::Computed { expression, .. } = field {
                        collect_carrier_blob_signed_methods_expression(expression, methods)?;
                    }
                }
            }
            Ok(())
        }
        ApplicationExpressionV1::Exists { condition, .. }
        | ApplicationExpressionV1::Aggregate { condition, .. } => {
            collect_carrier_blob_signed_methods_expression(condition, methods)
        }
    }
}

fn application_program_uses_typed_declared_sql_projections(program: &ApplicationProgramV1) -> bool {
    application_program_call_matches(
        program,
        &|kind, target, method, _, has_result_projection| {
            has_result_projection
                && *kind == ApplicationCallKindV1::Builtin
                && method.is_none()
                && matches!(
                    target,
                    "sql.one_as"
                        | "sql.list_as"
                        | "db.call_as"
                        | "db.fn_one_as"
                        | "db.graph_one_as"
                        | "db.graph_list_as"
                )
        },
    )
}

fn application_program_call_matches(
    program: &ApplicationProgramV1,
    predicate: &impl Fn(
        &ApplicationCallKindV1,
        &str,
        Option<&str>,
        &[ApplicationArgumentV1],
        bool,
    ) -> bool,
) -> bool {
    program.callables.values().any(|callable| {
        callable
            .body
            .iter()
            .any(|statement| carrier_statement_call_matches(statement, predicate))
    })
}

fn carrier_statement_call_matches(
    statement: &ApplicationStatementV1,
    predicate: &impl Fn(
        &ApplicationCallKindV1,
        &str,
        Option<&str>,
        &[ApplicationArgumentV1],
        bool,
    ) -> bool,
) -> bool {
    let expression =
        |value: &ApplicationExpressionV1| carrier_expression_call_matches(value, predicate);
    let block = |values: &[ApplicationStatementV1]| {
        values
            .iter()
            .any(|value| carrier_statement_call_matches(value, predicate))
    };
    match statement {
        ApplicationStatementV1::Let { value, .. }
        | ApplicationStatementV1::Assign { value, .. }
        | ApplicationStatementV1::Return { value }
        | ApplicationStatementV1::Expr { value }
        | ApplicationStatementV1::Emit { value, .. } => expression(value),
        ApplicationStatementV1::If {
            condition,
            then_branch,
            else_branch,
        } => expression(condition) || block(then_branch) || block(else_branch),
        ApplicationStatementV1::For { iterable, body, .. } => expression(iterable) || block(body),
        ApplicationStatementV1::While { condition, body } => expression(condition) || block(body),
        ApplicationStatementV1::Transaction { body, .. } => block(body),
        ApplicationStatementV1::Fail { code, message } => expression(code) || expression(message),
        ApplicationStatementV1::WithTimeout { timeout_ms, body } => {
            expression(timeout_ms) || block(body)
        }
        ApplicationStatementV1::WithRetry {
            attempts,
            backoff_ms,
            max_backoff_ms,
            body,
            ..
        } => {
            expression(attempts)
                || expression(backoff_ms)
                || max_backoff_ms.as_ref().is_some_and(expression)
                || block(body)
        }
        ApplicationStatementV1::CircuitBreaker {
            name,
            failure_threshold,
            reset_timeout_ms,
            body,
        } => {
            expression(name)
                || expression(failure_threshold)
                || expression(reset_timeout_ms)
                || block(body)
        }
        ApplicationStatementV1::Break | ApplicationStatementV1::Continue => false,
    }
}

fn carrier_expression_call_matches(
    expression: &ApplicationExpressionV1,
    predicate: &impl Fn(
        &ApplicationCallKindV1,
        &str,
        Option<&str>,
        &[ApplicationArgumentV1],
        bool,
    ) -> bool,
) -> bool {
    match expression {
        ApplicationExpressionV1::Variable { .. } | ApplicationExpressionV1::Literal { .. } => false,
        ApplicationExpressionV1::Object { fields } => fields
            .values()
            .any(|value| carrier_expression_call_matches(value, predicate)),
        ApplicationExpressionV1::Array { items } => items
            .iter()
            .any(|value| carrier_expression_call_matches(value, predicate)),
        ApplicationExpressionV1::Field { target, .. }
        | ApplicationExpressionV1::Unary { value: target, .. } => {
            carrier_expression_call_matches(target, predicate)
        }
        ApplicationExpressionV1::Index { target, index }
        | ApplicationExpressionV1::Binary {
            left: target,
            right: index,
            ..
        } => {
            carrier_expression_call_matches(target, predicate)
                || carrier_expression_call_matches(index, predicate)
        }
        ApplicationExpressionV1::Match { value, arms } => {
            carrier_expression_call_matches(value, predicate)
                || arms
                    .iter()
                    .any(|arm| carrier_expression_call_matches(&arm.value, predicate))
        }
        ApplicationExpressionV1::Call {
            kind,
            target,
            method,
            arguments,
            result_projection,
            ..
        } => {
            predicate(
                kind,
                target,
                method.as_deref(),
                arguments,
                result_projection.is_some(),
            ) || arguments
                .iter()
                .any(|argument| carrier_expression_call_matches(&argument.value, predicate))
                || result_projection.as_ref().is_some_and(|projection| {
                    projection.fields.iter().any(|field| match field {
                        ApplicationProjectionFieldV1::Computed { expression, .. } => {
                            carrier_expression_call_matches(expression, predicate)
                        }
                        ApplicationProjectionFieldV1::Field { .. }
                        | ApplicationProjectionFieldV1::Relation { .. } => false,
                    })
                })
        }
        ApplicationExpressionV1::Exists { condition, .. }
        | ApplicationExpressionV1::Aggregate { condition, .. } => {
            carrier_expression_call_matches(condition, predicate)
        }
    }
}

impl PackageMetadata {
    fn validate(&self, extension_name: &str) -> Result<()> {
        if !self.application.eq_ignore_ascii_case(extension_name) {
            return invalid("package application name differs from extension identity");
        }
        validate_nonempty("package version", &self.version, 64)?;
        for (label, value) in [
            ("package hash", &self.package_sha256),
            ("dependency lock hash", &self.dependency_lock_sha256),
            ("SBOM hash", &self.sbom_sha256),
            ("provenance hash", &self.provenance_sha256),
        ] {
            validate_sha256(label, value)?;
        }
        validate_nonempty("signature key id", &self.signature_key_id, 256)?;
        if !matches!(
            self.signature_algorithm.as_str(),
            "ed25519" | "ecdsa-p256-sha256" | "rsa-pss-sha256"
        ) {
            return invalid(format!(
                "unsupported signature algorithm `{}`",
                self.signature_algorithm
            ));
        }
        validate_nonempty("package signature", &self.signature, 16 * 1024)
    }
}

impl RawSqlDeclaration {
    fn validate(&self) -> Result<()> {
        validate_identifier("raw SQL statement", &self.id)?;
        validate_nonempty("raw SQL", &self.sql, 1024 * 1024)?;
        validate_sha256("raw SQL hash", &self.sha256)?;
        if self.actions.is_empty() {
            return invalid(format!(
                "raw SQL `{}` must declare concrete operations",
                self.id
            ));
        }
        if self.relations.is_empty()
            && self.routines.is_empty()
            && self.actions != BTreeSet::from([DatabaseAction::Select])
        {
            return invalid(format!(
                "raw SQL `{}` without relation or routine authority must be a capability-free SELECT projection",
                self.id
            ));
        }
        if self
            .actions
            .iter()
            .any(|action| matches!(action, DatabaseAction::RawSql))
        {
            return invalid(format!(
                "raw SQL `{}` must declare concrete operations, not raw_sql",
                self.id
            ));
        }
        for relation in &self.relations {
            validate_qualified("raw SQL relation", relation)?;
        }
        for routine in &self.routines {
            validate_qualified("raw SQL routine", routine)?;
        }
        if self.max_affected_rows == 0 || self.max_affected_rows > 10_000 {
            return invalid(format!(
                "raw SQL `{}` max_affected_rows must be 1..=10000",
                self.id
            ));
        }
        Ok(())
    }
}

impl ServiceImport {
    fn validate(&self) -> Result<()> {
        validate_identifier("service import", &self.name)?;
        validate_identifier("service", &self.service)?;
        semver::VersionReq::parse(&self.version).map_err(|error| {
            ExtensionError::InvalidManifest(format!(
                "service import `{}` has invalid version requirement: {error}",
                self.name
            ))
        })?;
        validate_sha256("service contract hash", &self.contract_sha256)
    }
}

impl ServiceExport {
    fn validate(&self) -> Result<()> {
        validate_identifier("service", &self.service)?;
        semver::Version::parse(&self.version).map_err(|error| {
            ExtensionError::InvalidManifest(format!(
                "service `{}` has invalid semantic version: {error}",
                self.service
            ))
        })?;
        validate_sha256("service contract hash", &self.contract_sha256)?;
        validate_identifier("service export", &self.export)?;
        unique_by("service method", self.methods.iter().map(|item| &item.name))?;
        for method in &self.methods {
            validate_identifier("service method", &method.name)?;
            unique_by(
                "service request field",
                method.request.iter().map(|field| &field.name),
            )?;
            unique_by(
                "service response field",
                method.response.iter().map(|field| &field.name),
            )?;
            for field in method.request.iter().chain(&method.response) {
                validate_identifier("service contract field", &field.name)?;
                if let Some(value_type) = &field.value_type {
                    value_type.validate(0)?;
                }
            }
            for code in &method.errors {
                validate_service_error_code(code)?;
            }
            if !method.retryable_errors.is_subset(&method.errors) {
                return invalid(format!(
                    "service method `{}` marks an undeclared error as retryable",
                    method.name
                ));
            }
        }
        Ok(())
    }
}

fn validate_service_error_code(code: &str) -> Result<()> {
    if code.is_empty()
        || code.len() > 128
        || !code
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
    {
        return invalid(format!("invalid service error code `{code}`"));
    }
    Ok(())
}

impl EgressDeclaration {
    fn validate(&self) -> Result<()> {
        validate_identifier("egress policy", &self.name)?;
        if let Some(provider) = self.provider.as_deref() {
            validate_identifier("egress provider", provider)?;
            if !self.hosts.is_empty() || !self.ports.is_empty() {
                return invalid(format!(
                    "provider-bound egress policy `{}` cannot embed physical hosts or ports",
                    self.name
                ));
            }
        } else if !self.required_provider_headers.is_empty() {
            return invalid(format!(
                "literal egress policy `{}` cannot require provider headers",
                self.name
            ));
        }
        if self.schemes.is_empty()
            || (self.provider.is_none() && (self.hosts.is_empty() || self.ports.is_empty()))
            || self.max_request_bytes == 0
            || self.max_response_bytes == 0
            || self.timeout_ms == 0
            || self.max_concurrency == 0
            || self.requests_per_minute == 0
        {
            return invalid(format!("egress policy `{}` has empty limits", self.name));
        }
        if !self
            .schemes
            .iter()
            .all(|scheme| matches!(scheme.as_str(), "https" | "http"))
        {
            return invalid(format!(
                "egress policy `{}` supports only http/https",
                self.name
            ));
        }
        if self.provider.is_none() && self.schemes.contains("http") && !self.allow_private_networks
        {
            // Cleartext is intentionally limited to an explicit exceptional
            // policy; ordinary internet egress must use TLS.
            return invalid(format!(
                "egress policy `{}` cannot use cleartext HTTP without an explicit private-network exception",
                self.name
            ));
        }
        for host in &self.hosts {
            validate_hostname(host)?;
        }
        let mut provider_headers = BTreeSet::new();
        for header in &self.required_provider_headers {
            validate_http_header_name("egress provider header", header)?;
            if !provider_headers.insert(header.to_ascii_lowercase()) {
                return invalid(format!(
                    "egress policy `{}` repeats provider header `{header}` case-insensitively",
                    self.name
                ));
            }
            if ["host", "content-length", "connection"]
                .iter()
                .any(|reserved| header.eq_ignore_ascii_case(reserved))
            {
                return invalid(format!(
                    "egress policy `{}` requires host-controlled provider header `{header}`",
                    self.name
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationFeature {
    TrustedConnectionIdentity,
    ExactColumnAuthority,
    OidcRs256,
    TypedServiceFailures,
    Crud,
    Upsert,
    Filters,
    Sorting,
    Pagination,
    Aggregates,
    FullTextSearch,
    VectorSearch,
    Spatial,
    Timeseries,
    JsonPath,
    BoundedSql,
    TypedDeclaredSqlProjections,
    ReadCommitted,
    Savepoints,
    RowLocks,
    PublishOnCommit,
    CommitValidators,
    Http,
    Streaming,
    Sse,
    WebSocket,
    Services,
    Broker,
    Jobs,
    Schedules,
    DurableSchedules,
    Secrets,
    Crypto,
    Egress,
    Blobs,
    Tokenizers,
    Embeddings,
    Llm,
    LlmTools,
    Rag,
    Agents,
    Evaluations,
    Tests,
    Observability,
    DelegatedServiceAuthority,
    Rls,
    OnlineMigrations,
    RepeatableRead,
    Serializable,
    PlPgSql,
    AdvisoryLocks,
    PostgreSqlEventTriggers,
    ArbitrarySql,
    ArbitraryPostGis,
    Timescale,
    MaterializedViews,
    RawSockets,
    Filesystem,
    Environment,
    Process,
    Threads,
    Wasi,
}

fn reject_unsupported_features(features: &BTreeSet<ApplicationFeature>) -> Result<()> {
    let unsupported = features
        .iter()
        .filter(|feature| {
            matches!(
                feature,
                ApplicationFeature::PlPgSql
                    | ApplicationFeature::AdvisoryLocks
                    | ApplicationFeature::PostgreSqlEventTriggers
                    | ApplicationFeature::ArbitrarySql
                    | ApplicationFeature::ArbitraryPostGis
                    | ApplicationFeature::Timescale
                    | ApplicationFeature::MaterializedViews
                    | ApplicationFeature::RawSockets
                    | ApplicationFeature::Filesystem
                    | ApplicationFeature::Environment
                    | ApplicationFeature::Process
                    | ApplicationFeature::Threads
                    | ApplicationFeature::Wasi
            )
        })
        .collect::<Vec<_>>();
    if unsupported.is_empty() {
        Ok(())
    } else {
        invalid(format!(
            "BicDB application profile does not support requested features: {unsupported:?}"
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Bool,
    Int64,
    Float64,
    Decimal,
    String,
    Bytes,
    Uuid,
    Timestamp,
    Date,
    Json,
    Vector {
        dimensions: u32,
    },
    Geometry {
        srid: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        geometry_type: Option<ApplicationGeometryTypeV1>,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationGeometryTypeV1 {
    Point,
    LineString,
    Polygon,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationVectorMetricV1 {
    Cosine,
    Euclidean,
    InnerProduct,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationVectorIndexKindV1 {
    Hnsw,
    Ivfflat,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceVectorSearchV1 {
    pub field: String,
    pub dimensions: u32,
    pub metric: ApplicationVectorMetricV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_kind: Option<ApplicationVectorIndexKindV1>,
    pub index_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub text_fields: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceTimeseriesV1 {
    pub time_field: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_interval: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<String>,
    pub index_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ContractField {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_name: Option<String>,
    pub field_type: FieldType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_type: Option<ApplicationRouteParameterTypeV1>,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub generated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_expression: Option<ApplicationExpressionV1>,
    #[serde(default)]
    pub default_json: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceUniqueTargetV1 {
    pub target: String,
    pub fields: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<ApplicationExpressionV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceIndexV1 {
    pub name: String,
    pub fields: Vec<String>,
    #[serde(default, skip_serializing_if = "ResourceIndexKindV1::is_btree")]
    pub kind: ResourceIndexKindV1,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<ApplicationExpressionV1>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceIndexKindV1 {
    #[default]
    Btree,
    Jsonb,
    Array,
    Spatial,
}

impl ResourceIndexKindV1 {
    fn is_btree(value: &Self) -> bool {
        *value == Self::Btree
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceCheckV1 {
    pub name: String,
    pub expression: ApplicationExpressionV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceForeignKeyV1 {
    pub name: String,
    pub fields: Vec<String>,
    pub target_resource: String,
    pub target_fields: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceExclusionElementV1 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    pub fields: Vec<String>,
    pub operator: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceExclusionV1 {
    pub name: String,
    pub elements: Vec<ResourceExclusionElementV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceReverseRelationV1 {
    pub name: String,
    pub target_resource: String,
    pub source_field: String,
    pub target_field: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via_resource: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via_target_field: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceRelationV1 {
    pub source_field: String,
    pub target_resource: String,
    pub target_field: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ResourceOperation {
    List,
    Get,
    Create,
    Upsert,
    Update,
    Delete,
    Restore,
    Action,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourcePolicyRoleMatchV1 {
    Any,
    All,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourcePolicyRuleV1 {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub roles: BTreeSet<String>,
    pub role_match: ResourcePolicyRoleMatchV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expression: Option<ApplicationExpressionV1>,
}

/// PostgreSQL row-level-security predicates compiled from the same BicDB application
/// policy as the typed expression contract. BicDB installs these clauses on
/// the embedded SQL table with RLS enabled and forced, so declared SQL gets
/// PostgreSQL `USING`/`WITH CHECK` behavior instead of bypassing row policy.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceSqlPolicyContractV1 {
    pub select_using: String,
    pub insert_with_check: String,
    pub update_using: String,
    pub update_with_check: String,
    pub delete_using: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourcePolicyContractV1 {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_expression: Option<ApplicationExpressionV1>,
    /// Absence is fail-closed, matching PostgreSQL FORCE RLS when BicDB application did
    /// not emit a SELECT policy for the model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read: Option<ResourcePolicyRuleV1>,
    /// Shared INSERT/UPDATE/DELETE rule. The host applies it to both the old
    /// and candidate row for UPDATE, matching USING plus WITH CHECK.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<ResourcePolicyRuleV1>,
    /// Additional authority needed to observe a soft-deleted row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_read: Option<ResourcePolicyRuleV1>,
    /// Signed SQL projection of this policy. Older packages omit it and remain
    /// valid for typed resource operations, but declared SQL must fail closed
    /// when an expression policy has no native SQL projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<ResourceSqlPolicyContractV1>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationInvariantKindV1 {
    MustAlways,
    MustNever,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationInvariantTransitionV1 {
    pub field: String,
    pub value: ApplicationExpressionV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationInvariantV1 {
    pub version: u32,
    pub name: String,
    pub subject_resource: String,
    pub kind: ApplicationInvariantKindV1,
    pub expression: ApplicationExpressionV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition: Option<ApplicationInvariantTransitionV1>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub dependency_resources: BTreeSet<String>,
    pub source: String,
}

impl ApplicationInvariantV1 {
    fn validate(&self, application: &ApplicationManifestV2) -> Result<()> {
        if self.version != 1 {
            return invalid(format!("invariant `{}` requires version 1", self.name));
        }
        validate_identifier("invariant", &self.name)?;
        validate_nonempty("invariant source", &self.source, 16 * 1024)?;
        let subject = application
            .resources
            .iter()
            .find(|resource| resource.name == self.subject_resource)
            .ok_or_else(|| {
                ExtensionError::InvalidManifest(format!(
                    "invariant `{}` references unknown subject resource `{}`",
                    self.name, self.subject_resource
                ))
            })?;
        let mut dependencies = BTreeSet::from([self.subject_resource.clone()]);
        let bindings = BTreeMap::new();
        validate_invariant_expression(
            application,
            subject,
            &bindings,
            &self.expression,
            &mut dependencies,
            0,
        )?;
        if dependencies != self.dependency_resources {
            return invalid(format!(
                "invariant `{}` dependency resources do not match its expression",
                self.name
            ));
        }
        if let Some(transition) = &self.transition {
            if !subject
                .fields
                .iter()
                .any(|field| field.name == transition.field)
            {
                return invalid(format!(
                    "invariant `{}` transition references undeclared subject field `{}`",
                    self.name, transition.field
                ));
            }
            validate_invariant_expression(
                application,
                subject,
                &bindings,
                &transition.value,
                &mut dependencies,
                0,
            )?;
        }
        Ok(())
    }
}

fn validate_invariant_expression(
    application: &ApplicationManifestV2,
    subject: &ResourceContractV1,
    bindings: &BTreeMap<String, String>,
    expression: &ApplicationExpressionV1,
    dependencies: &mut BTreeSet<String>,
    depth: usize,
) -> Result<()> {
    if depth > 64 {
        return invalid("invariant expression exceeds maximum depth");
    }
    let recurse = |value: &ApplicationExpressionV1,
                   bindings: &BTreeMap<String, String>,
                   dependencies: &mut BTreeSet<String>|
     -> Result<()> {
        validate_invariant_expression(
            application,
            subject,
            bindings,
            value,
            dependencies,
            depth + 1,
        )
    };
    match expression {
        ApplicationExpressionV1::Variable { name } => {
            if name == "subject"
                || subject.fields.iter().any(|field| field.name == *name)
                || bindings.contains_key(name)
            {
                Ok(())
            } else {
                invalid(format!("invariant references undeclared variable `{name}`"))
            }
        }
        ApplicationExpressionV1::Literal { value } => {
            if value.is_object() {
                invalid("invariant literals must be scalar or arrays of scalars")
            } else {
                Ok(())
            }
        }
        ApplicationExpressionV1::Field { target, field } => {
            let ApplicationExpressionV1::Variable { name } = target.as_ref() else {
                return invalid("invariant field access requires a subject or binding variable");
            };
            let resource = if name == "subject" {
                subject
            } else {
                let resource_name = bindings.get(name).ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "invariant field access uses unknown binding `{name}`"
                    ))
                })?;
                application
                    .resources
                    .iter()
                    .find(|resource| resource.name == *resource_name)
                    .expect("binding resources are validated")
            };
            if resource
                .fields
                .iter()
                .any(|candidate| candidate.name == *field)
            {
                Ok(())
            } else {
                invalid(format!(
                    "invariant binding `{name}` references undeclared field `{field}`"
                ))
            }
        }
        ApplicationExpressionV1::Unary {
            operator, value, ..
        } => {
            if !matches!(operator.as_str(), "not" | "negate") {
                return invalid(format!(
                    "invariant uses unsupported unary operator `{operator}`"
                ));
            }
            recurse(value, bindings, dependencies)
        }
        ApplicationExpressionV1::Binary {
            operator,
            left,
            right,
            ..
        } => {
            if !matches!(
                operator.as_str(),
                "add"
                    | "subtract"
                    | "multiply"
                    | "divide"
                    | "and"
                    | "or"
                    | "implies"
                    | "contains"
                    | "equal"
                    | "not_equal"
                    | "greater"
                    | "greater_equal"
                    | "less"
                    | "less_equal"
            ) {
                return invalid(format!(
                    "invariant uses unsupported binary operator `{operator}`"
                ));
            }
            recurse(left, bindings, dependencies)?;
            recurse(right, bindings, dependencies)
        }
        ApplicationExpressionV1::Call {
            kind,
            target,
            arguments,
            ..
        } if *kind == ApplicationCallKindV1::Builtin && target == "overlaps" => {
            if arguments.len() != 4 {
                return invalid("invariant overlaps requires four arguments");
            }
            for argument in arguments {
                recurse(&argument.value, bindings, dependencies)?;
            }
            Ok(())
        }
        ApplicationExpressionV1::Exists {
            binding,
            resource,
            condition,
        }
        | ApplicationExpressionV1::Aggregate {
            binding,
            resource,
            condition,
            ..
        } => {
            validate_identifier("invariant binding", binding)?;
            if binding == "subject" || bindings.contains_key(binding) {
                return invalid(format!("duplicate invariant binding `{binding}`"));
            }
            if !application
                .resources
                .iter()
                .any(|candidate| candidate.name == *resource)
            {
                return invalid(format!(
                    "invariant binding `{binding}` references unknown resource `{resource}`"
                ));
            }
            dependencies.insert(resource.clone());
            let mut nested = bindings.clone();
            nested.insert(binding.clone(), resource.clone());
            recurse(condition, &nested, dependencies)?;
            if let ApplicationExpressionV1::Aggregate {
                function, field, ..
            } = expression
            {
                let resource_contract = application
                    .resources
                    .iter()
                    .find(|candidate| candidate.name == *resource)
                    .expect("resource checked");
                match function {
                    ApplicationAggregateFunctionV1::Count if field.is_none() => {}
                    ApplicationAggregateFunctionV1::Sum
                        if field.as_ref().is_some_and(|field| {
                            resource_contract
                                .fields
                                .iter()
                                .any(|candidate| candidate.name == *field)
                        }) => {}
                    ApplicationAggregateFunctionV1::Count => {
                        return invalid("count invariant aggregate must not declare a field");
                    }
                    ApplicationAggregateFunctionV1::Sum => {
                        return invalid("sum invariant aggregate requires a declared field");
                    }
                }
            }
            Ok(())
        }
        _ => invalid("invariant contains an expression outside the enforced runtime corpus"),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceContractV1 {
    pub version: u32,
    pub name: String,
    pub relation: String,
    pub schema_version: u64,
    #[serde(default, skip_serializing_if = "is_false")]
    pub schema_only: bool,
    pub primary_key: String,
    pub fields: Vec<ContractField>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_search: Option<ResourceVectorSearchV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeseries: Option<ResourceTimeseriesV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unique_targets: Vec<ResourceUniqueTargetV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<ResourceIndexV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<ResourceCheckV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_keys: Vec<ResourceForeignKeyV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclusions: Vec<ResourceExclusionV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reverse_relations: Vec<ResourceReverseRelationV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<ResourceRelationV1>,
    #[serde(default)]
    pub create_fields: BTreeSet<String>,
    #[serde(default)]
    pub update_fields: BTreeSet<String>,
    #[serde(default)]
    pub required_create_fields: BTreeSet<String>,
    /// Fields populated by the trusted runtime after request validation.
    /// They stay out of public request schemas while remaining inside the
    /// signed mutation authority granted to the host.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub server_managed_fields: BTreeSet<String>,
    #[serde(default)]
    pub validation: Vec<ValidationRule>,
    #[serde(default)]
    pub operations: BTreeSet<ResourceOperation>,
    #[serde(default)]
    pub filters: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter_contracts: Vec<ResourceFilterContract>,
    #[serde(default)]
    pub relation_filters: BTreeSet<String>,
    #[serde(default)]
    pub json_path_filters: BTreeSet<String>,
    #[serde(default)]
    pub search_fields: BTreeSet<String>,
    #[serde(default)]
    pub sort_fields: BTreeSet<String>,
    /// Optional host-applied list defaults. Omitted by older ABI-v2 packages,
    /// preserving their canonical signing envelope and historical defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_defaults: Option<ResourceListDefaults>,
    pub list_route: String,
    pub item_route: String,
    #[serde(default)]
    pub soft_delete_field: Option<String>,
    /// Exact deleted sentinel for enum-state soft deletion. Its absence means
    /// the soft-delete field is the legacy nullable timestamp form.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft_delete_value: Option<JsonValue>,
    /// Exact active value restored for enum-state soft deletion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_value: Option<JsonValue>,
    #[serde(default)]
    pub version_field: Option<String>,
    #[serde(default)]
    pub tenant_field: Option<String>,
    #[serde(default)]
    pub workspace_field: Option<String>,
    /// Compiler-signed BicDB application row policy. BicDB evaluates this against the
    /// trusted immutable actor and the row; no SQL session state is involved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<ResourcePolicyContractV1>,
    #[serde(default)]
    pub required_roles: BTreeSet<String>,
    #[serde(default)]
    pub required_scopes: BTreeSet<String>,
    /// Trusted actor policy attributes that must exactly match for every
    /// operation on this resource.
    #[serde(default)]
    pub policy_attributes: BTreeMap<String, String>,
    #[serde(default)]
    pub read_roles: BTreeMap<String, BTreeSet<String>>,
    #[serde(default)]
    pub redacted_fields: BTreeSet<String>,
    #[serde(default)]
    pub immutable_fields: BTreeSet<String>,
    /// Field-to-secret bindings for host-owned encryption at rest. The secret
    /// name is an opaque signed handle; key bytes never enter the package.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub encrypted_fields: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privacy: Option<ResourcePrivacyContractV1>,
    #[serde(default)]
    pub idempotency: Option<IdempotencyContract>,
    #[serde(default)]
    pub cache: Option<ResourceCacheContract>,
    #[serde(default)]
    pub audit: AuditContract,
    #[serde(default)]
    pub events: Vec<ResourceEventContract>,
    #[serde(default)]
    pub operation_metadata: Vec<ResourceOperationMetadata>,
    pub contract_sha256: String,
    #[serde(default)]
    pub openapi: JsonValue,
}

impl ResourceContractV1 {
    fn validate(&self, application: &ApplicationManifestV2) -> Result<()> {
        if self.version != 1 {
            return invalid(format!(
                "resource `{}` uses unsupported contract version {}",
                self.name, self.version
            ));
        }
        validate_identifier("resource", &self.name)?;
        validate_qualified("resource relation", &self.relation)?;
        validate_identifier("primary key", &self.primary_key)?;
        validate_route_template(&self.list_route)?;
        validate_route_template(&self.item_route)?;
        validate_sha256("resource contract hash", &self.contract_sha256)?;
        if self.schema_version == 0
            || self.fields.is_empty()
            || self.schema_only == !self.operations.is_empty()
        {
            return invalid(format!(
                "resource `{}` requires a schema version and fields, with operations exactly when it is not schema-only",
                self.name
            ));
        }
        unique_by(
            "resource field",
            self.fields.iter().map(|field| &field.name),
        )?;
        let fields = self
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<BTreeSet<_>>();
        let mut storage_names = BTreeSet::new();
        for field in &self.fields {
            validate_identifier("resource field", &field.name)?;
            let storage_name = field.storage_name.as_deref().unwrap_or(&field.name);
            validate_identifier("resource field storage name", storage_name)?;
            if !storage_names.insert(storage_name.to_ascii_lowercase()) {
                return invalid(format!(
                    "resource `{}` repeats physical field `{storage_name}`",
                    self.name
                ));
            }
            if field.generated != field.generated_expression.is_some() {
                return invalid(format!(
                    "resource `{}.{}` has incomplete generated-field metadata",
                    self.name, field.name
                ));
            }
            if field.generated && field.default_json.is_some() {
                return invalid(format!(
                    "resource `{}.{}` cannot combine a generated expression and default",
                    self.name, field.name
                ));
            }
            if let Some(expression) = &field.generated_expression {
                validate_carrier_expression(expression)?;
            }
            if let Some(value_type) = &field.value_type {
                value_type.validate(0)?;
            }
            if let Some(default) = &field.default_json {
                serde_json::from_str::<JsonValue>(default).map_err(|error| {
                    ExtensionError::InvalidManifest(format!(
                        "resource `{}.{}` has invalid default JSON: {error}",
                        self.name, field.name
                    ))
                })?;
            }
            if matches!(field.field_type, FieldType::Vector { dimensions: 0 }) {
                return invalid(format!(
                    "resource `{}.{}` has a zero-dimensional vector type",
                    self.name, field.name
                ));
            }
            if matches!(field.field_type, FieldType::Geometry { srid: 0, .. }) {
                return invalid(format!(
                    "resource `{}.{}` has an invalid zero spatial reference",
                    self.name, field.name
                ));
            }
        }
        if let Some(vector) = &self.vector_search {
            validate_identifier("resource vector field", &vector.field)?;
            validate_identifier("resource vector index", &vector.index_name)?;
            let field = self
                .fields
                .iter()
                .find(|field| field.name == vector.field)
                .ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "resource `{}` vector search references undeclared field `{}`",
                        self.name, vector.field
                    ))
                })?;
            if field.field_type
                != (FieldType::Vector {
                    dimensions: vector.dimensions,
                })
                || vector.dimensions == 0
            {
                return invalid(format!(
                    "resource `{}.{}` vector search dimensions do not match its field type",
                    self.name, vector.field
                ));
            }
            let mut text_fields = BTreeSet::new();
            for text_field in &vector.text_fields {
                if !text_fields.insert(text_field)
                    || !self.search_fields.contains(text_field)
                    || !self.fields.iter().any(|field| {
                        field.name == *text_field && field.field_type == FieldType::String
                    })
                {
                    return invalid(format!(
                        "resource `{}` vector search has an invalid text field `{text_field}`",
                        self.name
                    ));
                }
            }
        }
        if let Some(timeseries) = &self.timeseries {
            validate_identifier("resource timeseries field", &timeseries.time_field)?;
            validate_identifier("resource timeseries index", &timeseries.index_name)?;
            if self
                .fields
                .iter()
                .find(|field| field.name == timeseries.time_field)
                .is_none_or(|field| field.field_type != FieldType::Timestamp)
                || timeseries
                    .chunk_interval
                    .as_deref()
                    .is_some_and(|value| value.trim().is_empty() || value.len() > 256)
                || timeseries
                    .retention
                    .as_deref()
                    .is_some_and(|value| value.trim().is_empty() || value.len() > 256)
            {
                return invalid(format!(
                    "resource `{}` has an invalid signed timeseries contract",
                    self.name
                ));
            }
        }
        if !fields.contains(self.primary_key.as_str()) {
            return invalid(format!(
                "resource `{}` primary key is not a declared field",
                self.name
            ));
        }
        unique_by(
            "resource unique target",
            self.unique_targets.iter().map(|target| &target.target),
        )?;
        for target in &self.unique_targets {
            validate_identifier("resource unique target", &target.target)?;
            if let Some(index_name) = &target.index_name {
                validate_identifier("resource unique index", index_name)?;
            }
            if target.fields.is_empty() || target.fields.len() > 16 {
                return invalid(format!(
                    "resource `{}` unique target `{}` requires 1..=16 fields",
                    self.name, target.target
                ));
            }
            let target_fields = target.fields.iter().collect::<BTreeSet<_>>();
            if target_fields.len() != target.fields.len()
                || target
                    .fields
                    .iter()
                    .any(|field| !fields.contains(field.as_str()))
            {
                return invalid(format!(
                    "resource `{}` unique target `{}` has duplicate or undeclared fields",
                    self.name, target.target
                ));
            }
            if let Some(predicate) = &target.predicate {
                validate_resource_index_predicate(predicate, &fields)?;
            }
        }
        unique_by(
            "resource index",
            self.indexes.iter().map(|index| &index.name),
        )?;
        for index in &self.indexes {
            validate_identifier("resource index", &index.name)?;
            if index.fields.is_empty() || index.fields.len() > 16 {
                return invalid(format!(
                    "resource `{}` index `{}` requires 1..=16 fields",
                    self.name, index.name
                ));
            }
            let index_fields = index.fields.iter().collect::<BTreeSet<_>>();
            if index_fields.len() != index.fields.len()
                || index
                    .fields
                    .iter()
                    .any(|field| !fields.contains(field.as_str()))
            {
                return invalid(format!(
                    "resource `{}` index `{}` has duplicate or undeclared fields",
                    self.name, index.name
                ));
            }
            if let Some(predicate) = &index.predicate {
                if index.kind != ResourceIndexKindV1::Btree {
                    return invalid(format!(
                        "resource `{}` specialized index `{}` cannot be partial",
                        self.name, index.name
                    ));
                }
                validate_resource_index_predicate(predicate, &fields)?;
            }
            if !index.paths.is_empty() {
                if index.paths.len() != index.fields.len()
                    || index.paths.iter().any(|path| {
                        path.is_empty()
                            || path
                                .iter()
                                .any(|segment| segment.is_empty() || segment.len() > 256)
                    })
                    || index
                        .paths
                        .iter()
                        .zip(&index.fields)
                        .any(|(path, field)| path.first() != Some(field))
                {
                    return invalid(format!(
                        "resource `{}` index `{}` has invalid signed field paths",
                        self.name, index.name
                    ));
                }
            }
            if index.kind != ResourceIndexKindV1::Btree
                && (index.fields.len() != 1 || index.paths.len() != 1)
            {
                return invalid(format!(
                    "resource `{}` specialized index `{}` requires one signed field path",
                    self.name, index.name
                ));
            }
            if index.kind == ResourceIndexKindV1::Spatial
                && self
                    .fields
                    .iter()
                    .find(|field| field.name == index.fields[0])
                    .is_none_or(|field| {
                        !matches!(
                            field.field_type,
                            FieldType::Geometry {
                                geometry_type: Some(_),
                                ..
                            }
                        )
                    })
            {
                return invalid(format!(
                    "resource `{}` spatial index `{}` requires an exactly typed geometry field",
                    self.name, index.name
                ));
            }
        }
        if let Some(timeseries) = &self.timeseries {
            if !self.indexes.iter().any(|index| {
                index.name == timeseries.index_name
                    && index.kind == ResourceIndexKindV1::Btree
                    && index.fields.len() == 1
                    && index.fields[0] == timeseries.time_field
            }) {
                return invalid(format!(
                    "resource `{}` timeseries contract lacks its signed time index",
                    self.name
                ));
            }
        }
        unique_by(
            "resource check",
            self.checks.iter().map(|check| &check.name),
        )?;
        for check in &self.checks {
            validate_identifier("resource check", &check.name)?;
            validate_carrier_expression(&check.expression)?;
        }
        unique_by(
            "resource foreign key",
            self.foreign_keys
                .iter()
                .map(|foreign_key| &foreign_key.name),
        )?;
        for foreign_key in &self.foreign_keys {
            validate_identifier("resource foreign key", &foreign_key.name)?;
            validate_identifier("resource foreign key target", &foreign_key.target_resource)?;
            if foreign_key.fields.is_empty()
                || foreign_key.fields.len() > 16
                || foreign_key.fields.len() != foreign_key.target_fields.len()
            {
                return invalid(format!(
                    "resource `{}` foreign key `{}` requires 1..=16 paired fields",
                    self.name, foreign_key.name
                ));
            }
            let source_fields = foreign_key.fields.iter().collect::<BTreeSet<_>>();
            let target_fields = foreign_key.target_fields.iter().collect::<BTreeSet<_>>();
            if source_fields.len() != foreign_key.fields.len()
                || target_fields.len() != foreign_key.target_fields.len()
                || foreign_key
                    .fields
                    .iter()
                    .any(|field| !fields.contains(field.as_str()))
            {
                return invalid(format!(
                    "resource `{}` foreign key `{}` has duplicate or undeclared source fields",
                    self.name, foreign_key.name
                ));
            }
            for target_field in &foreign_key.target_fields {
                validate_identifier("resource foreign key target field", target_field)?;
            }
            let target = application
                .resources
                .iter()
                .find(|resource| resource.name == foreign_key.target_resource)
                .ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "resource `{}` foreign key `{}` target `{}` is absent",
                        self.name, foreign_key.name, foreign_key.target_resource
                    ))
                })?;
            let target_is_unique = (foreign_key.target_fields.len() == 1
                && foreign_key.target_fields[0] == target.primary_key)
                || target
                    .unique_targets
                    .iter()
                    .any(|unique| unique.fields == foreign_key.target_fields);
            let compatible = foreign_key
                .fields
                .iter()
                .zip(&foreign_key.target_fields)
                .all(|(source_name, target_name)| {
                    self.fields
                        .iter()
                        .find(|field| field.name == *source_name)
                        .zip(
                            target
                                .fields
                                .iter()
                                .find(|field| field.name == *target_name),
                        )
                        .is_some_and(|(source, target)| source.field_type == target.field_type)
                });
            let has_read_authority = self.schema_only
                || application
                    .permission_for(&self.relation)
                    .filter(|permission| permission.actions.contains(&DatabaseAction::Select))
                    .zip(
                        application
                            .permission_for(&target.relation)
                            .filter(|permission| {
                                permission.actions.contains(&DatabaseAction::Select)
                            }),
                    )
                    .is_some_and(|(source, target)| {
                        foreign_key
                            .fields
                            .iter()
                            .all(|field| readable_column_allowed(application, source, field))
                            && foreign_key
                                .target_fields
                                .iter()
                                .all(|field| readable_column_allowed(application, target, field))
                    });
            if !target_is_unique || !compatible || !has_read_authority {
                return invalid(format!(
                    "resource `{}` foreign key `{}` has a non-unique target, incompatible fields, or insufficient read authority",
                    self.name, foreign_key.name
                ));
            }
        }
        unique_by(
            "resource exclusion",
            self.exclusions.iter().map(|exclusion| &exclusion.name),
        )?;
        for exclusion in &self.exclusions {
            validate_identifier("resource exclusion", &exclusion.name)?;
            if exclusion.elements.is_empty() || exclusion.elements.len() > 16 {
                return invalid(format!(
                    "resource `{}` exclusion `{}` requires 1..=16 elements",
                    self.name, exclusion.name
                ));
            }
            let mut exclusion_fields = BTreeSet::new();
            for element in &exclusion.elements {
                if element.fields.iter().any(|field| {
                    !fields.contains(field.as_str()) || !exclusion_fields.insert(field.as_str())
                }) {
                    return invalid(format!(
                        "resource `{}` exclusion `{}` has duplicate or undeclared fields",
                        self.name, exclusion.name
                    ));
                }
                match (element.function.as_deref(), element.operator.as_str()) {
                    (None, "=") if element.fields.len() == 1 => {}
                    (Some(function), "&&") if element.fields.len() == 2 => {
                        let expected = match function {
                            "daterange" => FieldType::Date,
                            "tsrange" | "tstzrange" => FieldType::Timestamp,
                            "int4range" | "int8range" => FieldType::Int64,
                            "numrange" => FieldType::Decimal,
                            _ => {
                                return invalid(format!(
                                    "resource `{}` exclusion `{}` uses unsupported range constructor `{function}`",
                                    self.name, exclusion.name
                                ));
                            }
                        };
                        if element.fields.iter().any(|name| {
                            self.fields
                                .iter()
                                .find(|field| field.name == *name)
                                .is_none_or(|field| field.field_type != expected)
                        }) {
                            return invalid(format!(
                                "resource `{}` exclusion `{}` range fields do not match `{function}`",
                                self.name, exclusion.name
                            ));
                        }
                    }
                    _ => {
                        return invalid(format!(
                            "resource `{}` exclusion `{}` requires `<field> with =` or a supported two-field range with &&",
                            self.name, exclusion.name
                        ));
                    }
                }
            }
        }
        unique_by(
            "resource relation",
            self.relations.iter().map(|relation| &relation.source_field),
        )?;
        for relation in &self.relations {
            validate_identifier("resource relation source field", &relation.source_field)?;
            validate_identifier("resource relation target", &relation.target_resource)?;
            validate_identifier("resource relation target field", &relation.target_field)?;
            let source_field = self
                .fields
                .iter()
                .find(|field| field.name == relation.source_field)
                .ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "resource `{}` relation source field `{}` is absent",
                        self.name, relation.source_field
                    ))
                })?;
            let target = application
                .resources
                .iter()
                .find(|resource| resource.name == relation.target_resource)
                .ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "resource `{}` relation target `{}` is absent",
                        self.name, relation.target_resource
                    ))
                })?;
            let target_field = target
                .fields
                .iter()
                .find(|field| field.name == relation.target_field)
                .ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "resource `{}` relation target field `{}.{}` is absent",
                        self.name, relation.target_resource, relation.target_field
                    ))
                })?;
            let source_permission = application
                .permission_for(&self.relation)
                .filter(|permission| permission.actions.contains(&DatabaseAction::Select));
            let target_permission = application
                .permission_for(&target.relation)
                .filter(|permission| permission.actions.contains(&DatabaseAction::Select));
            if source_field.field_type != target_field.field_type
                || (!self.schema_only
                    && (source_permission.is_none_or(|permission| {
                        !readable_column_allowed(application, permission, &relation.source_field)
                    }) || target_permission.is_none_or(|permission| {
                        !readable_column_allowed(application, permission, &relation.target_field)
                    })))
            {
                return invalid(format!(
                    "resource `{}` relation `{}` has incompatible fields or read authority",
                    self.name, relation.source_field
                ));
            }
        }
        unique_by(
            "resource reverse relation",
            self.reverse_relations.iter().map(|relation| &relation.name),
        )?;
        for relation in &self.reverse_relations {
            validate_identifier("resource reverse relation", &relation.name)?;
            validate_identifier(
                "resource reverse relation target",
                &relation.target_resource,
            )?;
            validate_identifier(
                "resource reverse relation source field",
                &relation.source_field,
            )?;
            validate_identifier(
                "resource reverse relation target field",
                &relation.target_field,
            )?;
            let owner_field = self
                .fields
                .iter()
                .find(|field| field.name == relation.target_field)
                .ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "resource `{}` reverse relation `{}` has an undeclared target field",
                        self.name, relation.name
                    ))
                })?;
            let target = application
                .resources
                .iter()
                .find(|resource| resource.name == relation.target_resource)
                .ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "resource `{}` reverse relation `{}` target is absent",
                        self.name, relation.name
                    ))
                })?;
            let target_permission = application
                .permission_for(&target.relation)
                .filter(|permission| {
                    permission.actions.contains(&DatabaseAction::Select)
                        && permission.actions.contains(&DatabaseAction::Aggregate)
                })
                .ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "resource `{}` reverse relation `{}` target lacks list authority",
                        self.name, relation.name
                    ))
                })?;
            let compatible = match (&relation.via_resource, &relation.via_target_field) {
                (None, None) => target
                    .fields
                    .iter()
                    .find(|field| field.name == relation.source_field)
                    .is_some_and(|source_field| {
                        owner_field.field_type == source_field.field_type
                            && target.relations.iter().any(|target_relation| {
                                target_relation.source_field == relation.source_field
                                    && target_relation.target_resource == self.name
                                    && target_relation.target_field == relation.target_field
                            })
                            && readable_column_allowed(
                                application,
                                target_permission,
                                &relation.source_field,
                            )
                    }),
                (Some(via_resource), Some(via_target_field)) => {
                    validate_identifier("resource reverse relation via", via_resource)?;
                    validate_identifier(
                        "resource reverse relation via target field",
                        via_target_field,
                    )?;
                    application
                        .resources
                        .iter()
                        .find(|resource| resource.name == *via_resource)
                        .is_some_and(|via| {
                            let via_source = via
                                .fields
                                .iter()
                                .find(|field| field.name == relation.source_field);
                            let via_target = via
                                .fields
                                .iter()
                                .find(|field| field.name == *via_target_field);
                            let target_relation = via.relations.iter().find(|candidate| {
                                candidate.source_field == *via_target_field
                                    && candidate.target_resource == target.name
                            });
                            let target_key = target_relation.and_then(|candidate| {
                                target
                                    .fields
                                    .iter()
                                    .find(|field| field.name == candidate.target_field)
                            });
                            let via_permission =
                                application
                                    .permission_for(&via.relation)
                                    .filter(|permission| {
                                        permission.actions.contains(&DatabaseAction::Select)
                                    });
                            via.operations.contains(&ResourceOperation::List)
                                && via_source.is_some_and(|source| {
                                    owner_field.field_type == source.field_type
                                })
                                && via_target.zip(target_key).is_some_and(|(source, target)| {
                                    source.field_type == target.field_type
                                })
                                && via.relations.iter().any(|candidate| {
                                    candidate.source_field == relation.source_field
                                        && candidate.target_resource == self.name
                                        && candidate.target_field == relation.target_field
                                })
                                && via_permission.is_some_and(|permission| {
                                    readable_column_allowed(
                                        application,
                                        permission,
                                        &relation.source_field,
                                    ) && readable_column_allowed(
                                        application,
                                        permission,
                                        via_target_field,
                                    )
                                })
                                && target_relation.is_some_and(|target_relation| {
                                    readable_column_allowed(
                                        application,
                                        target_permission,
                                        &target_relation.target_field,
                                    )
                                })
                        })
                }
                _ => false,
            };
            if !target.operations.contains(&ResourceOperation::List) || !compatible {
                return invalid(format!(
                    "resource `{}` reverse relation `{}` has incompatible fields, join metadata, target operations, or column authority",
                    self.name, relation.name
                ));
            }
        }
        for (label, names) in [
            ("create field", &self.create_fields),
            ("update field", &self.update_fields),
            ("required create field", &self.required_create_fields),
            ("server-managed field", &self.server_managed_fields),
            ("filter", &self.filters),
            ("JSON-path filter", &self.json_path_filters),
            ("search field", &self.search_fields),
            ("sort field", &self.sort_fields),
            ("redacted field", &self.redacted_fields),
            ("immutable field", &self.immutable_fields),
        ] {
            for name in names {
                if !fields.contains(name.as_str()) {
                    return invalid(format!(
                        "resource `{}` {label} `{name}` is not declared",
                        self.name
                    ));
                }
            }
        }
        if let Some(defaults) = &self.list_defaults {
            if defaults.limit == 0 || defaults.limit > 1_000 || defaults.sort.is_empty() {
                return invalid(format!(
                    "resource `{}` has invalid list defaults",
                    self.name
                ));
            }
            for sort in &defaults.sort {
                if !self.sort_fields.contains(&sort.field) {
                    return invalid(format!(
                        "resource `{}` default sort `{}` is undeclared",
                        self.name, sort.field
                    ));
                }
            }
        }
        for rule in &self.validation {
            rule.validate(&self.name, &fields)?;
        }
        let mut filter_query_names = BTreeSet::new();
        for filter in &self.filter_contracts {
            filter.validate(application, self, &fields)?;
            for query_name in filter.query_names() {
                if !filter_query_names.insert(query_name) {
                    return invalid(format!(
                        "resource `{}` repeats filter query parameters",
                        self.name
                    ));
                }
            }
        }
        for special in [
            self.soft_delete_field.as_ref(),
            self.version_field.as_ref(),
            self.tenant_field.as_ref(),
            self.workspace_field.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if !fields.contains(special.as_str()) {
                return invalid(format!(
                    "resource `{}` special field `{special}` is not declared",
                    self.name
                ));
            }
        }
        match (
            self.soft_delete_field.as_ref(),
            self.soft_delete_value.as_ref(),
            self.restore_value.as_ref(),
        ) {
            (Some(field), Some(deleted), Some(active)) => {
                let field = self
                    .fields
                    .iter()
                    .find(|candidate| &candidate.name == field)
                    .expect("soft-delete field was checked above");
                if field.field_type != FieldType::String
                    || !deleted.is_string()
                    || !active.is_string()
                    || deleted == active
                {
                    return invalid(format!(
                        "resource `{}` has an invalid enum-state soft-delete contract",
                        self.name
                    ));
                }
            }
            (Some(_), None, None) | (None, None, None) => {}
            _ => {
                return invalid(format!(
                    "resource `{}` has an incomplete soft-delete contract",
                    self.name
                ));
            }
        }
        if !self.schema_only
            && !application
                .permission_for(&self.relation)
                .is_some_and(|permission| {
                    self.operations.iter().all(|operation| {
                        let action = match operation {
                            ResourceOperation::List | ResourceOperation::Get => {
                                DatabaseAction::Select
                            }
                            ResourceOperation::Create => DatabaseAction::Insert,
                            ResourceOperation::Upsert => DatabaseAction::Upsert,
                            ResourceOperation::Update | ResourceOperation::Restore => {
                                DatabaseAction::Update
                            }
                            ResourceOperation::Delete if self.soft_delete_field.is_some() => {
                                DatabaseAction::Update
                            }
                            ResourceOperation::Delete => DatabaseAction::Delete,
                            ResourceOperation::Action => DatabaseAction::Update,
                        };
                        permission.actions.contains(&action)
                    }) && (self.search_fields.is_empty()
                        || permission.actions.contains(&DatabaseAction::FullTextSearch))
                })
        {
            return invalid(format!(
                "resource `{}` operations exceed relation permissions",
                self.name
            ));
        }
        if let Some(idempotency) = &self.idempotency {
            idempotency.validate()?;
            if !self.operations.iter().any(|operation| {
                matches!(
                    operation,
                    ResourceOperation::Create | ResourceOperation::Action
                )
            }) {
                return invalid(format!(
                    "resource `{}` idempotency requires create or action authority",
                    self.name
                ));
            }
            if application.routes.iter().any(|route| {
                route.resource.as_deref() == Some(self.name.as_str())
                    && matches!(
                        route.operation,
                        Some(ResourceOperation::Create | ResourceOperation::Action)
                    )
                    && route.public
            }) {
                return invalid(format!(
                    "resource `{}` idempotency requires identity-scoped protected mutation routes",
                    self.name
                ));
            }
        }
        if let Some(cache) = &self.cache {
            cache.validate()?;
            if !self.operations.iter().any(|operation| {
                matches!(operation, ResourceOperation::List | ResourceOperation::Get)
            }) {
                return invalid(format!(
                    "resource `{}` cache requires list or get authority",
                    self.name
                ));
            }
            if !cache.private
                && application.routes.iter().any(|route| {
                    route.resource.as_deref() == Some(self.name.as_str())
                        && matches!(
                            route.operation,
                            Some(ResourceOperation::List | ResourceOperation::Get)
                        )
                        && !route.public
                })
            {
                return invalid(format!(
                    "resource `{}` protected reads require a private cache contract",
                    self.name
                ));
            }
        }
        for (name, value) in &self.policy_attributes {
            validate_nonempty("resource policy attribute", name, 128)?;
            validate_nonempty("resource policy attribute value", value, 1024)?;
        }
        if let Some(policy) = &self.policy {
            if policy.version != 1 {
                return invalid(format!(
                    "resource `{}` policy requires version 1",
                    self.name
                ));
            }
            if self.tenant_field.is_none()
                && policy.tenant_expression.is_none()
                && policy.read.is_none()
                && policy.write.is_none()
            {
                return invalid(format!("resource `{}` has an empty policy", self.name));
            }
            let validate_rule = |label: &str, rule: &ResourcePolicyRuleV1| -> Result<()> {
                validate_string_set("resource policy role", &rule.roles)?;
                if let Some(expression) = &rule.expression {
                    validate_resource_policy_expression(
                        application,
                        self,
                        &BTreeMap::new(),
                        expression,
                        0,
                    )?;
                }
                let _ = label;
                Ok(())
            };
            if let Some(expression) = &policy.tenant_expression {
                validate_resource_policy_expression(
                    application,
                    self,
                    &BTreeMap::new(),
                    expression,
                    0,
                )?;
            }
            if let Some(rule) = &policy.read {
                validate_rule("read", rule)?;
            }
            if let Some(rule) = &policy.write {
                validate_rule("write", rule)?;
            }
            if let Some(rule) = &policy.deleted_read {
                validate_rule("deleted-read", rule)?;
            }
            if let Some(sql) = &policy.sql {
                for (label, expression) in [
                    ("SELECT USING", sql.select_using.as_str()),
                    ("INSERT WITH CHECK", sql.insert_with_check.as_str()),
                    ("UPDATE USING", sql.update_using.as_str()),
                    ("UPDATE WITH CHECK", sql.update_with_check.as_str()),
                    ("DELETE USING", sql.delete_using.as_str()),
                ] {
                    validate_nonempty(
                        &format!("resource policy {label} expression"),
                        expression,
                        65_536,
                    )?;
                }
            }
        }
        let privacy_fields = self
            .privacy
            .as_ref()
            .map(|privacy| privacy.fields.keys().cloned().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        if let Some(privacy) = &self.privacy {
            if privacy.fields.is_empty() && privacy.access_audit.is_none() {
                return invalid(format!(
                    "resource `{}` has an empty privacy contract",
                    self.name
                ));
            }
            for (field_name, field_privacy) in &privacy.fields {
                if !fields.contains(field_name.as_str()) {
                    return invalid(format!(
                        "resource `{}` privacy field `{field_name}` is not declared",
                        self.name
                    ));
                }
                let expected_classification = match field_privacy.pii_category {
                    Some(ResourcePiiCategoryV1::Health) => {
                        ResourcePrivacyClassificationV1::ProtectedData
                    }
                    Some(_) => ResourcePrivacyClassificationV1::Pii,
                    None => ResourcePrivacyClassificationV1::Confidential,
                };
                if field_privacy.classification != expected_classification
                    || field_privacy.encrypted != self.encrypted_fields.contains_key(field_name)
                {
                    return invalid(format!(
                        "resource `{}` privacy field `{field_name}` weakens its classification or encryption contract",
                        self.name
                    ));
                }
            }
            if let Some(access_audit) = &privacy.access_audit {
                validate_route_template(&access_audit.endpoint)?;
                let endpoint_without_id = access_audit.endpoint.replacen("{id}", "", 1);
                if access_audit.endpoint.len() > 512
                    || access_audit.endpoint.match_indices("{id}").count() != 1
                    || endpoint_without_id.contains('{')
                    || endpoint_without_id.contains('}')
                    || access_audit.events.is_empty()
                {
                    return invalid(format!(
                        "resource `{}` has an invalid access-audit endpoint or event contract",
                        self.name
                    ));
                }
                validate_identifier("resource access-audit purpose", &access_audit.purpose)?;
                if !application.routes.iter().any(|route| {
                    route.method == HttpMethod::Post
                        && route.template == access_audit.endpoint
                        && !route.public
                        && route.resource.is_none()
                        && route.operation.is_none()
                        && route.service_call.is_none()
                }) {
                    return invalid(format!(
                        "resource `{}` access-audit endpoint `{}` lacks an exact protected custom action",
                        self.name, access_audit.endpoint
                    ));
                }
            }
        }
        for (field_name, secret_name) in &self.encrypted_fields {
            let field = self
                .fields
                .iter()
                .find(|field| field.name == *field_name)
                .ok_or_else(|| {
                    ExtensionError::InvalidManifest(format!(
                        "resource `{}` encrypted field `{field_name}` is not declared",
                        self.name
                    ))
                })?;
            if field.field_type != FieldType::String
                || self.filters.contains(field_name)
                || self.json_path_filters.contains(field_name)
                || self.search_fields.contains(field_name)
                || self.sort_fields.contains(field_name)
            {
                return invalid(format!(
                    "resource `{}` encrypted field `{field_name}` has an unsupported type, filter, search, or sort surface",
                    self.name
                ));
            }
            let field_reference = format!("{}.{field_name}", self.name);
            if !application
                .application_program
                .as_ref()
                .and_then(|program| program.secret_bindings.get(&field_reference))
                .is_some_and(|binding| binding == secret_name)
                || !application.secrets.iter().any(|secret| {
                    secret.name == *secret_name
                        && secret.operations.contains(&CryptoOperation::Encrypt)
                        && secret.operations.contains(&CryptoOperation::Decrypt)
                        && !secret.allow_plaintext_read
                })
            {
                return invalid(format!(
                    "resource `{}` encrypted field `{field_name}` lacks its exact host-only secret binding",
                    self.name
                ));
            }
        }
        if !privacy_fields.is_empty()
            && (!self.audit.required || !privacy_fields.is_subset(&self.audit.redact_fields))
        {
            return invalid(format!(
                "resource `{}` privacy fields require a redacted durable audit contract",
                self.name
            ));
        }
        for field in self
            .audit
            .include_fields
            .iter()
            .chain(&self.audit.redact_fields)
            .chain(self.read_roles.keys())
        {
            if !fields.contains(field.as_str()) {
                return invalid(format!(
                    "resource `{}` privacy/audit field `{field}` is not declared",
                    self.name
                ));
            }
        }
        for event in &self.events {
            event.validate()?;
            if !self.operations.contains(&event.operation) {
                return invalid(format!(
                    "resource `{}` event uses an unsupported operation",
                    self.name
                ));
            }
            for field in event.before_fields.iter().chain(&event.after_fields) {
                if !fields.contains(field.as_str())
                    || self.redacted_fields.contains(field)
                    || privacy_fields.contains(field)
                {
                    return invalid(format!(
                        "resource `{}` event field `{field}` is absent or private",
                        self.name
                    ));
                }
            }
        }
        let mut operation_metadata = BTreeSet::new();
        for metadata in &self.operation_metadata {
            metadata.validate()?;
            if !self.operations.contains(&metadata.operation)
                || !operation_metadata.insert(metadata.operation)
            {
                return invalid(format!(
                    "resource `{}` has duplicate or unsupported operation metadata",
                    self.name
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceListDefaults {
    pub limit: u32,
    pub sort: Vec<SortField>,
    #[serde(default, skip_serializing_if = "ResourceRecordScope::is_active")]
    pub scope: ResourceRecordScope,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
// `deny_unknown_fields` cannot be placed on a struct that uses `flatten`;
// the flattened tagged enum rejects unknown operator fields itself.
pub struct ResourceFilterContract {
    pub query_name: String,
    #[serde(flatten)]
    pub filter: ResourceFilterKind,
}

impl ResourceFilterContract {
    fn validate(
        &self,
        application: &ApplicationManifestV2,
        resource: &ResourceContractV1,
        fields: &BTreeSet<&str>,
    ) -> Result<()> {
        validate_identifier("resource filter query", &self.query_name)?;
        let declared = |field: &str| fields.contains(field) && resource.filters.contains(field);
        let field_type = |name: &str| {
            resource
                .fields
                .iter()
                .find(|field| field.name == name)
                .map(|field| &field.field_type)
        };
        let declared_json = |field: &str| {
            fields.contains(field)
                && resource.json_path_filters.contains(field)
                && field_type(field) == Some(&FieldType::Json)
        };
        match &self.filter {
            ResourceFilterKind::Exact { field } => {
                if declared(field) {
                    Ok(())
                } else {
                    invalid("exact filter references an undeclared field")
                }
            }
            ResourceFilterKind::Contains { field } => {
                if declared(field) && field_type(field) == Some(&FieldType::String) {
                    Ok(())
                } else {
                    invalid("contains filter requires a declared string field")
                }
            }
            ResourceFilterKind::Minimum { field } => {
                if declared(field)
                    && matches!(
                        field_type(field),
                        Some(
                            FieldType::Int64
                                | FieldType::Float64
                                | FieldType::Decimal
                                | FieldType::Timestamp
                                | FieldType::Date
                        )
                    )
                {
                    Ok(())
                } else {
                    invalid("minimum filter requires a declared ordered field")
                }
            }
            ResourceFilterKind::Exists { field } => {
                let nullable = resource
                    .fields
                    .iter()
                    .find(|candidate| candidate.name == *field)
                    .is_some_and(|candidate| candidate.nullable);
                if declared(field) && nullable {
                    Ok(())
                } else {
                    invalid("exists filter requires a declared nullable field")
                }
            }
            ResourceFilterKind::Overlaps {
                start_field,
                end_field,
            } => {
                let start_type = field_type(start_field);
                if declared(start_field)
                    && declared(end_field)
                    && start_type == field_type(end_field)
                    && matches!(start_type, Some(FieldType::Timestamp | FieldType::Date))
                {
                    Ok(())
                } else {
                    invalid("overlap filter requires declared matching temporal fields")
                }
            }
            ResourceFilterKind::JsonExact {
                field,
                path,
                value_type,
            } => {
                validate_json_filter_path(path)?;
                value_type.validate(0)?;
                if declared_json(field) && json_exact_filter_type(value_type) {
                    Ok(())
                } else {
                    invalid("JSON exact filter requires a declared JSON field and scalar type")
                }
            }
            ResourceFilterKind::JsonContains {
                field,
                path,
                value_type,
                ..
            } => {
                validate_json_filter_path(path)?;
                value_type.validate(0)?;
                if declared_json(field) && value_type == &ApplicationRouteParameterTypeV1::String {
                    Ok(())
                } else {
                    invalid("JSON contains filter requires a declared JSON field and string type")
                }
            }
            ResourceFilterKind::JsonMinimum {
                field,
                path,
                value_type,
            } => {
                validate_json_filter_path(path)?;
                value_type.validate(0)?;
                if declared_json(field)
                    && matches!(
                        value_type,
                        ApplicationRouteParameterTypeV1::Int
                            | ApplicationRouteParameterTypeV1::Float
                            | ApplicationRouteParameterTypeV1::Timestamp
                            | ApplicationRouteParameterTypeV1::Date
                    )
                {
                    Ok(())
                } else {
                    invalid("JSON minimum filter requires a declared JSON field and ordered type")
                }
            }
            ResourceFilterKind::JsonExists { field, path } => {
                validate_json_filter_path(path)?;
                if declared_json(field) {
                    Ok(())
                } else {
                    invalid("JSON exists filter requires a declared JSON field")
                }
            }
            ResourceFilterKind::RelationExact {
                field,
                target_resource,
                target_field,
                value_type,
            } => validate_relation_filter(
                application,
                resource,
                fields,
                field,
                target_resource,
                target_field,
                value_type,
                "exact",
            ),
            ResourceFilterKind::RelationContains {
                field,
                target_resource,
                target_field,
                value_type,
            } => validate_relation_filter(
                application,
                resource,
                fields,
                field,
                target_resource,
                target_field,
                value_type,
                "contains",
            ),
            ResourceFilterKind::RelationMinimum {
                field,
                target_resource,
                target_field,
                value_type,
            } => validate_relation_filter(
                application,
                resource,
                fields,
                field,
                target_resource,
                target_field,
                value_type,
                "minimum",
            ),
        }
    }

    pub fn query_names(&self) -> Vec<String> {
        match &self.filter {
            ResourceFilterKind::Overlaps { .. } => vec![
                format!("{}_start", self.query_name),
                format!("{}_end", self.query_name),
            ],
            _ => vec![self.query_name.clone()],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operator", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceFilterKind {
    Exact {
        field: String,
    },
    Contains {
        field: String,
    },
    Minimum {
        field: String,
    },
    Exists {
        field: String,
    },
    Overlaps {
        start_field: String,
        end_field: String,
    },
    JsonExact {
        field: String,
        path: String,
        value_type: ApplicationRouteParameterTypeV1,
    },
    JsonContains {
        field: String,
        path: String,
        value_type: ApplicationRouteParameterTypeV1,
        array: bool,
    },
    JsonMinimum {
        field: String,
        path: String,
        value_type: ApplicationRouteParameterTypeV1,
    },
    JsonExists {
        field: String,
        path: String,
    },
    RelationExact {
        field: String,
        target_resource: String,
        target_field: String,
        value_type: ApplicationRouteParameterTypeV1,
    },
    RelationContains {
        field: String,
        target_resource: String,
        target_field: String,
        value_type: ApplicationRouteParameterTypeV1,
    },
    RelationMinimum {
        field: String,
        target_resource: String,
        target_field: String,
        value_type: ApplicationRouteParameterTypeV1,
    },
}

fn validate_relation_filter(
    application: &ApplicationManifestV2,
    resource: &ResourceContractV1,
    source_fields: &BTreeSet<&str>,
    source_field: &str,
    target_resource: &str,
    target_field: &str,
    value_type: &ApplicationRouteParameterTypeV1,
    operator: &str,
) -> Result<()> {
    value_type.validate(0)?;
    let Some(source) = resource.fields.iter().find(|field| {
        field.name == source_field
            && source_fields.contains(source_field)
            && resource.relation_filters.contains(source_field)
    }) else {
        return invalid("relation filter references an undeclared source field");
    };
    let Some(target) = application
        .resources
        .iter()
        .find(|candidate| candidate.name == target_resource)
    else {
        return invalid("relation filter target resource is absent");
    };
    let Some(primary) = target
        .fields
        .iter()
        .find(|field| field.name == target.primary_key)
    else {
        return invalid("relation filter target primary key is absent");
    };
    let Some(target_value) = target
        .fields
        .iter()
        .find(|field| field.name == target_field)
    else {
        return invalid("relation filter target field is absent");
    };
    if source.field_type != primary.field_type
        || !carrier_filter_type_matches_field(value_type, &target_value.field_type)
    {
        return invalid("relation filter source, key, or value types do not match");
    }
    let valid_operator = match operator {
        "exact" => json_exact_filter_type(value_type),
        "contains" => value_type == &ApplicationRouteParameterTypeV1::String,
        "minimum" => matches!(
            value_type,
            ApplicationRouteParameterTypeV1::Int
                | ApplicationRouteParameterTypeV1::Float
                | ApplicationRouteParameterTypeV1::Timestamp
                | ApplicationRouteParameterTypeV1::Date
        ),
        _ => false,
    };
    if !valid_operator {
        return invalid("relation filter operator does not match its target type");
    }
    let Some(permission) = application
        .permission_for(&target.relation)
        .filter(|permission| permission.actions.contains(&DatabaseAction::Select))
    else {
        return invalid("relation filter target lacks select authority");
    };
    if !readable_column_allowed(application, permission, &target.primary_key)
        || !readable_column_allowed(application, permission, target_field)
    {
        return invalid("relation filter target columns exceed read authority");
    }
    Ok(())
}

fn carrier_filter_type_matches_field(
    value_type: &ApplicationRouteParameterTypeV1,
    field_type: &FieldType,
) -> bool {
    matches!(
        (value_type, field_type),
        (
            ApplicationRouteParameterTypeV1::String | ApplicationRouteParameterTypeV1::Enum { .. },
            FieldType::String
        ) | (ApplicationRouteParameterTypeV1::Int, FieldType::Int64)
            | (ApplicationRouteParameterTypeV1::Float, FieldType::Float64)
            | (ApplicationRouteParameterTypeV1::Decimal, FieldType::Decimal)
            | (ApplicationRouteParameterTypeV1::Bool, FieldType::Bool)
            | (
                ApplicationRouteParameterTypeV1::Timestamp
                    | ApplicationRouteParameterTypeV1::LocalDateTime,
                FieldType::Timestamp
            )
            | (ApplicationRouteParameterTypeV1::Date, FieldType::Date)
            | (ApplicationRouteParameterTypeV1::Uuid, FieldType::Uuid)
    )
}

fn validate_json_filter_path(path: &str) -> Result<()> {
    if path.is_empty() || path.len() > 2_048 || !path.starts_with('/') {
        return invalid("resource JSON filter path must be a bounded JSON pointer");
    }
    for segment in path[1..].split('/') {
        let bytes = segment.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            if bytes[index] == b'~' {
                if index + 1 >= bytes.len() || !matches!(bytes[index + 1], b'0' | b'1') {
                    return invalid("resource JSON filter path has invalid pointer escaping");
                }
                index += 2;
            } else {
                index += 1;
            }
        }
    }
    Ok(())
}

fn json_exact_filter_type(value_type: &ApplicationRouteParameterTypeV1) -> bool {
    matches!(
        value_type,
        ApplicationRouteParameterTypeV1::String
            | ApplicationRouteParameterTypeV1::Int
            | ApplicationRouteParameterTypeV1::Float
            | ApplicationRouteParameterTypeV1::Decimal
            | ApplicationRouteParameterTypeV1::Bool
            | ApplicationRouteParameterTypeV1::Timestamp
            | ApplicationRouteParameterTypeV1::Date
            | ApplicationRouteParameterTypeV1::LocalDateTime
            | ApplicationRouteParameterTypeV1::TimeZone
            | ApplicationRouteParameterTypeV1::Uuid
            | ApplicationRouteParameterTypeV1::Enum { .. }
    )
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceRecordScope {
    #[default]
    Active,
    All,
    Deleted,
}

impl ResourceRecordScope {
    fn is_active(value: &Self) -> bool {
        *value == Self::Active
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
// `deny_unknown_fields` cannot be placed on a struct that uses `flatten`:
// Serde would reject the flattened `kind` discriminator itself. The flattened
// enum retains strict unknown-field rejection, covered by the regression test.
pub struct ValidationRule {
    pub field: String,
    #[serde(flatten)]
    pub rule: ValidationRuleKind,
}

impl ValidationRule {
    fn validate(&self, resource: &str, fields: &BTreeSet<&str>) -> Result<()> {
        if !fields.contains(self.field.as_str()) {
            return invalid(format!(
                "resource `{resource}` validation field `{}` is not declared",
                self.field
            ));
        }
        match &self.rule {
            ValidationRuleKind::MinLength { value } | ValidationRuleKind::MaxLength { value }
                if *value == 0 =>
            {
                invalid("string validation length must be positive")
            }
            ValidationRuleKind::Minimum { value } => {
                validate_numeric_bound("minimum", value).map(|_| ())
            }
            ValidationRuleKind::Maximum { value } => {
                validate_numeric_bound("maximum", value).map(|_| ())
            }
            ValidationRuleKind::Range { minimum, maximum } => {
                let minimum = validate_numeric_bound("minimum", minimum)?;
                let maximum = validate_numeric_bound("maximum", maximum)?;
                if minimum > maximum {
                    invalid("numeric validation range is invalid")
                } else {
                    Ok(())
                }
            }
            ValidationRuleKind::Pattern { expression } => {
                validate_nonempty("validation pattern", expression, 4096)
            }
            ValidationRuleKind::OneOf { values } if values.is_empty() || values.len() > 1024 => {
                invalid("one_of validation requires 1..=1024 values")
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ValidationRuleKind {
    MinLength {
        value: usize,
    },
    MaxLength {
        value: usize,
    },
    /// Decimal bounds are strings so package serialization is exact and
    /// deterministic across compiler implementations.
    Minimum {
        value: String,
    },
    /// Decimal bounds are strings so package serialization is exact and
    /// deterministic across compiler implementations.
    Maximum {
        value: String,
    },
    /// Decimal bounds are strings so package serialization is exact and
    /// deterministic across compiler implementations.
    Range {
        minimum: String,
        maximum: String,
    },
    Email,
    Pattern {
        expression: String,
    },
    OneOf {
        values: Vec<JsonValue>,
    },
}

fn validate_numeric_bound(kind: &str, value: &str) -> Result<f64> {
    let value = value.parse::<f64>().map_err(|_| {
        ExtensionError::InvalidManifest(format!("numeric validation {kind} is invalid"))
    })?;
    if value.is_finite() {
        Ok(value)
    } else {
        invalid(format!("numeric validation {kind} is invalid"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceCacheContract {
    pub max_age_seconds: u64,
    #[serde(default)]
    pub private: bool,
    #[serde(default)]
    pub vary: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RouteCacheContract {
    pub ttl_seconds: u64,
}

impl RouteCacheContract {
    fn validate(&self) -> Result<()> {
        if self.ttl_seconds == 0 {
            return invalid("route cache ttl_seconds must be positive");
        }
        Ok(())
    }
}

impl ResourceCacheContract {
    fn validate(&self) -> Result<()> {
        if self.max_age_seconds > 31_536_000 {
            return invalid("resource cache max_age_seconds exceeds one year");
        }
        for header in &self.vary {
            validate_http_header_name("cache vary header", header)?;
            if header != &header.to_ascii_lowercase()
                || matches!(
                    header.as_str(),
                    "cache-control" | "vary" | "set-cookie" | "content-length"
                )
            {
                return invalid("cache vary headers must be lowercase and guest-safe");
            }
        }
        if !self.private && (self.vary.contains("authorization") || self.vary.contains("cookie")) {
            return invalid("public resource cache cannot vary on credentials");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceOperationMetadata {
    pub operation: ResourceOperation,
    pub operation_id: String,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub tags: BTreeSet<String>,
}

impl ResourceOperationMetadata {
    fn validate(&self) -> Result<()> {
        validate_identifier("resource operation id", &self.operation_id)?;
        if !self.summary.is_empty() {
            validate_nonempty("resource operation summary", &self.summary, 1024)?;
        }
        validate_string_set("resource operation tag", &self.tags)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IdempotencyContract {
    pub header: String,
    pub ttl_seconds: u64,
    pub max_key_bytes: u32,
}

impl IdempotencyContract {
    fn validate(&self) -> Result<()> {
        validate_http_header_name("idempotency header", &self.header)?;
        if self.header != self.header.to_ascii_lowercase()
            || matches!(
                self.header.as_str(),
                "authorization"
                    | "cookie"
                    | "set-cookie"
                    | "host"
                    | "content-length"
                    | "connection"
                    | "cache-control"
                    | "vary"
            )
        {
            return invalid("idempotency header must be lowercase and guest-safe");
        }
        if self.ttl_seconds == 0
            || self.ttl_seconds > 31_536_000
            || self.max_key_bytes == 0
            || self.max_key_bytes > 4096
        {
            return invalid("invalid resource idempotency limits");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AuditContract {
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub include_fields: BTreeSet<String>,
    #[serde(default)]
    pub redact_fields: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourcePiiCategoryV1 {
    Contact,
    GovernmentId,
    Identity,
    Financial,
    Health,
    Education,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourcePrivacyClassificationV1 {
    Confidential,
    Pii,
    ProtectedData,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceListPresentationV1 {
    Visible,
    Partial,
    Hidden,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceDetailPresentationV1 {
    Visible,
    Masked,
    Hidden,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceInputPresentationV1 {
    Editable,
    Readonly,
    Hidden,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceClipboardPresentationV1 {
    Allowed,
    Explicit,
    Never,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceFieldPresentationV1 {
    pub list: ResourceListPresentationV1,
    pub detail: ResourceDetailPresentationV1,
    pub input: ResourceInputPresentationV1,
    pub clipboard: ResourceClipboardPresentationV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceFieldPrivacyV1 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pii_category: Option<ResourcePiiCategoryV1>,
    pub encrypted: bool,
    pub classification: ResourcePrivacyClassificationV1,
    pub presentation: ResourceFieldPresentationV1,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ResourceAccessAuditEventV1 {
    Open,
    Reveal,
    Copy,
    Export,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceAccessAuditV1 {
    pub endpoint: String,
    pub events: BTreeSet<ResourceAccessAuditEventV1>,
    pub purpose: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourcePrivacyContractV1 {
    pub fields: BTreeMap<String, ResourceFieldPrivacyV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_audit: Option<ResourceAccessAuditV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceEventContract {
    pub operation: ResourceOperation,
    pub queue: String,
    pub schema_version: u32,
    #[serde(default)]
    pub before_fields: BTreeSet<String>,
    #[serde(default)]
    pub after_fields: BTreeSet<String>,
}

impl ResourceEventContract {
    fn validate(&self) -> Result<()> {
        validate_identifier("event queue", &self.queue)?;
        if self.schema_version == 0 {
            return invalid("resource event schema version must be positive");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MigrationPlanV1 {
    pub version: u32,
    pub schema_version: u64,
    pub contract_versions: BTreeMap<String, u64>,
    pub forward: Vec<MigrationStep>,
    #[serde(default)]
    pub compatibility_checks: Vec<String>,
    #[serde(default)]
    pub transformations: Vec<MigrationStep>,
    pub activation_boundary: String,
    #[serde(default)]
    pub rollback: Vec<MigrationStep>,
    #[serde(default)]
    pub irreversible: bool,
    #[serde(default)]
    pub dependency_requirements: BTreeMap<String, String>,
}

impl MigrationPlanV1 {
    fn validate(&self) -> Result<()> {
        if self.version != 1 || self.schema_version == 0 || self.forward.is_empty() {
            return invalid("migration requires version 1, schema version, and forward steps");
        }
        validate_nonempty(
            "migration activation boundary",
            &self.activation_boundary,
            256,
        )?;
        if self.irreversible && !self.rollback.is_empty() {
            return invalid("irreversible migration must not claim rollback steps");
        }
        for step in self
            .forward
            .iter()
            .chain(&self.transformations)
            .chain(&self.rollback)
        {
            step.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MigrationStep {
    Sql {
        statement_id: String,
    },
    OnlineRewrite {
        relation: String,
        batch_rows: u32,
        transform_export: String,
    },
    ValidateContract {
        resource: String,
    },
    BackfillField {
        resource: String,
        field: String,
        expression: ApplicationExpressionV1,
    },
    StartWorker {
        worker: String,
    },
    StopWorker {
        worker: String,
    },
}

impl MigrationStep {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Sql { statement_id } => validate_identifier("migration SQL", statement_id),
            Self::OnlineRewrite {
                relation,
                batch_rows,
                transform_export,
            } => {
                validate_qualified("migration relation", relation)?;
                validate_identifier("migration transform", transform_export)?;
                if *batch_rows == 0 {
                    return invalid("online migration batch_rows must be positive");
                }
                Ok(())
            }
            Self::ValidateContract { resource } => {
                validate_identifier("migration resource", resource)
            }
            Self::BackfillField {
                resource,
                field,
                expression,
            } => {
                validate_identifier("migration backfill resource", resource)?;
                validate_identifier("migration backfill field", field)?;
                validate_carrier_expression(expression)
            }
            Self::StartWorker { worker } | Self::StopWorker { worker } => {
                validate_identifier("migration worker", worker)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct HostHandle(pub u64);

/// Fully parsed HTTP request delivered to ABI-v2 application code. Header and
/// query pairs remain ordered vectors so repeated values are never collapsed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HttpRequestV2 {
    pub request_id: String,
    pub method: HttpMethod,
    pub path: String,
    #[serde(default)]
    pub path_parameters: BTreeMap<String, String>,
    #[serde(default)]
    pub query: Vec<(String, String)>,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub cookies: Vec<(String, String)>,
    pub body: HttpRequestBodyV2,
    #[serde(default)]
    pub trusted_client_address: Option<String>,
    #[serde(default)]
    pub origin: Option<String>,
    pub deadline_unix_ms: i64,
    pub trace_id: String,
    pub actor: ActorContext,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum HttpRequestBodyV2 {
    Empty,
    Json(JsonValue),
    Form(Vec<(String, String)>),
    Multipart(Vec<MultipartPartV2>),
    Binary(Vec<u8>),
    Stream(HostHandle),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MultipartPartV2 {
    pub name: String,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HttpResponseV2 {
    pub status: u16,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    pub body: HttpResponseBodyV2,
    #[serde(default)]
    pub trailers: Vec<(String, String)>,
    #[serde(default)]
    pub retry_after_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum HttpResponseBodyV2 {
    Empty,
    Json(JsonValue),
    Text(String),
    Binary(Vec<u8>),
    Stream(HostHandle),
    Sse(HostHandle),
    WebSocket(HostHandle),
    Error(StructuredHttpErrorV2),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StructuredHttpErrorV2 {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub details: BTreeMap<String, String>,
    pub trace_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HostCall {
    pub request_id: u64,
    pub request: HostRequest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "capability", content = "operation", rename_all = "snake_case")]
pub enum HostRequest {
    Transaction(TransactionRequest),
    Database(DatabaseRequest),
    MutationGrant(MutationGrantRequest),
    Broker(BrokerRequest),
    Service(ServiceRequest),
    Clock(ClockRequest),
    Random(RandomRequest),
    Secret(SecretRequest),
    Crypto(CryptoRequest),
    Egress(EgressRequest),
    Grpc(GrpcRequest),
    Tokenizer(TokenizerRequest),
    Embeddings(EmbeddingsRequest),
    Llm(LlmRequest),
    Redis(RedisRequest),
    Email(EmailRequest),
    Blob(BlobRequest),
    Stream(StreamRequest),
    Observe(ObserveRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum GrpcRequest {
    Unary {
        client: String,
        method: String,
        payload: JsonValue,
        deadline_unix_ms: i64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum TokenizerRequest {
    Count { provider: String, text: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum EmbeddingsRequest {
    Embed {
        provider: String,
        text: String,
        dimensions: u32,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum LlmRequest {
    Estimate {
        client: String,
        input_tokens: u64,
        output_tokens: u64,
    },
    Complete {
        client: String,
        method: String,
        user_prompt: String,
        #[serde(default)]
        continuation: bool,
        #[serde(default)]
        history: Vec<JsonValue>,
        #[serde(default)]
        conversation_id: Option<String>,
        #[serde(default)]
        output_type: Option<String>,
        #[serde(default)]
        allowed_tools: Option<BTreeSet<String>>,
        deadline_unix_ms: i64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransactionRequest {
    Begin {
        isolation: IsolationLevel,
    },
    Commit {
        transaction: HostHandle,
    },
    Rollback {
        transaction: HostHandle,
    },
    Savepoint {
        transaction: HostHandle,
        name: String,
    },
    RollbackTo {
        transaction: HostHandle,
        savepoint: HostHandle,
    },
    Release {
        transaction: HostHandle,
        savepoint: HostHandle,
    },
    RegisterCommitValidation {
        transaction: HostHandle,
        validator: CommitValidator,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum DatabaseRequest {
    SelectPrimaryKey {
        transaction: HostHandle,
        relation: String,
        id: String,
        #[serde(default)]
        columns: Vec<String>,
    },
    Insert {
        transaction: HostHandle,
        grant: HostHandle,
        relation: String,
        record: JsonValue,
    },
    Update {
        transaction: HostHandle,
        grant: HostHandle,
        relation: String,
        id: String,
        patch: JsonValue,
        #[serde(default)]
        expected_version: Option<u64>,
    },
    Delete {
        transaction: HostHandle,
        grant: HostHandle,
        relation: String,
        id: String,
    },
    Upsert {
        transaction: HostHandle,
        grant: HostHandle,
        relation: String,
        record: JsonValue,
        #[serde(default)]
        expected_version: Option<u64>,
    },
    Query {
        transaction: HostHandle,
        query: QuerySpec,
    },
    RelationQuery {
        transaction: HostHandle,
        query: RelationQuerySpec,
    },
    RelationAggregate {
        transaction: HostHandle,
        query: RelationQuerySpec,
        aggregate: AggregateSpec,
    },
    Aggregate {
        transaction: HostHandle,
        relation: String,
        aggregate: AggregateSpec,
        #[serde(default)]
        filters: Vec<FilterExpression>,
    },
    FullTextSearch {
        transaction: HostHandle,
        relation: String,
        index: String,
        query: String,
        limit: u32,
    },
    VectorSearch {
        transaction: HostHandle,
        relation: String,
        index: String,
        vector: Vec<f32>,
        limit: u32,
        #[serde(default)]
        filters: Vec<FilterExpression>,
    },
    HybridSearch {
        transaction: HostHandle,
        relation: String,
        vector_index: String,
        text_index: String,
        query: String,
        vector: Vec<f32>,
        vector_weight: f64,
        text_weight: f64,
        limit: u32,
        #[serde(default)]
        filters: Vec<FilterExpression>,
    },
    Spatial {
        transaction: HostHandle,
        relation: String,
        operation: SpatialOperation,
        #[serde(default)]
        filters: Vec<FilterExpression>,
    },
    Recent {
        transaction: HostHandle,
        relation: String,
        time_field: String,
        window: String,
        limit: u32,
        #[serde(default)]
        filters: Vec<FilterExpression>,
    },
    JsonPath {
        transaction: HostHandle,
        relation: String,
        path: String,
        #[serde(default)]
        filters: Vec<FilterExpression>,
        limit: u32,
    },
    RawSql {
        transaction: HostHandle,
        statement_id: String,
        #[serde(default)]
        parameters: Vec<JsonValue>,
    },
    LockRows {
        transaction: HostHandle,
        relation: String,
        ids: Vec<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QuerySpec {
    pub relation: String,
    #[serde(default)]
    pub filters: Vec<FilterExpression>,
    #[serde(default)]
    pub sort: Vec<SortField>,
    #[serde(default)]
    pub columns: Vec<String>,
    pub limit: u32,
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RelationQuerySpec {
    pub source_relation: String,
    pub source_field: String,
    pub source_value: JsonValue,
    pub join_field: String,
    pub target_relation: String,
    pub target_field: String,
    #[serde(default)]
    pub target_filters: Vec<FilterExpression>,
    #[serde(default)]
    pub target_sort: Vec<SortField>,
    #[serde(default)]
    pub target_columns: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<RelationSearchSpec>,
    pub limit: u32,
    #[serde(default)]
    pub offset: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RelationSearchSpec {
    pub index: String,
    pub query: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum FilterExpression {
    Eq {
        field: String,
        value: JsonValue,
    },
    Ne {
        field: String,
        value: JsonValue,
    },
    Lt {
        field: String,
        value: JsonValue,
    },
    Le {
        field: String,
        value: JsonValue,
    },
    Gt {
        field: String,
        value: JsonValue,
    },
    Ge {
        field: String,
        value: JsonValue,
    },
    In {
        field: String,
        values: Vec<JsonValue>,
    },
    Contains {
        field: String,
        value: String,
    },
    TypedEq {
        field: String,
        value: JsonValue,
        value_type: ApplicationRouteParameterTypeV1,
    },
    TypedGe {
        field: String,
        value: JsonValue,
        value_type: ApplicationRouteParameterTypeV1,
    },
    JsonPathEq {
        field: String,
        path: String,
        value: JsonValue,
        #[serde(default)]
        value_type: Option<ApplicationRouteParameterTypeV1>,
    },
    JsonPathGe {
        field: String,
        path: String,
        value: JsonValue,
        value_type: ApplicationRouteParameterTypeV1,
    },
    JsonPathContains {
        field: String,
        path: String,
        value: String,
        array: bool,
    },
    JsonPathExists {
        field: String,
        path: String,
        exists: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SortField {
    pub field: String,
    #[serde(default)]
    pub descending: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "function", rename_all = "snake_case", deny_unknown_fields)]
pub enum AggregateSpec {
    Count,
    Sum { field: String },
    Min { field: String },
    Max { field: String },
    Average { field: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SpatialOperation {
    Within {
        field: String,
        geometry: JsonValue,
        limit: u32,
    },
    Intersects {
        field: String,
        geometry: JsonValue,
        limit: u32,
    },
    Nearest {
        field: String,
        point: JsonValue,
        limit: u32,
    },
    WithinRadius {
        field: String,
        point: JsonValue,
        radius: f64,
        limit: u32,
    },
    Contains {
        field: String,
        point: JsonValue,
        limit: u32,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MutationGrantRequest {
    pub transaction: HostHandle,
    pub resource: String,
    pub operation: ResourceOperation,
    pub relation: String,
    #[serde(default)]
    pub record_id: Option<String>,
    #[serde(default)]
    pub predicate: Option<Vec<FilterExpression>>,
    #[serde(default)]
    pub expected_version: Option<u64>,
    pub columns: BTreeSet<String>,
    #[serde(default)]
    pub bulk: bool,
    pub max_rows: u64,
    #[serde(default = "one_statement")]
    pub statement_budget: u32,
    #[serde(default)]
    pub audit_metadata: BTreeMap<String, String>,
}

fn one_statement() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrokerRequest {
    Publish {
        queue: String,
        payload: JsonValue,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        #[serde(default)]
        idempotency_key: Option<String>,
        #[serde(default)]
        delay_ms: Option<u64>,
    },
    PublishOnCommit {
        transaction: HostHandle,
        queue: String,
        payload: JsonValue,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        #[serde(default)]
        idempotency_key: Option<String>,
        #[serde(default)]
        delay_ms: Option<u64>,
    },
    Consume {
        queue: String,
        group: String,
        consumer: String,
        max_messages: u32,
        visibility_timeout_ms: u64,
    },
    Ack {
        delivery: HostHandle,
    },
    Nack {
        delivery: HostHandle,
        retry: bool,
        #[serde(default)]
        delay_ms: Option<u64>,
        #[serde(default)]
        error_code: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServiceRequest {
    Call {
        dependency: String,
        service: String,
        method: String,
        payload: JsonValue,
        #[serde(default)]
        transaction: Option<HostHandle>,
        deadline_unix_ms: i64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClockRequest {
    WallTime,
    MonotonicTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum RandomRequest {
    Bytes { len: u32 },
    Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum SecretRequest {
    Open {
        name: String,
        #[serde(default)]
        version: Option<String>,
    },
    Metadata {
        secret: HostHandle,
    },
    ReadPlaintext {
        secret: HostHandle,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum CryptoRequest {
    Sign {
        secret: HostHandle,
        algorithm: String,
        message: Vec<u8>,
    },
    Verify {
        secret: HostHandle,
        algorithm: String,
        message: Vec<u8>,
        signature: Vec<u8>,
    },
    Hmac {
        secret: HostHandle,
        algorithm: String,
        message: Vec<u8>,
    },
    Encrypt {
        secret: HostHandle,
        algorithm: String,
        plaintext: Vec<u8>,
        #[serde(default)]
        associated_data: Vec<u8>,
    },
    Decrypt {
        secret: HostHandle,
        algorithm: String,
        ciphertext: Vec<u8>,
        #[serde(default)]
        associated_data: Vec<u8>,
    },
    Derive {
        secret: HostHandle,
        algorithm: String,
        context: Vec<u8>,
        len: u32,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum EgressRequest {
    Http {
        policy: String,
        method: String,
        url: String,
        #[serde(default)]
        headers: Vec<(String, String)>,
        #[serde(default)]
        body: Vec<u8>,
        deadline_unix_ms: i64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum RedisRequest {
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum EmailRequest {
    Send {
        provider: String,
        from: String,
        to: Vec<String>,
        #[serde(default)]
        cc: Vec<String>,
        #[serde(default)]
        bcc: Vec<String>,
        #[serde(default)]
        reply_to: Option<String>,
        subject: String,
        #[serde(default)]
        text: Option<String>,
        #[serde(default)]
        html: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum BlobRequest {
    CreateUpload {
        namespace: String,
        #[serde(default)]
        content_type: Option<String>,
        #[serde(default)]
        metadata: BTreeMap<String, String>,
    },
    Write {
        upload: HostHandle,
        bytes: Vec<u8>,
    },
    Finish {
        upload: HostHandle,
    },
    OpenRead {
        namespace: String,
        blob_id: String,
    },
    Read {
        blob: HostHandle,
        max_bytes: u32,
    },
    Delete {
        namespace: String,
        blob_id: String,
    },
    Metadata {
        namespace: String,
        blob_id: String,
    },
    SignedUrl {
        namespace: String,
        blob_id: String,
        expires_seconds: u32,
    },
    Attach {
        transaction: HostHandle,
        grant: HostHandle,
        namespace: String,
        blob_id: String,
        relation: String,
        record_id: String,
        field: String,
    },
    CreateNamedUpload {
        namespace: String,
        key: String,
        #[serde(default)]
        content_type: Option<String>,
        #[serde(default)]
        metadata: BTreeMap<String, String>,
    },
    OpenNamedRead {
        namespace: String,
        key: String,
    },
    DeleteNamed {
        namespace: String,
        key: String,
    },
    NamedMetadata {
        namespace: String,
        key: String,
    },
    NamedSignedUrl {
        namespace: String,
        key: String,
        expires_seconds: u32,
        method: String,
        #[serde(default)]
        download_name: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum StreamRequest {
    OpenResponse {
        status: u16,
        #[serde(default)]
        headers: Vec<(String, String)>,
        kind: StreamKind,
    },
    Send {
        stream: HostHandle,
        bytes: Vec<u8>,
    },
    SendEvent {
        stream: HostHandle,
        event: ServerSentEvent,
    },
    Close {
        stream: HostHandle,
        #[serde(default)]
        trailers: Vec<(String, String)>,
    },
    Receive {
        stream: HostHandle,
        max_bytes: u32,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Bytes,
    ServerSentEvents,
    WebSocket,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServerSentEvent {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub event: Option<String>,
    pub data: String,
    #[serde(default)]
    pub retry_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObserveRequest {
    Log {
        level: LogLevel,
        message: String,
        #[serde(default)]
        fields: BTreeMap<String, JsonValue>,
    },
    TraceEvent {
        name: String,
        #[serde(default)]
        fields: BTreeMap<String, JsonValue>,
    },
    Metric {
        name: String,
        kind: MetricKind,
        value: f64,
        #[serde(default)]
        labels: BTreeMap<String, String>,
    },
    Audit {
        action: String,
        subject: String,
        #[serde(default)]
        fields: BTreeMap<String, JsonValue>,
    },
    Evidence {
        control: String,
        outcome: String,
        #[serde(default)]
        fields: BTreeMap<String, JsonValue>,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CommitValidator {
    pub name: String,
    pub kind: CommitValidatorKind,
    #[serde(default)]
    pub relations: BTreeSet<String>,
    #[serde(default)]
    pub subjects: BTreeSet<String>,
    #[serde(default)]
    pub parameters: JsonValue,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommitValidatorKind {
    AppendOnly,
    ImmutableFields,
    LedgerBalanced,
    AggregateInvariant,
    FinanceGuard,
    OptimisticVersion,
    TenantWorkspaceImmutable,
    CustomDeclared,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HostCallResult {
    pub request_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<HostValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<HostError>,
}

impl HostCallResult {
    pub fn success(request_id: u64, value: HostValue) -> Self {
        Self {
            request_id,
            value: Some(value),
            error: None,
        }
    }

    pub fn failure(request_id: u64, error: HostError) -> Self {
        Self {
            request_id,
            value: None,
            error: Some(error),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.value.is_some() == self.error.is_some() {
            return invalid("host response must contain exactly one of value or error");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum HostValue {
    Unit,
    Handle(HostHandle),
    Handles(Vec<HostHandle>),
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(JsonValue),
    Rows(Vec<JsonValue>),
    Message(BrokerMessage),
    Messages(Vec<BrokerMessage>),
    EgressResponse(EgressResponse),
    BlobMetadata(BlobMetadata),
    SecretMetadata(SecretMetadata),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BrokerMessage {
    pub delivery: HostHandle,
    pub message_id: String,
    pub payload: JsonValue,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub attempts: u32,
    pub trace_id: String,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub causation_id: Option<String>,
    #[serde(default)]
    pub actor_id: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub organization_id: Option<String>,
    pub originating_plugin: String,
    #[serde(default)]
    pub originating_resource: Option<String>,
    #[serde(default)]
    pub originating_action: Option<String>,
    pub schema_version: u32,
    #[serde(default)]
    pub contract_version: Option<u32>,
    #[serde(default = "default_event_schema_version")]
    pub event_schema_version: u32,
    #[serde(default)]
    pub transaction_id: Option<u64>,
    #[serde(default)]
    pub commit_sequence: Option<u64>,
}

fn default_event_schema_version() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EgressResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlobMetadata {
    pub blob_id: String,
    pub namespace: String,
    pub size: u64,
    pub sha256: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    pub scan_status: String,
    #[serde(default)]
    pub last_modified: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SecretMetadata {
    pub name: String,
    pub key_id: String,
    pub version: String,
    pub algorithm: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostError {
    pub code: String,
    pub class: ErrorClass,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub retry_after_ms: Option<u64>,
    pub trace_id: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    InvalidRequest,
    Unauthenticated,
    Unauthorized,
    PolicyDenied,
    MutationGrantDenied,
    NotFound,
    Conflict,
    OptimisticConflict,
    Constraint,
    CommitValidation,
    DependencyUnavailable,
    DependencyIncompatible,
    Timeout,
    Cancelled,
    ResourceExhausted,
    RateLimited,
    Provider,
    Package,
    Migration,
    Activation,
    Internal,
}

fn validate_route_template(template: &str) -> Result<()> {
    route_template_parameters(template).map(|_| ())
}

fn route_template_parameters(template: &str) -> Result<BTreeSet<String>> {
    if !template.starts_with('/')
        || template.len() > 1024
        || template.contains("..")
        || template.contains('\0')
    {
        return invalid(format!("invalid route template `{template}`"));
    }
    let mut parameters = BTreeSet::new();
    for segment in template.split('/') {
        if let Some(parameter) = segment
            .strip_prefix('{')
            .and_then(|value| value.strip_suffix('}'))
        {
            validate_identifier("route parameter", parameter)?;
            if !parameters.insert(parameter.to_string()) {
                return invalid(format!("duplicate route parameter `{parameter}`"));
            }
        } else if segment.contains('{') || segment.contains('}') {
            return invalid(format!("malformed route template `{template}`"));
        }
    }
    Ok(parameters)
}

impl RouteV2 {
    fn validate(&self) -> Result<()> {
        validate_identifier("v2 route", &self.name)?;
        let template_parameters = route_template_parameters(&self.template)?;
        validate_identifier("v2 route export", &self.export)?;
        if self.resource.is_some() != self.operation.is_some() {
            return invalid(format!(
                "route `{}` must bind both resource and operation",
                self.name
            ));
        }
        if self.service_call.is_some() && self.resource.is_some() {
            return invalid(format!(
                "route `{}` cannot bind both a resource and plugin service",
                self.name
            ));
        }
        if self.public
            && (!self.roles.is_empty()
                || !self.scopes.is_empty()
                || self.roles_any
                || self.scopes_any)
        {
            return invalid(format!(
                "public route `{}` cannot declare protected authority requirements",
                self.name
            ));
        }
        validate_signed_response_headers(
            &format!("route `{}`", self.name),
            &self.response_headers,
        )?;
        if let Some(call) = &self.service_call {
            validate_identifier("route service dependency", &call.dependency)?;
            validate_identifier("route service", &call.service)?;
            validate_identifier("route service method", &call.method)?;
        }
        if let Some(request) = &self.application_request {
            request.validate(&self.name, &template_parameters)?;
        }
        if let Some(response) = &self.application_response {
            response.validate(0)?;
        }
        if let Some(idempotency) = &self.idempotency {
            idempotency.validate()?;
            if self.resource.is_some() {
                return invalid(format!(
                    "route `{}` must declare idempotency on its resource contract",
                    self.name
                ));
            }
        }
        if let Some(cache) = &self.cache {
            cache.validate()?;
            if self.resource.is_some() || self.service_call.is_some() {
                return invalid(format!(
                    "route `{}` cache requires a direct BicDB application program route",
                    self.name
                ));
            }
            if self.method != HttpMethod::Get {
                return invalid(format!(
                    "route `{}` cache requires the GET method",
                    self.name
                ));
            }
            if self.idempotency.is_some() {
                return invalid(format!(
                    "route `{}` cannot combine idempotency and cache contracts",
                    self.name
                ));
            }
        }
        if self.max_request_bytes == 0 || self.max_response_bytes == 0 {
            return invalid(format!("route `{}` has zero byte limits", self.name));
        }
        if self.sse && self.websocket {
            return invalid(format!(
                "route `{}` cannot be both SSE and WebSocket",
                self.name
            ));
        }
        if (self.sse || self.websocket) && !self.streaming_response {
            return invalid(format!(
                "route `{}` realtime mode requires streaming_response",
                self.name
            ));
        }
        Ok(())
    }
}

fn validate_signed_response_headers(owner: &str, headers: &BTreeMap<String, String>) -> Result<()> {
    for (name, value) in headers {
        if name.is_empty()
            || name.len() > 256
            || !name.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#'
                            | b'$'
                            | b'%'
                            | b'&'
                            | b'\''
                            | b'*'
                            | b'+'
                            | b'-'
                            | b'.'
                            | b'^'
                            | b'_'
                            | b'`'
                            | b'|'
                            | b'~'
                    )
            })
            || value.len() > 16_384
            || value.contains('\r')
            || value.contains('\n')
        {
            return invalid(format!(
                "{owner} contains an invalid signed response header"
            ));
        }
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "content-length"
                | "connection"
                | "transfer-encoding"
                | "upgrade"
                | "set-cookie"
                | "traceparent"
                | "tracestate"
                | "x-request-id"
                | "x-correlation-id"
                | "x-trace-id"
        ) {
            return invalid(format!(
                "{owner} cannot sign host-controlled response header `{name}`"
            ));
        }
    }
    Ok(())
}

impl ApplicationRouteRequestV1 {
    fn validate(&self, route: &str, template_parameters: &BTreeSet<String>) -> Result<()> {
        if self.path_parameters.len() > 128 || self.query_parameters.len() > 128 {
            return invalid(format!(
                "BicDB application route `{route}` exceeds 128 path or query parameters"
            ));
        }
        let mut path_names = BTreeSet::new();
        for parameter in &self.path_parameters {
            parameter.validate(route, "path")?;
            if parameter.optional || parameter.default_json.is_some() {
                return invalid(format!(
                    "BicDB application route `{route}` path parameter `{}` cannot be optional or defaulted",
                    parameter.name
                ));
            }
            if !path_names.insert(parameter.name.clone()) {
                return invalid(format!(
                    "BicDB application route `{route}` repeats path parameter `{}`",
                    parameter.name
                ));
            }
        }
        if &path_names != template_parameters {
            return invalid(format!(
                "BicDB application route `{route}` path schema does not exactly match its template"
            ));
        }
        let mut query_names = BTreeSet::new();
        for parameter in &self.query_parameters {
            parameter.validate(route, "query")?;
            if !query_names.insert(parameter.name.clone()) {
                return invalid(format!(
                    "BicDB application route `{route}` repeats query parameter `{}`",
                    parameter.name
                ));
            }
        }
        if let Some(body) = &self.body {
            body.validate(0)?;
        }
        Ok(())
    }
}

impl ApplicationRouteParameterV1 {
    fn validate(&self, route: &str, location: &str) -> Result<()> {
        self.validate_at_depth(route, location, 0)
    }

    fn validate_at_depth(&self, route: &str, location: &str, depth: usize) -> Result<()> {
        validate_identifier("BicDB application route parameter", &self.name)?;
        self.value_type.validate(depth + 1)?;
        if let Some(default) = &self.default_json {
            let value: JsonValue = serde_json::from_str(default).map_err(|error| {
                ExtensionError::InvalidManifest(format!(
                    "BicDB application route `{route}` {location} parameter `{}` has invalid default JSON: {error}",
                    self.name
                ))
            })?;
            if !self.value_type.default_matches(&value) {
                return invalid(format!(
                    "BicDB application route `{route}` {location} parameter `{}` default has the wrong type",
                    self.name
                ));
            }
        }
        for validation in &self.validations {
            validation.validate(route, location, &self.name)?;
        }
        Ok(())
    }
}

impl ApplicationRouteParameterTypeV1 {
    fn validate(&self, depth: usize) -> Result<()> {
        if depth > 16 {
            return invalid("BicDB application route parameter type nesting exceeds 16 levels");
        }
        match self {
            Self::Enum { values } => {
                if values.is_empty() || values.len() > 1024 {
                    return invalid("BicDB application route enum requires 1..=1024 values");
                }
                validate_string_set("BicDB application route enum value", values)
            }
            Self::List { element } | Self::Set { element } => element.validate(depth + 1),
            Self::Optional { value } => value.validate(depth + 1),
            Self::Map { key, value } => {
                if !key.is_map_key() {
                    return invalid(
                        "BicDB application map key must be a scalar, enum, or optional scalar/enum",
                    );
                }
                key.validate(depth + 1)?;
                value.validate(depth + 1)
            }
            Self::Object { fields } => {
                if fields.len() > 512 {
                    return invalid("BicDB application object exceeds 512 fields");
                }
                let mut names = BTreeSet::new();
                for field in fields {
                    field.validate_at_depth(
                        "nested BicDB application object",
                        "body",
                        depth + 1,
                    )?;
                    if !names.insert(field.name.clone()) {
                        return invalid(format!(
                            "BicDB application object repeats field `{}`",
                            field.name
                        ));
                    }
                }
                Ok(())
            }
            Self::Vector { dimensions } if *dimensions == 0 || *dimensions > 65_535 => {
                invalid("BicDB application vector dimensions must be in 1..=65535")
            }
            _ => Ok(()),
        }
    }

    fn default_matches(&self, value: &JsonValue) -> bool {
        match self {
            Self::String
            | Self::Timestamp
            | Self::Date
            | Self::LocalDateTime
            | Self::TimeZone
            | Self::Uuid => value.is_string(),
            Self::Int => {
                value.as_i64().is_some() || value.as_u64().is_some_and(|v| v <= i64::MAX as u64)
            }
            Self::Float => value.as_f64().is_some_and(f64::is_finite),
            Self::Decimal => value.is_string() || value.as_f64().is_some_and(f64::is_finite),
            Self::Bool => value.is_boolean(),
            Self::Json => true,
            Self::Enum { values } => value.as_str().is_some_and(|value| values.contains(value)),
            Self::List { element } => value
                .as_array()
                .is_some_and(|values| values.iter().all(|value| element.default_matches(value))),
            Self::Set { element } => value.as_array().is_some_and(|values| {
                values.iter().enumerate().all(|(index, value)| {
                    element.default_matches(value) && !values[..index].contains(value)
                })
            }),
            Self::Optional { value: inner } => value.is_null() || inner.default_matches(value),
            Self::Object { fields } => value.as_object().is_some_and(|object| {
                fields.iter().all(|field| match object.get(&field.name) {
                    Some(value) => field.value_type.default_matches(value),
                    None => field.optional || field.default_json.is_some(),
                })
            }),
            Self::Map { key, value: inner } => value.as_array().is_some_and(|entries| {
                entries.iter().all(|entry| {
                    entry.as_object().is_some_and(|entry| {
                        entry
                            .get("key")
                            .is_some_and(|value| key.default_matches(value))
                            && entry
                                .get("value")
                                .is_some_and(|value| inner.default_matches(value))
                    })
                })
            }),
            Self::Vector { dimensions } => value.as_array().is_some_and(|values| {
                values.len() == *dimensions
                    && values
                        .iter()
                        .all(|value| value.as_f64().is_some_and(f64::is_finite))
            }),
            Self::Point => carrier_geometry_matches(value, "Point"),
            Self::LineString => carrier_geometry_matches(value, "LineString"),
            Self::Polygon => carrier_geometry_matches(value, "Polygon"),
            Self::Null => value.is_null(),
        }
    }

    fn is_map_key(&self) -> bool {
        matches!(
            self,
            Self::String
                | Self::Int
                | Self::Bool
                | Self::Timestamp
                | Self::Date
                | Self::LocalDateTime
                | Self::TimeZone
                | Self::Uuid
                | Self::Enum { .. }
        ) || matches!(self, Self::Optional { value } if value.is_map_key())
    }
}

fn carrier_geometry_matches(value: &JsonValue, expected: &str) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    if object.get("type").and_then(JsonValue::as_str) != Some(expected) {
        return false;
    }
    fn finite_coordinates(value: &JsonValue) -> bool {
        let Some(values) = value.as_array() else {
            return false;
        };
        if values.len() == 2
            && values
                .iter()
                .all(|value| value.as_f64().is_some_and(f64::is_finite))
        {
            return true;
        }
        !values.is_empty() && values.iter().all(finite_coordinates)
    }
    object.get("coordinates").is_some_and(finite_coordinates)
}

impl ApplicationRouteParameterValidationV1 {
    fn validate(&self, route: &str, location: &str, parameter: &str) -> Result<()> {
        let invalid_validation = || {
            invalid(format!(
                "BicDB application route `{route}` {location} parameter `{parameter}` has an invalid validation rule"
            ))
        };
        match self {
            Self::Email => Ok(()),
            Self::Length { min, max } => {
                if min.is_none() && max.is_none()
                    || (*min).zip(*max).is_some_and(|(min, max)| min > max)
                {
                    invalid_validation()
                } else {
                    Ok(())
                }
            }
            Self::Range { minimum, maximum } => {
                let minimum = minimum
                    .as_deref()
                    .map(str::parse::<f64>)
                    .transpose()
                    .map_err(|_| {
                        ExtensionError::InvalidManifest(
                            "BicDB application route range minimum is invalid".to_string(),
                        )
                    })?;
                let maximum = maximum
                    .as_deref()
                    .map(str::parse::<f64>)
                    .transpose()
                    .map_err(|_| {
                        ExtensionError::InvalidManifest(
                            "BicDB application route range maximum is invalid".to_string(),
                        )
                    })?;
                if minimum.is_none() && maximum.is_none()
                    || minimum.is_some_and(|value| !value.is_finite())
                    || maximum.is_some_and(|value| !value.is_finite())
                    || minimum.zip(maximum).is_some_and(|(min, max)| min > max)
                {
                    invalid_validation()
                } else {
                    Ok(())
                }
            }
            Self::Pattern { expression } => {
                if expression.is_empty() || expression.len() > 4096 {
                    invalid_validation()
                } else {
                    Ok(())
                }
            }
        }
    }
}

fn unique_by<'a>(label: &str, values: impl IntoIterator<Item = &'a String>) -> Result<()> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value.to_ascii_lowercase()) {
            return invalid(format!("duplicate {label} `{value}`"));
        }
    }
    Ok(())
}

fn validate_identifier(label: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 128 {
        return invalid(format!("{label} must contain 1..=128 bytes"));
    }
    let mut characters = value.chars();
    if !characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        || !characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
        })
    {
        return invalid(format!("invalid {label} `{value}`"));
    }
    Ok(())
}

fn validate_qualified(label: &str, value: &str) -> Result<()> {
    for part in value.split('.') {
        validate_identifier(label, part)?;
    }
    Ok(())
}

fn validate_nonempty(label: &str, value: &str, max: usize) -> Result<()> {
    if value.trim().is_empty() || value.len() > max || value.contains('\0') {
        invalid(format!("{label} must contain 1..={max} safe bytes"))
    } else {
        Ok(())
    }
}

fn validate_http_header_name(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
    {
        invalid(format!("invalid {label} `{value}`"))
    } else {
        Ok(())
    }
}

fn validate_string_set(label: &str, values: &BTreeSet<String>) -> Result<()> {
    for value in values {
        validate_nonempty(label, value, 256)?;
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str) -> Result<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        invalid(format!(
            "{label} must be 64 lowercase hexadecimal characters"
        ))
    }
}

fn validate_hostname(host: &str) -> Result<()> {
    if host.is_empty()
        || host.len() > 253
        || host.starts_with('.')
        || host.ends_with('.')
        || host.parse::<std::net::IpAddr>().is_ok()
        || !host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'*'))
    {
        return invalid(format!("invalid egress hostname `{host}`"));
    }
    if host.contains('*') && !host.starts_with("*.") {
        return invalid(format!(
            "wildcard egress hostname `{host}` must begin with `*.`"
        ));
    }
    Ok(())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(ExtensionError::InvalidManifest(message.into()))
}

/// Guest-side transport for the single capability-mediated ABI-v2 import.
///
/// Application code passes a typed [`HostRequest`] and receives a typed
/// [`HostValue`]. The transport owns request IDs, bounded retry for the
/// host-sized response buffer, JSON validation, and protocol status handling.
/// It does not expose native pointers, sockets, files, or database objects.
pub mod guest {
    use std::sync::atomic::{AtomicU64, Ordering};

    use thiserror::Error;

    use super::{HostCall, HostCallResult, HostError, HostRequest, HostValue};

    #[cfg(target_arch = "wasm32")]
    const CALL_OK: u32 = 0;
    #[cfg(target_arch = "wasm32")]
    const CALL_BUFFER_TOO_SMALL: u32 = 1;
    #[cfg(target_arch = "wasm32")]
    const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
    static REQUEST_ID: AtomicU64 = AtomicU64::new(1);

    #[derive(Clone, Debug, Error, PartialEq, Eq)]
    pub enum GuestCallError {
        #[error("host rejected the capability call: {0:?}")]
        Host(HostError),
        #[error("ABI-v2 host-call protocol failed: {0}")]
        Protocol(String),
        #[error("ABI-v2 host-call encoding failed: {0}")]
        Encoding(String),
    }

    pub type GuestResult<T> = std::result::Result<T, GuestCallError>;

    /// Invoke one declared host capability.
    pub fn call(request: HostRequest) -> GuestResult<HostValue> {
        let request_id = REQUEST_ID.fetch_add(1, Ordering::Relaxed).max(1);
        let bytes = serde_json::to_vec(&HostCall {
            request_id,
            request,
        })
        .map_err(|error| GuestCallError::Encoding(error.to_string()))?;
        let response = transport(&bytes)?;
        let result: HostCallResult = serde_json::from_slice(&response)
            .map_err(|error| GuestCallError::Encoding(error.to_string()))?;
        result
            .validate()
            .map_err(|error| GuestCallError::Protocol(error.to_string()))?;
        if result.request_id != request_id {
            return Err(GuestCallError::Protocol(
                "response request ID differs from the call".to_string(),
            ));
        }
        match (result.value, result.error) {
            (Some(value), None) => Ok(value),
            (None, Some(error)) => Err(GuestCallError::Host(error)),
            _ => unreachable!("validated host response"),
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn transport(request: &[u8]) -> GuestResult<Vec<u8>> {
        let mut response = vec![0_u8; 4096];
        let (mut status, mut length) = unsafe {
            invoke_import(
                request.as_ptr(),
                request.len(),
                response.as_mut_ptr(),
                response.len(),
            )
        }?;
        if status == CALL_BUFFER_TOO_SMALL {
            if length == 0 || length > MAX_RESPONSE_BYTES {
                return Err(GuestCallError::Protocol(format!(
                    "host requested an invalid {length}-byte response buffer"
                )));
            }
            response.resize(length, 0);
            (status, length) = unsafe {
                invoke_import(
                    request.as_ptr(),
                    request.len(),
                    response.as_mut_ptr(),
                    response.len(),
                )
            }?;
        }
        if status != CALL_OK || length > response.len() {
            return Err(GuestCallError::Protocol(format!(
                "host returned status {status} and length {length}"
            )));
        }
        response.truncate(length);
        Ok(response)
    }

    #[cfg(target_arch = "wasm32")]
    unsafe fn invoke_import(
        request: *const u8,
        request_len: usize,
        response: *mut u8,
        response_capacity: usize,
    ) -> GuestResult<(u32, usize)> {
        if request_len == 0
            || request_len > i32::MAX as usize
            || response_capacity > i32::MAX as usize
        {
            return Err(GuestCallError::Protocol(
                "guest buffer length is outside the ABI range".to_string(),
            ));
        }
        let packed = unsafe {
            bicdb_application_host_call(
                request as i32,
                request_len as i32,
                response as i32,
                response_capacity as i32,
            )
        } as u64;
        Ok(((packed >> 32) as u32, packed as u32 as usize))
    }

    #[cfg(target_arch = "wasm32")]
    #[link(wasm_import_module = "bicdb:app/host")]
    unsafe extern "C" {
        #[link_name = "call"]
        fn bicdb_application_host_call(
            request_pointer: i32,
            request_len: i32,
            response_pointer: i32,
            response_capacity: i32,
        ) -> i64;
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn transport(_request: &[u8]) -> GuestResult<Vec<u8>> {
        Err(GuestCallError::Protocol(
            "ABI-v2 guest calls are available only inside a wasm32 module".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExtensionIdentity, ExtensionLimits, ExtensionPermissions};
    use serde_json::json;

    fn hash() -> String {
        "a".repeat(64)
    }

    #[test]
    fn capability_free_sql_projection_requires_select_only() {
        let projection = RawSqlDeclaration {
            id: "carrier_sql_projection".to_string(),
            sql: "SELECT $1::text".to_string(),
            sha256: hash(),
            relations: BTreeSet::new(),
            routines: BTreeSet::new(),
            actions: BTreeSet::from([DatabaseAction::Select]),
            parameters: vec![FieldType::String],
            result: vec![],
            max_affected_rows: 1,
        };
        projection.validate().unwrap();

        let mut write_without_authority = projection.clone();
        write_without_authority.actions = BTreeSet::from([DatabaseAction::Update]);
        assert!(write_without_authority.validate().is_err());

        let mut actionless = projection;
        actionless.actions.clear();
        assert!(actionless.validate().is_err());
    }

    fn application(required_features: BTreeSet<ApplicationFeature>) -> ApplicationManifestV2 {
        ApplicationManifestV2 {
            abi_version: 2,
            application_profile: APPLICATION_COMPATIBILITY_PROFILE.to_string(),
            package: PackageMetadata {
                application: "carrier".to_string(),
                version: "1.0.0".to_string(),
                package_sha256: hash(),
                dependency_lock_sha256: hash(),
                sbom_sha256: hash(),
                provenance_sha256: hash(),
                signature_key_id: "release".to_string(),
                signature_algorithm: "ed25519".to_string(),
                signature: "signed-package-value".to_string(),
            },
            relation_permissions: vec![],
            raw_sql: vec![],
            service_imports: vec![],
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
            required_features,
            max_call_depth: 16,
        }
    }

    #[test]
    fn application_manifest_accepts_legacy_carrier_fields_but_emits_neutral_fields() {
        let mut encoded = serde_json::to_value(application(BTreeSet::new())).unwrap();
        let object = encoded.as_object_mut().unwrap();
        let profile = object.remove("application_profile").unwrap();
        object.insert("carrier_profile".to_string(), profile);
        object.insert(
            "carrier_program".to_string(),
            json!({
                "version": 1
            }),
        );

        let decoded: ApplicationManifestV2 = serde_json::from_value(encoded).unwrap();
        assert_eq!(
            decoded.application_profile,
            APPLICATION_COMPATIBILITY_PROFILE
        );
        assert_eq!(decoded.application_program.as_ref().unwrap().version, 1);

        let canonical = serde_json::to_value(decoded).unwrap();
        assert_eq!(
            canonical["application_profile"],
            APPLICATION_COMPATIBILITY_PROFILE
        );
        assert!(canonical.get("application_program").is_some());
        assert!(canonical.get("carrier_profile").is_none());
        assert!(canonical.get("carrier_program").is_none());
    }

    fn extension(app: ApplicationManifestV2) -> ExtensionManifest {
        ExtensionManifest {
            identity: ExtensionIdentity {
                name: "carrier".to_string(),
                version: "1.0.0".to_string(),
                abi_version: 2,
                description: String::new(),
            },
            dependencies: vec![],
            capabilities: BTreeSet::new(),
            permissions: ExtensionPermissions::default(),
            limits: ExtensionLimits::default(),
            functions: vec![],
            indexes: vec![],
            storage: vec![],
            routes: vec![],
            subscriptions: vec![],
            observability: vec![],
            application: Some(Box::new(app)),
        }
    }

    fn carrier_security_extension() -> ExtensionManifest {
        let mut app = application(BTreeSet::from([ApplicationFeature::Http]));
        app.auth_schemes.insert(
            "Auth".to_string(),
            ApplicationAuthSchemeV1 {
                kind: ApplicationAuthKindV1::JwtHs256,
                issuer: "https://security.example".to_string(),
                audience: "carrier-users".to_string(),
            },
        );
        app.secrets.push(SecretDeclaration {
            name: "AUTH_KEY".to_string(),
            operations: BTreeSet::from([
                CryptoOperation::Metadata,
                CryptoOperation::Sign,
                CryptoOperation::Verify,
            ]),
            versions: BTreeSet::new(),
            allow_plaintext_read: false,
        });
        app.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "security": {
                    "version": 1,
                    "helpers": ["auth.register", "auth.login", "auth.issue_tokens"],
                    "auth_scheme": "Auth",
                    "signing_secret": "AUTH_KEY",
                    "signing_algorithm": "hmac-sha256",
                    "access_ttl_seconds": 900,
                    "refresh_ttl_seconds": 86400,
                    "durable_replay": true,
                    "versioned_keys": true,
                    "emit_evidence": true
                }
            }))
            .unwrap(),
        );
        app.routes = [
            ("register", "POST", "/auth/register", true),
            ("login", "POST", "/auth/login", true),
            ("refresh", "POST", "/auth/refresh", true),
            ("logout", "POST", "/auth/logout", false),
            ("sessions", "GET", "/auth/sessions", false),
            (
                "session_revoke",
                "DELETE",
                "/auth/sessions/{session_id}",
                false,
            ),
        ]
        .into_iter()
        .map(|(name, method, template, public)| {
            serde_json::from_value(json!({
                "name": format!("__carrier_security_{name}"),
                "method": method,
                "template": template,
                "export": format!("__carrier_security_{name}"),
                "public": public,
                "auth_scheme": (!public).then_some("Auth"),
                "max_request_bytes": 65536,
                "max_response_bytes": 1048576
            }))
            .unwrap()
        })
        .collect();
        let mut manifest = extension(app);
        manifest.capabilities.extend([
            ExtensionCapability::Database,
            ExtensionCapability::Transactions,
            ExtensionCapability::Clock,
            ExtensionCapability::Random,
            ExtensionCapability::SecretsCrypto,
            ExtensionCapability::Observability,
            ExtensionCapability::HttpRoutes,
        ]);
        manifest
    }

    #[test]
    fn carrier_security_routes_are_exact_and_cannot_be_removed_or_forged() {
        let valid = carrier_security_extension();
        valid.validate().unwrap();

        let mut forged = valid.clone();
        let logout = forged
            .application
            .as_deref_mut()
            .unwrap()
            .routes
            .iter_mut()
            .find(|route| route.export == "__carrier_security_logout")
            .unwrap();
        logout.public = true;
        logout.auth_scheme = None;
        assert!(forged
            .validate()
            .unwrap_err()
            .to_string()
            .contains("weakened or forged"));

        let mut missing = valid;
        missing
            .application
            .as_deref_mut()
            .unwrap()
            .routes
            .retain(|route| route.export != "__carrier_security_refresh");
        assert!(missing
            .validate()
            .unwrap_err()
            .to_string()
            .contains("lacks required host route"));
    }

    #[test]
    fn durable_schedule_policy_is_additive_and_feature_gated() {
        let legacy: ScheduleDefinition = serde_json::from_value(json!({
            "name": "daily_reconcile",
            "export": "reconcile",
            "schedule": "0 9 * * *",
            "timezone": "UTC",
            "payload": {},
            "required": true
        }))
        .unwrap();
        assert_eq!(legacy.misfire, ScheduleMisfirePolicy::Skip);
        assert_eq!(legacy.overlap, ScheduleOverlapPolicy::Skip);
        assert_eq!(legacy.max_concurrency, 1);
        assert_eq!(legacy.catch_up_limit, 100);
        assert_eq!(legacy.upgrade, ScheduleUpgradePolicy::Preserve);
        let encoded = serde_json::to_value(&legacy).unwrap();
        for additive in [
            "misfire",
            "overlap",
            "max_concurrency",
            "catch_up_limit",
            "upgrade",
        ] {
            assert!(encoded.get(additive).is_none(), "unexpected `{additive}`");
        }

        let mut durable = legacy;
        durable.timezone = "America/Los_Angeles".to_string();
        durable.misfire = ScheduleMisfirePolicy::CatchUp;
        durable.overlap = ScheduleOverlapPolicy::Queue;
        durable.max_concurrency = 3;
        durable.catch_up_limit = 12;
        durable.upgrade = ScheduleUpgradePolicy::Reset;
        let mut app = application(BTreeSet::from([ApplicationFeature::Schedules]));
        app.schedules.push(durable);
        let manifest = extension(app.clone());
        assert!(app
            .validate(&manifest)
            .unwrap_err()
            .to_string()
            .contains("without the DurableSchedules feature"));
        app.required_features
            .insert(ApplicationFeature::DurableSchedules);
        let manifest = extension(app.clone());
        app.validate(&manifest).unwrap();
    }

    fn realtime_extension() -> ExtensionManifest {
        let queue = "carrier_realtime_widgets";
        let mut app = application(BTreeSet::from([
            ApplicationFeature::Http,
            ApplicationFeature::Streaming,
            ApplicationFeature::Sse,
            ApplicationFeature::WebSocket,
        ]));
        app.realtime = vec![serde_json::from_value(json!({
            "name": "WidgetEvents",
            "path": "/streams/widgets",
            "queue": queue,
            "events": ["WidgetChanged"],
            "output": {
                "kind": "object",
                "fields": [
                    {"name": "tenant_id", "value_type": {"kind": "string"}, "optional": false},
                    {"name": "widget_id", "value_type": {"kind": "uuid"}, "optional": false}
                ]
            },
            "tenant_field": "tenant_id",
            "group_field": "widget_id",
            "authorize_group": "authorize_widget"
        }))
        .unwrap()];
        app.routes = [
            ("negotiate", false, false, false, false),
            ("poll", false, false, false, false),
            ("sse", false, true, true, false),
            ("ws", true, true, false, true),
        ]
        .into_iter()
        .map(
            |(suffix, streaming_request, streaming_response, sse, websocket)| {
                serde_json::from_value(json!({
                    "name": format!("widget_events_{suffix}"),
                    "method": "GET",
                    "template": format!("/streams/widgets/{suffix}"),
                    "export": format!("carrier_realtime_widget_events_{suffix}"),
                    "public": true,
                    "max_request_bytes": 65_536,
                    "max_response_bytes": 16_777_216,
                    "streaming_request": streaming_request,
                    "streaming_response": streaming_response,
                    "sse": sse,
                    "websocket": websocket
                }))
                .unwrap()
            },
        )
        .collect();
        app.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "callables": {
                    "authorize_widget": {"parameters": ["group"], "body": []}
                },
                "realtime_bindings": {
                    "WidgetChanged": [queue]
                }
            }))
            .unwrap(),
        );
        let mut manifest = extension(app);
        manifest.capabilities.extend([
            ExtensionCapability::HttpRoutes,
            ExtensionCapability::QueueEvents,
            ExtensionCapability::Streaming,
        ]);
        manifest
            .permissions
            .publish_queues
            .insert(queue.to_string());
        manifest
    }

    fn privacy_extension() -> ExtensionManifest {
        let mut app = application(BTreeSet::from([
            ApplicationFeature::Secrets,
            ApplicationFeature::Crypto,
            ApplicationFeature::Observability,
        ]));
        app.relation_permissions = vec![RelationPermission {
            relation: "private_records".to_string(),
            actions: BTreeSet::from([DatabaseAction::Select]),
            readable_columns: BTreeSet::new(),
            writable_columns: BTreeSet::new(),
        }];
        app.secrets = vec![SecretDeclaration {
            name: "PRIVATE_RECORD_KEY".to_string(),
            operations: BTreeSet::from([CryptoOperation::Encrypt, CryptoOperation::Decrypt]),
            versions: BTreeSet::new(),
            allow_plaintext_read: false,
        }];
        app.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "secret_bindings": {
                    "PrivateRecord.secret": "PRIVATE_RECORD_KEY"
                }
            }))
            .unwrap(),
        );
        app.resources = vec![serde_json::from_value(json!({
            "version": 1,
            "name": "PrivateRecord",
            "relation": "private_records",
            "schema_version": 1,
            "primary_key": "id",
            "fields": [
                {"name": "id", "field_type": "uuid"},
                {"name": "email", "field_type": "string"},
                {"name": "secret", "field_type": "string"}
            ],
            "operations": ["get"],
            "list_route": "/private-records",
            "item_route": "/private-records/{id}",
            "encrypted_fields": {"secret": "PRIVATE_RECORD_KEY"},
            "privacy": {
                "fields": {
                    "email": {
                        "pii_category": "contact",
                        "encrypted": false,
                        "classification": "pii",
                        "presentation": {
                            "list": "partial",
                            "detail": "visible",
                            "input": "editable",
                            "clipboard": "explicit"
                        }
                    },
                    "secret": {
                        "encrypted": true,
                        "classification": "confidential",
                        "presentation": {
                            "list": "hidden",
                            "detail": "masked",
                            "input": "editable",
                            "clipboard": "never"
                        }
                    }
                }
            },
            "audit": {
                "required": true,
                "include_fields": ["id", "email", "secret"],
                "redact_fields": ["email", "secret"]
            },
            "contract_sha256": hash()
        }))
        .unwrap()];
        let mut manifest = extension(app);
        manifest.capabilities.extend([
            ExtensionCapability::Database,
            ExtensionCapability::Transactions,
            ExtensionCapability::SecretsCrypto,
            ExtensionCapability::Observability,
        ]);
        manifest
            .permissions
            .read_relations
            .insert("private_records".to_string());
        manifest
    }

    #[test]
    fn unsupported_carrier_features_fail_before_activation() {
        let app = application(BTreeSet::from([ApplicationFeature::PlPgSql]));
        let manifest = extension(app);
        let error = manifest.validate().unwrap_err();
        assert!(error.to_string().contains("does not support"));
    }

    #[test]
    fn trusted_connection_identity_feature_is_supported() {
        let app = application(BTreeSet::from([
            ApplicationFeature::TrustedConnectionIdentity,
            ApplicationFeature::ExactColumnAuthority,
        ]));
        extension(app).validate().unwrap();
    }

    fn carrier_blob_extension() -> ExtensionManifest {
        let mut app = application(BTreeSet::from([
            ApplicationFeature::Blobs,
            ApplicationFeature::Observability,
        ]));
        app.blobs = vec![BlobDeclaration {
            namespace: "carrier".to_string(),
            max_blob_bytes: 67_108_864,
            content_types: BTreeSet::new(),
            allow_signed_urls: true,
            require_scan: false,
        }];
        app.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "callables": {
                    "metadata": {
                        "body": [{
                            "op": "return",
                            "value": {
                                "op": "call",
                                "kind": "builtin",
                                "target": "blob.metadata",
                                "arguments": [{
                                    "value": {"op": "literal", "value": "reports/annual.txt"}
                                }]
                            }
                        }]
                    },
                    "download": {
                        "body": [{
                            "op": "return",
                            "value": {
                                "op": "call",
                                "kind": "builtin",
                                "target": "blob.signed_url",
                                "arguments": [{
                                    "value": {"op": "literal", "value": "reports/annual.txt"}
                                }]
                            }
                        }]
                    }
                },
                "blob": {
                    "version": 1,
                    "helpers": ["blob.metadata", "blob.signed_url"],
                    "namespace": "carrier",
                    "signed_methods": ["GET"],
                    "max_blob_bytes": 67_108_864,
                    "virtual_files": true,
                    "durable_keys": true,
                    "emit_evidence": true
                }
            }))
            .unwrap(),
        );
        let mut manifest = extension(app);
        manifest.capabilities.extend([
            ExtensionCapability::Blobs,
            ExtensionCapability::Observability,
        ]);
        manifest
    }

    #[test]
    fn carrier_blob_contract_is_exact_and_fail_closed() {
        let valid = carrier_blob_extension();
        valid.validate().unwrap();

        let mut forged_helpers = valid.clone();
        forged_helpers
            .application
            .as_deref_mut()
            .unwrap()
            .application_program
            .as_mut()
            .unwrap()
            .blob
            .as_mut()
            .unwrap()
            .helpers
            .remove("blob.metadata");
        assert!(forged_helpers
            .validate()
            .unwrap_err()
            .to_string()
            .contains("differs from the signed behavior program"));

        let mut forged_limit = valid.clone();
        forged_limit.application.as_deref_mut().unwrap().blobs[0].max_blob_bytes -= 1;
        assert!(forged_limit
            .validate()
            .unwrap_err()
            .to_string()
            .contains("size/signed-URL authority disagree"));

        let mut forged_methods = valid.clone();
        forged_methods
            .application
            .as_deref_mut()
            .unwrap()
            .application_program
            .as_mut()
            .unwrap()
            .blob
            .as_mut()
            .unwrap()
            .signed_methods
            .insert("PUT".to_string());
        assert!(forged_methods
            .validate()
            .unwrap_err()
            .to_string()
            .contains("differs from the signed behavior program"));

        let mut missing_observability = valid.clone();
        missing_observability
            .capabilities
            .remove(&ExtensionCapability::Observability);
        assert!(missing_observability
            .validate()
            .unwrap_err()
            .to_string()
            .contains("Observability capability"));

        let mut ambient_files = valid;
        ambient_files
            .application
            .as_deref_mut()
            .unwrap()
            .application_program
            .as_mut()
            .unwrap()
            .blob
            .as_mut()
            .unwrap()
            .virtual_files = false;
        assert!(ambient_files
            .validate()
            .unwrap_err()
            .to_string()
            .contains("virtual files"));
    }

    #[test]
    fn realtime_contract_requires_exact_routes_and_durable_fanout() {
        let manifest = realtime_extension();
        manifest.validate().unwrap();

        let mut missing_fanout = manifest.clone();
        missing_fanout
            .application
            .as_mut()
            .unwrap()
            .application_program
            .as_mut()
            .unwrap()
            .realtime_bindings
            .clear();
        assert!(missing_fanout
            .validate()
            .unwrap_err()
            .to_string()
            .contains("lacks its exact fan-out queue"));

        let mut weakened_transport = manifest;
        let application = weakened_transport.application.as_mut().unwrap();
        application
            .routes
            .iter_mut()
            .find(|route| route.template.ends_with("/ws"))
            .unwrap()
            .streaming_request = false;
        assert!(weakened_transport
            .validate()
            .unwrap_err()
            .to_string()
            .contains("weakened transport contract"));
    }

    #[test]
    fn resource_privacy_encryption_and_audit_are_fail_closed() {
        let manifest = privacy_extension();
        manifest.validate().unwrap();

        let mut missing_binding = manifest.clone();
        missing_binding
            .application
            .as_mut()
            .unwrap()
            .application_program
            .as_mut()
            .unwrap()
            .secret_bindings
            .clear();
        assert!(missing_binding
            .validate()
            .unwrap_err()
            .to_string()
            .contains("exact host-only secret binding"));

        let mut weakened_audit = manifest.clone();
        weakened_audit.application.as_mut().unwrap().resources[0]
            .audit
            .redact_fields
            .remove("email");
        assert!(weakened_audit
            .validate()
            .unwrap_err()
            .to_string()
            .contains("redacted durable audit contract"));

        let mut public_access_audit = manifest;
        let application = public_access_audit.application.as_mut().unwrap();
        application.resources[0]
            .privacy
            .as_mut()
            .unwrap()
            .access_audit = Some(ResourceAccessAuditV1 {
            endpoint: "/private-records/{id}/access-events".to_string(),
            events: BTreeSet::from([ResourceAccessAuditEventV1::Open]),
            purpose: "treatment".to_string(),
        });
        application.routes.push(
            serde_json::from_value(json!({
                "name": "private_record_access",
                "method": "POST",
                "template": "/private-records/{id}/access-events",
                "export": "private_record_access",
                "public": true,
                "max_request_bytes": 65536,
                "max_response_bytes": 65536
            }))
            .unwrap(),
        );
        application
            .application_program
            .as_mut()
            .unwrap()
            .callables
            .insert(
                "private_record_access".to_string(),
                serde_json::from_value(json!({"body": []})).unwrap(),
            );
        assert!(public_access_audit
            .validate()
            .unwrap_err()
            .to_string()
            .contains("exact protected custom action"));
    }

    #[test]
    fn transaction_isolation_features_are_exact_and_supported() {
        let program: ApplicationProgramV1 = serde_json::from_value(json!({
            "version": 1,
            "callables": {
                "run": {
                    "body": [{
                        "op": "transaction",
                        "isolation": "serializable",
                        "body": [{"op": "return", "value": {"op": "literal", "value": true}}]
                    }]
                }
            }
        }))
        .unwrap();
        let mut app = application(BTreeSet::from([ApplicationFeature::Savepoints]));
        app.application_program = Some(program);
        let mut manifest = extension(app.clone());
        manifest
            .capabilities
            .insert(ExtensionCapability::Transactions);
        let error = manifest.validate().unwrap_err();
        assert!(error.to_string().contains("Serializable"));

        app.required_features
            .insert(ApplicationFeature::Serializable);
        let mut manifest = extension(app);
        manifest
            .capabilities
            .insert(ExtensionCapability::Transactions);
        manifest.validate().unwrap();
    }

    #[test]
    fn grpc_authority_is_exactly_the_methods_used_by_client_calls() {
        let program: ApplicationProgramV1 = serde_json::from_value(json!({
            "version": 1,
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
                            "Request": {"proto_name": "ReserveRequest", "fields": []},
                            "Response": {"proto_name": "ReserveResponse", "fields": []}
                        },
                        "max_request_bytes": 1024,
                        "max_response_bytes": 1024,
                        "emit_evidence": true
                    }
                }
            }
        }))
        .unwrap();
        program.validate().unwrap();

        let mut overbroad = program.clone();
        let client = overbroad
            .grpc
            .as_mut()
            .unwrap()
            .clients
            .get_mut("InventoryGrpc")
            .unwrap();
        client.methods.insert(
            "Release".to_string(),
            ApplicationGrpcMethodV1 {
                path: "/carrier.inventory.v1.Inventory/Release".to_string(),
                request_type: "Request".to_string(),
                response_type: "Response".to_string(),
                deadline_ms: 2_000,
                retries: 1,
            },
        );
        assert!(overbroad
            .validate()
            .unwrap_err()
            .to_string()
            .contains("grants unused method"));

        let mut undeclared = program;
        let mut undeclared_call = undeclared.callables.get("reserve").unwrap().body[0].clone();
        let ApplicationStatementV1::Return {
            value: ApplicationExpressionV1::Call { method, .. },
        } = &mut undeclared_call
        else {
            unreachable!()
        };
        *method = Some("Release".to_string());
        undeclared
            .callables
            .get_mut("reserve")
            .unwrap()
            .body
            .push(undeclared_call);
        assert!(undeclared
            .validate()
            .unwrap_err()
            .to_string()
            .contains("outside its exact HTTP or gRPC binding authority"));
    }

    #[test]
    fn ai_provider_authority_is_exactly_used_and_capability_bound() {
        let program: ApplicationProgramV1 = serde_json::from_value(json!({
            "version": 1,
            "callables": {
                "count": {"body": [{"op": "expr", "value": {
                    "op": "call", "kind": "builtin", "target": "tokenizer.count",
                    "arguments": [{"value": {"op": "literal", "value": "hello"}}]
                }}]},
                "embed": {"body": [{"op": "expr", "value": {
                    "op": "call", "kind": "builtin", "target": "embeddings.embed",
                    "arguments": [{"value": {"op": "literal", "value": "hello"}}]
                }}]},
                "write": {"body": [{"op": "expr", "value": {
                    "op": "call", "kind": "llm", "target": "Writer", "method": "respond_as",
                    "arguments": [
                        {"value": {"op": "literal", "value": "Draft"}},
                        {"value": {"op": "literal", "value": "write"}}
                    ]
                }}]}
            },
            "tokenizer": {"version": 1, "providers": {
                "default": {"max_input_bytes": 65536, "max_tokens": 4096, "emit_evidence": true}
            }},
            "embeddings": {"version": 1, "providers": {
                "default": {"dimensions": 768, "max_input_bytes": 65536, "emit_evidence": true}
            }},
            "llm": {"version": 1, "clients": {"Writer": {
                "provider": "Writer",
                "tokenizer_provider": "default",
                "methods": ["respond_as"],
                "max_prompt_bytes": 65536,
                "max_history_messages": 8,
                "max_output_tokens": 512,
                "max_response_bytes": 65536,
                "temperature_millis": 200,
                "operator_system_prompt": false,
                "structured_outputs": {"Draft": {"kind": "string"}},
                "emit_evidence": true
            }}}
        }))
        .unwrap();
        let mut app = application(BTreeSet::from([
            ApplicationFeature::Tokenizers,
            ApplicationFeature::Embeddings,
            ApplicationFeature::Llm,
            ApplicationFeature::Observability,
            ApplicationFeature::ReadCommitted,
        ]));
        app.application_program = Some(program);
        let mut signed = extension(app.clone());
        signed.capabilities.extend([
            ExtensionCapability::AiInference,
            ExtensionCapability::NetworkEgress,
            ExtensionCapability::Observability,
            ExtensionCapability::Database,
            ExtensionCapability::Transactions,
        ]);
        app.validate(&signed).unwrap();

        let mut forged = app.clone();
        forged
            .application_program
            .as_mut()
            .unwrap()
            .llm
            .as_mut()
            .unwrap()
            .clients
            .get_mut("Writer")
            .unwrap()
            .methods
            .insert("respond".to_string());
        assert!(forged.validate(&signed).is_err());

        let mut missing_contract = app.clone();
        missing_contract
            .application_program
            .as_mut()
            .unwrap()
            .embeddings = None;
        assert!(missing_contract.validate(&signed).is_err());

        let mut missing_capability = signed;
        missing_capability
            .capabilities
            .remove(&ExtensionCapability::AiInference);
        assert!(app.validate(&missing_capability).is_err());
    }

    #[test]
    fn feature_flag_contracts_are_bounded_and_validated() {
        let program: ApplicationProgramV1 = serde_json::from_value(json!({
            "version": 1,
            "flags": {
                "rollout": {
                    "default": false,
                    "rules": [
                        {"kind": "tenant_in", "tenants": ["tenant-a"], "value": true},
                        {"kind": "percentage", "percent": 25, "grouped_by": "workspace_id", "value": true}
                    ]
                }
            }
        }))
        .unwrap();
        program.validate().unwrap();

        let mut invalid = program;
        let ApplicationFlagRuleV1::Percentage { percent, .. } =
            &mut invalid.flags.get_mut("rollout").unwrap().rules[1]
        else {
            unreachable!()
        };
        *percent = 101;
        assert!(invalid
            .validate()
            .unwrap_err()
            .to_string()
            .contains("exceeds 100"));
    }

    #[test]
    fn direct_job_bindings_require_an_exact_callable() {
        let program: ApplicationProgramV1 = serde_json::from_value(json!({
            "version": 1,
            "callables": {
                "deliver": {"parameters": ["payload"], "body": []}
            },
            "job_bindings": {
                "deliver": "carrier_job_slice_deliver"
            }
        }))
        .unwrap();
        program.validate().unwrap();

        let mut invalid = program;
        invalid.callables.clear();
        assert!(invalid
            .validate()
            .unwrap_err()
            .to_string()
            .contains("absent callable"));
    }

    #[test]
    fn additive_empty_contracts_preserve_the_existing_canonical_wire_shape() {
        let app = application(BTreeSet::new());
        let encoded = serde_json::to_value(&app).unwrap();
        assert_eq!(encoded.get("invariants"), Some(&json!([])));
        assert!(encoded.get("auth_schemes").is_none());
        assert!(encoded.get("response_headers").is_none());
        let decoded: ApplicationManifestV2 = serde_json::from_value(encoded).unwrap();
        assert!(decoded.invariants.is_empty());
    }

    #[test]
    fn signed_invariant_dependencies_and_feature_are_fail_closed() {
        let mut app = application(BTreeSet::from([ApplicationFeature::CommitValidators]));
        app.resources = vec![serde_json::from_value(json!({
            "version": 1,
            "name": "InventoryItem",
            "relation": "inventory_items",
            "schema_version": 1,
            "schema_only": true,
            "primary_key": "id",
            "fields": [
                {"name": "id", "field_type": "uuid"},
                {"name": "quantity", "field_type": "int64"}
            ],
            "list_route": "/inventory",
            "item_route": "/inventory/{id}",
            "contract_sha256": "b".repeat(64),
            "openapi": {}
        }))
        .unwrap()];
        app.invariants = vec![ApplicationInvariantV1 {
            version: 1,
            name: "InventoryNeverNegative".to_string(),
            subject_resource: "InventoryItem".to_string(),
            kind: ApplicationInvariantKindV1::MustAlways,
            expression: serde_json::from_value(json!({
                "op": "binary",
                "operator": "greater_equal",
                "left": {"op": "variable", "name": "quantity"},
                "right": {"op": "literal", "value": 0},
                "value_type": "bool",
                "left_type": "int",
                "right_type": "int"
            }))
            .unwrap(),
            transition: None,
            dependency_resources: BTreeSet::from(["InventoryItem".to_string()]),
            source: "quantity >= 0".to_string(),
        }];
        extension(app.clone()).validate().unwrap();

        let mut forged_dependencies = app.clone();
        forged_dependencies.invariants[0]
            .dependency_resources
            .clear();
        assert!(extension(forged_dependencies).validate().is_err());

        app.required_features.clear();
        let error = extension(app).validate().unwrap_err();
        assert!(error.to_string().contains("commit_validators"));
    }

    #[test]
    fn application_program_call_depth_is_signed_and_bounded_by_the_application() {
        let decoded: ApplicationProgramV1 = serde_json::from_value(serde_json::json!({
            "version": 1
        }))
        .unwrap();
        assert_eq!(decoded.max_call_depth, 16);

        let mut app = application(BTreeSet::new());
        app.max_call_depth = 4;
        app.application_program = Some(ApplicationProgramV1 {
            version: 1,
            max_steps: 100,
            max_call_depth: 5,
            blob: None,
            redis: None,
            email: None,
            grpc: None,
            tokenizer: None,
            embeddings: None,
            llm: None,
            rag: None,
            agents: None,
            evaluations: None,
            tests: None,
            observability: None,
            callables: BTreeMap::new(),
            service_bindings: BTreeMap::new(),
            client_bindings: BTreeMap::new(),
            flags: BTreeMap::new(),
            job_bindings: BTreeMap::new(),
            secret_bindings: BTreeMap::new(),
            security: None,
            event_bindings: BTreeMap::new(),
            realtime_bindings: BTreeMap::new(),
            mutation_bindings: Vec::new(),
            workflow_bindings: BTreeMap::new(),
        });
        let error = extension(app).validate().unwrap_err();
        assert!(error
            .to_string()
            .contains("exceeds application max_call_depth"));
    }

    #[test]
    fn carrier_model_projections_are_paged_unique_and_model_bound() {
        let projection = serde_json::json!({
            "fields": [
                {"kind": "field", "name": "label", "field": "name"},
                {"kind": "field", "name": "label", "field": "title"}
            ]
        });
        let program = |kind: &str, method: &str| {
            serde_json::from_value::<ApplicationProgramV1>(serde_json::json!({
                "version": 1,
                "callables": {
                    "route": {
                        "body": [{
                            "op": "return",
                            "value": {
                                "op": "call",
                                "kind": kind,
                                "target": "Doctor",
                                "method": method,
                                "result_projection": projection,
                                "arguments": []
                            }
                        }]
                    }
                }
            }))
            .unwrap()
        };
        assert!(program("model", "list")
            .validate()
            .unwrap_err()
            .to_string()
            .contains("repeats field"));
        assert!(program("builtin", "list")
            .validate()
            .unwrap_err()
            .to_string()
            .contains("paged model call or typed declared SQL call"));
        assert!(program("model", "get")
            .validate()
            .unwrap_err()
            .to_string()
            .contains("paged model call or typed declared SQL call"));

        let typed_sql: ApplicationProgramV1 = serde_json::from_value(serde_json::json!({
            "version": 1,
            "callables": {
                "route": {
                    "body": [{
                        "op": "return",
                        "value": {
                            "op": "call",
                            "kind": "builtin",
                            "target": "sql.one_as",
                            "result_projection": {
                                "fields": [{"kind": "field", "name": "label", "field": "label"}]
                            },
                            "arguments": []
                        }
                    }]
                }
            }
        }))
        .unwrap();
        typed_sql.validate().unwrap();
        assert!(application_program_uses_typed_declared_sql_projections(
            &typed_sql
        ));
    }

    #[test]
    fn carrier_map_contract_preserves_typed_keys_and_rejects_structural_keys() {
        ApplicationRouteParameterTypeV1::Map {
            key: Box::new(ApplicationRouteParameterTypeV1::Int),
            value: Box::new(ApplicationRouteParameterTypeV1::String),
        }
        .validate(0)
        .unwrap();
        let invalid = ApplicationRouteParameterTypeV1::Map {
            key: Box::new(ApplicationRouteParameterTypeV1::Json),
            value: Box::new(ApplicationRouteParameterTypeV1::String),
        };
        assert!(invalid
            .validate(0)
            .unwrap_err()
            .to_string()
            .contains("map key"));
    }

    #[test]
    fn host_results_are_unambiguous() {
        HostCallResult::success(1, HostValue::Unit)
            .validate()
            .unwrap();
        let invalid = HostCallResult {
            request_id: 1,
            value: Some(HostValue::Unit),
            error: Some(HostError {
                code: "bad".to_string(),
                class: ErrorClass::Internal,
                message: "bad".to_string(),
                retryable: false,
                retry_after_ms: None,
                trace_id: "trace".to_string(),
            }),
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn native_guest_transport_fails_without_ambient_fallback() {
        let error = guest::call(HostRequest::Clock(ClockRequest::WallTime)).unwrap_err();
        assert!(error.to_string().contains("only inside a wasm32 module"));
    }

    #[test]
    fn flattened_validation_rules_round_trip_and_reject_unknown_fields() {
        let encoded = serde_json::json!({
            "field": "name",
            "kind": "min_length",
            "value": 2
        });
        let rule: ValidationRule = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(
            rule,
            ValidationRule {
                field: "name".to_string(),
                rule: ValidationRuleKind::MinLength { value: 2 },
            }
        );
        assert_eq!(serde_json::to_value(rule).unwrap(), encoded);

        let invalid = serde_json::json!({
            "field": "name",
            "kind": "min_length",
            "value": 2,
            "unexpected": true
        });
        assert!(serde_json::from_value::<ValidationRule>(invalid).is_err());

        for (encoded, expected) in [
            (
                serde_json::json!({"field":"quantity","kind":"minimum","value":"1"}),
                ValidationRuleKind::Minimum {
                    value: "1".to_string(),
                },
            ),
            (
                serde_json::json!({"field":"quantity","kind":"maximum","value":"100"}),
                ValidationRuleKind::Maximum {
                    value: "100".to_string(),
                },
            ),
            (
                serde_json::json!({"field":"contact_email","kind":"email"}),
                ValidationRuleKind::Email,
            ),
        ] {
            let rule: ValidationRule = serde_json::from_value(encoded.clone()).unwrap();
            assert_eq!(rule.rule, expected);
            assert_eq!(serde_json::to_value(rule).unwrap(), encoded);
        }
    }

    #[test]
    fn flattened_resource_filters_round_trip_and_reject_unknown_fields() {
        let encoded = serde_json::json!({
            "query_name": "name",
            "operator": "contains",
            "field": "name"
        });
        let filter: ResourceFilterContract = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(
            filter,
            ResourceFilterContract {
                query_name: "name".to_string(),
                filter: ResourceFilterKind::Contains {
                    field: "name".to_string(),
                },
            }
        );
        assert_eq!(serde_json::to_value(filter).unwrap(), encoded);

        let invalid = serde_json::json!({
            "query_name": "name",
            "operator": "contains",
            "field": "name",
            "unexpected": true
        });
        assert!(serde_json::from_value::<ResourceFilterContract>(invalid).is_err());

        let json_encoded = serde_json::json!({
            "query_name": "company_size",
            "operator": "json_minimum",
            "field": "data",
            "path": "/company/size",
            "value_type": {"kind": "int"}
        });
        let json_filter: ResourceFilterContract =
            serde_json::from_value(json_encoded.clone()).unwrap();
        assert_eq!(
            json_filter,
            ResourceFilterContract {
                query_name: "company_size".to_string(),
                filter: ResourceFilterKind::JsonMinimum {
                    field: "data".to_string(),
                    path: "/company/size".to_string(),
                    value_type: ApplicationRouteParameterTypeV1::Int,
                },
            }
        );
        assert_eq!(serde_json::to_value(json_filter).unwrap(), json_encoded);
    }

    #[test]
    fn resource_policy_expressions_reject_shadowed_bindings_and_structured_literals() {
        let resource = |name: &str, relation: &str, fields: JsonValue| {
            serde_json::from_value::<ResourceContractV1>(serde_json::json!({
                "version": 1,
                "name": name,
                "relation": relation,
                "schema_version": 1,
                "primary_key": "id",
                "fields": fields,
                "operations": ["list"],
                "list_route": format!("/{relation}"),
                "item_route": format!("/{relation}/{{id}}"),
                "contract_sha256": hash()
            }))
            .unwrap()
        };
        let patient = resource(
            "Patient",
            "patients",
            json!([
                {"name": "id", "field_type": "uuid"},
                {"name": "tenant_id", "field_type": "uuid"}
            ]),
        );
        let care_team = resource(
            "CareTeamMember",
            "care_team_members",
            json!([
                {"name": "id", "field_type": "uuid"},
                {"name": "patient_id", "field_type": "uuid"}
            ]),
        );
        let mut app = application(BTreeSet::new());
        app.resources = vec![patient.clone(), care_team];
        let exists = |binding: &str| ApplicationExpressionV1::Exists {
            binding: binding.to_string(),
            resource: "CareTeamMember".to_string(),
            condition: Box::new(ApplicationExpressionV1::Literal {
                value: JsonValue::Bool(true),
            }),
        };

        for binding in ["auth", "id", "tenant_id"] {
            let error = validate_resource_policy_expression(
                &app,
                &patient,
                &BTreeMap::new(),
                &exists(binding),
                0,
            )
            .unwrap_err();
            assert!(error
                .to_string()
                .contains("shadows a trusted or subject value"));
        }

        for value in [json!({"forged": true}), json!(["safe", {"forged": true}])] {
            let error = validate_resource_policy_expression(
                &app,
                &patient,
                &BTreeMap::new(),
                &ApplicationExpressionV1::Literal { value },
                0,
            )
            .unwrap_err();
            assert!(error
                .to_string()
                .contains("literals must be scalar or arrays of scalars"));
        }

        validate_resource_policy_expression(
            &app,
            &patient,
            &BTreeMap::new(),
            &ApplicationExpressionV1::Literal {
                value: json!(["doctor", "clinician", 7, true, null]),
            },
            0,
        )
        .unwrap();
    }

    #[test]
    fn resource_cache_and_idempotency_headers_are_canonical_and_guest_safe() {
        IdempotencyContract {
            header: "x-request-key".to_string(),
            ttl_seconds: 60,
            max_key_bytes: 128,
        }
        .validate()
        .unwrap();
        ResourceCacheContract {
            max_age_seconds: 60,
            private: true,
            vary: BTreeSet::from(["accept-language".to_string(), "authorization".to_string()]),
        }
        .validate()
        .unwrap();

        for header in [
            "Authorization",
            "authorization",
            "cache-control",
            "bad header",
        ] {
            assert!(IdempotencyContract {
                header: header.to_string(),
                ttl_seconds: 60,
                max_key_bytes: 128,
            }
            .validate()
            .is_err());
        }
        assert!(ResourceCacheContract {
            max_age_seconds: 60,
            private: false,
            vary: BTreeSet::from(["authorization".to_string()]),
        }
        .validate()
        .is_err());
        assert!(ResourceCacheContract {
            max_age_seconds: 60,
            private: true,
            vary: BTreeSet::from(["Cache-Control".to_string()]),
        }
        .validate()
        .is_err());
        assert!(IdempotencyContract {
            header: "idempotency-key".to_string(),
            ttl_seconds: 31_536_001,
            max_key_bytes: 128,
        }
        .validate()
        .is_err());
    }

    #[test]
    fn resource_unique_targets_are_signed_and_schema_bound() {
        let resource = |unique_targets: JsonValue| {
            serde_json::from_value::<ResourceContractV1>(serde_json::json!({
                "version": 1,
                "name": "Doctor",
                "relation": "carrier_doctor",
                "schema_version": 1,
                "primary_key": "id",
                "fields": [
                    {"name": "id", "field_type": "uuid"},
                    {"name": "external_id", "field_type": "string"},
                    {"name": "clinic_id", "field_type": "uuid"}
                ],
                "unique_targets": unique_targets,
                "relations": [{
                    "source_field": "clinic_id",
                    "target_resource": "Clinic",
                    "target_field": "id"
                }],
                "operations": ["list", "upsert"],
                "list_route": "/doctors",
                "item_route": "/doctors/{id}",
                "contract_sha256": hash()
            }))
            .unwrap()
        };
        let base_application = application(BTreeSet::new());

        let duplicate = resource(serde_json::json!([
            {"target": "external_id", "fields": ["external_id", "external_id"]}
        ]));
        assert!(duplicate
            .validate(&base_application)
            .unwrap_err()
            .to_string()
            .contains("duplicate or undeclared fields"));

        let undeclared = resource(serde_json::json!([
            {"target": "external_id", "fields": ["missing"]}
        ]));
        assert!(undeclared
            .validate(&base_application)
            .unwrap_err()
            .to_string()
            .contains("duplicate or undeclared fields"));

        assert!(
            serde_json::from_value::<ResourceUniqueTargetV1>(serde_json::json!({
                "target": "external_id",
                "fields": ["external_id"],
                "unsigned": true
            }))
            .is_err()
        );

        let clinic = serde_json::from_value::<ResourceContractV1>(serde_json::json!({
            "version": 1,
            "name": "Clinic",
            "relation": "carrier_clinic",
            "schema_version": 1,
            "primary_key": "id",
            "fields": [{"name": "id", "field_type": "uuid"}],
            "reverse_relations": [{
                "name": "doctors",
                "target_resource": "Doctor",
                "source_field": "clinic_id",
                "target_field": "id"
            }],
            "operations": ["get"],
            "list_route": "/clinics",
            "item_route": "/clinics/{id}",
            "contract_sha256": hash()
        }))
        .unwrap();
        let doctor = resource(serde_json::json!([]));
        let mut relation_application = application(BTreeSet::new());
        relation_application.relation_permissions = vec![
            RelationPermission {
                relation: "carrier_clinic".to_string(),
                actions: BTreeSet::from([DatabaseAction::Select]),
                readable_columns: BTreeSet::from(["id".to_string()]),
                writable_columns: BTreeSet::new(),
            },
            RelationPermission {
                relation: "carrier_doctor".to_string(),
                actions: BTreeSet::from([
                    DatabaseAction::Select,
                    DatabaseAction::Aggregate,
                    DatabaseAction::Upsert,
                ]),
                readable_columns: BTreeSet::from([
                    "id".to_string(),
                    "external_id".to_string(),
                    "clinic_id".to_string(),
                ]),
                writable_columns: BTreeSet::from([
                    "id".to_string(),
                    "external_id".to_string(),
                    "clinic_id".to_string(),
                ]),
            },
        ];
        relation_application.resources = vec![clinic.clone(), doctor];
        let mut under_authorized = relation_application.clone();
        under_authorized.relation_permissions[1]
            .actions
            .remove(&DatabaseAction::Aggregate);
        assert!(clinic
            .validate(&under_authorized)
            .unwrap_err()
            .to_string()
            .contains("target lacks list authority"));
        clinic.validate(&relation_application).unwrap();
    }

    #[test]
    fn resource_vector_search_is_signed_and_schema_bound() {
        let resource = |vector_search: JsonValue| {
            serde_json::from_value::<ResourceContractV1>(serde_json::json!({
                "version": 1,
                "name": "Doc",
                "relation": "docs",
                "schema_version": 1,
                "schema_only": true,
                "primary_key": "id",
                "fields": [
                    {"name": "id", "field_type": "uuid"},
                    {"name": "title", "field_type": "string"},
                    {"name": "body", "field_type": "string"},
                    {"name": "embedding", "field_type": {"vector": {"dimensions": 3}}}
                ],
                "vector_search": vector_search,
                "search_fields": ["body", "title"],
                "list_route": "/docs",
                "item_route": "/docs/{id}",
                "contract_sha256": hash()
            }))
            .unwrap()
        };
        let signed = serde_json::json!({
            "field": "embedding",
            "dimensions": 3,
            "metric": "cosine",
            "index_kind": "hnsw",
            "index_name": "idx_docs_embedding_hnsw",
            "text_fields": ["title", "body"]
        });
        let app = application(BTreeSet::new());
        resource(signed.clone()).validate(&app).unwrap();

        let mut wrong_dimensions = signed.clone();
        wrong_dimensions["dimensions"] = json!(4);
        assert!(resource(wrong_dimensions)
            .validate(&app)
            .unwrap_err()
            .to_string()
            .contains("dimensions do not match"));

        let mut unsigned_text_field = signed;
        unsigned_text_field["text_fields"] = json!(["title", "summary"]);
        assert!(resource(unsigned_text_field)
            .validate(&app)
            .unwrap_err()
            .to_string()
            .contains("invalid text field"));
    }

    #[test]
    fn resource_spatial_and_timeseries_contracts_are_schema_bound() {
        let resource = |location_type: JsonValue, time_field: &str| {
            serde_json::from_value::<ResourceContractV1>(serde_json::json!({
                "version": 1,
                "name": "Observation",
                "relation": "observations",
                "schema_version": 1,
                "schema_only": true,
                "primary_key": "id",
                "fields": [
                    {"name": "id", "field_type": "uuid"},
                    {"name": "location", "field_type": location_type},
                    {"name": "observed_at", "field_type": "timestamp"}
                ],
                "timeseries": {
                    "time_field": time_field,
                    "chunk_interval": "1 day",
                    "retention": "30 days",
                    "index_name": "idx_observations_observed_at_desc"
                },
                "indexes": [
                    {
                        "name": "idx_observations_location_spatial",
                        "fields": ["location"],
                        "kind": "spatial",
                        "paths": [["location"]]
                    },
                    {
                        "name": "idx_observations_observed_at_desc",
                        "fields": ["observed_at"]
                    }
                ],
                "list_route": "/observations",
                "item_route": "/observations/{id}",
                "contract_sha256": hash()
            }))
            .unwrap()
        };
        let app = application(BTreeSet::new());
        resource(
            json!({"geometry":{"srid":4326,"geometry_type":"point"}}),
            "observed_at",
        )
        .validate(&app)
        .unwrap();

        let wrong_geometry = resource(json!("string"), "observed_at");
        assert!(wrong_geometry
            .validate(&app)
            .unwrap_err()
            .to_string()
            .contains("exactly typed geometry"));

        let wrong_time = resource(
            json!({"geometry":{"srid":4326,"geometry_type":"point"}}),
            "location",
        );
        assert!(wrong_time
            .validate(&app)
            .unwrap_err()
            .to_string()
            .contains("invalid signed timeseries contract"));
    }

    #[test]
    fn schema_only_resources_validate_generated_indexes_checks_and_composite_foreign_keys() {
        let tenant: ResourceContractV1 = serde_json::from_value(serde_json::json!({
            "version": 1,
            "name": "TenantIdentity",
            "relation": "tenant_identities",
            "schema_version": 1,
            "schema_only": true,
            "primary_key": "id",
            "fields": [
                {"name": "id", "field_type": "uuid"},
                {"name": "tenant_code", "field_type": "string"},
                {"name": "region", "field_type": "string"}
            ],
            "unique_targets": [{
                "target": "tenant_region",
                "fields": ["tenant_code", "region"],
                "index_name": "tenant_region_uq"
            }],
            "operations": [],
            "list_route": "/__schema/tenant-identities",
            "item_route": "/__schema/tenant-identities/{id}",
            "contract_sha256": hash()
        }))
        .unwrap();
        let ledger: ResourceContractV1 = serde_json::from_value(serde_json::json!({
            "version": 1,
            "name": "LedgerRecord",
            "relation": "ledger_records",
            "schema_version": 1,
            "schema_only": true,
            "primary_key": "id",
            "fields": [
                {"name": "id", "field_type": "uuid"},
                {"name": "tenant_code", "storage_name": "tenant_key", "field_type": "string"},
                {"name": "region", "field_type": "string"},
                {"name": "amount", "field_type": "int64"},
                {
                    "name": "nonnegative",
                    "field_type": "bool",
                    "generated": true,
                    "generated_expression": {
                        "op": "binary",
                        "operator": "greater_equal",
                        "left": {"op": "variable", "name": "amount"},
                        "right": {"op": "literal", "value": 0},
                        "value_type": "bool",
                        "left_type": "int",
                        "right_type": "int"
                    }
                },
                {"name": "starts_on", "field_type": "date"},
                {"name": "ends_on", "field_type": "date"},
                {"name": "cancelled_at", "field_type": "timestamp", "nullable": true}
            ],
            "indexes": [{
                "name": "ledger_tenant_idx",
                "fields": ["tenant_code", "region"],
                "predicate": {
                    "op": "binary",
                    "operator": "equal",
                    "left": {"op": "variable", "name": "cancelled_at"},
                    "right": {"op": "literal", "value": null},
                    "value_type": "bool",
                    "left_type": "timestamp",
                    "right_type": "timestamp"
                }
            }],
            "checks": [{
                "name": "ledger_amount_nonnegative",
                "expression": {"op": "variable", "name": "nonnegative"}
            }],
            "foreign_keys": [{
                "name": "ledger_tenant_fk",
                "fields": ["tenant_code", "region"],
                "target_resource": "TenantIdentity",
                "target_fields": ["tenant_code", "region"]
            }],
            "exclusions": [{
                "name": "ledger_no_overlap",
                "elements": [
                    {"fields": ["tenant_code"], "operator": "="},
                    {"function": "daterange", "fields": ["starts_on", "ends_on"], "operator": "&&"}
                ]
            }],
            "operations": [],
            "list_route": "/__schema/ledger-records",
            "item_route": "/__schema/ledger-records/{id}",
            "contract_sha256": hash()
        }))
        .unwrap();
        let mut app = application(BTreeSet::new());
        app.resources = vec![tenant, ledger.clone()];
        ledger.validate(&app).unwrap();

        let mut operational_schema = ledger.clone();
        operational_schema
            .operations
            .insert(ResourceOperation::List);
        assert!(operational_schema
            .validate(&app)
            .unwrap_err()
            .to_string()
            .contains("operations exactly"));

        let mut incomplete_generated = ledger.clone();
        incomplete_generated.fields[4].generated_expression = None;
        assert!(incomplete_generated
            .validate(&app)
            .unwrap_err()
            .to_string()
            .contains("incomplete generated-field"));

        let mut undeclared_index = ledger.clone();
        undeclared_index.indexes[0]
            .fields
            .push("missing".to_string());
        assert!(undeclared_index
            .validate(&app)
            .unwrap_err()
            .to_string()
            .contains("duplicate or undeclared fields"));

        let mut undeclared_predicate = ledger.clone();
        let ApplicationExpressionV1::Binary { left, .. } = undeclared_predicate.indexes[0]
            .predicate
            .as_mut()
            .expect("partial predicate")
        else {
            panic!("binary predicate");
        };
        *left = Box::new(ApplicationExpressionV1::Variable {
            name: "missing".to_string(),
        });
        assert!(undeclared_predicate
            .validate(&app)
            .unwrap_err()
            .to_string()
            .contains("undeclared field"));

        let mut invalid_exclusion = ledger.clone();
        invalid_exclusion.exclusions[0].elements[1].function = Some("intrange".to_string());
        assert!(invalid_exclusion
            .validate(&app)
            .unwrap_err()
            .to_string()
            .contains("unsupported range constructor"));

        let mut incompatible_foreign_key = ledger;
        incompatible_foreign_key.foreign_keys[0].target_fields = vec!["id".to_string()];
        incompatible_foreign_key.foreign_keys[0].fields = vec!["tenant_code".to_string()];
        assert!(incompatible_foreign_key
            .validate(&app)
            .unwrap_err()
            .to_string()
            .contains("incompatible fields"));
    }

    #[test]
    fn many_to_many_reverse_relations_require_a_typed_authorized_join_resource() {
        let user = serde_json::from_value::<ResourceContractV1>(serde_json::json!({
            "version": 1,
            "name": "User",
            "relation": "carrier_user",
            "schema_version": 1,
            "primary_key": "id",
            "fields": [{"name": "id", "field_type": "uuid"}],
            "reverse_relations": [{
                "name": "roles",
                "target_resource": "Role",
                "source_field": "user_id",
                "target_field": "id",
                "via_resource": "UserRole",
                "via_target_field": "role_id"
            }],
            "operations": ["get"],
            "list_route": "/users",
            "item_route": "/users/{id}",
            "contract_sha256": hash()
        }))
        .unwrap();
        let role = serde_json::from_value::<ResourceContractV1>(serde_json::json!({
            "version": 1,
            "name": "Role",
            "relation": "carrier_role",
            "schema_version": 1,
            "primary_key": "id",
            "fields": [
                {"name": "id", "field_type": "uuid"},
                {"name": "slug", "field_type": "string"}
            ],
            "operations": ["list"],
            "list_route": "/roles",
            "item_route": "/roles/{id}",
            "contract_sha256": hash()
        }))
        .unwrap();
        let user_role = serde_json::from_value::<ResourceContractV1>(serde_json::json!({
            "version": 1,
            "name": "UserRole",
            "relation": "carrier_user_role",
            "schema_version": 1,
            "primary_key": "id",
            "fields": [
                {"name": "id", "field_type": "uuid"},
                {"name": "user_id", "field_type": "uuid"},
                {"name": "role_id", "field_type": "uuid"}
            ],
            "relations": [
                {
                    "source_field": "user_id",
                    "target_resource": "User",
                    "target_field": "id"
                },
                {
                    "source_field": "role_id",
                    "target_resource": "Role",
                    "target_field": "id"
                }
            ],
            "operations": ["list"],
            "list_route": "/user_roles",
            "item_route": "/user_roles/{id}",
            "contract_sha256": hash()
        }))
        .unwrap();
        let mut app = application(BTreeSet::new());
        app.relation_permissions = vec![
            RelationPermission {
                relation: "carrier_user".to_string(),
                actions: BTreeSet::from([DatabaseAction::Select]),
                readable_columns: BTreeSet::from(["id".to_string()]),
                writable_columns: BTreeSet::new(),
            },
            RelationPermission {
                relation: "carrier_role".to_string(),
                actions: BTreeSet::from([DatabaseAction::Select, DatabaseAction::Aggregate]),
                readable_columns: BTreeSet::from(["id".to_string(), "slug".to_string()]),
                writable_columns: BTreeSet::new(),
            },
            RelationPermission {
                relation: "carrier_user_role".to_string(),
                actions: BTreeSet::from([DatabaseAction::Select]),
                readable_columns: BTreeSet::from([
                    "id".to_string(),
                    "user_id".to_string(),
                    "role_id".to_string(),
                ]),
                writable_columns: BTreeSet::new(),
            },
        ];
        app.resources = vec![user.clone(), role, user_role];
        user.validate(&app).unwrap();

        let mut partial = user.clone();
        partial.reverse_relations[0].via_target_field = None;
        assert!(partial
            .validate(&app)
            .unwrap_err()
            .to_string()
            .contains("incompatible fields, join metadata"));

        let mut under_authorized = app.clone();
        under_authorized.relation_permissions[2]
            .actions
            .remove(&DatabaseAction::Select);
        assert!(user
            .validate(&under_authorized)
            .unwrap_err()
            .to_string()
            .contains("incompatible fields, join metadata"));

        let mut missing_list_operation = app.clone();
        missing_list_operation.resources[2]
            .operations
            .remove(&ResourceOperation::List);
        assert!(user
            .validate(&missing_list_operation)
            .unwrap_err()
            .to_string()
            .contains("incompatible fields, join metadata"));
    }

    fn service_route() -> RouteV2 {
        RouteV2 {
            name: "plugin_info".to_string(),
            method: HttpMethod::Get,
            template: "/plugin-info".to_string(),
            export: "carrier_service_route".to_string(),
            resource: None,
            operation: None,
            service_call: Some(RouteServiceCall {
                dependency: "tokenizer_plugin".to_string(),
                service: "tokenizer_plugin".to_string(),
                method: "plugin_info".to_string(),
            }),
            application_request: None,
            application_response: None,
            idempotency: None,
            cache: None,
            telemetry: None,
            response_headers: BTreeMap::new(),
            public: false,
            auth_scheme: None,
            roles: BTreeSet::new(),
            scopes: BTreeSet::new(),
            roles_any: false,
            scopes_any: false,
            max_request_bytes: 1024,
            max_response_bytes: 4096,
            streaming_request: false,
            streaming_response: false,
            sse: false,
            websocket: false,
        }
    }

    #[test]
    fn carrier_route_request_schema_must_match_the_template() {
        let mut route = service_route();
        route.template = "/plugin-info/{id}".to_string();
        route.application_request = Some(ApplicationRouteRequestV1 {
            path_parameters: Vec::new(),
            query_parameters: Vec::new(),
            body: None,
        });
        let error = route.validate().unwrap_err();
        assert!(error
            .to_string()
            .contains("path schema does not exactly match"));
    }

    #[test]
    fn signed_auth_scheme_selection_is_complete_and_fail_closed() {
        let mut app = application(BTreeSet::new());
        app.auth_schemes.insert(
            "primary".to_string(),
            ApplicationAuthSchemeV1 {
                kind: ApplicationAuthKindV1::JwtHs256,
                issuer: "issuer".to_string(),
                audience: "audience".to_string(),
            },
        );
        app.routes.push(service_route());
        let error = app.validate(&extension(app.clone())).unwrap_err();
        assert!(error
            .to_string()
            .contains("must select an authentication scheme"));

        app.routes[0].auth_scheme = Some("missing".to_string());
        let error = app.validate(&extension(app.clone())).unwrap_err();
        assert!(error
            .to_string()
            .contains("references absent authentication scheme"));

        app.routes[0].public = true;
        let error = app.validate(&extension(app.clone())).unwrap_err();
        assert!(error
            .to_string()
            .contains("public route `plugin_info` cannot select"));
    }

    #[test]
    fn signed_response_headers_reject_host_controlled_values() {
        let mut route = service_route();
        route
            .response_headers
            .insert("set-cookie".to_string(), "session=forged".to_string());
        let error = route.validate().unwrap_err();
        assert!(error
            .to_string()
            .contains("host-controlled response header"));
    }

    #[test]
    fn service_routes_must_bind_a_declared_import() {
        let mut app = application(BTreeSet::new());
        app.routes.push(service_route());
        let error = app.validate(&extension(app.clone())).unwrap_err();
        assert!(error.to_string().contains("undeclared plugin service"));

        app.service_imports.push(ServiceImport {
            name: "tokenizer_plugin".to_string(),
            service: "tokenizer_plugin".to_string(),
            version: "=0.1.0".to_string(),
            contract_sha256: hash(),
            optional: false,
            propagate_transaction: false,
            allow_reentrant: false,
            delegated_authority: false,
        });
        app.validate(&extension(app.clone())).unwrap();

        app.service_imports[0].delegated_authority = true;
        let error = app.validate(&extension(app.clone())).unwrap_err();
        assert!(error
            .to_string()
            .contains("delegated service authority and its required feature"));
        app.required_features
            .insert(ApplicationFeature::DelegatedServiceAuthority);
        app.validate(&extension(app.clone())).unwrap();
    }

    #[test]
    fn service_routes_cannot_also_bind_resources() {
        let mut route = service_route();
        route.resource = Some("Widget".to_string());
        route.operation = Some(ResourceOperation::Get);
        let error = route.validate().unwrap_err();
        assert!(error
            .to_string()
            .contains("both a resource and plugin service"));
    }

    #[test]
    fn route_cache_is_positive_get_only_and_direct() {
        let mut route = service_route();
        route.service_call = None;
        route.cache = Some(RouteCacheContract { ttl_seconds: 0 });
        assert!(route
            .validate()
            .unwrap_err()
            .to_string()
            .contains("must be positive"));

        route.cache = Some(RouteCacheContract { ttl_seconds: 60 });
        route.method = HttpMethod::Post;
        assert!(route
            .validate()
            .unwrap_err()
            .to_string()
            .contains("requires the GET method"));

        route.method = HttpMethod::Get;
        route.service_call = Some(RouteServiceCall {
            dependency: "tokenizer_plugin".to_string(),
            service: "tokenizer_plugin".to_string(),
            method: "plugin_info".to_string(),
        });
        assert!(route
            .validate()
            .unwrap_err()
            .to_string()
            .contains("direct BicDB application program route"));

        route.service_call = None;
        route.idempotency = Some(IdempotencyContract {
            header: "idempotency-key".to_string(),
            ttl_seconds: 60,
            max_key_bytes: 128,
        });
        assert!(route
            .validate()
            .unwrap_err()
            .to_string()
            .contains("cannot combine idempotency and cache"));
    }

    #[test]
    fn cache_storage_requires_signed_database_and_transaction_capabilities() {
        let mut route = service_route();
        route.service_call = None;
        route.cache = Some(RouteCacheContract { ttl_seconds: 60 });
        let export = route.export.clone();
        let mut app = application(BTreeSet::new());
        app.routes.push(route);
        app.application_program = Some(ApplicationProgramV1 {
            version: 1,
            max_steps: 100,
            max_call_depth: 16,
            blob: None,
            redis: None,
            email: None,
            grpc: None,
            tokenizer: None,
            embeddings: None,
            llm: None,
            rag: None,
            agents: None,
            evaluations: None,
            tests: None,
            observability: None,
            callables: BTreeMap::from([(
                export,
                ApplicationCallableV1 {
                    parameters: Vec::new(),
                    body: Vec::new(),
                },
            )]),
            service_bindings: BTreeMap::new(),
            client_bindings: BTreeMap::new(),
            flags: BTreeMap::new(),
            job_bindings: BTreeMap::new(),
            secret_bindings: BTreeMap::new(),
            security: None,
            event_bindings: BTreeMap::new(),
            realtime_bindings: BTreeMap::new(),
            mutation_bindings: Vec::new(),
            workflow_bindings: BTreeMap::new(),
        });
        let unsigned = extension(app.clone());
        assert!(app
            .validate(&unsigned)
            .unwrap_err()
            .to_string()
            .contains("route cache requires signed database"));

        let mut signed = extension(app.clone());
        signed.capabilities.extend([
            ExtensionCapability::Database,
            ExtensionCapability::Transactions,
        ]);
        app.validate(&signed).unwrap();

        app.routes.clear();
        app.application_program.as_mut().unwrap().callables = BTreeMap::from([(
            "cached".to_string(),
            ApplicationCallableV1 {
                parameters: Vec::new(),
                body: vec![ApplicationStatementV1::Expr {
                    value: ApplicationExpressionV1::Call {
                        kind: ApplicationCallKindV1::Builtin,
                        target: "cache.exists".to_string(),
                        method: None,
                        result_type: Some(ApplicationExpressionTypeV1::Bool),
                        argument_types: vec![ApplicationExpressionTypeV1::String],
                        argument_item_types: vec![None],
                        result_projection: None,
                        arguments: vec![ApplicationArgumentV1 {
                            name: None,
                            value: ApplicationExpressionV1::Literal {
                                value: JsonValue::String("key".to_string()),
                            },
                        }],
                    },
                }],
            },
        )]);
        let unsigned = extension(app.clone());
        assert!(app
            .validate(&unsigned)
            .unwrap_err()
            .to_string()
            .contains("runtime cache requires signed database"));
    }

    #[test]
    fn provider_bound_egress_has_no_packaged_endpoint_or_header_values() {
        let mut declaration = EgressDeclaration {
            name: "Webhook".to_string(),
            provider: Some("webhook".to_string()),
            required_provider_headers: BTreeSet::from(["authorization".to_string()]),
            schemes: BTreeSet::from(["http".to_string(), "https".to_string()]),
            hosts: BTreeSet::new(),
            ports: BTreeSet::new(),
            allow_redirects: true,
            allow_private_networks: false,
            mtls_secret: None,
            max_request_bytes: 1024,
            max_response_bytes: 2048,
            timeout_ms: 1_000,
            max_concurrency: 2,
            requests_per_minute: 10,
        };
        declaration.validate().unwrap();
        declaration.hosts.insert("api.example.test".to_string());
        assert!(declaration.validate().is_err());
        declaration.hosts.clear();
        declaration.provider = None;
        assert!(declaration.validate().is_err());
        declaration.provider = Some("webhook".to_string());
        declaration.required_provider_headers = BTreeSet::from(["host".to_string()]);
        assert!(declaration.validate().is_err());
        declaration.required_provider_headers =
            BTreeSet::from(["authorization".to_string(), "Authorization".to_string()]);
        assert!(declaration.validate().is_err());
    }

    #[test]
    fn carrier_redis_contract_matches_program_features_and_capabilities_exactly() {
        let call = ApplicationExpressionV1::Call {
            kind: ApplicationCallKindV1::Builtin,
            target: "redis.incr".to_string(),
            method: None,
            result_type: Some(ApplicationExpressionTypeV1::Int),
            argument_types: vec![ApplicationExpressionTypeV1::String],
            argument_item_types: vec![None],
            result_projection: None,
            arguments: vec![ApplicationArgumentV1 {
                name: None,
                value: ApplicationExpressionV1::Literal {
                    value: JsonValue::String("counter".to_string()),
                },
            }],
        };
        let mut app = application(BTreeSet::from([
            ApplicationFeature::Egress,
            ApplicationFeature::Observability,
        ]));
        app.application_program = Some(ApplicationProgramV1 {
            version: 1,
            max_steps: 100,
            max_call_depth: 16,
            blob: None,
            redis: Some(ApplicationRedisContractV1 {
                version: 1,
                provider: "default".to_string(),
                helpers: BTreeSet::from(["redis.incr".to_string()]),
                max_key_bytes: 16 * 1024,
                max_channel_bytes: 16 * 1024,
                max_message_bytes: 16 * 1024 * 1024,
                tenant_scoped_keys: true,
                application_scoped_channels: true,
                emit_evidence: true,
            }),
            email: None,
            grpc: None,
            tokenizer: None,
            embeddings: None,
            llm: None,
            rag: None,
            agents: None,
            evaluations: None,
            tests: None,
            observability: None,
            callables: BTreeMap::from([(
                "route".to_string(),
                ApplicationCallableV1 {
                    parameters: Vec::new(),
                    body: vec![ApplicationStatementV1::Expr { value: call }],
                },
            )]),
            service_bindings: BTreeMap::new(),
            client_bindings: BTreeMap::new(),
            flags: BTreeMap::new(),
            job_bindings: BTreeMap::new(),
            secret_bindings: BTreeMap::new(),
            security: None,
            event_bindings: BTreeMap::new(),
            realtime_bindings: BTreeMap::new(),
            mutation_bindings: Vec::new(),
            workflow_bindings: BTreeMap::new(),
        });
        let mut signed = extension(app.clone());
        signed.capabilities.extend([
            ExtensionCapability::NetworkEgress,
            ExtensionCapability::Observability,
        ]);
        app.validate(&signed).unwrap();

        let mut forged = app.clone();
        forged
            .application_program
            .as_mut()
            .unwrap()
            .redis
            .as_mut()
            .unwrap()
            .helpers
            .insert("redis.publish".to_string());
        assert!(forged
            .validate(&extension(forged.clone()))
            .unwrap_err()
            .to_string()
            .contains("helper set differs"));

        let mut missing_feature = app.clone();
        missing_feature
            .required_features
            .remove(&ApplicationFeature::Observability);
        let mut manifest = extension(missing_feature.clone());
        manifest.capabilities.extend([
            ExtensionCapability::NetworkEgress,
            ExtensionCapability::Observability,
        ]);
        assert!(missing_feature.validate(&manifest).is_err());

        let missing_capability = extension(app.clone());
        assert!(app.validate(&missing_capability).is_err());
    }

    #[test]
    fn carrier_test_contract_is_bounded_feature_gated_and_callable_exact() {
        let mut app = application(BTreeSet::from([
            ApplicationFeature::Tests,
            ApplicationFeature::Observability,
        ]));
        app.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "callables": {
                    "test_case": {
                        "parameters": [],
                        "body": [{
                            "op": "return",
                            "value": {"op": "literal", "value": true}
                        }]
                    }
                },
                "tests": {
                    "version": 1,
                    "tests": {
                        "signed_test": {
                            "callable": "test_case",
                            "cases": 1,
                            "seed": 74,
                            "timeout_ms": 300000,
                            "emit_evidence": true
                        }
                    }
                }
            }))
            .unwrap(),
        );
        let mut signed = extension(app.clone());
        signed
            .capabilities
            .insert(ExtensionCapability::Observability);
        app.validate(&signed).unwrap();

        let mut unbounded = app.clone();
        unbounded
            .application_program
            .as_mut()
            .unwrap()
            .tests
            .as_mut()
            .unwrap()
            .tests
            .get_mut("signed_test")
            .unwrap()
            .cases = 0;
        assert!(unbounded.validate(&signed).is_err());

        let mut missing_callable = app.clone();
        missing_callable
            .application_program
            .as_mut()
            .unwrap()
            .callables
            .clear();
        assert!(missing_callable
            .validate(&signed)
            .unwrap_err()
            .to_string()
            .contains("absent callable"));

        let mut missing_feature = app.clone();
        missing_feature
            .required_features
            .remove(&ApplicationFeature::Observability);
        assert!(missing_feature
            .validate(&signed)
            .unwrap_err()
            .to_string()
            .contains("Observability"));
    }

    #[test]
    fn carrier_email_contract_rejects_request_selected_transport_authority() {
        let mut app = application(BTreeSet::from([
            ApplicationFeature::Egress,
            ApplicationFeature::Observability,
        ]));
        app.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "callables": {
                    "send": {
                        "parameters": ["to", "subject", "text"],
                        "body": [{
                            "op": "return",
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
                    "max_recipients": 100,
                    "max_address_bytes": 512,
                    "max_subject_bytes": 8192,
                    "max_body_bytes": 4194304,
                    "emit_evidence": true
                }
            }))
            .unwrap(),
        );
        let mut signed = extension(app.clone());
        signed.capabilities.extend([
            ExtensionCapability::NetworkEgress,
            ExtensionCapability::Observability,
        ]);
        app.validate(&signed).unwrap();

        let program = app.application_program.as_mut().unwrap();
        let ApplicationStatementV1::Return {
            value: ApplicationExpressionV1::Call { arguments, .. },
        } = &mut program.callables.get_mut("send").unwrap().body[0]
        else {
            panic!("email helper call");
        };
        arguments.push(ApplicationArgumentV1 {
            name: Some("smtp_url".to_string()),
            value: ApplicationExpressionV1::Literal {
                value: json!("smtp://attacker.invalid"),
            },
        });
        let error = app.validate(&signed).unwrap_err().to_string();
        assert!(error.contains("without a request-selected SMTP endpoint"));
    }

    #[test]
    fn carrier_observability_authority_is_exact_and_route_sampling_is_signed() {
        let mut app = application(BTreeSet::from([
            ApplicationFeature::Http,
            ApplicationFeature::Observability,
            ApplicationFeature::ReadCommitted,
        ]));
        app.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "callables": {
                    "run": {
                        "parameters": [],
                        "body": [
                            {"op": "expr", "value": {
                                "op": "call", "kind": "builtin",
                                "target": "metrics.counter.increment",
                                "arguments": [
                                    {"value": {"op": "literal", "value": "carrier.requests"}},
                                    {"value": {"op": "literal", "value": 1}}
                                ]
                            }},
                            {"op": "return", "value": {
                                "op": "call", "kind": "builtin", "target": "audit.record",
                                "arguments": [
                                    {"value": {"op": "literal", "value": "patient.read"}},
                                    {"value": {"op": "literal", "value": "Patient"}},
                                    {"value": {"op": "literal", "value": "patient-1"}}
                                ]
                            }}
                        ]
                    }
                },
                "observability": {
                    "version": 1,
                    "provider": "bicdb",
                    "protocol": "host",
                    "service_name": "carrier",
                    "sampling": {"kind": "parent_based_ratio", "millionths": 250000},
                    "helpers": ["audit.record", "metrics.counter.increment"],
                    "redacted_keys": ["email", "password"],
                    "metric_names": ["carrier.requests"],
                    "audit_actions": ["patient.read"],
                    "dynamic_metric_names": false,
                    "dynamic_audit_actions": false,
                    "max_field_depth": 16,
                    "max_field_bytes": 65536,
                    "durable_audit": true,
                    "propagate_w3c": true
                }
            }))
            .unwrap(),
        );
        let mut route = service_route();
        route.service_call = None;
        route.export = "run".to_string();
        route.public = true;
        route.telemetry = Some(ApplicationRouteTelemetryV1 {
            sampling: ApplicationSamplingV1::AlwaysOn,
        });
        app.routes.push(route);
        let mut signed = extension(app.clone());
        signed.capabilities.extend([
            ExtensionCapability::HttpRoutes,
            ExtensionCapability::Observability,
            ExtensionCapability::Database,
            ExtensionCapability::Transactions,
        ]);
        app.validate(&signed).unwrap();

        let mut forged = app.clone();
        forged
            .application_program
            .as_mut()
            .unwrap()
            .observability
            .as_mut()
            .unwrap()
            .metric_names
            .insert("forged.metric".to_string());
        assert!(forged
            .validate(&signed)
            .unwrap_err()
            .to_string()
            .contains("metric-name authority"));

        signed
            .capabilities
            .remove(&ExtensionCapability::Transactions);
        assert!(app
            .validate(&signed)
            .unwrap_err()
            .to_string()
            .contains("durable audit requires"));
    }
}
