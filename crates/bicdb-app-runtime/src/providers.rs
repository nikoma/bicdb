use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::{IpAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use bicdb_extension::abi_v2::{
    ActorContext, ApplicationGrpcClientV1, ApplicationLlmClientV1, ApplicationLlmToolV1,
    ApplicationObservabilityContractV1, ApplicationRouteParameterTypeV1,
    ApplicationTelemetryProtocolV1, BlobMetadata, EgressDeclaration, EgressResponse, LogLevel,
    MetricKind,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use url::Url;
use uuid::Uuid;

use crate::{AppRuntimeError, Result};
use crate::{LiveRealtimeFrame, LiveRealtimeResponse, RealtimeProvider, RealtimeResponse};

#[derive(Clone, Debug)]
pub struct SecretRecord {
    pub name: String,
    pub key_id: String,
    pub version: String,
    pub algorithm: String,
    pub material: Arc<[u8]>,
}

pub trait SecretProvider: Send + Sync {
    fn open(&self, name: &str, version: Option<&str>) -> Result<SecretRecord>;
}

#[derive(Clone, Default)]
pub struct InMemorySecretProvider {
    records: Arc<Mutex<BTreeMap<(String, String), SecretRecord>>>,
    current: Arc<Mutex<BTreeMap<String, String>>>,
}

impl InMemorySecretProvider {
    pub fn insert(
        &self,
        name: impl Into<String>,
        version: impl Into<String>,
        key_id: impl Into<String>,
        algorithm: impl Into<String>,
        material: Vec<u8>,
        current: bool,
    ) -> Result<()> {
        let name = name.into();
        let version = version.into();
        if name.is_empty() || version.is_empty() || material.is_empty() {
            return Err(AppRuntimeError::Provider(
                "secret name, version, and material are required".to_string(),
            ));
        }
        self.records.lock().expect("secret store poisoned").insert(
            (name.clone(), version.clone()),
            SecretRecord {
                name: name.clone(),
                key_id: key_id.into(),
                version: version.clone(),
                algorithm: algorithm.into(),
                material: Arc::from(material),
            },
        );
        if current {
            self.current
                .lock()
                .expect("secret store poisoned")
                .insert(name, version);
        }
        Ok(())
    }
}

impl SecretProvider for InMemorySecretProvider {
    fn open(&self, name: &str, version: Option<&str>) -> Result<SecretRecord> {
        let version = match version {
            Some(version) => version.to_string(),
            None => self
                .current
                .lock()
                .expect("secret store poisoned")
                .get(name)
                .cloned()
                .ok_or_else(|| {
                    AppRuntimeError::Provider(format!("secret `{name}` has no current version"))
                })?,
        };
        self.records
            .lock()
            .expect("secret store poisoned")
            .get(&(name.to_string(), version.clone()))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::Provider(format!("secret `{name}` version `{version}` not found"))
            })
    }
}

#[derive(Clone, Debug)]
pub struct BlobRecord {
    pub metadata: BlobMetadata,
    pub bytes: Vec<u8>,
}

pub trait BlobProvider: Send + Sync {
    fn put(
        &self,
        namespace: &str,
        bytes: &[u8],
        content_type: Option<&str>,
        metadata: &BTreeMap<String, String>,
        require_scan: bool,
    ) -> Result<BlobMetadata>;
    fn get(&self, namespace: &str, blob_id: &str) -> Result<Option<BlobRecord>>;
    fn delete(&self, namespace: &str, blob_id: &str) -> Result<bool>;
    fn signed_url(&self, namespace: &str, blob_id: &str, expires_seconds: u32) -> Result<String>;
    fn put_named(
        &self,
        namespace: &str,
        key: &str,
        bytes: &[u8],
        content_type: Option<&str>,
        metadata: &BTreeMap<String, String>,
        require_scan: bool,
    ) -> Result<BlobMetadata>;
    fn get_named(&self, namespace: &str, key: &str) -> Result<Option<BlobRecord>>;
    fn delete_named(&self, namespace: &str, key: &str) -> Result<bool>;
    fn signed_url_named(
        &self,
        namespace: &str,
        key: &str,
        expires_seconds: u32,
        method: &str,
        download_name: Option<&str>,
    ) -> Result<String>;
    fn authorize_named_url(
        &self,
        namespace: &str,
        key: &str,
        expires: i64,
        method: &str,
        download_name: Option<&str>,
        signature: &str,
    ) -> Result<()>;
}

/// Fail-closed blob capability used by runtimes that have not constructed an
/// approved durable blob provider. Keeping this as a real provider avoids
/// substituting an ambient filesystem path merely to satisfy the host API.
#[derive(Clone, Debug, Default)]
pub struct DenyBlobProvider;

impl BlobProvider for DenyBlobProvider {
    fn put(
        &self,
        _namespace: &str,
        _bytes: &[u8],
        _content_type: Option<&str>,
        _metadata: &BTreeMap<String, String>,
        _require_scan: bool,
    ) -> Result<BlobMetadata> {
        provider_error("blob provider is unavailable")
    }

    fn get(&self, _namespace: &str, _blob_id: &str) -> Result<Option<BlobRecord>> {
        provider_error("blob provider is unavailable")
    }

    fn delete(&self, _namespace: &str, _blob_id: &str) -> Result<bool> {
        provider_error("blob provider is unavailable")
    }

    fn signed_url(
        &self,
        _namespace: &str,
        _blob_id: &str,
        _expires_seconds: u32,
    ) -> Result<String> {
        provider_error("blob provider is unavailable")
    }

    fn put_named(
        &self,
        _namespace: &str,
        _key: &str,
        _bytes: &[u8],
        _content_type: Option<&str>,
        _metadata: &BTreeMap<String, String>,
        _require_scan: bool,
    ) -> Result<BlobMetadata> {
        provider_error("blob provider is unavailable")
    }

    fn get_named(&self, _namespace: &str, _key: &str) -> Result<Option<BlobRecord>> {
        provider_error("blob provider is unavailable")
    }

    fn delete_named(&self, _namespace: &str, _key: &str) -> Result<bool> {
        provider_error("blob provider is unavailable")
    }

    fn signed_url_named(
        &self,
        _namespace: &str,
        _key: &str,
        _expires_seconds: u32,
        _method: &str,
        _download_name: Option<&str>,
    ) -> Result<String> {
        provider_error("blob provider is unavailable")
    }

    fn authorize_named_url(
        &self,
        _namespace: &str,
        _key: &str,
        _expires: i64,
        _method: &str,
        _download_name: Option<&str>,
        _signature: &str,
    ) -> Result<()> {
        provider_error("blob provider is unavailable")
    }
}

#[derive(Clone, Debug)]
pub struct LocalBlobProvider {
    root: PathBuf,
    signing_key: Arc<[u8]>,
}

impl LocalBlobProvider {
    pub fn open(root: impl AsRef<Path>, signing_key: Vec<u8>) -> Result<Self> {
        if signing_key.len() < 32 {
            return Err(AppRuntimeError::Provider(
                "blob URL signing key must contain at least 32 bytes".to_string(),
            ));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            signing_key: Arc::from(signing_key),
        })
    }

    fn namespace(&self, namespace: &str) -> Result<PathBuf> {
        validate_component("blob namespace", namespace)?;
        Ok(self.root.join(namespace))
    }

    fn paths(&self, namespace: &str, blob_id: &str) -> Result<(PathBuf, PathBuf)> {
        validate_component("blob id", blob_id)?;
        let namespace = self.namespace(namespace)?;
        Ok((
            namespace.join(format!("{blob_id}.blob")),
            namespace.join(format!("{blob_id}.json")),
        ))
    }

    fn named_blob_id(key: &str) -> Result<String> {
        validate_blob_key(key)?;
        let mut digest = Sha256::new();
        digest.update(b"bicdb-named-blob-v1\0");
        digest.update(key.as_bytes());
        Ok(digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect())
    }

    fn put_with_id(
        &self,
        namespace: &str,
        blob_id: String,
        bytes: &[u8],
        content_type: Option<&str>,
        metadata: BTreeMap<String, String>,
        require_scan: bool,
    ) -> Result<BlobMetadata> {
        let (blob_path, metadata_path) = self.paths(namespace, &blob_id)?;
        let directory = blob_path.parent().expect("blob path always has namespace");
        fs::create_dir_all(directory)?;
        let record = BlobMetadata {
            blob_id: blob_id.clone(),
            namespace: namespace.to_string(),
            size: bytes.len() as u64,
            sha256: sha256(bytes),
            content_type: content_type.map(str::to_string),
            metadata,
            scan_status: if require_scan {
                "pending".to_string()
            } else {
                "not_required".to_string()
            },
            last_modified: Some(chrono::Utc::now().to_rfc3339()),
        };
        atomic_write(&blob_path, bytes)?;
        atomic_write(&metadata_path, &serde_json::to_vec(&record)?)?;
        Ok(record)
    }

    fn named_signature_payload(
        namespace: &str,
        key: &str,
        expires: i64,
        method: &str,
        download_name: Option<&str>,
    ) -> String {
        format!(
            "{method}\n{namespace}\n{key}\n{expires}\n{}",
            download_name.unwrap_or_default()
        )
    }
}

impl BlobProvider for LocalBlobProvider {
    fn put(
        &self,
        namespace: &str,
        bytes: &[u8],
        content_type: Option<&str>,
        metadata: &BTreeMap<String, String>,
        require_scan: bool,
    ) -> Result<BlobMetadata> {
        self.put_with_id(
            namespace,
            sha256(bytes),
            bytes,
            content_type,
            metadata.clone(),
            require_scan,
        )
    }

    fn get(&self, namespace: &str, blob_id: &str) -> Result<Option<BlobRecord>> {
        let (blob_path, metadata_path) = self.paths(namespace, blob_id)?;
        if !blob_path.exists() || !metadata_path.exists() {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        File::open(blob_path)?.read_to_end(&mut bytes)?;
        let metadata: BlobMetadata = serde_json::from_reader(File::open(metadata_path)?)?;
        if sha256(&bytes) != metadata.sha256 || bytes.len() as u64 != metadata.size {
            return Err(AppRuntimeError::Provider(format!(
                "blob `{namespace}/{blob_id}` failed hash/size verification"
            )));
        }
        Ok(Some(BlobRecord { metadata, bytes }))
    }

    fn delete(&self, namespace: &str, blob_id: &str) -> Result<bool> {
        let (blob_path, metadata_path) = self.paths(namespace, blob_id)?;
        let mut removed = false;
        for path in [blob_path, metadata_path] {
            match fs::remove_file(path) {
                Ok(()) => removed = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(removed)
    }

    fn signed_url(&self, namespace: &str, blob_id: &str, expires_seconds: u32) -> Result<String> {
        if expires_seconds == 0 || expires_seconds > 86_400 {
            return Err(AppRuntimeError::Provider(
                "signed blob URL expiry must be 1..=86400 seconds".to_string(),
            ));
        }
        let expiry = crate::host::now_ms()
            .saturating_div(1000)
            .saturating_add(expires_seconds as i64);
        let payload = format!("{namespace}/{blob_id}:{expiry}");
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.signing_key)
            .map_err(|error| AppRuntimeError::Provider(error.to_string()))?;
        mac.update(payload.as_bytes());
        let signature =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        Ok(format!(
            "bicdb-blob://{namespace}/{blob_id}?expires={expiry}&signature={signature}"
        ))
    }

    fn put_named(
        &self,
        namespace: &str,
        key: &str,
        bytes: &[u8],
        content_type: Option<&str>,
        metadata: &BTreeMap<String, String>,
        require_scan: bool,
    ) -> Result<BlobMetadata> {
        let mut metadata = metadata.clone();
        metadata.insert("bicdb.logical_key".to_string(), key.to_string());
        self.put_with_id(
            namespace,
            Self::named_blob_id(key)?,
            bytes,
            content_type,
            metadata,
            require_scan,
        )
    }

    fn get_named(&self, namespace: &str, key: &str) -> Result<Option<BlobRecord>> {
        let Some(record) = self.get(namespace, &Self::named_blob_id(key)?)? else {
            return Ok(None);
        };
        if record
            .metadata
            .metadata
            .get("bicdb.logical_key")
            .or_else(|| record.metadata.metadata.get("carrier_key"))
            .map(String::as_str)
            != Some(key)
        {
            return Err(AppRuntimeError::Provider(
                "named blob metadata does not match its logical key".to_string(),
            ));
        }
        Ok(Some(record))
    }

    fn delete_named(&self, namespace: &str, key: &str) -> Result<bool> {
        self.delete(namespace, &Self::named_blob_id(key)?)
    }

    fn signed_url_named(
        &self,
        namespace: &str,
        key: &str,
        expires_seconds: u32,
        method: &str,
        download_name: Option<&str>,
    ) -> Result<String> {
        validate_component("blob namespace", namespace)?;
        validate_blob_key(key)?;
        if let Some(download_name) = download_name {
            validate_blob_download_name(download_name)?;
        }
        let method = method.to_ascii_uppercase();
        if !matches!(method.as_str(), "GET" | "PUT")
            || expires_seconds == 0
            || expires_seconds > 86_400
        {
            return Err(AppRuntimeError::Provider(
                "signed named blob URL requires GET/PUT and 1..=86400 seconds".to_string(),
            ));
        }
        let expiry = crate::host::now_ms()
            .saturating_div(1000)
            .saturating_add(expires_seconds as i64);
        let payload = Self::named_signature_payload(namespace, key, expiry, &method, download_name);
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.signing_key)
            .map_err(|error| AppRuntimeError::Provider(error.to_string()))?;
        mac.update(payload.as_bytes());
        let signature =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("namespace", namespace)
            .append_pair("key", key)
            .append_pair("expires", &expiry.to_string())
            .append_pair("method", &method)
            .append_pair("download_name", download_name.unwrap_or_default())
            .append_pair("signature", &signature)
            .finish();
        Ok(format!("/_bicdb/blob?{query}"))
    }

    fn authorize_named_url(
        &self,
        namespace: &str,
        key: &str,
        expires: i64,
        method: &str,
        download_name: Option<&str>,
        signature: &str,
    ) -> Result<()> {
        validate_component("blob namespace", namespace)?;
        validate_blob_key(key)?;
        if let Some(download_name) = download_name {
            validate_blob_download_name(download_name)?;
        }
        if !matches!(method, "GET" | "PUT") {
            return Err(AppRuntimeError::Authentication(
                "signed blob URL has an invalid method".to_string(),
            ));
        }
        if expires < crate::host::now_ms().saturating_div(1000) {
            return Err(AppRuntimeError::Authentication(
                "signed blob URL has expired".to_string(),
            ));
        }
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| AppRuntimeError::Authentication("invalid blob signature".to_string()))?;
        let payload = Self::named_signature_payload(namespace, key, expires, method, download_name);
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.signing_key)
            .map_err(|error| AppRuntimeError::Provider(error.to_string()))?;
        mac.update(payload.as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| AppRuntimeError::Authentication("invalid blob signature".to_string()))
    }
}

pub trait EgressProvider: Send + Sync {
    fn available(&self, _application: &str, _policy: &EgressDeclaration) -> Result<()> {
        Ok(())
    }

    fn execute(
        &self,
        plugin: &str,
        policy: &EgressDeclaration,
        mtls_secret: Option<&SecretRecord>,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: &[u8],
        timeout_ms: u64,
    ) -> Result<EgressResponse>;
}

/// Fail-closed network capability for runtimes whose signed egress policy does
/// not authorize any destination.
#[derive(Clone, Debug, Default)]
pub struct DenyEgressProvider;

impl EgressProvider for DenyEgressProvider {
    fn available(&self, _application: &str, _policy: &EgressDeclaration) -> Result<()> {
        provider_error("network egress is denied by the runtime")
    }

    fn execute(
        &self,
        _plugin: &str,
        _policy: &EgressDeclaration,
        _mtls_secret: Option<&SecretRecord>,
        _method: &str,
        _url: &str,
        _headers: &[(String, String)],
        _body: &[u8],
        _timeout_ms: u64,
    ) -> Result<EgressResponse> {
        provider_error("network egress is denied by the runtime")
    }
}

#[derive(Clone)]
pub struct HttpEgressProviderConfig {
    pub application: String,
    pub provider: String,
    pub base_url: Url,
    pub headers: BTreeMap<String, String>,
    pub allow_insecure_http: bool,
    pub allow_private_networks: bool,
    pub healthcheck_path: String,
    pub healthcheck_statuses: BTreeSet<u16>,
}

impl std::fmt::Debug for HttpEgressProviderConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpEgressProviderConfig")
            .field("application", &self.application)
            .field("provider", &self.provider)
            .field("base_url_scheme", &self.base_url.scheme())
            .field("base_url_host", &self.base_url.host_str())
            .field("base_url_port", &self.base_url.port())
            .field("header_names", &self.headers.keys().collect::<Vec<_>>())
            .field("allow_insecure_http", &self.allow_insecure_http)
            .field("allow_private_networks", &self.allow_private_networks)
            .field("healthcheck_path", &self.healthcheck_path)
            .field("healthcheck_statuses", &self.healthcheck_statuses)
            .finish_non_exhaustive()
    }
}

impl HttpEgressProviderConfig {
    pub fn validate(&self) -> Result<()> {
        validate_provider_identifier("HTTP provider application", &self.application)?;
        validate_provider_identifier("HTTP provider name", &self.provider)?;
        if !matches!(self.base_url.scheme(), "http" | "https")
            || self.base_url.host_str().is_none()
            || !self.base_url.username().is_empty()
            || self.base_url.password().is_some()
            || self.base_url.query().is_some()
            || self.base_url.fragment().is_some()
        {
            return provider_error(
                "HTTP provider base URL must be an HTTP(S) origin/path without credentials, query, or fragment",
            );
        }
        if self.base_url.scheme() == "http" && !self.allow_insecure_http {
            return provider_error(
                "HTTP provider cleartext endpoint requires explicit operator policy",
            );
        }
        if self.healthcheck_path.is_empty()
            || !self.healthcheck_path.starts_with('/')
            || self.healthcheck_path.starts_with("//")
            || self.healthcheck_path.contains('\\')
            || self.healthcheck_path.chars().any(char::is_control)
            || self.healthcheck_path.len() > 8 * 1024
            || self.healthcheck_statuses.is_empty()
            || self
                .healthcheck_statuses
                .iter()
                .any(|status| !(100..=599).contains(status))
        {
            return provider_error("HTTP provider healthcheck path/status policy is invalid");
        }
        for (name, value) in &self.headers {
            if !valid_http_header(name, value)
                || ["host", "content-length", "connection"]
                    .iter()
                    .any(|reserved| name.eq_ignore_ascii_case(reserved))
            {
                return provider_error(
                    "HTTP provider contains an invalid or host-controlled header",
                );
            }
        }
        Ok(())
    }
}

pub trait RedisProvider: Send + Sync {
    fn available(&self, application: &str, provider: &str) -> Result<()>;
    fn publish(
        &self,
        application: &str,
        provider: &str,
        channel: &str,
        message: &str,
    ) -> Result<i64>;
    fn incr(
        &self,
        application: &str,
        provider: &str,
        tenant: Option<&str>,
        key: &str,
    ) -> Result<i64>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmailMessage {
    pub from: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub reply_to: Option<String>,
    pub subject: String,
    pub text: Option<String>,
    pub html: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmailDelivery {
    pub accepted: bool,
    pub delivery_id: String,
    pub status: String,
    pub transport: String,
}

pub trait EmailProvider: Send + Sync {
    fn available(&self, application: &str, provider: &str) -> Result<()>;
    fn send(
        &self,
        application: &str,
        provider: &str,
        message: EmailMessage,
    ) -> Result<EmailDelivery>;
}

pub trait GrpcProvider: Send + Sync {
    fn available(&self, application: &str, provider: &str) -> Result<()>;
    fn unary(
        &self,
        application: &str,
        provider: &str,
        method: &str,
        contract: &ApplicationGrpcClientV1,
        payload: Value,
        deadline_ms: u64,
        trace_id: &str,
    ) -> Result<Value>;
}

pub trait TokenizerProvider: Send + Sync {
    fn available(&self, application: &str, provider: &str) -> Result<()>;
    fn count(&self, application: &str, provider: &str, text: &str) -> Result<u64>;
}

#[derive(Clone, Debug, Default)]
pub struct DenyTokenizerProvider;

impl TokenizerProvider for DenyTokenizerProvider {
    fn available(&self, _application: &str, provider: &str) -> Result<()> {
        provider_error(format!("tokenizer provider `{provider}` is unavailable"))
    }

    fn count(&self, _application: &str, provider: &str, _text: &str) -> Result<u64> {
        self.available("", provider)?;
        unreachable!()
    }
}

pub trait EmbeddingsProvider: Send + Sync {
    fn available(&self, application: &str, provider: &str) -> Result<()>;
    fn embed(
        &self,
        application: &str,
        provider: &str,
        text: &str,
        dimensions: u32,
    ) -> Result<Vec<f32>>;
}

#[derive(Clone, Debug, Default)]
pub struct DenyEmbeddingsProvider;

impl EmbeddingsProvider for DenyEmbeddingsProvider {
    fn available(&self, _application: &str, provider: &str) -> Result<()> {
        provider_error(format!("embeddings provider `{provider}` is unavailable"))
    }

    fn embed(
        &self,
        _application: &str,
        provider: &str,
        _text: &str,
        _dimensions: u32,
    ) -> Result<Vec<f32>> {
        self.available("", provider)?;
        unreachable!()
    }
}

#[derive(Clone, Debug)]
pub struct LlmProviderRequest {
    pub messages: Vec<Value>,
    pub output_type: Option<String>,
    pub output_schema: Option<ApplicationRouteParameterTypeV1>,
    pub tools: BTreeMap<String, ApplicationLlmToolV1>,
    pub deadline_ms: u64,
    pub trace_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LlmProviderToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Clone, Debug)]
pub struct LlmProviderResponse {
    pub text: String,
    pub provider: String,
    pub model: String,
    pub structured_output: Option<Value>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub tool_calls: Vec<LlmProviderToolCall>,
}

pub trait LlmProvider: Send + Sync {
    fn available(&self, application: &str, provider: &str) -> Result<()>;
    fn complete(
        &self,
        application: &str,
        provider: &str,
        contract: &ApplicationLlmClientV1,
        request: LlmProviderRequest,
    ) -> Result<LlmProviderResponse>;
    fn stream(
        &self,
        _application: &str,
        provider: &str,
        _contract: &ApplicationLlmClientV1,
        _request: LlmProviderRequest,
        _emit: &mut dyn FnMut(&[u8]) -> Result<()>,
    ) -> Result<LlmProviderResponse> {
        provider_error(format!(
            "LLM provider `{provider}` does not support live streaming"
        ))
    }
    fn estimate_microusd(
        &self,
        application: &str,
        provider: &str,
        input_tokens: u64,
        output_tokens: u64,
    ) -> Result<u64>;
}

pub trait EvaluationProvider: Send + Sync {
    fn available(&self, application: &str, provider: &str) -> Result<()>;
    fn cases(&self, application: &str, provider: &str, max_cases: u32) -> Result<Vec<Value>>;
}

#[derive(Clone, Debug, Default)]
pub struct DenyEvaluationProvider;

impl EvaluationProvider for DenyEvaluationProvider {
    fn available(&self, _application: &str, provider: &str) -> Result<()> {
        provider_error(format!("evaluation provider `{provider}` is unavailable"))
    }

    fn cases(&self, application: &str, provider: &str, _max_cases: u32) -> Result<Vec<Value>> {
        self.available(application, provider)?;
        unreachable!()
    }
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryEvaluationProvider {
    datasets: Arc<Mutex<BTreeMap<(String, String), Vec<Value>>>>,
}

impl InMemoryEvaluationProvider {
    pub fn insert(
        &self,
        application: impl Into<String>,
        provider: impl Into<String>,
        cases: Vec<Value>,
    ) {
        self.datasets
            .lock()
            .expect("evaluation dataset mutex poisoned")
            .insert((application.into(), provider.into()), cases);
    }
}

impl EvaluationProvider for InMemoryEvaluationProvider {
    fn available(&self, application: &str, provider: &str) -> Result<()> {
        if self
            .datasets
            .lock()
            .expect("evaluation dataset mutex poisoned")
            .contains_key(&(application.to_string(), provider.to_string()))
        {
            Ok(())
        } else {
            provider_error(format!(
                "evaluation provider `{provider}` is unavailable for application `{application}`"
            ))
        }
    }

    fn cases(&self, application: &str, provider: &str, max_cases: u32) -> Result<Vec<Value>> {
        let cases = self
            .datasets
            .lock()
            .expect("evaluation dataset mutex poisoned")
            .get(&(application.to_string(), provider.to_string()))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::Provider(format!(
                    "evaluation provider `{provider}` is unavailable for application `{application}`"
                ))
            })?;
        if cases.is_empty() || cases.len() > max_cases as usize {
            return provider_error("evaluation dataset is empty or exceeds signed case bounds");
        }
        Ok(cases)
    }
}

#[derive(Clone, Debug, Default)]
pub struct DenyLlmProvider;

impl LlmProvider for DenyLlmProvider {
    fn available(&self, _application: &str, provider: &str) -> Result<()> {
        provider_error(format!("LLM provider `{provider}` is unavailable"))
    }

    fn complete(
        &self,
        _application: &str,
        provider: &str,
        _contract: &ApplicationLlmClientV1,
        _request: LlmProviderRequest,
    ) -> Result<LlmProviderResponse> {
        self.available("", provider)?;
        unreachable!()
    }

    fn estimate_microusd(
        &self,
        _application: &str,
        provider: &str,
        _input_tokens: u64,
        _output_tokens: u64,
    ) -> Result<u64> {
        self.available("", provider)?;
        unreachable!()
    }
}

#[derive(Clone, Debug, Default)]
pub struct DenyGrpcProvider;

impl GrpcProvider for DenyGrpcProvider {
    fn available(&self, _application: &str, provider: &str) -> Result<()> {
        provider_error(format!("gRPC provider `{provider}` is unavailable"))
    }

    fn unary(
        &self,
        _application: &str,
        provider: &str,
        _method: &str,
        _contract: &ApplicationGrpcClientV1,
        _payload: Value,
        _deadline_ms: u64,
        _trace_id: &str,
    ) -> Result<Value> {
        self.available("", provider)?;
        unreachable!()
    }
}

#[derive(Clone, Debug, Default)]
pub struct DenyEmailProvider;

impl EmailProvider for DenyEmailProvider {
    fn available(&self, _application: &str, provider: &str) -> Result<()> {
        provider_error(format!("email provider `{provider}` is unavailable"))
    }

    fn send(
        &self,
        _application: &str,
        provider: &str,
        _message: EmailMessage,
    ) -> Result<EmailDelivery> {
        self.available("", provider)?;
        unreachable!()
    }
}

#[derive(Clone, Debug, Default)]
pub struct DenyRedisProvider;

impl RedisProvider for DenyRedisProvider {
    fn available(&self, _application: &str, provider: &str) -> Result<()> {
        provider_error(format!(
            "Redis-compatible provider `{provider}` is unavailable"
        ))
    }

    fn publish(
        &self,
        _application: &str,
        provider: &str,
        _channel: &str,
        _message: &str,
    ) -> Result<i64> {
        self.available("", provider)?;
        unreachable!()
    }

    fn incr(
        &self,
        _application: &str,
        provider: &str,
        _tenant: Option<&str>,
        _key: &str,
    ) -> Result<i64> {
        self.available("", provider)?;
        unreachable!()
    }
}

#[derive(Clone)]
pub struct ProductionEgressProvider {
    clients: Arc<Mutex<BTreeMap<String, reqwest::blocking::Client>>>,
    admission: Arc<Mutex<BTreeMap<String, EgressAdmission>>>,
    providers: Arc<BTreeMap<(String, String), HttpEgressProviderConfig>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct EgressAdmission {
    minute: u64,
    requests: u32,
    active: u32,
}

impl ProductionEgressProvider {
    pub fn new() -> Result<Self> {
        Self::with_http_providers(Vec::new())
    }

    pub fn with_http_providers(providers: Vec<HttpEgressProviderConfig>) -> Result<Self> {
        let mut registry = BTreeMap::new();
        for provider in providers {
            provider.validate()?;
            let key = (provider.application.clone(), provider.provider.clone());
            if registry.insert(key, provider).is_some() {
                return provider_error("duplicate application HTTP provider binding");
            }
        }
        Ok(Self {
            clients: Arc::new(Mutex::new(BTreeMap::new())),
            admission: Arc::new(Mutex::new(BTreeMap::new())),
            providers: Arc::new(registry),
        })
    }

    pub fn healthcheck_providers(&self) -> Result<()> {
        for provider in self.providers.values() {
            let (url, host, port) = resolve_provider_url(provider, &provider.healthcheck_path)?;
            let policy = EgressDeclaration {
                name: provider.provider.clone(),
                provider: None,
                required_provider_headers: BTreeSet::new(),
                schemes: BTreeSet::from([url.scheme().to_string()]),
                hosts: BTreeSet::from([host]),
                ports: BTreeSet::from([port]),
                allow_redirects: false,
                allow_private_networks: provider.allow_private_networks,
                mtls_secret: None,
                max_request_bytes: 1,
                max_response_bytes: 64 * 1024,
                timeout_ms: 5_000,
                max_concurrency: 1,
                requests_per_minute: 60,
            };
            let headers = provider
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect::<Vec<_>>();
            let response = self.execute(
                &provider.application,
                &policy,
                None,
                "GET",
                url.as_str(),
                &headers,
                &[],
                5_000,
            )?;
            if !provider.healthcheck_statuses.contains(&response.status) {
                return provider_error(format!(
                    "HTTP provider healthcheck returned HTTP {}",
                    response.status
                ));
            }
        }
        Ok(())
    }
}

impl EgressProvider for ProductionEgressProvider {
    fn available(&self, application: &str, policy: &EgressDeclaration) -> Result<()> {
        let Some(provider) = policy.provider.as_deref() else {
            return Ok(());
        };
        let configured = self
            .providers
            .get(&(application.to_string(), provider.to_string()))
            .ok_or_else(|| {
                AppRuntimeError::Provider(format!(
                    "HTTP provider `{provider}` is unavailable for application `{application}`"
                ))
            })?;
        let required = policy
            .required_provider_headers
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let supplied = configured
            .headers
            .keys()
            .map(|name| name.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        if required != supplied {
            return provider_error(format!(
                "HTTP provider `{provider}` header contract is unavailable"
            ));
        }
        if !policy.schemes.contains(configured.base_url.scheme()) {
            return provider_error(format!(
                "HTTP provider `{provider}` scheme is outside the signed contract"
            ));
        }
        Ok(())
    }

    fn execute(
        &self,
        plugin: &str,
        policy: &EgressDeclaration,
        mtls_secret: Option<&SecretRecord>,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: &[u8],
        timeout_ms: u64,
    ) -> Result<EgressResponse> {
        self.available(plugin, policy)?;
        let configured = policy.provider.as_deref().map(|provider| {
            self.providers
                .get(&(plugin.to_string(), provider.to_string()))
                .expect("provider availability was checked")
        });
        let (parsed, effective_policy, mut effective_headers) = match configured {
            Some(provider) => {
                let (url, host, port) = resolve_provider_url(provider, url)?;
                let mut effective = policy.clone();
                effective.provider = None;
                effective.required_provider_headers.clear();
                effective.schemes = BTreeSet::from([url.scheme().to_string()]);
                effective.hosts = BTreeSet::from([host]);
                effective.ports = BTreeSet::from([port]);
                effective.allow_private_networks = provider.allow_private_networks;
                (
                    url,
                    effective,
                    provider
                        .headers
                        .iter()
                        .map(|(name, value)| (name.clone(), value.clone()))
                        .collect::<Vec<_>>(),
                )
            }
            None => (
                Url::parse(url)
                    .map_err(|_| AppRuntimeError::Provider("invalid egress URL".to_string()))?,
                policy.clone(),
                Vec::new(),
            ),
        };
        validate_egress_destination(&effective_policy, &parsed).map_err(|error| {
            if configured.is_some() {
                AppRuntimeError::Provider("HTTP provider destination was rejected".to_string())
            } else {
                error
            }
        })?;
        if body.len() as u64 > effective_policy.max_request_bytes {
            return Err(AppRuntimeError::Provider(
                "egress request exceeds declared byte limit".to_string(),
            ));
        }
        let _permit = EgressPermit::acquire(
            Arc::clone(&self.admission),
            format!("{plugin}:{}", policy.name),
            effective_policy.max_concurrency,
            effective_policy.requests_per_minute,
        )?;
        let method = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|_| AppRuntimeError::Provider("invalid egress method".to_string()))?;
        let identity_key = mtls_secret
            .map(|secret| format!("{}:{}", secret.name, secret.version))
            .unwrap_or_default();
        for (name, _) in headers {
            if name.eq_ignore_ascii_case("host")
                || name.eq_ignore_ascii_case("content-length")
                || name.eq_ignore_ascii_case("connection")
            {
                return Err(AppRuntimeError::Provider(format!(
                    "egress header `{name}` is host-controlled"
                )));
            }
            if effective_headers
                .iter()
                .any(|(provider_name, _)| provider_name.eq_ignore_ascii_case(name))
            {
                return Err(AppRuntimeError::CapabilityDenied(
                    "request attempted to override an operator provider header".to_string(),
                ));
            }
        }
        effective_headers.extend_from_slice(headers);
        let started = std::time::Instant::now();
        let maximum_timeout = Duration::from_millis(timeout_ms.min(effective_policy.timeout_ms));
        let original_origin = (
            parsed.scheme().to_string(),
            parsed.host_str().unwrap().to_ascii_lowercase(),
            parsed.port_or_known_default().unwrap(),
        );
        let mut current = parsed;
        let mut response = None;
        for redirects in 0..=5 {
            let elapsed = started.elapsed();
            let remaining = maximum_timeout
                .checked_sub(elapsed)
                .ok_or_else(|| AppRuntimeError::Provider("egress deadline exceeded".to_string()))?;
            let addresses =
                validate_egress_destination(&effective_policy, &current).map_err(|error| {
                    if configured.is_some() {
                        AppRuntimeError::Provider(
                            "HTTP provider destination was rejected".to_string(),
                        )
                    } else {
                        error
                    }
                })?;
            let host = current.host_str().expect("validated URL host");
            let client_key = format!(
                "{host}:{}:{identity_key}:{}",
                current.port_or_known_default().unwrap_or_default(),
                addresses
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let client = {
                let mut clients = self.clients.lock().expect("egress clients poisoned");
                if let Some(client) = clients.get(&client_key) {
                    client.clone()
                } else {
                    let mut builder = reqwest::blocking::Client::builder()
                        .redirect(reqwest::redirect::Policy::none())
                        .user_agent("bicdb-app-runtime/2")
                        .resolve_to_addrs(host, &addresses);
                    if let Some(secret) = mtls_secret {
                        let identity =
                            reqwest::Identity::from_pem(&secret.material).map_err(|_| {
                                AppRuntimeError::Provider(
                                    "invalid egress mTLS identity".to_string(),
                                )
                            })?;
                        builder = builder.identity(identity);
                    }
                    let client = builder.build().map_err(|_| {
                        AppRuntimeError::Provider("egress client setup failed".to_string())
                    })?;
                    clients.insert(client_key, client.clone());
                    client
                }
            };
            let mut request = client
                .request(method.clone(), current.clone())
                .timeout(remaining);
            for (name, value) in &effective_headers {
                request = request.header(name, value);
            }
            let candidate = request
                .body(body.to_vec())
                .send()
                .map_err(|_| AppRuntimeError::Provider("egress transport failed".to_string()))?;
            if !candidate.status().is_redirection() {
                response = Some(candidate);
                break;
            }
            if !effective_policy.allow_redirects {
                return Err(AppRuntimeError::Provider(
                    "egress redirect denied by policy".to_string(),
                ));
            }
            if redirects == 5 {
                return Err(AppRuntimeError::Provider(
                    "egress redirect limit exceeded".to_string(),
                ));
            }
            let location = candidate
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| {
                    AppRuntimeError::Provider("egress redirect lacks Location".to_string())
                })?;
            let redirected = current
                .join(location)
                .map_err(|_| AppRuntimeError::Provider("invalid egress redirect".to_string()))?;
            validate_egress_destination(&effective_policy, &redirected).map_err(|error| {
                if configured.is_some() {
                    AppRuntimeError::Provider("HTTP provider redirect was rejected".to_string())
                } else {
                    error
                }
            })?;
            let redirected_origin = (
                redirected.scheme().to_string(),
                redirected.host_str().unwrap().to_ascii_lowercase(),
                redirected.port_or_known_default().unwrap(),
            );
            if redirected_origin != original_origin {
                return Err(AppRuntimeError::Provider(
                    "cross-origin egress redirects are forbidden".to_string(),
                ));
            }
            current = redirected;
        }
        let response = response.ok_or_else(|| {
            AppRuntimeError::Provider("egress request produced no response".to_string())
        })?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter(|(name, _)| {
                !matches!(
                    name.as_str(),
                    "set-cookie" | "www-authenticate" | "proxy-authenticate"
                )
            })
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.to_string(), value.to_string()))
            })
            .collect::<Vec<_>>();
        let bounded = effective_policy.max_response_bytes.saturating_add(1);
        let mut body =
            Vec::with_capacity(usize::try_from(bounded.min(1024 * 1024)).unwrap_or(1024 * 1024));
        response
            .take(bounded)
            .read_to_end(&mut body)
            .map_err(|_| AppRuntimeError::Provider("egress response read failed".to_string()))?;
        if body.len() as u64 > effective_policy.max_response_bytes {
            return Err(AppRuntimeError::Provider(
                "egress response exceeds declared byte limit".to_string(),
            ));
        }
        Ok(EgressResponse {
            status,
            headers,
            body,
        })
    }
}

fn resolve_provider_url(
    provider: &HttpEgressProviderConfig,
    relative: &str,
) -> Result<(Url, String, u16)> {
    if !relative.starts_with('/')
        || relative.starts_with("//")
        || relative.contains('\\')
        || relative.chars().any(char::is_control)
    {
        return provider_error("HTTP provider request must use a safe relative path");
    }
    let mut base = provider.base_url.clone();
    if !base.path().ends_with('/') {
        base.set_path(&format!("{}/", base.path()));
    }
    let joined = base.join(relative.trim_start_matches('/')).map_err(|_| {
        AppRuntimeError::Provider("HTTP provider request path is invalid".to_string())
    })?;
    if joined.scheme() != base.scheme()
        || joined.host_str() != base.host_str()
        || joined.port_or_known_default() != base.port_or_known_default()
        || !joined.path().starts_with(base.path())
        || joined.fragment().is_some()
    {
        return provider_error("HTTP provider request escaped its configured base path");
    }
    let host = joined
        .host_str()
        .expect("validated provider URL host")
        .to_ascii_lowercase();
    let port = joined.port_or_known_default().ok_or_else(|| {
        AppRuntimeError::Provider("HTTP provider URL has no known port".to_string())
    })?;
    Ok((joined, host, port))
}

fn validate_provider_identifier(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return provider_error(format!("{label} is invalid"));
    }
    Ok(())
}

fn valid_http_header(name: &str, value: &str) -> bool {
    reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_ok()
        && reqwest::header::HeaderValue::try_from(value).is_ok()
}

fn provider_error<T>(message: impl Into<String>) -> Result<T> {
    Err(AppRuntimeError::Provider(message.into()))
}

fn validate_egress_destination(
    policy: &EgressDeclaration,
    url: &Url,
) -> Result<Vec<std::net::SocketAddr>> {
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(AppRuntimeError::Provider(
            "egress URL credentials and fragments are forbidden".to_string(),
        ));
    }
    if !policy.schemes.contains(url.scheme()) {
        return Err(AppRuntimeError::Provider(format!(
            "egress scheme `{}` is undeclared",
            url.scheme()
        )));
    }
    let host = url
        .host_str()
        .ok_or_else(|| AppRuntimeError::Provider("egress URL lacks host".to_string()))?
        .to_ascii_lowercase();
    if !policy.hosts.iter().any(|allowed| {
        allowed.eq_ignore_ascii_case(&host)
            || allowed
                .strip_prefix("*.")
                .is_some_and(|suffix| host.ends_with(&format!(".{suffix}")) && host != suffix)
    }) {
        return Err(AppRuntimeError::Provider(format!(
            "egress host `{host}` is undeclared"
        )));
    }
    let port = url
        .port_or_known_default()
        .ok_or_else(|| AppRuntimeError::Provider("egress URL lacks a known port".to_string()))?;
    if !policy.ports.contains(&port) {
        return Err(AppRuntimeError::Provider(format!(
            "egress port `{port}` is undeclared"
        )));
    }
    let addresses = (host.as_str(), port)
        .to_socket_addrs()
        .map_err(|error| AppRuntimeError::Provider(format!("egress DNS failed: {error}")))?
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err(AppRuntimeError::Provider(
            "egress DNS returned no addresses".to_string(),
        ));
    }
    if !policy.allow_private_networks
        && addresses
            .iter()
            .any(|address| private_or_metadata(address.ip()))
    {
        return Err(AppRuntimeError::Provider(
            "egress destination resolved to a private, local, or metadata address".to_string(),
        ));
    }
    Ok(addresses)
}

struct EgressPermit {
    admission: Arc<Mutex<BTreeMap<String, EgressAdmission>>>,
    key: String,
}

impl EgressPermit {
    fn acquire(
        admission: Arc<Mutex<BTreeMap<String, EgressAdmission>>>,
        key: String,
        max_concurrency: u32,
        requests_per_minute: u32,
    ) -> Result<Self> {
        let minute = crate::host::now_ms().max(0) as u64 / 60_000;
        let mut all = admission.lock().expect("egress admission poisoned");
        if all.len() > 100_000 {
            all.retain(|_, state| state.minute >= minute.saturating_sub(1) || state.active > 0);
        }
        let state = all.entry(key.clone()).or_default();
        if state.minute != minute {
            state.minute = minute;
            state.requests = 0;
        }
        if state.active >= max_concurrency {
            return Err(AppRuntimeError::CapabilityDenied(
                "egress concurrency quota exceeded".to_string(),
            ));
        }
        if state.requests >= requests_per_minute {
            return Err(AppRuntimeError::CapabilityDenied(
                "egress rate quota exceeded".to_string(),
            ));
        }
        state.active += 1;
        state.requests += 1;
        drop(all);
        Ok(Self { admission, key })
    }
}

impl Drop for EgressPermit {
    fn drop(&mut self) {
        if let Some(state) = self
            .admission
            .lock()
            .expect("egress admission poisoned")
            .get_mut(&self.key)
        {
            state.active = state.active.saturating_sub(1);
        }
    }
}

/// Whether an address belongs to infrastructure an app must never reach.
///
/// This is the last line of the SSRF defence: the host resolves an
/// allowlisted name, classifies the RESOLVED address here, and pins the
/// connection to it. Two families of bypass existed.
///
/// **The IPv6 arm never canonicalized.** `::ffff:169.254.169.254` is the
/// cloud metadata service written as an IPv4-mapped IPv6 address, and it
/// passed every check here because none of the v6 predicates match a
/// mapped v4 address. Same for `::ffff:127.0.0.1` and `::ffff:10.0.0.1`.
/// A resolver returning AAAA records — or a NAT64 host, where
/// `64:ff9b::/96` is globally routed to an embedded IPv4 — turned an
/// allowlisted hostname into an internal fetch.
///
/// **The IPv4 arm missed shared/reserved space.** 100.64.0.0/10 (CGNAT)
/// carries cloud internal load balancers and Alibaba's metadata endpoint
/// at 100.100.100.200.
///
/// Everything is therefore canonicalized to v4 where a v4 address is
/// embedded, then classified once.
fn private_or_metadata(address: IpAddr) -> bool {
    match canonical_address(address) {
        IpAddr::V4(address) => private_or_metadata_v4(address),
        IpAddr::V6(address) => {
            address.is_loopback()
                || address.is_unspecified()
                || address.is_unique_local()
                || address.is_unicast_link_local()
                // Documentation and unassigned-but-not-global space.
                || (address.segments()[0] & 0xe000) != 0x2000
        }
    }
}

/// Reduce an address to the IPv4 address it actually reaches, if any.
///
/// Handles IPv4-mapped (`::ffff:a.b.c.d`), NAT64 (`64:ff9b::/96`) and 6to4
/// (`2002::/16`) forms. Each is a different way of writing "send this to
/// an IPv4 destination", and each was a way around the v4 checks.
fn canonical_address(address: IpAddr) -> IpAddr {
    let IpAddr::V6(v6) = address else {
        return address;
    };
    if let Some(v4) = v6.to_ipv4_mapped() {
        return IpAddr::V4(v4);
    }
    let segments = v6.segments();
    // NAT64 well-known prefix 64:ff9b::/96 — the low 32 bits are the
    // embedded IPv4 destination.
    if segments[0] == 0x0064 && segments[1] == 0xff9b && segments[2..6] == [0, 0, 0, 0] {
        return IpAddr::V4(std::net::Ipv4Addr::new(
            (segments[6] >> 8) as u8,
            segments[6] as u8,
            (segments[7] >> 8) as u8,
            segments[7] as u8,
        ));
    }
    // 6to4 2002::/16 — the next 32 bits are the embedded IPv4.
    if segments[0] == 0x2002 {
        return IpAddr::V4(std::net::Ipv4Addr::new(
            (segments[1] >> 8) as u8,
            segments[1] as u8,
            (segments[2] >> 8) as u8,
            segments[2] as u8,
        ));
    }
    address
}

fn private_or_metadata_v4(address: std::net::Ipv4Addr) -> bool {
    let octets = address.octets();
    address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_broadcast()
        || address.is_documentation()
        || address.is_unspecified()
        || octets == [169, 254, 169, 254]
        || octets[0] == 0
        // 100.64.0.0/10, carrier-grade NAT: cloud internal load balancers
        // and Alibaba's metadata service at 100.100.100.200.
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        // 192.0.0.0/24 IETF protocol assignments.
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        // 192.88.99.0/24 former 6to4 relay anycast.
        || (octets[0] == 192 && octets[1] == 88 && octets[2] == 99)
        // 198.18.0.0/15 benchmarking.
        || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
        // 240.0.0.0/4 reserved, includes 255.255.255.255.
        || octets[0] >= 240
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "event_kind", rename_all = "snake_case")]
pub enum ObservabilityEvent {
    Log {
        actor: ActorContext,
        level: LogLevel,
        message: String,
        fields: BTreeMap<String, Value>,
    },
    Trace {
        actor: ActorContext,
        name: String,
        fields: BTreeMap<String, Value>,
    },
    Metric {
        actor: ActorContext,
        name: String,
        kind: MetricKind,
        value: f64,
        labels: BTreeMap<String, String>,
    },
    Audit {
        actor: ActorContext,
        action: String,
        subject: String,
        fields: BTreeMap<String, Value>,
    },
    Evidence {
        actor: ActorContext,
        control: String,
        outcome: String,
        fields: BTreeMap<String, Value>,
    },
}

pub trait HostObservability: Send + Sync {
    fn record(&self, event: ObservabilityEvent);

    fn available(
        &self,
        _application: &str,
        contract: &ApplicationObservabilityContractV1,
    ) -> Result<()> {
        if contract.protocol == ApplicationTelemetryProtocolV1::Host {
            Ok(())
        } else {
            Err(AppRuntimeError::Provider(format!(
                "observability protocol {:?} has no operator exporter",
                contract.protocol
            )))
        }
    }

    fn snapshot(&self) -> Vec<ObservabilityEvent> {
        Vec::new()
    }

    fn dropped(&self) -> u64 {
        0
    }
}

impl HostObservability for Mutex<Vec<ObservabilityEvent>> {
    fn record(&self, event: ObservabilityEvent) {
        self.lock()
            .expect("observability sink poisoned")
            .push(event);
    }

    fn snapshot(&self) -> Vec<ObservabilityEvent> {
        self.lock().expect("observability sink poisoned").clone()
    }

    fn available(
        &self,
        _application: &str,
        _contract: &ApplicationObservabilityContractV1,
    ) -> Result<()> {
        // The in-memory sink is an explicit deterministic test provider.
        Ok(())
    }
}

/// Fan-out across operator sinks while retaining one canonical bounded
/// snapshot for diagnostics. This permits a durable JSONL spool and an OTLP
/// exporter to receive the same already-redacted event without duplicate
/// application instrumentation.
pub struct CompositeObservability {
    sinks: Vec<Arc<dyn HostObservability>>,
}

impl CompositeObservability {
    pub fn new(sinks: Vec<Arc<dyn HostObservability>>) -> Result<Self> {
        if sinks.is_empty() {
            return Err(AppRuntimeError::Provider(
                "composite observability requires at least one sink".to_string(),
            ));
        }
        Ok(Self { sinks })
    }
}

impl HostObservability for CompositeObservability {
    fn record(&self, event: ObservabilityEvent) {
        for sink in &self.sinks {
            sink.record(event.clone());
        }
    }

    fn available(
        &self,
        application: &str,
        contract: &ApplicationObservabilityContractV1,
    ) -> Result<()> {
        if self
            .sinks
            .iter()
            .any(|sink| sink.available(application, contract).is_ok())
        {
            Ok(())
        } else {
            Err(AppRuntimeError::Provider(format!(
                "application `{application}` has no exporter for observability protocol {:?}",
                contract.protocol
            )))
        }
    }

    fn snapshot(&self) -> Vec<ObservabilityEvent> {
        self.sinks
            .first()
            .map(|sink| sink.snapshot())
            .unwrap_or_default()
    }

    fn dropped(&self) -> u64 {
        self.sinks
            .iter()
            .map(|sink| sink.dropped())
            .max()
            .unwrap_or_default()
    }
}

/// Process-wide bounded sink suitable for the first-party host. Required
/// durable audits are written through BicDB separately; this ring is for
/// operational logs, traces, metrics, and evidence telemetry.
pub struct BoundedObservability {
    events: Mutex<VecDeque<ObservabilityEvent>>,
    capacity: usize,
    dropped: AtomicU64,
}

impl BoundedObservability {
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(AppRuntimeError::Provider(
                "observability ring capacity must be positive".to_string(),
            ));
        }
        Ok(Self {
            events: Mutex::new(VecDeque::with_capacity(capacity.min(65_536))),
            capacity,
            dropped: AtomicU64::new(0),
        })
    }

    pub fn snapshot(&self) -> Vec<ObservabilityEvent> {
        self.events
            .lock()
            .expect("observability ring poisoned")
            .iter()
            .cloned()
            .collect()
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl HostObservability for BoundedObservability {
    fn record(&self, event: ObservabilityEvent) {
        let mut events = self.events.lock().expect("observability ring poisoned");
        if events.len() == self.capacity {
            events.pop_front();
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        events.push_back(event);
    }

    fn snapshot(&self) -> Vec<ObservabilityEvent> {
        BoundedObservability::snapshot(self)
    }

    fn dropped(&self) -> u64 {
        BoundedObservability::dropped(self)
    }
}

/// Operator-owned append-only JSONL sink with an in-process bounded snapshot.
/// Applications never receive the path or a file handle. A write failure is
/// counted as a dropped event and does not let telemetry failure alter
/// application transaction semantics.
pub struct JsonlObservability {
    ring: BoundedObservability,
    writer: Mutex<File>,
    write_failures: AtomicU64,
}

impl JsonlObservability {
    pub fn open(path: impl AsRef<Path>, capacity: usize) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let writer = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            ring: BoundedObservability::new(capacity)?,
            writer: Mutex::new(writer),
            write_failures: AtomicU64::new(0),
        })
    }

    pub fn write_failures(&self) -> u64 {
        self.write_failures.load(Ordering::Relaxed)
    }
}

impl HostObservability for JsonlObservability {
    fn record(&self, event: ObservabilityEvent) {
        let encoded = serde_json::to_vec(&serde_json::json!({
            "recorded_at_unix_ms": crate::host::now_ms(),
            "event": &event,
        }));
        let written = encoded.is_ok_and(|encoded| {
            let mut writer = self.writer.lock().expect("observability writer poisoned");
            writer.write_all(&encoded).is_ok()
                && writer.write_all(b"\n").is_ok()
                && writer.flush().is_ok()
        });
        if !written {
            self.write_failures.fetch_add(1, Ordering::Relaxed);
        }
        self.ring.record(event);
    }

    fn snapshot(&self) -> Vec<ObservabilityEvent> {
        self.ring.snapshot()
    }

    fn dropped(&self) -> u64 {
        self.ring
            .dropped()
            .saturating_add(self.write_failures.load(Ordering::Relaxed))
    }
}

#[derive(Clone)]
pub struct BufferedRealtimeProvider {
    streams: Arc<Mutex<BTreeMap<u64, BufferedStream>>>,
    pending_live:
        Arc<Mutex<BTreeMap<String, tokio::sync::oneshot::Sender<Result<LiveRealtimeResponse>>>>>,
    next_id: Arc<AtomicU64>,
    max_stream_bytes: usize,
    max_chunk_bytes: usize,
    live_channel_capacity: usize,
}

#[derive(Clone)]
struct BufferedStream {
    status: u16,
    headers: Vec<(String, String)>,
    kind: bicdb_extension::abi_v2::StreamKind,
    chunks: Vec<Vec<u8>>,
    bytes: usize,
    trailers: Vec<(String, String)>,
    closed: bool,
    live_sender: Option<tokio::sync::mpsc::Sender<Result<LiveRealtimeFrame>>>,
}

impl BufferedRealtimeProvider {
    pub fn new(max_stream_bytes: usize, max_chunk_bytes: usize) -> Result<Self> {
        if max_stream_bytes == 0 || max_chunk_bytes == 0 || max_chunk_bytes > max_stream_bytes {
            return Err(AppRuntimeError::Provider(
                "invalid bounded realtime limits".to_string(),
            ));
        }
        Ok(Self {
            streams: Arc::new(Mutex::new(BTreeMap::new())),
            pending_live: Arc::new(Mutex::new(BTreeMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
            max_stream_bytes,
            max_chunk_bytes,
            live_channel_capacity: 32,
        })
    }

    fn open_inner(
        &self,
        status: u16,
        headers: &[(String, String)],
        kind: bicdb_extension::abi_v2::StreamKind,
        scope: Option<&str>,
    ) -> Result<u64> {
        if !(100..=599).contains(&status) {
            return Err(AppRuntimeError::Provider(
                "invalid realtime HTTP status".to_string(),
            ));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == 0 {
            return Err(AppRuntimeError::Provider(
                "realtime stream identity exhausted".to_string(),
            ));
        }
        let capture = scope.and_then(|scope| {
            self.pending_live
                .lock()
                .expect("live realtime captures poisoned")
                .remove(scope)
        });
        let (live_sender, live_receiver) = if capture.is_some() {
            let (sender, receiver) = tokio::sync::mpsc::channel(self.live_channel_capacity);
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        self.streams
            .lock()
            .expect("realtime streams poisoned")
            .insert(
                id,
                BufferedStream {
                    status,
                    headers: headers.to_vec(),
                    kind,
                    chunks: Vec::new(),
                    bytes: 0,
                    trailers: Vec::new(),
                    closed: false,
                    live_sender,
                },
            );
        if let (Some(capture), Some(frames)) = (capture, live_receiver) {
            if capture
                .send(Ok(LiveRealtimeResponse {
                    status,
                    headers: headers.to_vec(),
                    kind,
                    frames,
                }))
                .is_err()
            {
                self.streams
                    .lock()
                    .expect("realtime streams poisoned")
                    .remove(&id);
                return Err(AppRuntimeError::Provider(
                    "live HTTP stream receiver disconnected".to_string(),
                ));
            }
        }
        Ok(id)
    }
}

impl RealtimeProvider for BufferedRealtimeProvider {
    fn supports(&self, kind: bicdb_extension::abi_v2::StreamKind) -> bool {
        matches!(
            kind,
            bicdb_extension::abi_v2::StreamKind::Bytes
                | bicdb_extension::abi_v2::StreamKind::ServerSentEvents
        )
    }

    fn open(
        &self,
        status: u16,
        headers: &[(String, String)],
        kind: bicdb_extension::abi_v2::StreamKind,
    ) -> Result<u64> {
        self.open_inner(status, headers, kind, None)
    }

    fn open_scoped(
        &self,
        status: u16,
        headers: &[(String, String)],
        kind: bicdb_extension::abi_v2::StreamKind,
        scope: &str,
    ) -> Result<u64> {
        self.open_inner(status, headers, kind, Some(scope))
    }

    fn send(&self, stream: u64, bytes: &[u8]) -> Result<()> {
        if bytes.len() > self.max_chunk_bytes {
            return Err(AppRuntimeError::CapabilityDenied(
                "realtime chunk exceeds host limit".to_string(),
            ));
        }
        let sender = {
            let mut streams = self.streams.lock().expect("realtime streams poisoned");
            let state = streams.get_mut(&stream).ok_or_else(|| {
                AppRuntimeError::CapabilityDenied("unknown realtime stream".to_string())
            })?;
            if state.closed {
                return Err(AppRuntimeError::CapabilityDenied(
                    "realtime stream is closed".to_string(),
                ));
            }
            if state.bytes.saturating_add(bytes.len()) > self.max_stream_bytes {
                return Err(AppRuntimeError::CapabilityDenied(
                    "realtime response exceeds host limit".to_string(),
                ));
            }
            state.bytes += bytes.len();
            if state.live_sender.is_none() {
                state.chunks.push(bytes.to_vec());
            }
            state.live_sender.clone()
        };
        if let Some(sender) = sender {
            sender
                .blocking_send(Ok(LiveRealtimeFrame::Data(bytes.to_vec())))
                .map_err(|_| {
                    AppRuntimeError::Provider("live HTTP stream disconnected".to_string())
                })?;
        }
        Ok(())
    }

    fn receive(&self, _stream: u64, _max_bytes: usize) -> Result<Vec<u8>> {
        Err(AppRuntimeError::CapabilityDenied(
            "buffered response streams do not accept inbound frames".to_string(),
        ))
    }

    fn close(&self, stream: u64, trailers: &[(String, String)]) -> Result<()> {
        let sender = {
            let mut streams = self.streams.lock().expect("realtime streams poisoned");
            let state = streams.get_mut(&stream).ok_or_else(|| {
                AppRuntimeError::CapabilityDenied("unknown realtime stream".to_string())
            })?;
            state.trailers = trailers.to_vec();
            state.closed = true;
            state.live_sender.take()
        };
        if let Some(sender) = sender {
            sender
                .blocking_send(Ok(LiveRealtimeFrame::Trailers(trailers.to_vec())))
                .map_err(|_| {
                    AppRuntimeError::Provider("live HTTP stream disconnected".to_string())
                })?;
            self.streams
                .lock()
                .expect("realtime streams poisoned")
                .remove(&stream);
        }
        Ok(())
    }

    fn register_live_response(
        &self,
        scope: &str,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<LiveRealtimeResponse>>> {
        if scope.is_empty() || scope.len() > 256 || scope.chars().any(char::is_control) {
            return Err(AppRuntimeError::InvalidRequest(
                "live HTTP stream scope is invalid".to_string(),
            ));
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut pending = self
            .pending_live
            .lock()
            .expect("live realtime captures poisoned");
        if pending.contains_key(scope) {
            return Err(AppRuntimeError::Provider(
                "live HTTP stream scope is already registered".to_string(),
            ));
        }
        pending.insert(scope.to_string(), sender);
        Ok(receiver)
    }

    fn cancel_live_response(&self, scope: &str, error: AppRuntimeError) {
        if let Some(sender) = self
            .pending_live
            .lock()
            .expect("live realtime captures poisoned")
            .remove(scope)
        {
            let _ = sender.send(Err(error));
        }
    }

    fn take_response(&self, stream: u64, max_bytes: usize) -> Result<Option<RealtimeResponse>> {
        let mut streams = self.streams.lock().expect("realtime streams poisoned");
        let Some(state) = streams.get(&stream) else {
            return Ok(None);
        };
        if !state.closed {
            return Err(AppRuntimeError::Provider(
                "application returned an open realtime stream".to_string(),
            ));
        }
        if state.bytes > max_bytes {
            return Err(AppRuntimeError::CapabilityDenied(
                "realtime response exceeds route limit".to_string(),
            ));
        }
        let state = streams.remove(&stream).expect("checked above");
        Ok(Some(RealtimeResponse {
            status: state.status,
            headers: state.headers,
            kind: state.kind,
            chunks: state.chunks,
            trailers: state.trailers,
        }))
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| AppRuntimeError::Provider("blob path lacks parent".to_string()))?;
    let temporary = parent.join(format!(".bicdb-blob-{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn validate_component(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(AppRuntimeError::Provider(format!(
            "invalid {label} `{value}`"
        )));
    }
    Ok(())
}

fn validate_blob_key(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 1_024
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains('\\')
        || value
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
        || value.chars().any(char::is_control)
    {
        return Err(AppRuntimeError::Provider(format!(
            "invalid logical blob key `{value}`"
        )));
    }
    Ok(())
}

fn validate_blob_download_name(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 255
        || value
            .chars()
            .any(|character| matches!(character, '/' | '\\' | '"') || character.is_control())
    {
        return Err(AppRuntimeError::Provider(
            "invalid signed blob download name".to_string(),
        ));
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod blob_tests {
    use super::*;

    fn http_provider() -> HttpEgressProviderConfig {
        HttpEgressProviderConfig {
            application: "clinic".to_string(),
            provider: "webhook".to_string(),
            base_url: Url::parse("https://api.example.test/v1").unwrap(),
            headers: BTreeMap::from([(
                "authorization".to_string(),
                "Bearer operator-secret".to_string(),
            )]),
            allow_insecure_http: false,
            allow_private_networks: false,
            healthcheck_path: "/health".to_string(),
            healthcheck_statuses: BTreeSet::from([200]),
        }
    }

    fn provider_policy() -> EgressDeclaration {
        EgressDeclaration {
            name: "Webhook".to_string(),
            provider: Some("webhook".to_string()),
            required_provider_headers: BTreeSet::from(["authorization".to_string()]),
            schemes: BTreeSet::from(["https".to_string()]),
            hosts: BTreeSet::new(),
            ports: BTreeSet::new(),
            allow_redirects: true,
            allow_private_networks: false,
            mtls_secret: None,
            max_request_bytes: 1024,
            max_response_bytes: 1024,
            timeout_ms: 1_000,
            max_concurrency: 2,
            requests_per_minute: 10,
        }
    }

    #[test]
    fn deny_providers_expose_no_ambient_blob_or_network_capability() {
        let blobs = DenyBlobProvider;
        assert!(blobs
            .put(
                "application",
                b"payload",
                Some("application/octet-stream"),
                &BTreeMap::new(),
                false,
            )
            .is_err());
        assert!(blobs.get("application", "blob").is_err());
        assert!(blobs.signed_url("application", "blob", 60).is_err());
        assert!(blobs.get_named("application", "objects/blob").is_err());

        let egress = DenyEgressProvider;
        let policy = provider_policy();
        assert!(egress.available("application", &policy).is_err());
        assert!(egress
            .execute(
                "application",
                &policy,
                None,
                "POST",
                "https://api.example.test/v1/events",
                &[],
                b"payload",
                1_000,
            )
            .is_err());
    }

    #[test]
    fn http_provider_paths_and_header_authority_are_exact() {
        let config = http_provider();
        let (url, host, port) = resolve_provider_url(&config, "/events?kind=created").unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.example.test/v1/events?kind=created"
        );
        assert_eq!(host, "api.example.test");
        assert_eq!(port, 443);
        for escape in ["../admin", "//evil.example/admin", "/events#fragment"] {
            assert!(resolve_provider_url(&config, escape).is_err(), "{escape}");
        }

        let provider = ProductionEgressProvider::with_http_providers(vec![config]).unwrap();
        let policy = provider_policy();
        provider.available("clinic", &policy).unwrap();
        assert!(provider.available("billing", &policy).is_err());
        let mut missing = policy.clone();
        missing.required_provider_headers.clear();
        assert!(provider.available("clinic", &missing).is_err());
        let mut scheme = policy;
        scheme.schemes = BTreeSet::from(["http".to_string()]);
        assert!(provider.available("clinic", &scheme).is_err());
    }

    #[test]
    fn http_provider_configuration_is_fail_closed_and_redacted() {
        let config = http_provider();
        config.validate().unwrap();
        let rendered = format!("{config:?}");
        assert!(rendered.contains("authorization"));
        assert!(!rendered.contains("operator-secret"));

        let mut cleartext = config.clone();
        cleartext.base_url = Url::parse("http://127.0.0.1:8080").unwrap();
        assert!(cleartext.validate().is_err());
        let mut credentials = config.clone();
        credentials.base_url = Url::parse("https://user:secret@example.test").unwrap();
        assert!(credentials.validate().is_err());
        assert!(!format!("{credentials:?}").contains("secret"));
        let mut reserved = config;
        reserved
            .headers
            .insert("host".to_string(), "forged.example".to_string());
        assert!(reserved.validate().is_err());
    }

    fn query(url: &str) -> BTreeMap<String, String> {
        let parsed = Url::parse(&format!("http://bicdb.invalid{url}")).unwrap();
        parsed.query_pairs().into_owned().collect()
    }

    #[test]
    fn named_blobs_are_durable_overwritable_and_path_safe() {
        let directory = tempfile::tempdir().unwrap();
        let provider = LocalBlobProvider::open(directory.path(), vec![7; 32]).unwrap();
        let first = provider
            .put_named(
                "app-carrier",
                "reports/annual.txt",
                b"first",
                Some("text/plain"),
                &BTreeMap::new(),
                false,
            )
            .unwrap();
        let second = provider
            .put_named(
                "app-carrier",
                "reports/annual.txt",
                b"second",
                Some("text/plain"),
                &BTreeMap::new(),
                false,
            )
            .unwrap();
        assert_eq!(first.blob_id, second.blob_id);
        assert_ne!(first.sha256, second.sha256);
        let stored = provider
            .get_named("app-carrier", "reports/annual.txt")
            .unwrap()
            .unwrap();
        assert_eq!(stored.bytes, b"second");
        assert_eq!(stored.metadata.size, 6);
        assert_eq!(
            stored
                .metadata
                .metadata
                .get("bicdb.logical_key")
                .map(String::as_str),
            Some("reports/annual.txt")
        );
        assert!(stored.metadata.last_modified.is_some());
        assert!(provider
            .put_named(
                "app-carrier",
                "../escape",
                b"denied",
                None,
                &BTreeMap::new(),
                false,
            )
            .is_err());
        assert!(provider
            .put_named(
                "app-carrier",
                "reports\\escape",
                b"denied",
                None,
                &BTreeMap::new(),
                false,
            )
            .is_err());
        assert!(provider
            .delete_named("app-carrier", "reports/annual.txt")
            .unwrap());
        assert!(provider
            .get_named("app-carrier", "reports/annual.txt")
            .unwrap()
            .is_none());
    }

    #[test]
    fn named_blob_urls_bind_method_key_expiry_and_download_name() {
        let directory = tempfile::tempdir().unwrap();
        let provider = LocalBlobProvider::open(directory.path(), vec![9; 32]).unwrap();
        let url = provider
            .signed_url_named(
                "app-carrier",
                "reports/annual.txt",
                300,
                "GET",
                Some("annual.txt"),
            )
            .unwrap();
        let values = query(&url);
        let expires = values["expires"].parse::<i64>().unwrap();
        provider
            .authorize_named_url(
                &values["namespace"],
                &values["key"],
                expires,
                &values["method"],
                Some(&values["download_name"]),
                &values["signature"],
            )
            .unwrap();
        assert!(provider
            .authorize_named_url(
                &values["namespace"],
                "reports/tampered.txt",
                expires,
                &values["method"],
                Some(&values["download_name"]),
                &values["signature"],
            )
            .is_err());
        assert!(provider
            .authorize_named_url(
                &values["namespace"],
                &values["key"],
                expires,
                "PUT",
                Some(&values["download_name"]),
                &values["signature"],
            )
            .is_err());
        assert!(provider
            .authorize_named_url(
                &values["namespace"],
                &values["key"],
                crate::host::now_ms().saturating_div(1000) - 1,
                &values["method"],
                Some(&values["download_name"]),
                &values["signature"],
            )
            .is_err());
        assert!(provider
            .signed_url_named(
                "app-carrier",
                "reports/annual.txt",
                300,
                "GET",
                Some("../annual.txt"),
            )
            .is_err());
    }

    #[test]
    fn jsonl_observability_is_append_only_and_keeps_a_bounded_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("observability/events.jsonl");
        let sink = JsonlObservability::open(&path, 2).unwrap();
        let actor = ActorContext {
            service_id: Some("test".to_string()),
            trace_id: "trace-1".to_string(),
            deadline_unix_ms: crate::host::now_ms() + 30_000,
            ..ActorContext::default()
        };
        for index in 0..3 {
            sink.record(ObservabilityEvent::Log {
                actor: actor.clone(),
                level: LogLevel::Info,
                message: format!("event-{index}"),
                fields: BTreeMap::from([("application".to_string(), Value::from("test"))]),
            });
        }
        assert_eq!(sink.snapshot().len(), 2);
        assert_eq!(sink.dropped(), 1);
        assert_eq!(sink.write_failures(), 0);
        assert_eq!(fs::read_to_string(path).unwrap().lines().count(), 3);
    }

    #[test]
    fn realtime_provider_exposes_live_frames_before_close() {
        let provider = BufferedRealtimeProvider::new(1024, 512).unwrap();
        let capture = provider.register_live_response("request-1").unwrap();
        let stream = provider
            .open_scoped(
                200,
                &[],
                bicdb_extension::abi_v2::StreamKind::ServerSentEvents,
                "request-1",
            )
            .unwrap();
        let mut live = capture.blocking_recv().unwrap().unwrap();
        provider.send(stream, b"data: first\n\n").unwrap();
        assert_eq!(
            live.frames.blocking_recv().unwrap().unwrap(),
            LiveRealtimeFrame::Data(b"data: first\n\n".to_vec())
        );
        provider.close(stream, &[]).unwrap();
        assert!(matches!(
            live.frames.blocking_recv().unwrap().unwrap(),
            LiveRealtimeFrame::Trailers(trailers) if trailers.is_empty()
        ));
    }
}

#[cfg(test)]
mod egress_classification_tests {
    use super::*;
    use std::net::IpAddr;

    fn blocked(value: &str) -> bool {
        private_or_metadata(value.parse::<IpAddr>().expect("address"))
    }

    /// Every one of these is a way of writing "reach internal
    /// infrastructure" that the classifier previously called public.
    #[test]
    fn alternate_encodings_of_internal_addresses_are_blocked() {
        // IPv4-mapped IPv6: the classic metadata bypass.
        assert!(blocked("::ffff:169.254.169.254"), "mapped metadata service");
        assert!(blocked("::ffff:127.0.0.1"), "mapped loopback");
        assert!(blocked("::ffff:10.0.0.1"), "mapped private");
        assert!(blocked("::ffff:192.168.1.1"), "mapped private");

        // NAT64: globally routed on NAT64 hosts, embeds the IPv4 target.
        assert!(blocked("64:ff9b::a9fe:a9fe"), "NAT64 metadata service");
        assert!(blocked("64:ff9b::7f00:1"), "NAT64 loopback");

        // 6to4 embeds IPv4 in the prefix.
        assert!(blocked("2002:a9fe:a9fe::1"), "6to4 metadata service");
        assert!(blocked("2002:7f00:1::1"), "6to4 loopback");

        // BicDB application-grade NAT: cloud internal LBs and Alibaba metadata.
        assert!(blocked("100.100.100.200"), "Alibaba metadata service");
        assert!(blocked("100.64.0.1"), "CGNAT lower bound");
        assert!(blocked("100.127.255.254"), "CGNAT upper bound");

        // Reserved and special-purpose ranges.
        assert!(blocked("192.0.0.1"), "IETF protocol assignments");
        assert!(blocked("192.88.99.1"), "6to4 relay anycast");
        assert!(blocked("198.18.0.1"), "benchmarking");
        assert!(blocked("198.19.255.255"), "benchmarking");
        assert!(blocked("240.0.0.1"), "reserved");
        assert!(blocked("255.255.255.255"), "broadcast");

        // The originally-covered cases must stay covered.
        assert!(blocked("169.254.169.254"));
        assert!(blocked("127.0.0.1"));
        assert!(blocked("10.1.2.3"));
        assert!(blocked("172.16.0.1"));
        assert!(blocked("::1"));
        assert!(blocked("fd00::1"), "unique local");
        assert!(blocked("fe80::1"), "link local");
    }

    /// Ordinary public destinations must still be reachable, or the fix
    /// would simply have broken egress.
    #[test]
    fn public_destinations_remain_reachable() {
        assert!(!blocked("93.184.216.34"), "example.com");
        assert!(!blocked("8.8.8.8"), "public resolver");
        assert!(!blocked("1.1.1.1"), "public resolver");
        assert!(
            !blocked("2606:2800:220:1:248:1893:25c8:1946"),
            "public IPv6"
        );
        assert!(!blocked("2001:4860:4860::8888"), "public IPv6 resolver");
        // 99.x and 101.x sit either side of the CGNAT block.
        assert!(!blocked("99.255.255.255"));
        assert!(!blocked("101.0.0.1"));
        assert!(!blocked("100.63.255.255"), "just below CGNAT");
        assert!(!blocked("100.128.0.1"), "just above CGNAT");
    }
}
