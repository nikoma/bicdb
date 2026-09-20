//! Trusted S3-compatible blob provider for the BicDB application host.
//!
//! This is a native host provider, not an application WASM module: operator
//! credentials and object-store network authority never enter guest memory.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use bicdb_app_runtime::{AppRuntimeError, BlobProvider, BlobRecord, Result};
use bicdb_extension::abi_v2::BlobMetadata;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::blocking::{Client, Response};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const MAX_ERROR_BYTES: u64 = 8 * 1024;
const MAX_METADATA_BYTES: u64 = 1024 * 1024;
const MAX_PROVIDER_OBJECT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(15 * 60);

#[derive(Clone, Debug)]
pub struct S3BlobProviderConfig {
    pub endpoint: Url,
    pub bucket: String,
    pub key_prefix: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub force_path_style: bool,
    pub allow_insecure_http: bool,
    pub max_object_bytes: u64,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub server_side_encryption: Option<String>,
    pub sse_kms_key_id: Option<String>,
}

impl S3BlobProviderConfig {
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.endpoint.scheme(), "https" | "http")
            || self.endpoint.host_str().is_none()
            || self.endpoint.username() != ""
            || self.endpoint.password().is_some()
            || self.endpoint.query().is_some()
            || self.endpoint.fragment().is_some()
            || !matches!(self.endpoint.path(), "" | "/")
        {
            return provider_error("S3 endpoint must be an HTTP(S) origin without credentials, path, query, or fragment");
        }
        if self.endpoint.scheme() == "http" && !self.allow_insecure_http {
            return provider_error(
                "S3 cleartext endpoint requires explicit allow_insecure_http operator policy",
            );
        }
        if !valid_bucket(&self.bucket) {
            return provider_error("S3 bucket is not a valid DNS-compatible name");
        }
        if !self.force_path_style
            && self
                .endpoint
                .host()
                .is_some_and(|host| matches!(host, url::Host::Ipv4(_) | url::Host::Ipv6(_)))
        {
            return provider_error("S3 IP endpoints require force_path_style");
        }
        if !valid_key_prefix(&self.key_prefix) {
            return provider_error("S3 key prefix must contain safe relative path segments");
        }
        if self.region.is_empty()
            || self.region.len() > 128
            || !self
                .region
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || self.access_key_id.is_empty()
            || self.access_key_id.len() > 256
            || !valid_header_value(&self.access_key_id)
            || self.secret_access_key.len() < 8
            || self.secret_access_key.len() > 4_096
            || self.max_object_bytes == 0
            || self.max_object_bytes > MAX_PROVIDER_OBJECT_BYTES
            || self.connect_timeout.is_zero()
            || self.connect_timeout > MAX_CONNECT_TIMEOUT
            || self.request_timeout.is_zero()
            || self.request_timeout > MAX_REQUEST_TIMEOUT
        {
            return provider_error(
                "S3 credentials, region, limits, and timeouts must be non-empty and bounded",
            );
        }
        if self.session_token.as_ref().is_some_and(|value| {
            value.is_empty() || value.len() > 16_384 || !valid_header_value(value)
        }) {
            return provider_error("S3 session token must not be empty");
        }
        match (
            self.server_side_encryption.as_deref(),
            self.sse_kms_key_id.as_deref(),
        ) {
            (None, None) | (Some("AES256"), None) | (Some("aws:kms"), Some(_)) => {}
            _ => {
                return provider_error(
                    "S3 encryption must be AES256 or aws:kms with a non-empty KMS key id",
                )
            }
        }
        if self.sse_kms_key_id.as_ref().is_some_and(|value| {
            value.is_empty() || value.len() > 2_048 || !valid_header_value(value)
        }) {
            return provider_error("S3 KMS key id must not be empty");
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct S3BlobProvider {
    config: Arc<S3BlobProviderConfig>,
    client: Client,
    url_signing_key: Arc<[u8]>,
}

impl std::fmt::Debug for S3BlobProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3BlobProvider")
            .field("endpoint", &self.config.endpoint)
            .field("bucket", &self.config.bucket)
            .field("key_prefix", &self.config.key_prefix)
            .field("region", &self.config.region)
            .field("force_path_style", &self.config.force_path_style)
            .field("max_object_bytes", &self.config.max_object_bytes)
            .finish_non_exhaustive()
    }
}

impl S3BlobProvider {
    pub fn new(config: S3BlobProviderConfig, url_signing_key: Vec<u8>) -> Result<Self> {
        config.validate()?;
        if url_signing_key.len() < 32 {
            return provider_error("blob URL signing key must contain at least 32 bytes");
        }
        let client = Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| AppRuntimeError::Provider(error.to_string()))?;
        Ok(Self {
            config: Arc::new(config),
            client,
            url_signing_key: Arc::from(url_signing_key),
        })
    }

    pub fn healthcheck(&self) -> Result<()> {
        let response = self.execute(Method::HEAD, "", &[], None)?;
        if response.status().is_success() {
            Ok(())
        } else {
            provider_error(format!(
                "S3 bucket healthcheck returned HTTP {}",
                response.status().as_u16()
            ))
        }
    }

    fn object_keys(namespace: &str, blob_id: &str) -> Result<(String, String)> {
        validate_component("blob namespace", namespace)?;
        validate_component("blob id", blob_id)?;
        let base = format!("{namespace}/{blob_id}");
        Ok((format!("{base}.blob"), format!("{base}.json")))
    }

    fn named_blob_id(key: &str) -> Result<String> {
        validate_blob_key(key)?;
        let mut digest = Sha256::new();
        digest.update(b"bicdb-named-blob-v1\0");
        digest.update(key.as_bytes());
        Ok(hex(&digest.finalize()))
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
        if bytes.len() as u64 > self.config.max_object_bytes {
            return Err(AppRuntimeError::ResourceExhausted(
                "S3 blob exceeds the operator provider limit".to_string(),
            ));
        }
        let (object_base, metadata_key) = Self::object_keys(namespace, &blob_id)?;
        let previous = self.get_stored_metadata(namespace, &blob_id)?;
        let object_key = format!(
            "{}.versions/{}.blob",
            object_base.trim_end_matches(".blob"),
            uuid::Uuid::new_v4().simple()
        );
        let record = BlobMetadata {
            blob_id,
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
            last_modified: Some(Utc::now().to_rfc3339()),
        };
        self.put_object(&object_key, bytes, content_type)?;
        let encoded = serde_json::to_vec(&S3StoredMetadata {
            metadata: record.clone(),
            object_key: object_key.clone(),
        })?;
        if let Err(error) = self.put_object(&metadata_key, &encoded, Some("application/json")) {
            let _ = self.delete_object(&object_key);
            return Err(error);
        }
        if let Some(previous) = previous {
            if previous.object_key != object_key {
                let _ = self.delete_object(&previous.object_key);
            }
        }
        Ok(record)
    }

    fn get_stored_metadata(
        &self,
        namespace: &str,
        blob_id: &str,
    ) -> Result<Option<S3StoredMetadata>> {
        let (_, metadata_key) = Self::object_keys(namespace, blob_id)?;
        let Some(encoded) = self.get_object(&metadata_key, MAX_METADATA_BYTES)? else {
            return Ok(None);
        };
        let stored: S3StoredMetadata = serde_json::from_slice(&encoded)
            .map_err(|_| AppRuntimeError::Provider("S3 blob metadata is invalid".to_string()))?;
        if stored.metadata.blob_id != blob_id
            || stored.metadata.namespace != namespace
            || !valid_generation_key(namespace, blob_id, &stored.object_key)
        {
            return provider_error("S3 blob metadata identity does not match its object key");
        }
        Ok(Some(stored))
    }

    fn get_record(&self, namespace: &str, blob_id: &str) -> Result<Option<BlobRecord>> {
        let Some(stored) = self.get_stored_metadata(namespace, blob_id)? else {
            return Ok(None);
        };
        let bytes = self
            .get_object(&stored.object_key, self.config.max_object_bytes)?
            .ok_or_else(|| AppRuntimeError::Provider("S3 blob bytes are missing".to_string()))?;
        if bytes.len() as u64 != stored.metadata.size || sha256(&bytes) != stored.metadata.sha256 {
            return provider_error("S3 blob failed hash/size verification");
        }
        Ok(Some(BlobRecord {
            metadata: stored.metadata,
            bytes,
        }))
    }

    fn object_url(&self, key: &str) -> Result<(Url, String)> {
        let mut url = self.config.endpoint.clone();
        let key = if key.is_empty() || self.config.key_prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{key}", self.config.key_prefix)
        };
        let host = url
            .host_str()
            .ok_or_else(|| AppRuntimeError::Provider("S3 endpoint has no host".to_string()))?;
        if self.config.force_path_style {
            url.set_path(&format!("/{}/{key}", self.config.bucket));
        } else {
            let bucket_host = format!("{}.{}", self.config.bucket, host);
            url.set_host(Some(&bucket_host))
                .map_err(|_| AppRuntimeError::Provider("invalid S3 virtual host".to_string()))?;
            url.set_path(&format!("/{key}"));
        }
        let canonical_host = url.host().expect("validated S3 host").to_string();
        let host = match url.port() {
            Some(port) => format!("{canonical_host}:{port}"),
            None => canonical_host,
        };
        Ok((url, host))
    }

    fn execute(
        &self,
        method: Method,
        key: &str,
        body: &[u8],
        content_type: Option<&str>,
    ) -> Result<Response> {
        let mut last_error = None;
        for attempt in 0..3 {
            match self.execute_once(method.clone(), key, body, content_type) {
                Ok(response)
                    if !matches!(
                        response.status(),
                        StatusCode::TOO_MANY_REQUESTS
                            | StatusCode::INTERNAL_SERVER_ERROR
                            | StatusCode::BAD_GATEWAY
                            | StatusCode::SERVICE_UNAVAILABLE
                            | StatusCode::GATEWAY_TIMEOUT
                    ) =>
                {
                    return Ok(response)
                }
                Ok(response) => {
                    last_error = Some(format!("HTTP {}", response.status().as_u16()));
                }
                Err(_) => last_error = Some("transport error".to_string()),
            }
            if attempt < 2 {
                std::thread::sleep(Duration::from_millis(50 * (1 << attempt)));
            }
        }
        provider_error(format!(
            "S3 request failed after bounded retries: {}",
            last_error.unwrap_or_else(|| "unknown transport error".to_string())
        ))
    }

    fn execute_once(
        &self,
        method: Method,
        key: &str,
        body: &[u8],
        content_type: Option<&str>,
    ) -> Result<Response> {
        let (url, host) = self.object_url(key)?;
        let payload_hash = if body.is_empty() {
            EMPTY_SHA256.to_string()
        } else {
            sha256(body)
        };
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let short_date = now.format("%Y%m%d").to_string();
        let mut signed_headers = BTreeMap::from([
            ("host".to_string(), host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ]);
        if let Some(token) = &self.config.session_token {
            signed_headers.insert("x-amz-security-token".to_string(), token.clone());
        }
        if method == Method::PUT {
            if let Some(encryption) = &self.config.server_side_encryption {
                signed_headers.insert(
                    "x-amz-server-side-encryption".to_string(),
                    encryption.clone(),
                );
            }
            if let Some(key_id) = &self.config.sse_kms_key_id {
                signed_headers.insert(
                    "x-amz-server-side-encryption-aws-kms-key-id".to_string(),
                    key_id.clone(),
                );
            }
        }
        let canonical_headers = signed_headers
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect::<String>();
        let signed_header_names = signed_headers.keys().cloned().collect::<Vec<_>>().join(";");
        let canonical_request = format!(
            "{}\n{}\n\n{}\n{}\n{}",
            method.as_str(),
            url.path(),
            canonical_headers,
            signed_header_names,
            payload_hash
        );
        let scope = format!("{short_date}/{}/s3/aws4_request", self.config.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256(canonical_request.as_bytes())
        );
        let signing_key = aws_signing_key(
            &self.config.secret_access_key,
            &short_date,
            &self.config.region,
        )?;
        let signature = hex(&hmac(&signing_key, string_to_sign.as_bytes())?);
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_header_names}, Signature={signature}",
            self.config.access_key_id
        );
        let mut request = self
            .client
            .request(method, url)
            .header("authorization", authorization);
        for (name, value) in signed_headers {
            request = request.header(name, value);
        }
        if let Some(content_type) = content_type {
            request = request.header("content-type", content_type);
        }
        if !body.is_empty() {
            request = request.body(body.to_vec());
        }
        request
            .send()
            .map_err(|_| AppRuntimeError::Provider("S3 transport failed".to_string()))
    }

    fn put_object(&self, key: &str, bytes: &[u8], content_type: Option<&str>) -> Result<()> {
        let response = self.execute(Method::PUT, key, bytes, content_type)?;
        if response.status().is_success() {
            Ok(())
        } else {
            self.status_error("PUT", response)
        }
    }

    fn get_object(&self, key: &str, limit: u64) -> Result<Option<Vec<u8>>> {
        let response = self.execute(Method::GET, key, &[], None)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return self.status_error("GET", response);
        }
        if response
            .content_length()
            .is_some_and(|length| length > limit)
        {
            return Err(AppRuntimeError::ResourceExhausted(
                "S3 object exceeds the operator provider limit".to_string(),
            ));
        }
        let mut bytes = Vec::new();
        response
            .take(limit.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > limit {
            return Err(AppRuntimeError::ResourceExhausted(
                "S3 object exceeds the operator provider limit".to_string(),
            ));
        }
        Ok(Some(bytes))
    }

    fn delete_object(&self, key: &str) -> Result<()> {
        let response = self.execute(Method::DELETE, key, &[], None)?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            self.status_error("DELETE", response)
        }
    }

    fn status_error<T>(&self, operation: &str, response: Response) -> Result<T> {
        let status = response.status();
        let mut discarded = Vec::new();
        let _ = response.take(MAX_ERROR_BYTES).read_to_end(&mut discarded);
        provider_error(format!("S3 {operation} returned HTTP {}", status.as_u16()))
    }

    fn signed_named_url(
        &self,
        namespace: &str,
        key: &str,
        expires_seconds: u32,
        method: &str,
        download_name: Option<&str>,
    ) -> Result<String> {
        validate_component("blob namespace", namespace)?;
        validate_blob_key(key)?;
        if let Some(name) = download_name {
            validate_download_name(name)?;
        }
        if !matches!(method, "GET" | "PUT") || expires_seconds == 0 || expires_seconds > 86_400 {
            return provider_error("signed S3-backed URL requires GET/PUT and 1..=86400 seconds");
        }
        let expiry = now_seconds().saturating_add(expires_seconds as i64);
        let payload = named_signature_payload(namespace, key, expiry, method, download_name);
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(hmac(&self.url_signing_key, payload.as_bytes())?);
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("namespace", namespace)
            .append_pair("key", key)
            .append_pair("expires", &expiry.to_string())
            .append_pair("method", method)
            .append_pair("download_name", download_name.unwrap_or_default())
            .append_pair("signature", &signature)
            .finish();
        Ok(format!("/_bicdb/blob?{query}"))
    }
}

impl BlobProvider for S3BlobProvider {
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
        self.get_record(namespace, blob_id)
    }

    fn delete(&self, namespace: &str, blob_id: &str) -> Result<bool> {
        let (_, metadata_key) = Self::object_keys(namespace, blob_id)?;
        let stored = self.get_stored_metadata(namespace, blob_id)?;
        if let Some(stored) = stored {
            self.delete_object(&stored.object_key)?;
            self.delete_object(&metadata_key)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn signed_url(&self, namespace: &str, blob_id: &str, expires_seconds: u32) -> Result<String> {
        validate_component("blob namespace", namespace)?;
        validate_component("blob id", blob_id)?;
        if expires_seconds == 0 || expires_seconds > 86_400 {
            return provider_error("signed blob URL expiry must be 1..=86400 seconds");
        }
        let expiry = now_seconds().saturating_add(expires_seconds as i64);
        let payload = format!("{namespace}/{blob_id}:{expiry}");
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(hmac(&self.url_signing_key, payload.as_bytes())?);
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
        let Some(record) = self.get_record(namespace, &Self::named_blob_id(key)?)? else {
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
            return provider_error("S3 named blob metadata does not match its logical key");
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
        self.signed_named_url(
            namespace,
            key,
            expires_seconds,
            &method.to_ascii_uppercase(),
            download_name,
        )
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
        if let Some(name) = download_name {
            validate_download_name(name)?;
        }
        if !matches!(method, "GET" | "PUT") {
            return Err(AppRuntimeError::Authentication(
                "signed blob URL has an invalid method".to_string(),
            ));
        }
        if expires < now_seconds() {
            return Err(AppRuntimeError::Authentication(
                "signed blob URL has expired".to_string(),
            ));
        }
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| AppRuntimeError::Authentication("invalid blob signature".to_string()))?;
        let payload = named_signature_payload(namespace, key, expires, method, download_name);
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.url_signing_key)
            .map_err(|error| AppRuntimeError::Provider(error.to_string()))?;
        mac.update(payload.as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| AppRuntimeError::Authentication("invalid blob signature".to_string()))
    }
}

fn aws_signing_key(secret: &str, date: &str, region: &str) -> Result<Vec<u8>> {
    let date_key = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes())?;
    let region_key = hmac(&date_key, region.as_bytes())?;
    let service_key = hmac(&region_key, b"s3")?;
    hmac(&service_key, b"aws4_request")
}

fn hmac(key: &[u8], value: &[u8]) -> Result<Vec<u8>> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|error| AppRuntimeError::Provider(error.to_string()))?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
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

fn validate_component(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return provider_error(format!("invalid {label} `{value}`"));
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
        return provider_error(format!("invalid logical blob key `{value}`"));
    }
    Ok(())
}

fn validate_download_name(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 255
        || value
            .chars()
            .any(|character| matches!(character, '/' | '\\' | '"') || character.is_control())
    {
        return provider_error("invalid signed blob download name");
    }
    Ok(())
}

fn valid_bucket(value: &str) -> bool {
    (3..=63).contains(&value.len())
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && !value.contains("..")
        && !value.contains(".-")
        && !value.contains("-.")
        && value.parse::<std::net::Ipv4Addr>().is_err()
}

fn valid_key_prefix(value: &str) -> bool {
    value.len() <= 512
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains('\\')
        && !value.chars().any(char::is_control)
        && (value.is_empty()
            || value.split('/').all(|segment| {
                !segment.is_empty()
                    && !matches!(segment, "." | "..")
                    && segment.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                    })
            }))
}

fn valid_generation_key(namespace: &str, blob_id: &str, value: &str) -> bool {
    let prefix = format!("{namespace}/{blob_id}.versions/");
    let Some(generation) = value.strip_prefix(&prefix) else {
        return false;
    };
    let Some(generation) = generation.strip_suffix(".blob") else {
        return false;
    };
    generation.len() == 32
        && generation
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn valid_header_value(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| matches!(byte, b'\t' | 0x20..=0x7e))
}

fn now_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn sha256(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn provider_error<T>(message: impl Into<String>) -> Result<T> {
    Err(AppRuntimeError::Provider(message.into()))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3BlobProviderDiagnostic {
    pub endpoint: String,
    pub bucket: String,
    pub key_prefix: String,
    pub region: String,
    pub force_path_style: bool,
    pub max_object_bytes: u64,
}

impl From<&S3BlobProviderConfig> for S3BlobProviderDiagnostic {
    fn from(config: &S3BlobProviderConfig) -> Self {
        Self {
            endpoint: config.endpoint.to_string(),
            bucket: config.bucket.clone(),
            key_prefix: config.key_prefix.clone(),
            region: config.region.clone(),
            force_path_style: config.force_path_style,
            max_object_bytes: config.max_object_bytes,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct S3StoredMetadata {
    metadata: BlobMetadata,
    object_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> S3BlobProviderConfig {
        S3BlobProviderConfig {
            endpoint: Url::parse("https://objects.example.test").unwrap(),
            bucket: "carrier-blobs".to_string(),
            key_prefix: "carrier/production".to_string(),
            region: "us-west-2".to_string(),
            access_key_id: "access-key".to_string(),
            secret_access_key: "secret-access-key".to_string(),
            session_token: None,
            force_path_style: true,
            allow_insecure_http: false,
            max_object_bytes: 64 * 1024 * 1024,
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(10),
            server_side_encryption: Some("AES256".to_string()),
            sse_kms_key_id: None,
        }
    }

    #[test]
    fn configuration_is_secure_and_diagnostics_redact_credentials() {
        let base = config();
        base.validate().unwrap();
        let diagnostic = serde_json::to_string(&S3BlobProviderDiagnostic::from(&base)).unwrap();
        assert!(!diagnostic.contains("access-key"));
        assert!(!diagnostic.contains("secret-access-key"));

        let mut cleartext = base.clone();
        cleartext.endpoint = Url::parse("http://objects.example.test").unwrap();
        assert!(cleartext.validate().is_err());
        cleartext.allow_insecure_http = true;
        cleartext.validate().unwrap();

        let mut invalid_encryption = base;
        invalid_encryption.server_side_encryption = Some("aws:kms".to_string());
        assert!(invalid_encryption.validate().is_err());

        let mut invalid_prefix = config();
        invalid_prefix.key_prefix = "../carrier".to_string();
        assert!(invalid_prefix.validate().is_err());
        let mut unbounded = config();
        unbounded.max_object_bytes = u64::MAX;
        assert!(unbounded.validate().is_err());
        let mut header_injection = config();
        header_injection.access_key_id = "access\r\ninjected".to_string();
        assert!(header_injection.validate().is_err());
    }

    #[test]
    fn signed_proxy_urls_bind_every_authority_dimension() {
        let provider = S3BlobProvider::new(config(), vec![5; 32]).unwrap();
        let url = provider
            .signed_url_named(
                "application-carrier",
                "reports/annual.pdf",
                300,
                "GET",
                Some("annual.pdf"),
            )
            .unwrap();
        let parsed = Url::parse(&format!("http://bicdb.invalid{url}")).unwrap();
        let query = parsed
            .query_pairs()
            .into_owned()
            .collect::<BTreeMap<_, _>>();
        provider
            .authorize_named_url(
                &query["namespace"],
                &query["key"],
                query["expires"].parse().unwrap(),
                &query["method"],
                Some(&query["download_name"]),
                &query["signature"],
            )
            .unwrap();
        assert!(provider
            .authorize_named_url(
                &query["namespace"],
                "reports/tampered.pdf",
                query["expires"].parse().unwrap(),
                &query["method"],
                Some(&query["download_name"]),
                &query["signature"],
            )
            .is_err());
    }

    #[test]
    fn transport_failures_do_not_expose_provider_configuration() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut config = config();
        config.endpoint =
            Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        config.force_path_style = true;
        config.allow_insecure_http = true;
        config.connect_timeout = Duration::from_millis(20);
        config.request_timeout = Duration::from_millis(20);
        let provider = S3BlobProvider::new(config, vec![8; 32]).unwrap();
        let failure = provider.healthcheck().unwrap_err().to_string();
        assert!(!failure.contains("127.0.0.1"));
        assert!(!failure.contains("access-key"));
        assert!(!failure.contains("secret-access-key"));
        assert!(failure.contains("bounded retries"));
    }

    #[test]
    fn object_keys_are_opaque_and_application_namespaced() {
        let id = S3BlobProvider::named_blob_id("reports/annual.pdf").unwrap();
        assert_eq!(id.len(), 64);
        assert!(!id.contains("annual"));
        let (object, metadata) = S3BlobProvider::object_keys("app-carrier", &id).unwrap();
        assert_eq!(object, format!("app-carrier/{id}.blob"));
        assert_eq!(metadata, format!("app-carrier/{id}.json"));
        assert!(S3BlobProvider::named_blob_id("../escape").is_err());

        let mut ipv6 = config();
        ipv6.endpoint = Url::parse("http://[::1]:9000").unwrap();
        ipv6.allow_insecure_http = true;
        let provider = S3BlobProvider::new(ipv6, vec![7; 32]).unwrap();
        let (url, host) = provider.object_url("namespace/blob.json").unwrap();
        assert_eq!(
            url.as_str(),
            "http://[::1]:9000/carrier-blobs/carrier/production/namespace/blob.json"
        );
        assert_eq!(host, "[::1]:9000");
    }

    #[test]
    #[ignore = "requires an operator-provided S3-compatible test endpoint"]
    fn live_s3_round_trip_covers_overwrite_hash_scan_and_delete() {
        let endpoint = std::env::var("BICDB_TEST_S3_ENDPOINT").unwrap();
        let bucket = std::env::var("BICDB_TEST_S3_BUCKET").unwrap();
        let access_key_id = std::env::var("BICDB_TEST_S3_ACCESS_KEY_ID").unwrap();
        let secret_access_key = std::env::var("BICDB_TEST_S3_SECRET_ACCESS_KEY").unwrap();
        let provider = S3BlobProvider::new(
            S3BlobProviderConfig {
                endpoint: Url::parse(&endpoint).unwrap(),
                bucket,
                key_prefix: std::env::var("BICDB_TEST_S3_KEY_PREFIX")
                    .unwrap_or_else(|_| "bicdb-live".to_string()),
                region: std::env::var("BICDB_TEST_S3_REGION")
                    .unwrap_or_else(|_| "us-east-1".to_string()),
                access_key_id,
                secret_access_key,
                session_token: None,
                force_path_style: true,
                allow_insecure_http: endpoint.starts_with("http://"),
                max_object_bytes: 8 * 1024 * 1024,
                connect_timeout: Duration::from_secs(2),
                request_timeout: Duration::from_secs(10),
                server_side_encryption: None,
                sse_kms_key_id: None,
            },
            vec![6; 32],
        )
        .unwrap();
        provider.healthcheck().unwrap();
        let namespace = format!("test-{}", uuid::Uuid::new_v4().simple());
        let key = "reports/live.txt";
        let first = provider
            .put_named(
                &namespace,
                key,
                b"first",
                Some("text/plain"),
                &BTreeMap::new(),
                true,
            )
            .unwrap();
        assert_eq!(first.scan_status, "pending");
        let second = provider
            .put_named(
                &namespace,
                key,
                b"second",
                Some("text/plain"),
                &BTreeMap::new(),
                false,
            )
            .unwrap();
        assert_eq!(first.blob_id, second.blob_id);
        assert_ne!(first.sha256, second.sha256);
        let stored = provider.get_named(&namespace, key).unwrap().unwrap();
        assert_eq!(stored.bytes, b"second");
        assert_eq!(stored.metadata.content_type.as_deref(), Some("text/plain"));
        assert!(provider.delete_named(&namespace, key).unwrap());
        assert!(provider.get_named(&namespace, key).unwrap().is_none());
        assert!(!provider.delete_named(&namespace, key).unwrap());
    }
}
