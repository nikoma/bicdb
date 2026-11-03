//! Operator lifecycle API mounted on the application HTTP listener.
//!
//! The API is disabled unless the host constructs a [`ManagementHttpHandler`]
//! with an operator bearer token. The token only authorizes lifecycle
//! operations: every installed or upgraded package is still verified against
//! the runtime's trusted signing keys, so the token never admits unsigned code.
use std::sync::Arc;

use bicdb_extension::abi_v2::{HttpRequestBodyV2, HttpResponseBodyV2};
use bicdb_extension::HttpMethod;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::{
    AppRuntimeError, ApplicationHttpRequest, ApplicationHttpResponse, ApplicationRuntime, Result,
    TrustedHttpHandler,
};

/// Path prefix reserved for the management API.
pub const MANAGEMENT_PATH_PREFIX: &str = "/_bicdb/apps";
const MIN_TOKEN_BYTES: usize = 32;

pub struct ManagementHttpHandler {
    runtime: Arc<ApplicationRuntime>,
    token_sha256: [u8; 32],
}

impl ManagementHttpHandler {
    /// `token` is the operator secret (surrounding whitespace is ignored). It
    /// must contain at least 32 bytes and is kept only as a SHA-256 digest.
    pub fn new(runtime: Arc<ApplicationRuntime>, token: &[u8]) -> Result<Self> {
        let token = token.trim_ascii();
        if token.len() < MIN_TOKEN_BYTES || token.iter().any(u8::is_ascii_whitespace) {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "management token must be at least {MIN_TOKEN_BYTES} bytes without whitespace"
            )));
        }
        Ok(Self {
            runtime,
            token_sha256: Sha256::digest(token).into(),
        })
    }

    fn authorize(&self, request: &ApplicationHttpRequest) -> Result<()> {
        let presented = request
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .and_then(|(_, value)| value.strip_prefix("Bearer "))
            .map(str::trim)
            .unwrap_or_default();
        let digest: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
        if presented.is_empty() || !bool::from(digest.ct_eq(&self.token_sha256)) {
            return Err(AppRuntimeError::Authentication(
                "invalid management token".to_string(),
            ));
        }
        Ok(())
    }

    fn package(&self, body: &HttpRequestBodyV2) -> Result<crate::ApplicationPackage> {
        let bytes = match body {
            HttpRequestBodyV2::Json(value) => serde_json::to_vec(value)?,
            HttpRequestBodyV2::Binary(bytes) => bytes.clone(),
            _ => {
                return Err(AppRuntimeError::InvalidRequest(
                    "management install and upgrade require a signed package JSON body".to_string(),
                ));
            }
        };
        self.runtime.read_package_bytes(&bytes).map_err(rejected)
    }

    fn list(&self) -> Value {
        Value::Array(
            self.runtime
                .list()
                .into_iter()
                .map(|diagnostic| {
                    json!({
                        "application": diagnostic.application,
                        "version": diagnostic.version,
                        "state": diagnostic.state,
                        "generation": diagnostic.generation,
                        "package_sha256": diagnostic.package_hash,
                        "ready": diagnostic.readiness.ready,
                        "routes": diagnostic.routes,
                        "history": self.runtime.history_len(&diagnostic.application),
                    })
                })
                .collect(),
        )
    }

    fn route(&self, request: &ApplicationHttpRequest, path: &str) -> Result<Value> {
        let segments = path
            .trim_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        match (request.method, segments.as_slice()) {
            (HttpMethod::Get, []) => Ok(self.list()),
            (HttpMethod::Post, ["install"]) => {
                let package = self.package(&request.body)?;
                let application = package.manifest.identity.name.clone();
                let verification = self.runtime.install(package).map_err(rejected)?;
                let readiness = self.runtime.activate(&application).map_err(rejected)?;
                Ok(json!({
                    "application": application,
                    "package_sha256": verification.package_sha256,
                    "readiness": readiness,
                }))
            }
            (HttpMethod::Post, ["upgrade"]) => {
                let package = self.package(&request.body)?;
                let application = package.manifest.identity.name.clone();
                let readiness = self.runtime.upgrade(package).map_err(rejected)?;
                Ok(json!({"application": application, "readiness": readiness}))
            }
            (HttpMethod::Post, [application, "rollback"]) => {
                let readiness = self.runtime.rollback(application).map_err(rejected)?;
                Ok(json!({"application": application, "readiness": readiness}))
            }
            (HttpMethod::Post, [application, "disable"]) => {
                self.runtime.disable(application).map_err(rejected)?;
                Ok(json!({"application": application, "state": "disabled"}))
            }
            (HttpMethod::Delete, [application]) => {
                if self
                    .runtime
                    .list()
                    .iter()
                    .any(|diagnostic| diagnostic.application.eq_ignore_ascii_case(application))
                {
                    // Disabling an inactive (staged-only) package is a no-op error.
                    let _ = self.runtime.disable(application);
                }
                self.runtime.remove(application).map_err(rejected)?;
                Ok(json!({"application": application, "state": "removed"}))
            }
            _ => Err(AppRuntimeError::NotFound(format!(
                "no management operation {} {}",
                request.method, request.path
            ))),
        }
    }
}

/// Package and lifecycle rejections are client errors, not host failures.
fn rejected(error: AppRuntimeError) -> AppRuntimeError {
    match error {
        AppRuntimeError::InvalidPackage(_)
        | AppRuntimeError::Signature(_)
        | AppRuntimeError::Extension(_)
        | AppRuntimeError::Json(_) => AppRuntimeError::InvalidRequest(error.to_string()),
        error => error,
    }
}

impl TrustedHttpHandler for ManagementHttpHandler {
    fn handle(&self, request: &ApplicationHttpRequest) -> Result<Option<ApplicationHttpResponse>> {
        let Some(rest) = request.path.strip_prefix(MANAGEMENT_PATH_PREFIX) else {
            return Ok(None);
        };
        if !(rest.is_empty() || rest.starts_with('/')) {
            return Ok(None);
        }
        self.authorize(request)?;
        let value = self.route(request, rest)?;
        Ok(Some(ApplicationHttpResponse {
            status: 200,
            headers: vec![("cache-control".to_string(), "no-store".to_string())],
            body: HttpResponseBodyV2::Json(value),
            trailers: Vec::new(),
            retry_after_ms: None,
        }))
    }
}

#[cfg(test)]
#[path = "management_tests.rs"]
mod tests;
