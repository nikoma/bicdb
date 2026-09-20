//! Tokio/Axum adapter for extension-owned HTTP routes.
//!
//! BicDB owns the listener and authentication boundary. WASM modules receive a
//! bounded JSON invocation and cannot bind sockets or accept connections.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::{Json, Router};
use serde_json::json;
use tokio::net::TcpListener;

use crate::host::{ExtensionHttpRequest, ExtensionRuntime, RouteAuthorization};
use crate::{HttpMethod, InvocationContext};

#[derive(Clone, Debug)]
pub struct ExtensionHttpAuthorization {
    pub decision: RouteAuthorization,
    pub context: InvocationContext,
}

impl Default for ExtensionHttpAuthorization {
    fn default() -> Self {
        Self {
            decision: RouteAuthorization::Anonymous,
            context: InvocationContext::default(),
        }
    }
}

/// Authentication/RLS adapter owned by the embedding application.
///
/// Returning `RowLevelSecurityChecked` is an assertion that the host has
/// already authenticated the request and installed a context suitable for
/// enforcing RLS on every database access performed for it.
pub type ExtensionHttpAuthorizer =
    Arc<dyn Fn(&HeaderMap, &Method, &Uri) -> ExtensionHttpAuthorization + Send + Sync>;

#[derive(Clone)]
pub struct ExtensionHttpConfig {
    pub max_request_bytes: usize,
    pub authorize: ExtensionHttpAuthorizer,
}

impl std::fmt::Debug for ExtensionHttpConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExtensionHttpConfig")
            .field("max_request_bytes", &self.max_request_bytes)
            .finish_non_exhaustive()
    }
}

impl Default for ExtensionHttpConfig {
    fn default() -> Self {
        Self {
            max_request_bytes: 1024 * 1024,
            authorize: Arc::new(|_, _, _| ExtensionHttpAuthorization::default()),
        }
    }
}

#[derive(Clone)]
struct HttpState {
    runtime: ExtensionRuntime,
    authorize: ExtensionHttpAuthorizer,
}

/// Build a fallback router for all catalog-declared extension routes.
///
/// Applications may nest this below a prefix or merge it with their existing
/// Axum router. Unknown paths return 404; methods not representable by ABI v1
/// return 405.
pub fn extension_router(runtime: ExtensionRuntime, config: ExtensionHttpConfig) -> Router {
    let max_request_bytes = config.max_request_bytes.max(1);
    Router::new()
        .fallback(any(dispatch))
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .with_state(HttpState {
            runtime,
            authorize: config.authorize,
        })
}

/// Serve extension routes on a listener created and policy-checked by the
/// BicDB embedding host.
///
/// Modules never receive this listener or a Tokio handle and cannot bind
/// sockets because the WASM ABI accepts no imports.
pub async fn serve_extension_http(
    listener: TcpListener,
    runtime: ExtensionRuntime,
    config: ExtensionHttpConfig,
) -> std::io::Result<()> {
    axum::serve(listener, extension_router(runtime, config)).await
}

async fn dispatch(
    State(state): State<HttpState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(extension_method) = http_method(&method) else {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            Json(json!({"error": "HTTP method is not supported by extension ABI v1"})),
        )
            .into_response();
    };
    let body = if body.is_empty() {
        serde_json::Value::Null
    } else {
        match serde_json::from_slice(&body) {
            Ok(body) => body,
            Err(error) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": format!("request body must be JSON: {error}")})),
                )
                    .into_response()
            }
        }
    };
    let authorization = (state.authorize)(&headers, &method, &uri);
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .unwrap_or_else(next_request_id);
    let request_headers = headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect();
    let query = uri
        .query()
        .map(|query| BTreeMap::from([("_raw".to_string(), query.to_string())]))
        .unwrap_or_default();
    let is_head = extension_method == HttpMethod::Head;
    match state.runtime.dispatch_http(ExtensionHttpRequest {
        id: request_id,
        method: extension_method,
        path: uri.path().to_string(),
        headers: request_headers,
        query,
        body,
        context: authorization.context,
        authorization: authorization.decision,
    }) {
        Ok(Some(result)) => {
            let mut response = extension_response(result);
            if is_head {
                *response.body_mut() = Body::empty();
            }
            response
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "extension route not found"})),
        )
            .into_response(),
        Err(error) => {
            let message = error.to_string();
            let status = if message.contains("requires") && message.contains("authorization") {
                StatusCode::FORBIDDEN
            } else if message.contains("resource limit") {
                StatusCode::SERVICE_UNAVAILABLE
            } else if message.contains("payload is invalid") {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (status, Json(json!({"error": message}))).into_response()
        }
    }
}

fn extension_response(result: crate::ExtensionInvocationResult) -> Response {
    let status = StatusCode::from_u16(result.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let raw_text = result
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .is_some_and(|(_, value)| !value.to_ascii_lowercase().starts_with("application/json"))
        .then(|| result.body.as_str().map(str::to_string))
        .flatten();
    let mut response = match raw_text {
        Some(body) => (status, body).into_response(),
        None => (status, Json(result.body)).into_response(),
    };
    for (name, value) in result.headers {
        let Ok(name) = HeaderName::try_from(name) else {
            continue;
        };
        let Ok(value) = HeaderValue::try_from(value) else {
            continue;
        };
        response.headers_mut().insert(name, value);
    }
    response
}

fn http_method(method: &Method) -> Option<HttpMethod> {
    match *method {
        Method::GET => Some(HttpMethod::Get),
        Method::POST => Some(HttpMethod::Post),
        Method::PUT => Some(HttpMethod::Put),
        Method::PATCH => Some(HttpMethod::Patch),
        Method::DELETE => Some(HttpMethod::Delete),
        Method::HEAD => Some(HttpMethod::Head),
        Method::OPTIONS => Some(HttpMethod::Options),
        _ => None,
    }
}

fn next_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);
    format!(
        "extension-http-{}-{}",
        std::process::id(),
        REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}
