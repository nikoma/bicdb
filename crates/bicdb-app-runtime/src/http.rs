//! Host-managed HTTP ABI v2 routing, authentication, limits, and policies.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::path::Path as StdPath;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::body::{to_bytes, Body};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderName, HeaderValue, Request, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{any, get};
use axum::Router;
use bicdb_extension::abi_v2::{
    ActorContext, ApplicationRealtimeContractV1, ApplicationRouteParameterTypeV1,
    ApplicationRouteParameterV1, ApplicationRouteParameterValidationV1, ApplicationRouteRequestV1,
    ApplicationSamplingV1, FieldType, HostHandle, HttpRequestBodyV2, HttpRequestV2,
    HttpResponseBodyV2, HttpResponseV2, ResourceFilterKind, ResourceOperation, ResourceRecordScope,
    RouteV2, StreamKind,
};
use bicdb_extension::{HttpMethod, InvocationKind};
use bytes::Bytes;
use http_body::Frame;
use http_body_util::StreamBody;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::{
    AppRuntimeError, ApplicationRuntime, JwtAuthenticator, ResourceRequest, ResourceResponse,
    Result,
};

/// A trusted deployment guard evaluated before reading a body or routing a
/// request. Implementations must be quick and must not perform network I/O.
pub trait HttpAdmissionCheck: std::fmt::Debug + Send + Sync {
    fn is_admitted(&self) -> bool;
}

#[derive(Clone, Debug)]
pub struct HttpHostPolicy {
    pub admission_check: Option<Arc<dyn HttpAdmissionCheck>>,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub request_timeout_ms: u64,
    pub trusted_proxies: BTreeSet<IpAddr>,
    pub cors_origins: BTreeSet<String>,
    pub allow_credentials: bool,
    pub csrf_header: Option<String>,
    pub rate_limit_per_minute: u32,
    pub max_concurrent_requests: usize,
    /// Optional host-owned session cookie. Application code never sees its
    /// value and cannot emit or overwrite the cookie.
    pub session_cookie_name: Option<String>,
}

impl HttpHostPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.max_request_bytes == 0
            || self.max_response_bytes == 0
            || self.request_timeout_ms == 0
            || self.rate_limit_per_minute == 0
            || self.max_concurrent_requests == 0
            || (self.allow_credentials && self.cors_origins.contains("*"))
            || self.session_cookie_name.as_deref().is_some_and(|name| {
                !name.starts_with("__Host-")
                    || name.len() > 128
                    || !name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            })
        {
            return Err(AppRuntimeError::InvalidPackage(
                "invalid HTTP host limits or credentialed wildcard CORS".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for HttpHostPolicy {
    fn default() -> Self {
        Self {
            admission_check: None,
            max_request_bytes: 8 * 1024 * 1024,
            max_response_bytes: 16 * 1024 * 1024,
            request_timeout_ms: 30_000,
            trusted_proxies: BTreeSet::new(),
            cors_origins: BTreeSet::new(),
            allow_credentials: false,
            csrf_header: Some("x-csrf-token".to_string()),
            rate_limit_per_minute: 1_000,
            max_concurrent_requests: 1_024,
            session_cookie_name: None,
        }
    }
}

/// Reserved host-origin HTTP surface. Implementations live in trusted host
/// code, run before application routing, and return only bounded response
/// values. Application modules cannot register these paths or obtain this
/// capability.
pub trait TrustedHttpHandler: Send + Sync {
    fn handle(&self, request: &ApplicationHttpRequest) -> Result<Option<ApplicationHttpResponse>>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationHttpRequest {
    pub method: HttpMethod,
    pub path: String,
    #[serde(default)]
    pub query: Vec<(String, String)>,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    pub body: HttpRequestBodyV2,
    #[serde(default)]
    pub peer_address: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationHttpResponse {
    pub status: u16,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    pub body: HttpResponseBodyV2,
    #[serde(default)]
    pub trailers: Vec<(String, String)>,
    #[serde(default)]
    pub retry_after_ms: Option<u64>,
}

pub struct HttpServerHandle {
    pub address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl HttpServerHandle {
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task
            .await
            .map_err(|error| AppRuntimeError::Provider(error.to_string()))??;
        Ok(())
    }
}

#[derive(Clone)]
struct HttpState {
    runtime: Arc<ApplicationRuntime>,
    authenticator: Arc<JwtAuthenticator>,
    policy: HttpHostPolicy,
    rate_windows: Arc<Mutex<BTreeMap<String, RateWindow>>>,
    admission: Arc<Semaphore>,
    trusted_handler: Option<Arc<dyn TrustedHttpHandler>>,
}

#[derive(Clone, Copy)]
struct RateWindow {
    minute: u64,
    requests: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ApplicationRealtimeTransport {
    Negotiate,
    LongPolling,
    ServerSentEvents,
    WebSocket,
}

#[derive(Clone)]
struct ApplicationRealtimeSession {
    application: String,
    contract: ApplicationRealtimeContractV1,
    actor: ActorContext,
    transport: ApplicationRealtimeTransport,
    cursor: u64,
    group: Option<String>,
    wait_ms: u64,
    requested_transport: Option<String>,
    authorization_fingerprint: Option<String>,
    response_headers: BTreeMap<String, String>,
    origin: Option<String>,
    max_request_bytes: usize,
    max_response_bytes: usize,
}

struct ApplicationRealtimeBatch {
    items: Vec<Value>,
    next_cursor: u64,
}

impl ApplicationRuntime {
    /// Dispatches a parsed request without granting the application ownership
    /// of the socket or connection lifecycle.
    pub fn dispatch_http_v2(
        &self,
        authenticator: &JwtAuthenticator,
        policy: &HttpHostPolicy,
        request: ApplicationHttpRequest,
    ) -> Result<ApplicationHttpResponse> {
        self.dispatch_http_v2_inner(Some(authenticator), policy, request, None)
    }

    /// Executes an authored BicDB application test request through the same coercion,
    /// routing, authority, and response path as network HTTP without creating
    /// an authentication bypass reachable from a socket.
    pub(crate) fn dispatch_carrier_test_http(
        &self,
        policy: &HttpHostPolicy,
        request: ApplicationHttpRequest,
        actor: Option<ActorContext>,
    ) -> Result<ApplicationHttpResponse> {
        self.dispatch_http_v2_inner(None, policy, request, actor)
    }

    fn dispatch_http_v2_inner(
        &self,
        authenticator: Option<&JwtAuthenticator>,
        policy: &HttpHostPolicy,
        request: ApplicationHttpRequest,
        trusted_test_actor: Option<ActorContext>,
    ) -> Result<ApplicationHttpResponse> {
        policy.validate()?;
        if policy
            .admission_check
            .as_ref()
            .is_some_and(|check| !check.is_admitted())
        {
            return Err(AppRuntimeError::NotReady(
                "deployment admission expired".to_string(),
            ));
        }
        if request.path == "/_bicdb/blob" {
            const SIGNED_BLOB_PARAMETERS: &[&str] = &[
                "namespace",
                "key",
                "expires",
                "method",
                "download_name",
                "signature",
            ];
            if let Some((name, _)) = request
                .query
                .iter()
                .find(|(name, _)| !SIGNED_BLOB_PARAMETERS.contains(&name.as_str()))
            {
                return Err(AppRuntimeError::InvalidRequest(format!(
                    "signed blob URL contains unknown parameter `{name}`"
                )));
            }
            let parameter = |name: &str, required: bool| -> Result<Option<String>> {
                let values = request
                    .query
                    .iter()
                    .filter(|(candidate, _)| candidate == name)
                    .map(|(_, value)| value.clone())
                    .collect::<Vec<_>>();
                if values.len() > 1 || (required && values.is_empty()) {
                    return Err(AppRuntimeError::InvalidRequest(format!(
                        "signed blob URL requires exactly one `{name}` parameter"
                    )));
                }
                Ok(values.into_iter().next())
            };
            let namespace = parameter("namespace", true)?.expect("required parameter");
            let key = parameter("key", true)?.expect("required parameter");
            let expires = parameter("expires", true)?
                .expect("required parameter")
                .parse::<i64>()
                .map_err(|_| {
                    AppRuntimeError::InvalidRequest(
                        "signed blob URL has an invalid expiry".to_string(),
                    )
                })?;
            let signed_method = parameter("method", true)?.expect("required parameter");
            let signature = parameter("signature", true)?.expect("required parameter");
            let download_name =
                parameter("download_name", false)?.filter(|value| !value.is_empty());
            let body = match &request.body {
                HttpRequestBodyV2::Empty => Vec::new(),
                HttpRequestBodyV2::Binary(bytes) => bytes.clone(),
                _ => {
                    return Err(AppRuntimeError::InvalidRequest(
                        "signed blob uploads require a binary request body".to_string(),
                    ));
                }
            };
            let (status, headers, response) = self.execute_signed_blob_http(
                &request.method.to_string(),
                &namespace,
                &key,
                expires,
                &signed_method,
                download_name.as_deref(),
                &signature,
                header(&request.headers, "content-type").as_deref(),
                &body,
            )?;
            let body = if headers.iter().any(|(name, value)| {
                name.eq_ignore_ascii_case("content-type")
                    && value.eq_ignore_ascii_case("application/json")
            }) {
                HttpResponseBodyV2::Json(serde_json::from_slice(&response)?)
            } else {
                HttpResponseBodyV2::Binary(response)
            };
            return Ok(ApplicationHttpResponse {
                status,
                headers,
                body,
                trailers: Vec::new(),
                retry_after_ms: None,
            });
        }
        let started = Instant::now();
        let request_id = request_identifier(&request.headers, "x-request-id")?
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let trace_context = request_trace_context(&request.headers);
        let trace_id = trace_context.trace_id.clone();
        let correlation_id = Some(
            request_identifier(&request.headers, "x-correlation-id")?
                .unwrap_or_else(|| request_id.clone()),
        );
        let origin = header(&request.headers, "origin");
        if let Some(origin) = origin.as_ref() {
            if !policy.cors_origins.is_empty()
                && !policy.cors_origins.contains(origin)
                && !policy.cors_origins.contains("*")
            {
                return Err(AppRuntimeError::CapabilityDenied(
                    "request origin is not allowed".to_string(),
                ));
            }
        }
        if matches!(
            request.method,
            HttpMethod::Post | HttpMethod::Put | HttpMethod::Patch | HttpMethod::Delete
        ) {
            if let Some(csrf_header) = policy.csrf_header.as_deref() {
                let cookie_authenticated = header(&request.headers, "cookie").is_some();
                if cookie_authenticated && header(&request.headers, csrf_header).is_none() {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "state-changing cookie request lacks CSRF evidence".to_string(),
                    ));
                }
            }
        }
        let deadline = crate::host::now_ms().saturating_add(policy.request_timeout_ms as i64);
        let bearer = authentication_credential(&request.headers, policy)?;

        let snapshots = self.active_snapshots();
        for snapshot in snapshots {
            let application = snapshot.package.manifest.application.as_deref().unwrap();
            if let Some((route, path_parameters)) =
                most_specific_matching_route(&application.routes, &request.method, &request.path)
            {
                let actor = if authenticator.is_none() {
                    match trusted_test_actor.clone() {
                        Some(actor) => actor,
                        None if route.public => anonymous_actor(
                            trace_id.clone(),
                            correlation_id.clone(),
                            origin.clone(),
                            deadline,
                        )?,
                        None => {
                            return Err(AppRuntimeError::Authentication(
                                "missing Authorization header".to_string(),
                            ));
                        }
                    }
                } else {
                    match bearer.as_deref() {
                        Some(bearer) => {
                            let authenticator = authenticator.expect("production authenticator");
                            if let Some(scheme_name) = route.auth_scheme.as_deref() {
                                let scheme = application.auth_schemes.get(scheme_name).ok_or_else(|| {
                                AppRuntimeError::InvalidPackage(format!(
                                    "route `{}` references absent authentication scheme `{scheme_name}`",
                                    route.name
                                ))
                            })?;
                                authenticator.authenticate_for(
                                    scheme,
                                    bearer,
                                    trace_id.clone(),
                                    correlation_id.clone(),
                                    origin.clone(),
                                    deadline,
                                )?
                            } else if application.auth_schemes.is_empty() {
                                authenticator.authenticate(
                                    bearer,
                                    trace_id.clone(),
                                    correlation_id.clone(),
                                    origin.clone(),
                                    deadline,
                                )?
                            } else {
                                authenticator.authenticate_for_any(
                                    application.auth_schemes.values(),
                                    bearer,
                                    trace_id.clone(),
                                    correlation_id.clone(),
                                    origin.clone(),
                                    deadline,
                                )?
                            }
                        }
                        None if route.public => anonymous_actor(
                            trace_id.clone(),
                            correlation_id.clone(),
                            origin.clone(),
                            deadline,
                        )?,
                        None => {
                            return Err(AppRuntimeError::Authentication(
                                "missing Authorization header".to_string(),
                            ));
                        }
                    }
                };
                let sampling = route
                    .telemetry
                    .as_ref()
                    .map(|telemetry| telemetry.sampling)
                    .or_else(|| {
                        application
                            .application_program
                            .as_ref()
                            .and_then(|program| program.observability.as_ref())
                            .map(|observability| observability.sampling)
                    })
                    .unwrap_or(ApplicationSamplingV1::ParentBasedAlwaysOn);
                let actor = apply_request_trace_context(
                    actor,
                    &request_id,
                    correlation_id
                        .as_deref()
                        .expect("HTTP correlation id is always populated"),
                    &trace_context,
                    sampling,
                );
                if !authority_matches(&route.roles, &actor.roles, route.roles_any)
                    || !authority_matches(&route.scopes, &actor.scopes, route.scopes_any)
                {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "actor lacks route roles or scopes".to_string(),
                    ));
                }
                let request_body_bytes = http_request_body_len(&request.body)?;
                if request_body_bytes > route.max_request_bytes as usize
                    || request_body_bytes > policy.max_request_bytes
                {
                    return Err(AppRuntimeError::Invocation(
                        "request body exceeds route limit".to_string(),
                    ));
                }
                let mut response = if let (Some(resource), Some(operation)) =
                    (route.resource.as_deref(), route.operation)
                {
                    let contract = application
                        .resources
                        .iter()
                        .find(|contract| contract.name == resource)
                        .ok_or_else(|| {
                            AppRuntimeError::InvalidPackage(format!(
                                "route references absent resource `{resource}`"
                            ))
                        })?;
                    let resource_request =
                        lower_resource_request(contract, operation, &path_parameters, &request)?;
                    from_resource(self.invoke_resource(
                        &snapshot.package.manifest.identity.name,
                        resource,
                        actor.clone(),
                        resource_request,
                    )?)
                } else if let Some(call) = &route.service_call {
                    let payload = match &request.body {
                        HttpRequestBodyV2::Json(value) => value.clone(),
                        HttpRequestBodyV2::Empty => json!({}),
                        _ => {
                            return Err(AppRuntimeError::Invocation(
                                "plugin service routes accept JSON bodies".to_string(),
                            ));
                        }
                    };
                    ApplicationHttpResponse {
                        status: 200,
                        headers: Vec::new(),
                        body: HttpResponseBodyV2::Json(self.invoke_route_service(
                            &snapshot.package.manifest.identity.name,
                            &call.dependency,
                            &call.service,
                            &call.method,
                            actor.clone(),
                            payload,
                        )?),
                        trailers: Vec::new(),
                        retry_after_ms: None,
                    }
                } else if route.export.starts_with("__carrier_security_") {
                    let payload = match &request.body {
                        HttpRequestBodyV2::Json(value) => value.clone(),
                        HttpRequestBodyV2::Empty => json!({}),
                        _ => {
                            return Err(AppRuntimeError::InvalidRequest(
                                "BicDB application security routes accept JSON bodies".to_string(),
                            ));
                        }
                    };
                    let (status, value) = self.execute_carrier_security_route(
                        &snapshot.package.manifest.identity.name,
                        &route.export,
                        actor.clone(),
                        payload,
                        path_parameters.get("session_id").map(String::as_str),
                    )?;
                    ApplicationHttpResponse {
                        status,
                        headers: Vec::new(),
                        body: HttpResponseBodyV2::Json(value),
                        trailers: Vec::new(),
                        retry_after_ms: None,
                    }
                } else if application
                    .application_program
                    .as_ref()
                    .is_some_and(|program| program.callables.contains_key(&route.export))
                {
                    let globals = carrier_route_globals(
                        &request,
                        &path_parameters,
                        route.application_request.as_ref(),
                        &actor,
                        &request_id,
                    )?;
                    let callable = application
                        .application_program
                        .as_ref()
                        .and_then(|program| program.callables.get(&route.export))
                        .expect("BicDB application callable route was checked above");
                    let arguments = carrier_callable_http_arguments(
                        &route.name,
                        &callable.parameters,
                        globals.get("input"),
                    )?;
                    let (value, headers) = if let Some(contract) = &route.idempotency {
                        let key = header(&request.headers, &contract.header)
                            .map(|value| value.trim().to_string())
                            .filter(|value| !value.is_empty())
                            .ok_or_else(|| {
                                AppRuntimeError::MissingIdempotencyKey(format!(
                                    "{} header is required for this route",
                                    contract.header
                                ))
                            })?;
                        let normalized_request = carrier_idempotency_request(&globals);
                        let execution = retry_idempotent_transaction_conflicts(|| {
                            self.execute_carrier_callable_idempotent(
                                &snapshot.package.manifest.identity.name,
                                &route.export,
                                actor.clone(),
                                globals.clone(),
                                arguments.clone(),
                                contract,
                                &route.method.to_string(),
                                &route.template,
                                &key,
                                &normalized_request,
                            )
                        })?;
                        let status = if execution.replayed {
                            "replayed"
                        } else {
                            "stored"
                        };
                        (
                            execution.value,
                            vec![
                                (contract.header.clone(), key),
                                ("x-carrier-idempotency-scope".to_string(), execution.scope),
                                (
                                    "x-carrier-idempotency-status".to_string(),
                                    status.to_string(),
                                ),
                            ],
                        )
                    } else if let Some(contract) = &route.cache {
                        let normalized_request = carrier_cache_request(&globals);
                        (
                            self.execute_carrier_callable_cached(
                                &snapshot.package.manifest.identity.name,
                                &route.export,
                                actor.clone(),
                                globals,
                                arguments,
                                contract,
                                &route.method.to_string(),
                                &route.template,
                                &normalized_request,
                                !route.public,
                            )?,
                            Vec::new(),
                        )
                    } else {
                        (
                            self.execute_carrier_callable(
                                &snapshot.package.manifest.identity.name,
                                &route.export,
                                actor.clone(),
                                globals,
                                arguments,
                            )?,
                            Vec::new(),
                        )
                    };
                    let body = if route.streaming_response && route.sse {
                        let handle = value
                            .get("__carrier_sse_stream")
                            .and_then(Value::as_u64)
                            .ok_or_else(|| {
                                AppRuntimeError::Invocation(format!(
                                    "streaming route `{}` omitted its SSE handle",
                                    route.name
                                ))
                            })?;
                        HttpResponseBodyV2::Sse(HostHandle(handle))
                    } else {
                        HttpResponseBodyV2::Json(value)
                    };
                    ApplicationHttpResponse {
                        status: 200,
                        headers,
                        body,
                        trailers: Vec::new(),
                        retry_after_ms: None,
                    }
                } else {
                    let wasm_request = HttpRequestV2 {
                        request_id: request_id.clone(),
                        method: request.method,
                        path: request.path.clone(),
                        path_parameters,
                        query: request.query.clone(),
                        headers: request
                            .headers
                            .iter()
                            .filter(|(name, _)| !sensitive_request_header(name))
                            .cloned()
                            .collect(),
                        // Authentication material is consumed by the trusted host.
                        // Guest modules receive ActorContext, never ambient cookies.
                        cookies: Vec::new(),
                        body: request.body.clone(),
                        trusted_client_address: request.peer_address.clone(),
                        origin: origin.clone(),
                        deadline_unix_ms: actor.deadline_unix_ms,
                        trace_id: trace_id.clone(),
                        actor: actor.clone(),
                    };
                    let result = self.invoke_export(
                        &snapshot.package.manifest.identity.name,
                        &route.export,
                        InvocationKind::HttpRoute,
                        actor.clone(),
                        serde_json::to_value(wasm_request)?,
                    )?;
                    let response: HttpResponseV2 =
                        serde_json::from_value(result.body).map_err(|error| {
                            AppRuntimeError::Invocation(format!(
                                "route returned invalid HTTP ABI v2 response: {error}"
                            ))
                        })?;
                    ApplicationHttpResponse {
                        status: response.status,
                        headers: response.headers,
                        body: response.body,
                        trailers: response.trailers,
                        retry_after_ms: response.retry_after_ms,
                    }
                };
                let signed_headers = if route.response_headers.is_empty() {
                    &application.response_headers
                } else {
                    &route.response_headers
                };
                apply_signed_response_headers(&mut response, signed_headers);
                if let Some(value_type) = route.application_response.as_ref() {
                    normalize_carrier_response(&mut response, value_type, &route.name)?;
                }
                let response_bytes = http_response_body_len(&response.body)?;
                if response_bytes > route.max_response_bytes as usize
                    || response_bytes > policy.max_response_bytes
                {
                    return Err(AppRuntimeError::Invocation(
                        "response body exceeds route limit".to_string(),
                    ));
                }
                apply_trace_response_headers(&mut response, &actor, &request_id);
                if crate::runtime::actor_trace_sampled(&actor) {
                    self.record_observability(crate::ObservabilityEvent::Trace {
                        actor: actor.clone(),
                        name: "bicdb.http_route".to_string(),
                        fields: BTreeMap::from([
                            (
                                "application".to_string(),
                                Value::String(snapshot.package.manifest.identity.name.clone()),
                            ),
                            ("route".to_string(), Value::String(route.name.clone())),
                            (
                                "method".to_string(),
                                Value::String(request.method.to_string()),
                            ),
                            ("path".to_string(), Value::String(route.template.clone())),
                            ("request_id".to_string(), Value::String(request_id.clone())),
                            ("status".to_string(), Value::from(response.status)),
                            (
                                "elapsed_us".to_string(),
                                Value::from(started.elapsed().as_micros() as u64),
                            ),
                            ("success".to_string(), Value::Bool(response.status < 500)),
                        ]),
                    });
                }
                return Ok(with_http_policy_headers(
                    response,
                    origin.as_deref(),
                    policy,
                ));
            }
        }
        Err(AppRuntimeError::NotFound(format!(
            "no active route matches {} {}",
            request.method, request.path
        )))
    }

    /// Starts the cleartext listener used behind a trusted TLS terminator or
    /// for loopback development. Applications never receive the listener.
    pub async fn serve_http(
        self: Arc<Self>,
        listener: TcpListener,
        authenticator: Arc<JwtAuthenticator>,
        policy: HttpHostPolicy,
    ) -> Result<HttpServerHandle> {
        self.serve_http_with_handler(listener, authenticator, policy, None)
            .await
    }

    pub async fn serve_http_with_handler(
        self: Arc<Self>,
        listener: TcpListener,
        authenticator: Arc<JwtAuthenticator>,
        policy: HttpHostPolicy,
        trusted_handler: Option<Arc<dyn TrustedHttpHandler>>,
    ) -> Result<HttpServerHandle> {
        policy.validate()?;
        let address = listener.local_addr()?;
        let state = HttpState {
            runtime: self,
            authenticator,
            rate_windows: Arc::new(Mutex::new(BTreeMap::new())),
            admission: Arc::new(Semaphore::new(policy.max_concurrent_requests)),
            trusted_handler,
            policy,
        };
        let router = http_router(state);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
        });
        Ok(HttpServerHandle {
            address,
            shutdown: Some(shutdown_tx),
            task,
        })
    }

    /// Starts the first-party HTTPS listener. TLS keys stay in the trusted
    /// host and are never visible to an application module.
    pub async fn serve_http_tls(
        self: Arc<Self>,
        address: SocketAddr,
        certificate: impl AsRef<StdPath>,
        private_key: impl AsRef<StdPath>,
        authenticator: Arc<JwtAuthenticator>,
        policy: HttpHostPolicy,
    ) -> Result<HttpServerHandle> {
        self.serve_http_tls_with_handler(
            address,
            certificate,
            private_key,
            authenticator,
            policy,
            None,
        )
        .await
    }

    pub async fn serve_http_tls_with_handler(
        self: Arc<Self>,
        address: SocketAddr,
        certificate: impl AsRef<StdPath>,
        private_key: impl AsRef<StdPath>,
        authenticator: Arc<JwtAuthenticator>,
        policy: HttpHostPolicy,
        trusted_handler: Option<Arc<dyn TrustedHttpHandler>>,
    ) -> Result<HttpServerHandle> {
        policy.validate()?;
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(
            certificate.as_ref(),
            private_key.as_ref(),
        )
        .await
        .map_err(|error| AppRuntimeError::Provider(error.to_string()))?;
        let listener = std::net::TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let state = HttpState {
            runtime: self,
            authenticator,
            rate_windows: Arc::new(Mutex::new(BTreeMap::new())),
            admission: Arc::new(Semaphore::new(policy.max_concurrent_requests)),
            trusted_handler,
            policy,
        };
        let router = http_router(state);
        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        let server = axum_server::from_tcp_rustls(listener, tls).handle(handle);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            tokio::spawn(async move {
                let _ = shutdown_rx.await;
                shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(30)));
            });
            server
                .serve(router.into_make_service_with_connect_info::<SocketAddr>())
                .await
        });
        Ok(HttpServerHandle {
            address,
            shutdown: Some(shutdown_tx),
            task,
        })
    }
}

fn anonymous_actor(
    trace_id: String,
    correlation_id: Option<String>,
    request_origin: Option<String>,
    deadline_unix_ms: i64,
) -> Result<ActorContext> {
    let actor = ActorContext {
        service_id: Some("bicdb-anonymous".to_string()),
        authentication_method: Some("anonymous".to_string()),
        trace_id,
        correlation_id,
        request_origin,
        deadline_unix_ms,
        ..ActorContext::default()
    };
    actor.validate()?;
    Ok(actor)
}

fn authority_matches(
    required: &BTreeSet<String>,
    actual: &BTreeSet<String>,
    match_any: bool,
) -> bool {
    required.is_empty()
        || if match_any {
            !required.is_disjoint(actual)
        } else {
            required.is_subset(actual)
        }
}

/// Whether a realtime session's credential has run out.
///
/// `ActorContext::deadline_unix_ms` is `min(request_deadline, token_exp)` as
/// computed by `JwtAuthenticator`, so the token's expiry is already on the
/// session. A deadline of zero means "not supplied" and must not be read as
/// "expired at the epoch", which would close every session that never carried
/// a bearer.
fn realtime_credential_expired(actor: &ActorContext, now_unix_ms: i64) -> bool {
    actor.deadline_unix_ms > 0 && now_unix_ms >= actor.deadline_unix_ms
}

impl ApplicationRuntime {
    fn prepare_carrier_realtime(
        &self,
        authenticator: &JwtAuthenticator,
        policy: &HttpHostPolicy,
        request: &ApplicationHttpRequest,
    ) -> Result<Option<ApplicationRealtimeSession>> {
        if request.method != HttpMethod::Get {
            return Ok(None);
        }
        let request_id = request_identifier(&request.headers, "x-request-id")?
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let trace_context = request_trace_context(&request.headers);
        let trace_id = trace_context.trace_id.clone();
        let correlation_id = Some(
            request_identifier(&request.headers, "x-correlation-id")?
                .unwrap_or_else(|| request_id.clone()),
        );
        let origin = header(&request.headers, "origin");
        if let Some(origin) = origin.as_ref() {
            if !policy.cors_origins.is_empty()
                && !policy.cors_origins.contains(origin)
                && !policy.cors_origins.contains("*")
            {
                return Err(AppRuntimeError::CapabilityDenied(
                    "request origin is not allowed".to_string(),
                ));
            }
        }
        let deadline = crate::host::now_ms().saturating_add(policy.request_timeout_ms as i64);
        let bearer = authentication_credential(&request.headers, policy)?;
        for snapshot in self.active_snapshots() {
            let application = snapshot.package.manifest.application.as_deref().unwrap();
            for route in &application.routes {
                if route.method != HttpMethod::Get || route.template != request.path {
                    continue;
                }
                let Some((contract, transport)) = carrier_realtime_route(application, route) else {
                    continue;
                };
                let actor = match bearer.as_deref() {
                    Some(bearer) => {
                        if let Some(scheme_name) = route.auth_scheme.as_deref() {
                            let scheme = application.auth_schemes.get(scheme_name).ok_or_else(|| {
                                AppRuntimeError::InvalidPackage(format!(
                                    "route `{}` references absent authentication scheme `{scheme_name}`",
                                    route.name
                                ))
                            })?;
                            authenticator.authenticate_for(
                                scheme,
                                bearer,
                                trace_id.clone(),
                                correlation_id.clone(),
                                origin.clone(),
                                deadline,
                            )?
                        } else if application.auth_schemes.is_empty() {
                            authenticator.authenticate(
                                bearer,
                                trace_id.clone(),
                                correlation_id.clone(),
                                origin.clone(),
                                deadline,
                            )?
                        } else {
                            authenticator.authenticate_for_any(
                                application.auth_schemes.values(),
                                bearer,
                                trace_id.clone(),
                                correlation_id.clone(),
                                origin.clone(),
                                deadline,
                            )?
                        }
                    }
                    None if route.public => anonymous_actor(
                        trace_id.clone(),
                        correlation_id.clone(),
                        origin.clone(),
                        deadline,
                    )?,
                    None => {
                        return Err(AppRuntimeError::Authentication(
                            "missing Authorization header".to_string(),
                        ));
                    }
                };
                let sampling = route
                    .telemetry
                    .as_ref()
                    .map(|telemetry| telemetry.sampling)
                    .or_else(|| {
                        application
                            .application_program
                            .as_ref()
                            .and_then(|program| program.observability.as_ref())
                            .map(|observability| observability.sampling)
                    })
                    .unwrap_or(ApplicationSamplingV1::ParentBasedAlwaysOn);
                let actor = apply_request_trace_context(
                    actor,
                    &request_id,
                    correlation_id
                        .as_deref()
                        .expect("realtime correlation id is always populated"),
                    &trace_context,
                    sampling,
                );
                if !authority_matches(&route.roles, &actor.roles, route.roles_any)
                    || !authority_matches(&route.scopes, &actor.scopes, route.scopes_any)
                {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "actor lacks realtime roles or scopes".to_string(),
                    ));
                }
                if contract.tenant_field.is_some() && actor.tenant_id.is_none() {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "tenant-scoped realtime requires a trusted tenant actor".to_string(),
                    ));
                }
                if contract.workspace_field.is_some() && actor.workspace_id.is_none() {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "workspace-scoped realtime requires a trusted workspace actor".to_string(),
                    ));
                }
                let cursor = realtime_query_value(&request.query, "cursor")
                    .map(|value| {
                        value.parse::<u64>().map_err(|_| {
                            AppRuntimeError::InvalidRequest(
                                "realtime cursor must be a non-negative integer".to_string(),
                            )
                        })
                    })
                    .transpose()?
                    .unwrap_or_default();
                let group = realtime_query_value(&request.query, "group")
                    .map(|value| value.trim().to_string());
                match (contract.group_field.as_ref(), group.as_deref()) {
                    (Some(_), Some(group)) if !group.is_empty() && group != "*" => {}
                    (Some(_), _) => {
                        return Err(AppRuntimeError::InvalidRequest(
                            "grouped realtime requires a non-blank, non-wildcard group".to_string(),
                        ));
                    }
                    (None, Some(_)) => {
                        return Err(AppRuntimeError::InvalidRequest(
                            "ungrouped realtime does not accept a group".to_string(),
                        ));
                    }
                    (None, None) => {}
                }
                let wait_ms = realtime_query_value(&request.query, "wait_ms")
                    .map(|value| {
                        value.parse::<u64>().map_err(|_| {
                            AppRuntimeError::InvalidRequest(
                                "realtime wait_ms must be an integer".to_string(),
                            )
                        })
                    })
                    .transpose()?
                    .unwrap_or(25_000)
                    .clamp(100, 30_000);
                let requested_transport = realtime_query_value(&request.query, "transport");
                let mut response_headers = if route.response_headers.is_empty() {
                    application.response_headers.clone()
                } else {
                    route.response_headers.clone()
                };
                response_headers.insert("x-request-id".to_string(), request_id.clone());
                response_headers.insert(
                    "x-correlation-id".to_string(),
                    actor
                        .correlation_id
                        .clone()
                        .expect("realtime correlation id"),
                );
                response_headers.insert("x-trace-id".to_string(), actor.trace_id.clone());
                let span_source = Uuid::new_v4().simple().to_string();
                response_headers.insert(
                    "traceparent".to_string(),
                    format!(
                        "00-{}-{}-{}",
                        actor.trace_id,
                        &span_source[..16],
                        actor
                            .policy_attributes
                            .get("w3c.trace_flags")
                            .map(String::as_str)
                            .unwrap_or("00")
                    ),
                );
                if let Some(tracestate) = actor.policy_attributes.get("w3c.tracestate") {
                    response_headers.insert("tracestate".to_string(), tracestate.clone());
                }
                let mut session = ApplicationRealtimeSession {
                    application: snapshot.package.manifest.identity.name.clone(),
                    contract: contract.clone(),
                    actor,
                    transport,
                    cursor,
                    group,
                    wait_ms,
                    requested_transport,
                    authorization_fingerprint: None,
                    response_headers,
                    origin,
                    max_request_bytes: (route.max_request_bytes as usize)
                        .min(policy.max_request_bytes),
                    max_response_bytes: (route.max_response_bytes as usize)
                        .min(policy.max_response_bytes),
                };
                session.authorization_fingerprint =
                    self.carrier_realtime_authorization_fingerprint(&session)?;
                return Ok(Some(session));
            }
        }
        Ok(None)
    }

    fn carrier_realtime_authorization_fingerprint(
        &self,
        session: &ApplicationRealtimeSession,
    ) -> Result<Option<String>> {
        let Some(callable) = session.contract.authorize_group.as_deref() else {
            return Ok(None);
        };
        let group = session.group.as_deref().ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(
                "realtime group authorization requires a group".to_string(),
            )
        })?;
        let value = self
            .execute_carrier_callable(
                &session.application,
                callable,
                session.actor.clone(),
                BTreeMap::from([("group".to_string(), Value::String(group.to_string()))]),
                vec![(Some("group".to_string()), Value::String(group.to_string()))],
            )
            .map_err(|_| {
                AppRuntimeError::CapabilityDenied("realtime group access denied".to_string())
            })?;
        value
            .as_str()
            .filter(|value| !value.is_empty())
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "realtime group authorization returned no fingerprint".to_string(),
                )
            })
    }

    fn revalidate_carrier_realtime(&self, session: &ApplicationRealtimeSession) -> Result<()> {
        // The credential is verified once, when the stream is established, and
        // a realtime stream then lives for as long as the client keeps it open.
        // The periodic check below re-runs the application's `authorize_group`
        // callable — but it re-runs it with the actor CACHED at connect time,
        // and it is a no-op for any contract that declares no `authorize_group`
        // at all. So nothing re-examined the bearer itself: a session opened a
        // second before its token expired kept streaming indefinitely, and
        // revoking a token could not end a stream already in flight, which is
        // the one control that matters for a leaked credential.
        //
        // `ActorContext::deadline_unix_ms` already carries
        // `min(request_deadline, token_exp)` from `JwtAuthenticator`, so the
        // expiry is on the session; it was simply never consulted. Checked
        // first, so it applies to every contract rather than only those with a
        // group callable.
        if realtime_credential_expired(&session.actor, crate::host::now_ms()) {
            return Err(AppRuntimeError::Authentication(
                "realtime session credential has expired".to_string(),
            ));
        }
        let current = self.carrier_realtime_authorization_fingerprint(session)?;
        if current != session.authorization_fingerprint {
            return Err(AppRuntimeError::CapabilityDenied(
                "realtime group authorization changed".to_string(),
            ));
        }
        Ok(())
    }

    fn carrier_realtime_batch(
        &self,
        session: &ApplicationRealtimeSession,
        cursor: u64,
        limit: usize,
    ) -> Result<ApplicationRealtimeBatch> {
        let now = crate::host::now_ms();
        let messages = self.peek_realtime_queue(&session.contract.queue, cursor, 4_096);
        let mut items = Vec::new();
        let mut next_cursor = cursor;
        for message in messages {
            if message.available_at > now {
                continue;
            }
            let sequence = message.sequence.saturating_add(1);
            next_cursor = next_cursor.max(sequence);
            let event = message
                .headers
                .get("bicdb_application")
                .and_then(|headers| headers.get("carrier_event"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    AppRuntimeError::Provider(
                        "realtime message lacks its host-owned event name".to_string(),
                    )
                })?;
            if !session.contract.events.contains(event) {
                continue;
            }
            let payload = normalize_carrier_json_value(
                &message.payload,
                &session.contract.output,
                "realtime.payload",
            )
            .map_err(|error| {
                AppRuntimeError::Provider(format!(
                    "realtime event `{event}` violates its signed output: {error}"
                ))
            })?;
            if !realtime_payload_matches(
                &payload,
                session.contract.tenant_field.as_deref(),
                session.actor.tenant_id.as_deref(),
            ) || !realtime_payload_matches(
                &payload,
                session.contract.workspace_field.as_deref(),
                session.actor.workspace_id.as_deref(),
            ) || !realtime_payload_matches(
                &payload,
                session.contract.group_field.as_deref(),
                session.group.as_deref(),
            ) {
                continue;
            }
            let created_at =
                chrono::DateTime::<chrono::Utc>::from_timestamp_millis(message.created_at)
                    .ok_or_else(|| {
                        AppRuntimeError::Provider(
                            "realtime event has an invalid timestamp".to_string(),
                        )
                    })?;
            items.push(json!({
                "stream": session.contract.name,
                "event": event,
                "sequence": sequence,
                "created_at": created_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                "group": session.group,
                "payload": payload,
            }));
            if items.len() >= limit {
                break;
            }
        }
        Ok(ApplicationRealtimeBatch { items, next_cursor })
    }
}

fn carrier_realtime_route<'a>(
    application: &'a bicdb_extension::abi_v2::ApplicationManifestV2,
    route: &RouteV2,
) -> Option<(
    &'a ApplicationRealtimeContractV1,
    ApplicationRealtimeTransport,
)> {
    application.realtime.iter().find_map(|contract| {
        [
            ("negotiate", ApplicationRealtimeTransport::Negotiate),
            ("poll", ApplicationRealtimeTransport::LongPolling),
            ("sse", ApplicationRealtimeTransport::ServerSentEvents),
            ("ws", ApplicationRealtimeTransport::WebSocket),
        ]
        .into_iter()
        .find(|(suffix, _)| route.template == format!("{}/{suffix}", contract.path))
        .map(|(_, transport)| (contract, transport))
    })
}

fn realtime_query_value(query: &[(String, String)], name: &str) -> Option<String> {
    query
        .iter()
        .rev()
        .find(|(candidate, _)| candidate == name)
        .map(|(_, value)| value.clone())
}

fn realtime_payload_matches(payload: &Value, field: Option<&str>, expected: Option<&str>) -> bool {
    match field {
        None => true,
        Some(field) => payload.get(field).and_then(Value::as_str) == expected,
    }
}

fn coerce_carrier_parameters(
    contract: &[ApplicationRouteParameterV1],
    raw: &serde_json::Map<String, Value>,
    location: &str,
) -> Result<serde_json::Map<String, Value>> {
    let mut output = serde_json::Map::new();
    for parameter in contract {
        let value = match raw.get(&parameter.name) {
            Some(value) => value.clone(),
            None => match parameter.default_json.as_deref() {
                Some(default) => serde_json::from_str(default).map_err(|error| {
                    AppRuntimeError::InvalidPackage(format!(
                        "BicDB application {location} parameter `{}` has invalid signed default JSON: {error}",
                        parameter.name
                    ))
                })?,
                None if parameter.optional => Value::Null,
                None => {
                    return Err(AppRuntimeError::InvalidRequest(format!(
                        "missing required BicDB application {location} parameter `{}`",
                        parameter.name
                    )));
                }
            },
        };
        let value = if parameter.optional && value.is_null() {
            Value::Null
        } else {
            coerce_carrier_parameter_value(
                &value,
                &parameter.value_type,
                location,
                &parameter.name,
            )?
        };
        validate_carrier_parameter(&value, parameter, location)?;
        output.insert(parameter.name.clone(), value);
    }
    Ok(output)
}

pub(crate) fn coerce_carrier_parameter_value(
    value: &Value,
    value_type: &ApplicationRouteParameterTypeV1,
    location: &str,
    name: &str,
) -> Result<Value> {
    let invalid = || {
        AppRuntimeError::InvalidRequest(format!(
            "BicDB application {location} parameter `{name}` does not match its signed type"
        ))
    };
    match value_type {
        ApplicationRouteParameterTypeV1::String => value
            .as_str()
            .map(|value| Value::String(value.to_string()))
            .ok_or_else(invalid),
        ApplicationRouteParameterTypeV1::Int => {
            let value = match value {
                Value::Number(value) => value.as_i64(),
                Value::String(value) => value.parse::<i64>().ok(),
                _ => None,
            }
            .ok_or_else(invalid)?;
            Ok(Value::Number(value.into()))
        }
        ApplicationRouteParameterTypeV1::Float => {
            let value = match value {
                Value::Number(value) => value.as_f64(),
                Value::String(value) => value.parse::<f64>().ok(),
                _ => None,
            }
            .filter(|value| value.is_finite())
            .ok_or_else(invalid)?;
            serde_json::Number::from_f64(value)
                .map(Value::Number)
                .ok_or_else(invalid)
        }
        ApplicationRouteParameterTypeV1::Decimal => {
            let value = match value {
                Value::String(value) => value.clone(),
                Value::Number(value) => value.to_string(),
                _ => return Err(invalid()),
            };
            let value = value.parse::<Decimal>().map_err(|_| invalid())?;
            if value.scale() > 28 {
                return Err(invalid());
            }
            Ok(Value::String(value.to_string()))
        }
        ApplicationRouteParameterTypeV1::Bool => {
            let value = match value {
                Value::Bool(value) => Some(*value),
                Value::String(value) if value == "true" => Some(true),
                Value::String(value) if value == "false" => Some(false),
                _ => None,
            }
            .ok_or_else(invalid)?;
            Ok(Value::Bool(value))
        }
        ApplicationRouteParameterTypeV1::Json => match value {
            Value::String(value) => serde_json::from_str(value).map_err(|_| invalid()),
            value => Ok(value.clone()),
        },
        ApplicationRouteParameterTypeV1::Timestamp => {
            let value = value.as_str().ok_or_else(invalid)?;
            let value = parse_flexible_utc_timestamp(value).ok_or_else(invalid)?;
            Ok(Value::String(
                value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            ))
        }
        ApplicationRouteParameterTypeV1::Date => {
            let value = value.as_str().ok_or_else(invalid)?;
            let value =
                chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| invalid())?;
            Ok(Value::String(value.format("%Y-%m-%d").to_string()))
        }
        ApplicationRouteParameterTypeV1::LocalDateTime => {
            let value = value.as_str().ok_or_else(invalid)?;
            let value = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .map_err(|_| invalid())?;
            Ok(Value::String(
                value.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
            ))
        }
        ApplicationRouteParameterTypeV1::TimeZone => {
            let value = value.as_str().ok_or_else(invalid)?;
            let value = value.parse::<chrono_tz::Tz>().map_err(|_| invalid())?;
            Ok(Value::String(value.to_string()))
        }
        ApplicationRouteParameterTypeV1::Uuid => {
            let value = value.as_str().ok_or_else(invalid)?;
            let value = Uuid::parse_str(value).map_err(|_| invalid())?;
            Ok(Value::String(value.to_string()))
        }
        ApplicationRouteParameterTypeV1::Enum { values } => {
            let value = value.as_str().ok_or_else(invalid)?;
            if !values.contains(value) {
                return Err(invalid());
            }
            Ok(Value::String(value.to_string()))
        }
        ApplicationRouteParameterTypeV1::List { element } => {
            let values = match value {
                Value::Array(values) => values.clone(),
                value => vec![value.clone()],
            };
            values
                .iter()
                .map(|value| coerce_carrier_parameter_value(value, element, location, name))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array)
        }
        ApplicationRouteParameterTypeV1::Set { element } => {
            let values = match value {
                Value::Array(values) => values.clone(),
                value => vec![value.clone()],
            };
            let mut output = Vec::with_capacity(values.len());
            for value in values {
                let value = coerce_carrier_parameter_value(&value, element, location, name)?;
                if !output.contains(&value) {
                    output.push(value);
                }
            }
            Ok(Value::Array(output))
        }
        ApplicationRouteParameterTypeV1::Optional { value: inner } => {
            if value.is_null() {
                Ok(Value::Null)
            } else {
                coerce_carrier_parameter_value(value, inner, location, name)
            }
        }
        ApplicationRouteParameterTypeV1::Object { .. }
        | ApplicationRouteParameterTypeV1::Map { .. }
        | ApplicationRouteParameterTypeV1::Vector { .. }
        | ApplicationRouteParameterTypeV1::Point
        | ApplicationRouteParameterTypeV1::LineString
        | ApplicationRouteParameterTypeV1::Polygon => {
            let value = match value {
                Value::String(value) => serde_json::from_str(value).map_err(|_| invalid())?,
                value => value.clone(),
            };
            normalize_carrier_json_value(&value, value_type, &format!("{location}.{name}"))
        }
        ApplicationRouteParameterTypeV1::Null => {
            if value.is_null() {
                Ok(Value::Null)
            } else {
                Err(invalid())
            }
        }
    }
}

/// A timestamp in any of the shapes the platform writes: RFC 3339, SQL text
/// (space separator, `+00` offsets — what `now()` stores), or a naive
/// date-time read as UTC.
fn parse_flexible_utc_timestamp(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(parsed.with_timezone(&chrono::Utc));
    }
    let mut candidate = value.trim().replacen(' ', "T", 1);
    if let Some(prefix) = candidate
        .strip_suffix("+00")
        .or_else(|| candidate.strip_suffix("-00"))
    {
        candidate = format!("{prefix}+00:00");
    }
    if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(&candidate) {
        return Some(parsed.with_timezone(&chrono::Utc));
    }
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(
        candidate.trim_end_matches('Z'),
        "%Y-%m-%dT%H:%M:%S%.f",
    ) {
        return Some(naive.and_utc());
    }
    // A bare date coerces to midnight UTC, as SQL coerces date into a
    // timestamp column.
    chrono::NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|naive| naive.and_utc())
}

pub(crate) fn normalize_carrier_json_value(
    value: &Value,
    value_type: &ApplicationRouteParameterTypeV1,
    path: &str,
) -> Result<Value> {
    // A `$bicdb_typed` storage envelope (a typed SQL value written by raw
    // SQL) normalizes from its canonical text form.
    if let Some(text) = value
        .as_object()
        .and_then(|object| object.get("$bicdb_typed"))
        .and_then(|typed| typed.get("text"))
        .and_then(Value::as_str)
    {
        return normalize_carrier_json_value(&Value::String(text.to_string()), value_type, path);
    }
    let invalid = || {
        AppRuntimeError::InvalidRequest(format!(
            "BicDB application request value `{path}` does not match its signed type; got {value}"
        ))
    };
    match value_type {
        ApplicationRouteParameterTypeV1::String => value
            .as_str()
            .map(|value| Value::String(value.to_string()))
            .ok_or_else(invalid),
        ApplicationRouteParameterTypeV1::Int => value
            .as_i64()
            .map(|value| Value::Number(value.into()))
            .ok_or_else(invalid),
        ApplicationRouteParameterTypeV1::Float => value
            .as_f64()
            .filter(|value| value.is_finite())
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or_else(invalid),
        ApplicationRouteParameterTypeV1::Decimal => {
            let value = match value {
                Value::String(value) => value.clone(),
                Value::Number(value) => value.to_string(),
                _ => return Err(invalid()),
            };
            let value = value.parse::<Decimal>().map_err(|_| invalid())?;
            Ok(Value::String(value.to_string()))
        }
        ApplicationRouteParameterTypeV1::Bool => {
            value.as_bool().map(Value::Bool).ok_or_else(invalid)
        }
        ApplicationRouteParameterTypeV1::Json => Ok(value.clone()),
        ApplicationRouteParameterTypeV1::Timestamp => {
            let value = value.as_str().ok_or_else(invalid)?;
            let value = parse_flexible_utc_timestamp(value).ok_or_else(invalid)?;
            Ok(Value::String(
                value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            ))
        }
        ApplicationRouteParameterTypeV1::Date => {
            let value = value.as_str().ok_or_else(invalid)?;
            let value =
                chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| invalid())?;
            Ok(Value::String(value.format("%Y-%m-%d").to_string()))
        }
        ApplicationRouteParameterTypeV1::LocalDateTime => {
            let value = value.as_str().ok_or_else(invalid)?;
            let value = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .map_err(|_| invalid())?;
            Ok(Value::String(
                value.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
            ))
        }
        ApplicationRouteParameterTypeV1::TimeZone => {
            let value = value.as_str().ok_or_else(invalid)?;
            let value = value.parse::<chrono_tz::Tz>().map_err(|_| invalid())?;
            Ok(Value::String(value.to_string()))
        }
        ApplicationRouteParameterTypeV1::Uuid => {
            let value = value.as_str().ok_or_else(invalid)?;
            let value = Uuid::parse_str(value).map_err(|_| invalid())?;
            Ok(Value::String(value.to_string()))
        }
        ApplicationRouteParameterTypeV1::Enum { values } => {
            let value = value.as_str().ok_or_else(invalid)?;
            if values.contains(value) {
                Ok(Value::String(value.to_string()))
            } else {
                Err(invalid())
            }
        }
        ApplicationRouteParameterTypeV1::List { element } => value
            .as_array()
            .ok_or_else(invalid)?
            .iter()
            .enumerate()
            .map(|(index, value)| {
                normalize_carrier_json_value(value, element, &format!("{path}[{index}]"))
            })
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        ApplicationRouteParameterTypeV1::Set { element } => {
            let values = value.as_array().ok_or_else(invalid)?;
            let mut output = Vec::with_capacity(values.len());
            for (index, value) in values.iter().enumerate() {
                let value =
                    normalize_carrier_json_value(value, element, &format!("{path}[{index}]"))?;
                if !output.contains(&value) {
                    output.push(value);
                }
            }
            Ok(Value::Array(output))
        }
        ApplicationRouteParameterTypeV1::Optional { value: inner } => {
            if value.is_null() {
                Ok(Value::Null)
            } else {
                normalize_carrier_json_value(value, inner, path)
            }
        }
        ApplicationRouteParameterTypeV1::Object { fields } => {
            let object = value.as_object().ok_or_else(invalid)?;
            let mut output = serde_json::Map::new();
            for field in fields {
                let field_value = match object.get(&field.name) {
                    Some(value) => value.clone(),
                    None => match field.default_json.as_deref() {
                        Some(default) => serde_json::from_str(default).map_err(|error| {
                            AppRuntimeError::InvalidPackage(format!(
                                "BicDB application body field `{}` has invalid signed default JSON: {error}",
                                field.name
                            ))
                        })?,
                        None if field.optional => Value::Null,
                        None => {
                            return Err(AppRuntimeError::InvalidRequest(format!(
                                "missing required BicDB application body field `{path}.{}`",
                                field.name
                            )));
                        }
                    },
                };
                let field_path = format!("{path}.{}", field.name);
                let field_value = if field.optional && field_value.is_null() {
                    Value::Null
                } else {
                    normalize_carrier_json_value(&field_value, &field.value_type, &field_path)?
                };
                validate_carrier_parameter(&field_value, field, &field_path)?;
                output.insert(field.name.clone(), field_value);
            }
            Ok(Value::Object(output))
        }
        ApplicationRouteParameterTypeV1::Map { key, value: inner } => {
            let entries = value.as_array().ok_or_else(invalid)?;
            let mut output: Vec<Value> = Vec::with_capacity(entries.len());
            for (index, entry) in entries.iter().enumerate() {
                let entry = entry.as_object().ok_or_else(invalid)?;
                let entry_key = entry.get("key").ok_or_else(invalid)?;
                let entry_value = entry.get("value").ok_or_else(invalid)?;
                let entry_key =
                    normalize_carrier_json_value(entry_key, key, &format!("{path}[{index}].key"))?;
                let entry_value = normalize_carrier_json_value(
                    entry_value,
                    inner,
                    &format!("{path}[{index}].value"),
                )?;
                let normalized = json!({"key": entry_key, "value": entry_value});
                if let Some(existing) = output
                    .iter_mut()
                    .find(|existing| existing.get("key") == normalized.get("key"))
                {
                    *existing = normalized;
                } else {
                    output.push(normalized);
                }
            }
            Ok(Value::Array(output))
        }
        ApplicationRouteParameterTypeV1::Vector { dimensions } => {
            let values = value.as_array().ok_or_else(invalid)?;
            if values.len() != *dimensions {
                return Err(invalid());
            }
            values
                .iter()
                .map(|value| {
                    value
                        .as_f64()
                        .filter(|value| value.is_finite())
                        .and_then(serde_json::Number::from_f64)
                        .map(Value::Number)
                        .ok_or_else(invalid)
                })
                .collect::<Result<Vec<_>>>()
                .map(Value::Array)
        }
        ApplicationRouteParameterTypeV1::Point => normalize_carrier_geometry(value, "Point", path),
        ApplicationRouteParameterTypeV1::LineString => {
            normalize_carrier_geometry(value, "LineString", path)
        }
        ApplicationRouteParameterTypeV1::Polygon => {
            normalize_carrier_geometry(value, "Polygon", path)
        }
        ApplicationRouteParameterTypeV1::Null => {
            if value.is_null() {
                Ok(Value::Null)
            } else {
                Err(invalid())
            }
        }
    }
}

fn normalize_carrier_geometry(value: &Value, expected: &str, path: &str) -> Result<Value> {
    let geometry = bicdb_core::Geometry::from_geojson_value(value.clone()).map_err(|_| {
        AppRuntimeError::InvalidRequest(format!(
            "BicDB application request value `{path}` is not a valid GeoJSON {expected}"
        ))
    })?;
    let valid = matches!(
        (&geometry, expected),
        (bicdb_core::Geometry::Point(_), "Point")
            | (bicdb_core::Geometry::LineString(_), "LineString")
            | (bicdb_core::Geometry::Polygon(_), "Polygon")
    );
    if !valid {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "BicDB application request value `{path}` is not a GeoJSON {expected}"
        )));
    }
    Ok(geometry.to_geojson_value())
}

fn normalize_carrier_response(
    response: &mut ApplicationHttpResponse,
    value_type: &ApplicationRouteParameterTypeV1,
    route: &str,
) -> Result<()> {
    let HttpResponseBodyV2::Json(value) = &response.body else {
        return Err(AppRuntimeError::Provider(format!(
            "BicDB application route `{route}` returned a non-JSON body for its signed response type"
        )));
    };
    let value = normalize_carrier_json_value(value, value_type, "response").map_err(|error| {
        AppRuntimeError::Provider(format!(
            "BicDB application route `{route}` violated its signed response type: {error}"
        ))
    })?;
    response.body = HttpResponseBodyV2::Json(value);
    Ok(())
}

fn validate_carrier_parameter(
    value: &Value,
    parameter: &ApplicationRouteParameterV1,
    location: &str,
) -> Result<()> {
    if value.is_null() && parameter.optional {
        return Ok(());
    }
    for validation in &parameter.validations {
        let valid = match validation {
            ApplicationRouteParameterValidationV1::Email => value.as_str().is_some_and(|value| {
                static EMAIL: OnceLock<regex::Regex> = OnceLock::new();
                EMAIL
                    .get_or_init(|| {
                        regex::Regex::new(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
                            .expect("BicDB application email regex compiles")
                    })
                    .is_match(value)
            }),
            ApplicationRouteParameterValidationV1::Length { min, max } => {
                let length = value
                    .as_str()
                    .map(|value| value.chars().count())
                    .or_else(|| value.as_array().map(Vec::len));
                length.is_some_and(|length| {
                    min.is_none_or(|minimum| length >= minimum)
                        && max.is_none_or(|maximum| length <= maximum)
                })
            }
            ApplicationRouteParameterValidationV1::Range { minimum, maximum } => {
                let numeric = value
                    .as_f64()
                    .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()));
                numeric.is_some_and(|numeric| {
                    minimum
                        .as_deref()
                        .and_then(|value| value.parse::<f64>().ok())
                        .is_none_or(|minimum| numeric >= minimum)
                        && maximum
                            .as_deref()
                            .and_then(|value| value.parse::<f64>().ok())
                            .is_none_or(|maximum| numeric <= maximum)
                })
            }
            ApplicationRouteParameterValidationV1::Pattern { expression } => {
                value.as_str().is_some_and(|value| {
                    regex::Regex::new(expression).is_ok_and(|re| re.is_match(value))
                })
            }
        };
        if !valid {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "BicDB application {location} parameter `{}` violates its signed validation rule",
                parameter.name
            )));
        }
    }
    Ok(())
}

fn carrier_callable_http_arguments(
    route: &str,
    parameters: &[String],
    input: Option<&Value>,
) -> Result<Vec<(Option<String>, Value)>> {
    if parameters.is_empty() {
        return Ok(Vec::new());
    }
    let object = input.and_then(Value::as_object).ok_or_else(|| {
        AppRuntimeError::InvalidRequest(format!(
            "BicDB application callable route `{route}` requires a JSON object body"
        ))
    })?;
    Ok(parameters
        .iter()
        .map(|parameter| {
            (
                Some(parameter.clone()),
                object.get(parameter).cloned().unwrap_or(Value::Null),
            )
        })
        .collect())
}

fn carrier_route_globals(
    request: &ApplicationHttpRequest,
    path_parameters: &BTreeMap<String, String>,
    contract: Option<&ApplicationRouteRequestV1>,
    actor: &bicdb_extension::abi_v2::ActorContext,
    request_id: &str,
) -> Result<BTreeMap<String, Value>> {
    let raw_params = path_parameters
        .iter()
        .map(|(name, value)| (name.clone(), Value::String(value.clone())))
        .collect::<serde_json::Map<_, _>>();
    let mut raw_query = serde_json::Map::new();
    for (name, value) in &request.query {
        match raw_query.remove(name) {
            None => {
                raw_query.insert(name.clone(), Value::String(value.clone()));
            }
            Some(Value::Array(mut values)) => {
                values.push(Value::String(value.clone()));
                raw_query.insert(name.clone(), Value::Array(values));
            }
            Some(previous) => {
                raw_query.insert(
                    name.clone(),
                    Value::Array(vec![previous, Value::String(value.clone())]),
                );
            }
        }
    }
    let (params, query) = if let Some(contract) = contract {
        (
            coerce_carrier_parameters(&contract.path_parameters, &raw_params, "path")?,
            coerce_carrier_parameters(&contract.query_parameters, &raw_query, "query")?,
        )
    } else {
        (raw_params, raw_query)
    };
    let headers = request
        .headers
        .iter()
        .filter(|(name, _)| !sensitive_request_header(name))
        .map(|(name, value)| (name.to_ascii_lowercase(), Value::String(value.clone())))
        .collect::<serde_json::Map<_, _>>();
    // Authentication state is represented only by the verified ActorContext.
    // Raw cookie values are ambient credentials and are never copied into a
    // guest global, even for non-cell application hosts.
    let cookies = serde_json::Map::new();
    let input = match &request.body {
        HttpRequestBodyV2::Empty => Value::Null,
        HttpRequestBodyV2::Json(value) => value.clone(),
        body => serde_json::to_value(body)?,
    };
    let input = match contract.and_then(|contract| contract.body.as_ref()) {
        Some(value_type) => normalize_carrier_json_value(&input, value_type, "input")?,
        None => input,
    };
    Ok(BTreeMap::from([
        ("params".to_string(), Value::Object(params)),
        ("query".to_string(), Value::Object(query)),
        ("headers".to_string(), Value::Object(headers)),
        ("cookies".to_string(), Value::Object(cookies)),
        ("input".to_string(), input),
        ("actor".to_string(), serde_json::to_value(actor)?),
        (
            "request_id".to_string(),
            Value::String(request_id.to_string()),
        ),
    ]))
}

fn http_router(state: HttpState) -> Router {
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/openapi/:application", get(openapi))
        .fallback(any(dispatch_axum))
        .layer(axum::middleware::from_fn_with_state(
            state.policy.admission_check.clone(),
            enforce_http_admission,
        ))
        .with_state(state)
}

async fn enforce_http_admission(
    State(check): State<Option<Arc<dyn HttpAdmissionCheck>>>,
    request: Request<Body>,
    next: axum::middleware::Next,
) -> Response<Body> {
    if check.as_ref().is_some_and(|check| !check.is_admitted()) {
        return admission_expired_response();
    }
    next.run(request).await
}

fn admission_expired_response() -> Response<Body> {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [("content-type", "application/problem+json")],
        json!({"code":"admission_expired","message":"Deployment admission is no longer valid"})
            .to_string(),
    )
        .into_response()
}

async fn livez() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn readyz(State(state): State<HttpState>) -> impl IntoResponse {
    let readiness = state.runtime.readiness();
    let status = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        [("content-type", "application/json")],
        serde_json::to_string(&readiness).unwrap_or_else(|_| "{}".to_string()),
    )
}

async fn openapi(
    State(state): State<HttpState>,
    Path(application): Path<String>,
) -> impl IntoResponse {
    match state.runtime.openapi(&application) {
        Ok(document) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            serde_json::to_string(&document).unwrap_or_else(|_| "{}".to_string()),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

async fn dispatch_axum(
    State(state): State<HttpState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    websocket: Option<WebSocketUpgrade>,
    request: Request<Body>,
) -> Response<Body> {
    let error_request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| {
            !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
        })
        .map(str::to_string);
    let permit = match state.admission.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [
                    ("content-type", "application/problem+json"),
                    ("retry-after", "1"),
                ],
                json!({"code":"overloaded","message":"HTTP admission limit is busy"}).to_string(),
            )
                .into_response();
        }
    };
    let result: Result<Response<Body>> = match lower_axum_request(&state.policy, peer, request)
        .await
    {
        Ok(mut request) => {
            // A slow body may cross the deadline after the middleware check.
            if state
                .policy
                .admission_check
                .as_ref()
                .is_some_and(|check| !check.is_admitted())
            {
                return admission_expired_response();
            }
            let request_id = header(&request.headers, "x-request-id")
                .unwrap_or_else(|| Uuid::new_v4().to_string());
            if header(&request.headers, "x-request-id").is_none() {
                request
                    .headers
                    .push(("x-request-id".to_string(), request_id.clone()));
            }
            let live_scope =
                header(&request.headers, "x-correlation-id").unwrap_or_else(|| request_id.clone());
            if !admit_rate(&state, &request) {
                Err(AppRuntimeError::RateLimited(
                    "HTTP rate limit exceeded".to_string(),
                ))
            } else {
                let trusted_response = match state
                    .trusted_handler
                    .as_ref()
                    .map(|handler| handler.handle(&request))
                    .transpose()
                {
                    Ok(response) => response.flatten(),
                    Err(error) => {
                        drop(permit);
                        return error_response_with_request_id(error, error_request_id.as_deref());
                    }
                };
                if let Some(response) = trusted_response {
                    Ok(axum_response_trusted(&state, response))
                } else {
                    match state.runtime.prepare_carrier_realtime(
                        &state.authenticator,
                        &state.policy,
                        &request,
                    ) {
                        Ok(Some(session)) => {
                            carrier_realtime_axum_response(&state, session, websocket).await
                        }
                        Ok(None) if is_live_streaming_request(&state.runtime, &request) => {
                            match state.runtime.register_live_realtime_response(&live_scope) {
                                Err(error) => Err(error),
                                Ok(capture) => {
                                    let runtime = state.runtime.clone();
                                    let monitor_runtime = state.runtime.clone();
                                    let authenticator = state.authenticator.clone();
                                    let policy = state.policy.clone();
                                    let monitor_scope = live_scope.clone();
                                    let timeout =
                                        std::time::Duration::from_millis(policy.request_timeout_ms);
                                    let task = tokio::task::spawn_blocking(move || {
                                        runtime.dispatch_http_v2(&authenticator, &policy, request)
                                    });
                                    tokio::spawn(async move {
                                        match task.await {
                                            Ok(Ok(response))
                                                if matches!(
                                                    response.body,
                                                    HttpResponseBodyV2::Sse(_)
                                                ) => {}
                                            Ok(Ok(_)) => monitor_runtime
                                                .cancel_live_realtime_response(
                                                &monitor_scope,
                                                AppRuntimeError::Invocation(
                                                    "streaming route returned a non-SSE response"
                                                        .to_string(),
                                                ),
                                            ),
                                            Ok(Err(error)) => monitor_runtime
                                                .cancel_live_realtime_response(
                                                    &monitor_scope,
                                                    error,
                                                ),
                                            Err(error) => monitor_runtime
                                                .cancel_live_realtime_response(
                                                    &monitor_scope,
                                                    AppRuntimeError::Provider(format!(
                                                        "application request task failed: {error}"
                                                    )),
                                                ),
                                        }
                                    });
                                    match tokio::time::timeout(timeout, capture).await {
                                        Ok(Ok(Ok(response))) => {
                                            live_realtime_axum_response(response)
                                        }
                                        Ok(Ok(Err(error))) => Err(error),
                                        Ok(Err(_)) => Err(AppRuntimeError::Provider(
                                            "live HTTP stream capture closed before response"
                                                .to_string(),
                                        )),
                                        Err(_) => {
                                            state.runtime.cancel_live_realtime_response(
                                                &live_scope,
                                                AppRuntimeError::Timeout(
                                                    "HTTP streaming response deadline exceeded"
                                                        .to_string(),
                                                ),
                                            );
                                            Err(AppRuntimeError::Timeout(
                                                "HTTP streaming response deadline exceeded"
                                                    .to_string(),
                                            ))
                                        }
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            let runtime = state.runtime.clone();
                            let authenticator = state.authenticator.clone();
                            let policy = state.policy.clone();
                            let timeout =
                                std::time::Duration::from_millis(policy.request_timeout_ms);
                            match tokio::time::timeout(
                                timeout,
                                tokio::task::spawn_blocking(move || {
                                    runtime.dispatch_http_v2(&authenticator, &policy, request)
                                }),
                            )
                            .await
                            {
                                Ok(Ok(result)) => {
                                    result.map(|response| axum_response(&state, response))
                                }
                                Ok(Err(error)) => Err(AppRuntimeError::Provider(format!(
                                    "application request task failed: {error}"
                                ))),
                                Err(_) => Err(AppRuntimeError::Timeout(
                                    "HTTP request deadline exceeded".to_string(),
                                )),
                            }
                        }
                        Err(error) => Err(error),
                    }
                }
            }
        }
        Err(error) => Err(error),
    };
    drop(permit);
    match result {
        Ok(response) => response,
        Err(AppRuntimeError::RateLimited(message)) => (
            StatusCode::TOO_MANY_REQUESTS,
            [
                ("content-type", "application/problem+json"),
                ("retry-after", "60"),
            ],
            json!({"code":"rate_limited","message":message}).to_string(),
        )
            .into_response(),
        Err(error) => error_response_with_request_id(error, error_request_id.as_deref()),
    }
}

fn is_live_streaming_request(
    runtime: &ApplicationRuntime,
    request: &ApplicationHttpRequest,
) -> bool {
    runtime.active_snapshots().iter().any(|snapshot| {
        snapshot
            .package
            .manifest
            .application
            .as_deref()
            .is_some_and(|application| {
                most_specific_matching_route(&application.routes, &request.method, &request.path)
                    .is_some_and(|(route, _)| route.streaming_response)
            })
    })
}

fn live_realtime_axum_response(response: crate::LiveRealtimeResponse) -> Result<Response<Body>> {
    let kind = response.kind;
    let stream = ReceiverStream::new(response.frames).map(|frame| match frame {
        Ok(crate::LiveRealtimeFrame::Data(bytes)) => {
            Ok::<_, std::io::Error>(Frame::data(Bytes::from(bytes)))
        }
        Ok(crate::LiveRealtimeFrame::Trailers(trailers)) => {
            let mut headers = axum::http::HeaderMap::new();
            for (name, value) in trailers {
                let name = HeaderName::try_from(name.as_str())
                    .map_err(|_| std::io::Error::other("live stream trailer name is invalid"))?;
                let value = HeaderValue::try_from(value.as_str())
                    .map_err(|_| std::io::Error::other("live stream trailer value is invalid"))?;
                headers.append(name, value);
            }
            Ok(Frame::trailers(headers))
        }
        Err(error) => Err(std::io::Error::other(error.to_string())),
    });
    let mut output = Response::new(Body::new(StreamBody::new(stream)));
    *output.status_mut() =
        StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    for (name, value) in response.headers {
        let name = HeaderName::try_from(name.as_str()).map_err(|_| {
            AppRuntimeError::InvalidRequest("live stream header name is invalid".to_string())
        })?;
        if host_controlled_response_header(&name) {
            return Err(AppRuntimeError::InvalidRequest(
                "live stream contains a host-controlled response header".to_string(),
            ));
        }
        let value = HeaderValue::try_from(value.as_str()).map_err(|_| {
            AppRuntimeError::InvalidRequest("live stream header value is invalid".to_string())
        })?;
        output.headers_mut().append(name, value);
    }
    if kind == StreamKind::ServerSentEvents {
        output.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream; charset=utf-8"),
        );
        output.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache"),
        );
        output.headers_mut().insert(
            HeaderName::from_static("x-accel-buffering"),
            HeaderValue::from_static("no"),
        );
    }
    Ok(output)
}

async fn carrier_realtime_axum_response(
    state: &HttpState,
    session: ApplicationRealtimeSession,
    websocket: Option<WebSocketUpgrade>,
) -> Result<Response<Body>> {
    match session.transport {
        ApplicationRealtimeTransport::Negotiate => {
            let selected = match session.requested_transport.as_deref() {
                None | Some("websocket") | Some("ws") => "websocket",
                Some("sse") | Some("server_sent_events") => "sse",
                Some("long_polling") | Some("poll") => "long_polling",
                Some(value) => {
                    return Err(AppRuntimeError::InvalidRequest(format!(
                        "unsupported realtime transport `{value}`"
                    )));
                }
            };
            let body = json!({
                "stream": session.contract.name,
                "connection_id": Uuid::new_v4().to_string(),
                "selected_transport": selected,
                "available_transports": ["websocket", "sse", "long_polling"],
                "heartbeat_seconds": 20,
                "last_sequence": state
                    .runtime
                    .realtime_queue_last_sequence(&session.contract.queue),
                "endpoints": {
                    "websocket": format!("{}/ws", session.contract.path),
                    "sse": format!("{}/sse", session.contract.path),
                    "long_polling": format!("{}/poll", session.contract.path),
                }
            });
            Ok(carrier_realtime_json_response(state, &session, body))
        }
        ApplicationRealtimeTransport::LongPolling => {
            let deadline =
                tokio::time::Instant::now() + std::time::Duration::from_millis(session.wait_ms);
            let mut batch = state
                .runtime
                .carrier_realtime_batch(&session, session.cursor, 64)?;
            while batch.items.is_empty() && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                state.runtime.revalidate_carrier_realtime(&session)?;
                batch = state
                    .runtime
                    .carrier_realtime_batch(&session, batch.next_cursor, 64)?;
            }
            state.runtime.revalidate_carrier_realtime(&session)?;
            let timed_out = batch.items.is_empty();
            let body = json!({
                "stream": session.contract.name,
                "items": batch.items,
                "next_cursor": batch.next_cursor,
                "timed_out": timed_out,
            });
            Ok(carrier_realtime_json_response(state, &session, body))
        }
        ApplicationRealtimeTransport::ServerSentEvents => {
            let (sender, receiver) = mpsc::channel::<std::result::Result<Bytes, Infallible>>(16);
            let runtime = state.runtime.clone();
            let stream_session = session.clone();
            tokio::spawn(async move {
                let mut cursor = stream_session.cursor;
                let mut emitted_bytes = 0usize;
                let mut last_keepalive = tokio::time::Instant::now();
                let mut last_authorization = tokio::time::Instant::now();
                loop {
                    if last_authorization.elapsed() >= std::time::Duration::from_secs(5) {
                        if runtime
                            .revalidate_carrier_realtime(&stream_session)
                            .is_err()
                        {
                            break;
                        }
                        last_authorization = tokio::time::Instant::now();
                    }
                    let batch = match runtime.carrier_realtime_batch(&stream_session, cursor, 64) {
                        Ok(batch) => batch,
                        Err(_) => break,
                    };
                    cursor = batch.next_cursor;
                    if batch.items.is_empty() {
                        if last_keepalive.elapsed() >= std::time::Duration::from_secs(20) {
                            let keepalive = Bytes::from_static(b": keepalive\n\n");
                            emitted_bytes = emitted_bytes.saturating_add(keepalive.len());
                            if emitted_bytes > stream_session.max_response_bytes
                                || sender.send(Ok(keepalive)).await.is_err()
                            {
                                break;
                            }
                            last_keepalive = tokio::time::Instant::now();
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        continue;
                    }
                    for item in batch.items {
                        let event = item["event"].as_str().unwrap_or("message");
                        let sequence = item["sequence"].as_u64().unwrap_or(cursor);
                        let encoded = format!(
                            "id: {sequence}\nevent: {event}\ndata: {}\n\n",
                            serde_json::to_string(&item).unwrap_or_else(|_| "null".to_string())
                        );
                        emitted_bytes = emitted_bytes.saturating_add(encoded.len());
                        if emitted_bytes > stream_session.max_response_bytes
                            || sender.send(Ok(Bytes::from(encoded))).await.is_err()
                        {
                            return;
                        }
                    }
                }
            });
            let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("text/event-stream; charset=utf-8"),
            );
            response.headers_mut().insert(
                axum::http::header::CACHE_CONTROL,
                HeaderValue::from_static("no-cache"),
            );
            apply_carrier_realtime_headers(state, &session, &mut response)?;
            Ok(response)
        }
        ApplicationRealtimeTransport::WebSocket => {
            let websocket = websocket.ok_or_else(|| {
                AppRuntimeError::InvalidRequest(
                    "realtime WebSocket route requires an Upgrade request".to_string(),
                )
            })?;
            let runtime = state.runtime.clone();
            let socket_session = session.clone();
            let mut response = websocket
                .on_upgrade(move |socket| {
                    run_carrier_realtime_websocket(runtime, socket_session, socket)
                })
                .into_response();
            apply_carrier_realtime_headers(state, &session, &mut response)?;
            Ok(response)
        }
    }
}

async fn run_carrier_realtime_websocket(
    runtime: Arc<ApplicationRuntime>,
    session: ApplicationRealtimeSession,
    mut socket: WebSocket,
) {
    let mut cursor = session.cursor;
    let mut received_bytes = 0usize;
    let mut emitted_bytes = 0usize;
    let mut last_keepalive = tokio::time::Instant::now();
    let mut last_authorization = tokio::time::Instant::now();
    loop {
        if last_authorization.elapsed() >= std::time::Duration::from_secs(5) {
            if runtime.revalidate_carrier_realtime(&session).is_err() {
                let _ = socket.send(Message::Close(None)).await;
                break;
            }
            last_authorization = tokio::time::Instant::now();
        }
        let batch = match runtime.carrier_realtime_batch(&session, cursor, 64) {
            Ok(batch) => batch,
            Err(_) => break,
        };
        cursor = batch.next_cursor;
        for item in batch.items {
            let encoded = serde_json::to_string(&item).unwrap_or_else(|_| "null".to_string());
            emitted_bytes = emitted_bytes.saturating_add(encoded.len());
            if emitted_bytes > session.max_response_bytes
                || socket.send(Message::Text(encoded)).await.is_err()
            {
                return;
            }
        }
        if last_keepalive.elapsed() >= std::time::Duration::from_secs(20) {
            if socket.send(Message::Ping(Vec::new())).await.is_err() {
                break;
            }
            last_keepalive = tokio::time::Instant::now();
        }
        match tokio::time::timeout(std::time::Duration::from_millis(200), socket.recv()).await {
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => break,
            Ok(Some(Ok(Message::Ping(payload)))) => {
                if socket.send(Message::Pong(payload)).await.is_err() {
                    break;
                }
            }
            Ok(Some(Ok(Message::Text(value)))) => {
                received_bytes = received_bytes.saturating_add(value.len());
                if received_bytes > session.max_request_bytes {
                    let _ = socket.send(Message::Close(None)).await;
                    break;
                }
            }
            Ok(Some(Ok(Message::Binary(value)))) => {
                received_bytes = received_bytes.saturating_add(value.len());
                if received_bytes > session.max_request_bytes {
                    let _ = socket.send(Message::Close(None)).await;
                    break;
                }
            }
            Ok(Some(Ok(Message::Pong(_)))) | Err(_) => {}
            Ok(Some(Err(_))) => break,
        }
    }
}

fn carrier_realtime_json_response(
    state: &HttpState,
    session: &ApplicationRealtimeSession,
    body: Value,
) -> Response<Body> {
    let mut response = ApplicationHttpResponse {
        status: 200,
        headers: Vec::new(),
        body: HttpResponseBodyV2::Json(body),
        trailers: Vec::new(),
        retry_after_ms: None,
    };
    apply_signed_response_headers(&mut response, &session.response_headers);
    response = with_http_policy_headers(response, session.origin.as_deref(), &state.policy);
    axum_response(state, response)
}

fn apply_carrier_realtime_headers(
    state: &HttpState,
    session: &ApplicationRealtimeSession,
    response: &mut Response<Body>,
) -> Result<()> {
    let mut headers = ApplicationHttpResponse {
        status: response.status().as_u16(),
        headers: Vec::new(),
        body: HttpResponseBodyV2::Empty,
        trailers: Vec::new(),
        retry_after_ms: None,
    };
    apply_signed_response_headers(&mut headers, &session.response_headers);
    headers = with_http_policy_headers(headers, session.origin.as_deref(), &state.policy);
    for (name, value) in headers.headers {
        let name = HeaderName::try_from(name.as_str()).map_err(|_| {
            AppRuntimeError::InvalidPackage(
                "signed realtime response contains an invalid header".to_string(),
            )
        })?;
        if host_controlled_response_header(&name) {
            return Err(AppRuntimeError::InvalidPackage(
                "signed realtime response contains a host-controlled header".to_string(),
            ));
        }
        let value = HeaderValue::try_from(value.as_str()).map_err(|_| {
            AppRuntimeError::InvalidPackage(
                "signed realtime response contains an invalid header value".to_string(),
            )
        })?;
        response.headers_mut().insert(name, value);
    }
    Ok(())
}

fn admit_rate(state: &HttpState, request: &ApplicationHttpRequest) -> bool {
    let identity = header(&request.headers, "authorization")
        .or_else(|| request.peer_address.clone())
        .unwrap_or_else(|| "anonymous".to_string());
    let key = Sha256::digest(identity.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let minute = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() / 60)
        .unwrap_or_default();
    let mut windows = state
        .rate_windows
        .lock()
        .expect("HTTP rate windows poisoned");
    // Old windows are boundedly reaped so attacker-controlled identities
    // cannot grow this table without limit.
    if windows.len() > 100_000 {
        windows.retain(|_, window| window.minute >= minute.saturating_sub(1));
    }
    let window = windows.entry(key).or_insert(RateWindow {
        minute,
        requests: 0,
    });
    if window.minute != minute {
        *window = RateWindow {
            minute,
            requests: 0,
        };
    }
    if window.requests >= state.policy.rate_limit_per_minute {
        return false;
    }
    window.requests += 1;
    true
}

async fn lower_axum_request(
    policy: &HttpHostPolicy,
    peer: SocketAddr,
    request: Request<Body>,
) -> Result<ApplicationHttpRequest> {
    let (parts, body) = request.into_parts();
    let method = parse_method(parts.method.as_str())?;
    let headers = parts
        .headers
        .iter()
        .map(|(name, value)| {
            Ok((
                name.as_str().to_string(),
                value
                    .to_str()
                    .map_err(|_| {
                        AppRuntimeError::Invocation(
                            "HTTP header contains non-text bytes".to_string(),
                        )
                    })?
                    .to_string(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let header_bytes = headers
        .iter()
        .map(|(name, value)| name.len().saturating_add(value.len()))
        .sum::<usize>();
    if headers.len() > 256 || header_bytes > 64 * 1024 {
        return Err(AppRuntimeError::ResourceExhausted(
            "HTTP request headers exceed host limits".to_string(),
        ));
    }
    let trusted_peer = policy.trusted_proxies.contains(&peer.ip());
    let peer_address = if trusted_peer {
        trusted_client_address(&headers, peer, &policy.trusted_proxies)?
    } else {
        Some(peer.to_string())
    };
    let bytes = to_bytes(body, policy.max_request_bytes)
        .await
        .map_err(|error| AppRuntimeError::Invocation(error.to_string()))?;
    let content_type = header(&headers, "content-type").unwrap_or_default();
    let body = if bytes.is_empty() {
        HttpRequestBodyV2::Empty
    } else if content_type.starts_with("application/json") {
        HttpRequestBodyV2::Json(serde_json::from_slice(&bytes)?)
    } else if content_type.starts_with("application/x-www-form-urlencoded") {
        HttpRequestBodyV2::Form(parse_urlencoded(std::str::from_utf8(&bytes).map_err(
            |error| AppRuntimeError::Invocation(format!("form body is not UTF-8: {error}")),
        )?)?)
    } else if content_type.starts_with("multipart/form-data") {
        HttpRequestBodyV2::Multipart(parse_multipart(&content_type, &bytes)?)
    } else {
        HttpRequestBodyV2::Binary(bytes.to_vec())
    };
    Ok(ApplicationHttpRequest {
        method,
        path: parts.uri.path().to_string(),
        query: parts
            .uri
            .query()
            .map(parse_urlencoded)
            .transpose()?
            .unwrap_or_default(),
        headers,
        body,
        peer_address,
    })
}

fn trusted_client_address(
    headers: &[(String, String)],
    peer: SocketAddr,
    trusted_proxies: &BTreeSet<IpAddr>,
) -> Result<Option<String>> {
    let Some(forwarded) = header(headers, "x-forwarded-for") else {
        return Ok(Some(peer.to_string()));
    };
    let mut hops = forwarded
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value.parse::<IpAddr>().map_err(|_| {
                AppRuntimeError::Invocation(
                    "trusted proxy supplied an invalid X-Forwarded-For chain".to_string(),
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if hops.len() > 32 {
        return Err(AppRuntimeError::Invocation(
            "X-Forwarded-For chain exceeds 32 hops".to_string(),
        ));
    }
    hops.push(peer.ip());
    Ok(hops
        .into_iter()
        .rev()
        .find(|address| !trusted_proxies.contains(address))
        .map(|address| address.to_string())
        .or_else(|| Some(peer.to_string())))
}

fn parse_multipart(
    content_type: &str,
    bytes: &[u8],
) -> Result<Vec<bicdb_extension::abi_v2::MultipartPartV2>> {
    let boundary = content_type
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix("boundary="))
        .map(|boundary| boundary.trim_matches('"'))
        .filter(|boundary| {
            !boundary.is_empty()
                && boundary.len() <= 70
                && boundary
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"'()+_,-./:=?".contains(&byte))
        })
        .ok_or_else(|| {
            AppRuntimeError::Invocation("multipart body has an invalid boundary".to_string())
        })?;
    let marker = format!("--{boundary}").into_bytes();
    let mut parts = Vec::new();
    for section in split_bytes(bytes, &marker).into_iter().skip(1) {
        if section.starts_with(b"--") {
            break;
        }
        let section = section
            .strip_prefix(b"\r\n")
            .unwrap_or(section)
            .strip_suffix(b"\r\n")
            .unwrap_or(section);
        if section.is_empty() {
            continue;
        }
        if parts.len() >= 256 {
            return Err(AppRuntimeError::Invocation(
                "multipart body exceeds 256 parts".to_string(),
            ));
        }
        let header_end = find_bytes(section, b"\r\n\r\n").ok_or_else(|| {
            AppRuntimeError::Invocation("multipart part lacks header terminator".to_string())
        })?;
        if header_end > 64 * 1024 {
            return Err(AppRuntimeError::Invocation(
                "multipart part headers exceed 64 KiB".to_string(),
            ));
        }
        let header_text = std::str::from_utf8(&section[..header_end]).map_err(|error| {
            AppRuntimeError::Invocation(format!("multipart headers are not UTF-8: {error}"))
        })?;
        let headers = header_text
            .split("\r\n")
            .map(|line| {
                let (name, value) = line.split_once(':').ok_or_else(|| {
                    AppRuntimeError::Invocation("invalid multipart header".to_string())
                })?;
                Ok((name.trim().to_ascii_lowercase(), value.trim().to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let disposition = header(&headers, "content-disposition").ok_or_else(|| {
            AppRuntimeError::Invocation("multipart part lacks Content-Disposition".to_string())
        })?;
        let mut name = None;
        let mut filename = None;
        for parameter in disposition.split(';').map(str::trim).skip(1) {
            if let Some(value) = parameter.strip_prefix("name=") {
                name = Some(value.trim_matches('"').to_string());
            } else if let Some(value) = parameter.strip_prefix("filename=") {
                filename = Some(value.trim_matches('"').to_string());
            }
        }
        let name = name
            .filter(|name| !name.is_empty() && name.len() <= 256)
            .ok_or_else(|| {
                AppRuntimeError::Invocation("multipart part has no valid name".to_string())
            })?;
        parts.push(bicdb_extension::abi_v2::MultipartPartV2 {
            name,
            filename,
            headers,
            body: section[header_end + 4..].to_vec(),
        });
    }
    Ok(parts)
}

fn split_bytes<'a>(bytes: &'a [u8], marker: &[u8]) -> Vec<&'a [u8]> {
    let mut output = Vec::new();
    let mut start = 0;
    while let Some(offset) = find_bytes(&bytes[start..], marker) {
        output.push(&bytes[start..start + offset]);
        start += offset + marker.len();
    }
    output.push(&bytes[start..]);
    output
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    (!needle.is_empty())
        .then(|| {
            haystack
                .windows(needle.len())
                .position(|window| window == needle)
        })
        .flatten()
}

fn lower_resource_request(
    contract: &bicdb_extension::abi_v2::ResourceContractV1,
    operation: ResourceOperation,
    path_parameters: &BTreeMap<String, String>,
    request: &ApplicationHttpRequest,
) -> Result<ResourceRequest> {
    let mut filters = Vec::new();
    let mut relation_filters = BTreeMap::new();
    let mut sort = contract
        .list_defaults
        .as_ref()
        .map(|defaults| defaults.sort.clone())
        .unwrap_or_default();
    let mut limit = contract
        .list_defaults
        .as_ref()
        .map(|defaults| defaults.limit)
        .unwrap_or(50);
    let mut offset = 0;
    let mut search = None;
    let mut direct_filter_values = BTreeMap::new();
    let mut scope = if operation == ResourceOperation::List {
        contract
            .list_defaults
            .as_ref()
            .map(|defaults| defaults.scope)
            .unwrap_or_default()
    } else {
        ResourceRecordScope::Active
    };
    for (name, raw) in &request.query {
        if contract
            .filter_contracts
            .iter()
            .any(|filter| filter.query_names().iter().any(|query| query == name))
        {
            direct_filter_values.insert(name.clone(), raw.clone());
            continue;
        }
        match name.as_str() {
            "limit" => {
                limit = raw.parse().map_err(|_| {
                    AppRuntimeError::Invocation("invalid limit parameter".to_string())
                })?
            }
            "offset" => {
                offset = raw.parse().map_err(|_| {
                    AppRuntimeError::Invocation("invalid offset parameter".to_string())
                })?
            }
            "search" | "q" => search = Some(raw.clone()),
            "scope" => {
                scope = match raw.as_str() {
                    "active" => ResourceRecordScope::Active,
                    "all" => ResourceRecordScope::All,
                    "deleted" => ResourceRecordScope::Deleted,
                    _ => {
                        return Err(AppRuntimeError::InvalidRequest(format!(
                            "invalid resource scope `{raw}`"
                        )));
                    }
                }
            }
            "sort" => {
                sort.clear();
                for field in raw.split(',').filter(|field| !field.is_empty()) {
                    let (descending, field) = match field.strip_prefix('-') {
                        Some(field) => (true, field),
                        None => (false, field),
                    };
                    sort.push(bicdb_extension::abi_v2::SortField {
                        field: field.to_string(),
                        descending,
                    });
                }
            }
            name if name.starts_with("filter[") && name.ends_with(']') => {
                if !contract.filter_contracts.is_empty() {
                    return Err(AppRuntimeError::InvalidRequest(
                        "legacy bracket filters are unavailable for this signed resource contract"
                            .to_string(),
                    ));
                }
                filters.push(bicdb_extension::abi_v2::FilterExpression::Eq {
                    field: name[7..name.len() - 1].to_string(),
                    value: parse_scalar(raw),
                });
            }
            _ => {}
        }
    }
    for filter in &contract.filter_contracts {
        match &filter.filter {
            ResourceFilterKind::Exact { field } => {
                if let Some(raw) = direct_filter_values.get(&filter.query_name) {
                    filters.push(bicdb_extension::abi_v2::FilterExpression::Eq {
                        field: field.clone(),
                        value: parse_resource_filter_value(contract, field, raw)?,
                    });
                }
            }
            ResourceFilterKind::Contains { field } => {
                if let Some(raw) = direct_filter_values.get(&filter.query_name) {
                    filters.push(bicdb_extension::abi_v2::FilterExpression::Contains {
                        field: field.clone(),
                        value: raw.clone(),
                    });
                }
            }
            ResourceFilterKind::Minimum { field } => {
                if let Some(raw) = direct_filter_values.get(&filter.query_name) {
                    filters.push(bicdb_extension::abi_v2::FilterExpression::Ge {
                        field: field.clone(),
                        value: parse_resource_filter_value(contract, field, raw)?,
                    });
                }
            }
            ResourceFilterKind::Exists { field } => {
                if let Some(raw) = direct_filter_values.get(&filter.query_name) {
                    let exists = raw.parse::<bool>().map_err(|_| {
                        AppRuntimeError::InvalidRequest(format!(
                            "resource filter `{}` requires true or false",
                            filter.query_name
                        ))
                    })?;
                    filters.push(if exists {
                        bicdb_extension::abi_v2::FilterExpression::Ne {
                            field: field.clone(),
                            value: Value::Null,
                        }
                    } else {
                        bicdb_extension::abi_v2::FilterExpression::Eq {
                            field: field.clone(),
                            value: Value::Null,
                        }
                    });
                }
            }
            ResourceFilterKind::Overlaps {
                start_field,
                end_field,
            } => {
                let start_name = format!("{}_start", filter.query_name);
                let end_name = format!("{}_end", filter.query_name);
                match (
                    direct_filter_values.get(&start_name),
                    direct_filter_values.get(&end_name),
                ) {
                    (Some(start), Some(end)) => {
                        filters.push(bicdb_extension::abi_v2::FilterExpression::Lt {
                            field: start_field.clone(),
                            value: parse_resource_filter_value(contract, start_field, end)?,
                        });
                        filters.push(bicdb_extension::abi_v2::FilterExpression::Gt {
                            field: end_field.clone(),
                            value: parse_resource_filter_value(contract, end_field, start)?,
                        });
                    }
                    (None, None) => {}
                    _ => {
                        return Err(AppRuntimeError::InvalidRequest(format!(
                            "overlap filter `{}` requires both bounds",
                            filter.query_name
                        )));
                    }
                }
            }
            ResourceFilterKind::JsonExact {
                field,
                path,
                value_type,
            } => {
                if let Some(raw) = direct_filter_values.get(&filter.query_name) {
                    filters.push(bicdb_extension::abi_v2::FilterExpression::JsonPathEq {
                        field: field.clone(),
                        path: path.clone(),
                        value: coerce_carrier_parameter_value(
                            &Value::String(raw.clone()),
                            value_type,
                            "query",
                            &filter.query_name,
                        )?,
                        value_type: Some(value_type.clone()),
                    });
                }
            }
            ResourceFilterKind::JsonContains {
                field,
                path,
                value_type,
                array,
            } => {
                if let Some(raw) = direct_filter_values.get(&filter.query_name) {
                    let value = coerce_carrier_parameter_value(
                        &Value::String(raw.clone()),
                        value_type,
                        "query",
                        &filter.query_name,
                    )?
                    .as_str()
                    .expect("validated JSON contains filter type")
                    .to_string();
                    filters.push(
                        bicdb_extension::abi_v2::FilterExpression::JsonPathContains {
                            field: field.clone(),
                            path: path.clone(),
                            value,
                            array: *array,
                        },
                    );
                }
            }
            ResourceFilterKind::JsonMinimum {
                field,
                path,
                value_type,
            } => {
                if let Some(raw) = direct_filter_values.get(&filter.query_name) {
                    filters.push(bicdb_extension::abi_v2::FilterExpression::JsonPathGe {
                        field: field.clone(),
                        path: path.clone(),
                        value: coerce_carrier_parameter_value(
                            &Value::String(raw.clone()),
                            value_type,
                            "query",
                            &filter.query_name,
                        )?,
                        value_type: value_type.clone(),
                    });
                }
            }
            ResourceFilterKind::JsonExists { field, path } => {
                if let Some(raw) = direct_filter_values.get(&filter.query_name) {
                    let exists = raw.parse::<bool>().map_err(|_| {
                        AppRuntimeError::InvalidRequest(format!(
                            "resource filter `{}` requires true or false",
                            filter.query_name
                        ))
                    })?;
                    filters.push(bicdb_extension::abi_v2::FilterExpression::JsonPathExists {
                        field: field.clone(),
                        path: path.clone(),
                        exists,
                    });
                }
            }
            ResourceFilterKind::RelationExact { value_type, .. }
            | ResourceFilterKind::RelationMinimum { value_type, .. }
            | ResourceFilterKind::RelationContains { value_type, .. } => {
                if let Some(raw) = direct_filter_values.get(&filter.query_name) {
                    relation_filters.insert(
                        filter.query_name.clone(),
                        coerce_carrier_parameter_value(
                            &Value::String(raw.clone()),
                            value_type,
                            "query",
                            &filter.query_name,
                        )?,
                    );
                }
            }
        }
    }
    let body = match &request.body {
        HttpRequestBodyV2::Json(value) => value.clone(),
        HttpRequestBodyV2::Empty => Value::Null,
        _ => {
            return Err(AppRuntimeError::Invocation(
                "resource operations accept JSON bodies".to_string(),
            ));
        }
    };
    let version = header(&request.headers, "if-match")
        .map(|value| value.trim_matches('"').parse::<u64>())
        .transpose()
        .map_err(|_| AppRuntimeError::Invocation("invalid If-Match version".to_string()))?;
    let idempotency_key = contract
        .idempotency
        .as_ref()
        .and_then(|contract| header(&request.headers, &contract.header));
    Ok(ResourceRequest {
        operation,
        id: path_parameters
            .get(&contract.primary_key)
            .or_else(|| path_parameters.get("id"))
            .cloned(),
        body,
        filters,
        relation_filters,
        sort,
        search,
        limit,
        offset,
        scope,
        expected_version: version,
        idempotency_key,
        // The HTTP list envelope carries an exact `page_info.total`, matching
        // the paged envelope every other BicDB application target serves.
        include_total: true,
        internal_model_call: false,
    })
}

fn carrier_idempotency_request(globals: &BTreeMap<String, Value>) -> Value {
    let mut request = serde_json::Map::new();
    for name in ["params", "query", "input"] {
        if let Some(value) = globals.get(name) {
            request.insert(name.to_string(), value.clone());
        }
    }
    Value::Object(request)
}

/// A READ COMMITTED writer can deliberately lose a row-lock ordering race to
/// prevent a deadlock cycle. Retry the complete operation only for an
/// idempotent route: its signed key makes replay safe, and a fresh transaction
/// receives a newer ordering id that can wait for the current owner. Business
/// conflicts and non-idempotent routes are never retried here.
fn retry_idempotent_transaction_conflicts<T>(mut execute: impl FnMut() -> Result<T>) -> Result<T> {
    const MAX_ATTEMPTS: usize = 16;
    for attempt in 0..MAX_ATTEMPTS {
        match execute() {
            Err(AppRuntimeError::Conflict(detail))
                if is_transaction_lock_conflict(&detail) && attempt + 1 < MAX_ATTEMPTS =>
            {
                std::thread::sleep(std::time::Duration::from_millis(1_u64 << attempt.min(5)));
            }
            result => return result,
        }
    }
    unreachable!("the bounded retry loop always returns on its final attempt")
}

fn is_transaction_lock_conflict(detail: &str) -> bool {
    detail.starts_with("transaction conflict:")
        || (detail.starts_with("conflict (Conflict, trace ")
            && detail.contains("): transaction conflict:"))
}

fn carrier_cache_request(globals: &BTreeMap<String, Value>) -> Value {
    let mut request = serde_json::Map::new();
    for name in ["params", "query"] {
        if let Some(value) = globals.get(name) {
            request.insert(name.to_string(), value.clone());
        }
    }
    Value::Object(request)
}

fn http_request_body_len(body: &HttpRequestBodyV2) -> Result<usize> {
    Ok(match body {
        HttpRequestBodyV2::Empty => 0,
        HttpRequestBodyV2::Json(value) => serde_json::to_vec(value)?.len(),
        HttpRequestBodyV2::Form(fields) => fields
            .iter()
            .map(|(name, value)| name.len().saturating_add(value.len()).saturating_add(2))
            .sum(),
        HttpRequestBodyV2::Multipart(parts) => parts
            .iter()
            .map(|part| {
                part.name
                    .len()
                    .saturating_add(part.filename.as_ref().map_or(0, String::len))
                    .saturating_add(
                        part.headers
                            .iter()
                            .map(|(name, value)| name.len().saturating_add(value.len()))
                            .sum::<usize>(),
                    )
                    .saturating_add(part.body.len())
                    .saturating_add(256)
            })
            .sum(),
        // Binary payloads remain byte vectors from socket parsing through the
        // direct host route. They are not expanded into a JSON integer array.
        HttpRequestBodyV2::Binary(bytes) => bytes.len(),
        HttpRequestBodyV2::Stream(_) => 0,
    })
}

fn http_response_body_len(body: &HttpResponseBodyV2) -> Result<usize> {
    Ok(match body {
        HttpResponseBodyV2::Empty
        | HttpResponseBodyV2::Stream(_)
        | HttpResponseBodyV2::Sse(_)
        | HttpResponseBodyV2::WebSocket(_) => 0,
        HttpResponseBodyV2::Json(value) => serde_json::to_vec(value)?.len(),
        HttpResponseBodyV2::Text(value) => value.len(),
        HttpResponseBodyV2::Binary(bytes) => bytes.len(),
        HttpResponseBodyV2::Error(value) => serde_json::to_vec(value)?.len(),
    })
}

fn from_resource(response: ResourceResponse) -> ApplicationHttpResponse {
    ApplicationHttpResponse {
        status: response.status,
        headers: response.headers,
        body: if response.body.is_null() {
            HttpResponseBodyV2::Empty
        } else {
            HttpResponseBodyV2::Json(response.body)
        },
        trailers: Vec::new(),
        retry_after_ms: None,
    }
}

fn with_http_policy_headers(
    mut response: ApplicationHttpResponse,
    origin: Option<&str>,
    policy: &HttpHostPolicy,
) -> ApplicationHttpResponse {
    set_response_header(&mut response, "x-content-type-options", "nosniff");
    set_response_header(
        &mut response,
        "content-security-policy",
        "default-src 'none'; frame-ancestors 'none'",
    );
    set_response_header(&mut response, "x-frame-options", "DENY");
    set_response_header(&mut response, "referrer-policy", "no-referrer");
    if let Some(origin) = origin {
        if !has_response_header(&response, "access-control-allow-origin")
            && (policy.cors_origins.contains(origin) || policy.cors_origins.contains("*"))
        {
            response.headers.push((
                "access-control-allow-origin".to_string(),
                origin.to_string(),
            ));
            response
                .headers
                .push(("vary".to_string(), "origin".to_string()));
            if policy.allow_credentials {
                response.headers.push((
                    "access-control-allow-credentials".to_string(),
                    "true".to_string(),
                ));
            }
        }
    }
    response
}

fn apply_signed_response_headers(
    response: &mut ApplicationHttpResponse,
    headers: &BTreeMap<String, String>,
) {
    for (name, value) in headers {
        response
            .headers
            .retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
        response.headers.push((name.clone(), value.clone()));
    }
}

fn has_response_header(response: &ApplicationHttpResponse, name: &str) -> bool {
    response
        .headers
        .iter()
        .any(|(existing, _)| existing.eq_ignore_ascii_case(name))
}

fn set_response_header(response: &mut ApplicationHttpResponse, name: &str, value: &str) {
    response
        .headers
        .retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
    response.headers.push((name.to_string(), value.to_string()));
}

fn axum_response(state: &HttpState, response: ApplicationHttpResponse) -> Response<Body> {
    axum_response_inner(state, response, false)
}

fn axum_response_trusted(state: &HttpState, response: ApplicationHttpResponse) -> Response<Body> {
    axum_response_inner(state, response, true)
}

fn axum_response_inner(
    state: &HttpState,
    response: ApplicationHttpResponse,
    trusted: bool,
) -> Response<Body> {
    match http_response_body_len(&response.body) {
        Ok(bytes) if bytes <= state.policy.max_response_bytes => {}
        Ok(_) => {
            return error_response(AppRuntimeError::ResourceExhausted(
                "HTTP response body exceeds the host limit".to_string(),
            ));
        }
        Err(error) => return error_response(error),
    }
    let response_status = response.status;
    let response_headers = response.headers.clone();
    let response_trailers = response.trailers.clone();
    let retry_after_ms = response.retry_after_ms;
    let mut output = match response.body {
        HttpResponseBodyV2::Empty => match framed_body(Vec::new(), response_trailers.clone()) {
            Ok(body) => Response::new(body),
            Err(error) => return error_response(error),
        },
        HttpResponseBodyV2::Json(value) => {
            let body = match framed_body(
                vec![serde_json::to_vec(&value).unwrap_or_else(|_| b"null".to_vec())],
                response_trailers.clone(),
            ) {
                Ok(body) => body,
                Err(error) => return error_response(error),
            };
            let mut response = Response::new(body);
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            response
        }
        HttpResponseBodyV2::Text(value) => {
            match framed_body(vec![value.into_bytes()], response_trailers.clone()) {
                Ok(body) => Response::new(body),
                Err(error) => return error_response(error),
            }
        }
        HttpResponseBodyV2::Binary(value) => {
            match framed_body(vec![value], response_trailers.clone()) {
                Ok(body) => Response::new(body),
                Err(error) => return error_response(error),
            }
        }
        HttpResponseBodyV2::Error(value) => {
            let body = match framed_body(
                vec![serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec())],
                response_trailers.clone(),
            ) {
                Ok(body) => body,
                Err(error) => return error_response(error),
            };
            let mut response = Response::new(body);
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/problem+json"),
            );
            response
        }
        HttpResponseBodyV2::Stream(handle) => {
            match bounded_stream_response(
                state,
                handle.0,
                StreamKind::Bytes,
                response_trailers.clone(),
            ) {
                Ok(response) => response,
                Err(error) => return error_response(error),
            }
        }
        HttpResponseBodyV2::Sse(handle) => {
            match bounded_stream_response(
                state,
                handle.0,
                StreamKind::ServerSentEvents,
                response_trailers.clone(),
            ) {
                Ok(response) => response,
                Err(error) => return error_response(error),
            }
        }
        HttpResponseBodyV2::WebSocket(_) => {
            return error_response(AppRuntimeError::Provider(
                "WebSocket upgrades require the dedicated realtime host adapter".to_string(),
            ));
        }
    };
    if output.status() == StatusCode::OK || output.status().as_u16() == response_status {
        *output.status_mut() =
            StatusCode::from_u16(response_status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    } else {
        return error_response(AppRuntimeError::Invocation(
            "stream response status differs from its opened stream".to_string(),
        ));
    }
    for (name, value) in response_headers {
        let name = match HeaderName::try_from(name.as_str()) {
            Ok(name)
                if if trusted {
                    !transport_controlled_response_header(&name)
                } else {
                    !host_controlled_response_header(&name)
                } =>
            {
                name
            }
            _ => {
                return error_response(AppRuntimeError::InvalidRequest(
                    "application response contains an invalid or host-controlled header"
                        .to_string(),
                ));
            }
        };
        let value = match HeaderValue::try_from(value.as_str()) {
            Ok(value) => value,
            Err(_) => {
                return error_response(AppRuntimeError::InvalidRequest(
                    "application response contains an invalid header value".to_string(),
                ));
            }
        };
        output.headers_mut().append(name, value);
    }
    if output.headers().len() > 256 {
        return error_response(AppRuntimeError::ResourceExhausted(
            "application response contains too many headers".to_string(),
        ));
    }
    let header_bytes = output
        .headers()
        .iter()
        .map(|(name, value)| name.as_str().len().saturating_add(value.as_bytes().len()))
        .sum::<usize>();
    if header_bytes > 64 * 1024 {
        return error_response(AppRuntimeError::ResourceExhausted(
            "application response headers exceed host limits".to_string(),
        ));
    }
    if !response_trailers.is_empty() {
        let names = response_trailers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        if let Ok(value) = HeaderValue::try_from(names) {
            output
                .headers_mut()
                .insert(axum::http::header::TRAILER, value);
        }
    }
    if let Some(milliseconds) = retry_after_ms {
        let seconds = milliseconds.saturating_add(999) / 1_000;
        if let Ok(value) = HeaderValue::try_from(seconds.to_string()) {
            output
                .headers_mut()
                .insert(axum::http::header::RETRY_AFTER, value);
        }
    }
    output
}

fn bounded_stream_response(
    state: &HttpState,
    stream: u64,
    expected: StreamKind,
    mut application_trailers: Vec<(String, String)>,
) -> Result<Response<Body>> {
    let response = state
        .runtime
        .take_realtime_response(stream, state.policy.max_response_bytes)?;
    if response.kind != expected {
        return Err(AppRuntimeError::Invocation(
            "stream response kind differs from its HTTP body".to_string(),
        ));
    }
    application_trailers.extend(response.trailers);
    let trailer_names = application_trailers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let mut output = Response::new(framed_body(response.chunks, application_trailers)?);
    *output.status_mut() =
        StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    for (name, value) in response.headers {
        let name = HeaderName::try_from(name.as_str()).map_err(|_| {
            AppRuntimeError::InvalidRequest("stream contains an invalid header name".to_string())
        })?;
        if host_controlled_response_header(&name) {
            return Err(AppRuntimeError::InvalidRequest(
                "stream contains a host-controlled response header".to_string(),
            ));
        }
        let value = HeaderValue::try_from(value.as_str()).map_err(|_| {
            AppRuntimeError::InvalidRequest("stream contains an invalid header value".to_string())
        })?;
        output.headers_mut().append(name, value);
    }
    if !trailer_names.is_empty() {
        let value = HeaderValue::try_from(trailer_names).map_err(|_| {
            AppRuntimeError::Invocation("response contains invalid trailer names".to_string())
        })?;
        output
            .headers_mut()
            .insert(axum::http::header::TRAILER, value);
    }
    if expected == StreamKind::ServerSentEvents {
        output.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        output.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache"),
        );
    }
    Ok(output)
}

fn host_controlled_response_header(name: &HeaderName) -> bool {
    transport_controlled_response_header(name)
        || matches!(name.as_str(), "set-cookie" | "strict-transport-security")
}

fn transport_controlled_response_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "content-length"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn framed_body(chunks: Vec<Vec<u8>>, trailers: Vec<(String, String)>) -> Result<Body> {
    let mut frames = chunks
        .into_iter()
        .map(|chunk| Ok::<_, Infallible>(Frame::data(Bytes::from(chunk))))
        .collect::<Vec<_>>();
    if !trailers.is_empty() {
        let mut map = axum::http::HeaderMap::new();
        for (name, value) in trailers {
            let name = HeaderName::try_from(name.as_str()).map_err(|_| {
                AppRuntimeError::Invocation("response contains an invalid trailer name".to_string())
            })?;
            let value = HeaderValue::try_from(value.as_str()).map_err(|_| {
                AppRuntimeError::Invocation(
                    "response contains an invalid trailer value".to_string(),
                )
            })?;
            map.append(name, value);
        }
        frames.push(Ok(Frame::trailers(map)));
    }
    Ok(Body::new(StreamBody::new(tokio_stream::iter(frames))))
}

fn carrier_error_message_key(code: &str, message: &str) -> String {
    let mut normalized = String::new();
    let mut separator = false;
    for character in code.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '_' | '-' | '.')
        {
            normalized.push(character);
            separator = false;
        } else if !separator {
            normalized.push('_');
            separator = true;
        }
    }
    if normalized.is_empty() {
        normalized.push_str("error");
    }
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in message.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("runtime.errors.{normalized}.{hash:016x}")
}

fn error_response(error: AppRuntimeError) -> Response<Body> {
    error_response_with_request_id(error, None)
}

fn error_response_with_request_id(
    error: AppRuntimeError,
    request_id: Option<&str>,
) -> Response<Body> {
    let request_id = request_id
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let error = match error {
        AppRuntimeError::ApplicationFailure { code, message, .. } => {
            let message_key = carrier_error_message_key(&code, &message);
            return (
                StatusCode::BAD_REQUEST,
                [("content-type", "application/problem+json")],
                json!({
                    "code": code,
                    "message": message,
                    "message_key": message_key,
                    "parameters": {},
                    "request_id": request_id,
                })
                .to_string(),
            )
                .into_response();
        }
        error => error,
    };
    let (status, code) = match &error {
        AppRuntimeError::Authentication(_) => (StatusCode::UNAUTHORIZED, "unauthenticated"),
        AppRuntimeError::CapabilityDenied(_) => (StatusCode::FORBIDDEN, "forbidden"),
        AppRuntimeError::NotReady(_) => (StatusCode::SERVICE_UNAVAILABLE, "not_ready"),
        AppRuntimeError::CircuitOpen(_) => (StatusCode::SERVICE_UNAVAILABLE, "circuit_open"),
        AppRuntimeError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
        AppRuntimeError::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
        AppRuntimeError::IdempotencyKeyReused(_) => {
            (StatusCode::CONFLICT, "idempotency_key_reused")
        }
        AppRuntimeError::OptimisticConflict(_) => (StatusCode::CONFLICT, "optimistic_conflict"),
        AppRuntimeError::Timeout(_) => (StatusCode::GATEWAY_TIMEOUT, "deadline_exceeded"),
        AppRuntimeError::ResilienceTimeout(_) => (StatusCode::GATEWAY_TIMEOUT, "timeout"),
        AppRuntimeError::Cancelled(_) => (StatusCode::from_u16(499).unwrap(), "cancelled"),
        AppRuntimeError::ResourceExhausted(_) => {
            (StatusCode::SERVICE_UNAVAILABLE, "resource_exhausted")
        }
        AppRuntimeError::RateLimited(_) => (StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        AppRuntimeError::InvalidRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
        AppRuntimeError::MissingIdempotencyKey(_) => {
            (StatusCode::BAD_REQUEST, "missing_idempotency_key")
        }
        AppRuntimeError::Invocation(message) if message.contains("not found") => {
            (StatusCode::NOT_FOUND, "not_found")
        }
        AppRuntimeError::Invocation(message) if message.contains("conflict") => {
            (StatusCode::CONFLICT, "conflict")
        }
        AppRuntimeError::Invocation(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    };
    (
        status,
        [("content-type", "application/problem+json")],
        json!({
            "code": code,
            "message": error.to_string(),
        })
        .to_string(),
    )
        .into_response()
}

fn match_route(template: &str, path: &str) -> Option<BTreeMap<String, String>> {
    let template = template.trim_matches('/').split('/').collect::<Vec<_>>();
    let path = path.trim_matches('/').split('/').collect::<Vec<_>>();
    if template.len() != path.len() {
        return None;
    }
    let mut parameters = BTreeMap::new();
    for (template, actual) in template.into_iter().zip(path) {
        if let Some(name) = template
            .strip_prefix('{')
            .and_then(|template| template.strip_suffix('}'))
        {
            parameters.insert(name.to_string(), percent_decode(actual).ok()?);
        } else if template != actual {
            return None;
        }
    }
    Some(parameters)
}

fn most_specific_matching_route<'a>(
    routes: &'a [RouteV2],
    method: &HttpMethod,
    path: &str,
) -> Option<(&'a RouteV2, BTreeMap<String, String>)> {
    let mut best: Option<(&RouteV2, BTreeMap<String, String>, Vec<bool>)> = None;
    for route in routes {
        if &route.method != method {
            continue;
        }
        let Some(parameters) = match_route(&route.template, path) else {
            continue;
        };
        let specificity = route_specificity(&route.template);
        if best
            .as_ref()
            .is_none_or(|(_, _, current)| specificity > *current)
        {
            best = Some((route, parameters, specificity));
        }
    }
    best.map(|(route, parameters, _)| (route, parameters))
}

fn route_specificity(template: &str) -> Vec<bool> {
    template
        .trim_matches('/')
        .split('/')
        .map(|segment| !(segment.starts_with('{') && segment.ends_with('}') && segment.len() > 2))
        .collect()
}

fn parse_method(method: &str) -> Result<HttpMethod> {
    match method {
        "GET" => Ok(HttpMethod::Get),
        "POST" => Ok(HttpMethod::Post),
        "PUT" => Ok(HttpMethod::Put),
        "PATCH" => Ok(HttpMethod::Patch),
        "DELETE" => Ok(HttpMethod::Delete),
        "HEAD" => Ok(HttpMethod::Head),
        "OPTIONS" => Ok(HttpMethod::Options),
        _ => Err(AppRuntimeError::Invocation(format!(
            "unsupported HTTP method `{method}`"
        ))),
    }
}

fn parse_urlencoded(input: &str) -> Result<Vec<(String, String)>> {
    input
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            Ok((percent_decode(name)?, percent_decode(value)?))
        })
        .collect()
}

fn percent_decode(input: &str) -> Result<String> {
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => output.push(b' '),
            b'%' if index + 2 < bytes.len() => {
                let encoded = std::str::from_utf8(&bytes[index + 1..index + 3])
                    .map_err(|error| AppRuntimeError::Invocation(error.to_string()))?;
                output.push(u8::from_str_radix(encoded, 16).map_err(|_| {
                    AppRuntimeError::Invocation("invalid percent encoding".to_string())
                })?);
                index += 2;
            }
            b'%' => {
                return Err(AppRuntimeError::Invocation(
                    "truncated percent encoding".to_string(),
                ));
            }
            byte => output.push(byte),
        }
        index += 1;
    }
    String::from_utf8(output).map_err(|error| AppRuntimeError::Invocation(error.to_string()))
}

fn parse_scalar(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()))
}

fn parse_resource_filter_value(
    contract: &bicdb_extension::abi_v2::ResourceContractV1,
    field: &str,
    raw: &str,
) -> Result<Value> {
    let field = contract
        .fields
        .iter()
        .find(|candidate| candidate.name == field)
        .ok_or_else(|| AppRuntimeError::InvalidPackage("filter field is absent".to_string()))?;
    let invalid = || {
        AppRuntimeError::InvalidRequest(format!(
            "resource filter `{}` does not match its signed field type",
            field.name
        ))
    };
    if let Some(value_type) = &field.value_type {
        return coerce_carrier_parameter_value(
            &Value::String(raw.to_string()),
            value_type,
            "resource filter",
            &field.name,
        );
    }
    match field.field_type {
        FieldType::Bool => raw.parse::<bool>().map(Value::Bool).map_err(|_| invalid()),
        FieldType::Int64 => raw
            .parse::<i64>()
            .map(|value| Value::Number(value.into()))
            .map_err(|_| invalid()),
        FieldType::Float64 | FieldType::Decimal => raw
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or_else(invalid),
        FieldType::Uuid => Uuid::parse_str(raw)
            .map(|value| Value::String(value.to_string()))
            .map_err(|_| invalid()),
        FieldType::Timestamp => chrono::DateTime::parse_from_rfc3339(raw)
            .map(|value| {
                Value::String(
                    value
                        .with_timezone(&chrono::Utc)
                        .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
                )
            })
            .map_err(|_| invalid()),
        FieldType::Date => chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
            .map(|value| Value::String(value.format("%Y-%m-%d").to_string()))
            .map_err(|_| invalid()),
        FieldType::Json => serde_json::from_str(raw).map_err(|_| invalid()),
        _ => Ok(Value::String(raw.to_string())),
    }
}

#[derive(Clone, Debug)]
struct RequestTraceContext {
    trace_id: String,
    parent_span_id: Option<String>,
    parent_sampled: Option<bool>,
    trace_flags: u8,
    tracestate: Option<String>,
}

fn request_identifier(headers: &[(String, String)], name: &str) -> Result<Option<String>> {
    let Some(value) = header(headers, name) else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(AppRuntimeError::InvalidRequest(format!(
            "{name} must contain 1..=256 non-control characters"
        )));
    }
    Ok(Some(value.to_string()))
}

fn request_trace_context(headers: &[(String, String)]) -> RequestTraceContext {
    let parsed = header(headers, "traceparent").and_then(|value| parse_traceparent(&value));
    let (trace_id, parent_span_id, parent_sampled, trace_flags) = match parsed {
        Some((trace_id, parent_span_id, flags)) => {
            (trace_id, Some(parent_span_id), Some(flags & 1 == 1), flags)
        }
        None => (Uuid::new_v4().simple().to_string(), None, None, 0),
    };
    let tracestate = header(headers, "tracestate").and_then(|value| {
        let value = value.trim();
        (!value.is_empty()
            && value.len() <= 512
            && value.is_ascii()
            && !value.chars().any(char::is_control))
        .then(|| value.to_string())
    });
    RequestTraceContext {
        trace_id,
        parent_span_id,
        parent_sampled,
        trace_flags,
        tracestate,
    }
}

fn parse_traceparent(value: &str) -> Option<(String, String, u8)> {
    let mut parts = value.trim().split('-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    let parent_id = parts.next()?;
    let flags = parts.next()?;
    if parts.next().is_some()
        || version.len() != 2
        || version.eq_ignore_ascii_case("ff")
        || trace_id.len() != 32
        || parent_id.len() != 16
        || flags.len() != 2
        || !version.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !trace_id.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !parent_id.bytes().all(|byte| byte.is_ascii_hexdigit())
        || trace_id.bytes().all(|byte| byte == b'0')
        || parent_id.bytes().all(|byte| byte == b'0')
    {
        return None;
    }
    let flags = u8::from_str_radix(flags, 16).ok()?;
    Some((
        trace_id.to_ascii_lowercase(),
        parent_id.to_ascii_lowercase(),
        flags,
    ))
}

fn trace_sampled(sampling: ApplicationSamplingV1, trace: &RequestTraceContext) -> bool {
    crate::runtime::carrier_sampling_decision(sampling, &trace.trace_id, trace.parent_sampled)
}

fn apply_request_trace_context(
    mut actor: ActorContext,
    request_id: &str,
    correlation_id: &str,
    trace: &RequestTraceContext,
    sampling: ApplicationSamplingV1,
) -> ActorContext {
    let sampled = trace_sampled(sampling, trace);
    actor.trace_id.clone_from(&trace.trace_id);
    actor.correlation_id = Some(correlation_id.to_string());
    actor.causation_id = Some(request_id.to_string());
    actor
        .policy_attributes
        .insert("carrier.trace.sampled".to_string(), sampled.to_string());
    actor.policy_attributes.insert(
        "w3c.trace_flags".to_string(),
        format!("{:02x}", (trace.trace_flags & !1) | u8::from(sampled)),
    );
    if let Some(parent_span_id) = &trace.parent_span_id {
        actor
            .policy_attributes
            .insert("w3c.parent_span_id".to_string(), parent_span_id.clone());
    }
    if let Some(tracestate) = &trace.tracestate {
        actor
            .policy_attributes
            .insert("w3c.tracestate".to_string(), tracestate.clone());
    }
    actor
}

fn apply_trace_response_headers(
    response: &mut ApplicationHttpResponse,
    actor: &ActorContext,
    request_id: &str,
) {
    for name in [
        "x-request-id",
        "x-correlation-id",
        "x-trace-id",
        "traceparent",
        "tracestate",
    ] {
        response
            .headers
            .retain(|(candidate, _)| !candidate.eq_ignore_ascii_case(name));
    }
    response
        .headers
        .push(("x-request-id".to_string(), request_id.to_string()));
    if let Some(correlation_id) = &actor.correlation_id {
        response
            .headers
            .push(("x-correlation-id".to_string(), correlation_id.clone()));
    }
    response
        .headers
        .push(("x-trace-id".to_string(), actor.trace_id.clone()));
    let span_source = Uuid::new_v4().simple().to_string();
    let span_id = &span_source[..16];
    let flags = actor
        .policy_attributes
        .get("w3c.trace_flags")
        .map(String::as_str)
        .unwrap_or("00");
    response.headers.push((
        "traceparent".to_string(),
        format!("00-{}-{span_id}-{flags}", actor.trace_id),
    ));
    if let Some(tracestate) = actor.policy_attributes.get("w3c.tracestate") {
        response
            .headers
            .push(("tracestate".to_string(), tracestate.clone()));
    }
}

fn header(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
}

fn sensitive_request_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("authorization")
        || name.eq_ignore_ascii_case("proxy-authorization")
        || name.eq_ignore_ascii_case("cookie")
        || name.eq_ignore_ascii_case("set-cookie")
        || name.eq_ignore_ascii_case("x-api-key")
}

fn authentication_credential(
    headers: &[(String, String)],
    policy: &HttpHostPolicy,
) -> Result<Option<String>> {
    let authorization = header(headers, "authorization");
    let Some(cookie_name) = policy.session_cookie_name.as_deref() else {
        return Ok(authorization);
    };
    let matching = parse_cookies(headers)
        .into_iter()
        .filter(|(name, _)| name == cookie_name)
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        return Err(AppRuntimeError::Authentication(
            "request contains duplicate host session cookies".to_string(),
        ));
    }
    let cookie = matching.into_iter().next();
    if authorization.is_some() && cookie.is_some() {
        return Err(AppRuntimeError::Authentication(
            "request contains ambiguous bearer and host session credentials".to_string(),
        ));
    }
    Ok(authorization.or(cookie))
}

fn parse_cookies(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("cookie"))
        .flat_map(|(_, value)| value.split(';'))
        .filter_map(|cookie| {
            let (name, value) = cookie.trim().split_once('=')?;
            Some((name.to_string(), value.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_extension::abi_v2::{
        AuditContract, ContractField, FieldType, ResourceContractV1, ResourceListDefaults,
        SortField,
    };

    #[tokio::test]
    async fn admission_expiry_blocks_all_http_routes_before_body_read() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        #[derive(Debug)]
        struct TestAdmission(AtomicBool);
        impl HttpAdmissionCheck for TestAdmission {
            fn is_admitted(&self) -> bool {
                self.0.load(Ordering::SeqCst)
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let database = bicdb_core::BicDb::open(directory.path().join("db")).unwrap();
        let runtime = Arc::new(
            ApplicationRuntime::new(
                Arc::new(parking_lot::RwLock::new(database)),
                crate::ApplicationHostConfig::new(directory.path().join("packages"), "test"),
                crate::PackageVerifier::new(Default::default(), 1024).unwrap(),
                crate::InvocationServices::new(
                    Arc::new(crate::InMemorySecretProvider::default()),
                    Arc::new(crate::DenyEgressProvider),
                    Arc::new(crate::DenyBlobProvider),
                    Arc::new(crate::BoundedObservability::new(10).unwrap()),
                ),
            )
            .unwrap(),
        );
        let authenticator = Arc::new(
            JwtAuthenticator::hs256(
                crate::JwtConfiguration {
                    issuer: "test".to_string(),
                    audience: "test".to_string(),
                    authentication_method: "test".to_string(),
                    maximum_lifetime_seconds: 60,
                    clock_skew_seconds: 0,
                },
                vec![7; 32],
            )
            .unwrap(),
        );
        let check = Arc::new(TestAdmission(AtomicBool::new(true)));
        let policy = HttpHostPolicy {
            admission_check: Some(check.clone()),
            ..HttpHostPolicy::default()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = runtime
            .serve_http(listener, authenticator, policy)
            .await
            .unwrap();
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .unwrap();
        let url = format!("http://{}", server.address);
        assert_eq!(
            client
                .get(format!("{url}/livez"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        check.0.store(false, Ordering::SeqCst);
        for path in [
            "/livez",
            "/readyz",
            "/openapi/test",
            "/_bicdb/session",
            "/application",
        ] {
            let response = client.get(format!("{url}{path}")).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
            assert!(response.text().await.unwrap().contains("admission_expired"));
        }
        // The client advertises a body but sends none. Expiry must be rejected
        // immediately, without waiting for body completion or its timeout.
        let mut connection = tokio::net::TcpStream::connect(server.address)
            .await
            .unwrap();
        connection
            .write_all(
                b"POST /application HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = [0; 1024];
        let size = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            connection.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(std::str::from_utf8(&response[..size])
            .unwrap()
            .starts_with("HTTP/1.1 503"));
        drop(connection);
        server.shutdown().await.unwrap();
    }

    #[test]
    fn idempotent_routes_retry_only_transaction_lock_conflicts() {
        let mut attempts = 0;
        let value = retry_idempotent_transaction_conflicts(|| {
            attempts += 1;
            if attempts < 3 {
                Err(AppRuntimeError::Conflict(
                    if attempts == 1 {
                        "transaction conflict: number series is locked".to_string()
                    } else {
                        "conflict (Conflict, trace 25f57c2a): transaction conflict: number series is locked"
                            .to_string()
                    },
                ))
            } else {
                Ok(7)
            }
        })
        .unwrap();
        assert_eq!(value, 7);
        assert_eq!(attempts, 3);

        let mut business_attempts = 0;
        let error = retry_idempotent_transaction_conflicts(|| {
            business_attempts += 1;
            Err::<(), _>(AppRuntimeError::Conflict(
                "document number already exists".to_string(),
            ))
        })
        .unwrap_err();
        assert!(matches!(error, AppRuntimeError::Conflict(_)));
        assert_eq!(business_attempts, 1);
    }

    /// A realtime stream verifies its bearer once, at connect, and then lives
    /// as long as the client holds it open. The periodic re-check re-ran only
    /// the application's `authorize_group` callable — with the actor cached at
    /// connect — and did nothing at all for a contract that declares no such
    /// callable. So token expiry and revocation could not end a stream already
    /// in flight. `revalidate_carrier_realtime` now consults this first, for
    /// every contract.
    #[test]
    fn a_realtime_credential_expires_at_its_deadline() {
        let mut actor = ActorContext::default();

        actor.deadline_unix_ms = 1_000;
        assert!(
            !realtime_credential_expired(&actor, 999),
            "a live credential must keep the session open"
        );
        assert!(
            realtime_credential_expired(&actor, 1_000),
            "the session must end the moment the deadline is reached"
        );
        assert!(
            realtime_credential_expired(&actor, 10_000),
            "a long-expired credential must not keep streaming"
        );

        // Absent deadline: must not be read as "expired at the epoch", which
        // would close every session that never carried a bearer.
        actor.deadline_unix_ms = 0;
        assert!(!realtime_credential_expired(&actor, 10_000));
        actor.deadline_unix_ms = -1;
        assert!(!realtime_credential_expired(&actor, 10_000));
    }

    #[test]
    fn binary_body_limits_use_raw_bytes_without_json_expansion() {
        let bytes = vec![0xa5; 1024 * 1024];
        assert_eq!(
            http_request_body_len(&HttpRequestBodyV2::Binary(bytes.clone())).unwrap(),
            bytes.len()
        );
        assert_eq!(
            http_response_body_len(&HttpResponseBodyV2::Binary(bytes.clone())).unwrap(),
            bytes.len()
        );
        assert!(
            serde_json::to_vec(&HttpRequestBodyV2::Binary(bytes))
                .unwrap()
                .len()
                > 3 * 1024 * 1024
        );
    }

    fn contract(list_defaults: Option<ResourceListDefaults>) -> ResourceContractV1 {
        ResourceContractV1 {
            version: 1,
            name: "widgets".to_string(),
            relation: "widgets".to_string(),
            schema_version: 1,
            schema_only: false,
            primary_key: "id".to_string(),
            fields: vec![ContractField {
                name: "id".to_string(),
                storage_name: None,
                field_type: FieldType::Uuid,
                value_type: None,
                nullable: false,
                generated: false,
                generated_expression: None,
                default_json: None,
            }],
            vector_search: None,
            timeseries: None,
            unique_targets: vec![],
            indexes: vec![],
            checks: vec![],
            foreign_keys: vec![],
            exclusions: vec![],
            reverse_relations: vec![],
            relations: vec![],
            create_fields: BTreeSet::new(),
            update_fields: BTreeSet::new(),
            required_create_fields: BTreeSet::new(),
            server_managed_fields: BTreeSet::new(),
            validation: Vec::new(),
            operations: BTreeSet::from([ResourceOperation::List]),
            filters: BTreeSet::new(),
            filter_contracts: Vec::new(),
            relation_filters: BTreeSet::new(),
            json_path_filters: BTreeSet::new(),
            search_fields: BTreeSet::new(),
            sort_fields: BTreeSet::from(["name".to_string()]),
            list_defaults,
            list_route: "/widgets".to_string(),
            item_route: "/widgets/{id}".to_string(),
            soft_delete_field: None,
            soft_delete_value: None,
            restore_value: None,
            version_field: None,
            tenant_field: None,
            workspace_field: None,
            policy: None,
            required_roles: BTreeSet::new(),
            required_scopes: BTreeSet::new(),
            policy_attributes: BTreeMap::new(),
            read_roles: BTreeMap::new(),
            redacted_fields: BTreeSet::new(),
            immutable_fields: BTreeSet::new(),
            encrypted_fields: BTreeMap::new(),
            privacy: None,
            idempotency: None,
            cache: None,
            audit: AuditContract::default(),
            events: Vec::new(),
            operation_metadata: Vec::new(),
            contract_sha256: "a".repeat(64),
            openapi: Value::Null,
        }
    }

    fn request(query: Vec<(String, String)>) -> ApplicationHttpRequest {
        ApplicationHttpRequest {
            method: HttpMethod::Get,
            path: "/widgets".to_string(),
            query,
            headers: Vec::new(),
            body: HttpRequestBodyV2::Empty,
            peer_address: None,
        }
    }

    #[test]
    fn resource_list_defaults_apply_and_query_parameters_override_them() {
        let contract_with_defaults = contract(Some(ResourceListDefaults {
            limit: 20,
            sort: vec![SortField {
                field: "name".to_string(),
                descending: false,
            }],
            scope: ResourceRecordScope::Deleted,
        }));
        let lowered = lower_resource_request(
            &contract_with_defaults,
            ResourceOperation::List,
            &BTreeMap::new(),
            &request(Vec::new()),
        )
        .unwrap();
        assert_eq!(lowered.limit, 20);
        assert_eq!(lowered.scope, ResourceRecordScope::Deleted);
        assert_eq!(
            lowered.sort,
            contract_with_defaults.list_defaults.unwrap().sort
        );

        let lowered = lower_resource_request(
            &contract(None),
            ResourceOperation::List,
            &BTreeMap::new(),
            &request(vec![
                ("limit".to_string(), "7".to_string()),
                ("sort".to_string(), "-name".to_string()),
                ("scope".to_string(), "all".to_string()),
            ]),
        )
        .unwrap();
        assert_eq!(lowered.limit, 7);
        assert_eq!(lowered.scope, ResourceRecordScope::All);
        assert_eq!(
            lowered.sort,
            vec![SortField {
                field: "name".to_string(),
                descending: true,
            }]
        );
    }

    #[test]
    fn route_authority_preserves_any_and_all_semantics() {
        let required = BTreeSet::from(["operator".to_string(), "auditor".to_string()]);
        let operator = BTreeSet::from(["operator".to_string()]);
        let both = BTreeSet::from(["operator".to_string(), "auditor".to_string()]);
        let unrelated = BTreeSet::from(["viewer".to_string()]);

        assert!(authority_matches(&required, &operator, true));
        assert!(!authority_matches(&required, &operator, false));
        assert!(authority_matches(&required, &both, false));
        assert!(!authority_matches(&required, &unrelated, true));
        assert!(authority_matches(&BTreeSet::new(), &unrelated, false));
    }

    #[test]
    fn static_routes_outrank_parameter_routes_regardless_of_manifest_order() {
        let route = |name: &str, template: &str| -> RouteV2 {
            serde_json::from_value(json!({
                "name": name,
                "method": "GET",
                "template": template,
                "export": name,
                "public": true,
                "max_request_bytes": 1024,
                "max_response_bytes": 1024
            }))
            .unwrap()
        };
        let routes = vec![
            route("patient_by_id", "/patients/{id}"),
            route("patient_search", "/patients/search"),
        ];

        let (matched, parameters) =
            most_specific_matching_route(&routes, &HttpMethod::Get, "/patients/search").unwrap();
        assert_eq!(matched.name, "patient_search");
        assert!(parameters.is_empty());

        let (matched, parameters) = most_specific_matching_route(
            &routes,
            &HttpMethod::Get,
            "/patients/550e8400-e29b-41d4-a716-446655440000",
        )
        .unwrap();
        assert_eq!(matched.name, "patient_by_id");
        assert_eq!(
            parameters.get("id").map(String::as_str),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );
    }

    #[test]
    fn realtime_route_and_scope_matching_are_exact() {
        let application: bicdb_extension::abi_v2::ApplicationManifestV2 =
            serde_json::from_value(json!({
                "application_profile": "carrier-enterprise/v1",
                "package": {
                    "application": "carrier",
                    "version": "1.0.0",
                    "package_sha256": "a".repeat(64),
                    "dependency_lock_sha256": "a".repeat(64),
                    "sbom_sha256": "a".repeat(64),
                    "provenance_sha256": "a".repeat(64),
                    "signature_key_id": "release",
                    "signature_algorithm": "ed25519",
                    "signature": "signed"
                },
                "realtime": [{
                    "name": "WidgetEvents",
                    "path": "/streams/widgets",
                    "queue": "carrier_realtime_widgets",
                    "events": ["WidgetChanged"],
                    "output": {"kind": "json"}
                }]
            }))
            .unwrap();
        let route = |template: &str| -> RouteV2 {
            serde_json::from_value(json!({
                "name": "widget_events",
                "method": "GET",
                "template": template,
                "export": "carrier_realtime_widget_events",
                "public": true,
                "max_request_bytes": 1024,
                "max_response_bytes": 1024
            }))
            .unwrap()
        };
        assert!(matches!(
            carrier_realtime_route(&application, &route("/streams/widgets/ws")),
            Some((_, ApplicationRealtimeTransport::WebSocket))
        ));
        assert!(
            carrier_realtime_route(&application, &route("/streams/widgets/ws/extra")).is_none()
        );

        let payload = json!({"tenant_id": "tenant-a", "widget_id": "widget-a"});
        assert!(realtime_payload_matches(
            &payload,
            Some("tenant_id"),
            Some("tenant-a")
        ));
        assert!(!realtime_payload_matches(
            &payload,
            Some("tenant_id"),
            Some("tenant-b")
        ));
        assert!(!realtime_payload_matches(&payload, Some("tenant_id"), None));
        assert_eq!(
            realtime_query_value(
                &[
                    ("cursor".to_string(), "1".to_string()),
                    ("cursor".to_string(), "7".to_string())
                ],
                "cursor"
            ),
            Some("7".to_string())
        );
    }

    #[test]
    fn anonymous_route_actor_is_explicit_and_valid() {
        let actor = anonymous_actor(
            "trace".to_string(),
            Some("correlation".to_string()),
            Some("https://example.test".to_string()),
            1,
        )
        .unwrap();
        assert_eq!(actor.service_id.as_deref(), Some("bicdb-anonymous"));
        assert_eq!(actor.authentication_method.as_deref(), Some("anonymous"));
        assert!(actor.roles.is_empty());
        assert!(actor.scopes.is_empty());
    }

    #[test]
    fn carrier_parameters_are_typed_defaulted_and_validated() {
        let parameters = vec![
            ApplicationRouteParameterV1 {
                name: "count".to_string(),
                value_type: ApplicationRouteParameterTypeV1::Int,
                optional: false,
                default_json: None,
                validations: Vec::new(),
            },
            ApplicationRouteParameterV1 {
                name: "limit".to_string(),
                value_type: ApplicationRouteParameterTypeV1::Int,
                optional: false,
                default_json: Some("3".to_string()),
                validations: vec![ApplicationRouteParameterValidationV1::Range {
                    minimum: Some("1".to_string()),
                    maximum: Some("10".to_string()),
                }],
            },
            ApplicationRouteParameterV1 {
                name: "labels".to_string(),
                value_type: ApplicationRouteParameterTypeV1::List {
                    element: Box::new(ApplicationRouteParameterTypeV1::String),
                },
                optional: true,
                default_json: None,
                validations: Vec::new(),
            },
        ];
        let raw = serde_json::Map::from_iter([
            ("count".to_string(), json!("7")),
            ("labels".to_string(), json!(["alpha", "beta"])),
        ]);
        let typed = coerce_carrier_parameters(&parameters, &raw, "query").unwrap();
        assert_eq!(typed["count"], json!(7));
        assert_eq!(typed["limit"], json!(3));
        assert_eq!(typed["labels"], json!(["alpha", "beta"]));

        let invalid = serde_json::Map::from_iter([
            ("count".to_string(), json!("7")),
            ("limit".to_string(), json!("11")),
        ]);
        let error = coerce_carrier_parameters(&parameters, &invalid, "query").unwrap_err();
        assert!(error.to_string().contains("signed validation rule"));
    }

    #[test]
    fn carrier_callable_routes_bind_validated_body_fields_to_named_arguments() {
        let input = json!({
            "request": {"legal_entity_id": "550e8400-e29b-41d4-a716-446655440000"},
            "include_zero": true
        });
        let arguments = carrier_callable_http_arguments(
            "carrier_action_trial_balance",
            &["request".to_string(), "include_zero".to_string()],
            Some(&input),
        )
        .unwrap();
        assert_eq!(arguments[0].0.as_deref(), Some("request"));
        assert_eq!(
            arguments[0].1["legal_entity_id"],
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(
            arguments[1],
            (Some("include_zero".to_string()), json!(true))
        );

        let missing_optional = carrier_callable_http_arguments(
            "carrier_action_trial_balance",
            &["optional_note".to_string()],
            Some(&json!({})),
        )
        .unwrap();
        assert_eq!(missing_optional[0].1, Value::Null);
        assert!(carrier_callable_http_arguments(
            "carrier_action_trial_balance",
            &["request".to_string()],
            Some(&json!([])),
        )
        .is_err());
    }

    #[test]
    fn complete_carrier_request_type_corpus_coerces_or_normalizes() {
        let scalar_cases = vec![
            (ApplicationRouteParameterTypeV1::String, json!("value")),
            (ApplicationRouteParameterTypeV1::Int, json!("42")),
            (ApplicationRouteParameterTypeV1::Float, json!("4.25")),
            (ApplicationRouteParameterTypeV1::Decimal, json!("12.340")),
            (ApplicationRouteParameterTypeV1::Bool, json!("true")),
            (
                ApplicationRouteParameterTypeV1::Json,
                json!("{\"ok\":true}"),
            ),
            (
                ApplicationRouteParameterTypeV1::Timestamp,
                json!("2026-07-31T10:20:30-07:00"),
            ),
            (ApplicationRouteParameterTypeV1::Date, json!("2026-07-31")),
            (
                ApplicationRouteParameterTypeV1::LocalDateTime,
                json!("2026-07-31T10:20:30"),
            ),
            (
                ApplicationRouteParameterTypeV1::TimeZone,
                json!("America/Los_Angeles"),
            ),
            (
                ApplicationRouteParameterTypeV1::Uuid,
                json!("550e8400-e29b-41d4-a716-446655440000"),
            ),
            (
                ApplicationRouteParameterTypeV1::Enum {
                    values: BTreeSet::from(["ready".to_string()]),
                },
                json!("ready"),
            ),
            (ApplicationRouteParameterTypeV1::Null, Value::Null),
        ];
        for (value_type, input) in scalar_cases {
            coerce_carrier_parameter_value(&input, &value_type, "query", "value")
                .unwrap_or_else(|error| panic!("{value_type:?} failed: {error}"));
        }

        let rich_cases = vec![
            (
                ApplicationRouteParameterTypeV1::List {
                    element: Box::new(ApplicationRouteParameterTypeV1::Int),
                },
                json!(["1", "2"]),
            ),
            (
                ApplicationRouteParameterTypeV1::Set {
                    element: Box::new(ApplicationRouteParameterTypeV1::String),
                },
                json!(["a", "a", "b"]),
            ),
            (
                ApplicationRouteParameterTypeV1::Optional {
                    value: Box::new(ApplicationRouteParameterTypeV1::String),
                },
                Value::Null,
            ),
            (
                ApplicationRouteParameterTypeV1::Object {
                    fields: vec![ApplicationRouteParameterV1 {
                        name: "amount".to_string(),
                        value_type: ApplicationRouteParameterTypeV1::Decimal,
                        optional: false,
                        default_json: None,
                        validations: Vec::new(),
                    }],
                },
                json!({"amount": 12.5}),
            ),
            (
                ApplicationRouteParameterTypeV1::Map {
                    key: Box::new(ApplicationRouteParameterTypeV1::String),
                    value: Box::new(ApplicationRouteParameterTypeV1::Int),
                },
                json!([{"key":"one","value":1}]),
            ),
            (
                ApplicationRouteParameterTypeV1::Vector { dimensions: 3 },
                json!([1.0, 2.0, 3.0]),
            ),
            (
                ApplicationRouteParameterTypeV1::Point,
                json!({"type":"Point","coordinates":[-122.4,37.8]}),
            ),
            (
                ApplicationRouteParameterTypeV1::LineString,
                json!({"type":"LineString","coordinates":[[0.0,0.0],[1.0,1.0]]}),
            ),
            (
                ApplicationRouteParameterTypeV1::Polygon,
                json!({"type":"Polygon","coordinates":[[[0.0,0.0],[1.0,0.0],[1.0,1.0],[0.0,0.0]]]}),
            ),
        ];
        for (value_type, input) in rich_cases {
            coerce_carrier_parameter_value(&input, &value_type, "query", "value")
                .unwrap_or_else(|error| panic!("{value_type:?} failed: {error}"));
        }
    }

    #[test]
    fn carrier_route_globals_preserve_request_data_but_withhold_credentials() {
        let request = ApplicationHttpRequest {
            method: HttpMethod::Post,
            path: "/widgets/7".to_string(),
            query: vec![
                ("tag".to_string(), "alpha".to_string()),
                ("tag".to_string(), "beta".to_string()),
            ],
            headers: vec![
                ("X-Request-Meta".to_string(), "present".to_string()),
                ("Authorization".to_string(), "Bearer secret".to_string()),
                ("Cookie".to_string(), "session=abc; theme=dark".to_string()),
            ],
            body: HttpRequestBodyV2::Json(json!({"enabled": true})),
            peer_address: None,
        };
        let contract = ApplicationRouteRequestV1 {
            path_parameters: vec![ApplicationRouteParameterV1 {
                name: "id".to_string(),
                value_type: ApplicationRouteParameterTypeV1::Int,
                optional: false,
                default_json: None,
                validations: Vec::new(),
            }],
            query_parameters: vec![ApplicationRouteParameterV1 {
                name: "tag".to_string(),
                value_type: ApplicationRouteParameterTypeV1::List {
                    element: Box::new(ApplicationRouteParameterTypeV1::String),
                },
                optional: false,
                default_json: None,
                validations: Vec::new(),
            }],
            body: Some(ApplicationRouteParameterTypeV1::Object {
                fields: vec![ApplicationRouteParameterV1 {
                    name: "enabled".to_string(),
                    value_type: ApplicationRouteParameterTypeV1::Bool,
                    optional: false,
                    default_json: None,
                    validations: Vec::new(),
                }],
            }),
        };
        let actor = anonymous_actor("trace".to_string(), None, None, 1).unwrap();
        let globals = carrier_route_globals(
            &request,
            &BTreeMap::from([("id".to_string(), "7".to_string())]),
            Some(&contract),
            &actor,
            "request-1",
        )
        .unwrap();
        assert_eq!(globals["params"]["id"], 7);
        assert_eq!(globals["query"]["tag"], json!(["alpha", "beta"]));
        assert_eq!(globals["headers"]["x-request-meta"], "present");
        assert!(globals["headers"].get("authorization").is_none());
        assert!(globals["headers"].get("cookie").is_none());
        assert_eq!(globals["cookies"], json!({}));
        assert_eq!(globals["input"], json!({"enabled": true}));
    }

    #[test]
    fn host_session_cookie_selection_fails_closed_on_ambiguous_credentials() {
        let mut policy = HttpHostPolicy::default();
        policy.session_cookie_name = Some("__Host-bicdb-session".to_string());

        assert_eq!(
            authentication_credential(
                &[(
                    "Cookie".to_string(),
                    "theme=dark; __Host-bicdb-session=session-a".to_string()
                )],
                &policy,
            )
            .unwrap()
            .as_deref(),
            Some("session-a")
        );
        assert!(authentication_credential(
            &[
                ("Authorization".to_string(), "Bearer one".to_string()),
                ("Cookie".to_string(), "__Host-bicdb-session=two".to_string(),),
            ],
            &policy,
        )
        .is_err());
        assert!(authentication_credential(
            &[(
                "Cookie".to_string(),
                "__Host-bicdb-session=one; __Host-bicdb-session=two".to_string(),
            )],
            &policy,
        )
        .is_err());
    }

    #[test]
    fn carrier_response_contract_normalizes_records_and_fails_as_provider_error() {
        let schema = ApplicationRouteParameterTypeV1::Object {
            fields: vec![ApplicationRouteParameterV1 {
                name: "count".to_string(),
                value_type: ApplicationRouteParameterTypeV1::Int,
                optional: false,
                default_json: None,
                validations: Vec::new(),
            }],
        };
        let mut valid = ApplicationHttpResponse {
            status: 200,
            headers: Vec::new(),
            body: HttpResponseBodyV2::Json(json!({"count": 4, "ignored": true})),
            trailers: Vec::new(),
            retry_after_ms: None,
        };
        normalize_carrier_response(&mut valid, &schema, "typed").unwrap();
        assert_eq!(valid.body, HttpResponseBodyV2::Json(json!({"count": 4})));

        let mut invalid = ApplicationHttpResponse {
            status: 200,
            headers: Vec::new(),
            body: HttpResponseBodyV2::Json(json!({"count": "4"})),
            trailers: Vec::new(),
            retry_after_ms: None,
        };
        let error = normalize_carrier_response(&mut invalid, &schema, "typed").unwrap_err();
        assert!(matches!(error, AppRuntimeError::Provider(_)));
        assert!(error.to_string().contains("signed response type"));
    }

    #[tokio::test]
    async fn authored_carrier_failure_preserves_problem_code_and_message() {
        let response = error_response(AppRuntimeError::ApplicationFailure {
            code: "patient_portal_denied".to_string(),
            message: "communication is unavailable".to_string(),
            retryable: false,
        });
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], "patient_portal_denied");
        assert_eq!(body["message"], "communication is unavailable");
        assert_eq!(
            body["message_key"],
            "runtime.errors.patient_portal_denied.e669c1e154e0cd4f"
        );
        assert_eq!(body["parameters"], json!({}));
        assert!(body["request_id"]
            .as_str()
            .is_some_and(|value| !value.is_empty()));
    }

    #[test]
    fn host_security_headers_override_application_values_without_duplication() {
        let mut response = ApplicationHttpResponse {
            status: 200,
            headers: Vec::new(),
            body: HttpResponseBodyV2::Json(json!({"ok": true})),
            trailers: Vec::new(),
            retry_after_ms: None,
        };
        apply_signed_response_headers(
            &mut response,
            &BTreeMap::from([
                (
                    "content-security-policy".to_string(),
                    "default-src 'self'".to_string(),
                ),
                ("x-frame-options".to_string(), "SAMEORIGIN".to_string()),
            ]),
        );
        let response = with_http_policy_headers(response, None, &HttpHostPolicy::default());
        assert_eq!(
            response
                .headers
                .iter()
                .filter(|(name, _)| name.eq_ignore_ascii_case("content-security-policy"))
                .count(),
            1
        );
        assert!(response.headers.contains(&(
            "content-security-policy".to_string(),
            "default-src 'none'; frame-ancestors 'none'".to_string()
        )));
        assert!(response
            .headers
            .contains(&("x-frame-options".to_string(), "DENY".to_string())));
    }

    #[test]
    fn w3c_trace_context_drives_sampling_correlation_and_response_headers() {
        let headers = vec![
            (
                "traceparent".to_string(),
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            ),
            ("tracestate".to_string(), "vendor=value".to_string()),
            ("x-request-id".to_string(), "request-7".to_string()),
            ("x-correlation-id".to_string(), "correlation-4".to_string()),
        ];
        let trace = request_trace_context(&headers);
        assert_eq!(trace.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(trace.parent_sampled, Some(true));
        assert!(trace_sampled(
            ApplicationSamplingV1::ParentBasedAlwaysOff,
            &trace
        ));
        let actor = apply_request_trace_context(
            anonymous_actor(trace.trace_id.clone(), None, None, 1).unwrap(),
            "request-7",
            "correlation-4",
            &trace,
            ApplicationSamplingV1::ParentBasedAlwaysOff,
        );
        assert_eq!(actor.correlation_id.as_deref(), Some("correlation-4"));
        assert_eq!(actor.causation_id.as_deref(), Some("request-7"));
        assert_eq!(
            actor.policy_attributes.get("carrier.trace.sampled"),
            Some(&"true".to_string())
        );

        let mut response = ApplicationHttpResponse {
            status: 200,
            headers: vec![("traceparent".to_string(), "forged".to_string())],
            body: HttpResponseBodyV2::Empty,
            trailers: Vec::new(),
            retry_after_ms: None,
        };
        apply_trace_response_headers(&mut response, &actor, "request-7");
        assert_eq!(
            header(&response.headers, "x-request-id").as_deref(),
            Some("request-7")
        );
        assert_eq!(
            header(&response.headers, "x-correlation-id").as_deref(),
            Some("correlation-4")
        );
        assert!(header(&response.headers, "traceparent")
            .unwrap()
            .starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"));
        assert_eq!(
            header(&response.headers, "tracestate").as_deref(),
            Some("vendor=value")
        );

        assert!(parse_traceparent("00-not-a-trace-id-00f067aa0ba902b7-01").is_none());
        assert!(request_identifier(
            &[("x-request-id".to_string(), "for\0ged".to_string())],
            "x-request-id"
        )
        .is_err());
    }
}
