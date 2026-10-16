use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use axum::{
    body::{to_bytes, Body},
    extract::{DefaultBodyLimit, Path as RoutePath, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{json, Value};
use subtle::ConstantTimeEq;
use tokio::sync::{RwLock, Semaphore};
use uuid::Uuid;

use crate::{
    database::{Database, QueryBatch},
    fleet::{Execution, Fleet},
    manifest::{Release, MAX_RELEASE_BYTES},
    runtime::{find_node, read_log, Process},
    state::{DatabaseHandle, Replica},
    store::{Deployment, Store},
};

const MAX_BODY: usize = MAX_RELEASE_BYTES * 4 / 3 + 2 * 1024 * 1024;
mod state_lifecycle;

pub struct Application {
    deployment: Mutex<Deployment>,
    database: DatabaseHandle,
    replica: Option<Arc<Replica>>,
    process: Mutex<Option<Execution>>,
    release: Mutex<Release>,
    token: Mutex<String>,
    gate: RwLock<()>,
    admission: Mutex<Arc<Semaphore>>,
    error: Mutex<Option<String>>,
    api_admission: Mutex<Arc<Semaphore>>,
    peers: Mutex<BTreeMap<String, crate::sync::Peer>>,
    sync_status: Mutex<BTreeMap<String, crate::sync::SyncStatus>>,
}

pub struct Platform {
    store: Store,
    applications: Mutex<BTreeMap<String, Arc<Application>>>,
    admin: tokio::sync::Mutex<()>,
    token: String,
    node: PathBuf,
    microvm: Option<crate::microvm::Host>,
    fleet: Option<Arc<Fleet>>,
    endpoint: String,
    client: reqwest::Client,
    recovering: AtomicBool,
    replicas: BTreeMap<String, Arc<Replica>>,
}

#[derive(Debug)]
struct ApiError(StatusCode, String);
type ApiResult<T> = std::result::Result<T, ApiError>;
impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        let message = format!("{error:#}");
        let status = if message.contains("state writer authority")
            || message.contains("state replica has no active writer authority")
            || message.contains("application state is reopening")
        {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::BAD_REQUEST
        };
        Self(status, message)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

pub(crate) fn authorized(headers: &HeaderMap, token: &str) -> bool {
    let expected = format!("Bearer {token}");
    headers
        .get("authorization")
        .map(|value| value.as_bytes().ct_eq(expected.as_bytes()).into())
        .unwrap_or(false)
}

impl Platform {
    pub fn open(
        root: &Path,
        token: String,
        node: Option<&Path>,
        endpoint: String,
        microvm_config: Option<&Path>,
        fleet_config: Option<&Path>,
        state_config: Option<&Path>,
    ) -> Result<Arc<Self>> {
        ensure!(
            token.len() >= 32 && !token.chars().any(char::is_whitespace),
            "management token must contain at least 32 non-whitespace characters"
        );
        let node = find_node(node)?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(2))
            .build()?;
        let fleet = fleet_config.map(Fleet::read).transpose()?;
        let store = Store::open(root)?;
        let microvm = microvm_config
            .map(|path| crate::microvm::Host::open(path, &store.root))
            .transpose()?;
        if microvm.is_none() {
            crate::microvm::fence_previous(&store.root)?;
        }
        let config = state_config.map(crate::state::Config::read).transpose()?;
        let mut replicas = BTreeMap::new();
        if let Some(config) = config {
            for deployment in store.deployments()? {
                ensure!(
                    config.applications.contains_key(&deployment.application),
                    "existing application {} lacks a state allocation",
                    deployment.application
                );
                ensure!(
                    store
                        .load_release(&deployment.application, &deployment.release)?
                        .manifest
                        .sync
                        .is_empty(),
                    "native state replication does not yet carry signed edge record journals"
                );
            }
            for (name, group) in config.applications {
                let path = store.app_dir(&name)?.join("database");
                replicas.insert(name, Replica::open(group, path)?);
            }
        }
        Ok(Arc::new(Self {
            store,
            microvm,
            fleet,
            applications: Mutex::new(BTreeMap::new()),
            admin: tokio::sync::Mutex::new(()),
            token,
            node,
            endpoint,
            client,
            recovering: AtomicBool::new(true),
            replicas,
        }))
    }

    fn app(&self, name: &str) -> ApiResult<Arc<Application>> {
        self.applications
            .lock()
            .get(name)
            .cloned()
            .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "application not found".into()))
    }

    fn admin_auth(&self, headers: &HeaderMap) -> ApiResult<()> {
        if !authorized(headers, &self.token) {
            return Err(ApiError(
                StatusCode::UNAUTHORIZED,
                "management authorization required".into(),
            ));
        }
        if self.recovering.load(Ordering::Acquire) {
            return Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "platform is recovering".into(),
            ));
        }
        Ok(())
    }

    fn create_app(&self, deployment: Deployment, release: Release) -> Result<Arc<Application>> {
        let replica = self.replicas.get(&deployment.application).cloned();
        let database = match &replica {
            Some(replica) => replica.database.clone(),
            None => DatabaseHandle::new(Database::open(
                &self
                    .store
                    .app_dir(&deployment.application)?
                    .join("database"),
            )?),
        };
        Ok(Arc::new(Application {
            admission: Mutex::new(Arc::new(Semaphore::new(
                release.manifest.resources.max_concurrent_requests,
            ))),
            api_admission: Mutex::new(Arc::new(Semaphore::new(
                release.manifest.resources.max_concurrent_requests.min(8),
            ))),
            deployment: Mutex::new(deployment.clone()),
            database,
            replica,
            process: Mutex::new(None),
            release: Mutex::new(release),
            token: Mutex::new(fresh_token()),
            gate: RwLock::new(()),
            error: Mutex::new(None),
            peers: Mutex::new(crate::sync::load_peers(
                &self
                    .store
                    .app_dir(&deployment.application)?
                    .join("peers.json"),
            )?),
            sync_status: Mutex::new(BTreeMap::new()),
        }))
    }

    fn launch(&self, app: &Application, release: &Release, digest: &str) -> Result<Execution> {
        app.require_authority()?;
        let name = &release.manifest.name;
        let app_dir = self.store.app_dir(name)?;
        self.store.load_release(name, digest)?;
        let directory = app_dir.join("releases").join(digest);
        let endpoint = format!("{}/internal/{name}", self.endpoint);
        self.validate_isolation(release)?;
        let token = fresh_token();
        *app.token.lock() = token.clone();
        let result = (|| -> Result<Execution> {
            if release.manifest.placement.is_some() {
                return self
                    .fleet
                    .as_ref()
                    .context("worker fleet is not configured")?
                    .launch(
                        release,
                        |token| *app.token.lock() = token,
                        app.replica
                            .as_ref()
                            .and_then(|replica| replica.lease.lock().clone()),
                        app.replica.as_ref().map(|replica| {
                            replica.group.nodes[&replica.group.node_id].endpoint.clone()
                        }),
                    );
            }
            let mut process = match release.manifest.isolation {
                crate::manifest::Isolation::Process => {
                    Process::start(release, &directory, &app_dir, &endpoint, &token, &self.node)?
                }
                crate::manifest::Isolation::Microvm => self
                    .microvm
                    .as_ref()
                    .context("microVM host is not configured")?
                    .launch(release, &app_dir, &token)?,
            };
            process.wait_ready(release, &token)?;
            app.require_authority()?;
            Ok(Execution::Local(process, token.clone()))
        })();
        if result.is_err() {
            *app.token.lock() = fresh_token();
        }
        result
    }

    fn validate_isolation(&self, release: &Release) -> Result<()> {
        let name = &release.manifest.name;
        if self
            .microvm
            .as_ref()
            .is_some_and(|host| host.allocations().contains_key(name))
        {
            ensure!(
                release.manifest.isolation == crate::manifest::Isolation::Microvm,
                "operator allocation requires microvm isolation for {name}"
            );
        }
        if release.manifest.placement.is_some() {
            return self
                .fleet
                .as_ref()
                .context("worker fleet is not configured")?
                .validate(release);
        }
        if release.manifest.isolation == crate::manifest::Isolation::Microvm {
            self.microvm
                .as_ref()
                .context("microVM host is not configured")?
                .require_application(name)?;
        }
        Ok(())
    }

    fn recover(&self) -> Result<()> {
        for deployment in self.store.deployments()? {
            if self.replicas.contains_key(&deployment.application) {
                continue;
            }
            let release = self
                .store
                .load_release(&deployment.application, &deployment.release)?;
            let app = self.create_app(deployment.clone(), release.clone())?;
            self.applications
                .lock()
                .insert(deployment.application.clone(), app.clone());
            if deployment.enabled {
                let prepared = app.database.lock()?.prepare(&release);
                let result =
                    prepared.and_then(|_| self.launch(&app, &release, &deployment.release));
                match result {
                    Ok(process) => *app.process.lock() = Some(process),
                    Err(error) => *app.error.lock() = Some(format!("{error:#}")),
                }
            }
        }
        self.recovering.store(false, Ordering::Release);
        Ok(())
    }

    fn deploy(&self, release: Release) -> Result<Deployment> {
        release.validate()?;
        self.validate_isolation(&release)?;
        ensure!(
            self.replicas.is_empty() || self.replicas.contains_key(&release.manifest.name),
            "application lacks an operator state allocation"
        );
        if let Some(replica) = self.replicas.get(&release.manifest.name) {
            ensure!(
                replica.authoritative(),
                "state writer authority is unavailable"
            );
            ensure!(
                release.manifest.sync.is_empty(),
                "native state replication does not yet carry signed edge record journals"
            );
        }
        let digest = self.store.save_release(&release)?;
        let name = &release.manifest.name;
        let existing = self.applications.lock().get(name).cloned();
        let old = existing.as_ref().map(|app| app.deployment.lock().clone());
        let deployment = Deployment {
            application: name.clone(),
            release: digest.clone(),
            previous: old
                .as_ref()
                .map(|d| d.release.clone())
                .filter(|d| d != &digest),
            generation: old
                .as_ref()
                .map(|d| d.generation.checked_add(1).context("generation exhausted"))
                .transpose()?
                .unwrap_or(1),
            enabled: true,
        };
        let app = match existing {
            Some(app) => app,
            None => {
                let mut initial = deployment.clone();
                initial.enabled = false;
                let app = self.create_app(initial, release.clone())?;
                self.applications.lock().insert(name.clone(), app.clone());
                app
            }
        };
        if old.is_some() && !app.release.lock().manifest.sync.is_empty() {
            ensure!(app.release.lock().manifest.sync == release.manifest.sync, "synchronization policy is immutable for an existing application; deploy a new application for a different scope");
        }
        // Persist the non-serving transition before any migration can commit.
        // A crash during activation must never restart the previous code against
        // a schema whose new migration history it does not cover.
        let mut pending = deployment.clone();
        pending.enabled = false;
        // Callers hold the async write gate, so all routed requests and jobs drained.
        *app.token.lock() = fresh_token();
        app.process.lock().take();
        *app.deployment.lock() = pending.clone();
        app.set_release(release.clone());
        self.persist_deployment(&app, &pending, &[&release])?;
        let result = (|| -> Result<Execution> {
            app.database.lock()?.prepare(&release)?;
            self.launch(&app, &release, &digest)
        })();
        match result {
            Ok(process) => {
                // Persist desired state before exposing the new route. A supervisor
                // crash here restarts the exact content-addressed release.
                app.require_authority()?;
                self.persist_deployment(&app, &deployment, &[&release])?;
                *app.error.lock() = None;
                *app.process.lock() = Some(process);
                *app.deployment.lock() = deployment.clone();
                Ok(deployment)
            }
            Err(error) => {
                *app.token.lock() = fresh_token();
                // Schema changes can already be committed. Do not silently run an
                // older application against a newer schema after failed activation.
                let mut disabled = deployment;
                disabled.enabled = false;
                self.persist_deployment(&app, &disabled, &[&release])?;
                *app.deployment.lock() = disabled;
                *app.error.lock() = Some(format!("{error:#}"));
                Err(error)
            }
        }
    }

    fn persist_deployment(
        &self,
        app: &Application,
        deployment: &Deployment,
        releases: &[&Release],
    ) -> Result<()> {
        app.require_authority()?;
        if app.replica.is_some() {
            app.database
                .lock()?
                .publish_deployment(deployment, releases)?;
        }
        self.store.save_deployment(deployment)
    }
}

impl Application {
    fn set_release(&self, release: Release) {
        *self.admission.lock() = Arc::new(Semaphore::new(
            release.manifest.resources.max_concurrent_requests,
        ));
        *self.api_admission.lock() = Arc::new(Semaphore::new(
            release.manifest.resources.max_concurrent_requests.min(8),
        ));
        *self.release.lock() = release;
    }
    fn authoritative(&self) -> bool {
        self.replica
            .as_ref()
            .is_none_or(|replica| replica.authoritative())
    }
    fn require_authority(&self) -> Result<()> {
        ensure!(
            self.authoritative(),
            "state writer authority is unavailable"
        );
        Ok(())
    }
}

pub fn router(platform: Arc<Platform>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/apps", get(list).post(deploy))
        .route("/v1/apps/:name/:operation", post(operation))
        .route("/v1/apps/:name/logs", get(logs))
        .route("/v1/apps/:name/jobs", get(job_status))
        .route("/v1/apps/:name/backup", post(backup))
        .route("/v1/apps/:name/identity", get(identity))
        .route("/v1/apps/:name/peers", get(peers).post(add_peer))
        .route(
            "/v1/apps/:name/peers/:node",
            axum::routing::delete(remove_peer),
        )
        .route("/sync/:name/vector", post(sync_vector))
        .route("/sync/:name/exchange", post(sync_exchange))
        .route("/internal/:name/query", post(query))
        .route("/internal/:name/jobs", post(enqueue))
        .route("/internal/:name/conflicts", post(conflicts))
        .route("/apps/:name", axum::routing::any(proxy))
        .route("/apps/:name/*path", axum::routing::any(proxy_path))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .layer(axum::middleware::from_fn_with_state(
            platform.clone(),
            authenticate,
        ))
        .with_state(platform)
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> ApiResult<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "platform task failed".into(),
            )
        })?
        .map_err(Into::into)
}

async fn health(State(platform): State<Arc<Platform>>) -> impl IntoResponse {
    if platform.recovering.load(Ordering::Acquire) {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"recovering"})),
        )
    } else {
        (StatusCode::OK, Json(json!({"status":"ready"})))
    }
}

async fn list(State(platform): State<Arc<Platform>>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    platform.admin_auth(&headers)?;
    let applications: Vec<_> = platform.applications.lock().values().cloned().collect();
    let mut apps: Vec<_> = applications
        .iter()
        .map(|app| {
            let deployment = app.deployment.lock().clone();
            let slot = app.process.lock();
            let running = slot.is_some() && app.authoritative();
            let worker = slot.as_ref().and_then(Execution::worker).map(str::to_owned);
            drop(slot);
            let error = app.error.lock().clone();
            json!({"application":deployment.application, "deployment": deployment, "running": running, "worker": worker, "error": error, "state":app.replica.as_ref().map(|replica| state_lifecycle::status(replica))})
        })
        .collect();
    for (name, replica) in &platform.replicas {
        if !applications
            .iter()
            .any(|app| app.deployment.lock().application == *name)
        {
            apps.push(json!({"application":name,"deployment":null,"running":false,"worker":null,"error":null,"state":state_lifecycle::status(replica)}));
        }
    }
    Ok(Json(json!(apps)))
}

async fn deploy(
    State(platform): State<Arc<Platform>>,
    headers: HeaderMap,
    Json(release): Json<Release>,
) -> ApiResult<Json<Value>> {
    platform.admin_auth(&headers)?;
    release.validate()?;
    let _admin = platform.admin.lock().await;
    let app = platform
        .applications
        .lock()
        .get(&release.manifest.name)
        .cloned();
    let _gate = match app.as_ref() {
        Some(app) => Some(app.gate.write().await),
        None => None,
    };
    let deployment = blocking({
        let platform = platform.clone();
        move || platform.deploy(release)
    })
    .await?;
    Ok(Json(json!(deployment)))
}

async fn operation(
    State(platform): State<Arc<Platform>>,
    RoutePath((name, operation)): RoutePath<(String, String)>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    platform.admin_auth(&headers)?;
    let _admin = platform.admin.lock().await;
    let app = platform.app(&name)?;
    let _gate = app.gate.write().await;
    let action = blocking({
        let app = app.clone();
        let platform = platform.clone();
        move || {
            let mut deployment = app.deployment.lock().clone();
            match operation.as_str() {
                "stop" => {
                    deployment.enabled = false;
                    app.require_authority()?;
                    *app.deployment.lock() = deployment.clone();
                    *app.token.lock() = fresh_token();
                    app.process.lock().take();
                    platform.persist_deployment(&app, &deployment, &[])?;
                    drop(app.database.lock()?);
                }
                "start" => {
                    if app.process.lock().is_none() {
                        let release = platform.store.load_release(&name, &deployment.release)?;
                        app.database.lock()?.prepare(&release)?;
                        app.set_release(release.clone());
                        let process = platform.launch(&app, &release, &deployment.release)?;
                        deployment.enabled = true;
                        platform.persist_deployment(&app, &deployment, &[])?;
                        *app.deployment.lock() = deployment.clone();
                        *app.process.lock() = Some(process);
                        *app.error.lock() = None;
                    }
                }
                "rollback" => {
                    let previous = deployment.previous.context("no previous release")?;
                    let release = platform.store.load_release(&name, &previous)?;
                    // migrate() rejects rollback that removes committed migrations.
                    return platform.deploy(release);
                }
                _ => anyhow::bail!("unknown application operation"),
            }
            Ok(deployment)
        }
    })
    .await?;
    Ok(Json(json!(action)))
}

async fn logs(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
) -> ApiResult<String> {
    platform.admin_auth(&headers)?;
    let app = platform.app(&name)?;
    let remote = app.process.lock().as_ref().and_then(Execution::log_request);
    if let Some((endpoint, token)) = remote {
        return blocking(move || {
            use std::io::Read;
            let response = reqwest::blocking::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(3))
                .build()?
                .get(endpoint)
                .bearer_auth(token)
                .send()?
                .error_for_status()?;
            let mut bytes = Vec::new();
            response.take(64 * 1024 + 1).read_to_end(&mut bytes)?;
            ensure!(bytes.len() <= 64 * 1024, "worker log response too large");
            Ok(String::from_utf8_lossy(&bytes).into_owned())
        })
        .await;
    }
    blocking(move || read_log(&platform.store.app_dir(&name)?.join("application.log"))).await
}

async fn query(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
    Json(batch): Json<QueryBatch>,
) -> ApiResult<Json<Value>> {
    let app = platform.app(&name)?;
    let token = app.token.lock().clone();
    if !authorized(&headers, &token) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "application authorization required".into(),
        ));
    }
    let deadline = std::time::Instant::now()
        + Duration::from_millis(app.release.lock().manifest.resources.request_timeout_ms);
    let result = blocking(move || {
        let mut database = app.database.lock()?;
        ensure!(
            *app.token.lock() == token,
            "application execution was replaced"
        );
        database.query_with_deadline(&batch, deadline)
    })
    .await?;
    Ok(Json(json!(result)))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Enqueue {
    payload: Value,
    #[serde(default)]
    idempotency_key: Option<String>,
}

async fn enqueue(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
    Json(job): Json<Enqueue>,
) -> ApiResult<Json<Value>> {
    let app = platform.app(&name)?;
    let token = app.token.lock().clone();
    if !authorized(&headers, &token) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "application authorization required".into(),
        ));
    }
    if let Some(key) = &job.idempotency_key {
        if key.is_empty() || key.len() > 256 {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "invalid idempotency key".into(),
            ));
        }
    }
    Ok(Json(
        blocking(move || {
            let mut database = app.database.lock()?;
            ensure!(
                *app.token.lock() == token,
                "application execution was replaced"
            );
            database.enqueue(job.payload, job.idempotency_key)
        })
        .await?,
    ))
}

async fn proxy(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    request: Request,
) -> ApiResult<Response> {
    forward(platform, name, String::new(), request).await
}
async fn proxy_path(
    State(platform): State<Arc<Platform>>,
    RoutePath((name, path)): RoutePath<(String, String)>,
    request: Request,
) -> ApiResult<Response> {
    forward(platform, name, path, request).await
}

pub(crate) fn hop_header(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
            | "content-length"
    ) || name.starts_with("x-bic-")
}

async fn forward(
    platform: Arc<Platform>,
    name: String,
    path: String,
    request: Request,
) -> ApiResult<Response> {
    if path.starts_with("__bic/") || path == "__bic" {
        return Err(ApiError(StatusCode::NOT_FOUND, "route not found".into()));
    }
    let app = platform.app(&name)?;
    app.require_authority()?;
    let _gate = app.gate.read().await;
    let _permit = app
        .admission
        .lock()
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            ApiError(
                StatusCode::TOO_MANY_REQUESTS,
                "application concurrency limit reached".into(),
            )
        })?;
    let (endpoint, client, worker_token, execution_token) = {
        let slot = app.process.lock();
        let process = slot.as_ref().ok_or_else(|| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "application is unavailable".into(),
            )
        })?;
        process.route()
    };
    let timeout = app.release.lock().manifest.resources.request_timeout_ms;
    let (parts, body) = request.into_parts();
    let bytes = tokio::time::timeout(Duration::from_secs(30), to_bytes(body, 8 * 1024 * 1024))
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::REQUEST_TIMEOUT,
                "request body deadline exceeded".into(),
            )
        })?
        .map_err(|_| ApiError(StatusCode::PAYLOAD_TOO_LARGE, "request too large".into()))?;
    // Preserve the original encoded path after the application prefix. Decoded
    // route captures are used only for authorization and never URL construction.
    let prefix = format!("/apps/{name}");
    let suffix = parts.uri.path().strip_prefix(&prefix).unwrap_or("/");
    let suffix = if suffix.is_empty() { "/" } else { suffix };
    let url = format!(
        "{endpoint}{suffix}{}",
        parts
            .uri
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default()
    );
    let mut upstream = client
        .request(parts.method, url)
        .timeout(Duration::from_millis(timeout));
    for (name, value) in &parts.headers {
        if !hop_header(name.as_str()) {
            upstream = upstream.header(name, value);
        }
    }
    if let Some(token) = worker_token {
        upstream = upstream.header("x-bic-worker-token", token);
    }
    let mut response = upstream.body(bytes).send().await.map_err(|error| {
        if error.is_timeout() {
            retire_execution(&app, &execution_token);
            ApiError(
                StatusCode::GATEWAY_TIMEOUT,
                "application execution deadline exceeded".into(),
            )
        } else {
            ApiError(StatusCode::BAD_GATEWAY, "application request failed".into())
        }
    })?;
    app.require_authority()?;
    let status = response.status();
    let headers = response.headers().clone();
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        if error.is_timeout() {
            retire_execution(&app, &execution_token);
            ApiError(
                StatusCode::GATEWAY_TIMEOUT,
                "application execution deadline exceeded".into(),
            )
        } else {
            ApiError(
                StatusCode::BAD_GATEWAY,
                "application response interrupted".into(),
            )
        }
    })? {
        app.require_authority()?;
        if bytes.len().saturating_add(chunk.len()) > 16 * 1024 * 1024 {
            return Err(ApiError(
                StatusCode::BAD_GATEWAY,
                "application response too large".into(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let mut result = Response::builder().status(status);
    for (name, value) in &headers {
        if !hop_header(name.as_str()) {
            result = result.header(name, value);
        }
    }
    result.body(Body::from(bytes)).map_err(|_| {
        ApiError(
            StatusCode::BAD_GATEWAY,
            "invalid application response".into(),
        )
    })
}

fn retire_execution(app: &Application, token: &str) {
    let mut slot = app.process.lock();
    if slot
        .as_ref()
        .is_some_and(|execution| execution.token() == token)
    {
        *app.token.lock() = fresh_token();
        slot.take();
    }
}

async fn supervise(platform: Arc<Platform>) {
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let apps: Vec<_> = platform.applications.lock().values().cloned().collect();
        for app in apps {
            if app.replica.is_some() {
                continue;
            }
            let Ok(_gate) = app.gate.try_read() else {
                continue;
            };
            let work = {
                let app = app.clone();
                let platform = platform.clone();
                move || -> Result<()> {
                    if !app.deployment.lock().enabled {
                        return Ok(());
                    }
                    let dead = {
                        let mut slot = app.process.lock();
                        let dead = match slot.as_mut() {
                            Some(process) => process.exited()?,
                            None => true,
                        };
                        if dead {
                            *app.token.lock() = fresh_token();
                            slot.take();
                        }
                        dead
                    };
                    if dead {
                        let release = app.release.lock().clone();
                        let digest = app.deployment.lock().release.clone();
                        let prepared = app.database.lock()?.prepare(&release);
                        match prepared.and_then(|_| platform.launch(&app, &release, &digest)) {
                            Ok(process) => {
                                *app.process.lock() = Some(process);
                                *app.error.lock() = None;
                            }
                            Err(error) => {
                                *app.token.lock() = fresh_token();
                                *app.error.lock() = Some(format!("{error:#}"));
                            }
                        }
                    }
                    Ok(())
                }
            };
            let _ = blocking(work).await;
        }
    }
}

async fn jobs(platform: Arc<Platform>) {
    let consumer = format!("worker-{}", Uuid::new_v4().simple());
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let apps: Vec<_> = platform.applications.lock().values().cloned().collect();
        for app in apps {
            if !app.authoritative() || !app.deployment.lock().enabled {
                continue;
            }
            let Ok(_gate) = app.gate.try_read() else {
                continue;
            };
            let Some((endpoint, client, worker_token, execution_token)) =
                app.process.lock().as_ref().map(Execution::route)
            else {
                continue;
            };
            let timeout = app.release.lock().manifest.resources.request_timeout_ms;
            let message = blocking({
                let app = app.clone();
                let consumer = consumer.clone();
                move || {
                    let mut database = app.database.lock()?;
                    database.drain_outbox(32)?;
                    database.consume(&consumer, timeout)
                }
            })
            .await;
            let Ok(Some(message)) = message else {
                if let Err(error) = &message {
                    eprintln!("background job admission failed: {}", error.1);
                }
                continue;
            };
            if !app.authoritative() {
                continue;
            }
            let token = execution_token.clone();
            let mut request = client.post(format!("{endpoint}/__bic/job"));
            if let Some(token) = worker_token {
                request = request.header("x-bic-worker-token", token);
            }
            let response = request
                .header("x-bic-internal-token", token).timeout(Duration::from_millis(timeout))
                .json(&json!({"id":message.message_id,"attempt":message.attempts,"payload":message.payload})).send().await;
            if response.as_ref().is_err_and(|error| error.is_timeout()) {
                retire_execution(&app, &execution_token);
            }
            let success = response.is_ok_and(|r| r.status().is_success());
            let consumer = consumer.clone();
            let app = app.clone();
            let _ =
                blocking(move || app.database.lock()?.settle(&consumer, &message, success)).await;
        }
    }
}

pub async fn serve(
    root: PathBuf,
    listen: SocketAddr,
    token: String,
    node: Option<PathBuf>,
    microvm_config: Option<PathBuf>,
    fleet_config: Option<PathBuf>,
    state_config: Option<PathBuf>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    let address = listener.local_addr()?;
    let local = SocketAddr::new(
        if address.ip().is_unspecified() {
            "127.0.0.1".parse()?
        } else {
            address.ip()
        },
        address.port(),
    );
    let platform = Platform::open(
        &root,
        token,
        node.as_deref(),
        format!("http://{local}"),
        microvm_config.as_deref(),
        fleet_config.as_deref(),
        state_config.as_deref(),
    )?;
    let mut private_listeners = Vec::new();
    if let Some(host) = &platform.microvm {
        for allocation in host.allocations().values() {
            private_listeners.push(
                tokio::net::TcpListener::bind(SocketAddr::from((
                    allocation.host_ip,
                    allocation.api_port,
                )))
                .await?,
            );
        }
    }
    let mut private_tasks = Vec::new();
    for listener in private_listeners {
        let platform = platform.clone();
        private_tasks.push(tokio::spawn(async move {
            axum::serve(listener, internal_router(platform)).await
        }));
    }
    let recovery = tokio::task::spawn_blocking({
        let platform = platform.clone();
        move || platform.recover()
    });
    let supervision = tokio::spawn({
        let platform = platform.clone();
        async move {
            match recovery.await {
                Ok(Ok(())) => {}
                other => {
                    eprintln!("platform recovery failed: {other:?}");
                    return;
                }
            }
            tokio::join!(
                supervise(platform.clone()),
                jobs(platform.clone()),
                synchronize(platform.clone()),
                state_lifecycle::run(platform.clone()),
                state_lifecycle::fence(platform)
            );
        }
    });
    println!("{}", json!({"listen":address,"data":root}));
    let result = axum::serve(listener, router(platform.clone()))
        .with_graceful_shutdown(shutdown_signal())
        .await;
    supervision.abort();
    for task in private_tasks {
        task.abort();
    }
    // Drop children before releasing the exclusive platform lock.
    for app in platform.applications.lock().values() {
        app.process.lock().take();
    }
    for replica in platform.replicas.values() {
        if let Some(database) = replica.database.0.lock().as_ref() {
            database.request_shutdown();
        }
    }
    result?;
    Ok(())
}

async fn identity(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    platform.admin_auth(&headers)?;
    let app = platform.app(&name)?;
    let identity = blocking(move || {
        crate::sync::identity(&*app.database.lock()?, &app.deployment.lock().release)
    })
    .await?;
    Ok(Json(json!(identity)))
}

async fn peers(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    platform.admin_auth(&headers)?;
    let app = platform.app(&name)?;
    let status = app.sync_status.lock();
    let peers: Vec<_> = app.peers.lock().values().map(|peer| json!({"node_id":peer.node_id,"public_key":peer.public_key,"endpoint":peer.endpoint,"status":status.get(&peer.node_id.to_string())})).collect();
    Ok(Json(json!(peers)))
}

async fn add_peer(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
    Json(peer): Json<crate::sync::Peer>,
) -> ApiResult<Json<Value>> {
    platform.admin_auth(&headers)?;
    peer.validate()?;
    if peer.token == platform.token {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "peer transport must use a separate token from management".into(),
        ));
    }
    let _admin = platform.admin.lock().await;
    let app = platform.app(&name)?;
    let _gate = app.gate.write().await;
    let result = blocking({
        let app = app.clone();
        let platform = platform.clone();
        move || {
            let database = app.database.lock()?;
            ensure!(
                database.db.read().node_id() != peer.node_id,
                "cannot add this database as its own peer"
            );
            database
                .db
                .write()
                .pin_node_key(&peer.node_id, &peer.public_key)?;
            let mut peers = app.peers.lock().clone();
            peers.insert(peer.node_id.to_string(), peer.clone());
            crate::sync::save_peers(&platform.store.app_dir(&name)?.join("peers.json"), &peers)?;
            *app.peers.lock() = peers;
            Ok(json!({"node_id":peer.node_id,"configured":true}))
        }
    })
    .await?;
    Ok(Json(result))
}

fn peer_auth(
    app: &Application,
    headers: &HeaderMap,
    identity: &crate::sync::Identity,
) -> ApiResult<()> {
    let peers = app.peers.lock();
    let peer = peers
        .get(&identity.node_id.to_string())
        .ok_or_else(|| ApiError(StatusCode::UNAUTHORIZED, "peer is not authorized".into()))?;
    if !authorized(headers, &peer.token) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "peer authorization required".into(),
        ));
    }
    let deployment = app.deployment.lock();
    if !deployment.enabled {
        return Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "application synchronization is stopped".into(),
        ));
    }
    crate::sync::verify_identity(peer, identity, &deployment.release)?;
    Ok(())
}

async fn sync_vector(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
    Json(identity): Json<crate::sync::Identity>,
) -> ApiResult<Json<crate::sync::VectorResponse>> {
    let app = platform.app(&name)?;
    let _gate = app.gate.read().await;
    peer_auth(&app, &headers, &identity)?;
    let response = blocking({
        let app = app.clone();
        move || {
            let database = app.database.lock()?;
            let vector = database.db.read().sync_vector()?;
            Ok(crate::sync::VectorResponse {
                identity: crate::sync::identity(&database, &app.deployment.lock().release)?,
                vector,
            })
        }
    })
    .await?;
    Ok(Json(response))
}

fn validate_origins(app: &Application, exchange: &crate::sync::Exchange) -> Result<()> {
    let own = app.database.lock()?.db.read().node_id();
    let peers = app.peers.lock();
    for event in &exchange.bundle.events {
        ensure!(
            event.envelope.node_id == own
                || peers.contains_key(&event.envelope.node_id.to_string()),
            "bundle contains an unprovisioned author"
        );
    }
    Ok(())
}

async fn sync_exchange(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
    Json(exchange): Json<crate::sync::Exchange>,
) -> ApiResult<Json<crate::sync::Exchange>> {
    let app = platform.app(&name)?;
    let _gate = app.gate.read().await;
    peer_auth(&app, &headers, &exchange.identity)?;
    validate_origins(&app, &exchange)?;
    let response = blocking({
        let app = app.clone();
        move || {
            let release = app.release.lock().clone();
            let digest = app.deployment.lock().release.clone();
            let vector = exchange.vector.clone();
            let mut database = app.database.lock()?;
            crate::sync::import(&mut database, &release, exchange)?;
            crate::sync::export(&mut database, &release, &digest, &vector)
        }
    })
    .await?;
    Ok(Json(response))
}

async fn sync_peer(
    platform: Arc<Platform>,
    app: Arc<Application>,
    peer: crate::sync::Peer,
) -> Result<()> {
    let local = { crate::sync::identity(&*app.database.lock()?, &app.deployment.lock().release)? };
    let name = app.deployment.lock().application.clone();
    let base = peer.endpoint.trim_end_matches('/');
    let response = platform
        .client
        .post(format!("{base}/sync/{name}/vector"))
        .bearer_auth(&peer.token)
        .timeout(Duration::from_secs(10))
        .json(&local)
        .send()
        .await?
        .error_for_status()?;
    let remote: crate::sync::VectorResponse = bounded_json(response, 64 * 1024).await?;
    crate::sync::verify_identity(&peer, &remote.identity, &local.release)?;
    let exchange = blocking({
        let app = app.clone();
        move || {
            let release = app.release.lock().clone();
            let mut database = app.database.lock()?;
            crate::sync::export(&mut database, &release, &local.release, &remote.vector)
        }
    })
    .await
    .map_err(|error| anyhow::anyhow!(error.1))?;
    let expected_release = exchange.identity.release.clone();
    let response = platform
        .client
        .post(format!("{base}/sync/{name}/exchange"))
        .bearer_auth(&peer.token)
        .timeout(Duration::from_secs(30))
        .json(&exchange)
        .send()
        .await?
        .error_for_status()?;
    ensure!(
        response
            .content_length()
            .is_none_or(|len| len <= 20 * 1024 * 1024),
        "peer response too large"
    );
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len().saturating_add(chunk.len()) <= 20 * 1024 * 1024,
            "peer response too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    let exchange: crate::sync::Exchange = serde_json::from_slice(&bytes)?;
    crate::sync::verify_identity(&peer, &exchange.identity, &expected_release)?;
    validate_origins(&app, &exchange)?;
    blocking(move || {
        let release = app.release.lock().clone();
        let mut database = app.database.lock()?;
        crate::sync::import(&mut database, &release, exchange)
    })
    .await
    .map_err(|error| anyhow::anyhow!(error.1))
}

async fn synchronize(platform: Arc<Platform>) {
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let apps: Vec<_> = platform.applications.lock().values().cloned().collect();
        for app in apps {
            let Ok(_gate) = app.gate.try_read() else {
                continue;
            };
            if !app.deployment.lock().enabled {
                continue;
            }
            let peers: Vec<_> = app.peers.lock().values().cloned().collect();
            for peer in peers {
                let result = sync_peer(platform.clone(), app.clone(), peer.clone()).await;
                let mut statuses = app.sync_status.lock();
                let status = statuses.entry(peer.node_id.to_string()).or_default();
                match result {
                    Ok(()) => {
                        status.last_success_unix_ms = Some(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64);
                        status.last_error = None;
                    }
                    // The status intentionally excludes URLs, credentials and provider errors.
                    Err(_) => status.last_error = Some("synchronization failed; check reachability, release and peer authorization".into()),
                }
            }
        }
    }
}

async fn remove_peer(
    State(platform): State<Arc<Platform>>,
    RoutePath((name, node)): RoutePath<(String, String)>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    platform.admin_auth(&headers)?;
    let _admin = platform.admin.lock().await;
    let app = platform.app(&name)?;
    let _gate = app.gate.write().await;
    let result = blocking({
        let app = app.clone();
        let platform = platform.clone();
        move || {
            let mut peers = app.peers.lock().clone();
            ensure!(peers.remove(&node).is_some(), "peer not found");
            crate::sync::save_peers(&platform.store.app_dir(&name)?.join("peers.json"), &peers)?;
            *app.peers.lock() = peers;
            app.sync_status.lock().remove(&node);
            Ok(json!({"node_id":node,"configured":false}))
        }
    })
    .await?;
    Ok(Json(result))
}

async fn authenticate(
    State(platform): State<Arc<Platform>>,
    mut request: Request,
    next: axum::middleware::Next,
) -> Response {
    let path = request.uri().path().to_owned();
    let mut _permit = None;
    if path.starts_with("/v1/") {
        if let Err(error) = platform.admin_auth(request.headers()) {
            return error.into_response();
        }
    }
    if !platform.replicas.is_empty() {
        match state_lifecycle::route(&platform, request).await {
            Ok(state_lifecycle::Routed::Local(local)) => request = local,
            Ok(state_lifecycle::Routed::Response(response)) => return response,
            Err(error) => return error.into_response(),
        }
    }
    if path.starts_with("/internal/") || path.starts_with("/sync/") {
        let Some(name) = path.split('/').nth(2) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        let app = match platform.app(name) {
            Ok(app) => app,
            Err(error) => return error.into_response(),
        };
        let allowed = if path.starts_with("/internal/") {
            authorized(request.headers(), &app.token.lock())
        } else {
            app.peers
                .lock()
                .values()
                .any(|peer| authorized(request.headers(), &peer.token))
        };
        if allowed && path.starts_with("/internal/") {
            _permit = match app.api_admission.lock().clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    return ApiError(
                        StatusCode::TOO_MANY_REQUESTS,
                        "database admission limit reached".into(),
                    )
                    .into_response()
                }
            };
        }
        if !allowed {
            return ApiError(
                StatusCode::UNAUTHORIZED,
                "scoped authorization required".into(),
            )
            .into_response();
        }
        if !app.authoritative() {
            return ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "state writer authority is unavailable".into(),
            )
            .into_response();
        }
    }
    next.run(request).await
}

async fn bounded_json<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<T> {
    ensure!(
        response
            .content_length()
            .is_none_or(|len| len <= limit as u64),
        "peer response too large"
    );
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len().saturating_add(chunk.len()) <= limit,
            "peer response too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConflictQuery {
    table: String,
}

async fn conflicts(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
    Json(query): Json<ConflictQuery>,
) -> ApiResult<Json<Value>> {
    let app = platform.app(&name)?;
    let token = app.token.lock().clone();
    if !authorized(&headers, &token) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "application authorization required".into(),
        ));
    }
    Ok(Json(
        blocking(move || {
            let database = app.database.lock()?;
            ensure!(
                *app.token.lock() == token,
                "application execution was replaced"
            );
            database.conflicts(&query.table)
        })
        .await?,
    ))
}

async fn job_status(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    platform.admin_auth(&headers)?;
    let app = platform.app(&name)?;
    Ok(Json(
        blocking(move || app.database.lock()?.job_status()).await?,
    ))
}

pub(crate) async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupRequest {
    passphrase: String,
}

async fn backup(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
    Json(request): Json<BackupRequest>,
) -> ApiResult<Response> {
    platform.admin_auth(&headers)?;
    if request.passphrase.len() < 32 || request.passphrase.len() > 4096 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "backup passphrase must contain 32 to 4096 bytes".into(),
        ));
    }
    let _admin = platform.admin.lock().await;
    let app = platform.app(&name)?;
    let _gate = app.gate.write().await;
    let path = blocking({
        let app = app.clone();
        let platform = platform.clone();
        move || {
            let deployment = app.deployment.lock().clone();
            let release = platform.store.load_release(&name, &deployment.release)?;
            let previous = deployment
                .previous
                .as_ref()
                .map(|digest| platform.store.load_release(&name, digest))
                .transpose()?;
            let snapshots = platform.store.root.join("snapshots");
            std::fs::create_dir_all(&snapshots)?;
            let path = snapshots.join(format!("{}.bicbak", Uuid::new_v4()));
            crate::recovery::snapshot(
                &mut *app.database.lock()?,
                deployment,
                release,
                previous,
                &path,
                &request.passphrase,
            )?;
            Ok(path)
        }
    })
    .await?;
    let file = std::fs::File::open(&path).map_err(anyhow::Error::from)?;
    let size = file.metadata().map_err(anyhow::Error::from)?.len();
    // Unix is the supervisor's supported host. The opened inode remains readable
    // while unlinking prevents completed/interrupted downloads accumulating files.
    std::fs::remove_file(&path).map_err(anyhow::Error::from)?;
    let stream = tokio_util::io::ReaderStream::new(tokio::fs::File::from_std(file));
    Response::builder()
        .header("content-type", "application/octet-stream")
        .header("content-length", size)
        .body(Body::from_stream(stream))
        .map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "backup response failed".into(),
            )
        })
}

fn internal_router(platform: Arc<Platform>) -> Router {
    Router::new()
        .route("/internal/:name/query", post(query))
        .route("/internal/:name/jobs", post(enqueue))
        .route("/internal/:name/conflicts", post(conflicts))
        .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
        .layer(axum::middleware::from_fn_with_state(
            platform.clone(),
            authenticate,
        ))
        .with_state(platform)
}

impl Drop for Platform {
    fn drop(&mut self) {
        for app in self.applications.lock().values() {
            app.process.lock().take();
            app.database.0.lock().take();
        }
        for replica in self.replicas.values() {
            *replica.lease.lock() = None;
            replica.database.0.lock().take();
        }
        // Network resources leave before Store releases its ownership lock.
        self.microvm.take();
    }
}

pub(crate) fn fresh_token() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}
