//! Leased compute worker. Durable SQL, receipts and jobs stay at the state service.
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::{ensure, Context, Result};
use axum::{
    body::{to_bytes, Body},
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use parking_lot::Mutex;
use serde_json::json;

use crate::{
    fleet::{Launch, LEASE},
    manifest::{Isolation, MAX_RELEASE_BYTES},
    runtime::Process,
    store::Store,
};

struct Running {
    process: Process,
    expires: Instant,
}
struct Instance {
    name: String,
    token: String,
    state_endpoint: String,
    running: Mutex<Option<Running>>,
    starting: AtomicBool,
    created: Instant,
}
struct Worker {
    store: Store,
    token: String,
    node: PathBuf,
    endpoint: String,
    state_endpoint: String,
    state_peers: Vec<String>,
    capacity: usize,
    microvm: Option<crate::microvm::Host>,
    instances: Mutex<BTreeMap<String, Arc<Instance>>>,
    admin: tokio::sync::Mutex<()>,
    client: reqwest::Client,
}

pub struct StateOrigins {
    pub primary: String,
    pub peers: Vec<String>,
}

type Error = (StatusCode, &'static str);
fn denied() -> Error {
    (StatusCode::UNAUTHORIZED, "worker authorization required")
}
fn unavailable() -> Error {
    (StatusCode::SERVICE_UNAVAILABLE, "execution unavailable")
}
fn failure() -> Error {
    (
        StatusCode::BAD_REQUEST,
        "worker activation failed; inspect operator logs",
    )
}

impl Worker {
    fn authenticate(&self, headers: &HeaderMap) -> std::result::Result<(), Error> {
        if crate::server::authorized(headers, &self.token) {
            Ok(())
        } else {
            Err(denied())
        }
    }
    fn instance(&self, id: &str) -> std::result::Result<Arc<Instance>, Error> {
        self.instances
            .lock()
            .get(id)
            .cloned()
            .ok_or_else(unavailable)
    }
    fn launch(&self, launch: Launch) -> Result<()> {
        ensure!(
            launch.id.len() == 32 && launch.id.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid execution id"
        );
        ensure!(
            launch.token.len() >= 32
                && launch.token.len() <= 256
                && !launch.token.chars().any(char::is_whitespace),
            "invalid execution capability"
        );
        launch.release.validate()?;
        let state_endpoint = launch
            .state_endpoint
            .as_deref()
            .unwrap_or(&self.state_endpoint)
            .trim_end_matches('/')
            .to_owned();
        ensure!(
            state_endpoint == self.state_endpoint || self.state_peers.contains(&state_endpoint),
            "execution state origin is not operator-pinned"
        );
        ensure!(
            launch.release.manifest.placement.is_some(),
            "remote placement is required"
        );
        let name = &launch.release.manifest.name;
        let mut instances = self.instances.lock();
        ensure!(
            !instances.contains_key(&launch.id),
            "execution identity already exists"
        );
        // A worker belongs to one state service. Replacing an application's
        // execution cannot accumulate old instances under different identifiers.
        let prior: Vec<_> = instances
            .iter()
            .filter(|(_, entry)| entry.name == *name)
            .map(|(id, _)| id.clone())
            .collect();
        for id in prior {
            if let Some(entry) = instances.remove(&id) {
                entry.running.lock().take();
            }
        }
        ensure!(instances.len() < self.capacity, "worker capacity exhausted");
        let instance = Arc::new(Instance {
            name: name.clone(),
            token: launch.token.clone(),
            state_endpoint,
            running: Mutex::new(None),
            starting: AtomicBool::new(true),
            created: Instant::now(),
        });
        instances.insert(launch.id.clone(), instance.clone());
        drop(instances);
        let result = (|| -> Result<Process> {
            let digest = self.store.save_release(&launch.release)?;
            let app_dir = self.store.app_dir(name)?;
            let mut process = match launch.release.manifest.isolation {
                Isolation::Process => {
                    ensure!(
                        !self
                            .microvm
                            .as_ref()
                            .is_some_and(|host| host.allocations().contains_key(name)),
                        "operator allocation requires microvm isolation"
                    );
                    Process::start(
                        &launch.release,
                        &app_dir.join("releases").join(digest),
                        &app_dir,
                        &format!("{}/internal/{name}", self.endpoint),
                        &launch.token,
                        &self.node,
                    )?
                }
                Isolation::Microvm => self
                    .microvm
                    .as_ref()
                    .context("microVM host not configured")?
                    .launch(&launch.release, &app_dir, &launch.token)?,
            };
            process.wait_ready(&launch.release, &launch.token)?;
            Ok(process)
        })();
        match result {
            Ok(process) => {
                *instance.running.lock() = Some(Running {
                    process,
                    expires: Instant::now() + LEASE,
                })
            }
            Err(error) => {
                self.instances.lock().remove(&launch.id);
                return Err(error);
            }
        }
        instance.starting.store(false, Ordering::Release);
        Ok(())
    }
}

async fn launch(
    State(worker): State<Arc<Worker>>,
    headers: HeaderMap,
    Json(launch): Json<Launch>,
) -> std::result::Result<StatusCode, Error> {
    worker.authenticate(&headers)?;
    let _admin = worker.admin.lock().await;
    tokio::task::spawn_blocking({
        let worker = worker.clone();
        move || worker.launch(launch)
    })
    .await
    .map_err(|_| failure())?
    .map_err(|error| {
        eprintln!("worker activation: {error:#}");
        failure()
    })?;
    Ok(StatusCode::CREATED)
}

async fn control(
    State(worker): State<Arc<Worker>>,
    Path((id, operation)): Path<(String, String)>,
    headers: HeaderMap,
) -> std::result::Result<StatusCode, Error> {
    worker.authenticate(&headers)?;
    let entry = worker.instance(&id)?;
    let mut running = entry.running.lock();
    if operation == "stop" {
        running.take();
        return Ok(StatusCode::NO_CONTENT);
    }
    if operation != "renew" {
        return Err((StatusCode::NOT_FOUND, "operation not found"));
    }
    let Some(runtime) = running.as_mut() else {
        return Err(unavailable());
    };
    if runtime.expires <= Instant::now() || runtime.process.exited().map_err(|_| unavailable())? {
        running.take();
        return Err(unavailable());
    }
    runtime.expires = Instant::now() + LEASE;
    Ok(StatusCode::NO_CONTENT)
}

async fn logs(
    State(worker): State<Arc<Worker>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<String, Error> {
    worker.authenticate(&headers)?;
    let instance = worker.instance(&id)?;
    let path = worker
        .store
        .app_dir(&instance.name)
        .map_err(|_| unavailable())?
        .join("application.log");
    tokio::task::spawn_blocking(move || crate::runtime::read_log(&path))
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())
}

async fn authenticate(
    State(worker): State<Arc<Worker>>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    let path = request.uri().path();
    let allowed = if path == "/health" {
        true
    } else if path.starts_with("/v1/") {
        worker.authenticate(request.headers()).is_ok()
    } else if path.starts_with("/run/") {
        let id = path.split('/').nth(2).unwrap_or_default();
        worker.instance(id).is_ok_and(|instance| {
            use subtle::ConstantTimeEq;
            request
                .headers()
                .get("x-bic-worker-token")
                .is_some_and(|token| bool::from(token.as_bytes().ct_eq(instance.token.as_bytes())))
        })
    } else if path.starts_with("/internal/") {
        let name = path.split('/').nth(2).unwrap_or_default();
        worker.instances.lock().values().any(|instance| {
            instance.name == name && crate::server::authorized(request.headers(), &instance.token)
        })
    } else {
        false
    };
    if !allowed {
        return denied().into_response();
    }
    next.run(request).await
}

async fn forward(
    State(worker): State<Arc<Worker>>,
    request: Request,
) -> std::result::Result<Response, Error> {
    let path = request.uri().path().to_owned();
    let (target, client, _instance) = if path.starts_with("/run/") {
        let id = path.split('/').nth(2).unwrap_or_default();
        let instance = worker.instance(id)?;
        let (address, client) = {
            let mut slot = instance.running.lock();
            let running = slot.as_mut().ok_or_else(unavailable)?;
            if running.expires <= Instant::now() {
                slot.take();
                return Err(unavailable());
            }
            (running.process.address, running.process.client.clone())
        };
        let prefix = format!("/run/{id}");
        let suffix = path.strip_prefix(&prefix).unwrap_or("/");
        (
            format!(
                "http://{address}{}",
                if suffix.is_empty() { "/" } else { suffix }
            ),
            client,
            Some(instance),
        )
    } else {
        let name = path.split('/').nth(2).unwrap_or_default();
        let instance = worker
            .instances
            .lock()
            .values()
            .find(|entry| {
                entry.name == name && crate::server::authorized(request.headers(), &entry.token)
            })
            .cloned()
            .ok_or_else(unavailable)?;
        {
            let mut slot = instance.running.lock();
            if !instance.starting.load(Ordering::Acquire)
                && slot.as_ref().is_none_or(|r| r.expires <= Instant::now())
            {
                slot.take();
                return Err(unavailable());
            }
        }
        // The destination is operator-pinned, never selected by a deployment.
        (
            format!("{}{}", instance.state_endpoint, path),
            worker.client.clone(),
            Some(instance),
        )
    };
    let query = request
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let (parts, body) = request.into_parts();
    let bytes = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        to_bytes(body, 8 * 1024 * 1024),
    )
    .await
    .map_err(|_| (StatusCode::REQUEST_TIMEOUT, "body deadline exceeded"))?
    .map_err(|_| (StatusCode::PAYLOAD_TOO_LARGE, "body too large"))?;
    let mut upstream = client
        .request(parts.method, format!("{target}{query}"))
        .timeout(std::time::Duration::from_secs(300));
    for (name, value) in &parts.headers {
        if !crate::server::hop_header(name.as_str()) || name == "x-bic-internal-token" {
            upstream = upstream.header(name, value);
        }
    }
    let mut response = upstream
        .body(bytes)
        .send()
        .await
        .map_err(|_| unavailable())?;
    let mut result = Response::builder().status(response.status());
    for (name, value) in response.headers() {
        if !crate::server::hop_header(name.as_str()) {
            result = result.header(name, value);
        }
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
        if bytes.len().saturating_add(chunk.len()) > 16 * 1024 * 1024 {
            return Err((StatusCode::BAD_GATEWAY, "response too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    result.body(Body::from(bytes)).map_err(|_| unavailable())
}

pub async fn serve(
    root: PathBuf,
    listen: SocketAddr,
    token: String,
    node: Option<PathBuf>,
    state: StateOrigins,
    capacity: usize,
    microvm_config: Option<PathBuf>,
) -> Result<()> {
    let StateOrigins {
        primary: state_endpoint,
        peers: state_peers,
    } = state;
    crate::fleet::validate_origin(&state_endpoint)?;
    ensure!(state_peers.len() <= 128, "too many state origins");
    for endpoint in &state_peers {
        crate::fleet::validate_origin(endpoint)?;
    }
    ensure!((1..=4096).contains(&capacity), "invalid worker capacity");
    let node = crate::runtime::find_node(node.as_deref())?;
    let store = Store::open(&root)?;
    let microvm = microvm_config
        .as_deref()
        .map(|path| crate::microvm::Host::open(path, &store.root))
        .transpose()?;
    if microvm.is_none() {
        crate::microvm::fence_previous(&store.root)?;
    }
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
    let worker = Arc::new(Worker {
        store,
        token,
        node,
        endpoint: format!("http://{local}"),
        state_endpoint: state_endpoint.trim_end_matches('/').into(),
        state_peers: state_peers
            .iter()
            .map(|endpoint| endpoint.trim_end_matches('/').into())
            .collect(),
        capacity,
        microvm,
        instances: Mutex::new(BTreeMap::new()),
        admin: tokio::sync::Mutex::new(()),
        client: crate::runtime::upstream_client()?,
    });
    let routes = Router::new()
        .route("/health", get(|| async { Json(json!({"status":"ready"})) }))
        .route("/v1/executions", post(launch))
        .route("/v1/executions/:id/:operation", post(control))
        .route("/v1/executions/:id/logs", get(logs))
        .route("/run/:id", axum::routing::any(forward))
        .route("/run/:id/", axum::routing::any(forward))
        .route("/run/:id/*path", axum::routing::any(forward))
        .route("/internal/:name/query", post(forward))
        .route("/internal/:name/jobs", post(forward))
        .route("/internal/:name/conflicts", post(forward))
        .layer(DefaultBodyLimit::max(
            MAX_RELEASE_BYTES * 4 / 3 + 2 * 1024 * 1024,
        ))
        .layer(axum::middleware::from_fn_with_state(
            worker.clone(),
            authenticate,
        ))
        .with_state(worker.clone());
    let mut private = Vec::new();
    if let Some(host) = &worker.microvm {
        for allocation in host.allocations().values() {
            let listener = tokio::net::TcpListener::bind(SocketAddr::from((
                allocation.host_ip,
                allocation.api_port,
            )))
            .await?;
            // Only scoped database routes are exposed on guest-facing listeners.
            let router = Router::new()
                .route("/internal/:name/query", post(forward))
                .route("/internal/:name/jobs", post(forward))
                .route("/internal/:name/conflicts", post(forward))
                .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
                .layer(axum::middleware::from_fn_with_state(
                    worker.clone(),
                    authenticate,
                ))
                .with_state(worker.clone());
            private.push(tokio::spawn(
                async move { axum::serve(listener, router).await },
            ));
        }
    }
    let reaper = tokio::spawn({
        let worker = worker.clone();
        async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                worker.instances.lock().retain(|_, instance| {
                    let mut running = instance.running.lock();
                    if running
                        .as_ref()
                        .is_some_and(|r| r.expires <= Instant::now())
                    {
                        running.take();
                    }
                    running.is_some()
                        || (instance.starting.load(Ordering::Acquire)
                            && instance.created.elapsed() < std::time::Duration::from_secs(60))
                });
            }
        }
    });
    let result = axum::serve(listener, routes)
        .with_graceful_shutdown(crate::server::shutdown_signal())
        .await;
    reaper.abort();
    for task in private {
        task.abort();
    }
    worker.instances.lock().clear();
    result?;
    Ok(())
}

impl Drop for Worker {
    fn drop(&mut self) {
        for instance in self.instances.lock().values() {
            instance.running.lock().take();
        }
        self.instances.lock().clear();
        self.microvm.take();
    }
}
