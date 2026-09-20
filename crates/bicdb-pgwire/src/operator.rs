//! Optional host-operator HTTP API. SQL privileges never authorize this API.
use super::*;
use axum::{
    body::to_bytes,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};

/// Loaded once at startup. Debug deliberately excludes bearer material.
pub struct OperatorService {
    address: SocketAddr,
    actor: String,
    token_digest: [u8; 32],
}
impl std::fmt::Debug for OperatorService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperatorService")
            .field("address", &self.address)
            .field("actor", &self.actor)
            .finish_non_exhaustive()
    }
}
impl OperatorService {
    /// Token file: owner-only regular file on Unix, 32..256 non-whitespace
    /// ASCII bytes (an optional trailing newline is accepted). Use random tokens.
    pub fn from_token_file(
        address: SocketAddr,
        actor: String,
        token_file: impl AsRef<Path>,
    ) -> Result<Self> {
        if !valid_field(&actor) {
            return Err(operator_error("invalid operator actor"));
        }
        let mut options = fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(token_file)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > 258 {
            return Err(operator_error("invalid operator token file"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.mode() & 0o077 != 0 || metadata.uid() != unsafe { libc::geteuid() } {
                return Err(operator_error("operator token file must be owned by this process user and have mode 0600 or stricter"));
            }
        }
        let mut bytes = Vec::new();
        file.take(259).read_to_end(&mut bytes)?;
        while matches!(bytes.last(), Some(b'\n' | b'\r')) {
            bytes.pop();
        }
        if !(32..=256).contains(&bytes.len()) || !bytes.iter().all(|b| b.is_ascii_graphic()) {
            return Err(operator_error(
                "operator token must contain 32..256 non-whitespace ASCII bytes",
            ));
        }
        Ok(Self {
            address,
            actor,
            token_digest: Sha256::digest(&bytes).into(),
        })
    }
}
fn operator_error(message: &str) -> PgWireError {
    PgWireError::Server(message.to_owned())
}
fn valid_field(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control)
}
fn valid_identity(identity: &PgWireUserIdentity) -> bool {
    valid_field(&identity.user_id)
        && identity.tenant_id.len() <= 1024
        && !identity.tenant_id.chars().any(char::is_control)
        && identity.client_id.as_deref().is_none_or(valid_field)
        && identity.workspace_id.as_deref().is_none_or(valid_field)
        && identity.roles.len() <= 128
        && identity.scopes.len() <= 128
        && identity
            .roles
            .iter()
            .chain(&identity.scopes)
            .all(|value| valid_field(value))
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Operation {
    Create {
        username: String,
        password: String,
        identity: PgWireUserIdentity,
    },
    Identity {
        username: String,
        identity: PgWireUserIdentity,
    },
    Password {
        username: String,
        password: String,
    },
    Enabled {
        username: String,
        enabled: bool,
    },
    Revoke {
        username: String,
    },
    List {
        #[serde(default)]
        after: Option<String>,
        #[serde(default = "default_limit")]
        limit: usize,
    },
}
fn default_limit() -> usize {
    100
}
impl Operation {
    fn name(&self) -> &'static str {
        match self {
            Self::Create { .. } => "create",
            Self::Identity { .. } => "identity",
            Self::Password { .. } => "password",
            Self::Enabled { .. } => "enabled",
            Self::Revoke { .. } => "revoke",
            Self::List { .. } => "list",
        }
    }
    fn target(&self) -> Option<&str> {
        match self {
            Self::Create { username, .. }
            | Self::Identity { username, .. }
            | Self::Password { username, .. }
            | Self::Enabled { username, .. }
            | Self::Revoke { username } => Some(username),
            Self::List { .. } => None,
        }
    }
    fn valid(&self) -> bool {
        if self
            .target()
            .is_some_and(|name| name.len() > 255 || validate_username(name).is_err())
        {
            return false;
        }
        match self {
            Self::Create {
                password, identity, ..
            } => valid_password(password) && valid_identity(identity),
            Self::Identity { identity, .. } => valid_identity(identity),
            Self::Password { password, .. } => valid_password(password),
            Self::List { after, limit } => {
                (1..=1000).contains(limit)
                    && after
                        .as_ref()
                        .is_none_or(|name| name.len() <= 255 && validate_username(name).is_ok())
            }
            _ => true,
        }
    }
}
fn valid_password(password: &str) -> bool {
    !password.is_empty() && password.len() <= 4096 && !password.contains('\0')
}

#[derive(Serialize)]
struct LoginMetadata {
    username: String,
    enabled: bool,
    identity: Option<PgWireUserIdentity>,
}
impl From<&StoredUser> for LoginMetadata {
    fn from(user: &StoredUser) -> Self {
        Self {
            username: user.username.clone(),
            enabled: !user.disabled,
            identity: user.security_identity.clone(),
        }
    }
}
struct ApiState {
    path: PathBuf,
    actor: String,
    token_digest: [u8; 32],
    requests: Arc<tokio::sync::Semaphore>,
}
impl PgWireHostService for OperatorService {
    fn name(&self) -> &'static str {
        "login-operator-api"
    }
    fn start(&self, context: PgWireHostContext) -> Result<()> {
        let tls = context.server.tls_config.as_ref().map(|tls| {
            let mut config = (*tls.server).clone();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(config))
        });
        if !self.address.ip().is_loopback() && tls.is_none() {
            return Err(operator_error(
                "non-loopback operator API requires server TLS",
            ));
        }
        if !context.config().require_auth {
            return Err(operator_error(
                "operator API requires pgwire authentication",
            ));
        }
        let listener = TcpListener::bind(self.address)?;
        listener.set_nonblocking(true)?;
        let state = Arc::new(ApiState {
            path: context.server.auth_path.clone(),
            actor: self.actor.clone(),
            token_digest: self.token_digest,
            requests: Arc::new(tokio::sync::Semaphore::new(2)),
        });
        state.event(None, "startup", "ready", "startup")?;
        enable_operator_only_credentials(&state.path)?;
        let app = Router::new()
            .route("/v1/logins", post(handle))
            .with_state(state);
        let runtime = TokioRuntimeBuilder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(2)
            .enable_all()
            .build()?;
        let worker = BackgroundWorker::register(context.server.clone());
        thread::Builder::new()
            .name("bicdb-login-operator".into())
            .spawn(move || {
                let _worker = worker;
                runtime.block_on(async move {
                    let handle = axum_server::Handle::new();
                    let shutdown = handle.clone();
                    let server = context.server.clone();
                    tokio::spawn(async move {
                        while !server.is_shutdown_requested() {
                            time::sleep(Duration::from_millis(25)).await;
                        }
                        shutdown.graceful_shutdown(Some(Duration::from_secs(6)));
                    });
                    let result = if let Some(tls) = tls {
                        axum_server::from_tcp_rustls(listener, tls)
                            .handle(handle)
                            .serve(app.into_make_service())
                            .await
                    } else {
                        axum_server::from_tcp(listener)
                            .handle(handle)
                            .serve(app.into_make_service())
                            .await
                    };
                    if result.is_err() {
                        eprintln!("BicDB operator API listener failed; requesting shutdown");
                        context.request_shutdown();
                    }
                });
            })?;
        Ok(())
    }
}
impl ApiState {
    // Allowlisted fields only: no request body, credential, identity or error text.
    fn event(
        &self,
        target: Option<&str>,
        operation: &str,
        outcome: &str,
        request_id: &str,
    ) -> Result<()> {
        let event = json!({"actor":self.actor,"target":target,"operation":operation,"outcome":outcome,"request_id":request_id,"unix_ms":SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()});
        let mut options = fs::OpenOptions::new();
        options.append(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = options.open(self.path.join("server_operator.jsonl"))?;
        file.lock()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        let mut bytes = serde_json::to_vec(&event)
            .map_err(|_| operator_error("operator event serialization failed"))?;
        bytes.push(b'\n');
        file.write_all(&bytes)?;
        file.sync_all()?;
        #[cfg(unix)]
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }
    fn apply(&self, operation: Operation) -> Result<(StatusCode, JsonValue)> {
        let _lock = lock_user_catalog(&self.path)?;
        let mut catalog = load_user_catalog(&self.path)?;
        if let Operation::List { after, limit } = operation {
            let mut users = catalog
                .users
                .values()
                .filter(|user| after.as_ref().is_none_or(|after| user.username > *after))
                .collect::<Vec<_>>();
            users.sort_by(|a, b| a.username.cmp(&b.username));
            let has_more = users.len() > limit;
            let items = users
                .into_iter()
                .take(limit)
                .map(LoginMetadata::from)
                .collect::<Vec<_>>();
            let next = if has_more {
                items.last().map(|user| user.username.clone())
            } else {
                None
            };
            return Ok((StatusCode::OK, json!({"logins":items,"next_after":next})));
        }
        let username = operation.target().expect("non-list target").to_owned();
        let exists = catalog.users.contains_key(&username);
        if matches!(operation, Operation::Create { .. }) {
            if exists {
                return Ok((
                    StatusCode::CONFLICT,
                    json!({"error":"login already exists"}),
                ));
            }
        } else if !exists {
            return Ok((StatusCode::NOT_FOUND, json!({"error":"login not found"})));
        }
        match operation {
            Operation::Create {
                password, identity, ..
            } => {
                let user = password_record(&username, &password, Some(identity), false)?;
                delegation::write_policy_locked(&self.path, &username, None)?;
                catalog.users.insert(username.clone(), user);
            }
            Operation::Password { password, .. } => {
                let user = &catalog.users[&username];
                let user = password_record(
                    &username,
                    &password,
                    user.security_identity.clone(),
                    user.disabled,
                )?;
                catalog.users.insert(username.clone(), user);
            }
            Operation::Identity { identity, .. } => {
                catalog.users.get_mut(&username).unwrap().security_identity = Some(identity)
            }
            Operation::Enabled { enabled, .. } => {
                catalog.users.get_mut(&username).unwrap().disabled = !enabled
            }
            Operation::Revoke { .. } => {
                // Clear extra authority first. A crash can leave a less privileged
                // existing login, never a deleted login's policy for its successor.
                delegation::write_policy_locked(&self.path, &username, None)?;
                catalog.users.remove(&username);
            }
            Operation::List { .. } => unreachable!(),
        }
        persist_user_catalog(&self.path, &catalog)?;
        Ok((
            StatusCode::OK,
            json!({"login":catalog.users.get(&username).map(LoginMetadata::from)}),
        ))
    }
}
fn response(status: StatusCode, body: JsonValue) -> Response {
    let mut response = (status, Json(body)).into_response();
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
}
async fn handle(State(state): State<Arc<ApiState>>, request: Request) -> Response {
    let Ok(permit) = state.requests.clone().try_acquire_owned() else {
        return response(
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error":"operator API busy"}),
        );
    };
    let authenticated = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| value.len() <= 256)
        .is_some_and(|value| {
            let digest: [u8; 32] = Sha256::digest(value.as_bytes()).into();
            bool::from(digest.ct_eq(&state.token_digest))
        });
    let operation = if authenticated {
        match time::timeout(
            Duration::from_secs(5),
            to_bytes(request.into_body(), 32 * 1024),
        )
        .await
        {
            Ok(Ok(bytes)) => serde_json::from_slice::<Operation>(&bytes)
                .ok()
                .filter(Operation::valid),
            _ => None,
        }
    } else {
        None
    };
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let request_id = Uuid::new_v4().to_string();
        let name = operation.as_ref().map_or("request", Operation::name);
        let target = operation
            .as_ref()
            .and_then(Operation::target)
            .map(str::to_owned);
        if !authenticated || operation.is_none() {
            let outcome = if authenticated {
                "invalid"
            } else {
                "unauthorized"
            };
            // The configured actor is only asserted after bearer verification.
            let anonymous = ApiState {
                path: state.path.clone(),
                actor: "unauthenticated".into(),
                token_digest: [0; 32],
                requests: state.requests.clone(),
            };
            let logger = if authenticated {
                state.as_ref()
            } else {
                &anonymous
            };
            logger.event(None, "request", outcome, &request_id)?;
            return Ok((
                if authenticated {
                    StatusCode::BAD_REQUEST
                } else {
                    StatusCode::UNAUTHORIZED
                },
                json!({"error":outcome,"request_id":request_id}),
            ));
        }
        state.event(target.as_deref(), name, "started", &request_id)?;
        let result = state.apply(operation.unwrap());
        let outcome = match &result {
            Ok((status, _)) if status.is_success() => "succeeded",
            Ok(_) => "rejected",
            Err(_) => "indeterminate",
        };
        state.event(target.as_deref(), name, outcome, &request_id)?;
        result.map(|(status, mut body)| {
            body["request_id"] = json!(request_id);
            (status, body)
        })
    })
    .await
    {
        Ok(Ok((status, body))) => response(status, body),
        _ => response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"operator operation unavailable; outcome may be indeterminate, inspect login metadata before retrying"}),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_listener_rejects_remote_plaintext_and_unauthenticated_pgwire() {
        for (address, require_auth) in [("0.0.0.0:0", true), ("127.0.0.1:0", false)] {
            let directory = tempfile::tempdir().unwrap();
            let server = PgWireServer::open(
                directory.path(),
                PgWireConfig {
                    require_auth,
                    ..PgWireConfig::default()
                },
            )
            .unwrap();
            let service = OperatorService {
                address: address.parse().unwrap(),
                actor: "operator".into(),
                token_digest: [0; 32],
            };
            assert!(service.start(PgWireHostContext { server }).is_err());
            assert!(!directory.path().join("server_operator.jsonl").exists());
        }
    }
}
