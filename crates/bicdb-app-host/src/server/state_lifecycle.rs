use super::*;

pub(super) fn status(replica: &Replica) -> Value {
    json!({"node":replica.group.node_id, "primary":replica.primary.lock().clone(), "writer":replica.authoritative(), "error":replica.error.lock().as_ref().map(|_| "state recovery is unavailable; inspect operator logs")})
}

pub(super) async fn run(platform: Arc<Platform>) {
    let mut tasks = tokio::task::JoinSet::new();
    for name in platform.replicas.keys() {
        let platform = platform.clone();
        let name = name.clone();
        tasks.spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let Ok(_admin) = platform.admin.try_lock() else {
                    continue;
                };
                let app = platform.applications.lock().get(&name).cloned();
                let _gate = match &app {
                    Some(app) => match app.gate.try_write() {
                        Ok(gate) => Some(gate),
                        Err(_) => continue,
                    },
                    None => None,
                };
                let result = blocking({
                    let platform = platform.clone();
                    let name = name.clone();
                    move || reconcile(&platform, &name)
                })
                .await;
                let replica = &platform.replicas[&name];
                match result {
                    Err(error) => {
                        if let Some(app) = &app {
                            *app.error.lock() = Some(error.1.clone());
                        }
                        let previous = replica.error.lock().replace(error.1.clone());
                        if previous.as_ref() != Some(&error.1) {
                            eprintln!("application {name} state recovery: {}", error.1);
                        }
                    }
                    Ok(()) => {
                        replica.error.lock().take();
                    }
                }
            }
        });
    }
    while tasks.join_next().await.is_some() {}
}

fn reconcile(platform: &Platform, name: &str) -> Result<()> {
    let replica = &platform.replicas[name];
    let existing = platform.applications.lock().get(name).cloned();
    if replica
        .database
        .0
        .lock()
        .as_ref()
        .is_none_or(Database::restart_required)
    {
        if let Some(app) = &existing {
            retire(app);
        }
    }
    replica.refresh()?;
    let mut desired = replica.database.lock()?.deployment()?;
    if desired.is_none()
        && replica.authoritative()
        && replica.group.node_id == replica.group.initial_primary
    {
        if let Some(deployment) = platform
            .store
            .deployments()?
            .into_iter()
            .find(|deployment| deployment.application == name)
        {
            let release = platform.store.load_release(name, &deployment.release)?;
            let previous = deployment
                .previous
                .as_ref()
                .map(|digest| platform.store.load_release(name, digest))
                .transpose()?;
            let mut packages = vec![&release];
            if let Some(previous) = &previous {
                packages.push(previous);
            }
            replica
                .database
                .lock()?
                .publish_deployment(&deployment, &packages)?;
            desired = Some(deployment);
        }
    }
    let Some(desired) = desired else {
        return Ok(());
    };
    ensure!(
        desired.application == name && desired.generation > 0,
        "invalid replicated deployment identity"
    );
    let changed = existing
        .as_ref()
        .is_none_or(|app| *app.deployment.lock() != desired);
    let app = if changed {
        if let Some(app) = &existing {
            retire(app);
        }
        let database = replica.database.lock()?;
        let release = database.deployment_release(&desired.release)?;
        ensure!(
            release.manifest.name == name,
            "replicated release belongs to another application"
        );
        platform.store.save_release(&release)?;
        if let Some(previous) = &desired.previous {
            platform
                .store
                .save_release(&database.deployment_release(previous)?)?;
        }
        drop(database);
        platform.store.save_deployment(&desired)?;
        let app = match existing {
            Some(app) => {
                *app.deployment.lock() = desired.clone();
                app.set_release(release);
                app
            }
            None => platform.create_app(desired.clone(), release)?,
        };
        platform
            .applications
            .lock()
            .insert(name.into(), app.clone());
        app
    } else {
        existing.unwrap()
    };
    if !replica.authoritative() || !desired.enabled {
        retire(&app);
        return Ok(());
    }
    platform.refresh_peer_grants(&app)?;
    let needs_execution = {
        let mut slot = app.process.lock();
        slot.as_mut()
            .is_none_or(|process| process.exited().unwrap_or(true))
    };
    if needs_execution {
        retire(&app);
        let release = app.release.lock().clone();
        platform.validate_isolation(&release)?;
        app.database.lock()?.prepare(&release)?;
        let execution = platform.launch(&app, &release, &desired.release)?;
        app.require_authority()?;
        *app.process.lock() = Some(execution);
        *app.error.lock() = None;
    }
    Ok(())
}

fn retire(app: &Application) {
    let mut slot = app.process.lock();
    if slot.is_some() {
        *app.token.lock() = fresh_token();
        slot.take();
    }
}

pub(super) async fn fence(platform: Arc<Platform>) {
    loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let apps: Vec<_> = platform
            .applications
            .lock()
            .values()
            .filter(|app| app.replica.is_some())
            .cloned()
            .collect();
        for app in apps {
            if !app.authoritative() {
                // Revocation does not take the database mutex or wait for a
                // request gate. Destruction happens off the async executor.
                let execution = {
                    let mut slot = app.process.lock();
                    if let Some(execution) = slot.as_ref() {
                        execution.revoke();
                        *app.token.lock() = fresh_token();
                    }
                    slot.take()
                };
                if let Some(execution) = execution {
                    tokio::task::spawn_blocking(move || drop(execution));
                }
            }
        }
    }
}

pub(super) enum Routed {
    Local(Request),
    Response(Response),
}

pub(super) async fn route(platform: &Platform, mut request: Request) -> ApiResult<Routed> {
    let path = request.uri().path().to_owned();
    let name = if path == "/v1/apps" && request.method() == axum::http::Method::POST {
        let (parts, body) = request.into_parts();
        let bytes = tokio::time::timeout(Duration::from_secs(30), to_bytes(body, MAX_BODY))
            .await
            .map_err(|_| {
                ApiError(
                    StatusCode::REQUEST_TIMEOUT,
                    "deployment body deadline exceeded".into(),
                )
            })?
            .map_err(|_| ApiError(StatusCode::PAYLOAD_TOO_LARGE, "release too large".into()))?;
        let release: Release = serde_json::from_slice(&bytes)
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid release".into()))?;
        release.validate()?;
        let name = release.manifest.name;
        request = Request::from_parts(parts, Body::from(bytes));
        Some(name)
    } else if path.starts_with("/v1/apps/") {
        path.split('/').nth(3).map(str::to_owned)
    } else if path.starts_with("/apps/") || path.starts_with("/sync/") {
        path.split('/').nth(2).map(str::to_owned)
    } else {
        None
    };
    let Some(replica) = name.and_then(|name| platform.replicas.get(&name)) else {
        return Ok(Routed::Local(request));
    };
    if replica.authoritative() {
        return Ok(Routed::Local(request));
    }
    if request.headers().contains_key("x-bic-state-forwarded") {
        return Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "state primary is changing".into(),
        ));
    }
    let endpoint = replica.primary_endpoint().map_err(|_| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "state writer authority is unavailable".into(),
        )
    })?;
    let (parts, body) = request.into_parts();
    let bytes = tokio::time::timeout(Duration::from_secs(30), to_bytes(body, MAX_BODY))
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::REQUEST_TIMEOUT,
                "request body deadline exceeded".into(),
            )
        })?
        .map_err(|_| ApiError(StatusCode::PAYLOAD_TOO_LARGE, "request too large".into()))?;
    let mut upstream = platform.client.request(
        parts.method,
        format!(
            "{endpoint}{}",
            parts.uri.path_and_query().context("request URI")?
        ),
    );
    upstream = upstream.timeout(Duration::from_secs(120));
    for (name, value) in &parts.headers {
        if !hop_header(name.as_str()) {
            upstream = upstream.header(name, value);
        }
    }
    let response = upstream
        .header("x-bic-state-forwarded", "1")
        .body(bytes)
        .send()
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::BAD_GATEWAY,
                "state primary is unreachable".into(),
            )
        })?;
    let mut result = Response::builder().status(response.status());
    for (name, value) in response.headers() {
        if !hop_header(name.as_str()) {
            result = result.header(name, value);
        }
    }
    let body = Body::from_stream(futures_util::stream::try_unfold(
        response,
        |mut response| async move {
            let chunk = response.chunk().await?;
            Ok::<_, reqwest::Error>(chunk.map(|chunk| (chunk, response)))
        },
    ));
    Ok(Routed::Response(result.body(body).map_err(|_| {
        ApiError(StatusCode::BAD_GATEWAY, "invalid primary response".into())
    })?))
}
