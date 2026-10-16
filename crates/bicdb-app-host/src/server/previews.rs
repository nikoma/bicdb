use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreviewRequest {
    application: String,
    #[serde(default)]
    start: bool,
}

struct Snapshot(Arc<Application>);
impl Drop for Snapshot {
    fn drop(&mut self) {
        self.0.snapshotting.store(false, Ordering::Release);
    }
}

pub(super) async fn create(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
    Json(request): Json<PreviewRequest>,
) -> ApiResult<Json<Value>> {
    platform.admin_auth(&headers)?;
    if !platform.replicas.is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "create previews in a standalone application catalog".into(),
        ));
    }
    if !crate::manifest::valid_name(&request.application) || request.application == name {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "invalid preview application name".into(),
        ));
    }
    let admin = platform.admin.clone().lock_owned().await;
    let source = platform.app(&name)?;
    let gate = source.gate.clone().write_owned().await;
    source.snapshotting.store(true, Ordering::Release);
    let snapshot = Snapshot(source.clone());
    let start = request.start;
    let (deployment, release, admin) = blocking({
        let platform = platform.clone();
        let source = source.clone();
        move || {
            let _gate = gate;
            let _snapshot = snapshot;
            source.require_authority()?;
            let release = source.release.lock().clone();
            let deployment = crate::preview::create(
                &platform.store,
                &*source.database.lock()?,
                &release,
                &request.application,
            )?;
            let release = platform
                .store
                .load_release(&deployment.application, &deployment.release)?;
            let app = platform.create_app(deployment.clone(), release.clone())?;
            platform
                .applications
                .lock()
                .insert(deployment.application.clone(), app);
            Ok((deployment, release, admin))
        }
    })
    .await?;
    let deployment = if start {
        blocking(move || {
            let _admin = admin;
            platform.deploy(release)
        })
        .await?
    } else {
        drop(admin);
        deployment
    };
    Ok(Json(json!(deployment)))
}
