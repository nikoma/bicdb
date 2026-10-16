use super::*;
use bicdb_core::ClusterEventCursor;

impl Platform {
    pub(super) fn validate_event_binding(&self, release: &Release) -> Result<()> {
        if release.manifest.bindings.events {
            let host = self
                .event_cluster
                .lock()
                .clone()
                .context("event cluster is not configured")?;
            host.namespace(&release.manifest.name)?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Operation {
    Append {
        key: String,
        value: Value,
    },
    Get {
        key: String,
    },
    List {
        cursor: Option<ClusterEventCursor>,
        limit: usize,
    },
}

pub(super) async fn request(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
    Json(operation): Json<Operation>,
) -> ApiResult<Json<Value>> {
    let app = platform.app(&name)?;
    let token = app.token.lock().clone();
    if !authorized(&headers, &token) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "application authorization required".into(),
        ));
    }
    let host = platform
        .event_cluster
        .lock()
        .clone()
        .context("event cluster is not configured")?;
    let namespace = host.namespace(&name)?.to_owned();
    let result = blocking(move || {
        // Recheck capability and writer authority after blocking admission.
        app.require_authority()?;
        ensure!(
            *app.token.lock() == token,
            "application execution was replaced"
        );
        ensure!(
            app.release.lock().manifest.bindings.events,
            "event binding was not declared by this release"
        );
        Ok(match operation {
            Operation::Append { key, value } => {
                json!({"inserted": host.client.append(&namespace, &key, value)?})
            }
            Operation::Get { key } => json!({"value": host.client.get(&namespace, &key)?}),
            Operation::List { cursor, limit } => {
                serde_json::to_value(host.client.scan(&namespace, cursor, limit)?)?
            }
        })
    })
    .await?;
    Ok(Json(result))
}
