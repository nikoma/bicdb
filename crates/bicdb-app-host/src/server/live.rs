//! Pull-driven HTTP subscriptions retain no unbounded notification queue.
use super::*;
use crate::database::Query;
use tokio::sync::OwnedSemaphorePermit;

struct Stream {
    app: Arc<Application>,
    token: String,
    query: Arc<Mutex<crate::database::LiveQuery>>,
    initial: Option<Vec<u8>>,
    last_emit: std::time::Instant,
    deadline: std::time::Instant,
    total: usize,
    _permit: Arc<OwnedSemaphorePermit>,
}

fn encode(result: bicdb_sql::SqlResult, initial: bool) -> Result<Vec<u8>> {
    ensure!(result.columns.len() >= 2, "invalid live query schema");
    let columns = &result.columns[2..];
    let mut changes = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        ensure!(row.len() == columns.len() + 2, "invalid live query row");
        let bicdb_sql::SqlValue::Int(sequence) = &row[0] else {
            anyhow::bail!("invalid live query sequence");
        };
        let data = columns
            .iter()
            .cloned()
            .zip(
                row[2..]
                    .iter()
                    .map(serde_json::to_value)
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            )
            .collect::<serde_json::Map<_, _>>();
        changes.push(json!({"commit_seq":sequence.to_string(), "change":row[1], "row":data}));
    }
    Ok(format!(
        "data: {}\n\n",
        serde_json::to_string(&json!({"initial":initial,"columns":columns,"changes":changes}))?
    )
    .into_bytes())
}

pub(super) async fn subscribe(
    State(platform): State<Arc<Platform>>,
    RoutePath(name): RoutePath<String>,
    headers: HeaderMap,
    Json(query): Json<Query>,
) -> ApiResult<Response> {
    let app = platform.app(&name)?;
    let token = app.token.lock().clone();
    if !authorized(&headers, &token) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "application authorization required".into(),
        ));
    }
    let permit = app
        .live_admission
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            ApiError(
                StatusCode::TOO_MANY_REQUESTS,
                "live query admission limit reached".into(),
            )
        })?;
    let permit = Arc::new(permit);
    let (query, initial, permit) = blocking({
        let app = app.clone();
        let token = token.clone();
        move || {
            app.require_authority()?;
            ensure!(
                *app.token.lock() == token,
                "application execution was replaced"
            );
            let database = app.database.lock()?;
            ensure!(
                *app.token.lock() == token,
                "application execution was replaced"
            );
            let mut live = database.subscribe(query)?;
            let initial = database
                .poll_subscription(&mut live)?
                .context("live query initial result missing")?;
            Ok((live, encode(initial, true)?, permit))
        }
    })
    .await?;
    let timeout = app.release.lock().manifest.resources.request_timeout_ms;
    let stream = Stream {
        app,
        token,
        query: Arc::new(Mutex::new(query)),
        initial: Some(initial),
        last_emit: std::time::Instant::now(),
        deadline: std::time::Instant::now() + Duration::from_millis(timeout),
        total: 0,
        _permit: permit,
    };
    let body = Body::from_stream(futures_util::stream::try_unfold(
        stream,
        |mut state| async move {
            let stopped =
                || std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "live query ended");
            loop {
                if std::time::Instant::now() >= state.deadline {
                    return Ok(None);
                }
                if !state.app.authoritative() || *state.app.token.lock() != state.token {
                    return Err(stopped());
                }
                let data = if let Some(initial) = state.initial.take() {
                    Some(initial)
                } else {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    let app = state.app.clone();
                    let query = state.query.clone();
                    let token = state.token.clone();
                    let permit = state._permit.clone();
                    let result = blocking(move || {
                        let _permit = permit;
                        app.require_authority()?;
                        ensure!(
                            *app.token.lock() == token,
                            "application execution was replaced"
                        );
                        let database = app.database.lock()?;
                        ensure!(
                            *app.token.lock() == token,
                            "application execution was replaced"
                        );
                        let result = database.poll_subscription(&mut query.lock())?;
                        result.map(|result| encode(result, false)).transpose()
                    })
                    .await
                    .map_err(|_| stopped())?;
                    result.or_else(|| {
                        (state.last_emit.elapsed() >= Duration::from_secs(5))
                            .then(|| b": keepalive\n\n".to_vec())
                    })
                };
                if let Some(data) = data {
                    if !state.app.authoritative() || *state.app.token.lock() != state.token {
                        return Err(stopped());
                    }
                    state.total = state.total.saturating_add(data.len());
                    if state.total > 16 * 1024 * 1024 {
                        return Err(stopped());
                    }
                    state.last_emit = std::time::Instant::now();
                    return Ok(Some((data, state)));
                }
            }
        },
    ));
    Response::builder()
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("x-accel-buffering", "no")
        .body(body)
        .map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid live response".into(),
            )
        })
}
