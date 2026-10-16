//! Bounded response streaming with execution revocation and owned admission.
use axum::{body::Body, http::HeaderMap};

pub(crate) fn is_events(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case("text/event-stream")
        })
}

pub(crate) fn body<G: Send + 'static>(
    response: reqwest::Response,
    guard: G,
    authorized: impl Fn() -> bool + Send + 'static,
) -> Body {
    Body::from_stream(futures_util::stream::try_unfold(
        (response, guard, authorized, 0usize),
        |(mut response, guard, authorized, total)| async move {
            let stopped = || {
                std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "application stream ended",
                )
            };
            if !authorized() {
                return Err(stopped());
            }
            let chunk = {
                let next = response.chunk();
                tokio::pin!(next);
                loop {
                    tokio::select! {
                        chunk = &mut next => break chunk.map_err(|_| stopped())?,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                            if !authorized() { return Err(stopped()); }
                        }
                    }
                }
            };
            if !authorized() {
                return Err(stopped());
            }
            let Some(chunk) = chunk else {
                return Ok(None);
            };
            let total = total.saturating_add(chunk.len());
            if total > 16 * 1024 * 1024 {
                return Err(stopped());
            }
            Ok(Some((chunk, (response, guard, authorized, total))))
        },
    ))
}
