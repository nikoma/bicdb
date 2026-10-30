//! Prometheus metrics for `bicdb serve --metrics-listen`: connections,
//! queries, replication (role, epoch, standby positions and lag, synchronous
//! commit waits, write lease) and history storage (flashback, recycle bin).
//! `GET /metrics` returns the text exposition format; nothing else is
//! served. There is no authentication: bind it to a trusted interface.
use super::*;
use std::fmt::Write as _;
use std::io::{Read as _, Write as _};

/// Starts the metrics listener if one is configured.
pub(crate) fn start(server: Arc<PgWireServer>) -> Result<()> {
    let Some(address) = server.config.metrics_listen else {
        return Ok(());
    };
    let listener = std::net::TcpListener::bind(address)?;
    eprintln!("bicdb metrics: http://{address}/metrics");
    std::thread::Builder::new()
        .name("bicdb-metrics".to_string())
        .spawn(move || {
            for connection in listener.incoming() {
                if server.is_shutdown_requested() {
                    return;
                }
                let Ok(mut stream) = connection else {
                    continue;
                };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
                let mut request = [0u8; 2048];
                let read = stream.read(&mut request).unwrap_or(0);
                let request = String::from_utf8_lossy(&request[..read]);
                let response = if request.starts_with("GET /metrics ") {
                    match render(&server) {
                        Ok(body) => format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        ),
                        Err(error) => {
                            let body = format!("{error}\n");
                            format!(
                                "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            )
                        }
                    }
                } else {
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_string()
                };
                let _ = stream.write_all(response.as_bytes());
            }
        })?;
    Ok(())
}

fn label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

struct Exposition(String);

impl Exposition {
    fn metric(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP {name} {help}");
        let _ = writeln!(self.0, "# TYPE {name} {kind}");
    }

    fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: impl std::fmt::Display) {
        if labels.is_empty() {
            let _ = writeln!(self.0, "{name} {value}");
        } else {
            let labels = labels
                .iter()
                .map(|(key, value)| format!("{key}=\"{}\"", label(value)))
                .collect::<Vec<_>>()
                .join(",");
            let _ = writeln!(self.0, "{name}{{{labels}}} {value}");
        }
    }

    fn gauge(&mut self, name: &str, help: &str, value: impl std::fmt::Display) {
        self.metric(name, "gauge", help);
        self.sample(name, &[], value);
    }

    fn counter(&mut self, name: &str, help: &str, value: impl std::fmt::Display) {
        self.metric(name, "counter", help);
        self.sample(name, &[], value);
    }
}

fn render(server: &Arc<PgWireServer>) -> Result<String> {
    let mut out = Exposition(String::new());
    let stats = server.stats_snapshot();
    out.counter(
        "bicdb_live_breaker_trips_total",
        "Subscriptions stopped for evaluation, fan-out, or backlog budgets.",
        server.live_breaker_trips.load(Ordering::Relaxed),
    );
    out.metric(
        "bicdb_workload_active_queries",
        "gauge",
        "Queries admitted for an authenticated role or tenant.",
    );
    out.metric(
        "bicdb_workload_reserved_memory_bytes",
        "gauge",
        "Reserved query working memory by identity.",
    );
    out.metric(
        "bicdb_workload_rejected_total",
        "counter",
        "Cost or concurrency admission refusals by identity.",
    );
    for (identity, usage) in server.workload.usage.lock().unwrap().iter() {
        let labels = [("identity", identity.as_str())];
        out.sample("bicdb_workload_active_queries", &labels, usage.active);
        out.sample(
            "bicdb_workload_reserved_memory_bytes",
            &labels,
            usage.reserved_bytes.saturating_add(usage.cached_bytes),
        );
        out.sample("bicdb_workload_rejected_total", &labels, usage.rejected);
    }

    out.gauge(
        "bicdb_verify_enabled",
        "Scheduled verification is configured.",
        u8::from(server.config.verification.is_some()),
    );
    {
        let healthy = server.read_db()?.replication_verification_healthy();
        let runs = server.verification_runs.lock().unwrap();
        let mut latest = BTreeMap::new();
        for run in runs.iter() {
            latest.insert(run.check, run);
        }
        out.gauge(
            "bicdb_verify_last_success",
            "Last drill succeeded; zero before any drill.",
            u8::from(!latest.is_empty() && healthy && latest.values().all(|run| run.success)),
        );
        out.gauge(
            "bicdb_verify_last_completed_timestamp_seconds",
            "Last completed verification; zero before any drill.",
            runs.last().map_or(0, |run| run.completed_at),
        );
        out.gauge(
            "bicdb_verify_last_restore_and_failover",
            "Last drill restored, promoted, wrote and reopened scratch data.",
            runs.last()
                .map_or(0, |run| u8::from(run.restore_and_failover)),
        );
    }

    out.gauge("bicdb_up", "The server is serving.", 1);
    out.gauge(
        "bicdb_connections_active",
        "Open client connections.",
        stats.active_connections,
    );
    out.counter(
        "bicdb_connections_total",
        "Client connections accepted.",
        stats.total_connections,
    );
    out.gauge(
        "bicdb_queries_active",
        "Queries running.",
        stats.active_queries,
    );
    out.counter(
        "bicdb_queries_total",
        "Queries executed.",
        stats.queries_executed,
    );
    out.counter(
        "bicdb_queries_failed_total",
        "Queries that failed.",
        stats.failed_queries,
    );

    out.gauge(
        "bicdb_live_subscriptions",
        "Open SUBSCRIBE statements.",
        server.live_subscriptions.load(Ordering::SeqCst) + server.live_registry.len() as u64,
    );
    out.counter(
        "bicdb_live_full_runs_total",
        "Subscription evaluations that re-read the whole query.",
        server.live_full_runs.load(Ordering::Relaxed),
    );
    out.counter(
        "bicdb_live_incremental_runs_total",
        "Subscription evaluations that re-read only the changed rows or groups.",
        server.live_incremental_runs.load(Ordering::Relaxed),
    );
    let connectors = connectors::statuses(server);
    out.metric(
        "bicdb_connector_messages_total",
        "counter",
        "Messages a connector moved.",
    );
    for status in &connectors {
        out.sample(
            "bicdb_connector_messages_total",
            &[
                ("connector", &status.name),
                ("kind", &status.kind),
                ("system", &status.system),
            ],
            status.messages,
        );
    }
    out.metric(
        "bicdb_connector_failures_total",
        "counter",
        "Failed deliveries, reads or writes of a connector (each retried).",
    );
    for status in &connectors {
        out.sample(
            "bicdb_connector_failures_total",
            &[
                ("connector", &status.name),
                ("kind", &status.kind),
                ("system", &status.system),
            ],
            status.failures,
        );
    }
    out.metric(
        "bicdb_connector_retrying",
        "gauge",
        "1 while a connector is retrying after a failure.",
    );
    for status in &connectors {
        out.sample(
            "bicdb_connector_retrying",
            &[("connector", &status.name)],
            u8::from(status.state == "retrying"),
        );
    }

    let db = server.read_db()?;
    let current = db.current_commit_seq();
    out.gauge(
        "bicdb_commit_seq",
        "Last commit sequence number of this database.",
        current,
    );
    let replication = db.replication_config().enabled;
    let node = db.ha_node_status();
    let role = if replication {
        format!("{:?}", node.role).to_lowercase()
    } else {
        "standalone".to_string()
    };
    out.metric(
        "bicdb_replication_role",
        "gauge",
        "1 for this node's replication role.",
    );
    for candidate in ["standalone", "primary", "standby", "fenced"] {
        out.sample(
            "bicdb_replication_role",
            &[("role", candidate)],
            u8::from(role == candidate),
        );
    }
    out.gauge(
        "bicdb_replication_epoch",
        "Highest primary epoch this node knows.",
        node.epoch,
    );
    out.gauge(
        "bicdb_replication_applied_commit_seq",
        "Last commit applied on this node (standbys) or written (primaries).",
        db.last_applied_commit_seq(),
    );

    let quorum = db.sync_quorum().status();
    out.metric(
        "bicdb_replication_standby_acknowledged_commit_seq",
        "gauge",
        "Last commit a standby confirmed durably.",
    );
    for (standby, acked) in &quorum.standbys {
        out.sample(
            "bicdb_replication_standby_acknowledged_commit_seq",
            &[("standby", standby)],
            acked,
        );
    }
    out.metric(
        "bicdb_replication_standby_lag_commits",
        "gauge",
        "Commits a standby has not confirmed yet.",
    );
    for (standby, acked) in &quorum.standbys {
        out.sample(
            "bicdb_replication_standby_lag_commits",
            &[("standby", standby)],
            current.saturating_sub(*acked),
        );
    }
    out.metric(
        "bicdb_replication_standby_last_heard_seconds",
        "gauge",
        "Seconds since a standby was last heard from.",
    );
    for (standby, ms) in &quorum.last_heard_ms {
        out.sample(
            "bicdb_replication_standby_last_heard_seconds",
            &[("standby", standby)],
            *ms as f64 / 1000.0,
        );
    }
    out.metric(
        "bicdb_log_hold_commit_seq",
        "gauge",
        "Commit position up to which a local consumer (the log archive) holds the transaction log.",
    );
    for (holder, seq) in &quorum.holds {
        out.sample("bicdb_log_hold_commit_seq", &[("holder", holder)], seq);
    }
    out.gauge(
        "bicdb_sync_standbys_required",
        "Standbys that must confirm each commit.",
        quorum.required,
    );
    out.counter(
        "bicdb_sync_commit_waits_total",
        "Commits that waited for synchronous confirmations.",
        quorum.waits,
    );
    out.counter(
        "bicdb_sync_commit_wait_seconds_total",
        "Time commits spent waiting for synchronous confirmations.",
        quorum.wait_micros as f64 / 1_000_000.0,
    );
    out.counter(
        "bicdb_sync_commit_failed_total",
        "Waits that ended without the confirmations.",
        quorum.failed_waits,
    );
    out.gauge(
        "bicdb_sync_degraded",
        "1 while synchronous replication is degraded to asynchronous.",
        u8::from(quorum.degraded),
    );
    out.counter(
        "bicdb_sync_degraded_total",
        "Times synchronous replication degraded to asynchronous.",
        quorum.degraded_events,
    );
    if quorum.lease_configured {
        out.gauge(
            "bicdb_write_lease_held",
            "1 while this primary holds its write lease.",
            u8::from(quorum.lease_refusal.is_none()),
        );
    }

    out.metric(
        "bicdb_flashback_history_rows",
        "gauge",
        "History rows kept for an archived table.",
    );
    for (table, _) in db.flashback_collections() {
        let history = bicdb_core::flashback_history_collection(&table);
        let rows = db.collection_record_count(&history).unwrap_or(0);
        out.sample("bicdb_flashback_history_rows", &[("table", &table)], rows);
    }
    let bin = db
        .collections()
        .into_iter()
        .filter(|meta| meta.name.starts_with("__bicdb_bin_"))
        .map(|meta| db.collection_record_count(&meta.name).unwrap_or(0))
        .collect::<Vec<_>>();
    out.gauge(
        "bicdb_recyclebin_tables",
        "Dropped tables held in the recycle bin.",
        bin.len(),
    );
    out.gauge(
        "bicdb_recyclebin_rows",
        "Rows of the tables held in the recycle bin.",
        bin.iter().sum::<usize>(),
    );
    Ok(out.0)
}
