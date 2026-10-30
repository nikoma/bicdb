//! Conservative staleness bounds based on local monotonic time, never clock
//! synchronization or the timestamp of the last data-changing commit.
use super::*;

pub(crate) fn bound(settings: &SqlSessionGucState) -> Result<Option<Duration>> {
    let Some(value) = settings.session_gucs().get("bicdb.max_lag") else {
        return Ok(None);
    };
    bicdb_core::query_budget::parse_duration(value)
        .map(Some)
        .ok_or_else(|| {
            PgWireError::QueryRejected(
                "bicdb.max_lag must be a nonnegative duration, e.g. '2s'".into(),
            )
        })
}

pub(crate) fn check(
    server: &PgWireServer,
    settings: &SqlSessionGucState,
    transaction_watermark: Option<Option<Instant>>,
) -> Result<()> {
    let Some(bound) = bound(settings)? else {
        return Ok(());
    };
    let db = server.read_db()?;
    if !db.replication_verification_healthy() {
        return Err(PgWireError::QueryRejected(
            "replica verification failed; fresh snapshot required".into(),
        ));
    }
    if !db.is_standby_role() {
        return Ok(());
    }
    drop(db);
    let current = *server.replica_read_watermark.lock().unwrap();
    let watermark = match (current, transaction_watermark) {
        (Some(current), Some(Some(transaction))) => Some(current.min(transaction)),
        (current, None) => current,
        (None, _) | (_, Some(None)) => None,
    };
    if watermark.is_none_or(|sample| sample.elapsed() > bound) {
        return Err(PgWireError::QueryRejected(format!("replica cannot prove bicdb.max_lag = {bound:?}; reconnect to the primary or retry after catch-up")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duration_bound_rejects_nonfinite_negative_and_overflow_values() {
        for value in ["-2s", "NaN", "inf", "1e200s", "garbage"] {
            let state = SqlSessionGucState::from_session_gucs(HashMap::from([(
                "bicdb.max_lag".into(),
                value.into(),
            )]));
            assert!(bound(&state).is_err(), "{value}");
        }
        let state = SqlSessionGucState::from_session_gucs(HashMap::from([(
            "bicdb.max_lag".into(),
            "2s".into(),
        )]));
        assert_eq!(bound(&state).unwrap(), Some(Duration::from_secs(2)));
    }
}

pub(crate) fn snapshot_watermark(
    server: &PgWireServer,
    state: &ConnectionState,
) -> Option<Instant> {
    let current = *server.replica_read_watermark.lock().unwrap();
    if state.in_transaction && state.tx_fixed_snapshot {
        match (current, state.replica_tx_watermark) {
            (Some(current), Some(transaction)) => Some(current.min(transaction)),
            _ => None,
        }
    } else {
        current
    }
}

#[cfg(test)]
mod freshness_tests {
    use super::*;
    #[test]
    fn bounded_reads_refuse_unknown_old_and_old_fixed_snapshots() {
        let root = tempfile::tempdir().unwrap();
        let mut config = PgWireConfig::default();
        config.high_availability = Some(HaServerConfig {
            replication: ReplicationConfig {
                cluster_id: "freshness".into(),
                node_id: "s".into(),
                tls: Some(ReplicationTlsConfig {
                    cert_path: PathBuf::new(),
                    key_path: PathBuf::new(),
                    ca_path: PathBuf::new(),
                    require_client_cert: true,
                    dev_localhost_plaintext: true,
                }),
                ..ReplicationConfig::default()
            },
            role: HaServerRole::Standby {
                primary: "127.0.0.1:9".into(),
                primary_server_name: "localhost".into(),
            },
            advertise_sql: None,
            failover: None,
        });
        let server = PgWireServer::open(root.path(), config).unwrap();
        let settings = SqlSessionGucState::from_session_gucs(HashMap::from([(
            "bicdb.max_lag".into(),
            "2s".into(),
        )]));
        assert!(check(&server, &settings, None).is_err());
        *server.replica_read_watermark.lock().unwrap() = Some(Instant::now());
        assert!(check(&server, &settings, None).is_ok());
        assert!(check(&server, &settings, Some(None)).is_err());
        assert!(check(
            &server,
            &settings,
            Some(Some(Instant::now() - Duration::from_secs(3)))
        )
        .is_err());
        *server.replica_read_watermark.lock().unwrap() =
            Some(Instant::now() - Duration::from_secs(3));
        assert!(check(&server, &settings, None).is_err());
        assert!(check(&server, &SqlSessionGucState::default(), None).is_ok());
    }
}
