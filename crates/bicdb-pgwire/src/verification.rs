//! Scheduled local integrity checks and isolated PITR/promotion drills.
use super::*;
use bicdb_core::{verify_restore_drill, VerificationManifest};

#[derive(Clone, Debug)]
pub struct VerificationConfig {
    pub interval: Duration,
    pub scratch_root: PathBuf,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct VerificationRun {
    pub check: &'static str,
    pub completed_at: i64,
    pub duration_ms: u64,
    pub commit_seq: u64,
    pub success: bool,
    pub restore_and_failover: bool,
    pub tables: usize,
    pub indexes: usize,
    pub error: Option<String>,
}

pub(crate) fn record(server: &PgWireServer, mut run: VerificationRun, result: Result<()>) {
    run.success = result.is_ok();
    run.error = result.err().map(|error| error.to_string());
    run.completed_at = unix_timestamp();
    if let Some(error) = &run.error {
        log_operational_event(
            "verification.failed",
            "error",
            json!({"error": error, "commit_seq": run.commit_seq}),
        );
    }
    let mut runs = server.verification_runs.lock().unwrap();
    if runs.len() >= 64 {
        runs.remove(0);
    }
    runs.push(run);
}

pub(crate) fn capture(server: &Arc<PgWireServer>) -> Result<(VerificationManifest, usize)> {
    let db = server.read_db()?;
    let indexes = db.verify_indexes_consistently()?;
    if indexes.iter().any(|report| !report.valid) {
        return Err(PgWireError::Server(
            "an index differs from its table".into(),
        ));
    }
    let vector_indexes = db.verify_vector_indexes_consistently()?;
    Ok((db.verification_manifest()?, indexes.len() + vector_indexes))
}

pub(crate) fn run(
    server: &Arc<PgWireServer>,
    archive: Option<(
        &Path,
        &str,
        &mut bicdb_core::log_archive::LogArchive,
        &mut bicdb_core::ReplicationCursor,
    )>,
) {
    let Some(config) = server.config.verification.as_ref() else {
        return;
    };
    let started = Instant::now();
    let mut run = VerificationRun {
        check: if archive.is_some() {
            "restore"
        } else {
            "integrity"
        },
        completed_at: 0,
        duration_ms: 0,
        commit_seq: 0,
        success: false,
        restore_and_failover: false,
        tables: 0,
        indexes: 0,
        error: None,
    };
    let result = (|| {
        let (manifest, indexes) = capture(server)?;
        run.commit_seq = manifest.commit_seq;
        run.tables = manifest.tables.len();
        run.indexes = indexes;
        if let Some((dir, key, archive, cursor)) = archive {
            // The source snapshot is fixed while writers keep running. Archive
            // at least that prefix before releasing the database for the drill.
            while archive.last_commit_seq() < manifest.commit_seq {
                if server.read_db()?.archive_commits(archive, cursor, 512)? == 0 {
                    return Err(PgWireError::Server(
                        "archive has not reached the verification snapshot".into(),
                    ));
                }
            }
            verify_restore_drill(dir, key, &config.scratch_root, Some(&manifest))?;
            run.restore_and_failover = true;
        }
        Ok(())
    })();
    run.duration_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    record(server, run, result);
}

/// An archive performs its drill on its own worker, excluding pruning and
/// concurrent appends. Without an archive, still check every table and index.
pub(crate) fn start(server: Arc<PgWireServer>) -> Result<()> {
    let Some(config) = server.config.verification.clone() else {
        return Ok(());
    };
    if config.interval.is_zero() {
        return Err(PgWireError::Server(
            "verification interval must be positive".into(),
        ));
    }
    if let Some(archive) = &server.config.log_archive {
        if archive.base_backup_key.is_none() {
            return Err(PgWireError::Server(
                "archive verification requires the base backup key".into(),
            ));
        }
        return Ok(());
    }
    let weak = Arc::downgrade(&server);
    std::thread::Builder::new()
        .name("bicdb-verifier".into())
        .spawn(move || {
            let mut due = Instant::now();
            loop {
                let Some(server) = weak.upgrade() else {
                    return;
                };
                if server.is_shutdown_requested() {
                    return;
                }
                if Instant::now() >= due {
                    run(&server, None);
                    due = Instant::now() + config.interval;
                }
                drop(server);
                std::thread::sleep(Duration::from_millis(250));
            }
        })?;
    Ok(())
}
