//! The commit log archiver of `bicdb serve --log-archive`: appends every
//! commit to the archive as it becomes durable, for whole-database
//! point-in-time recovery (`bicdb backup restore --log-archive`).
use super::*;
use bicdb_core::log_archive::LogArchive;
use bicdb_core::ReplicationCursor;

/// Where and how a server archives its commits.
#[derive(Clone)]
pub struct LogArchiveConfig {
    /// Archive directory (in cluster mode, one subdirectory per database).
    pub dir: PathBuf,
    /// Encryption key of the base backups the archiver takes. Without one,
    /// only commits are archived and base backups come from elsewhere.
    pub base_backup_key: Option<String>,
    /// How often to take a base backup.
    pub base_backup_interval: Duration,
    /// Base backups to keep; older ones, and the log only they need, are
    /// removed.
    pub keep_base_backups: usize,
}

impl std::fmt::Debug for LogArchiveConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LogArchiveConfig")
            .field("dir", &self.dir)
            .field(
                "base_backup_key",
                &self.base_backup_key.as_ref().map(|_| "<redacted>"),
            )
            .field("base_backup_interval", &self.base_backup_interval)
            .field("keep_base_backups", &self.keep_base_backups)
            .finish()
    }
}

/// Commits archived per round.
const BATCH_COMMITS: usize = 512;
/// Idle wait between rounds when nothing new committed.
const IDLE: Duration = Duration::from_secs(1);

/// Replication settings that make a server without standbys keep complete
/// commit frames for its archive.
pub(crate) fn local_replication() -> ReplicationConfig {
    ReplicationConfig {
        enabled: true,
        mode: ReplicationMode::Primary,
        cluster_id: "local".to_string(),
        node_id: "local".to_string(),
        listen_addr: Some("127.0.0.1:0".to_string()),
        tls: Some(ReplicationTlsConfig {
            cert_path: PathBuf::new(),
            key_path: PathBuf::new(),
            ca_path: PathBuf::new(),
            require_client_cert: true,
            dev_localhost_plaintext: true,
        }),
        ..ReplicationConfig::default()
    }
}

/// Starts the archiver, or releases the log an earlier archive held when
/// archiving is off.
pub(crate) fn start(server: Arc<PgWireServer>) -> Result<()> {
    let Some(config) = server.config.log_archive.clone() else {
        return server
            .read_db()?
            .release_log_archive_hold()
            .map_err(PgWireError::from);
    };
    let root = config.dir.clone();
    // One archive per database in cluster mode.
    let dir = match server.cluster.as_ref() {
        Some(_) => {
            let path = server.read_db()?.data_path().to_path_buf();
            root.join(path.file_name().unwrap_or_default())
        }
        None => root,
    };
    let mut archive = LogArchive::open(&dir, server.config.fsync).map_err(PgWireError::from)?;
    eprintln!(
        "bicdb log archive: {} (archived through commit {})",
        dir.display(),
        archive.last_commit_seq()
    );
    let worker = BackgroundWorker::register(Arc::clone(&server));
    std::thread::Builder::new()
        .name("bicdb-log-archiver".to_string())
        .spawn(move || {
            let _worker = worker;
            let mut cursor = ReplicationCursor::new(archive.last_commit_seq());
            let mut last_error = None::<String>;
            let mut next_base = Instant::now();
            let mut next_verify = Instant::now();
            let mut base_task: Option<
                std::thread::JoinHandle<
                    std::result::Result<
                        Option<bicdb_core::log_archive::ArchivedBaseBackup>,
                        String,
                    >,
                >,
            > = None;
            while !server.is_shutdown_requested() {
                if base_task.as_ref().is_some_and(|task| task.is_finished()) {
                    let result = base_task
                        .take()
                        .unwrap()
                        .join()
                        .unwrap_or_else(|_| Err("base backup worker panicked".into()));
                    match result {
                        Ok(Some(_)) => {
                            if let Err(error) = prune_retained(&server, &config, &dir) {
                                eprintln!("bicdb log archive: retention failed: {error}");
                            }
                        }
                        Ok(None) => {
                            next_base = Instant::now()
                                + config.base_backup_interval.min(Duration::from_secs(1))
                        }
                        Err(error) => {
                            eprintln!("bicdb log archive: base backup failed: {error}");
                            next_base = Instant::now() + Duration::from_secs(60);
                        }
                    }
                }
                if config.base_backup_key.is_some()
                    && base_task.is_none()
                    && Instant::now() >= next_base
                {
                    next_base = Instant::now() + config.base_backup_interval;
                    if archive.meta().is_some() {
                        let prepared = server
                            .read_db()
                            .map_err(|error| error.to_string())
                            .and_then(|db| {
                                db.prepare_log_archive(&archive)
                                    .map_err(|error| error.to_string())
                            });
                        match prepared {
                            Ok(()) => {
                                let copying_server = Arc::clone(&server);
                                let copying_config = config.clone();
                                let copying_dir = dir.clone();
                                let worker = BackgroundWorker::register(Arc::clone(&server));
                                match std::thread::Builder::new()
                                    .name("bicdb-archive-base".into())
                                    .spawn(move || {
                                        let _worker = worker;
                                        copy_base_if_due(
                                            &copying_server,
                                            &copying_config,
                                            &copying_dir,
                                            false,
                                        )
                                    }) {
                                    Ok(task) => base_task = Some(task),
                                    Err(error) => {
                                        eprintln!(
                                            "bicdb log archive: cannot start base worker: {error}"
                                        );
                                        next_base = Instant::now() + Duration::from_secs(60);
                                    }
                                }
                            }
                            Err(error) => {
                                eprintln!("bicdb log archive: {error}");
                                next_base = Instant::now() + Duration::from_secs(60);
                            }
                        }
                    } else {
                        // Bootstrap still anchors only at a fully published first
                        // base. Once established, payload copying never stalls the
                        // commit stream, including during later full/large bases.
                        let initialized = server
                            .read_db()
                            .map_err(|error| error.to_string())
                            .and_then(|db| {
                                db.prepare_log_archive(&archive)
                                    .map_err(|error| error.to_string())
                            })
                            .and_then(|_| {
                                take_base_backup(&server, &config, &dir, archive.meta().is_none())
                            })
                            .and_then(|base| {
                                if base.is_none() {
                                    next_base = Instant::now()
                                        + config.base_backup_interval.min(Duration::from_secs(1));
                                }
                                if archive.meta().is_none() {
                                    if let Some(base) = base {
                                        server
                                            .read_db()
                                            .map_err(|error| error.to_string())?
                                            .initialize_log_archive_from_base(&mut archive, &base)
                                            .map_err(|error| error.to_string())?;
                                        cursor = ReplicationCursor::new(archive.last_commit_seq());
                                    }
                                }
                                Ok(())
                            });
                        if let Err(error) = initialized {
                            eprintln!("bicdb log archive: base backup failed: {error}");
                            next_base = Instant::now() + Duration::from_secs(60);
                        }
                    }
                }
                let archived = server
                    .read_db()
                    .map_err(|error| error.to_string())
                    .and_then(|db| {
                        if db.is_standby_role() {
                            return Ok(0);
                        }
                        db.archive_commits(&mut archive, &mut cursor, BATCH_COMMITS)
                            .map_err(|error| error.to_string())
                    });
                if Instant::now() >= next_verify {
                    if let Some(verify) = &server.config.verification {
                        // Base selection and restore run on this worker so a
                        // retention pass cannot delete files during the drill.
                        let primary = server.read_db().is_ok_and(|db| !db.is_standby_role());
                        let input = config
                            .base_backup_key
                            .as_deref()
                            .filter(|_| primary)
                            .map(|key| (dir.as_path(), key, &mut archive, &mut cursor));
                        match &archived {
                            Ok(_) if input.is_some() => {
                                if let Some(_drill) = backups::begin_restore_drill(&server) {
                                    verification::run(&server, input);
                                    next_verify = Instant::now() + verify.interval;
                                }
                            }
                            Ok(_) => {
                                verification::run(&server, input);
                                next_verify = Instant::now() + verify.interval;
                            }
                            Err(error) => {
                                verification::archive_failure(&server, error);
                                next_verify = Instant::now() + verify.interval;
                            }
                        }
                    }
                }
                match archived {
                    Ok(0) => {
                        last_error = None;
                        server.replication_feed.wait_beyond(cursor.sent_seq(), IDLE);
                    }
                    Ok(_) => last_error = None,
                    Err(error) => {
                        if last_error.as_ref() != Some(&error) {
                            eprintln!("bicdb log archive: {error}");
                            last_error = Some(error);
                        }
                        std::thread::sleep(Duration::from_secs(5));
                    }
                }
            }
            // Shutdown cancels each admitted backup; keep its registered
            // worker owned until it has released all pins and descriptors.
            if let Some(task) = base_task {
                let _ = task.join();
            }
        })?;
    Ok(())
}

/// Takes a base backup if the newest one is older than the interval, then
/// prunes what the kept base backups no longer need.
fn take_base_backup(
    server: &Arc<PgWireServer>,
    config: &LogArchiveConfig,
    dir: &Path,
    force: bool,
) -> std::result::Result<Option<bicdb_core::log_archive::ArchivedBaseBackup>, String> {
    let base = copy_base_if_due(server, config, dir, force)?;
    prune_retained(server, config, dir)?;
    Ok(base)
}

fn copy_base_if_due(
    server: &Arc<PgWireServer>,
    config: &LogArchiveConfig,
    dir: &Path,
    force: bool,
) -> std::result::Result<Option<bicdb_core::log_archive::ArchivedBaseBackup>, String> {
    let Some(key) = &config.base_backup_key else {
        return Ok(None);
    };
    let newest = bicdb_core::log_archive::list_base_backups(dir)
        .map_err(|error| error.to_string())?
        .pop();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64);
    let due = newest.is_none_or(|base| {
        now.saturating_sub(base.created_at) >= config.base_backup_interval.as_secs() as i64
    });
    let db = server.read_db().map_err(|error| error.to_string())?;
    if db.is_standby_role() {
        return Ok(None);
    }
    drop(db);
    let base = if force || due {
        let Some(report) = backups::archive_base(server, dir, key).map_err(|e| e.to_string())?
        else {
            return Ok(None);
        };
        bicdb_core::log_archive::record_base_backup(
            &report.artifact.path,
            report.commit_seq,
            report.captured_at,
            server.config.fsync,
        )
        .map_err(|e| e.to_string())?;
        let base = bicdb_core::log_archive::ArchivedBaseBackup {
            commit_seq: report.commit_seq,
            created_at: report.captured_at,
            path: report.artifact.path,
        };
        eprintln!(
            "bicdb log archive: base backup at commit {} ({})",
            base.commit_seq,
            base.path.display()
        );
        Some(base)
    } else {
        None
    };
    Ok(base)
}

fn prune_retained(
    server: &PgWireServer,
    config: &LogArchiveConfig,
    dir: &Path,
) -> std::result::Result<(), String> {
    let Some(key) = &config.base_backup_key else {
        return Ok(());
    };
    let pruned = bicdb_core::log_archive::prune_log_archive(
        dir,
        config.keep_base_backups,
        server.config.fsync,
    )
    .map_err(|error| error.to_string())?;
    if pruned.base_backups + pruned.segments > 0 {
        eprintln!(
            "bicdb log archive: removed {} old base backup(s) and {} log segment(s)",
            pruned.base_backups, pruned.segments
        );
    }
    // A fresh upgrade can retain a recent v3 base until the next base is due.
    // It has no object repository yet. Existing objects without configuration
    // still fail closed rather than being mistaken for a legacy-only archive.
    if dir.join("repository.json").exists() || dir.join("objects").exists() {
        bicdb_core::prune_backup_repository(dir, key, Duration::from_secs(3600), false)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod legacy_archive_tests {
    use super::*;
    #[test]
    fn a_recent_legacy_base_needs_no_repository_gc_before_v4_adoption() {
        let root = tempfile::tempdir().unwrap();
        let archive = root.path().join("archive");
        std::fs::create_dir(&archive).unwrap();
        let mut config = PgWireConfig::default();
        config.fsync = false;
        let server = PgWireServer::open(root.path().join("db"), config).unwrap();
        let path = bicdb_core::log_archive::base_backup_path(&archive, 0);
        let (_, sequence) = server
            .read_db()
            .unwrap()
            .create_consistent_backup(
                &path,
                bicdb_core::BackupCreateOptions {
                    passphrase: "key".into(),
                    base_backup: None,
                },
            )
            .unwrap();
        bicdb_core::log_archive::record_base_backup(&path, sequence, unix_timestamp(), true)
            .unwrap();
        let settings = LogArchiveConfig {
            dir: archive.clone(),
            base_backup_key: Some("key".into()),
            base_backup_interval: Duration::from_secs(24 * 3600),
            keep_base_backups: 7,
        };
        let result = take_base_backup(&server, &settings, &archive, false);
        assert!(
            matches!(result, Ok(None)),
            "legacy-only archives have no v4 repository to collect: {result:?}"
        );
        assert!(!archive.join("repository.json").exists());
        bicdb_core::verify_backup(path, "key").unwrap();
    }
}

#[cfg(test)]
mod streaming_during_base_tests {
    use super::*;
    use bicdb_core::log_archive::{read_log_archive, ArchivedBaseBackup, ReplayTarget};
    use bicdb_core::{BackupCreateOptions, BicDb, DbConfig, Record, StorageMode};

    struct Running(Arc<PgWireServer>);
    impl Drop for Running {
        fn drop(&mut self) {
            self.0.request_shutdown();
            let _ = self.0.wait_for_background_quiesce(Duration::from_secs(5));
        }
    }

    #[test]
    fn established_commit_archive_advances_while_a_periodic_base_is_copying() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("db");
        let directory = root.path().join("archive");
        let mut db = BicDb::open_with_config(
            &path,
            DbConfig::default()
                .with_storage_mode(StorageMode::ServerPaged)
                .with_paged_extent_bytes(1024 * 1024)
                .with_replication(local_replication()),
        )
        .unwrap();
        db.create_collection("rows").unwrap();
        db.bulk_load_insert(
            "rows",
            (0..200).map(|i| {
                Record::new(format!("row-{i}")).with_metadata(json!({"body":"p".repeat(4096)}))
            }),
        )
        .unwrap();
        let mut archive = LogArchive::open(&directory, true).unwrap();
        db.prepare_log_archive(&archive).unwrap();
        let base_path =
            bicdb_core::log_archive::base_backup_path(&directory, db.last_applied_commit_seq());
        let (_, sequence) = db
            .create_consistent_backup(
                &base_path,
                BackupCreateOptions {
                    passphrase: "key".into(),
                    base_backup: None,
                },
            )
            .unwrap();
        let old = ArchivedBaseBackup {
            commit_seq: sequence,
            created_at: unix_timestamp() - 60,
            path: base_path,
        };
        bicdb_core::log_archive::record_base_backup(&old.path, sequence, old.created_at, true)
            .unwrap();
        db.initialize_log_archive_from_base(&mut archive, &old)
            .unwrap();
        drop(archive);
        drop(db);
        let mut config = PgWireConfig::default();
        config.automatic_paged_checkpoint = false;
        config.online_backup_limits.bytes_per_second = 64 * 1024;
        config.log_archive = Some(LogArchiveConfig {
            dir: directory.clone(),
            base_backup_key: Some("key".into()),
            base_backup_interval: Duration::from_secs(1),
            keep_base_backups: 7,
        });
        let server = PgWireServer::open(&path, config).unwrap();
        let _running = Running(Arc::clone(&server));
        start(Arc::clone(&server)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let jobs = server.backup_jobs.lock().unwrap().status(None);
            if jobs.as_array().unwrap().iter().any(|job| {
                job["phase"] == "copying"
                    && job["progress"]["source_bytes_read"].as_u64().unwrap_or(0) > 0
            }) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "periodic base did not begin: {jobs}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let committed = {
            let mut db = server.db.write();
            db.insert("rows", Record::new("committed-during-copy"))
                .unwrap();
            db.last_applied_commit_seq()
        };
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            let mut archived = sequence;
            // Read-only scanning must never open a live archive with tail repair.
            let result = read_log_archive(&directory, sequence, ReplayTarget::default(), |frame| {
                archived = frame.commit_seq;
                Ok(true)
            });
            if result.is_ok() && archived >= committed {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "commit stream stalled behind the base copy: {result:?}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let jobs = server.backup_jobs.lock().unwrap().status(None);
        assert!(
            jobs.as_array()
                .unwrap()
                .iter()
                .any(|job| job["phase"] == "copying"),
            "archive caught up only after copying ended: {jobs}"
        );
        eprintln!(
            "archive advanced through commit {committed} while the periodic base was still copying"
        );
        server.request_shutdown();
        assert!(
            server.wait_for_background_quiesce(Duration::from_secs(5)),
            "copy or archive worker leaked past shutdown"
        );
    }
}
