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
    std::thread::Builder::new()
        .name("bicdb-log-archiver".to_string())
        .spawn(move || {
            let mut cursor = ReplicationCursor::new(archive.last_commit_seq());
            let mut last_error = None::<String>;
            let mut next_base = Instant::now();
            let mut next_verify = Instant::now();
            while !server.is_shutdown_requested() {
                if config.base_backup_key.is_some() && Instant::now() >= next_base {
                    next_base = Instant::now() + config.base_backup_interval;
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
                            Ok(_) => verification::run(&server, input),
                            Err(error) => verification::archive_failure(&server, error),
                        }
                        next_verify = Instant::now() + verify.interval;
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
        })?;
    Ok(())
}

/// Takes a base backup if the newest one is older than the interval, then
/// prunes what the kept base backups no longer need.
fn take_base_backup(
    server: &Arc<PgWireServer>,
    config: &LogArchiveConfig,
    dir: &std::path::Path,
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
    let base = if force || due {
        let base = db
            .archive_base_backup(dir, key)
            .map_err(|error| error.to_string())?;
        eprintln!(
            "bicdb log archive: base backup at commit {} ({})",
            base.commit_seq,
            base.path.display()
        );
        Some(base)
    } else {
        None
    };
    drop(db);
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
    Ok(base)
}
