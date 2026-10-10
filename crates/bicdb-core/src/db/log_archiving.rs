//! Feeding the commit log archive, and point-in-time recovery from it.
use super::*;
use crate::log_archive::{read_log_archive, LogArchive, ReplayTarget};

/// Hold name the commit log archive keeps the transaction log under.
pub const LOG_ARCHIVE_HOLDER: &str = "log-archive";
/// Commits applied per transaction while replaying.
const REPLAY_BATCH: usize = 256;

/// Outcome of replaying a log archive.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogReplayReport {
    /// Last commit of the base backup.
    pub base_commit_seq: u64,
    /// Last commit applied.
    pub restored_commit_seq: u64,
    /// Commits applied from the archive.
    pub commits_applied: u64,
    /// Time of the last applied commit that wrote data (Unix seconds).
    pub restored_timestamp: Option<i64>,
}

impl BicDb {
    /// Archives the commits after the archive's last one, up to `limit`, and
    /// holds the transaction log from there on. Returns how many were
    /// archived. `cursor` must start at `archive.last_commit_seq()`.
    pub fn archive_commits(
        &self,
        archive: &mut LogArchive,
        cursor: &mut ReplicationCursor,
        limit: usize,
    ) -> Result<usize> {
        // Hold before reading, so a checkpoint cannot cut what is unread.
        self.hold_log_for_archive(archive.last_commit_seq())?;
        let frames = self.replication_frames_after(cursor, limit)?;
        archive.append(&self.config.replication.cluster_id, &frames)?;
        if !frames.is_empty() {
            self.hold_log_for_archive(archive.last_commit_seq())?;
        }
        Ok(frames.len())
    }

    fn hold_log_for_archive(&self, seq: u64) -> Result<()> {
        if self.sync_quorum.holds().get(LOG_ARCHIVE_HOLDER) == Some(&seq) {
            return Ok(());
        }
        self.sync_quorum.hold(LOG_ARCHIVE_HOLDER, seq);
        self.persist_standby_slots()
    }

    /// Stops holding the transaction log for an archive (archiving was
    /// turned off).
    pub fn release_log_archive_hold(&self) -> Result<()> {
        if !self.sync_quorum.holds().contains_key(LOG_ARCHIVE_HOLDER) {
            return Ok(());
        }
        self.sync_quorum.release(LOG_ARCHIVE_HOLDER);
        self.persist_standby_slots()
    }

    /// Applies archived commits after this database's last commit, up to
    /// `target`. The database must be open as a replication standby.
    pub fn replay_log_archive(
        &mut self,
        archive: &Path,
        target: ReplayTarget,
    ) -> Result<LogReplayReport> {
        if !self.is_replication_standby() {
            return Err(BicDbError::Backup(
                "log replay needs the database open as a replication standby".to_string(),
            ));
        }
        let base = self.last_applied_commit_seq();
        let mut report = LogReplayReport {
            base_commit_seq: base,
            restored_commit_seq: base,
            ..LogReplayReport::default()
        };
        let mut batch = Vec::with_capacity(REPLAY_BATCH);
        let apply = |db: &mut BicDb, batch: &mut Vec<CommitFrame>, report: &mut LogReplayReport| {
            if batch.is_empty() {
                return Ok::<_, BicDbError>(());
            }
            let applied = db.apply_replication_batch(batch)?;
            report.restored_commit_seq = applied.last_applied_commit_seq;
            report.commits_applied += batch.len() as u64;
            if let Some(frame) = batch.iter().rev().find(|frame| !frame.writes.is_empty()) {
                report.restored_timestamp = Some(frame.timestamp);
            }
            batch.clear();
            Ok(())
        };
        read_log_archive(archive, base, target, |frame| {
            batch.push(frame);
            if batch.len() >= REPLAY_BATCH {
                apply(self, &mut batch, &mut report)?;
            }
            Ok(true)
        })?;
        apply(self, &mut batch, &mut report)?;
        Ok(report)
    }
}

/// Point-in-time recovery of a whole database: restores `backup` into
/// `target_dir`, then replays the commit log archive up to `target`. The
/// result opens as an ordinary database.
pub fn restore_from_log_archive(
    backup: &Path,
    target_dir: &Path,
    options: crate::backup::BackupRestoreOptions,
    archive: &Path,
    target: ReplayTarget,
) -> Result<LogReplayReport> {
    let meta = serde_json::from_slice::<crate::log_archive::LogArchiveMeta>(
        &fs::read(archive.join("archive.json")).map_err(|error| {
            BicDbError::Backup(format!("log archive {}: {error}", archive.display()))
        })?,
    )?;
    crate::backup::restore_backup(backup, target_dir, options)?;
    // Replay as a standby of the archived cluster.
    for file in [ha_node::HA_NODE_FILE, DEFAULT_REPLICATION_STATE] {
        match fs::remove_file(target_dir.join(file)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let storage_mode = crate::format::load_metadata(target_dir)?
        .map(|metadata| metadata.storage_mode)
        .unwrap_or_default();
    let replication = ReplicationConfig {
        enabled: true,
        mode: ReplicationMode::Standby,
        cluster_id: meta.cluster_id.clone(),
        node_id: "log-replay".to_string(),
        listen_addr: Some("127.0.0.1:0".to_string()),
        tls: Some(crate::replication::ReplicationTlsConfig {
            cert_path: PathBuf::new(),
            key_path: PathBuf::new(),
            ca_path: PathBuf::new(),
            require_client_cert: true,
            dev_localhost_plaintext: true,
        }),
        ..ReplicationConfig::default()
    };
    let report = {
        let mut db = BicDb::open_with_config(
            target_dir,
            DbConfig::default()
                .with_storage_mode(storage_mode)
                .with_replication(replication),
        )?;
        let report = db.replay_log_archive(archive, target)?;
        db.close()?;
        report
    };
    // From here on it is an ordinary database, not a standby.
    for file in [ha_node::HA_NODE_FILE, DEFAULT_REPLICATION_STATE] {
        match fs::remove_file(target_dir.join(file)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(report)
}

impl BicDb {
    /// Takes a base backup into the commit log archive in `dir`, encrypted
    /// with `key`. Commits pause while it is written.
    pub fn archive_base_backup(
        &self,
        dir: &Path,
        key: &str,
    ) -> Result<crate::log_archive::ArchivedBaseBackup> {
        let staging = dir.join(format!(".base-{}.partial", uuid::Uuid::new_v4().simple()));
        let (_, commit_seq) = self.create_consistent_backup(
            &staging,
            crate::backup::BackupCreateOptions {
                passphrase: key.to_string(),
                base_backup: None,
            },
        )?;
        let path = crate::log_archive::base_backup_path(dir, commit_seq);
        fs::rename(&staging, &path)?;
        let created_at = unix_timestamp();
        crate::log_archive::record_base_backup(&path, commit_seq, created_at, self.config.fsync)?;
        Ok(crate::log_archive::ArchivedBaseBackup {
            commit_seq,
            created_at,
            path,
        })
    }
}

/// Point-in-time recovery from a self-contained archive: picks the newest
/// base backup in `archive` that precedes `target`, restores it into
/// `target_dir`, and replays the archived commits up to `target`.
pub fn restore_point_in_time(
    archive: &Path,
    target_dir: &Path,
    key: &str,
    force: bool,
    target: ReplayTarget,
) -> Result<LogReplayReport> {
    let base = crate::log_archive::base_backup_for(archive, target)?;
    restore_from_log_archive(
        &base.path,
        target_dir,
        crate::backup::BackupRestoreOptions {
            passphrase: key.to_string(),
            force,
        },
        archive,
        target,
    )
}
