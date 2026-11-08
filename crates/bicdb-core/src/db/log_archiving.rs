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
        // A failed archive is never retried on the same handle: a failed
        // fsync may have dropped the pages it could not write, and a later
        // successful one would then confirm bytes that are not on disk. The
        // hold stays at the last confirmed commit, so the log keeps
        // everything after it for a new handle to rewrite.
        archive.check_healthy()?;
        // Hold before reading, so a checkpoint cannot cut what is unread.
        self.prepare_log_archive(archive)?;
        let frames = self.replication_frames_after(cursor, limit)?;
        if let Err(error) = archive.append(&self.config.replication.cluster_id, &frames) {
            // Nothing of a failed batch is confirmed; resume from the last
            // confirmed commit, not the end of the batch the cursor read.
            *cursor = ReplicationCursor::new(archive.last_commit_seq());
            return Err(error);
        }
        if !frames.is_empty() {
            // Only now are the records (and their directory entries) durable
            // in the archive: release the log up to them.
            self.hold_log_for_archive(archive.last_commit_seq())?;
        }
        Ok(frames.len())
    }

    /// Opens the commit log archive in `dir` for a server that feeds it.
    ///
    /// Whatever the archive holds past the persisted transaction-log hold was
    /// never confirmed by the archiver that wrote it (it failed, or crashed
    /// before advancing the hold), so it is discarded and rewritten from the
    /// log, which the hold kept. This is what makes a failed archive safe to
    /// resume: bytes whose `fsync` failed are never trusted, they are
    /// replaced. When the log no longer holds that suffix (the hold was
    /// released or never persisted), the archive is kept as it is.
    ///
    /// An archive that cannot be opened (damage in its newest segment, an
    /// I/O error) or whose discard fails comes back as a failed handle, not
    /// an error: the archiver stays stopped and visible, and the database
    /// keeps running with the log retained.
    pub fn open_log_archive(&self, dir: &Path, fsync: bool) -> Result<LogArchive> {
        let mut archive = match LogArchive::open(dir, fsync) {
            Ok(archive) => archive,
            Err(error) => return Ok(LogArchive::failed(dir, fsync, "log_archive.open", &error)),
        };
        if let Err(error) = self.reconcile_log_archive(&mut archive) {
            if archive.failure().is_none() {
                archive.fail_with("log_archive.open", &error);
            }
        }
        Ok(archive)
    }

    /// See [`Self::open_log_archive`]. Returns the commits discarded for
    /// rewriting.
    pub fn reconcile_log_archive(&self, archive: &mut LogArchive) -> Result<u64> {
        let discarded = self.reconcile_log_archive_suffix(archive)?;
        // A recorded verification failure keeps every new handle stopped
        // until the confirmed prefix reads back intact again.
        let recorded = crate::archive_guard::load_verification(
            &self
                .path
                .join(crate::archive_guard::COMMIT_LOG_VERIFICATION_FILE),
        );
        if recorded.is_some_and(|verification| !verification.ok) {
            if let Some(dir) = archive.dir().map(Path::to_path_buf) {
                let verification = self.verify_log_archive(&dir)?;
                if !verification.ok {
                    archive.fail_verification(&verification.detail);
                }
            }
        }
        Ok(discarded)
    }

    /// Read back the commit-log archive's confirmed prefix (from the
    /// archive's first commit through the transaction-log hold): every
    /// record's checksum and the commit-sequence continuity. The result is
    /// persisted in the database directory. A failed result does not stop
    /// an archiver by itself; the caller fails its handle (the server's
    /// archiver does), and every new handle stays failed while it is
    /// recorded (see [`Self::reconcile_log_archive`]).
    pub fn verify_log_archive(
        &self,
        dir: &Path,
    ) -> Result<crate::archive_guard::ArchiveVerification> {
        let hold = self.sync_quorum.holds().get(LOG_ARCHIVE_HOLDER).copied();
        let meta = fs::read(dir.join("archive.json")).ok().and_then(|bytes| {
            serde_json::from_slice::<crate::log_archive::LogArchiveMeta>(&bytes).ok()
        });
        let (ok, through, detail) = match (meta, hold) {
            (Some(meta), Some(hold)) if hold >= meta.first_commit_seq => {
                let mut count = 0u64;
                let result = read_log_archive(
                    dir,
                    meta.first_commit_seq - 1,
                    ReplayTarget {
                        commit_seq: Some(hold),
                        timestamp: None,
                    },
                    |_| {
                        count += 1;
                        Ok(true)
                    },
                );
                match result {
                    Ok(()) => (
                        true,
                        hold,
                        format!(
                            "commits {}..={hold} read back ({count})",
                            meta.first_commit_seq
                        ),
                    ),
                    Err(error) => (false, hold, error.to_string()),
                }
            }
            (_, hold) => (true, hold.unwrap_or(0), "nothing confirmed yet".to_string()),
        };
        let verification = crate::archive_guard::ArchiveVerification {
            archive: "commit_log".into(),
            ok,
            at_unix_millis: crate::archive_guard::unix_millis(),
            through,
            detail,
        };
        crate::archive_guard::store_verification(
            &self
                .path
                .join(crate::archive_guard::COMMIT_LOG_VERIFICATION_FILE),
            &verification,
            self.config.fsync,
        )?;
        if !ok {
            bicdb_page::health::log_event(&serde_json::json!({
                "event": "archive.verification_failed",
                "severity": "critical",
                "archive": "commit_log",
                "through": through,
                "detail": verification.detail,
                "action": "the archiver stops; nothing more is confirmed until the prefix reads back intact",
            }));
        }
        Ok(verification)
    }

    /// The last persisted commit-log archive verification.
    pub fn log_archive_verification(&self) -> Option<crate::archive_guard::ArchiveVerification> {
        crate::archive_guard::load_verification(
            &self
                .path
                .join(crate::archive_guard::COMMIT_LOG_VERIFICATION_FILE),
        )
    }

    /// Set the write backpressure (from an [`crate::archive_guard::ArchiveGuard`]
    /// evaluation). `Refuse` fails write operations with
    /// [`BicDbError::ArchiveBackpressure`] (SQLSTATE 53100); `Throttle` delays
    /// each write commit by `delay`; `None` lifts it.
    pub fn set_archive_backpressure(
        &self,
        backpressure: crate::archive_guard::Backpressure,
        reason: &str,
        delay: std::time::Duration,
    ) {
        self.archive_backpressure.set(backpressure, reason, delay);
    }

    pub fn archive_backpressure(&self) -> crate::archive_guard::Backpressure {
        self.archive_backpressure.get()
    }

    /// Guard inputs for this database: the page-log archive (failure,
    /// unarchived bytes, its volume), the commit-log archive given by the
    /// caller (`dir` and its current failure), the bytes of
    /// `transactions.log` held for it, both verification records and the
    /// data volume.
    pub fn archive_guard_inputs(
        &self,
        commit_log: Option<(&Path, Option<&crate::WriteFailure>)>,
    ) -> crate::archive_guard::ArchiveGuardInputs {
        use crate::archive_guard::volume_space;
        let mut inputs = crate::archive_guard::ArchiveGuardInputs {
            now_unix_millis: crate::archive_guard::unix_millis(),
            data_volume: volume_space(&self.path),
            ..Default::default()
        };
        fn since(
            inputs: &mut crate::archive_guard::ArchiveGuardInputs,
            failure: Option<&crate::WriteFailure>,
        ) {
            if let Some(failure) = failure {
                inputs.failure_since_unix_millis = Some(
                    inputs
                        .failure_since_unix_millis
                        .map_or(failure.at_unix_millis, |at| at.min(failure.at_unix_millis)),
                );
            }
        }
        if let Some(status) = self.wal_archive_status() {
            since(&mut inputs, status.failure.as_ref());
            inputs.retained_bytes = inputs
                .retained_bytes
                .saturating_add(status.unarchived_bytes);
            if let Some(destination) = self
                .paged_records
                .as_ref()
                .and_then(|paged| paged.store().wal().archive_destination())
            {
                if let Some(space) = volume_space(&destination) {
                    inputs
                        .archive_volumes
                        .push(("page_log_archive".into(), space));
                }
            }
            let recorded = self.paged_records.as_ref().and_then(|paged| {
                crate::archive_guard::load_verification(
                    &super::spatial_paged::page_log_verification_path(paged.store().wal().path()),
                )
            });
            inputs.verification_failed |= recorded.is_some_and(|verification| !verification.ok);
        }
        if let Some((dir, failure)) = commit_log {
            since(&mut inputs, failure);
            // The held log counts only while the archive is behind.
            let hold = self.sync_quorum.holds().get(LOG_ARCHIVE_HOLDER).copied();
            if hold.is_some_and(|hold| hold < self.current_commit_seq()) {
                inputs.retained_bytes = inputs
                    .retained_bytes
                    .saturating_add(fs::metadata(self.tx_log_path()).map_or(0, |m| m.len()));
            }
            if let Some(space) = volume_space(dir) {
                inputs
                    .archive_volumes
                    .push(("commit_log_archive".into(), space));
            }
            inputs.verification_failed |= self
                .log_archive_verification()
                .is_some_and(|verification| !verification.ok);
        }
        inputs
    }

    fn reconcile_log_archive_suffix(&self, archive: &mut LogArchive) -> Result<u64> {
        let Some(hold) = self.sync_quorum.holds().get(LOG_ARCHIVE_HOLDER).copied() else {
            return Ok(0);
        };
        let Some(meta) = archive.meta().cloned() else {
            return Ok(0);
        };
        // The hold can sit below a fresh archive's anchor (it is set before
        // the anchor is installed); nothing below the anchor is in the
        // archive to discard.
        let keep_through = hold.max(meta.first_commit_seq.saturating_sub(1));
        if archive.last_commit_seq() <= keep_through {
            return Ok(0);
        }
        if read_wal_base(&self.path)? > keep_through {
            // The log was cut past the hold (the hold was released while
            // archiving was off): the suffix cannot be rewritten, keep it.
            return Ok(0);
        }
        let discarded = archive.discard_after(keep_through)?;
        if discarded > 0 {
            bicdb_page::health::log_event(&serde_json::json!({
                "event": "archive.unconfirmed_suffix_discarded",
                "severity": "warning",
                "archive": "commit_log",
                "kept_through_commit": keep_through,
                "discarded_commits": discarded,
                "action": "rewriting the discarded commits from the transaction log",
            }));
        }
        Ok(discarded)
    }

    fn hold_log_for_archive(&self, seq: u64) -> Result<()> {
        if self.sync_quorum.holds().get(LOG_ARCHIVE_HOLDER) == Some(&seq) {
            return Ok(());
        }
        self.sync_quorum.hold(LOG_ARCHIVE_HOLDER, seq);
        self.persist_standby_slots()
    }

    /// Validate the archive identity before writing a base into its directory,
    /// and pin its required suffix before a snapshot or export can race retention.
    pub fn prepare_log_archive(&self, archive: &LogArchive) -> Result<()> {
        archive.check_healthy()?;
        if archive
            .meta()
            .is_some_and(|meta| meta.cluster_id != self.config.replication.cluster_id)
        {
            return Err(BicDbError::Backup(
                "log archive: archive cluster identity mismatch".into(),
            ));
        }
        self.hold_log_for_archive(archive.last_commit_seq())
    }

    /// Initialize a fresh archive only after this database published its base.
    /// The base's suffix is retained by `archive_base_backup` across checkpoint
    /// and the interval before the archive metadata is installed.
    pub fn initialize_log_archive_from_base(
        &self,
        archive: &mut LogArchive,
        base: &crate::log_archive::ArchivedBaseBackup,
    ) -> Result<()> {
        if base.commit_seq > self.current_commit_seq() {
            return Err(BicDbError::Backup(
                "archive base is ahead of this database".into(),
            ));
        }
        archive.initialize_from_base(&self.config.replication.cluster_id, base.commit_seq)
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
        // Establish retention before taking the snapshot. Pinning only after
        // the base is published can lose its new suffix to a checkpoint. An
        // existing hold already retains everything after it, which the
        // base's suffix is; moving it back to 0 would also make a later
        // reopen treat the whole archive as unconfirmed.
        if !self.sync_quorum.holds().contains_key(LOG_ARCHIVE_HOLDER) {
            self.hold_log_for_archive(0)?;
        }
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
