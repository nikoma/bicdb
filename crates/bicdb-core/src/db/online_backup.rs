use super::*;

pub(super) fn ensure_no_pending_restore(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path.join("backup-restore.pending")) {
        Ok(_) => Err(BicDbError::Backup(
            "database has an interrupted backup restore; retry the restore with --force before opening it".into(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

impl BicDb {
    /// Capture one committed physical recovery point. The returned owned
    /// capability can copy without borrowing BicDb; hosts must release their
    /// outer database guard before starting repository I/O.
    pub fn capture_online_backup(
        &self,
        limits: crate::backup::OnlineBackupLimits,
    ) -> Result<crate::backup::OnlineBackupSnapshot> {
        limits.validate()?;
        let owner = crate::backup::OnlineBackupCheckpoint::begin_open_database(self.data_path())?;
        let started = Instant::now();
        let _admission = self
            .serializable_commit_admission
            .try_write_for(Duration::from_millis(limits.max_coordination_millis))
            .ok_or_else(|| {
                BicDbError::Backup(
                    "commit admission exceeded the backup coordination budget".into(),
                )
            })?;
        self.check_recovery_state()?;
        let commit_seq = self.last_applied_commit_seq();
        self.tx_log_handle().write_durable(commit_seq)?;
        crate::backup::capture_online_database(
            self,
            self.paged_records.as_ref().map(|p| Arc::clone(p.store())),
            commit_seq,
            limits,
            started,
            owner,
            self._directory_lock.clone(),
        )
    }
}
