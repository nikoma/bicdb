//! Bootstrapping a standby from a snapshot of the primary.
//!
//! A standby that cannot continue from its own position (an empty directory
//! when the primary's log no longer reaches back to the start, a standby that
//! fell behind the retained log, or a former primary whose history diverged
//! from the current one) receives a consistent archive of the primary instead
//! of the commit stream.
//!
//! The standby restores the archive into a staging directory next to its own
//! (`<dir>.bootstrap`) and marks it complete. The next open of the database
//! swaps the staging directory in; until then the old directory is untouched,
//! so a crash at any point leaves either the old or the new database whole.
use super::*;

const BOOTSTRAP_READY: &str = "BOOTSTRAP_READY";

/// Staging directory used while a standby bootstrap is installed.
pub fn standby_bootstrap_staging(path: &Path) -> PathBuf {
    sibling_path(path, "bootstrap")
}

fn sibling_path(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    name.push(format!(".{suffix}"));
    path.with_file_name(name)
}

/// How a connecting standby continues.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StandbyResume {
    /// Stream commits after the standby's position.
    Stream,
    /// The standby needs a snapshot; the reason is for logs.
    Snapshot(String),
}

impl BicDb {
    /// Decides whether a standby at `applied`, last following a primary of
    /// `standby_epoch`, can continue from the commit stream.
    pub fn standby_resume(&self, standby_epoch: u64, applied: u64) -> Result<StandbyResume> {
        let current = self.current_commit_seq();
        if applied > current {
            return Ok(StandbyResume::Snapshot(format!(
                "the standby is at commit {applied}, ahead of this primary's {current}: its history diverged"
            )));
        }
        // Commits after the start of a newer epoch came from a primary the
        // standby did not follow: anything it holds past that point may differ.
        let diverged_after = self.ha_node.read(|file| {
            file.epoch_starts
                .range(standby_epoch.saturating_add(1)..)
                .map(|(_, start)| *start)
                .min()
        });
        if let Some(start) = diverged_after {
            if applied > start {
                return Ok(StandbyResume::Snapshot(format!(
                    "the standby holds commits up to {applied} from an older primary term, which ended at {start}"
                )));
            }
        }
        let wal_base = read_wal_base(&self.path)?;
        if applied < wal_base {
            return Ok(StandbyResume::Snapshot(format!(
                "the standby is at commit {applied}, but the log only reaches back to {wal_base}"
            )));
        }
        Ok(StandbyResume::Stream)
    }

    /// Writes a consistent archive of this database for a standby bootstrap,
    /// encrypted with `key`. Returns the commit sequence number it contains.
    /// Commits pause while the archive is written locally.
    pub fn create_standby_snapshot(&self, archive: &Path, key: &str) -> Result<u64> {
        let (_, commit_seq) = self.create_consistent_backup(
            archive,
            crate::backup::BackupCreateOptions {
                passphrase: key.to_string(),
                base_backup: None,
            },
        )?;
        Ok(commit_seq)
    }
}

/// Restores a primary's snapshot archive into the staging directory of the
/// standby at `path` and marks it ready; the next open installs it. The
/// standby keeps its own voting identity and highest observed epoch, while
/// adopting the snapshot's data epoch and accepted history.
pub fn stage_standby_bootstrap(path: &Path, archive: &Path, key: &str, fsync: bool) -> Result<()> {
    let staging = standby_bootstrap_staging(path);
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    crate::backup::restore_backup(
        archive,
        &staging,
        crate::backup::BackupRestoreOptions {
            passphrase: key.to_string(),
            force: true,
        },
    )?;
    let source = match fs::read(staging.join(ha_node::HA_NODE_FILE)) {
        Ok(bytes) => serde_json::from_slice::<ha_node::HaNodeFile>(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            ha_node::HaNodeFile::default()
        }
        Err(error) => return Err(error.into()),
    };
    // The primary's own role, votes and replication slots do not belong to
    // this node. Its data epoch does describe the snapshot's exact history.
    for file in [ha_node::HA_NODE_FILE, DEFAULT_REPLICATION_STATE] {
        match fs::remove_file(staging.join(file)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let mut node = match fs::read(path.join(ha_node::HA_NODE_FILE)) {
        Ok(bytes) => serde_json::from_slice::<ha_node::HaNodeFile>(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            ha_node::HaNodeFile::default()
        }
        Err(error) => return Err(error.into()),
    };
    node.role = Some(ha_node::NodeRole::Standby);
    node.slots.clear();
    node.switchover = None;
    node.verification_error = None;
    node.epoch = node.epoch.max(source.epoch);
    node.data_epoch = source.data_epoch;
    node.epoch_starts = source.epoch_starts;
    node.primary_id = source.primary_id.or(node.primary_id);
    node.primary_sql_address = source.primary_sql_address.or(node.primary_sql_address);
    storage::write_atomic(
        &staging.join(ha_node::HA_NODE_FILE),
        &serde_json::to_vec_pretty(&node)?,
        fsync,
    )?;
    storage::write_atomic(&staging.join(BOOTSTRAP_READY), b"ready\n", fsync)?;
    Ok(())
}

/// Installs a staged standby bootstrap of `path`, if one is complete, and
/// discards an incomplete one. Returns true when a bootstrap was installed.
pub fn finish_standby_bootstrap(path: &Path) -> Result<bool> {
    let staging = standby_bootstrap_staging(path);
    if !staging.exists() {
        return Ok(false);
    }
    if !staging.join(BOOTSTRAP_READY).exists() {
        // Interrupted while staging: the old directory is still whole.
        fs::remove_dir_all(&staging)?;
        return Ok(false);
    }
    // Claim both inodes before renaming either. Installing before the normal
    // open lock used to replace a running server's directory underneath it.
    let _old_lock = if path.exists() {
        Some(bicdb_page::DirectoryLock::acquire(path).map_err(|error| {
            BicDbError::Replication(format!(
                "cannot install bootstrap over an open database: {error}"
            ))
        })?)
    } else {
        None
    };
    let _new_lock = bicdb_page::DirectoryLock::acquire(&staging).map_err(|error| {
        BicDbError::Replication(format!("bootstrap is already being installed: {error}"))
    })?;
    let replaced = sibling_path(path, "replaced");
    if replaced.exists() {
        fs::remove_dir_all(&replaced)?;
    }
    if path.exists() {
        fs::rename(path, &replaced)?;
    }
    fs::rename(&staging, path)?;
    fs::remove_file(path.join(BOOTSTRAP_READY))?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    if replaced.exists() {
        fs::remove_dir_all(&replaced)?;
    }
    eprintln!(
        "bicdb replication: installed a snapshot of the primary into {}",
        path.display()
    );
    Ok(true)
}
