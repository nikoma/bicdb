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
use std::fs::OpenOptions;

const BOOTSTRAP_READY: &str = "BOOTSTRAP_READY";
#[cfg(test)]
mod namespace_cut;
mod rename_no_replace;

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
    // The old server may genuinely remain open until its host requests restart.
    // Stage only reads that root; its backup owner + actual SH preserve its
    // identity against a concurrent physical backup/install, without requiring
    // or impersonating a closed data owner.
    let _old_read = BootstrapReadSource::acquire_if_existing(path)?;
    crate::backup::restore_backup_with_retained_tail(
        archive,
        &staging,
        crate::backup::BackupRestoreOptions {
            passphrase: key.to_string(),
            force: true,
        },
        |staging| {
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
            // READY may now be visible, but the SAME restore still owns its
            // pending marker. A failure/crash here must never permit install.
            bicdb_page::test_faults::io_fault("bootstrap.ready.before_restore_complete", staging)
                .map_err(BicDbError::from)?;
            Ok(())
        },
    )?;
    Ok(())
}

/// Fault-injection and failure site of the bootstrap install (steps
/// `bootstrap.rename` and `bootstrap.dir_sync`).
pub const BOOTSTRAP_SITE: &str = "bootstrap";

/// Installs a staged standby bootstrap of `path`, if one is complete, and
/// discards an incomplete one. Returns true when a bootstrap was installed.
///
/// The install renames the old directory aside, renames the staging
/// directory into place, and `fsync`s the parent before retiring the current
/// old preimage. An obsolete replaced tree is cleared earlier only while an
/// actual current old owner remains. If the old name is absent, the retained
/// replaced preimage stays through installation and the parent durability cut.
/// `BOOTSTRAP_READY` stays inside the installed directory until
/// that `fsync` succeeded: an install whose directory sync failed (the open
/// fails with `WriteFailed`, site `bootstrap.dir_sync`) is completed by the
/// next open, which syncs again before it removes the marker and the old
/// directory.
pub fn finish_standby_bootstrap(path: &Path) -> Result<bool> {
    let staging = standby_bootstrap_staging(path);
    let replaced = sibling_path(path, "replaced");
    let staging_exists = path_entry_exists(&staging)?;
    if !staging_exists && !path_entry_exists(&path.join(BOOTSTRAP_READY))? {
        return Ok(false);
    }
    // All real closed owners are acquired before any removal or rename. No
    // guard is rebound by its textual path after a directory moves.
    let ownership = BootstrapInstallOwnership {
        old: BootstrapTreeOwner::acquire_if_existing(path)?,
        staged: BootstrapTreeOwner::acquire_if_existing(&staging)?,
        replaced: BootstrapTreeOwner::acquire_if_existing(&replaced)?,
    };
    #[cfg(test)]
    namespace_cut::observe(
        path,
        "after_owner_capture",
        serde_json::json!({
            "original_old_claimed":ownership.old.is_some(),
            "original_replaced_claimed":ownership.replaced.is_some(),
            "original_staged_claimed":ownership.staged.is_some(),
        }),
    );
    if !staging_exists {
        let installed = ownership.old.as_ref().ok_or_else(|| {
            BicDbError::Replication(
                "bootstrap completion has no actual installed-tree owner".into(),
            )
        })?;
        let ready = prepare_bootstrap_ready_durability(path, &ownership, installed)?;
        complete_bootstrap_install(path, &ready, ownership.replaced.as_ref())?;
        return Ok(true);
    }
    let installed = ownership.staged.as_ref().ok_or_else(|| {
        BicDbError::Replication("bootstrap staging disappeared before ownership".into())
    })?;
    if !path_entry_exists(&staging.join(BOOTSTRAP_READY))? {
        installed.require_at(&staging)?;
        fs::remove_dir_all(&staging)?;
        return Ok(false);
    }
    let ready = prepare_bootstrap_ready_durability(&staging, &ownership, installed)?;
    // Only the actual originally retained cleanup owner may be removed.
    // A path which appeared after an original None is not cleanup authority.
    if ownership.old.is_some() {
        if let Some(original_replaced) = ownership.replaced.as_ref() {
            original_replaced.require_at(&replaced)?;
            fs::remove_dir_all(&replaced)?;
        }
    }
    let renamed = (|| -> Result<Option<&BootstrapTreeOwner>> {
        bicdb_page::test_faults::io_fault(&format!("{BOOTSTRAP_SITE}.rename"), path)?;
        let old_at_replaced = if let Some(old) = ownership.old.as_ref() {
            old.require_at(path)?;
            rename_no_replace::rename(path, &replaced)?;
            Some(old)
        } else {
            None
        };
        installed.require_at(&staging)?;
        // A genuine interrupted old->replaced state, with no synthetic marker
        // or substituted owner, is observable before the second real rename.
        bicdb_page::test_faults::io_fault(&format!("{BOOTSTRAP_SITE}.stage_rename"), path)?;
        // This atomic destination condition closes normal Core creation after
        // custody capture; never move aside a fresh unowned root or replace it.
        rename_no_replace::rename(&staging, path)?;
        Ok(old_at_replaced)
    })();
    let old_at_replaced = renamed.map_err(|error| BicDbError::WriteFailed {
        site: format!("{BOOTSTRAP_SITE}.rename"),
        detail: format!("{}: {error}", path.display()),
    })?;
    bicdb_page::test_faults::crash_point(&format!("{BOOTSTRAP_SITE}.after_rename"));
    #[cfg(test)]
    namespace_cut::observe(
        path,
        "after_install_rename",
        serde_json::json!({
            "original_old_claimed":ownership.old.is_some(),
            "original_replaced_claimed":ownership.replaced.is_some(),
            "actual_old_moved_to_replaced":old_at_replaced.is_some(),
        }),
    );
    // The SAME staged owner follows the installed root. Cleanup retains the
    // actual moved old owner, or the already-retained replaced predecessor.
    let cleanup = old_at_replaced.or_else(|| {
        // A prior interrupted install may have moved old here. Keep this SAME
        // actual retained preimage through parent fsync. Its logical association
        // across crashes is a separate, still-open recovery protocol question.
        ownership
            .old
            .is_none()
            .then_some(ownership.replaced.as_ref())
            .flatten()
    });
    complete_bootstrap_install(path, &ready, cleanup)?;
    eprintln!(
        "bicdb replication: installed a snapshot of the primary into {}",
        path.display()
    );
    Ok(true)
}

fn require_bootstrap_restore_complete(root: &Path) -> Result<()> {
    if path_entry_exists(&root.join("backup-restore.pending"))? {
        return Err(BicDbError::Replication(
            "bootstrap has an interrupted backup restore; READY cannot authorize installation while backup-restore.pending remains".into(),
        ));
    }
    Ok(())
}

/// An actual closed tree, including a Page owner which can outlive BicDb.
/// Physical/File owners remain lexical through direct filesystem mutation;
/// nothing exposes or reconstructs this custody from a marker or path string.
struct BootstrapTreeOwner {
    _physical: storage::BackupWriteGuard,
    _backup: File,
    _paged: bicdb_page::DirectoryLock,
    _root: bicdb_page::DirectoryLock,
}

fn bootstrap_directory_exists(root: &Path) -> Result<bool> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(BicDbError::Replication(
            "bootstrap tree must be an actual non-symlink directory".into(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

impl BootstrapTreeOwner {
    fn acquire_if_existing(root: &Path) -> Result<Option<Self>> {
        if !bootstrap_directory_exists(root)? {
            return Ok(None);
        }
        let claim = bicdb_page::DirectoryLock::acquire(root).map_err(|error| {
            BicDbError::Replication(format!(
                "cannot mutate bootstrap tree over an open database: {error}"
            ))
        })?;
        claim
            .require_directory(root)
            .map_err(|error| BicDbError::Replication(error.to_string()))?;
        let backup = crate::backup::open_backup_lock_file(&root.join(storage::BACKUP_ONLINE_LOCK))?;
        // Core uses this order too. Hold real SH before creating a missing
        // conventional child, then take its nonblocking claim even in Memory
        // mode. A later standalone Page owner cannot enter a formerly None slot.
        let physical = storage::acquire_database_backup_write_guard(root)?;
        #[cfg(test)]
        namespace_cut::observe(
            root,
            "before_page_claim",
            serde_json::json!({
                "actual_root_claimed":true,"actual_root_SH_acquired":true,
            }),
        );
        let paged_path = root.join(DEFAULT_PAGED_DIR);
        if !bootstrap_directory_exists(&paged_path)? {
            fs::create_dir_all(&paged_path)?;
        }
        let paged = bicdb_page::DirectoryLock::acquire(&paged_path).map_err(|error| {
            BicDbError::Replication(format!("bootstrap Page owner remains live: {error}"))
        })?;
        paged
            .require_directory(&paged_path)
            .map_err(|error| BicDbError::Replication(error.to_string()))?;
        Ok(Some(Self {
            _physical: physical,
            _backup: backup,
            _paged: paged,
            _root: claim,
        }))
    }
    fn require_at(&self, expected: &Path) -> Result<()> {
        self._root
            .require_retained_directory_at(expected)
            .map_err(|error| BicDbError::Replication(error.to_string()))?;
        self._paged
            .require_retained_directory_at(&expected.join(DEFAULT_PAGED_DIR))
            .map_err(|error| BicDbError::Replication(error.to_string()))
    }
}

struct BootstrapInstallOwnership {
    old: Option<BootstrapTreeOwner>,
    staged: Option<BootstrapTreeOwner>,
    replaced: Option<BootstrapTreeOwner>,
}

struct BootstrapReadSource {
    _physical: storage::BackupWriteGuard,
    _backup: File,
}
impl BootstrapReadSource {
    fn acquire_if_existing(root: &Path) -> Result<Option<Self>> {
        if !bootstrap_directory_exists(root)? {
            return Ok(None);
        }
        let backup = crate::backup::open_backup_lock_file(&root.join(storage::BACKUP_ONLINE_LOCK))?;
        let physical = storage::acquire_database_backup_write_guard(root)?;
        Ok(Some(Self {
            _physical: physical,
            _backup: backup,
        }))
    }
}

/// A NEW actual file/directory durability result under the same closed-tree
/// ownership, not a claim that an earlier restore API returned Ok. It retains
/// those actual files/owners across the subsequent rename/completion tail.
struct BootstrapReadyDurability<'a> {
    _ownership: &'a BootstrapInstallOwnership,
    tree: &'a BootstrapTreeOwner,
    _ready: File,
    _node: File,
}

fn bootstrap_barrier_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(BicDbError::Replication(
            "bootstrap barrier requires an actual regular file".into(),
        ));
    }
    Ok(file)
}

fn prepare_bootstrap_ready_durability<'a>(
    root: &Path,
    ownership: &'a BootstrapInstallOwnership,
    tree: &'a BootstrapTreeOwner,
) -> Result<BootstrapReadyDurability<'a>> {
    use std::io::Read;
    tree.require_at(root)?;
    require_bootstrap_restore_complete(root)?;
    let result = (|| -> Result<(File, File)> {
        let mut ready = bootstrap_barrier_file(&root.join(BOOTSTRAP_READY))?;
        let mut bytes = Vec::with_capacity(7);
        (&mut ready).take(7).read_to_end(&mut bytes)?;
        if bytes != b"ready\n" {
            return Err(BicDbError::Replication(
                "bootstrap READY content is not the original complete marker".into(),
            ));
        }
        let mut node = bootstrap_barrier_file(&root.join(ha_node::HA_NODE_FILE))?;
        let state: ha_node::HaNodeFile = serde_json::from_reader(&mut node)?;
        if state.role != Some(ha_node::NodeRole::Standby)
            || !state.slots.is_empty()
            || state.switchover.is_some()
            || state.verification_error.is_some()
        {
            return Err(BicDbError::Replication(
                "bootstrap HA state is not the completed standby tail".into(),
            ));
        }
        // Payloads/MAC/native-copy authorization preceded the original tail.
        // Absence of pending permits only this fresh durability attempt; it is
        // not historical fsync success. Sync both tail files unconditionally,
        // including a prior fsync=false tail, and the actual directory entry.
        bicdb_page::test_faults::io_fault("bootstrap.ready.file_sync", root)?;
        node.sync_all()?;
        ready.sync_all()?;
        bicdb_page::test_faults::io_fault("bootstrap.ready.dir_sync", root)?;
        storage::sync_parent_directory(&root.join(BOOTSTRAP_READY))?;
        require_bootstrap_restore_complete(root)?;
        tree.require_at(root)?;
        Ok((ready, node))
    })();
    let (ready, node) = result.map_err(|error| BicDbError::WriteFailed {
        site: "bootstrap.ready.durability".into(),
        detail: format!("{}: {error}", root.display()),
    })?;
    Ok(BootstrapReadyDurability {
        _ownership: ownership,
        tree,
        _ready: ready,
        _node: node,
    })
}

/// The durable tail of an install: the parent directory `fsync` (the commit
/// point), then the marker and the old directory go.
fn complete_bootstrap_install(
    path: &Path,
    ready: &BootstrapReadyDurability<'_>,
    original_old_at_replaced: Option<&BootstrapTreeOwner>,
) -> Result<()> {
    ready.tree.require_at(path)?;
    let synced = bicdb_page::test_faults::io_fault(&format!("{BOOTSTRAP_SITE}.dir_sync"), path)
        .map_err(BicDbError::from)
        .and_then(|()| storage::sync_parent_directory(path));
    synced.map_err(|error| BicDbError::WriteFailed {
        site: format!("{BOOTSTRAP_SITE}.dir_sync"),
        detail: format!("{}: {error}", path.display()),
    })?;
    bicdb_page::test_faults::crash_point(&format!("{BOOTSTRAP_SITE}.after_dir_sync"));
    ready.tree.require_at(path)?;
    fs::remove_file(path.join(BOOTSTRAP_READY))?;
    storage::sync_parent_directory(&path.join(BOOTSTRAP_READY))?;
    if let Some(original_old) = original_old_at_replaced {
        let replaced = sibling_path(path, "replaced");
        original_old.require_at(&replaced)?;
        fs::remove_dir_all(&replaced)?;
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod physical_gate_tests;

#[cfg(all(test, unix))]
mod namespace_race_tests;

#[cfg(test)]
pub(super) fn test_after_initial_bootstrap_check(path: &Path, actual_installed: bool) {
    namespace_cut::observe(
        path,
        "after_core_bootstrap_check",
        serde_json::json!({
            "actual_initial_bootstrap_installed":actual_installed,
        }),
    );
}

#[cfg(all(test, unix))]
mod preimage_retry_tests;
