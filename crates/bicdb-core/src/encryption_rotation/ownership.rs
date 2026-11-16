//! Closed filesystem custody only; no engine/replication permission is minted.
use super::*;

pub(super) struct RotationTreeOwner {
    physical: Option<storage::BackupGateGuard>,
    directory: bicdb_page::DirectoryLock,
}

impl RotationTreeOwner {
    pub(super) fn existing(root: &Path, label: &str) -> Result<Self> {
        require_actual_directory(root)?;
        let directory = bicdb_page::DirectoryLock::acquire(root).map_err(|error| {
            BicDbError::EncryptionKeyInvalid(format!(
                "{label} must remain closed during key rotation: {error}"
            ))
        })?;
        directory
            .require_directory(root)
            .map_err(|error| BicDbError::EncryptionKeyInvalid(error.to_string()))?;
        let mut owner = Self {
            directory,
            physical: None,
        };
        owner.enter_physical(root)?;
        Ok(owner)
    }

    pub(super) fn fresh(
        root: &Path,
        created: &bicdb_page::DirectoryHandleObservation,
    ) -> Result<Self> {
        require_actual_directory(root)?;
        let directory = bicdb_page::DirectoryLock::acquire(root).map_err(|error| {
            BicDbError::EncryptionKeyInvalid(format!(
                "fresh rotation staging directory was claimed by another owner: {error}"
            ))
        })?;
        directory
            .require_directory_observation(root, created)
            .map_err(|error| BicDbError::EncryptionKeyInvalid(error.to_string()))?;
        // A normal Core owner can win and finish between mkdir and our claim.
        // Its closed, populated tree is still foreign: never copy or clean it.
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if entry.file_name() != "store.lock"
                || !metadata.is_file()
                || metadata.file_type().is_symlink()
            {
                return Err(BicDbError::EncryptionKeyInvalid(
                    "fresh rotation staging directory was populated by another owner".into(),
                ));
            }
        }
        let mut owner = Self {
            directory,
            physical: None,
        };
        // This SAME successful fresh allocation/claim authorizes a NEW inode,
        // before source FORMAT is copied. Existing admission stays strict.
        owner.enter_physical(root)?;
        Ok(owner)
    }

    pub(super) fn require_at(&self, root: &Path) -> Result<()> {
        self.directory
            .require_retained_directory_at(root)
            .map_err(|error| BicDbError::EncryptionKeyInvalid(error.to_string()))
    }

    pub(super) fn enter_physical(&mut self, root: &Path) -> Result<()> {
        self.require_at(root)?;
        if self.physical.is_none() {
            // An enrolled existing tree with a missing gate is rejected by
            // this unchanged initializer; no retry reconstructs its inode.
            self.physical = Some(storage::acquire_database_backup_exclusive_guard(root)?);
        }
        Ok(())
    }

    pub(super) fn borrow(&self) -> Result<bicdb_page::physical_gate::PhysicalBackupBorrow<'_>> {
        self.physical
            .as_ref()
            .ok_or_else(|| {
                BicDbError::EncryptionKeyInvalid(
                    "rotation has no actual physical scope at this namespace boundary".into(),
                )
            })?
            .borrow()
    }

    pub(super) fn release_physical(&mut self) {
        self.physical = None;
    }

    pub(super) fn cleanup_fresh(&self, root: &Path) -> Result<()> {
        self.require_at(root)?;
        fs::remove_dir_all(root)?;
        Ok(())
    }
}

fn require_actual_directory(root: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(root)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(BicDbError::EncryptionKeyInvalid(
            "rotation owner requires an actual non-symlink directory".into(),
        ));
    }
    Ok(())
}
