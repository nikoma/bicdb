//! Encrypted application recovery images. Restores are always inactive.
use std::fs::{self, File};
use std::path::Path;

use anyhow::{ensure, Context, Result};
use bicdb_core::{
    create_backup_from_open_database, restore_backup, BackupCreateOptions, BackupRestoreOptions,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    database::Database,
    manifest::{Release, MAX_RELEASE_BYTES},
    store::{atomic_write, read_bounded, Deployment, Store},
};

const METADATA: &str = "platform-recovery.json";
const JOBS: &str = "platform-recovery-jobs.bicbak";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryMetadata {
    format: u32,
    deployment: Deployment,
    release: Release,
    previous: Option<Release>,
}

pub fn snapshot(
    database: &mut Database,
    deployment: Deployment,
    release: Release,
    previous: Option<Release>,
    output: &Path,
    passphrase: &str,
) -> Result<()> {
    ensure!(!output.exists(), "backup output already exists");
    ensure!(
        deployment.release == release.digest()?,
        "deployment and release differ"
    );
    let directory = database.db.read().data_path().to_owned();
    let jobs = directory.join(JOBS);
    let metadata = directory.join(METADATA);
    // These are temporary backup payloads inside the data archive, not database
    // collections. Previous interrupted attempts cannot be treated as current.
    for path in [&jobs, &metadata] {
        if path.exists() {
            fs::remove_file(path)?;
        }
    }
    let result = (|| -> Result<()> {
        database.snapshot_jobs(&jobs, passphrase)?;
        atomic_write(
            &metadata,
            &serde_json::to_vec(&RecoveryMetadata {
                format: 2,
                deployment,
                release,
                previous,
            })?,
        )?;
        create_backup_from_open_database(
            &database.db.read(),
            output,
            BackupCreateOptions {
                passphrase: passphrase.into(),
                base_backup: None,
            },
        )?;
        Ok(())
    })();
    for path in [&jobs, &metadata] {
        let _ = fs::remove_file(path);
    }
    File::open(&directory)?.sync_all()?;
    result
}

pub fn restore(store: &Store, archive: &Path, passphrase: &str) -> Result<Deployment> {
    let staging = store.root.join(format!(".restore-{}", Uuid::new_v4()));
    fs::create_dir(&staging)?;
    let result = (|| -> Result<Deployment> {
        let data = staging.join("database/data");
        restore_backup(
            archive,
            &data,
            BackupRestoreOptions {
                passphrase: passphrase.into(),
                force: false,
            },
        )?;
        let metadata: RecoveryMetadata =
            serde_json::from_slice(&read_bounded(&data.join(METADATA), MAX_RELEASE_BYTES * 4)?)?;
        ensure!(
            matches!(metadata.format, 1 | 2),
            "unsupported application recovery format"
        );
        metadata.release.validate()?;
        ensure!(
            metadata.release.manifest.name == metadata.deployment.application
                && metadata.release.digest()? == metadata.deployment.release,
            "recovery release binding mismatch"
        );
        if let Some(previous) = &metadata.previous {
            previous.validate()?;
            ensure!(
                previous.manifest.name == metadata.deployment.application
                    && metadata.deployment.previous.as_deref() == Some(previous.digest()?.as_str()),
                "recovery predecessor binding mismatch"
            );
        } else {
            ensure!(
                metadata.deployment.previous.is_none(),
                "recovery predecessor missing"
            );
        }
        let target = store.app_dir(&metadata.deployment.application)?;
        ensure!(
            !target.exists(),
            "refusing to replace an existing application"
        );
        restore_backup(
            data.join(JOBS),
            staging.join("database/jobs"),
            BackupRestoreOptions {
                passphrase: passphrase.into(),
                force: false,
            },
        )?;
        fs::remove_file(data.join(JOBS))?;
        fs::remove_file(data.join(METADATA))?;
        // Validate the recovered data before it can enter the deployment catalog.
        let mut database = Database::open(&staging.join("database"))?;
        database.prepare(&metadata.release)?;
        drop(database);
        let mut deployment = metadata.deployment;
        deployment.enabled = false;
        // Assemble the complete inactive application under staging. Publishing
        // one directory rename prevents a crash leaving a half-restored catalog.
        let staged_store = Store::open(&staging.join("catalog"))?;
        staged_store.save_release(&metadata.release)?;
        if let Some(previous) = &metadata.previous {
            staged_store.save_release(previous)?;
        }
        let staged_app = staged_store.app_dir(&deployment.application)?;
        fs::rename(staging.join("database"), staged_app.join("database"))?;
        staged_store.save_deployment(&deployment)?;
        fs::rename(staged_app, &target)?;
        File::open(target.parent().context("application parent")?)?.sync_all()?;
        Ok(deployment)
    })();
    let _ = fs::remove_dir_all(staging);
    result
}
