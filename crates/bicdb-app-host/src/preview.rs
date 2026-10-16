//! Atomic, inactive application previews with fresh state and capabilities.
use crate::{
    database::Database,
    manifest::{valid_name, Release},
    store::{write_synced, Deployment, Store},
};
use anyhow::{ensure, Context, Result};
use serde_json::json;
use std::fs::{self, File};
use uuid::Uuid;

/// The caller must hold source application admission through this operation.
/// Only the completed inactive directory is published; the source is not modified.
pub fn create(
    store: &Store,
    source: &Database,
    release: &Release,
    name: &str,
) -> Result<Deployment> {
    ensure!(
        valid_name(name) && name != release.manifest.name,
        "invalid preview application name"
    );
    let destination = store.app_dir(name)?;
    ensure!(!destination.exists(), "preview application already exists");
    let staging = store.root.join(format!(".preview-{}", Uuid::new_v4()));
    fs::create_dir(&staging)?;
    let result = (|| -> Result<Deployment> {
        let staged = Store::open(&staging)?;
        let mut preview = release.clone();
        preview.manifest.name = name.into();
        preview.manifest.sync.clear();
        preview.validate()?;
        let digest = staged.save_release(&preview)?;
        let app = staged.app_dir(name)?;
        let report = source.materialize_preview(&app.join("database"), release)?;
        let deployment = Deployment {
            application: name.into(),
            release: digest,
            previous: None,
            generation: 1,
            enabled: false,
        };
        staged.save_deployment(&deployment)?;
        write_synced(
            &app.join("preview.json"),
            &serde_json::to_vec(&json!({
                "format":1, "source":release.manifest.name, "source_release":release.digest()?,
                "copy":report
            }))?,
        )?;
        File::open(&app)?.sync_all()?;
        fs::rename(&app, &destination)?;
        File::open(destination.parent().context("preview parent")?)?.sync_all()?;
        Ok(deployment)
    })();
    let cleanup = fs::remove_dir_all(&staging);
    if result.is_ok() {
        cleanup?;
    }
    result
}
