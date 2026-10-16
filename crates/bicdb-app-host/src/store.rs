use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::manifest::{valid_name, Release};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    pub application: String,
    pub release: String,
    pub previous: Option<String>,
    pub generation: u64,
    pub enabled: bool,
}

pub struct Store {
    pub root: PathBuf,
    _lock: File,
}

impl Store {
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        let root = root.canonicalize()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .open(root.join("platform.lock"))?;
            ensure!(
                unsafe {
                    libc::flock(
                        std::os::fd::AsRawFd::as_raw_fd(&lock),
                        libc::LOCK_EX | libc::LOCK_NB,
                    )
                } == 0,
                "platform directory is already owned by another supervisor"
            );
            fs::create_dir_all(root.join("apps"))?;
            File::open(&root)?.sync_all()?;
            Ok(Self { root, _lock: lock })
        }
        #[cfg(not(unix))]
        {
            anyhow::bail!("the application supervisor currently requires Unix");
        }
    }

    pub fn app_dir(&self, name: &str) -> Result<PathBuf> {
        ensure!(valid_name(name), "invalid application name");
        Ok(self.root.join("apps").join(name))
    }

    pub fn save_release(&self, release: &Release) -> Result<String> {
        release.validate()?;
        let digest = release.digest()?;
        let releases = self.app_dir(&release.manifest.name)?.join("releases");
        fs::create_dir_all(&releases)?;
        File::open(releases.parent().context("application directory")?)?.sync_all()?;
        File::open(self.root.join("apps"))?.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        let destination = releases.join(&digest);
        if destination.exists() {
            ensure!(
                self.load_release(&release.manifest.name, &digest)?
                    .digest()?
                    == digest,
                "stored release integrity failure"
            );
            return Ok(digest);
        }
        let staging = releases.join(format!(".stage-{}", Uuid::new_v4()));
        fs::create_dir(&staging)?;
        let result = (|| -> Result<()> {
            for name in release.files.keys() {
                let path = staging.join(name);
                fs::create_dir_all(path.parent().context("file parent")?)?;
                write_synced(&path, &release.decode(name)?)?;
            }
            write_synced(
                &staging.join(".release.json"),
                &serde_json::to_vec(release)?,
            )?;
            sync_tree(&staging)?;
            fs::rename(&staging, &destination)?;
            File::open(&releases)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_dir_all(staging);
        }
        result?;
        Ok(digest)
    }

    pub fn load_release(&self, name: &str, digest: &str) -> Result<Release> {
        ensure!(
            digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid release digest"
        );
        let path = self
            .app_dir(name)?
            .join("releases")
            .join(digest)
            .join(".release.json");
        let release: Release = serde_json::from_slice(&read_bounded(
            &path,
            crate::manifest::MAX_RELEASE_BYTES * 4 / 3 + 2 * 1024 * 1024,
        )?)?;
        release.validate()?;
        ensure!(
            release.manifest.name == name && release.digest()? == digest,
            "stored release integrity failure"
        );
        let directory = path.parent().context("release directory")?;
        for name in release.files.keys() {
            ensure!(
                read_bounded(&directory.join(name), crate::manifest::MAX_RELEASE_BYTES)?
                    == release.decode(name)?,
                "unpacked release file integrity failure"
            );
        }
        Ok(release)
    }

    pub fn deployments(&self) -> Result<Vec<Deployment>> {
        let mut result = Vec::new();
        for entry in fs::read_dir(self.root.join("apps"))? {
            let path = entry?.path().join("deployment.json");
            if path.exists() {
                let deployment: Deployment = serde_json::from_slice(&fs::read(path)?)?;
                self.app_dir(&deployment.application)?;
                result.push(deployment);
            }
        }
        result.sort_by(|a, b| a.application.cmp(&b.application));
        Ok(result)
    }

    pub fn save_deployment(&self, deployment: &Deployment) -> Result<()> {
        let dir = self.app_dir(&deployment.application)?;
        atomic_write(
            &dir.join("deployment.json"),
            &serde_json::to_vec_pretty(deployment)?,
        )
    }
}

pub fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::create_dir_all(path.parent().context("file parent")?)?;
    let temporary = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
    write_synced(&temporary, bytes)?;
    fs::rename(&temporary, path)?;
    File::open(path.parent().context("file parent")?)?.sync_all()?;
    Ok(())
}

fn sync_tree(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            sync_tree(&path)?;
        }
    }
    File::open(path)?.sync_all()?;
    Ok(())
}

pub fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let file = File::open(path)?;
    ensure!(
        file.metadata()?.len() <= limit as u64,
        "file exceeds size limit"
    );
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= limit, "file exceeds size limit");
    Ok(bytes)
}
