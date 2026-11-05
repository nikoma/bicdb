//! Private operator configuration is separate from content-addressed releases.
use crate::manifest::{Isolation, Release, Runtime};
use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    process::Command,
};

const FILE: &str = "runtime-private.json";
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    format: u32,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    pub identity: Option<Identity>,
    #[serde(default = "startup_timeout")]
    pub startup_timeout_ms: u64,
}
fn startup_timeout() -> u64 {
    10_000
}
impl Default for Config {
    fn default() -> Self {
        Self {
            format: 0,
            environment: BTreeMap::new(),
            identity: None,
            startup_timeout_ms: startup_timeout(),
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    uid: u32,
    gid: u32,
    execution_root: PathBuf,
}

fn private_file(path: &Path) -> Result<Option<File>> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("open private application configuration"),
    };
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.permissions().mode() & 0o077 == 0,
        "application runtime configuration must be a private regular file owned by the supervisor"
    );
    Ok(Some(file))
}
impl Config {
    pub fn read(app_dir: &Path) -> Result<Self> {
        let Some(file) = private_file(&app_dir.join(FILE))? else {
            return Ok(Self::default());
        };
        let mut bytes = Vec::new();
        file.take(128 * 1024 + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() <= 128 * 1024,
            "application runtime configuration exceeds its size limit"
        );
        let config: Self = serde_json::from_slice(&bytes).map_err(|error| {
            anyhow::anyhow!(
                "invalid application runtime configuration at line {} column {}",
                error.line(),
                error.column()
            )
        })?;
        ensure!(
            config.format == 1,
            "unsupported application runtime configuration"
        );
        ensure!(
            config.environment.len() <= 128,
            "too many application environment bindings"
        );
        let mut size = 0usize;
        for (name, value) in &config.environment {
            ensure!(
                !name.is_empty()
                    && name.len() <= 128
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
                    && !name.as_bytes()[0].is_ascii_digit(),
                "invalid application environment name"
            );
            ensure!(
                !name.starts_with("BICDB_")
                    && (!name.starts_with("NODE_") || name == "NODE_EXTRA_CA_CERTS")
                    && !matches!(
                        name.as_str(),
                        "PATH" | "PORT" | "HOST" | "HOSTNAME" | "HOME" | "TMPDIR"
                    ),
                "reserved application environment binding"
            );
            ensure!(
                !value.contains('\0') && value.len() <= 16384,
                "invalid application environment value"
            );
            size += name.len() + value.len();
        }
        ensure!(
            size <= 64 * 1024,
            "application environment bindings exceed their size limit"
        );
        ensure!(
            (1000..=300_000).contains(&config.startup_timeout_ms),
            "invalid application startup deadline"
        );
        if let Some(identity) = &config.identity {
            ensure!(
                cfg!(target_os = "linux") && unsafe { libc::geteuid() } == 0,
                "isolated process credentials require a Linux root supervisor"
            );
            ensure!(
                identity.uid >= 10000 && identity.gid >= 10000,
                "application UID and GID must be dedicated unprivileged identities"
            );
            ensure!(
                identity.execution_root.is_absolute()
                    && identity.execution_root.components().count() >= 4,
                "application execution root must be a dedicated absolute directory"
            );
        }
        Ok(config)
    }
    pub fn validate_release(&self, release: &Release) -> Result<()> {
        if !self.environment.is_empty()
            || self.identity.is_some()
            || self.startup_timeout_ms != startup_timeout()
        {
            ensure!(
                release.manifest.placement.is_none()
                    && release.manifest.isolation == Isolation::Process,
                "private runtime configuration requires local process execution"
            );
        }
        if self.identity.is_some() {
            ensure!(
                release.manifest.runtime == Runtime::NodeHttp,
                "isolated credentials currently require node_http"
            );
        }
        Ok(())
    }
    pub fn prepare(&self, release: &Release, directory: &Path, app_dir: &Path) -> Result<Prepared> {
        self.validate_release(release)?;
        let Some(identity) = &self.identity else {
            return Ok(Prepared {
                directory: directory.to_owned(),
                work: None,
                identity: None,
            });
        };
        use std::os::unix::fs::PermissionsExt;
        release.validate()?;
        secure_execution_root(&identity.execution_root)?;
        let root = identity.execution_root.canonicalize()?;
        ensure!(
            !root.starts_with(app_dir.canonicalize()?),
            "execution files must remain outside private application state"
        );
        let releases = root.join("releases");
        safe_directory(&releases, 0o755)?;
        let destination = releases.join(release.digest()?);
        if !destination.exists() {
            let staging = releases.join(format!("stage-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&staging)?;
            let result = (|| -> Result<()> {
                for name in release.files.keys() {
                    let path = staging.join(name);
                    fs::create_dir_all(path.parent().context("release parent")?)?;
                    let mut file = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&path)?;
                    std::io::Write::write_all(&mut file, &release.decode(name)?)?;
                    file.sync_all()?;
                    fs::set_permissions(&path, fs::Permissions::from_mode(0o444))?;
                }
                protect_directories(&staging)?;
                fs::rename(&staging, &destination)?;
                File::open(&releases)?.sync_all()?;
                Ok(())
            })();
            if result.is_err() {
                let _ = fs::remove_dir_all(&staging);
            }
            result?;
        }
        let mut actual = Vec::new();
        verify_files(&destination, &destination, &mut actual)?;
        actual.sort();
        ensure!(
            actual == release.files.keys().cloned().collect::<Vec<_>>(),
            "execution release file set changed"
        );
        for name in &actual {
            ensure!(
                fs::read(destination.join(name))? == release.decode(name)?,
                "execution release integrity failure"
            );
        }
        let work_root = root.join("work");
        safe_directory(&work_root, 0o711)?;
        let work = work_root.join(uuid::Uuid::new_v4().to_string());
        fs::create_dir(&work)?;
        fs::set_permissions(&work, fs::Permissions::from_mode(0o700))?;
        let c_path = std::ffi::CString::new(work.as_os_str().as_encoded_bytes())?;
        ensure!(
            unsafe { libc::chown(c_path.as_ptr(), identity.uid, identity.gid) } == 0,
            "cannot assign application work directory"
        );
        Ok(Prepared {
            directory: destination,
            work: Some(work),
            identity: Some(identity.clone()),
        })
    }
    pub fn configure(&self, command: &mut Command, prepared: &Prepared) -> Result<()> {
        command.envs(&self.environment);
        if let Some(identity) = &prepared.identity {
            #[cfg(not(target_os = "linux"))]
            anyhow::bail!("isolated process credentials require Linux");
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::process::CommandExt;
                let identity = identity.clone();
                command
                    .env("PATH", "/usr/local/bin:/usr/bin:/bin")
                    .env("HOME", prepared.work.as_ref().unwrap())
                    .env("TMPDIR", prepared.work.as_ref().unwrap());
                unsafe {
                    command.pre_exec(move || {
                        if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0
                            || libc::setgroups(0, std::ptr::null()) != 0
                            || libc::setgid(identity.gid) != 0
                            || libc::setuid(identity.uid) != 0
                        {
                            return Err(std::io::Error::last_os_error());
                        }
                        Ok(())
                    });
                }
            }
        }
        Ok(())
    }
}

fn secure_execution_root(path: &Path) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    for ancestor in path.ancestors() {
        let meta = fs::symlink_metadata(ancestor)?;
        let mode = meta.permissions().mode();
        ensure!(meta.is_dir() && !meta.file_type().is_symlink()
            && meta.uid()==unsafe{libc::geteuid()}
            && (mode&0o022==0 || mode&0o1000!=0) && mode&1!=0,
            "execution root and ancestors must be traversable supervisor-owned directories with protected names");
    }
    Ok(())
}
fn safe_directory(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
    match fs::DirBuilder::new().mode(mode).create(path) {
        Ok(()) => {
            let directory = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(path)?;
            directory.set_permissions(fs::Permissions::from_mode(mode))?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        meta.is_dir()
            && !meta.file_type().is_symlink()
            && meta.uid() == unsafe { libc::geteuid() }
            && meta.permissions().mode() & 0o022 == 0
            && meta.permissions().mode() & 1 != 0,
        "execution directory must be supervisor-owned, traversable and not writable by other users"
    );
    Ok(())
}
fn protect_directories(root: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            protect_directories(&path)?;
        }
    }
    fs::set_permissions(root, fs::Permissions::from_mode(0o555))?;
    File::open(root)?.sync_all()?;
    Ok(())
}
fn verify_files(root: &Path, path: &Path, files: &mut Vec<String>) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        !meta.file_type().is_symlink()
            && meta.uid() == unsafe { libc::geteuid() }
            && meta.permissions().mode() & 0o022 == 0,
        "unsafe execution release path"
    );
    if meta.is_dir() {
        for entry in fs::read_dir(path)? {
            verify_files(root, &entry?.path(), files)?;
        }
    } else {
        ensure!(
            meta.is_file() && meta.nlink() == 1,
            "execution release must contain ordinary files"
        );
        files.push(
            path.strip_prefix(root)?
                .to_str()
                .context("non UTF-8 execution path")?
                .to_owned(),
        );
    }
    Ok(())
}
pub struct Prepared {
    pub directory: PathBuf,
    pub work: Option<PathBuf>,
    identity: Option<Identity>,
}
impl Drop for Prepared {
    fn drop(&mut self) {
        if let Some(work) = self.work.take() {
            let _ = fs::remove_dir_all(work);
        }
    }
}
