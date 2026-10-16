use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use anyhow::{bail, ensure, Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_RELEASE_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_FILES: usize = 4096;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub format: u32,
    pub name: String,
    pub version: String,
    pub runtime: Runtime,
    #[serde(default, skip_serializing_if = "Isolation::is_process")]
    pub isolation: Isolation,
    pub entrypoint: String,
    pub files: Vec<String>,
    #[serde(default)]
    pub migrations: Vec<String>,
    #[serde(default)]
    pub resources: Resources,
    #[serde(default)]
    pub sync: BTreeMap<String, TableSync>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Isolation {
    #[default]
    Process,
    Microvm,
}

impl Isolation {
    fn is_process(&self) -> bool {
        *self == Self::Process
    }
}

/// Public-table synchronization policy. Cross-table constraints remain local.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum TableSync {
    /// Any authorized peer may insert new records. Updates and deletes are refused.
    AppendOnly,
    /// Only this stable database node may author changes; others are read replicas.
    SingleWriter { node: bicdb_core::NodeId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Runtime {
    /// Trusted JavaScript application using the platform fetch/queue adapter.
    Node,
    /// Trusted application that starts its own HTTP server on PORT.
    NodeHttp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub memory_mb: u32,
    pub request_timeout_ms: u64,
    pub max_concurrent_requests: usize,
}

impl Default for Resources {
    fn default() -> Self {
        Self {
            memory_mb: 128,
            request_timeout_ms: 30_000,
            max_concurrent_requests: 64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Release {
    pub manifest: Manifest,
    /// Explicit file set, base64 encoded. No installation or build hooks run.
    pub files: BTreeMap<String, String>,
}

pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name.as_bytes()[name.len() - 1].is_ascii_alphanumeric()
}

pub fn safe_relative(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty() && path.len() <= 1024 && !path.contains('\\'),
        "invalid release path"
    );
    ensure!(
        Path::new(path)
            .components()
            .all(|p| matches!(p, Component::Normal(_))),
        "release paths must be relative without traversal"
    );
    // These paths belong to the platform, never to a package.
    ensure!(
        !path.split('/').any(|part| part.starts_with('.')),
        "hidden release paths are reserved"
    );
    Ok(())
}

impl Release {
    pub fn read(path: &Path) -> Result<Self> {
        let bytes =
            crate::store::read_bounded(path, 64 * 1024).context("read application manifest")?;
        ensure!(bytes.len() <= 64 * 1024, "manifest too large");
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        let root = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .canonicalize()?;
        let mut files = BTreeMap::new();
        let mut total = 0usize;
        ensure!(manifest.files.len() <= MAX_FILES, "too many release files");
        for name in &manifest.files {
            safe_relative(name)?;
            let mut current = root.clone();
            for part in Path::new(name).components() {
                current.push(part);
                ensure!(
                    !std::fs::symlink_metadata(&current)?
                        .file_type()
                        .is_symlink(),
                    "release symlinks are unsupported"
                );
            }
            let data =
                crate::store::read_bounded(&current, MAX_RELEASE_BYTES.saturating_sub(total))
                    .with_context(|| format!("read release file {name}"))?;
            ensure!(data.len() <= MAX_RELEASE_BYTES, "release file too large");
            total += data.len();
            files.insert(
                name.clone(),
                base64::engine::general_purpose::STANDARD.encode(data),
            );
        }
        let release = Self { manifest, files };
        release.validate()?;
        Ok(release)
    }

    pub fn validate(&self) -> Result<()> {
        let m = &self.manifest;
        ensure!(m.format == 1, "unsupported application format");
        ensure!(valid_name(&m.name), "invalid application name");
        ensure!(
            !m.version.is_empty()
                && m.version.len() <= 128
                && !m.version.chars().any(char::is_control),
            "invalid application version"
        );
        ensure!(
            !m.files.is_empty() && m.files.len() <= MAX_FILES,
            "invalid release file count"
        );
        let names: BTreeSet<_> = m.files.iter().collect();
        ensure!(
            names.len() == m.files.len() && names.len() == self.files.len(),
            "duplicate or undeclared release files"
        );
        ensure!(
            names.contains(&m.entrypoint),
            "entrypoint must be a declared file"
        );
        let mut bytes = 0usize;
        for (name, encoded) in &self.files {
            safe_relative(name)?;
            ensure!(names.contains(name), "undeclared release file");
            bytes = bytes
                .checked_add(encoded.len())
                .context("release size overflow")?;
            ensure!(
                bytes <= MAX_RELEASE_BYTES * 4 / 3 + MAX_FILES * 4,
                "release too large"
            );
            let data = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .context("invalid file encoding")?;
            ensure!(data.len() <= MAX_RELEASE_BYTES, "release file too large");
        }
        ensure!(
            m.migrations.len() <= 1024
                && m.migrations.iter().collect::<BTreeSet<_>>().len() == m.migrations.len(),
            "invalid migrations"
        );
        for migration in &m.migrations {
            ensure!(
                names.contains(migration),
                "migration must be a declared file"
            );
            ensure!(
                self.decode(migration)?.len() <= 1024 * 1024,
                "migration too large"
            );
            std::str::from_utf8(&self.decode(migration)?)?;
        }
        ensure!(
            (16..=16384).contains(&m.resources.memory_mb),
            "memory_mb outside supported range"
        );
        ensure!(
            (100..=300_000).contains(&m.resources.request_timeout_ms),
            "request timeout outside supported range"
        );
        ensure!(
            (1..=4096).contains(&m.resources.max_concurrent_requests),
            "concurrency outside supported range"
        );
        if m.isolation == Isolation::Microvm {
            ensure!(
                m.resources.memory_mb >= 128,
                "microVM memory_mb must be at least 128"
            );
        }
        for table in m.sync.keys() {
            ensure!(
                table.len() <= 63
                    && table
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
                    && table.as_bytes().first().is_some_and(u8::is_ascii_lowercase),
                "sync tables must be lowercase public table names"
            );
        }
        if bytes == 0 {
            bail!("empty release");
        }
        Ok(())
    }

    pub fn decode(&self, name: &str) -> Result<Vec<u8>> {
        Ok(base64::engine::general_purpose::STANDARD
            .decode(self.files.get(name).context("missing release file")?)?)
    }

    pub fn digest(&self) -> Result<String> {
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }
}
