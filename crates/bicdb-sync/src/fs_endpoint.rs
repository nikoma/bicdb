use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use bicdb_core::{BicDbError, EncryptionConfig, NodeId, Result, SyncBundle};

use crate::{ClientSyncCheckpoint, PushBundleReport, SyncEndpoint};

#[derive(Clone, Debug)]
/// A directory of sync bundles — a shared folder, a USB stick, a hub
/// dropbox.
///
/// Bundles carry full record payloads, so anyone who can read the
/// directory reads the data. `SyncBundle` has supported authenticated
/// encryption since the mesh landed, but nothing ever called it: every
/// bundle this endpoint wrote was pretty-printed JSON in the clear.
/// Configure `encryption` and every bundle written here is sealed;
/// reading accepts both forms, so existing plaintext bundles still
/// import.
pub struct FileSyncEndpoint {
    root: PathBuf,
    fsync: bool,
    encryption: Option<EncryptionConfig>,
}

impl FileSyncEndpoint {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_fsync(root, false)
    }

    /// Open an endpoint that seals every bundle it writes.
    pub fn open_encrypted(
        root: impl AsRef<Path>,
        fsync: bool,
        encryption: EncryptionConfig,
    ) -> Result<Self> {
        let mut endpoint = Self::open_with_fsync(root, fsync)?;
        endpoint.encryption = Some(encryption);
        Ok(endpoint)
    }

    pub fn open_with_fsync(root: impl AsRef<Path>, fsync: bool) -> Result<Self> {
        let endpoint = Self {
            encryption: None,
            root: root.as_ref().to_path_buf(),
            fsync,
        };
        fs::create_dir_all(endpoint.bundles_dir())?;
        fs::create_dir_all(endpoint.checkpoints_dir())?;
        Ok(endpoint)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn bundles_dir(&self) -> PathBuf {
        self.root.join("bundles")
    }

    fn checkpoints_dir(&self) -> PathBuf {
        self.root.join("checkpoints")
    }

    fn source_bundles_dir(&self, source_node_id: &NodeId) -> PathBuf {
        self.bundles_dir().join(source_node_id.to_string())
    }

    fn bundle_path(&self, bundle: &SyncBundle) -> PathBuf {
        self.source_bundles_dir(&bundle.source_node_id)
            .join(format!(
                "{:020}-{}.syncbundle",
                bundle.next_checkpoint.event_offset, bundle.bundle_id
            ))
    }

    fn checkpoint_path(&self, node_id: &NodeId) -> PathBuf {
        self.checkpoints_dir().join(format!("{}.json", node_id))
    }

    fn bundle_paths(&self) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        let bundles_dir = self.bundles_dir();
        if !bundles_dir.exists() {
            return Ok(paths);
        }

        for source in fs::read_dir(bundles_dir)? {
            let source = source?;
            if !source.file_type()?.is_dir() {
                continue;
            }
            for entry in fs::read_dir(source.path())? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                if entry.path().extension().and_then(|ext| ext.to_str()) == Some("syncbundle") {
                    paths.push(entry.path());
                }
            }
        }
        Ok(paths)
    }
}

impl SyncEndpoint for FileSyncEndpoint {
    fn load_checkpoint(&self, client_node_id: &NodeId) -> Result<ClientSyncCheckpoint> {
        let path = self.checkpoint_path(client_node_id);
        if !path.exists() {
            return Ok(ClientSyncCheckpoint::default());
        }

        let bytes = fs::read(path)?;
        if bytes.is_empty() {
            return Ok(ClientSyncCheckpoint::default());
        }

        let checkpoint: ClientSyncCheckpoint = serde_json::from_slice(&bytes)?;
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    fn save_checkpoint(
        &mut self,
        client_node_id: &NodeId,
        checkpoint: &ClientSyncCheckpoint,
    ) -> Result<()> {
        checkpoint.validate()?;
        fs::create_dir_all(self.checkpoints_dir())?;
        write_json_atomic(
            &self.checkpoint_path(client_node_id),
            &serde_json::to_vec_pretty(checkpoint)?,
            self.fsync,
        )
    }

    fn push_bundle(&mut self, bundle: &SyncBundle) -> Result<PushBundleReport> {
        bundle.verify()?;
        let source_dir = self.source_bundles_dir(&bundle.source_node_id);
        fs::create_dir_all(source_dir)?;
        let path = self.bundle_path(bundle);

        if path.exists() {
            let existing = SyncBundle::read_auto(&path, self.encryption.clone())?;
            if existing.checksum != bundle.checksum {
                return Err(BicDbError::SyncBundle(format!(
                    "bundle path collision for {} with different checksum",
                    bundle.bundle_id
                )));
            }
            return Ok(PushBundleReport::from(bundle));
        }

        match &self.encryption {
            Some(encryption) => {
                bundle.write_encrypted_atomic(&path, self.fsync, encryption.clone())?
            }
            None => bundle.write_atomic(&path, self.fsync)?,
        }
        Ok(PushBundleReport::from(bundle))
    }

    fn pull_bundles(
        &self,
        client_node_id: &NodeId,
        checkpoint: &ClientSyncCheckpoint,
    ) -> Result<Vec<SyncBundle>> {
        checkpoint.validate()?;
        let mut bundles = Vec::new();
        for path in self.bundle_paths()? {
            let bundle = SyncBundle::read_auto(path, self.encryption.clone())?;
            if bundle.source_node_id == *client_node_id {
                continue;
            }

            let imported = checkpoint.remote_checkpoint(&bundle.source_node_id);
            if bundle.next_checkpoint.event_offset > imported.event_offset {
                bundles.push(bundle);
            }
        }

        bundles.sort_by(|left, right| {
            left.source_node_id
                .cmp(&right.source_node_id)
                .then_with(|| {
                    left.from_checkpoint
                        .event_offset
                        .cmp(&right.from_checkpoint.event_offset)
                })
                .then_with(|| {
                    left.next_checkpoint
                        .event_offset
                        .cmp(&right.next_checkpoint.event_offset)
                })
                .then_with(|| left.created_at.cmp(&right.created_at))
                .then_with(|| left.bundle_id.cmp(&right.bundle_id))
        });
        Ok(bundles)
    }
}

fn write_json_atomic(path: &Path, bytes: &[u8], fsync: bool) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        BicDbError::SyncBundle(format!(
            "checkpoint path {} has no parent directory",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent)?;

    let tmp = path.with_extension("json.tmp");
    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        if fsync {
            file.sync_all()?;
        }
    }
    fs::rename(tmp, path)?;
    Ok(())
}
