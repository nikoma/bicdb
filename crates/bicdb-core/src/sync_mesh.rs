use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::encryption::{self, EncryptedBlob, EncryptionConfig};
use crate::error::{BicDbError, Result};
use crate::event::{Event, StoredEvent};
use crate::storage;

pub const DEFAULT_SYNC_STATE: &str = "sync_state.json";
pub const SYNC_BUNDLE_VERSION: u32 = 2;
pub const ENCRYPTED_SYNC_BUNDLE_VERSION: u32 = 2;
pub const SYNC_METADATA_KEY: &str = "_bicdb_sync";
const ENCRYPTED_SYNC_BUNDLE_KIND: &str = "bicdb.encrypted_sync_bundle";
const LEGACY_SYNC_BUNDLE_AAD: &[u8] = b"bicdb.syncbundle.v1";
const MAX_SYNC_BUNDLE_FILE_BYTES: u64 = 256 * 1024 * 1024;

pub type StreamId = String;
pub type EventId = Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub Uuid);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl FromStr for NodeId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(value)?))
    }
}

fn read_bundle_file(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > MAX_SYNC_BUNDLE_FILE_BYTES
    {
        return Err(BicDbError::SyncBundle(
            "sync bundle path is unsafe or exceeds the 256 MiB file bound".to_string(),
        ));
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    let file = options.open(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() > MAX_SYNC_BUNDLE_FILE_BYTES {
        return Err(BicDbError::SyncBundle(
            "sync bundle changed to an unsafe file".to_string(),
        ));
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    file.take(MAX_SYNC_BUNDLE_FILE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != opened.len() {
        return Err(BicDbError::SyncBundle(
            "sync bundle changed while it was read".to_string(),
        ));
    }
    Ok(bytes)
}

fn encrypted_bundle_aad(version: u32, kind: &str, created_at: i64) -> Vec<u8> {
    if version == 1 {
        return LEGACY_SYNC_BUNDLE_AAD.to_vec();
    }
    format!("bicdb.syncbundle.v2\0{version}\0{kind}\0{created_at}").into_bytes()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventEnvelope {
    pub node_id: NodeId,
    pub event_id: EventId,
    pub stream_id: StreamId,
    /// The event's **origin position**: a durable monotonic position in the
    /// authoring node's event log — NOT an event count. In practice this is
    /// a byte offset, so values like `38_194_722` next to another origin's
    /// `844_193` are normal, not corruption. Positions are strictly
    /// increasing in authorship order per origin and survive relays
    /// verbatim; that monotonicity is all the vector mathematics relies on.
    /// (The wire name stays `sequence` for compatibility.)
    pub sequence: u64,
    pub timestamp: i64,
    pub payload_hash: [u8; 32],
    /// Hex ed25519 signature by the ORIGIN node over
    /// [`envelope_signing_message`]. Survives relays verbatim (carried in
    /// `_bicdb_sync` metadata), so a receiver verifies "this really
    /// originated on that node and has not changed" without trusting any of
    /// the nodes that transported the bytes — the transporter/reader/author
    /// role separation. Bundle format v2 also covers this field in its
    /// checksum so stripping a signature is always detectable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// Canonical bytes an origin signs: domain tag, complete envelope, event
/// type/timestamp, user metadata, and the payload hash. Transport metadata
/// (`_bicdb_sync`) is excluded so the same origin signature survives relays.
pub fn envelope_signing_message(entry: &SyncBundleEvent) -> Vec<u8> {
    let envelope = &entry.envelope;
    let mut message = Vec::with_capacity(160 + envelope.stream_id.len());
    message.extend_from_slice(b"bicdb.mesh.envelope.v2");
    message.extend_from_slice(envelope.node_id.0.as_bytes());
    message.extend_from_slice(envelope.event_id.as_bytes());
    message.extend_from_slice(&(envelope.stream_id.len() as u64).to_be_bytes());
    message.extend_from_slice(envelope.stream_id.as_bytes());
    message.extend_from_slice(&envelope.sequence.to_be_bytes());
    message.extend_from_slice(&envelope.timestamp.to_be_bytes());
    message.extend_from_slice(&envelope.payload_hash);
    hash_str_into_bytes(&mut message, &entry.event.event_type);
    message.extend_from_slice(&entry.event.timestamp.to_be_bytes());
    let mut metadata_hasher = Sha256::new();
    stable_hash_user_metadata(&mut metadata_hasher, &entry.event.metadata);
    message.extend_from_slice(&metadata_hasher.finalize());
    message
}

fn hash_str_into_bytes(output: &mut Vec<u8>, value: &str) {
    output.extend_from_slice(&(value.len() as u64).to_be_bytes());
    output.extend_from_slice(value.as_bytes());
}

fn stable_hash_user_metadata(hasher: &mut Sha256, metadata: &Value) {
    if let Value::Object(fields) = metadata {
        if fields.contains_key(SYNC_METADATA_KEY) {
            if let Some(original) = fields.get("_bicdb_user_metadata") {
                stable_hash_json(hasher, original);
                return;
            }
            hasher.update(b"{");
            let mut entries = fields
                .iter()
                .filter(|(key, _)| key.as_str() != SYNC_METADATA_KEY)
                .collect::<Vec<_>>();
            hash_u64(hasher, entries.len() as u64);
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (key, value) in entries {
                hash_str(hasher, key);
                stable_hash_json(hasher, value);
            }
            return;
        }
    }
    stable_hash_json(hasher, metadata);
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SyncBundleEvent {
    pub envelope: EventEnvelope,
    pub event: Event,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncCheckpoint {
    pub event_offset: u64,
}

impl SyncCheckpoint {
    pub fn new(event_offset: u64) -> Self {
        Self { event_offset }
    }
}

/// Per-origin replication watermarks: for each authoring node, the highest
/// **origin position** (see [`EventEnvelope::sequence`] — a durable
/// monotonic log position, not an event count) this database holds.
/// `covers(origin, position)` relies on the prefix property — every export
/// streams a node's events in origin-position order and every import
/// appends in bundle order, so holdings per origin are always a gapless
/// prefix of that origin's authored (exportable) events. Build vectors only
/// from [`crate::BicDb::sync_vector`]; a hand-built vector that claims
/// positions the node does not hold would create silent holes.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncVector {
    pub origins: BTreeMap<String, u64>,
    /// Rolling digest of the exact gapless event prefix claimed for each
    /// origin. Equal watermarks with unequal digests are proof of a dropped,
    /// substituted, or reordered event rather than convergence.
    #[serde(default)]
    pub digests: BTreeMap<String, String>,
}

impl SyncVector {
    pub fn observe(&mut self, node: &NodeId, sequence: u64) {
        let entry = self.origins.entry(node.to_string()).or_default();
        if sequence > *entry {
            *entry = sequence;
        }
    }

    pub fn observe_envelope(&mut self, envelope: &EventEnvelope) {
        let origin = envelope.node_id.to_string();
        if self
            .origins
            .get(&origin)
            .is_some_and(|watermark| *watermark >= envelope.sequence)
        {
            return;
        }
        let previous = self
            .digests
            .get(&origin)
            .and_then(|digest| hex::decode(digest).ok())
            .unwrap_or_else(|| vec![0u8; 32]);
        let mut hasher = Sha256::new();
        hasher.update(b"bicdb.mesh.coverage-chain.v1");
        hasher.update(&previous);
        hasher.update(envelope.node_id.0.as_bytes());
        hasher.update(envelope.event_id.as_bytes());
        hash_str(&mut hasher, &envelope.stream_id);
        hash_u64(&mut hasher, envelope.sequence);
        hash_i64(&mut hasher, envelope.timestamp);
        hasher.update(envelope.payload_hash);
        match &envelope.signature {
            Some(signature) => {
                hasher.update([1]);
                hash_str(&mut hasher, signature);
            }
            None => hasher.update([0]),
        }
        self.origins.insert(origin.clone(), envelope.sequence);
        self.digests.insert(origin, hex::encode(hasher.finalize()));
    }

    /// True when this vector already holds `sequence` from `node`. Sequence 0
    /// is a valid origin offset, so "no entry" and "entry 0" differ: an entry
    /// exists only once at least one event from that origin was observed.
    pub fn covers(&self, node: &NodeId, sequence: u64) -> bool {
        self.origins
            .get(&node.to_string())
            .is_some_and(|watermark| *watermark >= sequence)
    }

    pub fn merge(&mut self, other: &SyncVector) {
        for (origin, sequence) in &other.origins {
            let entry = self.origins.entry(origin.clone()).or_default();
            if *sequence > *entry {
                *entry = *sequence;
                if let Some(digest) = other.digests.get(origin) {
                    self.digests.insert(origin.clone(), digest.clone());
                }
            }
        }
    }

    pub fn ensure_matching_equal_prefixes(&self, other: &SyncVector) -> Result<()> {
        for (origin, watermark) in &self.origins {
            if other.origins.get(origin) != Some(watermark) {
                continue;
            }
            if let (Some(left), Some(right)) = (self.digests.get(origin), other.digests.get(origin))
            {
                if left != right {
                    return Err(BicDbError::SyncBundle(format!(
                        "coverage proof mismatch for origin {origin} at position {watermark}"
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn watermark(&self, node: &NodeId) -> Option<u64> {
        self.origins.get(&node.to_string()).copied()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SyncBundle {
    pub version: u32,
    pub bundle_id: Uuid,
    pub source_node_id: NodeId,
    pub created_at: i64,
    pub from_checkpoint: SyncCheckpoint,
    pub next_checkpoint: SyncCheckpoint,
    pub event_count: usize,
    pub events: Vec<SyncBundleEvent>,
    pub checksum: String,
    /// The exporter's own [`SyncVector`] at export time, so a receiver learns
    /// the sender's coverage without another round trip. Advisory until frame
    /// signing lands: deliberately excluded from the v1 checksum so bundles
    /// written by newer binaries still verify on older ones (and vice versa).
    /// Per-event integrity stays covered by the envelope payload hashes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_vector: Option<SyncVector>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct EncryptedSyncBundle {
    version: u32,
    kind: String,
    created_at: i64,
    payload: EncryptedBlob,
}

impl SyncBundle {
    pub fn new(
        source_node_id: NodeId,
        from_checkpoint: SyncCheckpoint,
        next_checkpoint: SyncCheckpoint,
        events: Vec<SyncBundleEvent>,
    ) -> Result<Self> {
        let mut events: Vec<SyncBundleEvent> =
            serde_json::from_slice(&serde_json::to_vec(&events)?)?;
        for entry in &mut events {
            entry.envelope.payload_hash = payload_hash(&entry.event)?;
        }
        let mut bundle = Self {
            version: SYNC_BUNDLE_VERSION,
            bundle_id: Uuid::new_v4(),
            source_node_id,
            created_at: unix_timestamp(),
            from_checkpoint,
            next_checkpoint,
            event_count: events.len(),
            events,
            checksum: String::new(),
            source_vector: None,
        };
        bundle.checksum = bundle.calculate_checksum()?;
        Ok(bundle)
    }

    /// Attaches the exporter's coverage vector. Safe after construction: the
    /// vector is advisory metadata outside the v1 checksum, so this never
    /// invalidates `verify()`.
    pub fn with_source_vector(mut self, vector: SyncVector) -> Self {
        self.source_vector = Some(vector);
        self
    }

    pub fn read(path: impl AsRef<Path>) -> Result<Self> {
        let bytes = read_bundle_file(path.as_ref())?;
        let bundle: Self = serde_json::from_slice(&bytes)?;
        bundle.verify()?;
        Ok(bundle)
    }

    pub fn read_auto(
        path: impl AsRef<Path>,
        encryption_config: Option<EncryptionConfig>,
    ) -> Result<Self> {
        let path = path.as_ref();
        let bytes = read_bundle_file(path)?;
        if is_encrypted_bundle(&bytes)? {
            let Some(encryption_config) = encryption_config else {
                return Err(BicDbError::EncryptionKeyRequired);
            };
            return Self::read_encrypted_bytes(path, &bytes, encryption_config);
        }
        let bundle: Self = serde_json::from_slice(&bytes)?;
        bundle.verify()?;
        Ok(bundle)
    }

    pub fn read_encrypted(
        path: impl AsRef<Path>,
        encryption_config: EncryptionConfig,
    ) -> Result<Self> {
        let path = path.as_ref();
        let bytes = read_bundle_file(path)?;
        Self::read_encrypted_bytes(path, &bytes, encryption_config)
    }

    fn read_encrypted_bytes(
        path: &Path,
        bytes: &[u8],
        encryption_config: EncryptionConfig,
    ) -> Result<Self> {
        let encrypted: EncryptedSyncBundle = serde_json::from_slice(&bytes)?;
        if !matches!(encrypted.version, 1 | ENCRYPTED_SYNC_BUNDLE_VERSION) {
            return Err(BicDbError::SyncBundle(format!(
                "unsupported encrypted sync bundle version {}",
                encrypted.version
            )));
        }
        if encrypted.kind != ENCRYPTED_SYNC_BUNDLE_KIND {
            return Err(BicDbError::SyncBundle(format!(
                "unsupported encrypted sync bundle kind {}",
                encrypted.kind
            )));
        }
        let aad = encrypted_bundle_aad(encrypted.version, &encrypted.kind, encrypted.created_at);
        let plaintext =
            encryption::decrypt_blob(path, &encrypted.payload, &encryption_config, &aad)?;
        let bundle: Self = serde_json::from_slice(&plaintext)?;
        bundle.verify()?;
        Ok(bundle)
    }

    pub fn write_atomic(&self, path: impl AsRef<Path>, fsync: bool) -> Result<()> {
        self.verify()?;
        let bytes = serde_json::to_vec_pretty(self)?;
        storage::write_atomic(path.as_ref(), &bytes, fsync)
    }

    pub fn write_encrypted_atomic(
        &self,
        path: impl AsRef<Path>,
        fsync: bool,
        encryption_config: EncryptionConfig,
    ) -> Result<()> {
        self.verify()?;
        let plaintext = serde_json::to_vec(self)?;
        let created_at = unix_timestamp();
        let kind = ENCRYPTED_SYNC_BUNDLE_KIND.to_string();
        let aad = encrypted_bundle_aad(ENCRYPTED_SYNC_BUNDLE_VERSION, &kind, created_at);
        let encrypted = EncryptedSyncBundle {
            version: ENCRYPTED_SYNC_BUNDLE_VERSION,
            kind,
            created_at,
            payload: encryption::encrypt_blob(&plaintext, &encryption_config, &aad)?,
        };
        let bytes = serde_json::to_vec_pretty(&encrypted)?;
        storage::write_atomic(path.as_ref(), &bytes, fsync)
    }

    pub fn verify(&self) -> Result<()> {
        if self.version != SYNC_BUNDLE_VERSION {
            return Err(BicDbError::SyncBundle(format!(
                "unsupported sync bundle version {}",
                self.version
            )));
        }
        if self.event_count != self.events.len() {
            return Err(BicDbError::SyncBundle(format!(
                "event_count {} does not match {} events",
                self.event_count,
                self.events.len()
            )));
        }
        let checksum = self.calculate_checksum()?;
        if checksum != self.checksum {
            return Err(BicDbError::SyncBundle(format!(
                "bundle checksum mismatch: expected {}, calculated {}",
                self.checksum, checksum
            )));
        }

        for entry in &self.events {
            if entry.envelope.event_id != entry.event.id {
                return Err(BicDbError::SyncBundle(format!(
                    "event envelope id {} does not match event {}",
                    entry.envelope.event_id, entry.event.id
                )));
            }
            if entry.envelope.stream_id != entry.event.stream {
                return Err(BicDbError::SyncBundle(format!(
                    "event {} stream mismatch",
                    entry.event.id
                )));
            }
            let actual = payload_hash(&entry.event)?;
            if actual != entry.envelope.payload_hash {
                return Err(BicDbError::SyncBundle(format!(
                    "event {} payload hash mismatch",
                    entry.event.id
                )));
            }
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        let mut hasher = Sha256::new();
        hash_u32(&mut hasher, self.version);
        hasher.update(self.bundle_id.as_bytes());
        hasher.update(self.source_node_id.0.as_bytes());
        hash_i64(&mut hasher, self.created_at);
        hash_u64(&mut hasher, self.from_checkpoint.event_offset);
        hash_u64(&mut hasher, self.next_checkpoint.event_offset);
        hash_u64(&mut hasher, self.event_count as u64);

        for entry in &self.events {
            hasher.update(entry.envelope.node_id.0.as_bytes());
            hasher.update(entry.envelope.event_id.as_bytes());
            hash_str(&mut hasher, &entry.envelope.stream_id);
            hash_u64(&mut hasher, entry.envelope.sequence);
            hash_i64(&mut hasher, entry.envelope.timestamp);
            hasher.update(entry.envelope.payload_hash);
            match &entry.envelope.signature {
                Some(signature) => {
                    hasher.update([1]);
                    hash_str(&mut hasher, signature);
                }
                None => hasher.update([0]),
            }

            hasher.update(entry.event.id.as_bytes());
            hash_str(&mut hasher, &entry.event.stream);
            hash_str(&mut hasher, &entry.event.event_type);
            hash_i64(&mut hasher, entry.event.timestamp);
            stable_hash_json(&mut hasher, &entry.event.metadata);
        }

        Ok(hex::encode(hasher.finalize()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SyncMeshState {
    pub node_id: NodeId,
    pub last_export_offset: u64,
    pub last_export_at: Option<i64>,
    pub last_import_at: Option<i64>,
    pub last_sync_at: Option<i64>,
    pub exported_events: u64,
    pub imported_events: u64,
    pub conflicts_resolved: u64,
    /// Reconciliation high-water mark: every audit event below this log
    /// position has already been folded into record state. Imports only
    /// re-resolve records touched by events at or above it. Log rewrites
    /// reset it to zero (like `last_export_offset`); replay is idempotent.
    #[serde(default)]
    pub last_reconciled_offset: u64,
    /// Last known coverage vector per peer (keyed by node id), learned from
    /// session hellos and bundle `source_vector`s. Merged max-wise — vectors
    /// only grow — and used for mesh-era "pending for peer" status, never
    /// for correctness (sessions always exchange fresh vectors).
    #[serde(default)]
    pub peer_vectors: BTreeMap<String, SyncVector>,
    /// Origin-position floor for locally-authored events: local envelope
    /// positions are `base + log offset`. Advanced (never decreased) by
    /// every event-log rewrite after retained local events freeze their
    /// positions into metadata.
    #[serde(default)]
    pub origin_position_base: u64,
    /// Trust-on-first-use pins: node id → hex ed25519 verifying key. A pin
    /// never silently changes — a conflicting key is an alarm, not an
    /// update. Certificate chains (Phase 2) will layer real identity on
    /// top; pins are the interim spoof barrier.
    #[serde(default)]
    pub known_node_keys: BTreeMap<String, String>,
}

/// Mesh-era per-peer sync status: what this node still holds that the peer
/// had not covered as of the last session. The hub-era single
/// `pending_events` in [`SyncStatus`] cannot describe a mesh; this can.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerSyncStatus {
    pub node_id: NodeId,
    pub last_known_vector: SyncVector,
    pub pending_events_for_peer: usize,
}

impl SyncMeshState {
    pub fn load_or_create(
        path: impl AsRef<Path>,
        configured_node_id: Option<NodeId>,
        fsync: bool,
    ) -> Result<Self> {
        let path = path.as_ref();
        if path.exists() {
            let bytes = fs::read(path)?;
            if !bytes.is_empty() {
                return serde_json::from_slice(&bytes).map_err(Into::into);
            }
        }

        let state = Self {
            node_id: configured_node_id.unwrap_or_default(),
            last_export_offset: 0,
            last_export_at: None,
            last_import_at: None,
            last_sync_at: None,
            exported_events: 0,
            imported_events: 0,
            conflicts_resolved: 0,
            last_reconciled_offset: 0,
            peer_vectors: BTreeMap::new(),
            origin_position_base: 0,
            known_node_keys: BTreeMap::new(),
        };
        state.persist(path, fsync)?;
        Ok(state)
    }

    pub fn persist(&self, path: impl AsRef<Path>, fsync: bool) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(self)?;
        storage::write_atomic(path.as_ref(), &bytes, fsync)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncPendingChanges {
    pub node_id: NodeId,
    pub pending_events: usize,
    pub from_checkpoint: SyncCheckpoint,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncStatus {
    pub node_id: NodeId,
    pub total_events: usize,
    pub pending_events: usize,
    pub last_export_at: Option<i64>,
    pub last_import_at: Option<i64>,
    pub last_sync_at: Option<i64>,
    pub last_export_checkpoint: SyncCheckpoint,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncExportReport {
    pub bundle_id: Uuid,
    pub source_node_id: NodeId,
    pub event_count: usize,
    pub from_checkpoint: SyncCheckpoint,
    pub next_checkpoint: SyncCheckpoint,
    pub path: std::path::PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncImportReport {
    pub bundle_id: Uuid,
    pub source_node_id: NodeId,
    pub imported_events: usize,
    pub duplicate_events: usize,
    pub records_merged: usize,
    pub conflicts_resolved: usize,
    pub last_sync_at: i64,
}

pub struct SyncMesh<'db> {
    pub(crate) db: &'db mut crate::db::BicDb,
}

impl SyncMesh<'_> {
    pub fn export_since(&mut self, checkpoint: SyncCheckpoint) -> Result<SyncBundle> {
        self.db.export_sync_bundle_since(checkpoint)
    }

    /// This node's per-origin coverage vector; see [`crate::BicDb::sync_vector`].
    pub fn vector(&self) -> Result<SyncVector> {
        self.db.sync_vector()
    }

    /// Everything this node holds that `peer` does not, regardless of which
    /// node authored it; see [`crate::BicDb::export_sync_bundle_delta`].
    pub fn export_delta(&mut self, peer: &SyncVector) -> Result<SyncBundle> {
        self.db.export_sync_bundle_delta(peer)
    }

    pub fn export_to_path(
        &mut self,
        checkpoint: SyncCheckpoint,
        path: impl AsRef<Path>,
    ) -> Result<SyncExportReport> {
        self.db.write_sync_bundle_since(checkpoint, path)
    }

    pub fn export_encrypted_to_path(
        &mut self,
        checkpoint: SyncCheckpoint,
        path: impl AsRef<Path>,
        encryption_config: EncryptionConfig,
    ) -> Result<SyncExportReport> {
        self.db
            .write_encrypted_sync_bundle_since(checkpoint, path, encryption_config)
    }

    pub fn import(&mut self, events: Vec<SyncBundleEvent>) -> Result<SyncImportReport> {
        let next_checkpoint = SyncCheckpoint::new(events.len() as u64);
        let bundle = SyncBundle::new(
            self.db.node_id(),
            SyncCheckpoint::default(),
            next_checkpoint,
            events,
        )?;
        self.db.import_sync_bundle(bundle)
    }

    pub fn import_bundle(&mut self, bundle: SyncBundle) -> Result<SyncImportReport> {
        self.db.import_sync_bundle(bundle)
    }

    pub fn import_path(&mut self, path: impl AsRef<Path>) -> Result<SyncImportReport> {
        let bundle = SyncBundle::read(path)?;
        self.db.import_sync_bundle(bundle)
    }

    pub fn import_path_auto(
        &mut self,
        path: impl AsRef<Path>,
        encryption_config: Option<EncryptionConfig>,
    ) -> Result<SyncImportReport> {
        let bundle = SyncBundle::read_auto(path, encryption_config)?;
        self.db.import_sync_bundle(bundle)
    }
}

pub(crate) fn bundle_event_for_stored(
    owner_node_id: &NodeId,
    stored: &StoredEvent,
) -> Result<SyncBundleEvent> {
    bundle_event_for_stored_based(owner_node_id, 0, stored)
}

pub(crate) fn bundle_event_for_stored_based(
    owner_node_id: &NodeId,
    origin_position_base: u64,
    stored: &StoredEvent,
) -> Result<SyncBundleEvent> {
    let envelope = envelope_for_event_based(owner_node_id, origin_position_base, stored)?;
    Ok(SyncBundleEvent {
        envelope,
        event: stored.event.clone(),
    })
}

pub(crate) fn envelope_for_event(
    owner_node_id: &NodeId,
    stored: &StoredEvent,
) -> Result<EventEnvelope> {
    envelope_for_event_based(owner_node_id, 0, stored)
}

/// Local events derive their origin position as `base + log offset`. The
/// base is the durable floor advanced by every log rewrite (trim,
/// compaction) after the retained events' positions are frozen into
/// metadata — so origin positions stay monotonic in authorship order
/// forever, and mesh vectors survive retention without resyncs.
pub(crate) fn envelope_for_event_based(
    owner_node_id: &NodeId,
    origin_position_base: u64,
    stored: &StoredEvent,
) -> Result<EventEnvelope> {
    if let Some(envelope) = envelope_from_metadata(&stored.event)? {
        return Ok(envelope);
    }

    Ok(EventEnvelope {
        node_id: owner_node_id.clone(),
        event_id: stored.event.id,
        stream_id: stored.event.stream.clone(),
        sequence: origin_position_base.saturating_add(stored.offset),
        timestamp: stored.event.timestamp,
        payload_hash: payload_hash(&stored.event)?,
        signature: None,
    })
}

pub(crate) fn event_with_sync_metadata(
    mut event: Event,
    envelope: &EventEnvelope,
) -> Result<Event> {
    let mut sync_value = json!({
        "node_id": envelope.node_id.to_string(),
        "sequence": envelope.sequence,
        "stream_id": envelope.stream_id,
        "timestamp": envelope.timestamp,
        "payload_hash": hex::encode(envelope.payload_hash),
    });
    if let (Some(signature), Value::Object(fields)) = (&envelope.signature, &mut sync_value) {
        fields.insert("signature".to_string(), json!(signature));
    }

    match &mut event.metadata {
        Value::Object(metadata) => {
            metadata.insert(SYNC_METADATA_KEY.to_string(), sync_value);
        }
        other => {
            let original = other.clone();
            event.metadata = json!({
                "_bicdb_user_metadata": original,
                SYNC_METADATA_KEY: sync_value,
            });
        }
    }
    Ok(event)
}

fn is_encrypted_bundle(bytes: &[u8]) -> Result<bool> {
    let value: Value = serde_json::from_slice(bytes)?;
    Ok(value
        .get("kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == ENCRYPTED_SYNC_BUNDLE_KIND))
}

pub(crate) fn envelope_from_metadata(event: &Event) -> Result<Option<EventEnvelope>> {
    let Some(sync) = event.metadata.get(SYNC_METADATA_KEY) else {
        return Ok(None);
    };

    let node_id = sync
        .get("node_id")
        .and_then(Value::as_str)
        .ok_or_else(|| BicDbError::SyncBundle("event sync metadata missing node_id".to_string()))?
        .parse::<NodeId>()
        .map_err(|error| BicDbError::SyncBundle(error.to_string()))?;
    let sequence = sync
        .get("sequence")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            BicDbError::SyncBundle("event sync metadata missing sequence".to_string())
        })?;
    let stream_id = sync
        .get("stream_id")
        .and_then(Value::as_str)
        .unwrap_or(&event.stream)
        .to_string();
    let timestamp = sync
        .get("timestamp")
        .and_then(Value::as_i64)
        .unwrap_or(event.timestamp);

    let signature = sync
        .get("signature")
        .and_then(Value::as_str)
        .map(str::to_string);

    Ok(Some(EventEnvelope {
        node_id,
        event_id: event.id,
        stream_id,
        sequence,
        timestamp,
        payload_hash: payload_hash(event)?,
        signature,
    }))
}

pub(crate) fn payload_hash(event: &Event) -> Result<[u8; 32]> {
    let mut hasher = Sha256::new();
    stable_hash_json(&mut hasher, &event.payload);
    Ok(hasher.finalize().into())
}

fn stable_hash_json(hasher: &mut Sha256, value: &Value) {
    match value {
        Value::Null => hasher.update(b"n"),
        Value::Bool(value) => {
            hasher.update(b"b");
            hasher.update([u8::from(*value)]);
        }
        Value::Number(value) => {
            hasher.update(b"#");
            hash_str(hasher, &value.to_string());
        }
        Value::String(value) => {
            hasher.update(b"s");
            hash_str(hasher, value);
        }
        Value::Array(values) => {
            hasher.update(b"[");
            hash_u64(hasher, values.len() as u64);
            for value in values {
                stable_hash_json(hasher, value);
            }
        }
        Value::Object(values) => {
            hasher.update(b"{");
            hash_u64(hasher, values.len() as u64);
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (key, value) in entries {
                hash_str(hasher, key);
                stable_hash_json(hasher, value);
            }
        }
    }
}

fn hash_str(hasher: &mut Sha256, value: &str) {
    hash_u64(hasher, value.len() as u64);
    hasher.update(value.as_bytes());
}

fn hash_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_be_bytes());
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
}

fn hash_i64(hasher: &mut Sha256, value: i64) {
    hasher.update(value.to_be_bytes());
}

pub(crate) fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(value: u128) -> NodeId {
        NodeId(Uuid::from_u128(value))
    }

    #[test]
    fn a_sync_vector_distinguishes_sequence_zero_from_absence() {
        let mut vector = SyncVector::default();
        assert!(!vector.covers(&node(1), 0));
        vector.observe(&node(1), 0);
        assert!(vector.covers(&node(1), 0));
        assert!(!vector.covers(&node(1), 1));
        assert_eq!(vector.watermark(&node(1)), Some(0));
        assert_eq!(vector.watermark(&node(2)), None);
    }

    #[test]
    fn a_sync_vector_observe_and_merge_take_pointwise_maxima() {
        let mut left = SyncVector::default();
        left.observe(&node(1), 7);
        left.observe(&node(1), 3);
        assert_eq!(left.watermark(&node(1)), Some(7));

        let mut right = SyncVector::default();
        right.observe(&node(1), 5);
        right.observe(&node(2), 9);
        left.merge(&right);
        assert_eq!(left.watermark(&node(1)), Some(7));
        assert_eq!(left.watermark(&node(2)), Some(9));
    }

    #[test]
    fn a_bundle_source_vector_survives_serde_and_stays_outside_the_checksum() {
        let bundle = SyncBundle::new(
            node(1),
            SyncCheckpoint::default(),
            SyncCheckpoint::new(0),
            Vec::new(),
        )
        .unwrap();
        let mut vector = SyncVector::default();
        vector.observe(&node(1), 4);
        let stamped = bundle.clone().with_source_vector(vector.clone());
        stamped.verify().unwrap();

        let round_trip: SyncBundle =
            serde_json::from_slice(&serde_json::to_vec(&stamped).unwrap()).unwrap();
        assert_eq!(round_trip.source_vector, Some(vector));
        round_trip.verify().unwrap();

        // A pre-vector reader sees the same checksum it would compute itself.
        assert_eq!(stamped.checksum, bundle.checksum);
    }
}
