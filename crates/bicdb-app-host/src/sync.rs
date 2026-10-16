//! Application-scoped, explicitly authorized record synchronization.
//!
//! Database replication, job delivery and application release activation are
//! separate protocols. Only named data collections enter this exchange.
mod grants;
pub use grants::PeerCipher;

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{ensure, Context, Result};
use bicdb_core::{NodeId, SyncBundle, SyncVector, RECORD_AUDIT_STREAM};
use serde::{Deserialize, Serialize};

use crate::{
    database::Database,
    manifest::{Release, TableSync},
    store::atomic_write,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub node_id: NodeId,
    pub public_key: String,
    pub release: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub node_id: NodeId,
    pub public_key: String,
    /// Origin URL for the peer's application platform.
    pub endpoint: String,
    /// Application-scoped, pair-specific transport token. Never returned by status APIs.
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorResponse {
    pub identity: Identity,
    pub vector: SyncVector,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Exchange {
    pub identity: Identity,
    pub vector: SyncVector,
    pub bundle: SyncBundle,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SyncStatus {
    pub last_success_unix_ms: Option<u64>,
    pub last_error: Option<String>,
}

pub fn validate_endpoint(endpoint: &str) -> Result<()> {
    let url = reqwest::Url::parse(endpoint)?;
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/",
        "peer endpoint must be an origin URL without credentials"
    );
    let local = matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
    );
    ensure!(
        url.scheme() == "https" || (url.scheme() == "http" && local),
        "remote peer connections require HTTPS"
    );
    Ok(())
}

impl Peer {
    pub fn validate(&self) -> Result<()> {
        validate_endpoint(&self.endpoint)?;
        ensure!(
            self.public_key.len() == 64 && self.public_key.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid peer signing key"
        );
        ensure!(
            self.token.len() >= 32
                && self.token.len() <= 4096
                && !self.token.chars().any(char::is_whitespace),
            "invalid peer token"
        );
        Ok(())
    }
}

pub fn load_peers(path: &Path) -> Result<BTreeMap<String, Peer>> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    ensure!(
        std::fs::metadata(path)?.len() <= 1024 * 1024,
        "peer configuration too large"
    );
    let peers: BTreeMap<String, Peer> = serde_json::from_slice(&std::fs::read(path)?)?;
    ensure!(peers.len() <= 128, "too many synchronization peers");
    for (id, peer) in &peers {
        peer.validate()?;
        ensure!(id == &peer.node_id.to_string(), "peer identity mismatch");
    }
    Ok(peers)
}

pub fn save_peers(path: &Path, peers: &BTreeMap<String, Peer>) -> Result<()> {
    ensure!(peers.len() <= 128, "too many synchronization peers");
    atomic_write(path, &serde_json::to_vec(peers)?)
}

pub fn identity(database: &Database, release: &str) -> Result<Identity> {
    identity_from(&database.db.read(), release)
}
fn identity_from(db: &bicdb_core::BicDb, release: &str) -> Result<Identity> {
    Ok(Identity {
        node_id: db.node_id(),
        public_key: db
            .mesh_verifying_key()
            .context("database signing key unavailable")?,
        release: release.to_owned(),
    })
}

pub fn verify_identity(peer: &Peer, identity: &Identity, release: &str) -> Result<()> {
    ensure!(
        identity.node_id == peer.node_id && identity.public_key == peer.public_key,
        "peer identity does not match its provisioned pin"
    );
    ensure!(
        identity.release == release,
        "peer application release differs; deploy the same release before synchronizing"
    );
    Ok(())
}

fn check_bundle(bundle: &SyncBundle, release: &Release) -> Result<()> {
    ensure!(
        bundle.event_count <= 10_000,
        "synchronization delta exceeds 10000 events"
    );
    bundle.verify()?;
    for entry in &bundle.events {
        ensure!(
            entry.event.stream == RECORD_AUDIT_STREAM,
            "only declared application records may synchronize"
        );
        let table = entry
            .event
            .payload
            .get("collection")
            .and_then(serde_json::Value::as_str)
            .context("record event lacks a collection")?;
        let policy = release
            .manifest
            .sync
            .get(table)
            .context("record targets an undeclared synchronization table")?;
        match policy {
            TableSync::AppendOnly => ensure!(
                entry.event.event_type == "RecordCreated",
                "append-only tables refuse updates and deletes"
            ),
            TableSync::SingleWriter { node } => ensure!(
                entry.envelope.node_id == *node,
                "record was authored by an unauthorized writer"
            ),
        }
    }
    Ok(())
}

pub fn export(
    database: &mut Database,
    release: &Release,
    digest: &str,
    peer: &SyncVector,
) -> Result<Exchange> {
    database.edge_barrier()?;
    let mut db = database.db.write();
    let vector = db.sync_vector()?;
    vector.ensure_matching_equal_prefixes(peer)?;
    let bundle = db.export_sync_bundle_delta_bounded(peer, 10_000, 15 * 1024 * 1024)?;
    check_bundle(&bundle, release)?;
    ensure!(
        serde_json::to_vec(&bundle)?.len() <= 16 * 1024 * 1024,
        "synchronization delta exceeds 16 MiB"
    );
    Ok(Exchange {
        identity: identity_from(&db, digest)?,
        vector,
        bundle,
    })
}

pub fn import(database: &mut Database, release: &Release, exchange: Exchange) -> Result<()> {
    ensure!(
        exchange.bundle.source_node_id == exchange.identity.node_id,
        "bundle source differs from authenticated peer"
    );
    check_bundle(&exchange.bundle, release)?;
    database.edge_barrier()?;
    database
        .db
        .read()
        .sync_vector()?
        .ensure_matching_equal_prefixes(&exchange.vector)?;
    database
        .db
        .write()
        .import_sync_bundle_strict(exchange.bundle)?;
    database.edge_barrier()
}
