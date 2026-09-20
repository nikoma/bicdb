//! Transport-neutral duplex mesh session.
//!
//! Drives one full pairwise BicDB Mesh sync over any ordered, reliable byte
//! pipe — a TCP stream, a BLE L2CAP channel, a Unix socket pair, an in-memory
//! duplex. The transport never sees replication concepts; it moves frames.
//!
//! The exchange is a strict ping-pong so it is safe on half-duplex links and
//! never deadlocks two blocked writers on a small pipe buffer:
//!
//! ```text
//! initiator → responder: Hello { node, signing key }
//! responder → initiator: Hello { node, signing key }
//!             (both sides verify pre-provisioned pins)
//! initiator → responder: Vector { authenticated coverage }
//! responder → initiator: Vector { authenticated coverage }
//! initiator → responder: Delta { bundle for responder's vector }
//! responder → initiator: Delta { bundle for initiator's vector }
//! initiator → responder: Complete { import counts }
//! responder → initiator: Complete { import counts }
//! ```
//!
//! Interruption is the normal case, not the error path: imports are
//! idempotent (event-UUID dedupe) and vectors are recomputed from the durable
//! log, so a session cut at any byte is repaired by simply running another
//! session. One-sided progress is fine — if the link dies after the responder
//! imported but before the initiator did, nothing is lost and nothing is
//! duplicated on retry.

use std::io::{Read, Write};
use std::sync::Mutex;

use bicdb_core::{BicDb, BicDbError, NodeId, Result, SyncBundle, SyncVector};
use serde::{Deserialize, Serialize};

pub const MESH_PROTOCOL_VERSION: u32 = 2;

/// Upper bound on a single frame. Deltas beyond this indicate either a
/// corrupted length prefix or a bundle that should have been chunked by a
/// future protocol revision; refuse rather than allocate blindly.
pub const MAX_MESH_FRAME_BYTES: u32 = 16 * 1024 * 1024;
pub const MAX_MESH_EVENTS_PER_BUNDLE: usize = 10_000;

/// Per-frame timing stamps, both on the sender's clock. Combined with the
/// receiver's own send/receive instants, each received frame yields a full
/// NTP-style tuple, so both sides estimate the peer clock offset without
/// extra round trips. Purely additive: peers without stamps still sync.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrameTiming {
    pub sent_at_ms: i64,
    pub prev_received_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MeshMessage {
    Hello {
        protocol: u32,
        node_id: NodeId,
        /// Fresh session nonce bound into coverage signatures.
        nonce: String,
        #[serde(default)]
        timing: Option<FrameTiming>,
        /// Hex ed25519 verifying key. Receivers require an exact
        /// out-of-band-provisioned pin before exchanging coverage or data.
        #[serde(default)]
        public_key: Option<String>,
    },
    Vector {
        vector: SyncVector,
        signature: String,
        #[serde(default)]
        timing: Option<FrameTiming>,
    },
    Delta {
        bundle: SyncBundle,
        #[serde(default)]
        timing: Option<FrameTiming>,
    },
    Complete {
        imported_events: usize,
        duplicate_events: usize,
        #[serde(default)]
        timing: Option<FrameTiming>,
    },
}

impl MeshMessage {
    fn timing(&self) -> Option<FrameTiming> {
        match self {
            MeshMessage::Hello { timing, .. }
            | MeshMessage::Vector { timing, .. }
            | MeshMessage::Delta { timing, .. }
            | MeshMessage::Complete { timing, .. } => *timing,
        }
    }
}

/// Tracks send/receive instants across the ping-pong and keeps the
/// lowest-uncertainty offset estimate the session produced.
#[derive(Debug, Default)]
struct TimingTracker {
    last_sent_at_ms: Option<i64>,
    last_received_at_ms: Option<i64>,
    /// (peer_clock_minus_ours_ms, error_ms)
    best: Option<(i64, i64)>,
}

impl TimingTracker {
    fn outgoing(&mut self) -> Option<FrameTiming> {
        let now = unix_ms();
        self.last_sent_at_ms = Some(now);
        Some(FrameTiming {
            sent_at_ms: now,
            prev_received_at_ms: self.last_received_at_ms,
        })
    }

    fn ingest(&mut self, message: &MeshMessage) {
        let received_at = unix_ms();
        self.last_received_at_ms = Some(received_at);
        let Some(timing) = message.timing() else {
            return;
        };
        let (Some(t1), Some(t2)) = (self.last_sent_at_ms, timing.prev_received_at_ms) else {
            return;
        };
        let (t3, t4) = (timing.sent_at_ms, received_at);
        let round_trip = (t4 - t1) - (t3 - t2);
        if round_trip < 0 {
            return;
        }
        let offset = ((t2 - t1) + (t3 - t4)) / 2;
        let error = (round_trip / 2).max(1);
        if self.best.is_none_or(|(_, best_error)| error < best_error) {
            self.best = Some((offset, error));
        }
    }
}

/// Session estimates worse than this never become replicated clock
/// observations — a congested link's guess is not evidence.
const MAX_OBSERVATION_ERROR_MS: i64 = 5_000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MeshSyncReport {
    pub local_node_id: NodeId,
    pub peer_node_id: NodeId,
    pub sent_events: usize,
    pub received_events: usize,
    pub imported_events: usize,
    pub duplicate_events: usize,
    pub records_merged: usize,
    pub conflicts_resolved: usize,
    /// What the peer reported importing from our delta, if the session lived
    /// long enough to hear it.
    pub peer_imported_events: usize,
    /// Best NTP-style estimate this session produced: how far the peer's
    /// clock is ahead of ours, in milliseconds. `None` when the peer sent no
    /// timing stamps.
    #[serde(default)]
    pub peer_clock_offset_ms: Option<i64>,
    #[serde(default)]
    pub peer_clock_error_ms: Option<i64>,
}

/// Runs a mesh session as the side that speaks first.
pub fn run_mesh_initiator(db: &mut BicDb, io: &mut (impl Read + Write)) -> Result<MeshSyncReport> {
    run_session(&mut BorrowedDatabase(db), io, true)
}

/// Runs a mesh session as the side that answers.
pub fn run_mesh_responder(db: &mut BicDb, io: &mut (impl Read + Write)) -> Result<MeshSyncReport> {
    run_session(&mut BorrowedDatabase(db), io, false)
}

/// Runs an initiating session against a shared database, taking the mutex
/// only for individual database operations and never across socket I/O.
pub fn run_mesh_initiator_shared(
    db: &Mutex<BicDb>,
    io: &mut (impl Read + Write),
) -> Result<MeshSyncReport> {
    run_session(&mut SharedDatabase(db), io, true)
}

/// Runs a responding session against a shared database, taking the mutex
/// only for individual database operations and never across socket I/O.
pub fn run_mesh_responder_shared(
    db: &Mutex<BicDb>,
    io: &mut (impl Read + Write),
) -> Result<MeshSyncReport> {
    run_session(&mut SharedDatabase(db), io, false)
}

trait MeshDatabase {
    fn with_db<T>(&mut self, operation: impl FnOnce(&mut BicDb) -> Result<T>) -> Result<T>;
}

struct BorrowedDatabase<'a>(&'a mut BicDb);

impl MeshDatabase for BorrowedDatabase<'_> {
    fn with_db<T>(&mut self, operation: impl FnOnce(&mut BicDb) -> Result<T>) -> Result<T> {
        operation(self.0)
    }
}

struct SharedDatabase<'a>(&'a Mutex<BicDb>);

impl MeshDatabase for SharedDatabase<'_> {
    fn with_db<T>(&mut self, operation: impl FnOnce(&mut BicDb) -> Result<T>) -> Result<T> {
        let mut guard = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        operation(&mut guard)
    }
}

fn run_session<D: MeshDatabase>(
    database: &mut D,
    io: &mut (impl Read + Write),
    initiator: bool,
) -> Result<MeshSyncReport> {
    let (local_node_id, local_public_key) = database.with_db(|db| {
        let public_key = db.mesh_verifying_key().ok_or_else(|| {
            BicDbError::SyncBundle(
                "mesh networking requires a local signing key; enable mesh_signing".to_string(),
            )
        })?;
        Ok((db.node_id(), public_key))
    })?;
    let mut clock = TimingTracker::default();
    let local_nonce = uuid::Uuid::new_v4().to_string();
    let hello = |clock: &mut TimingTracker| MeshMessage::Hello {
        protocol: MESH_PROTOCOL_VERSION,
        node_id: local_node_id.clone(),
        nonce: local_nonce.clone(),
        timing: clock.outgoing(),
        public_key: Some(local_public_key.clone()),
    };

    let (peer_node_id, peer_public_key, peer_nonce) = if initiator {
        let message = hello(&mut clock);
        write_frame(io, &message)?;
        expect_hello(read_timed(io, &mut clock)?)?
    } else {
        let peer = expect_hello(read_timed(io, &mut clock)?)?;
        let message = hello(&mut clock);
        write_frame(io, &message)?;
        peer
    };
    let peer_public_key = peer_public_key.ok_or_else(|| {
        BicDbError::SyncBundle(format!(
            "mesh peer {peer_node_id} did not present a signing key"
        ))
    })?;
    // Trust must be provisioned out of band. Do this before recording the
    // peer vector or exporting a delta so an unknown connection learns no
    // database contents and cannot create durable trust state.
    database.with_db(|db| db.require_pinned_node_key(&peer_node_id, &peer_public_key))?;

    let (initiator_nonce, responder_nonce) = if initiator {
        (local_nonce.as_str(), peer_nonce.as_str())
    } else {
        (peer_nonce.as_str(), local_nonce.as_str())
    };

    let local_vector = database.with_db(|db| db.sync_vector())?;
    let local_vector_signature = database.with_db(|db| {
        db.sign_mesh_coverage(
            MESH_PROTOCOL_VERSION,
            &peer_node_id,
            initiator_nonce,
            responder_nonce,
            &local_vector,
        )
    })?;
    let vector_message = |clock: &mut TimingTracker| MeshMessage::Vector {
        vector: local_vector.clone(),
        signature: local_vector_signature.clone(),
        timing: clock.outgoing(),
    };
    let (peer_vector, peer_vector_signature) = if initiator {
        let message = vector_message(&mut clock);
        write_frame(io, &message)?;
        expect_vector(read_timed(io, &mut clock)?)?
    } else {
        let vector = expect_vector(read_timed(io, &mut clock)?)?;
        let message = vector_message(&mut clock);
        write_frame(io, &message)?;
        vector
    };
    database.with_db(|db| {
        db.verify_mesh_coverage(
            MESH_PROTOCOL_VERSION,
            &peer_node_id,
            &peer_public_key,
            initiator_nonce,
            responder_nonce,
            &peer_vector,
            &peer_vector_signature,
        )
    })?;
    // Even if the session dies after this point, we learned the peer's
    // coverage — mesh_peer_status stays useful across interruptions.
    let delta = database.with_db(|db| {
        db.record_peer_vector(&peer_node_id, &peer_vector)?;
        db.export_sync_bundle_delta(&peer_vector)
    })?;
    let sent_events = delta.event_count;
    let delta_source_vector = delta.source_vector.clone();

    let peer_bundle = if initiator {
        let message = MeshMessage::Delta {
            bundle: delta,
            timing: clock.outgoing(),
        };
        write_frame(io, &message)?;
        expect_delta(read_timed(io, &mut clock)?)?
    } else {
        let bundle = expect_delta(read_timed(io, &mut clock)?)?;
        let message = MeshMessage::Delta {
            bundle: delta,
            timing: clock.outgoing(),
        };
        write_frame(io, &message)?;
        bundle
    };

    if peer_bundle.source_node_id != peer_node_id {
        return Err(BicDbError::SyncBundle(format!(
            "mesh peer {peer_node_id} sent a bundle claiming source {}",
            peer_bundle.source_node_id
        )));
    }
    let received_events = peer_bundle.event_count;
    let import = database.with_db(|db| db.import_sync_bundle_strict(peer_bundle))?;

    let complete = |clock: &mut TimingTracker| MeshMessage::Complete {
        imported_events: import.imported_events,
        duplicate_events: import.duplicate_events,
        timing: clock.outgoing(),
    };
    let peer_imported_events = if initiator {
        let message = complete(&mut clock);
        write_frame(io, &message)?;
        expect_complete(read_timed(io, &mut clock)?)?
    } else {
        let peer_imported = expect_complete(read_timed(io, &mut clock)?)?;
        let message = complete(&mut clock);
        write_frame(io, &message)?;
        peer_imported
    };

    // The peer's COMPLETE confirms it imported our delta, so its coverage is
    // now its hello vector plus everything we held at export time — without
    // this, per-peer status would trail one session behind.
    if let Some(source_vector) = delta_source_vector {
        let mut peer_after = peer_vector;
        peer_after.merge(&source_vector);
        database.with_db(|db| db.record_peer_vector(&peer_node_id, &peer_after))?;
    }

    // A tight estimate becomes replicated evidence for the conflict
    // resolver; a loose one stays a session-local report detail.
    if let Some((offset_ms, error_ms)) = clock.best {
        if error_ms <= MAX_OBSERVATION_ERROR_MS {
            let _ = database
                .with_db(|db| db.record_clock_observation(&peer_node_id, offset_ms, error_ms));
        }
    }

    Ok(MeshSyncReport {
        local_node_id,
        peer_node_id,
        sent_events,
        received_events,
        imported_events: import.imported_events,
        duplicate_events: import.duplicate_events,
        records_merged: import.records_merged,
        conflicts_resolved: import.conflicts_resolved,
        peer_imported_events,
        peer_clock_offset_ms: clock.best.map(|(offset, _)| offset),
        peer_clock_error_ms: clock.best.map(|(_, error)| error),
    })
}

fn read_timed(io: &mut impl Read, clock: &mut TimingTracker) -> Result<MeshMessage> {
    let message = read_frame(io)?;
    clock.ingest(&message);
    Ok(message)
}

fn unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn expect_hello(message: MeshMessage) -> Result<(NodeId, Option<String>, String)> {
    match message {
        MeshMessage::Hello {
            protocol,
            node_id,
            nonce,
            public_key,
            ..
        } => {
            if protocol != MESH_PROTOCOL_VERSION {
                return Err(BicDbError::SyncBundle(format!(
                    "unsupported mesh protocol version {protocol} (this build speaks {MESH_PROTOCOL_VERSION})"
                )));
            }
            if uuid::Uuid::parse_str(&nonce).is_err() {
                return Err(BicDbError::SyncBundle(
                    "mesh hello nonce is invalid".to_string(),
                ));
            }
            Ok((node_id, public_key, nonce))
        }
        other => Err(unexpected("hello", &other)),
    }
}

fn expect_vector(message: MeshMessage) -> Result<(SyncVector, String)> {
    match message {
        MeshMessage::Vector {
            vector, signature, ..
        } => Ok((vector, signature)),
        other => Err(unexpected("vector", &other)),
    }
}

fn expect_delta(message: MeshMessage) -> Result<SyncBundle> {
    match message {
        MeshMessage::Delta { bundle, .. } => {
            if bundle.event_count > MAX_MESH_EVENTS_PER_BUNDLE
                || bundle.events.len() > MAX_MESH_EVENTS_PER_BUNDLE
            {
                return Err(BicDbError::SyncBundle(format!(
                    "mesh bundle contains {} events; limit is {MAX_MESH_EVENTS_PER_BUNDLE}",
                    bundle.event_count.max(bundle.events.len())
                )));
            }
            Ok(bundle)
        }
        other => Err(unexpected("delta", &other)),
    }
}

fn expect_complete(message: MeshMessage) -> Result<usize> {
    match message {
        MeshMessage::Complete {
            imported_events, ..
        } => Ok(imported_events),
        other => Err(unexpected("complete", &other)),
    }
}

fn unexpected(expected: &str, got: &MeshMessage) -> BicDbError {
    let kind = match got {
        MeshMessage::Hello { .. } => "hello",
        MeshMessage::Vector { .. } => "vector",
        MeshMessage::Delta { .. } => "delta",
        MeshMessage::Complete { .. } => "complete",
    };
    BicDbError::SyncBundle(format!(
        "mesh session expected {expected} frame, peer sent {kind}"
    ))
}

fn write_frame(io: &mut impl Write, message: &MeshMessage) -> Result<()> {
    let bytes = serde_json::to_vec(message)?;
    let length = u32::try_from(bytes.len())
        .map_err(|_| BicDbError::SyncBundle("mesh frame exceeds u32 length".to_string()))?;
    if length > MAX_MESH_FRAME_BYTES {
        return Err(BicDbError::SyncBundle(format!(
            "mesh frame of {length} bytes exceeds the {MAX_MESH_FRAME_BYTES}-byte limit"
        )));
    }
    io.write_all(&length.to_be_bytes())?;
    io.write_all(&bytes)?;
    io.flush()?;
    Ok(())
}

/// Read exactly `length` bytes, allocating only as they arrive.
fn read_exact_growing(io: &mut impl Read, length: usize) -> Result<Vec<u8>> {
    const CHUNK: usize = 64 * 1024;
    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = vec![0_u8; CHUNK.min(length.max(1))];
    while buffer.len() < length {
        let wanted = (length - buffer.len()).min(chunk.len());
        io.read_exact(&mut chunk[..wanted])?;
        buffer.try_reserve(wanted).map_err(|_| {
            BicDbError::SyncBundle(format!(
                "cannot allocate {wanted} bytes for an inbound mesh frame"
            ))
        })?;
        buffer.extend_from_slice(&chunk[..wanted]);
    }
    Ok(buffer)
}

fn read_frame(io: &mut impl Read) -> Result<MeshMessage> {
    let mut length_bytes = [0_u8; 4];
    io.read_exact(&mut length_bytes)?;
    let length = u32::from_be_bytes(length_bytes);
    if length > MAX_MESH_FRAME_BYTES {
        return Err(BicDbError::SyncBundle(format!(
            "mesh frame of {length} bytes exceeds the {MAX_MESH_FRAME_BYTES}-byte limit"
        )));
    }
    // Grow with the bytes that actually arrive rather than trusting the
    // declared length. `vec![0u8; length]` let a four-byte header commit
    // 16 MiB of resident memory per connection — and the FIRST frame of a
    // session is read BEFORE the peer's key is pinned
    // (`require_pinned_node_key` runs on the Hello contents), so this was
    // a four-million-fold amplification available to an unauthenticated
    // TCP peer, repeatable across connections. Same rule as everywhere
    // else: no wire-provided count sizes an allocation
    // (`bicdb_core::parse_budget`).
    let bytes = read_exact_growing(io, length as usize)?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(test)]
mod frame_allocation_tests {
    use super::*;

    /// A four-byte header declaring a 16 MiB frame used to commit 16 MiB
    /// of resident memory immediately — and the first frame of a session
    /// is read BEFORE the peer's signing key is pinned, so an
    /// unauthenticated TCP peer could do this repeatedly across
    /// connections for four bytes each.
    #[test]
    fn a_declared_length_costs_only_the_bytes_that_arrive() {
        // Declares the maximum, sends nothing.
        let mut framed = Vec::new();
        framed.extend_from_slice(&MAX_MESH_FRAME_BYTES.to_be_bytes());
        let mut reader = framed.as_slice();
        assert!(
            read_frame(&mut reader).is_err(),
            "a frame body that never arrives must error"
        );

        // Declares the maximum, sends a handful of bytes, then ends.
        let mut framed = Vec::new();
        framed.extend_from_slice(&MAX_MESH_FRAME_BYTES.to_be_bytes());
        framed.extend_from_slice(b"not sixteen mebibytes");
        let mut reader = framed.as_slice();
        assert!(read_frame(&mut reader).is_err(), "a short body must error");

        // Beyond the protocol limit is refused before any read.
        let mut framed = Vec::new();
        framed.extend_from_slice(&(MAX_MESH_FRAME_BYTES + 1).to_be_bytes());
        let mut reader = framed.as_slice();
        let error = read_frame(&mut reader).expect_err("oversize must be refused");
        assert!(format!("{error}").contains("exceeds"));
    }

    /// Honest frames must still round-trip, or the bound would simply have
    /// broken replication.
    #[test]
    fn well_formed_frames_still_round_trip() {
        let message = MeshMessage::Hello {
            protocol: MESH_PROTOCOL_VERSION,
            node_id: NodeId(uuid::Uuid::from_u128(7)),
            nonce: "nonce".to_string(),
            timing: None,
            public_key: None,
        };
        let mut buffer = Vec::new();
        write_frame(&mut buffer, &message).unwrap();
        let mut reader = buffer.as_slice();
        let decoded = read_frame(&mut reader).unwrap();
        assert!(matches!(decoded, MeshMessage::Hello { .. }));
    }
}
