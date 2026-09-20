//! Authenticated, bounded RPC transport for physical range relocation.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use rustls::{ClientConnection, StreamOwned};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::distribution::{
    ClusterBootstrapSnapshot, ClusterId, ClusterNetworkTransportConfig, ClusterNode, ClusterNodeId,
    ClusterNodeLifecycle, ClusterTopology, DistributionStore, MetadataMemberRole, RangeDescriptor,
    RangeId, RangeRelocation, RangeSnapshotOptions, CLUSTER_DATA_PROTOCOL_VERSION,
    SCHEMA_BOOTSTRAP_NODE_LABEL, SCHEMA_COMPATIBILITY_NODE_LABEL,
    SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL,
};
use crate::distribution_anti_entropy::{
    RangeDigestBucketScanStep, RangeDigestLimits, RangeDigestState, RangeDigestTransport,
};
use crate::distribution_anti_entropy_repair::{
    RangeDigestRepairBatch, RangeDigestRepairLimits, RangeDigestRepairState,
    RangeDigestRepairTransport,
};
use crate::distribution_consensus::{
    MetadataAppendRequest, MetadataAppendResponse, MetadataConsensusStore, MetadataVoteRequest,
    MetadataVoteResponse,
};
use crate::distribution_data::{
    CatchUpSourceBatch, ClusterDataNodeService, ClusterSchemaActivationReceipt,
    ClusterSchemaFinalizationReceipt, ClusterSchemaStageReceipt, SnapshotSourceStep,
};
use crate::distribution_range_consensus::{
    RangeWriteAck, RangeWriteCommand, RangeWriteProbe, RangeWriteProgress, RangeWriteRepairBatch,
    RangeWriteRepairLimits, RangeWriteTransport,
};
use crate::distribution_schema_rollout::{
    ClusterSchemaActivationTransport, ClusterSchemaFinalizationTransport,
    ClusterSchemaStageTransport,
};
use crate::distribution_supervisor::{
    CatchUpProgress, CleanupProgress, ClusterRelocationTransport, SnapshotCopyProgress,
};
use crate::error::{BicDbError, Result};
use crate::replication_transport::{
    build_client_tls, build_server_tls, certificate_der_sha256, client_tls_stream,
    replication_certificate_sha256, server_tls_stream, ReplicationClientTls,
};
use crate::{CancellationToken, ClusterSchemaBundle, SignedClusterSchemaBundle};

const FRAME_HEADER_BYTES: usize = 4;

fn transport_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(message.into())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ClusterDataRequestEnvelope {
    protocol_version: u32,
    cluster_id: ClusterId,
    caller_node_id: ClusterNodeId,
    destination_node_id: ClusterNodeId,
    request_id: u64,
    request: ClusterDataRequest,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum ClusterDataRequest {
    Ping,
    ExportSchemaBundle {
        range_id: RangeId,
        range_epoch: u64,
    },
    InstallSchemaBundle {
        relocation: RangeRelocation,
        range: RangeDescriptor,
        bundle: ClusterSchemaBundle,
    },
    StageSignedSchemaBundle {
        signed_bundle: SignedClusterSchemaBundle,
    },
    AdvanceSignedSchemaActivation {
        rollout_id: Uuid,
        stage_id: Uuid,
        base_fingerprint_sha256: String,
        target_fingerprint_sha256: String,
    },
    FinalizeSignedSchemaActivation {
        activation: ClusterSchemaActivationReceipt,
    },
    PrepareLearner {
        relocation: RangeRelocation,
    },
    ExportSnapshot {
        relocation: RangeRelocation,
        range: RangeDescriptor,
        options: RangeSnapshotOptions,
    },
    ApplySnapshot {
        relocation: RangeRelocation,
        range: RangeDescriptor,
        step: SnapshotSourceStep,
    },
    LearnerWatermark {
        relocation: RangeRelocation,
    },
    ExportCatchUp {
        range: RangeDescriptor,
        durable_commit_sequence: u64,
        max_commit_frames: usize,
    },
    ApplyCatchUp {
        relocation: RangeRelocation,
        range: RangeDescriptor,
        batch: CatchUpSourceBatch,
    },
    CleanupSource {
        relocation: RangeRelocation,
        range: RangeDescriptor,
        resume_after_key: Option<String>,
        max_records: usize,
    },
    CompleteRelocation {
        relocation: RangeRelocation,
    },
    Heartbeat {
        node_id: ClusterNodeId,
        incarnation: u64,
        used_bytes: u64,
        capacity_bytes: u64,
        labels: BTreeMap<String, String>,
        tls_certificate_sha256: Option<String>,
        now_ms: u64,
    },
    StageTlsCertificateRotation {
        node_id: ClusterNodeId,
        next_tls_certificate_sha256: String,
        now_ms: u64,
    },
    AbortTlsCertificateRotation {
        node_id: ClusterNodeId,
        now_ms: u64,
    },
    RegisterMetadataLearner {
        node: ClusterNode,
        now_ms: u64,
    },
    PromoteMetadataLearner {
        node_id: ClusterNodeId,
        installed_commit_index: u64,
        installed_topology_generation: u64,
        now_ms: u64,
    },
    FetchBootstrapSnapshot,
    FetchTopology,
    MetadataVote {
        request: MetadataVoteRequest,
    },
    MetadataAppend {
        request: MetadataAppendRequest,
    },
    PrepareRangeWrite {
        command: RangeWriteCommand,
    },
    CommitRangeWrite {
        command: RangeWriteCommand,
    },
    CertifyRangeWrite {
        command: RangeWriteCommand,
    },
    ApplyRangeWrite {
        command: RangeWriteCommand,
    },
    AbortRangeWrite {
        command: RangeWriteCommand,
    },
    InstallRangeBackupFence {
        plan_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
        installed_at_ms: u64,
        expires_at_ms: u64,
    },
    ReleaseRangeBackupFence {
        plan_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
    },
    RangeWriteProgress {
        range_id: crate::distribution::RangeId,
        range_epoch: u64,
    },
    ApplyRangeWriteRepair {
        batch: RangeWriteRepairBatch,
        limits: RangeWriteRepairLimits,
    },
    FetchRangeWriteRepair {
        range_id: crate::distribution::RangeId,
        range_epoch: u64,
        previous_resolved_index: u64,
        limits: RangeWriteRepairLimits,
    },
    ProbeRangeWrite {
        range_id: crate::distribution::RangeId,
        range_epoch: u64,
        index: u64,
    },
    AdvanceRangeDigest {
        range_id: RangeId,
        range_epoch: u64,
        session_id: Uuid,
        expected_checksum_sha256: Option<String>,
        limits: RangeDigestLimits,
    },
    ExportRangeDigestBucket {
        range_id: RangeId,
        range_epoch: u64,
        session_id: Uuid,
        bucket: u32,
        resume_after_key: Option<String>,
        limits: RangeDigestLimits,
    },
    ApplyRangeDigestRepair {
        range_id: RangeId,
        range_epoch: u64,
        batch: RangeDigestRepairBatch,
        limits: RangeDigestRepairLimits,
    },
}

impl ClusterDataRequest {
    fn range_id_for_schema_fence(&self) -> Option<RangeId> {
        match self {
            Self::ExportSchemaBundle { range_id, .. } => Some(*range_id),
            Self::InstallSchemaBundle { .. }
            | Self::StageSignedSchemaBundle { .. }
            | Self::AdvanceSignedSchemaActivation { .. }
            | Self::FinalizeSignedSchemaActivation { .. } => None,
            Self::PrepareLearner { relocation }
            | Self::LearnerWatermark { relocation }
            | Self::CompleteRelocation { relocation } => Some(relocation.range_id),
            Self::ExportSnapshot { range, .. }
            | Self::ApplySnapshot { range, .. }
            | Self::ExportCatchUp { range, .. }
            | Self::ApplyCatchUp { range, .. }
            | Self::CleanupSource { range, .. } => Some(range.id),
            Self::PrepareRangeWrite { command }
            | Self::CommitRangeWrite { command }
            | Self::CertifyRangeWrite { command }
            | Self::ApplyRangeWrite { command }
            | Self::AbortRangeWrite { command } => Some(command.range_id),
            Self::RangeWriteProgress { range_id, .. }
            | Self::InstallRangeBackupFence { range_id, .. }
            | Self::ReleaseRangeBackupFence { range_id, .. }
            | Self::FetchRangeWriteRepair { range_id, .. }
            | Self::ProbeRangeWrite { range_id, .. }
            | Self::AdvanceRangeDigest { range_id, .. }
            | Self::ExportRangeDigestBucket { range_id, .. }
            | Self::ApplyRangeDigestRepair { range_id, .. } => Some(*range_id),
            Self::ApplyRangeWriteRepair { batch, .. } => Some(batch.range_id),
            Self::Ping
            | Self::Heartbeat { .. }
            | Self::StageTlsCertificateRotation { .. }
            | Self::AbortTlsCertificateRotation { .. }
            | Self::RegisterMetadataLearner { .. }
            | Self::PromoteMetadataLearner { .. } => None,
            Self::FetchBootstrapSnapshot
            | Self::FetchTopology
            | Self::MetadataVote { .. }
            | Self::MetadataAppend { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ClusterDataResponseEnvelope {
    protocol_version: u32,
    cluster_id: ClusterId,
    source_node_id: ClusterNodeId,
    request_id: u64,
    response: std::result::Result<ClusterDataResponse, ClusterDataRemoteError>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum ClusterDataResponse {
    Pong,
    Ack,
    SchemaBundle(ClusterSchemaBundle),
    SchemaStage(ClusterSchemaStageReceipt),
    SchemaActivation(ClusterSchemaActivationReceipt),
    SchemaFinalization(ClusterSchemaFinalizationReceipt),
    SnapshotStep(SnapshotSourceStep),
    SnapshotProgress(SnapshotCopyProgress),
    LearnerWatermark(u64),
    CatchUpBatch(CatchUpSourceBatch),
    CatchUpProgress(CatchUpProgress),
    CleanupProgress(CleanupProgress),
    BootstrapSnapshot(ClusterBootstrapSnapshot),
    Topology(ClusterTopology),
    MetadataVote(MetadataVoteResponse),
    MetadataAppend(MetadataAppendResponse),
    RangeWrite(RangeWriteAck),
    RangeWriteProgress(RangeWriteProgress),
    RangeWriteRepair(RangeWriteRepairBatch),
    RangeWriteProbe(RangeWriteProbe),
    RangeBackupFenceReleased(bool),
    RangeDigestState(RangeDigestState),
    RangeDigestBucket(RangeDigestBucketScanStep),
    RangeDigestRepairState(RangeDigestRepairState),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ClusterDataRemoteError {
    code: String,
    message: String,
}

fn send_json_frame<T: Serialize>(
    writer: &mut impl Write,
    value: &T,
    max_frame_bytes: usize,
) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.is_empty() || bytes.len() > max_frame_bytes || bytes.len() > u32::MAX as usize {
        return Err(transport_error(format!(
            "cluster RPC frame size {} exceeds configured bound {}",
            bytes.len(),
            max_frame_bytes
        )));
    }
    writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
    writer.write_all(&bytes)?;
    writer.flush()?;
    Ok(())
}

fn recv_json_frame<T: DeserializeOwned>(
    reader: &mut impl Read,
    max_frame_bytes: usize,
) -> Result<T> {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    reader.read_exact(&mut header)?;
    let len = u32::from_be_bytes(header) as usize;
    if len == 0 || len > max_frame_bytes {
        return Err(transport_error(format!(
            "cluster RPC frame size {len} exceeds configured bound {max_frame_bytes}"
        )));
    }
    let mut bytes = vec![0_u8; len];
    reader.read_exact(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub struct ClusterDataServerHandle {
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for ClusterDataServerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterDataServerHandle")
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

impl ClusterDataServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    pub fn join(mut self) -> Result<()> {
        self.request_shutdown();
        if let Some(handle) = self.thread.take() {
            handle
                .join()
                .map_err(|_| transport_error("cluster data RPC server thread panicked"))?;
        }
        Ok(())
    }
}

impl Drop for ClusterDataServerHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

pub fn start_cluster_data_server(
    address: impl ToSocketAddrs,
    config: ClusterNetworkTransportConfig,
    topology: Arc<RwLock<ClusterTopology>>,
    service: Arc<Mutex<ClusterDataNodeService>>,
    control_store: Option<Arc<Mutex<DistributionStore>>>,
    metadata_consensus: Option<Arc<Mutex<MetadataConsensusStore>>>,
    metadata_distribution_store: Option<Arc<Mutex<DistributionStore>>>,
) -> Result<ClusterDataServerHandle> {
    config.validate()?;
    let listener = TcpListener::bind(address)?;
    let local_addr = listener.local_addr()?;
    if config.dev_localhost_plaintext && !local_addr.ip().is_loopback() {
        return Err(transport_error(
            "plaintext cluster data RPC may bind only to loopback",
        ));
    }
    listener.set_nonblocking(true)?;
    let server_tls = if config.dev_localhost_plaintext {
        None
    } else {
        Some(build_server_tls(
            config
                .tls
                .as_ref()
                .expect("validated cluster TLS configuration"),
        )?)
    };
    let shutdown = Arc::new(AtomicBool::new(false));
    let thread_shutdown = Arc::clone(&shutdown);
    let active = Arc::new(AtomicUsize::new(0));
    let thread = thread::spawn(move || {
        while !thread_shutdown.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, peer)) => {
                    if config.dev_localhost_plaintext && !peer.ip().is_loopback() {
                        continue;
                    }
                    if active
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                            (value < config.max_inbound_connections)
                                .then_some(value.saturating_add(1))
                        })
                        .is_err()
                    {
                        continue;
                    }
                    let active = Arc::clone(&active);
                    let topology = Arc::clone(&topology);
                    let service = Arc::clone(&service);
                    let control_store = control_store.as_ref().map(Arc::clone);
                    let metadata_consensus = metadata_consensus.as_ref().map(Arc::clone);
                    let metadata_distribution_store =
                        metadata_distribution_store.as_ref().map(Arc::clone);
                    let tls = server_tls.as_ref().map(|tls| Arc::clone(&tls.config));
                    let config = config.clone();
                    thread::spawn(move || {
                        let _active = ActiveConnection(active);
                        let _ = configure_stream(&stream, &config);
                        let result = if let Some(tls) = tls {
                            server_tls_stream(tls, stream).and_then(|mut stream| {
                                while stream.conn.is_handshaking() {
                                    stream.conn.complete_io(&mut stream.sock)?;
                                }
                                let peer_certificate_sha256 = stream
                                    .conn
                                    .peer_certificates()
                                    .and_then(|certificates| certificates.first())
                                    .map(|certificate| certificate_der_sha256(certificate.as_ref()))
                                    .ok_or_else(|| {
                                        transport_error(
                                            "cluster mTLS peer supplied no leaf certificate",
                                        )
                                    })?;
                                handle_cluster_data_connection(
                                    &mut stream,
                                    &config,
                                    &topology,
                                    &service,
                                    control_store.as_ref(),
                                    metadata_consensus.as_ref(),
                                    metadata_distribution_store.as_ref(),
                                    Some(&peer_certificate_sha256),
                                )
                            })
                        } else {
                            let mut stream = stream;
                            handle_cluster_data_connection(
                                &mut stream,
                                &config,
                                &topology,
                                &service,
                                control_store.as_ref(),
                                metadata_consensus.as_ref(),
                                metadata_distribution_store.as_ref(),
                                None,
                            )
                        };
                        if let Err(error) = result {
                            eprintln!("bicdb cluster data RPC error: {error}");
                        }
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => {
                    eprintln!("bicdb cluster data RPC accept error: {error}");
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    });
    Ok(ClusterDataServerHandle {
        local_addr,
        shutdown,
        thread: Some(thread),
    })
}

struct ActiveConnection(Arc<AtomicUsize>);

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn configure_stream(stream: &TcpStream, config: &ClusterNetworkTransportConfig) -> Result<()> {
    let timeout = Some(Duration::from_millis(config.io_timeout_ms));
    stream.set_read_timeout(timeout)?;
    stream.set_write_timeout(timeout)?;
    stream.set_nodelay(true)?;
    Ok(())
}

fn complete_client_tls_handshake(
    stream: &mut StreamOwned<ClientConnection, TcpStream>,
) -> Result<String> {
    while stream.conn.is_handshaking() {
        stream.conn.complete_io(&mut stream.sock)?;
    }
    stream
        .conn
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .map(|certificate| certificate_der_sha256(certificate.as_ref()))
        .ok_or_else(|| transport_error("cluster mTLS server supplied no leaf certificate"))
}

fn verify_destination_certificate(
    destination_node_id: &ClusterNodeId,
    active_sha256: Option<&str>,
    pending_sha256: Option<&str>,
    presented_sha256: Option<&str>,
) -> Result<()> {
    if let Some(active) = active_sha256 {
        if presented_sha256 != Some(active) && presented_sha256 != pending_sha256 {
            return Err(transport_error(format!(
                "cluster destination {destination_node_id} TLS certificate fingerprint does not match membership"
            )));
        }
    }
    Ok(())
}

fn handle_cluster_data_connection(
    stream: &mut (impl Read + Write),
    config: &ClusterNetworkTransportConfig,
    topology: &RwLock<ClusterTopology>,
    service: &Mutex<ClusterDataNodeService>,
    control_store: Option<&Arc<Mutex<DistributionStore>>>,
    metadata_consensus: Option<&Arc<Mutex<MetadataConsensusStore>>>,
    metadata_distribution_store: Option<&Arc<Mutex<DistributionStore>>>,
    peer_certificate_sha256: Option<&str>,
) -> Result<()> {
    let request = recv_json_frame::<ClusterDataRequestEnvelope>(stream, config.max_frame_bytes)?;
    let request_id = request.request_id;
    let caller_node_id = request.caller_node_id.clone();
    let (local_cluster_id, local_node_id) = {
        let service = service.lock();
        (service.cluster_id().clone(), service.node_id().clone())
    };
    let response = (|| {
        if request.protocol_version != CLUSTER_DATA_PROTOCOL_VERSION {
            return Err(transport_error(format!(
                "unsupported cluster data protocol {}; expected {}",
                request.protocol_version, CLUSTER_DATA_PROTOCOL_VERSION
            )));
        }
        if request.cluster_id != local_cluster_id {
            return Err(transport_error(format!(
                "cluster data request for {} reached {}",
                request.cluster_id, local_cluster_id
            )));
        }
        if request.destination_node_id != local_node_id {
            return Err(transport_error(format!(
                "cluster data request for node {} reached {}",
                request.destination_node_id, local_node_id
            )));
        }
        let topology_guard = topology.read();
        let caller = topology_guard.nodes.get(&request.caller_node_id);
        authorize_cluster_data_caller(
            &request.caller_node_id,
            caller,
            &request.request,
            peer_certificate_sha256,
            config.dev_localhost_plaintext,
        )?;
        drop(topology_guard);
        dispatch_cluster_data_request(
            service,
            control_store,
            metadata_consensus,
            metadata_distribution_store,
            topology,
            &caller_node_id,
            request.request,
        )
    })();
    let response = ClusterDataResponseEnvelope {
        protocol_version: CLUSTER_DATA_PROTOCOL_VERSION,
        cluster_id: local_cluster_id,
        source_node_id: local_node_id,
        request_id,
        response: response.map_err(|error| ClusterDataRemoteError {
            code: "cluster_data_operation_failed".to_string(),
            message: error.to_string(),
        }),
    };
    send_json_frame(stream, &response, config.max_frame_bytes)
}

fn authorize_cluster_data_caller(
    caller_node_id: &ClusterNodeId,
    caller: Option<&ClusterNode>,
    request: &ClusterDataRequest,
    peer_certificate_sha256: Option<&str>,
    dev_localhost_plaintext: bool,
) -> Result<()> {
    let self_registration = matches!(
        request,
        ClusterDataRequest::RegisterMetadataLearner { node, .. }
            if node.id == *caller_node_id
    );
    let bootstrap_fetch = matches!(request, ClusterDataRequest::FetchBootstrapSnapshot);
    if caller.is_none() && !self_registration && !bootstrap_fetch {
        return Err(transport_error("cluster data caller is not a member"));
    }
    if caller.is_some_and(|caller| caller.lifecycle == ClusterNodeLifecycle::Decommissioned) {
        return Err(transport_error(
            "decommissioned cluster data caller is fenced",
        ));
    }

    if let Some(caller) = caller.filter(|caller| caller.tls_certificate_sha256.is_some()) {
        let presented = peer_certificate_sha256.ok_or_else(|| {
            transport_error(format!(
                "cluster data caller {caller_node_id} supplied no TLS certificate"
            ))
        })?;
        if !caller.accepts_tls_certificate_sha256(presented) {
            return Err(transport_error(format!(
                "cluster data caller {caller_node_id} TLS certificate fingerprint does not match membership"
            )));
        }
        match request {
            ClusterDataRequest::Heartbeat {
                tls_certificate_sha256,
                ..
            } if tls_certificate_sha256.as_deref() != Some(presented) => {
                return Err(transport_error(
                    "heartbeat TLS fingerprint does not match the presented certificate",
                ));
            }
            ClusterDataRequest::StageTlsCertificateRotation { node_id, .. }
                if node_id != caller_node_id
                    || caller.tls_certificate_sha256.as_deref() != Some(presented) =>
            {
                return Err(transport_error(
                    "TLS certificate rotation must be staged by the node's active certificate",
                ));
            }
            ClusterDataRequest::AbortTlsCertificateRotation { node_id, .. }
                if node_id != caller_node_id =>
            {
                return Err(transport_error(
                    "TLS certificate rotation abort identity does not match the caller",
                ));
            }
            _ => {}
        }
        return Ok(());
    }
    if dev_localhost_plaintext {
        return Ok(());
    }

    if caller.is_none() && self_registration {
        let declared = match request {
            ClusterDataRequest::RegisterMetadataLearner { node, .. } => {
                if node.pending_tls_certificate_sha256.is_some() {
                    return Err(transport_error(
                        "bootstrap learner cannot pre-stage a TLS certificate rotation",
                    ));
                }
                node.tls_certificate_sha256.as_deref()
            }
            _ => None,
        };
        if declared.is_none() || declared != peer_certificate_sha256 {
            return Err(transport_error(
                "bootstrap learner identity is not bound to the presented TLS certificate",
            ));
        }
        return Ok(());
    }
    if caller.is_none() && bootstrap_fetch {
        // Any cluster-CA-authenticated peer may obtain the bounded bootstrap
        // view. It cannot mutate metadata until self-registration binds its
        // exact leaf certificate to its requested node ID.
        return Ok(());
    }

    let declared = match request {
        ClusterDataRequest::Heartbeat {
            tls_certificate_sha256,
            ..
        } => tls_certificate_sha256.as_deref(),
        _ => None,
    };
    if declared.is_some() && declared == peer_certificate_sha256 {
        // Backward-compatible migration for a member written before
        // certificate fingerprints existed. The heartbeat is the only RPC it
        // may issue until the binding is quorum committed and published.
        return Ok(());
    }
    Err(transport_error(format!(
        "cluster data caller {caller_node_id} must bind its presented TLS certificate by heartbeat before issuing cluster RPCs"
    )))
}

fn require_local_schema_compatibility(
    service: &Mutex<ClusterDataNodeService>,
    published_topology: &RwLock<ClusterTopology>,
    range_id: RangeId,
) -> Result<()> {
    let (required, pending_target, advertised, advertised_pending, bootstrap_claim, node_id) = {
        let topology = published_topology.read();
        let range = topology
            .range_by_id(range_id)
            .ok_or_else(|| transport_error(format!("unknown range {range_id}")))?;
        let Some(required) = range.required_schema_sha256(&topology.nodes) else {
            // A topology produced by an older binary has no schema label. It
            // remains readable during a rolling upgrade, but the application
            // host does not open strict write admission until it advertises a
            // local fingerprint.
            return Ok(());
        };
        let node_id = service.lock().node_id().clone();
        let pending_target = topology
            .nodes
            .get(&range.leader)
            .and_then(|node| node.labels.get(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL))
            .cloned();
        let advertised = topology
            .nodes
            .get(&node_id)
            .and_then(|node| node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL))
            .cloned();
        let advertised_pending = topology
            .nodes
            .get(&node_id)
            .and_then(|node| node.labels.get(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL))
            .cloned();
        let bootstrap_claim = topology.nodes.get(&node_id).is_some_and(|node| {
            node.labels
                .get(SCHEMA_BOOTSTRAP_NODE_LABEL)
                .is_some_and(|value| value == "true")
        });
        (
            required.to_string(),
            pending_target,
            advertised,
            advertised_pending,
            bootstrap_claim,
            node_id,
        )
    };
    if advertised.as_deref() != Some(required.as_str()) && !bootstrap_claim {
        return Err(transport_error(format!(
            "range {range_id} schema fence rejected node {node_id}: required {required}, advertised {}",
            advertised.as_deref().unwrap_or("missing")
        )));
    }
    if advertised_pending != pending_target {
        return Err(transport_error(format!(
            "range {range_id} schema fence rejected node {node_id}: pending target advertisement differs from the leader"
        )));
    }
    let service = service.lock();
    if !service.schema_compatibility_allows(&required, pending_target.as_deref()) {
        return Err(transport_error(format!(
            "range {range_id} schema fence rejected node {node_id}: required active {required}, pending {}, local verified {}",
            pending_target.as_deref().unwrap_or("none"),
            service
                .verified_schema_compatibility_sha256()
                .as_deref()
                .unwrap_or("stale or missing")
        )));
    }
    Ok(())
}

fn require_schema_activation_authority(
    topology: &ClusterTopology,
    voters: &BTreeSet<ClusterNodeId>,
    base_fingerprint_sha256: &str,
    target_fingerprint_sha256: &str,
) -> Result<()> {
    let strict_target = voters.iter().all(|voter| {
        topology.nodes.get(voter).is_some_and(|node| {
            node.lifecycle == ClusterNodeLifecycle::Active
                && node.metadata_role == MetadataMemberRole::Voter
                && node
                    .labels
                    .get(SCHEMA_COMPATIBILITY_NODE_LABEL)
                    .map(String::as_str)
                    == Some(target_fingerprint_sha256)
                && !node
                    .labels
                    .contains_key(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
        })
    });
    let additive_window = base_fingerprint_sha256 != target_fingerprint_sha256
        && voters.iter().all(|voter| {
            topology.nodes.get(voter).is_some_and(|node| {
                node.lifecycle == ClusterNodeLifecycle::Active
                    && node.metadata_role == MetadataMemberRole::Voter
                    && node
                        .labels
                        .get(SCHEMA_COMPATIBILITY_NODE_LABEL)
                        .map(String::as_str)
                        == Some(base_fingerprint_sha256)
                    && node
                        .labels
                        .get(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
                        .map(String::as_str)
                        == Some(target_fingerprint_sha256)
            })
        });
    if strict_target || additive_window {
        return Ok(());
    }
    Err(transport_error(
        "cluster schema activation voters do not share the exact committed strict fence or additive compatibility window",
    ))
}

/// Fence a relocation data-plane RPC on the authenticated caller.
///
/// Every range-write RPC is leader-fenced through `validate_range_write`, but
/// the relocation data plane — PrepareLearner, ExportSnapshot, ApplySnapshot,
/// LearnerWatermark, ExportCatchUp, ApplyCatchUp, CleanupSource,
/// CompleteRelocation — reached the service with no caller at all. The
/// service's own `ensure_target` / `ensure_snapshot_source` checks validate the
/// LOCAL node's role in the relocation, never the SENDER's, and every field of
/// a `RangeRelocation` is gossiped topology that any member can replay. So any
/// authenticated member holding a valid certificate could drive forged
/// snapshot and catch-up batches into a live relocation's target, where they
/// are persisted through `bypass_commit_admission` + `write_upserts_unchecked`.
///
/// Relocation is driven from the SOURCE SIDE: `TransportClusterRelocationDriver`
/// runs as the coordinating actor and issues every arm — including the ones
/// handled on the target — so the authenticated caller is always the range
/// leader (which `snapshot_source` resolves to) or the relocation's recorded
/// source. The target is therefore never a legitimate caller, and admitting it
/// would leave a compromised learner able to pull the source's range out
/// through ExportSnapshot / ExportCatchUp, or destroy data on it through
/// CleanupSource.
fn validate_relocation_source_caller(
    caller_node_id: &ClusterNodeId,
    relocation: &RangeRelocation,
    range: Option<&RangeDescriptor>,
) -> Result<()> {
    if relocation.source.as_ref() == Some(caller_node_id)
        || range.is_some_and(|range| range.leader == *caller_node_id)
    {
        return Ok(());
    }
    Err(transport_error(format!(
        "relocation caller {caller_node_id} is not the source of relocation {}",
        relocation.id
    )))
}

/// Authorize the node driving a committed relocation.
///
/// The ordinary controller is the relocation source/range leader. Metadata
/// leadership is independent from range leadership, however, and the
/// quorum-elected metadata leader owns the cluster supervisor. Refusing that
/// leader wedges every relocation whenever those two legitimate authorities
/// live on different nodes. Conversely, accepting any authenticated member
/// would restore the learner-forgery vulnerability this fence was introduced
/// to close.
///
/// A non-source caller is therefore accepted only when it is the current
/// quorum leader and the request is an exact member of the topology generation
/// committed by that consensus state. A stale leader, an uncommitted staged
/// relocation, or a forged range descriptor remains fail-closed.
fn validate_relocation_controller_caller(
    caller_node_id: &ClusterNodeId,
    relocation: &RangeRelocation,
    range: Option<&RangeDescriptor>,
    metadata_consensus: Option<&Arc<Mutex<MetadataConsensusStore>>>,
    published_topology: &RwLock<ClusterTopology>,
) -> Result<()> {
    if validate_relocation_source_caller(caller_node_id, relocation, range).is_ok() {
        return Ok(());
    }

    let consensus = metadata_consensus.ok_or_else(|| {
        transport_error(format!(
            "relocation caller {caller_node_id} is neither its source nor an authenticated metadata controller"
        ))
    })?;
    let status = consensus.lock().status();
    if status.leader_id.as_ref() != Some(caller_node_id) || !status.voters.contains(caller_node_id)
    {
        return Err(transport_error(format!(
            "relocation caller {caller_node_id} is not the current metadata leader {:?}",
            status.leader_id
        )));
    }

    let topology = published_topology.read();
    if status.topology_generation != topology.generation {
        return Err(transport_error(format!(
            "metadata leader {caller_node_id} has consensus topology generation {}, but the data plane has published generation {}",
            status.topology_generation, topology.generation
        )));
    }
    let controller = topology.nodes.get(caller_node_id).ok_or_else(|| {
        transport_error(format!(
            "metadata leader {caller_node_id} is absent from the committed topology"
        ))
    })?;
    if controller.lifecycle != ClusterNodeLifecycle::Active
        || controller.metadata_role != MetadataMemberRole::Voter
    {
        return Err(transport_error(format!(
            "metadata leader {caller_node_id} is not an active metadata voter"
        )));
    }
    let committed_relocation = topology.relocations.get(&relocation.id);
    let exact_committed_request = committed_relocation == Some(relocation);
    let exact_committed_retry = committed_relocation.is_some_and(|committed| {
        let Some(resume_phase) = committed
            .resume_phase
            .filter(|_| committed.phase == crate::distribution::RelocationPhase::Failed)
        else {
            return false;
        };
        let mut authorized_retry = committed.clone();
        authorized_retry.phase = resume_phase;
        authorized_retry.resume_phase = None;
        authorized_retry.last_error = None;
        // Retrying is staged with the current controller timestamp. It is the
        // only field not derived byte-for-byte from the committed failure.
        authorized_retry.updated_at_ms = relocation.updated_at_ms;
        authorized_retry == *relocation
    });
    if !exact_committed_request && !exact_committed_retry {
        return Err(transport_error(format!(
            "metadata leader {caller_node_id} requested relocation {} outside the exact committed topology",
            relocation.id
        )));
    }
    if let Some(range) = range {
        if topology.range_by_id(range.id) != Some(range) {
            return Err(transport_error(format!(
                "metadata leader {caller_node_id} requested a forged or stale descriptor for range {}",
                range.id
            )));
        }
    }
    Ok(())
}

fn dispatch_cluster_data_request(
    service: &Mutex<ClusterDataNodeService>,
    control_store: Option<&Arc<Mutex<DistributionStore>>>,
    metadata_consensus: Option<&Arc<Mutex<MetadataConsensusStore>>>,
    metadata_distribution_store: Option<&Arc<Mutex<DistributionStore>>>,
    published_topology: &RwLock<ClusterTopology>,
    caller_node_id: &ClusterNodeId,
    request: ClusterDataRequest,
) -> Result<ClusterDataResponse> {
    if let Some(range_id) = request.range_id_for_schema_fence() {
        require_local_schema_compatibility(service, published_topology, range_id)?;
    }
    match request {
        ClusterDataRequest::Ping => Ok(ClusterDataResponse::Pong),
        ClusterDataRequest::ExportSchemaBundle {
            range_id,
            range_epoch,
        } => {
            let topology = published_topology.read();
            let range = topology
                .range_by_id(range_id)
                .filter(|range| range.epoch == range_epoch)
                .ok_or_else(|| {
                    transport_error(format!(
                        "unknown or stale range {range_id} epoch {range_epoch}"
                    ))
                })?;
            let local_node = service.lock().node_id().clone();
            if range.leader != local_node {
                return Err(transport_error(format!(
                    "node {local_node} is not schema authority for {range_id} epoch {range_epoch}"
                )));
            }
            drop(topology);
            let db = service.lock().database();
            let bundle = db.read().cluster_schema_bundle()?;
            Ok(ClusterDataResponse::SchemaBundle(bundle))
        }
        ClusterDataRequest::InstallSchemaBundle {
            relocation,
            range,
            bundle,
        } => {
            {
                let topology = published_topology.read();
                let current = topology
                    .range_by_id(range.id)
                    .filter(|current| current.epoch == range.epoch)
                    .ok_or_else(|| {
                        transport_error(format!(
                            "schema bootstrap range {} epoch {} is stale",
                            range.id, range.epoch
                        ))
                    })?;
                let allocated_target = current.replicas.iter().any(|replica| {
                    replica.id == relocation.target_replica_id
                        && replica.node_id == relocation.target
                        && replica.role == crate::distribution::RangeReplicaRole::Learner
                });
                if relocation.range_id != current.id
                    || relocation.learner_epoch != current.epoch
                    || !allocated_target
                {
                    return Err(transport_error(format!(
                        "schema bootstrap relocation {} has no authoritative learner allocation",
                        relocation.id
                    )));
                }
                let required =
                    current
                        .required_schema_sha256(&topology.nodes)
                        .ok_or_else(|| {
                            transport_error(format!(
                                "schema bootstrap range {} has no leader fingerprint",
                                range.id
                            ))
                        })?;
                if bundle.fingerprint.sha256 != required {
                    return Err(transport_error(format!(
                        "schema bootstrap bundle {} does not match range {} leader fingerprint {required}",
                        bundle.fingerprint.sha256, range.id
                    )));
                }
            }
            service
                .lock()
                .install_schema_bundle(&relocation, &range, &bundle)?;
            Ok(ClusterDataResponse::Ack)
        }
        ClusterDataRequest::StageSignedSchemaBundle { signed_bundle } => {
            let consensus = metadata_consensus.ok_or_else(|| {
                transport_error(
                    "signed cluster schema staging requires local metadata consensus authority",
                )
            })?;
            let status = consensus.lock().status();
            if status.leader_id.as_ref() != Some(caller_node_id) {
                return Err(transport_error(format!(
                    "signed cluster schema staging caller {caller_node_id} is not metadata leader {:?}",
                    status.leader_id
                )));
            }
            let local_node_id = service.lock().node_id().clone();
            let topology = published_topology.read();
            let local_node = topology.nodes.get(&local_node_id).ok_or_else(|| {
                transport_error("cluster schema staging destination is not a member")
            })?;
            if local_node.lifecycle != ClusterNodeLifecycle::Active
                || local_node.metadata_role != MetadataMemberRole::Voter
            {
                return Err(transport_error(format!(
                    "cluster schema staging destination {local_node_id} is not an active voter"
                )));
            }
            drop(topology);
            let receipt = service.lock().stage_signed_schema_bundle(
                signed_bundle.clone(),
                crate::distribution::unix_time_ms(),
            )?;
            receipt.validate_for(&signed_bundle)?;
            Ok(ClusterDataResponse::SchemaStage(receipt))
        }
        ClusterDataRequest::AdvanceSignedSchemaActivation {
            rollout_id,
            stage_id,
            base_fingerprint_sha256,
            target_fingerprint_sha256,
        } => {
            let consensus = metadata_consensus.ok_or_else(|| {
                transport_error(
                    "signed cluster schema activation requires local metadata consensus authority",
                )
            })?;
            let status = consensus.lock().status();
            if status.leader_id.as_ref() != Some(caller_node_id) {
                return Err(transport_error(format!(
                    "signed cluster schema activation caller {caller_node_id} is not metadata leader {:?}",
                    status.leader_id
                )));
            }
            let local_node_id = service.lock().node_id().clone();
            let topology = published_topology.read();
            let consensus_voters = status.voters.iter().cloned().collect::<BTreeSet<_>>();
            if status.topology_generation != topology.generation
                || consensus_voters != topology.metadata_voters()
            {
                return Err(transport_error(
                    "signed cluster schema activation requires the exact committed voter topology",
                ));
            }
            require_schema_activation_authority(
                &topology,
                &consensus_voters,
                &base_fingerprint_sha256,
                &target_fingerprint_sha256,
            )?;
            if !consensus_voters.contains(&local_node_id) {
                return Err(transport_error(format!(
                    "cluster schema activation destination {local_node_id} is not a voter"
                )));
            }
            drop(topology);
            let receipt = service.lock().advance_signed_schema_activation(
                rollout_id,
                stage_id,
                &target_fingerprint_sha256,
                crate::distribution::unix_time_ms(),
            )?;
            Ok(ClusterDataResponse::SchemaActivation(receipt))
        }
        ClusterDataRequest::FinalizeSignedSchemaActivation { activation } => {
            let consensus = metadata_consensus.ok_or_else(|| {
                transport_error(
                    "signed cluster schema finalization requires local metadata consensus authority",
                )
            })?;
            let status = consensus.lock().status();
            if status.leader_id.as_ref() != Some(caller_node_id) {
                return Err(transport_error(format!(
                    "signed cluster schema finalization caller {caller_node_id} is not metadata leader {:?}",
                    status.leader_id
                )));
            }
            let local_node_id = service.lock().node_id().clone();
            let topology = published_topology.read();
            let consensus_voters = status.voters.iter().cloned().collect::<BTreeSet<_>>();
            if status.topology_generation != topology.generation
                || consensus_voters != topology.metadata_voters()
            {
                return Err(transport_error(
                    "signed cluster schema finalization requires the exact committed voter topology",
                ));
            }
            for voter in &consensus_voters {
                let node = topology.nodes.get(voter).ok_or_else(|| {
                    transport_error(format!(
                        "cluster schema finalization voter {voter} is absent"
                    ))
                })?;
                if node.lifecycle != ClusterNodeLifecycle::Active
                    || node.metadata_role != MetadataMemberRole::Voter
                    || node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL)
                        != Some(&activation.target_fingerprint_sha256)
                    || node
                        .labels
                        .contains_key(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
                {
                    return Err(transport_error(format!(
                        "cluster schema finalization voter {voter} is not fenced at the completed target"
                    )));
                }
            }
            if !consensus_voters.contains(&local_node_id) {
                return Err(transport_error(format!(
                    "cluster schema finalization destination {local_node_id} is not a voter"
                )));
            }
            drop(topology);
            let receipt = service.lock().finalize_signed_schema_activation(
                activation,
                crate::distribution::unix_time_ms(),
            )?;
            Ok(ClusterDataResponse::SchemaFinalization(receipt))
        }
        ClusterDataRequest::PrepareLearner { relocation } => {
            validate_relocation_controller_caller(
                caller_node_id,
                &relocation,
                None,
                metadata_consensus,
                published_topology,
            )?;
            service.lock().prepare_learner(&relocation)?;
            Ok(ClusterDataResponse::Ack)
        }
        ClusterDataRequest::ExportSnapshot {
            relocation,
            range,
            options,
        } => Ok(ClusterDataResponse::SnapshotStep({
            validate_relocation_controller_caller(
                caller_node_id,
                &relocation,
                Some(&range),
                metadata_consensus,
                published_topology,
            )?;
            service
                .lock()
                .export_snapshot_step(&relocation, &range, &options)?
        })),
        ClusterDataRequest::ApplySnapshot {
            relocation,
            range,
            step,
        } => Ok(ClusterDataResponse::SnapshotProgress({
            validate_relocation_controller_caller(
                caller_node_id,
                &relocation,
                Some(&range),
                metadata_consensus,
                published_topology,
            )?;
            service
                .lock()
                .apply_snapshot_step(&relocation, &range, &step)?
        })),
        ClusterDataRequest::LearnerWatermark { relocation } => {
            validate_relocation_controller_caller(
                caller_node_id,
                &relocation,
                None,
                metadata_consensus,
                published_topology,
            )?;
            Ok(ClusterDataResponse::LearnerWatermark(
                service
                    .lock()
                    .learner_durable_commit_sequence(&relocation)?,
            ))
        }
        ClusterDataRequest::ExportCatchUp {
            range,
            durable_commit_sequence,
            max_commit_frames,
        } => Ok(ClusterDataResponse::CatchUpBatch({
            // This arm carries no relocation, so it cannot take the
            // participant fence; it exports this range's commit frames, and
            // the caller must at least be a replica of the range it is asking
            // for rather than any authenticated member.
            if !range
                .replicas
                .iter()
                .any(|replica| replica.node_id == *caller_node_id)
                && range.leader != *caller_node_id
            {
                return Err(transport_error(format!(
                    "catch-up export caller {caller_node_id} is not a replica of {}",
                    range.id
                )));
            }
            service
                .lock()
                .export_catch_up(&range, durable_commit_sequence, max_commit_frames)?
        })),
        ClusterDataRequest::ApplyCatchUp {
            relocation,
            range,
            batch,
        } => Ok(ClusterDataResponse::CatchUpProgress({
            validate_relocation_controller_caller(
                caller_node_id,
                &relocation,
                Some(&range),
                metadata_consensus,
                published_topology,
            )?;
            service.lock().apply_catch_up(&relocation, &range, &batch)?
        })),
        ClusterDataRequest::CleanupSource {
            relocation,
            range,
            resume_after_key,
            max_records,
        } => Ok(ClusterDataResponse::CleanupProgress({
            validate_relocation_controller_caller(
                caller_node_id,
                &relocation,
                Some(&range),
                metadata_consensus,
                published_topology,
            )?;
            service.lock().cleanup_source_step(
                &relocation,
                &range,
                resume_after_key.as_deref(),
                max_records,
            )?
        })),
        ClusterDataRequest::CompleteRelocation { relocation } => {
            validate_relocation_controller_caller(
                caller_node_id,
                &relocation,
                None,
                metadata_consensus,
                published_topology,
            )?;
            service.lock().complete_relocation(&relocation)?;
            Ok(ClusterDataResponse::Ack)
        }
        ClusterDataRequest::PrepareRangeWrite { command } => {
            let range = published_topology
                .read()
                .range_by_id(command.range_id)
                .cloned()
                .ok_or_else(|| transport_error(format!("unknown range {}", command.range_id)))?;
            Ok(ClusterDataResponse::RangeWrite(
                service
                    .lock()
                    .prepare_range_write(&range, caller_node_id, command)?,
            ))
        }
        ClusterDataRequest::CommitRangeWrite { command } => {
            let range = published_topology
                .read()
                .range_by_id(command.range_id)
                .cloned()
                .ok_or_else(|| transport_error(format!("unknown range {}", command.range_id)))?;
            Ok(ClusterDataResponse::RangeWrite(
                service
                    .lock()
                    .commit_range_write(&range, caller_node_id, &command)?,
            ))
        }
        ClusterDataRequest::CertifyRangeWrite { command } => {
            let range = published_topology
                .read()
                .range_by_id(command.range_id)
                .cloned()
                .ok_or_else(|| transport_error(format!("unknown range {}", command.range_id)))?;
            Ok(ClusterDataResponse::RangeWrite(
                service
                    .lock()
                    .certify_range_write(&range, caller_node_id, &command)?,
            ))
        }
        ClusterDataRequest::ApplyRangeWrite { command } => {
            let range = published_topology
                .read()
                .range_by_id(command.range_id)
                .cloned()
                .ok_or_else(|| transport_error(format!("unknown range {}", command.range_id)))?;
            Ok(ClusterDataResponse::RangeWrite(
                service
                    .lock()
                    .apply_certified_range_write(&range, caller_node_id, &command)?,
            ))
        }
        ClusterDataRequest::AbortRangeWrite { command } => {
            let range = published_topology
                .read()
                .range_by_id(command.range_id)
                .cloned()
                .ok_or_else(|| transport_error(format!("unknown range {}", command.range_id)))?;
            Ok(ClusterDataResponse::RangeWrite(
                service
                    .lock()
                    .abort_range_write(&range, caller_node_id, &command)?,
            ))
        }
        ClusterDataRequest::InstallRangeBackupFence {
            plan_id,
            range_id,
            range_epoch,
            installed_at_ms,
            expires_at_ms,
        } => {
            let range = published_topology
                .read()
                .range_by_id(range_id)
                .filter(|range| range.epoch == range_epoch)
                .cloned()
                .ok_or_else(|| {
                    transport_error(format!(
                        "unknown or stale range {range_id} epoch {range_epoch}"
                    ))
                })?;
            Ok(ClusterDataResponse::RangeWriteProgress(
                service.lock().install_range_backup_fence(
                    &range,
                    caller_node_id,
                    plan_id,
                    installed_at_ms,
                    expires_at_ms,
                )?,
            ))
        }
        ClusterDataRequest::ReleaseRangeBackupFence {
            plan_id,
            range_id,
            range_epoch,
        } => {
            let range = published_topology
                .read()
                .range_by_id(range_id)
                .filter(|range| range.epoch == range_epoch)
                .cloned()
                .ok_or_else(|| {
                    transport_error(format!(
                        "unknown or stale range {range_id} epoch {range_epoch}"
                    ))
                })?;
            Ok(ClusterDataResponse::RangeBackupFenceReleased(
                service
                    .lock()
                    .release_range_backup_fence(&range, caller_node_id, plan_id)?,
            ))
        }
        ClusterDataRequest::RangeWriteProgress {
            range_id,
            range_epoch,
        } => {
            let range = published_topology
                .read()
                .range_by_id(range_id)
                .filter(|range| range.epoch == range_epoch)
                .cloned()
                .ok_or_else(|| {
                    transport_error(format!(
                        "unknown or stale range {range_id} epoch {range_epoch}"
                    ))
                })?;
            Ok(ClusterDataResponse::RangeWriteProgress(
                service
                    .lock()
                    .range_write_progress_for_peer(&range, caller_node_id)?,
            ))
        }
        ClusterDataRequest::ApplyRangeWriteRepair { batch, limits } => {
            let range = published_topology
                .read()
                .range_by_id(batch.range_id)
                .filter(|range| range.epoch == batch.range_epoch)
                .cloned()
                .ok_or_else(|| {
                    transport_error(format!(
                        "unknown or stale range {} epoch {}",
                        batch.range_id, batch.range_epoch
                    ))
                })?;
            Ok(ClusterDataResponse::RangeWriteProgress(
                service
                    .lock()
                    .apply_range_write_repair(&range, caller_node_id, &batch, &limits)?,
            ))
        }
        ClusterDataRequest::FetchRangeWriteRepair {
            range_id,
            range_epoch,
            previous_resolved_index,
            limits,
        } => {
            let range = published_topology
                .read()
                .range_by_id(range_id)
                .filter(|range| range.epoch == range_epoch)
                .cloned()
                .ok_or_else(|| {
                    transport_error(format!(
                        "unknown or stale range {range_id} epoch {range_epoch}"
                    ))
                })?;
            Ok(ClusterDataResponse::RangeWriteRepair(
                service.lock().export_range_write_repair(
                    &range,
                    caller_node_id,
                    previous_resolved_index,
                    &limits,
                )?,
            ))
        }
        ClusterDataRequest::ProbeRangeWrite {
            range_id,
            range_epoch,
            index,
        } => {
            let range = published_topology
                .read()
                .range_by_id(range_id)
                .filter(|range| range.epoch == range_epoch)
                .cloned()
                .ok_or_else(|| {
                    transport_error(format!(
                        "unknown or stale range {range_id} epoch {range_epoch}"
                    ))
                })?;
            Ok(ClusterDataResponse::RangeWriteProbe(
                service
                    .lock()
                    .probe_range_write_for_peer(&range, caller_node_id, index)?,
            ))
        }
        ClusterDataRequest::AdvanceRangeDigest {
            range_id,
            range_epoch,
            session_id,
            expected_checksum_sha256,
            limits,
        } => {
            let range = published_topology
                .read()
                .range_by_id(range_id)
                .filter(|range| range.epoch == range_epoch)
                .cloned()
                .ok_or_else(|| {
                    transport_error(format!(
                        "unknown or stale range {range_id} epoch {range_epoch}"
                    ))
                })?;
            Ok(ClusterDataResponse::RangeDigestState(
                service.lock().advance_persisted_range_digest(
                    &range,
                    caller_node_id,
                    session_id,
                    expected_checksum_sha256.as_deref(),
                    &limits,
                    crate::distribution::unix_time_ms(),
                )?,
            ))
        }
        ClusterDataRequest::ExportRangeDigestBucket {
            range_id,
            range_epoch,
            session_id,
            bucket,
            resume_after_key,
            limits,
        } => {
            let range = published_topology
                .read()
                .range_by_id(range_id)
                .filter(|range| range.epoch == range_epoch)
                .cloned()
                .ok_or_else(|| {
                    transport_error(format!(
                        "unknown or stale range {range_id} epoch {range_epoch}"
                    ))
                })?;
            Ok(ClusterDataResponse::RangeDigestBucket(
                service.lock().export_range_digest_bucket_step(
                    &range,
                    caller_node_id,
                    session_id,
                    bucket,
                    resume_after_key.as_deref(),
                    &limits,
                    crate::distribution::unix_time_ms(),
                )?,
            ))
        }
        ClusterDataRequest::ApplyRangeDigestRepair {
            range_id,
            range_epoch,
            batch,
            limits,
        } => {
            let range = published_topology
                .read()
                .range_by_id(range_id)
                .filter(|range| range.epoch == range_epoch)
                .cloned()
                .ok_or_else(|| {
                    transport_error(format!(
                        "unknown or stale range {range_id} epoch {range_epoch}"
                    ))
                })?;
            Ok(ClusterDataResponse::RangeDigestRepairState(
                service.lock().apply_range_digest_repair_batch(
                    &range,
                    caller_node_id,
                    &batch,
                    &limits,
                    crate::distribution::unix_time_ms(),
                )?,
            ))
        }
        ClusterDataRequest::Heartbeat {
            node_id,
            incarnation,
            used_bytes,
            capacity_bytes,
            labels,
            tls_certificate_sha256,
            now_ms,
        } => {
            if &node_id != caller_node_id {
                return Err(transport_error(
                    "heartbeat node identity does not match authenticated caller",
                ));
            }
            if let (Some(consensus), Some(distribution_store)) =
                (metadata_consensus, metadata_distribution_store)
            {
                let status = consensus.lock().status();
                if status.role != crate::distribution_consensus::MetadataConsensusRole::Leader {
                    return Err(transport_error(format!(
                        "cluster node is not the metadata leader; current leader is {}",
                        status
                            .leader_id
                            .as_ref()
                            .map(ClusterNodeId::as_str)
                            .unwrap_or("unknown")
                    )));
                }
                distribution_store.lock().queue_heartbeat_with_certificate(
                    &node_id,
                    incarnation,
                    used_bytes,
                    capacity_bytes,
                    labels,
                    tls_certificate_sha256,
                    now_ms,
                )?;
                return Ok(ClusterDataResponse::Topology(
                    consensus.lock().committed_topology().clone(),
                ));
            }
            let store = control_store.ok_or_else(|| {
                transport_error("cluster node is not the active metadata controller")
            })?;
            let mut store = store.lock();
            store.heartbeat_with_certificate(
                &node_id,
                incarnation,
                used_bytes,
                capacity_bytes,
                labels,
                tls_certificate_sha256,
                now_ms,
            )?;
            Ok(ClusterDataResponse::Topology(store.topology().clone()))
        }
        ClusterDataRequest::StageTlsCertificateRotation {
            node_id,
            next_tls_certificate_sha256,
            now_ms,
        } => {
            if &node_id != caller_node_id {
                return Err(transport_error(
                    "TLS certificate rotation identity does not match authenticated caller",
                ));
            }
            if let (Some(consensus), Some(distribution_store)) =
                (metadata_consensus, metadata_distribution_store)
            {
                let status = consensus.lock().status();
                if status.role != crate::distribution_consensus::MetadataConsensusRole::Leader {
                    return Err(transport_error(format!(
                        "cluster node is not the metadata leader; current leader is {}",
                        status
                            .leader_id
                            .as_ref()
                            .map(ClusterNodeId::as_str)
                            .unwrap_or("unknown")
                    )));
                }
                distribution_store.lock().queue_tls_certificate_rotation(
                    &node_id,
                    next_tls_certificate_sha256,
                    &node_id,
                    now_ms,
                )?;
                return Ok(ClusterDataResponse::Topology(
                    consensus.lock().committed_topology().clone(),
                ));
            }
            let store = control_store.ok_or_else(|| {
                transport_error("cluster node is not the active metadata controller")
            })?;
            let mut store = store.lock();
            store.stage_tls_certificate_rotation(
                &node_id,
                next_tls_certificate_sha256,
                &node_id,
                now_ms,
            )?;
            Ok(ClusterDataResponse::Topology(store.topology().clone()))
        }
        ClusterDataRequest::AbortTlsCertificateRotation { node_id, now_ms } => {
            if &node_id != caller_node_id {
                return Err(transport_error(
                    "TLS certificate rotation abort identity does not match authenticated caller",
                ));
            }
            if let (Some(consensus), Some(distribution_store)) =
                (metadata_consensus, metadata_distribution_store)
            {
                let status = consensus.lock().status();
                if status.role != crate::distribution_consensus::MetadataConsensusRole::Leader {
                    return Err(transport_error(format!(
                        "cluster node is not the metadata leader; current leader is {}",
                        status
                            .leader_id
                            .as_ref()
                            .map(ClusterNodeId::as_str)
                            .unwrap_or("unknown")
                    )));
                }
                distribution_store
                    .lock()
                    .queue_tls_certificate_rotation_abort(&node_id, &node_id, now_ms)?;
                return Ok(ClusterDataResponse::Topology(
                    consensus.lock().committed_topology().clone(),
                ));
            }
            let store = control_store.ok_or_else(|| {
                transport_error("cluster node is not the active metadata controller")
            })?;
            let mut store = store.lock();
            store.abort_tls_certificate_rotation(&node_id, &node_id, now_ms)?;
            Ok(ClusterDataResponse::Topology(store.topology().clone()))
        }
        ClusterDataRequest::RegisterMetadataLearner { node, now_ms } => {
            if node.metadata_role != MetadataMemberRole::Learner {
                return Err(transport_error(
                    "metadata registration requires a learner node",
                ));
            }
            if &node.id != caller_node_id {
                let topology = published_topology.read();
                if !topology.is_metadata_voter(caller_node_id) {
                    return Err(transport_error(
                        "a bootstrap caller may register only its own learner identity",
                    ));
                }
            }
            let consensus = metadata_consensus
                .ok_or_else(|| transport_error("metadata consensus is not active on this node"))?;
            let (committed, actor) = {
                let consensus = consensus.lock();
                let status = consensus.status();
                if status.role != crate::distribution_consensus::MetadataConsensusRole::Leader {
                    return Err(transport_error(format!(
                        "cluster node is not the metadata leader; current leader is {}",
                        status
                            .leader_id
                            .as_ref()
                            .map(ClusterNodeId::as_str)
                            .unwrap_or("unknown")
                    )));
                }
                (consensus.committed_topology().clone(), status.node_id)
            };
            let store = metadata_distribution_store.ok_or_else(|| {
                transport_error("metadata distribution store is not active on this node")
            })?;
            store.lock().queue_metadata_learner(node, &actor, now_ms)?;
            Ok(ClusterDataResponse::Topology(committed))
        }
        ClusterDataRequest::PromoteMetadataLearner {
            node_id,
            installed_commit_index,
            installed_topology_generation,
            now_ms,
        } => {
            if &node_id != caller_node_id {
                return Err(transport_error(
                    "metadata promotion node identity does not match authenticated caller",
                ));
            }
            let consensus = metadata_consensus
                .ok_or_else(|| transport_error("metadata consensus is not active on this node"))?;
            let committed = {
                let consensus = consensus.lock();
                let status = consensus.status();
                if status.role != crate::distribution_consensus::MetadataConsensusRole::Leader {
                    return Err(transport_error(format!(
                        "cluster node is not the metadata leader; current leader is {}",
                        status
                            .leader_id
                            .as_ref()
                            .map(ClusterNodeId::as_str)
                            .unwrap_or("unknown")
                    )));
                }
                let topology = consensus.committed_topology();
                let node = topology.nodes.get(&node_id).ok_or_else(|| {
                    transport_error(format!("unknown metadata learner {node_id}"))
                })?;
                if node.metadata_role == MetadataMemberRole::Voter {
                    return Ok(ClusterDataResponse::Topology(topology.clone()));
                }
                if node.metadata_role != MetadataMemberRole::Learner {
                    return Err(transport_error(format!(
                        "cluster node {node_id} is not a metadata learner"
                    )));
                }
                if installed_commit_index < status.commit_index
                    || installed_topology_generation != status.topology_generation
                {
                    return Err(transport_error(format!(
                        "metadata learner {node_id} is not caught up: installed commit/generation \
                         {installed_commit_index}/{installed_topology_generation}, leader \
                         {}/{}",
                        status.commit_index, status.topology_generation
                    )));
                }
                topology.clone()
            };
            let store = metadata_distribution_store.ok_or_else(|| {
                transport_error("metadata distribution store is not active on this node")
            })?;
            store
                .lock()
                .queue_metadata_promotion(&node_id, caller_node_id, now_ms)?;
            Ok(ClusterDataResponse::Topology(committed))
        }
        ClusterDataRequest::FetchBootstrapSnapshot => {
            let consensus = metadata_consensus
                .ok_or_else(|| transport_error("metadata consensus is not active on this node"))?;
            let (topology, metadata_leader_id) = {
                let consensus = consensus.lock();
                let status = consensus.status();
                let leader = if status.role
                    == crate::distribution_consensus::MetadataConsensusRole::Leader
                {
                    Some(status.node_id)
                } else {
                    status.leader_id
                };
                (consensus.committed_topology().clone(), leader)
            };
            let store = metadata_distribution_store.ok_or_else(|| {
                transport_error("metadata distribution store is not active on this node")
            })?;
            let snapshot = ClusterBootstrapSnapshot {
                topology,
                config_template: store.lock().config().clone(),
                metadata_leader_id,
            };
            snapshot.validate()?;
            Ok(ClusterDataResponse::BootstrapSnapshot(snapshot))
        }
        ClusterDataRequest::FetchTopology => {
            if let Some(consensus) = metadata_consensus {
                let consensus = consensus.lock();
                let status = consensus.status();
                if status.role != crate::distribution_consensus::MetadataConsensusRole::Leader {
                    return Err(transport_error(format!(
                        "cluster node is not the metadata leader; current leader is {}",
                        status
                            .leader_id
                            .as_ref()
                            .map(ClusterNodeId::as_str)
                            .unwrap_or("unknown")
                    )));
                }
                return Ok(ClusterDataResponse::Topology(
                    consensus.committed_topology().clone(),
                ));
            }
            let store = control_store.ok_or_else(|| {
                transport_error("cluster node is not the active metadata controller")
            })?;
            Ok(ClusterDataResponse::Topology(
                store.lock().topology().clone(),
            ))
        }
        ClusterDataRequest::MetadataVote { request } => {
            if &request.candidate_id != caller_node_id {
                return Err(transport_error(
                    "metadata vote candidate does not match authenticated caller",
                ));
            }
            let consensus = metadata_consensus
                .ok_or_else(|| transport_error("metadata consensus is not active on this node"))?;
            Ok(ClusterDataResponse::MetadataVote(
                consensus.lock().handle_vote_request(request)?,
            ))
        }
        ClusterDataRequest::MetadataAppend { request } => {
            if &request.leader_id != caller_node_id {
                return Err(transport_error(
                    "metadata append leader does not match authenticated caller",
                ));
            }
            let consensus = metadata_consensus
                .ok_or_else(|| transport_error("metadata consensus is not active on this node"))?;
            let (response, committed, commit_advanced) = {
                let mut consensus = consensus.lock();
                let commit_before = consensus.status().commit_index;
                let response = consensus.handle_append_request(request)?;
                let committed = consensus.committed_topology().clone();
                let commit_advanced = consensus.status().commit_index > commit_before;
                (response, committed, commit_advanced)
            };
            if response.success && commit_advanced {
                if let Some(store) = metadata_distribution_store {
                    store
                        .lock()
                        .install_authoritative_topology(committed.clone())?;
                }
                let mut published = published_topology.write();
                if committed.generation > published.generation {
                    *published = committed;
                } else if committed.generation == published.generation && committed != *published {
                    return Err(transport_error(
                        "metadata commit conflicts with published topology generation",
                    ));
                }
            }
            Ok(ClusterDataResponse::MetadataAppend(response))
        }
    }
}

/// Restricted client used by an empty server before its identity appears in
/// cluster topology. The server accepts only bootstrap snapshot fetches and a
/// self-registration as a metadata learner from this caller.
pub struct TcpClusterBootstrapClient {
    cluster_id: ClusterId,
    caller_node_id: ClusterNodeId,
    config: ClusterNetworkTransportConfig,
    client_tls: Option<ReplicationClientTls>,
    client_certificate_sha256: Option<String>,
    trusted_destination_fingerprints: RwLock<BTreeMap<ClusterNodeId, (String, Option<String>)>>,
    next_request_id: AtomicU64,
}

impl std::fmt::Debug for TcpClusterBootstrapClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TcpClusterBootstrapClient")
            .field("cluster_id", &self.cluster_id)
            .field("caller_node_id", &self.caller_node_id)
            .field("config", &self.config)
            .field("tls_configured", &self.client_tls.is_some())
            .finish_non_exhaustive()
    }
}

impl TcpClusterBootstrapClient {
    pub fn new(
        cluster_id: ClusterId,
        caller_node_id: ClusterNodeId,
        config: ClusterNetworkTransportConfig,
    ) -> Result<Self> {
        config.validate()?;
        let client_tls = if config.dev_localhost_plaintext {
            None
        } else {
            Some(build_client_tls(
                config
                    .tls
                    .as_ref()
                    .expect("validated cluster TLS configuration"),
            )?)
        };
        let client_certificate_sha256 = if client_tls.is_some() {
            Some(replication_certificate_sha256(
                &config
                    .tls
                    .as_ref()
                    .expect("validated cluster TLS configuration")
                    .cert_path,
            )?)
        } else {
            None
        };
        Ok(Self {
            cluster_id,
            caller_node_id,
            config,
            client_tls,
            client_certificate_sha256,
            trusted_destination_fingerprints: RwLock::new(BTreeMap::new()),
            next_request_id: AtomicU64::new(1),
        })
    }

    pub fn fetch_snapshot(
        &self,
        destination_node_id: &ClusterNodeId,
        destination_address: &str,
    ) -> Result<ClusterBootstrapSnapshot> {
        let (response, peer_certificate_sha256) = self.call(
            destination_node_id,
            destination_address,
            ClusterDataRequest::FetchBootstrapSnapshot,
        )?;
        match response {
            ClusterDataResponse::BootstrapSnapshot(snapshot) => {
                snapshot.validate()?;
                if snapshot.topology.cluster_id != self.cluster_id {
                    return Err(transport_error(
                        "bootstrap snapshot belongs to another cluster",
                    ));
                }
                let destination = snapshot
                    .topology
                    .nodes
                    .get(destination_node_id)
                    .ok_or_else(|| {
                        transport_error(format!(
                            "bootstrap snapshot omits destination node {destination_node_id}"
                        ))
                    })?;
                verify_destination_certificate(
                    destination_node_id,
                    destination.tls_certificate_sha256.as_deref(),
                    destination.pending_tls_certificate_sha256.as_deref(),
                    peer_certificate_sha256.as_deref(),
                )?;
                let mut trusted = self.trusted_destination_fingerprints.write();
                for (node_id, node) in &snapshot.topology.nodes {
                    if let Some(fingerprint) = &node.tls_certificate_sha256 {
                        trusted.insert(
                            node_id.clone(),
                            (
                                fingerprint.clone(),
                                node.pending_tls_certificate_sha256.clone(),
                            ),
                        );
                    }
                }
                Ok(snapshot)
            }
            response => Err(unexpected_response("bootstrap snapshot", response)),
        }
    }

    pub fn register_metadata_learner(
        &self,
        destination_node_id: &ClusterNodeId,
        destination_address: &str,
        node: ClusterNode,
        now_ms: u64,
    ) -> Result<ClusterTopology> {
        if node.id != self.caller_node_id || node.metadata_role != MetadataMemberRole::Learner {
            return Err(transport_error(
                "bootstrap client may register only its own learner identity",
            ));
        }
        if self.client_certificate_sha256.is_some()
            && node.tls_certificate_sha256.as_ref() != self.client_certificate_sha256.as_ref()
        {
            return Err(transport_error(
                "bootstrap learner fingerprint does not match the client certificate",
            ));
        }
        if node.pending_tls_certificate_sha256.is_some() {
            return Err(transport_error(
                "bootstrap learner cannot pre-stage a TLS certificate rotation",
            ));
        }
        match self
            .call(
                destination_node_id,
                destination_address,
                ClusterDataRequest::RegisterMetadataLearner { node, now_ms },
            )?
            .0
        {
            ClusterDataResponse::Topology(topology) => Ok(topology),
            response => Err(unexpected_response("topology", response)),
        }
    }

    fn call(
        &self,
        destination_node_id: &ClusterNodeId,
        destination_address: &str,
        request: ClusterDataRequest,
    ) -> Result<(ClusterDataResponse, Option<String>)> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let envelope = ClusterDataRequestEnvelope {
            protocol_version: CLUSTER_DATA_PROTOCOL_VERSION,
            cluster_id: self.cluster_id.clone(),
            caller_node_id: self.caller_node_id.clone(),
            destination_node_id: destination_node_id.clone(),
            request_id,
            request,
        };
        let mut addrs = destination_address.to_socket_addrs()?;
        let address_resolved = addrs.next().ok_or_else(|| {
            transport_error(format!(
                "cluster address `{destination_address}` resolved empty"
            ))
        })?;
        if self.config.dev_localhost_plaintext && !address_resolved.ip().is_loopback() {
            return Err(transport_error(
                "plaintext cluster bootstrap RPC may connect only to loopback",
            ));
        }
        let stream = TcpStream::connect_timeout(
            &address_resolved,
            Duration::from_millis(self.config.connect_timeout_ms),
        )?;
        configure_stream(&stream, &self.config)?;
        let expected_certificate_sha256 = self
            .trusted_destination_fingerprints
            .read()
            .get(destination_node_id)
            .cloned();
        let (response, peer_certificate_sha256) = if let Some(tls) = self.client_tls.as_ref() {
            let server_name = server_name_from_address(destination_address)?;
            let mut stream = client_tls_stream(Arc::clone(&tls.config), &server_name, stream)?;
            let peer_certificate_sha256 = complete_client_tls_handshake(&mut stream)?;
            verify_destination_certificate(
                destination_node_id,
                expected_certificate_sha256
                    .as_ref()
                    .map(|(active, _)| active.as_str()),
                expected_certificate_sha256
                    .as_ref()
                    .and_then(|(_, pending)| pending.as_deref()),
                Some(&peer_certificate_sha256),
            )?;
            send_json_frame(&mut stream, &envelope, self.config.max_frame_bytes)?;
            (
                recv_json_frame::<ClusterDataResponseEnvelope>(
                    &mut stream,
                    self.config.max_frame_bytes,
                )?,
                Some(peer_certificate_sha256),
            )
        } else {
            let mut stream = stream;
            verify_destination_certificate(
                destination_node_id,
                expected_certificate_sha256
                    .as_ref()
                    .map(|(active, _)| active.as_str()),
                expected_certificate_sha256
                    .as_ref()
                    .and_then(|(_, pending)| pending.as_deref()),
                None,
            )?;
            send_json_frame(&mut stream, &envelope, self.config.max_frame_bytes)?;
            (
                recv_json_frame::<ClusterDataResponseEnvelope>(
                    &mut stream,
                    self.config.max_frame_bytes,
                )?,
                None,
            )
        };
        if response.protocol_version != CLUSTER_DATA_PROTOCOL_VERSION
            || response.cluster_id != self.cluster_id
            || response.source_node_id != *destination_node_id
            || response.request_id != request_id
        {
            return Err(transport_error(
                "cluster bootstrap RPC response identity or protocol mismatch",
            ));
        }
        response
            .response
            .map(|response| (response, peer_certificate_sha256))
            .map_err(|remote| {
                transport_error(format!(
                    "cluster node {destination_node_id} rejected bootstrap RPC {}: {}",
                    remote.code, remote.message
                ))
            })
    }
}

pub struct TcpClusterRelocationTransport {
    cluster_id: ClusterId,
    caller_node_id: ClusterNodeId,
    topology: Arc<RwLock<ClusterTopology>>,
    config: ClusterNetworkTransportConfig,
    client_tls: Option<ReplicationClientTls>,
    client_certificate_sha256: Option<String>,
    next_request_id: AtomicU64,
}

impl std::fmt::Debug for TcpClusterRelocationTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TcpClusterRelocationTransport")
            .field("cluster_id", &self.cluster_id)
            .field("caller_node_id", &self.caller_node_id)
            .field("config", &self.config)
            .field("tls_configured", &self.client_tls.is_some())
            .finish_non_exhaustive()
    }
}

impl ClusterSchemaStageTransport for TcpClusterRelocationTransport {
    fn stage_signed_schema_bundle(
        &mut self,
        node_id: &ClusterNodeId,
        signed_bundle: &SignedClusterSchemaBundle,
    ) -> Result<ClusterSchemaStageReceipt> {
        TcpClusterRelocationTransport::stage_signed_schema_bundle(
            self,
            node_id,
            signed_bundle.clone(),
        )
    }
}

impl ClusterSchemaActivationTransport for TcpClusterRelocationTransport {
    fn advance_signed_schema_activation(
        &mut self,
        node_id: &ClusterNodeId,
        rollout_id: Uuid,
        stage_id: Uuid,
        base_fingerprint_sha256: &str,
        target_fingerprint_sha256: &str,
    ) -> Result<ClusterSchemaActivationReceipt> {
        TcpClusterRelocationTransport::advance_signed_schema_activation(
            self,
            node_id,
            rollout_id,
            stage_id,
            base_fingerprint_sha256.to_string(),
            target_fingerprint_sha256.to_string(),
        )
    }
}

impl ClusterSchemaFinalizationTransport for TcpClusterRelocationTransport {
    fn finalize_signed_schema_activation(
        &mut self,
        node_id: &ClusterNodeId,
        activation: &ClusterSchemaActivationReceipt,
    ) -> Result<ClusterSchemaFinalizationReceipt> {
        TcpClusterRelocationTransport::finalize_signed_schema_activation(
            self,
            node_id,
            activation.clone(),
        )
    }
}

impl TcpClusterRelocationTransport {
    pub fn from_topology(
        cluster_id: ClusterId,
        caller_node_id: ClusterNodeId,
        topology: ClusterTopology,
        config: ClusterNetworkTransportConfig,
    ) -> Result<Self> {
        Self::new(
            cluster_id,
            caller_node_id,
            Arc::new(RwLock::new(topology)),
            config,
        )
    }

    pub fn new(
        cluster_id: ClusterId,
        caller_node_id: ClusterNodeId,
        topology: Arc<RwLock<ClusterTopology>>,
        config: ClusterNetworkTransportConfig,
    ) -> Result<Self> {
        config.validate()?;
        let client_certificate_sha256 = if config.dev_localhost_plaintext {
            None
        } else {
            Some(replication_certificate_sha256(
                &config
                    .tls
                    .as_ref()
                    .expect("validated cluster TLS configuration")
                    .cert_path,
            )?)
        };
        {
            let topology = topology.read();
            topology.validate()?;
            if topology.cluster_id != cluster_id {
                return Err(transport_error(format!(
                    "transport cluster {} does not match topology {}",
                    cluster_id, topology.cluster_id
                )));
            }
            let caller = topology.nodes.get(&caller_node_id).ok_or_else(|| {
                transport_error(format!(
                    "transport caller {caller_node_id} is not a cluster member"
                ))
            })?;
            if caller.tls_certificate_sha256.is_some()
                && !client_certificate_sha256
                    .as_deref()
                    .is_some_and(|fingerprint| caller.accepts_tls_certificate_sha256(fingerprint))
            {
                return Err(transport_error(format!(
                    "transport caller {caller_node_id} certificate does not match membership"
                )));
            }
        }
        let client_tls = if config.dev_localhost_plaintext {
            None
        } else {
            Some(build_client_tls(
                config
                    .tls
                    .as_ref()
                    .expect("validated cluster TLS configuration"),
            )?)
        };
        Ok(Self {
            cluster_id,
            caller_node_id,
            topology,
            config,
            client_tls,
            client_certificate_sha256,
            next_request_id: AtomicU64::new(1),
        })
    }

    pub fn install_topology(&self, topology: ClusterTopology) -> Result<bool> {
        topology.validate()?;
        if topology.cluster_id != self.cluster_id {
            return Err(transport_error("refusing topology from another cluster"));
        }
        let mut installed = self.topology.write();
        if topology.generation < installed.generation {
            return Ok(false);
        }
        if topology.generation == installed.generation {
            if topology != *installed {
                return Err(transport_error(
                    "conflicting topology at the same generation",
                ));
            }
            return Ok(false);
        }
        *installed = topology;
        Ok(true)
    }

    pub fn ping(&self, node_id: &ClusterNodeId) -> Result<()> {
        match self.call(node_id, ClusterDataRequest::Ping)? {
            ClusterDataResponse::Pong => Ok(()),
            response => Err(unexpected_response("pong", response)),
        }
    }

    /// Transfer one signed schema stage over the member-bound cluster RPC.
    /// Only the current metadata leader is accepted by the destination, and
    /// the destination verifies the signature using its own host trust store.
    pub fn stage_signed_schema_bundle(
        &self,
        node_id: &ClusterNodeId,
        signed_bundle: SignedClusterSchemaBundle,
    ) -> Result<ClusterSchemaStageReceipt> {
        signed_bundle.bundle.verify()?;
        {
            let topology = self.topology.read();
            let node = topology.nodes.get(node_id).ok_or_else(|| {
                transport_error(format!(
                    "cluster schema staging destination {node_id} is not a member"
                ))
            })?;
            if node.lifecycle != ClusterNodeLifecycle::Active
                || node.metadata_role != MetadataMemberRole::Voter
            {
                return Err(transport_error(format!(
                    "cluster schema staging destination {node_id} is not an active voter"
                )));
            }
        }
        match self.call(
            node_id,
            ClusterDataRequest::StageSignedSchemaBundle {
                signed_bundle: signed_bundle.clone(),
            },
        )? {
            ClusterDataResponse::SchemaStage(receipt) => {
                receipt.validate_for(&signed_bundle)?;
                Ok(receipt)
            }
            response => Err(unexpected_response(
                "cluster schema stage receipt",
                response,
            )),
        }
    }

    /// Advance one bounded local activation step after metadata consensus has
    /// published the all-voter target-digest fence.
    pub fn advance_signed_schema_activation(
        &self,
        node_id: &ClusterNodeId,
        rollout_id: Uuid,
        stage_id: Uuid,
        base_fingerprint_sha256: String,
        target_fingerprint_sha256: String,
    ) -> Result<ClusterSchemaActivationReceipt> {
        if rollout_id.is_nil() || stage_id.is_nil() {
            return Err(transport_error(
                "cluster schema activation request identity is invalid",
            ));
        }
        {
            let topology = self.topology.read();
            if !topology.nodes.contains_key(node_id) {
                return Err(transport_error(format!(
                    "cluster schema activation destination {node_id} is not a member"
                )));
            }
            let voters = topology.metadata_voters();
            if !voters.contains(node_id) {
                return Err(transport_error(format!(
                    "cluster schema activation destination {node_id} is not a voter"
                )));
            }
            require_schema_activation_authority(
                &topology,
                &voters,
                &base_fingerprint_sha256,
                &target_fingerprint_sha256,
            )?;
        }
        match self.call(
            node_id,
            ClusterDataRequest::AdvanceSignedSchemaActivation {
                rollout_id,
                stage_id,
                base_fingerprint_sha256,
                target_fingerprint_sha256,
            },
        )? {
            ClusterDataResponse::SchemaActivation(receipt) => Ok(receipt),
            response => Err(unexpected_response(
                "cluster schema activation receipt",
                response,
            )),
        }
    }

    /// Verify a node's exact completion receipt, preserve a compact durable
    /// finalization proof, and remove its signed stage and activation cursor.
    pub fn finalize_signed_schema_activation(
        &self,
        node_id: &ClusterNodeId,
        activation: ClusterSchemaActivationReceipt,
    ) -> Result<ClusterSchemaFinalizationReceipt> {
        activation.validate_completion_for_node(node_id)?;
        {
            let topology = self.topology.read();
            let node = topology.nodes.get(node_id).ok_or_else(|| {
                transport_error(format!(
                    "cluster schema finalization destination {node_id} is not a member"
                ))
            })?;
            if node.lifecycle != ClusterNodeLifecycle::Active
                || node.metadata_role != MetadataMemberRole::Voter
                || node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL)
                    != Some(&activation.target_fingerprint_sha256)
                || node
                    .labels
                    .contains_key(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
            {
                return Err(transport_error(format!(
                    "cluster schema finalization destination {node_id} is not fenced at the target"
                )));
            }
        }
        match self.call(
            node_id,
            ClusterDataRequest::FinalizeSignedSchemaActivation {
                activation: activation.clone(),
            },
        )? {
            ClusterDataResponse::SchemaFinalization(receipt) => {
                receipt.validate_for(node_id, &activation)?;
                Ok(receipt)
            }
            response => Err(unexpected_response(
                "cluster schema finalization receipt",
                response,
            )),
        }
    }

    pub fn heartbeat(
        &self,
        controller_node_id: &ClusterNodeId,
        incarnation: u64,
        used_bytes: u64,
        capacity_bytes: u64,
        labels: BTreeMap<String, String>,
        now_ms: u64,
    ) -> Result<ClusterTopology> {
        match self.call(
            controller_node_id,
            ClusterDataRequest::Heartbeat {
                node_id: self.caller_node_id.clone(),
                incarnation,
                used_bytes,
                capacity_bytes,
                labels,
                tls_certificate_sha256: self.client_certificate_sha256.clone(),
                now_ms,
            },
        )? {
            ClusterDataResponse::Topology(topology) => Ok(topology),
            response => Err(unexpected_response("topology", response)),
        }
    }

    pub fn stage_tls_certificate_rotation(
        &self,
        leader_node_id: &ClusterNodeId,
        next_tls_certificate_sha256: String,
        now_ms: u64,
    ) -> Result<ClusterTopology> {
        match self.call(
            leader_node_id,
            ClusterDataRequest::StageTlsCertificateRotation {
                node_id: self.caller_node_id.clone(),
                next_tls_certificate_sha256,
                now_ms,
            },
        )? {
            ClusterDataResponse::Topology(topology) => Ok(topology),
            response => Err(unexpected_response("topology", response)),
        }
    }

    pub fn abort_tls_certificate_rotation(
        &self,
        leader_node_id: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<ClusterTopology> {
        match self.call(
            leader_node_id,
            ClusterDataRequest::AbortTlsCertificateRotation {
                node_id: self.caller_node_id.clone(),
                now_ms,
            },
        )? {
            ClusterDataResponse::Topology(topology) => Ok(topology),
            response => Err(unexpected_response("topology", response)),
        }
    }

    pub fn fetch_topology(&self, controller_node_id: &ClusterNodeId) -> Result<ClusterTopology> {
        match self.call(controller_node_id, ClusterDataRequest::FetchTopology)? {
            ClusterDataResponse::Topology(topology) => Ok(topology),
            response => Err(unexpected_response("topology", response)),
        }
    }

    pub fn register_metadata_learner(
        &self,
        leader_node_id: &ClusterNodeId,
        node: ClusterNode,
        now_ms: u64,
    ) -> Result<ClusterTopology> {
        match self.call(
            leader_node_id,
            ClusterDataRequest::RegisterMetadataLearner { node, now_ms },
        )? {
            ClusterDataResponse::Topology(topology) => Ok(topology),
            response => Err(unexpected_response("topology", response)),
        }
    }

    pub fn promote_metadata_learner(
        &self,
        leader_node_id: &ClusterNodeId,
        installed_commit_index: u64,
        installed_topology_generation: u64,
        now_ms: u64,
    ) -> Result<ClusterTopology> {
        match self.call(
            leader_node_id,
            ClusterDataRequest::PromoteMetadataLearner {
                node_id: self.caller_node_id.clone(),
                installed_commit_index,
                installed_topology_generation,
                now_ms,
            },
        )? {
            ClusterDataResponse::Topology(topology) => Ok(topology),
            response => Err(unexpected_response("topology", response)),
        }
    }

    pub fn request_metadata_vote(
        &self,
        voter_node_id: &ClusterNodeId,
        request: MetadataVoteRequest,
    ) -> Result<MetadataVoteResponse> {
        if request.candidate_id != self.caller_node_id {
            return Err(transport_error(
                "metadata vote candidate must be the transport caller",
            ));
        }
        match self.call(voter_node_id, ClusterDataRequest::MetadataVote { request })? {
            ClusterDataResponse::MetadataVote(response)
                if response.voter_id == *voter_node_id
                    && response.cluster_id == self.cluster_id =>
            {
                Ok(response)
            }
            ClusterDataResponse::MetadataVote(_) => {
                Err(transport_error("metadata vote response identity mismatch"))
            }
            response => Err(unexpected_response("metadata vote", response)),
        }
    }

    pub fn append_metadata(
        &self,
        follower_node_id: &ClusterNodeId,
        request: MetadataAppendRequest,
    ) -> Result<MetadataAppendResponse> {
        if request.leader_id != self.caller_node_id {
            return Err(transport_error(
                "metadata append leader must be the transport caller",
            ));
        }
        match self.call(
            follower_node_id,
            ClusterDataRequest::MetadataAppend { request },
        )? {
            ClusterDataResponse::MetadataAppend(response) => Ok(response),
            response => Err(unexpected_response("metadata append", response)),
        }
    }

    fn call(
        &self,
        destination_node_id: &ClusterNodeId,
        request: ClusterDataRequest,
    ) -> Result<ClusterDataResponse> {
        let (address, expected_certificate_sha256, pending_certificate_sha256) = {
            let topology = self.topology.read();
            let node = topology.nodes.get(destination_node_id).ok_or_else(|| {
                transport_error(format!("unknown cluster node {destination_node_id}"))
            })?;
            if node.lifecycle == ClusterNodeLifecycle::Decommissioned {
                return Err(transport_error(format!(
                    "cluster node {destination_node_id} is decommissioned"
                )));
            }
            (
                node.address.clone(),
                node.tls_certificate_sha256.clone(),
                node.pending_tls_certificate_sha256.clone(),
            )
        };
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let envelope = ClusterDataRequestEnvelope {
            protocol_version: CLUSTER_DATA_PROTOCOL_VERSION,
            cluster_id: self.cluster_id.clone(),
            caller_node_id: self.caller_node_id.clone(),
            destination_node_id: destination_node_id.clone(),
            request_id,
            request,
        };
        let mut addrs = address.to_socket_addrs()?;
        let address_resolved = addrs.next().ok_or_else(|| {
            transport_error(format!("cluster address `{address}` resolved empty"))
        })?;
        if self.config.dev_localhost_plaintext && !address_resolved.ip().is_loopback() {
            return Err(transport_error(
                "plaintext cluster data RPC may connect only to loopback",
            ));
        }
        let stream = TcpStream::connect_timeout(
            &address_resolved,
            Duration::from_millis(self.config.connect_timeout_ms),
        )?;
        configure_stream(&stream, &self.config)?;
        let response = if let Some(tls) = self.client_tls.as_ref() {
            let server_name = server_name_from_address(&address)?;
            let mut stream = client_tls_stream(Arc::clone(&tls.config), &server_name, stream)?;
            let peer_certificate_sha256 = complete_client_tls_handshake(&mut stream)?;
            verify_destination_certificate(
                destination_node_id,
                expected_certificate_sha256.as_deref(),
                pending_certificate_sha256.as_deref(),
                Some(&peer_certificate_sha256),
            )?;
            send_json_frame(&mut stream, &envelope, self.config.max_frame_bytes)?;
            recv_json_frame::<ClusterDataResponseEnvelope>(
                &mut stream,
                self.config.max_frame_bytes,
            )?
        } else {
            let mut stream = stream;
            verify_destination_certificate(
                destination_node_id,
                expected_certificate_sha256.as_deref(),
                pending_certificate_sha256.as_deref(),
                None,
            )?;
            send_json_frame(&mut stream, &envelope, self.config.max_frame_bytes)?;
            recv_json_frame::<ClusterDataResponseEnvelope>(
                &mut stream,
                self.config.max_frame_bytes,
            )?
        };
        if response.protocol_version != CLUSTER_DATA_PROTOCOL_VERSION
            || response.cluster_id != self.cluster_id
            || response.source_node_id != *destination_node_id
            || response.request_id != request_id
        {
            return Err(transport_error(
                "cluster data RPC response identity or protocol mismatch",
            ));
        }
        response.response.map_err(|remote| {
            transport_error(format!(
                "cluster node {destination_node_id} rejected RPC {}: {}",
                remote.code, remote.message
            ))
        })
    }
}

fn server_name_from_address(address: &str) -> Result<String> {
    if let Some(rest) = address.strip_prefix('[') {
        let (host, suffix) = rest
            .split_once(']')
            .ok_or_else(|| transport_error(format!("invalid cluster address `{address}`")))?;
        if !suffix.starts_with(':') {
            return Err(transport_error(format!(
                "cluster address `{address}` has no port"
            )));
        }
        return Ok(host.to_string());
    }
    let (host, _) = address
        .rsplit_once(':')
        .ok_or_else(|| transport_error(format!("cluster address `{address}` has no port")))?;
    if host.is_empty() {
        return Err(transport_error(format!(
            "cluster address `{address}` has no host"
        )));
    }
    Ok(host.to_string())
}

fn unexpected_response(expected: &str, response: ClusterDataResponse) -> BicDbError {
    transport_error(format!(
        "cluster data RPC expected {expected}, received {response:?}"
    ))
}

impl RangeWriteTransport for TcpClusterRelocationTransport {
    fn install_range_backup_fence(
        &self,
        destination: &ClusterNodeId,
        plan_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
        installed_at_ms: u64,
        expires_at_ms: u64,
    ) -> Result<RangeWriteProgress> {
        match self.call(
            destination,
            ClusterDataRequest::InstallRangeBackupFence {
                plan_id,
                range_id,
                range_epoch,
                installed_at_ms,
                expires_at_ms,
            },
        )? {
            ClusterDataResponse::RangeWriteProgress(progress) => Ok(progress),
            response => Err(unexpected_response("range backup fence progress", response)),
        }
    }

    fn release_range_backup_fence(
        &self,
        destination: &ClusterNodeId,
        plan_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
    ) -> Result<bool> {
        match self.call(
            destination,
            ClusterDataRequest::ReleaseRangeBackupFence {
                plan_id,
                range_id,
                range_epoch,
            },
        )? {
            ClusterDataResponse::RangeBackupFenceReleased(released) => Ok(released),
            response => Err(unexpected_response("range backup fence release", response)),
        }
    }

    fn range_write_progress(
        &self,
        destination: &ClusterNodeId,
        range_id: crate::distribution::RangeId,
        range_epoch: u64,
    ) -> Result<RangeWriteProgress> {
        match self.call(
            destination,
            ClusterDataRequest::RangeWriteProgress {
                range_id,
                range_epoch,
            },
        )? {
            ClusterDataResponse::RangeWriteProgress(progress) => Ok(progress),
            response => Err(unexpected_response("range-write progress", response)),
        }
    }

    fn apply_range_write_repair(
        &self,
        destination: &ClusterNodeId,
        batch: &RangeWriteRepairBatch,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        match self.call(
            destination,
            ClusterDataRequest::ApplyRangeWriteRepair {
                batch: batch.clone(),
                limits: *limits,
            },
        )? {
            ClusterDataResponse::RangeWriteProgress(progress) => Ok(progress),
            response => Err(unexpected_response("range-write repair progress", response)),
        }
    }

    fn fetch_range_write_repair(
        &self,
        source: &ClusterNodeId,
        range_id: crate::distribution::RangeId,
        range_epoch: u64,
        previous_resolved_index: u64,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        match self.call(
            source,
            ClusterDataRequest::FetchRangeWriteRepair {
                range_id,
                range_epoch,
                previous_resolved_index,
                limits: *limits,
            },
        )? {
            ClusterDataResponse::RangeWriteRepair(batch) => Ok(batch),
            response => Err(unexpected_response("range-write repair batch", response)),
        }
    }

    fn probe_range_write(
        &self,
        source: &ClusterNodeId,
        range_id: crate::distribution::RangeId,
        range_epoch: u64,
        index: u64,
    ) -> Result<RangeWriteProbe> {
        match self.call(
            source,
            ClusterDataRequest::ProbeRangeWrite {
                range_id,
                range_epoch,
                index,
            },
        )? {
            ClusterDataResponse::RangeWriteProbe(probe) => Ok(probe),
            response => Err(unexpected_response("range-write probe", response)),
        }
    }

    fn prepare_range_write(
        &self,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        match self.call(
            destination,
            ClusterDataRequest::PrepareRangeWrite {
                command: command.clone(),
            },
        )? {
            ClusterDataResponse::RangeWrite(ack) => Ok(ack),
            response => Err(unexpected_response("range-write acknowledgement", response)),
        }
    }

    fn commit_range_write(
        &self,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        match self.call(
            destination,
            ClusterDataRequest::CommitRangeWrite {
                command: command.clone(),
            },
        )? {
            ClusterDataResponse::RangeWrite(ack) => Ok(ack),
            response => Err(unexpected_response("range-write acknowledgement", response)),
        }
    }

    fn certify_range_write(
        &self,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        match self.call(
            destination,
            ClusterDataRequest::CertifyRangeWrite {
                command: command.clone(),
            },
        )? {
            ClusterDataResponse::RangeWrite(ack) => Ok(ack),
            response => Err(unexpected_response("range-write acknowledgement", response)),
        }
    }

    fn apply_range_write(
        &self,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        match self.call(
            destination,
            ClusterDataRequest::ApplyRangeWrite {
                command: command.clone(),
            },
        )? {
            ClusterDataResponse::RangeWrite(ack) => Ok(ack),
            response => Err(unexpected_response("range-write acknowledgement", response)),
        }
    }

    fn abort_range_write(
        &self,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        match self.call(
            destination,
            ClusterDataRequest::AbortRangeWrite {
                command: command.clone(),
            },
        )? {
            ClusterDataResponse::RangeWrite(ack) => Ok(ack),
            response => Err(unexpected_response("range-write acknowledgement", response)),
        }
    }
}

impl RangeDigestTransport for TcpClusterRelocationTransport {
    fn advance_range_digest(
        &self,
        destination: &ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        session_id: Uuid,
        expected_checksum_sha256: Option<&str>,
        limits: &RangeDigestLimits,
        now_ms: u64,
    ) -> Result<RangeDigestState> {
        let _ = now_ms;
        match self.call(
            destination,
            ClusterDataRequest::AdvanceRangeDigest {
                range_id,
                range_epoch,
                session_id,
                expected_checksum_sha256: expected_checksum_sha256.map(str::to_string),
                limits: *limits,
            },
        )? {
            ClusterDataResponse::RangeDigestState(state) => Ok(state),
            response => Err(unexpected_response("range digest state", response)),
        }
    }

    fn export_range_digest_bucket(
        &self,
        destination: &ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        session_id: Uuid,
        bucket: u32,
        resume_after_key: Option<&str>,
        limits: &RangeDigestLimits,
        now_ms: u64,
    ) -> Result<RangeDigestBucketScanStep> {
        let _ = now_ms;
        match self.call(
            destination,
            ClusterDataRequest::ExportRangeDigestBucket {
                range_id,
                range_epoch,
                session_id,
                bucket,
                resume_after_key: resume_after_key.map(str::to_string),
                limits: *limits,
            },
        )? {
            ClusterDataResponse::RangeDigestBucket(step) => {
                step.validate(limits)?;
                if step.source_node_id != *destination
                    || step.range_id != range_id
                    || step.range_epoch != range_epoch
                    || step.session_id != session_id
                    || step.bucket != bucket
                    || step.expected_previous_resume.as_deref() != resume_after_key
                {
                    return Err(transport_error(
                        "range digest bucket response identity or cursor mismatch",
                    ));
                }
                Ok(step)
            }
            response => Err(unexpected_response("range digest bucket step", response)),
        }
    }
}

impl RangeDigestRepairTransport for TcpClusterRelocationTransport {
    fn apply_range_digest_repair(
        &self,
        destination: &ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        batch: &RangeDigestRepairBatch,
        limits: &RangeDigestRepairLimits,
        now_ms: u64,
    ) -> Result<RangeDigestRepairState> {
        let _ = now_ms;
        batch.validate(limits)?;
        if batch.destination_node_id != *destination
            || batch.range_id != range_id
            || batch.range_epoch != range_epoch
        {
            return Err(transport_error(
                "range digest repair request identity mismatch",
            ));
        }
        match self.call(
            destination,
            ClusterDataRequest::ApplyRangeDigestRepair {
                range_id,
                range_epoch,
                batch: batch.clone(),
                limits: *limits,
            },
        )? {
            ClusterDataResponse::RangeDigestRepairState(state) => {
                state.validate(limits)?;
                state.validate_batch_identity(batch)?;
                if state.applied_batches != batch.sequence
                    || state.last_batch_sha256.as_deref() != Some(batch.checksum_sha256.as_str())
                {
                    return Err(transport_error(
                        "range digest repair response sequence mismatch",
                    ));
                }
                Ok(state)
            }
            response => Err(unexpected_response("range digest repair state", response)),
        }
    }
}

impl ClusterRelocationTransport for TcpClusterRelocationTransport {
    fn prepare_learner(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        cancellation.check()?;
        let bundle = match self.call(
            &range.leader,
            ClusterDataRequest::ExportSchemaBundle {
                range_id: range.id,
                range_epoch: range.epoch,
            },
        )? {
            ClusterDataResponse::SchemaBundle(bundle) => bundle,
            response => return Err(unexpected_response("cluster schema bundle", response)),
        };
        cancellation.check()?;
        match self.call(
            &relocation.target,
            ClusterDataRequest::InstallSchemaBundle {
                relocation: relocation.clone(),
                range: range.clone(),
                bundle,
            },
        )? {
            ClusterDataResponse::Ack => {}
            response => return Err(unexpected_response("schema install ack", response)),
        }
        cancellation.check()?;
        match self.call(
            &relocation.target,
            ClusterDataRequest::PrepareLearner {
                relocation: relocation.clone(),
            },
        )? {
            ClusterDataResponse::Ack => Ok(()),
            response => Err(unexpected_response("ack", response)),
        }
    }

    fn copy_snapshot_step(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        options: &RangeSnapshotOptions,
        cancellation: &CancellationToken,
    ) -> Result<SnapshotCopyProgress> {
        cancellation.check()?;
        let source = range.leader.clone();
        let step = match self.call(
            &source,
            ClusterDataRequest::ExportSnapshot {
                relocation: relocation.clone(),
                range: range.clone(),
                options: options.clone(),
            },
        )? {
            ClusterDataResponse::SnapshotStep(step) => step,
            response => return Err(unexpected_response("snapshot step", response)),
        };
        cancellation.check()?;
        match self.call(
            &relocation.target,
            ClusterDataRequest::ApplySnapshot {
                relocation: relocation.clone(),
                range: range.clone(),
                step,
            },
        )? {
            ClusterDataResponse::SnapshotProgress(progress) => Ok(progress),
            response => Err(unexpected_response("snapshot progress", response)),
        }
    }

    fn catch_up_step(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        max_commit_frames: usize,
        cancellation: &CancellationToken,
    ) -> Result<CatchUpProgress> {
        cancellation.check()?;
        let durable = match self.call(
            &relocation.target,
            ClusterDataRequest::LearnerWatermark {
                relocation: relocation.clone(),
            },
        )? {
            ClusterDataResponse::LearnerWatermark(watermark) => watermark,
            response => return Err(unexpected_response("learner watermark", response)),
        };
        cancellation.check()?;
        let batch = match self.call(
            &range.leader,
            ClusterDataRequest::ExportCatchUp {
                range: range.clone(),
                durable_commit_sequence: durable,
                max_commit_frames,
            },
        )? {
            ClusterDataResponse::CatchUpBatch(batch) => batch,
            response => return Err(unexpected_response("catch-up batch", response)),
        };
        cancellation.check()?;
        match self.call(
            &relocation.target,
            ClusterDataRequest::ApplyCatchUp {
                relocation: relocation.clone(),
                range: range.clone(),
                batch,
            },
        )? {
            ClusterDataResponse::CatchUpProgress(progress) => Ok(progress),
            response => Err(unexpected_response("catch-up progress", response)),
        }
    }

    fn cleanup_source(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        resume_after_key: Option<&str>,
        max_records: usize,
        cancellation: &CancellationToken,
    ) -> Result<CleanupProgress> {
        cancellation.check()?;
        let progress = if let Some(source) = relocation.source.as_ref() {
            match self.call(
                source,
                ClusterDataRequest::CleanupSource {
                    relocation: relocation.clone(),
                    range: range.clone(),
                    resume_after_key: resume_after_key.map(str::to_string),
                    max_records,
                },
            )? {
                ClusterDataResponse::CleanupProgress(progress) => progress,
                response => return Err(unexpected_response("cleanup progress", response)),
            }
        } else {
            CleanupProgress {
                resume_after_key: None,
                records_deleted: relocation.cleanup_records_deleted,
                completed: true,
            }
        };
        if progress.completed {
            cancellation.check()?;
            match self.call(
                &relocation.target,
                ClusterDataRequest::CompleteRelocation {
                    relocation: relocation.clone(),
                },
            )? {
                ClusterDataResponse::Ack => {}
                response => return Err(unexpected_response("ack", response)),
            }
        }
        Ok(progress)
    }
}

#[cfg(test)]
mod tests {

    /// D-1: the relocation data plane was dispatched without the authenticated
    /// caller, so any member holding a valid certificate — not just the
    /// relocation's source — could drive forged snapshot batches into a live
    /// relocation's target, where they persist through
    /// `bypass_commit_admission` + `write_upserts_unchecked`. Every range-write
    /// RPC beside it is leader-fenced; this plane was the one that was not.
    #[test]
    fn relocation_data_plane_rejects_a_non_participant_caller() {
        let topology_root = tempfile::tempdir().unwrap();
        let actor = ClusterNodeId::new("n1").unwrap();
        let cluster_id = ClusterId::new("cluster-a").unwrap();
        let config = crate::DistributionConfig {
            enabled: true,
            cluster_id: cluster_id.clone(),
            node_id: actor.clone(),
            node_address: "127.0.0.1:9441".to_string(),
            node_capacity_bytes: 10_000,
            replication_factor: 2,
            initial_ranges: 4,
            suspect_after_ms: 1_000,
            dead_after_ms: 2_000,
            ..crate::DistributionConfig::default()
        };
        let mut topology =
            DistributionStore::initialize_at(topology_root.path(), config, false, 10).unwrap();
        topology
            .join_node(
                crate::ClusterNode::new(
                    ClusterNodeId::new("n2").unwrap(),
                    "127.0.0.1:9442",
                    1,
                    10_000,
                    20,
                )
                .unwrap(),
                &actor,
                20,
            )
            .unwrap();
        let (_, relocations) = topology
            .start_failure_repair_cycle(
                &crate::RebalanceOptions {
                    max_replica_moves: 1,
                    max_moves_per_node: 1,
                    unknown_range_bytes: 1,
                    ..crate::RebalanceOptions::default()
                },
                &actor,
                30,
            )
            .unwrap();
        let relocation = topology.relocation(relocations[0]).unwrap().clone();
        let range = topology
            .topology()
            .range_by_id(relocation.range_id)
            .unwrap()
            .clone();

        // A member that is neither the range leader, nor the relocation's
        // source, nor its target.
        let outsider = ClusterNodeId::new("n99").unwrap();
        assert!(relocation.target != outsider);
        assert!(relocation.source.as_ref() != Some(&outsider));
        assert!(range.leader != outsider);

        let error =
            validate_relocation_source_caller(&outsider, &relocation, Some(&range)).unwrap_err();
        assert!(
            error.to_string().contains("is not the source"),
            "unexpected refusal: {error}"
        );

        // The relocation is driven from the source side, so the TARGET is not
        // a legitimate caller either. Admitting it would let a compromised
        // learner pull the source's range out through ExportSnapshot /
        // ExportCatchUp, or destroy data on it through CleanupSource.
        assert!(relocation.target != range.leader);
        assert!(relocation.source.as_ref() != Some(&relocation.target));
        let error =
            validate_relocation_source_caller(&relocation.target, &relocation, Some(&range))
                .unwrap_err();
        assert!(
            error.to_string().contains("is not the source"),
            "the target must not be able to drive the source side: {error}"
        );

        // The source side still passes, so a live relocation cannot stall.
        for source in [relocation.source.clone(), Some(range.leader.clone())]
            .into_iter()
            .flatten()
        {
            validate_relocation_source_caller(&source, &relocation, Some(&range))
                .unwrap_or_else(|error| panic!("source {source} must be accepted: {error}"));
        }

        // Metadata leadership is allowed to differ from range leadership, but
        // it authorizes only the exact relocation/range in its committed
        // generation. This is the production path used by the cluster
        // supervisor when the target (or another voter) wins an election.
        let committed = topology.topology().clone();
        let consensus_root = tempfile::tempdir().unwrap();
        let mut consensus = MetadataConsensusStore::open(
            consensus_root.path(),
            cluster_id,
            relocation.target.clone(),
            committed.clone(),
            false,
        )
        .unwrap();
        consensus.start_election().unwrap();
        consensus
            .become_leader(&BTreeSet::from([actor.clone(), relocation.target.clone()]))
            .unwrap();
        let consensus = Arc::new(Mutex::new(consensus));
        let published = RwLock::new(committed.clone());
        validate_relocation_controller_caller(
            &relocation.target,
            &relocation,
            Some(&range),
            Some(&consensus),
            &published,
        )
        .unwrap();

        let mut forged = relocation.clone();
        forged.snapshot_bytes_copied = forged.snapshot_bytes_copied.saturating_add(1);
        let error = validate_relocation_controller_caller(
            &relocation.target,
            &forged,
            Some(&range),
            Some(&consensus),
            &published,
        )
        .unwrap_err();
        assert!(error.to_string().contains("exact committed topology"));

        let mut stale = committed;
        stale.generation = stale.generation.saturating_add(1);
        let stale = RwLock::new(stale);
        let error = validate_relocation_controller_caller(
            &relocation.target,
            &relocation,
            Some(&range),
            Some(&consensus),
            &stale,
        )
        .unwrap_err();
        assert!(error.to_string().contains("consensus topology generation"));
    }
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use std::collections::BTreeSet;

    const CERT_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const CERT_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn node(id: &str) -> ClusterNode {
        ClusterNode::new(
            ClusterNodeId::new(id).unwrap(),
            "127.0.0.1:9444",
            1,
            10_000,
            1,
        )
        .unwrap()
    }

    fn heartbeat(fingerprint: Option<&str>) -> ClusterDataRequest {
        ClusterDataRequest::Heartbeat {
            node_id: ClusterNodeId::new("n1").unwrap(),
            incarnation: 1,
            used_bytes: 0,
            capacity_bytes: 10_000,
            labels: BTreeMap::new(),
            tls_certificate_sha256: fingerprint.map(str::to_string),
            now_ms: 2,
        }
    }

    #[test]
    fn signed_schema_stage_requires_metadata_leader_and_returns_durable_receipt() {
        let root = tempfile::tempdir().unwrap();
        let node_id = ClusterNodeId::new("n1").unwrap();
        let cluster_id = ClusterId::new("signed-schema-stage-rpc").unwrap();
        let config = crate::distribution::DistributionConfig {
            enabled: true,
            cluster_id: cluster_id.clone(),
            node_id: node_id.clone(),
            node_address: "127.0.0.1:9444".to_string(),
            replication_factor: 1,
            initial_ranges: 1,
            ..crate::distribution::DistributionConfig::default()
        };
        let distribution = crate::distribution::DistributionStore::initialize_at(
            root.path().join("control"),
            config,
            false,
            1,
        )
        .unwrap();
        let topology = distribution.topology().clone();
        let published = RwLock::new(topology.clone());
        let consensus_root = root.path().join("consensus");
        std::fs::create_dir_all(&consensus_root).unwrap();
        let mut consensus = MetadataConsensusStore::open(
            &consensus_root,
            cluster_id.clone(),
            node_id.clone(),
            topology,
            false,
        )
        .unwrap();
        consensus.start_election().unwrap();
        consensus
            .become_leader(&BTreeSet::from([node_id.clone()]))
            .unwrap();
        let consensus = Arc::new(Mutex::new(consensus));

        let db = Arc::new(RwLock::new(
            crate::BicDb::open_with_config(
                root.path().join("data"),
                crate::DbConfig::default().with_fsync(false),
            )
            .unwrap(),
        ));
        db.write().create_collection("items").unwrap();
        db.write()
            .insert("items", crate::Record::new("kept"))
            .unwrap();
        let base = db.read().schema_compatibility_fingerprint().unwrap();
        let mut base_topology = published.read().clone();
        base_topology.generation += 1;
        base_topology
            .nodes
            .get_mut(&node_id)
            .unwrap()
            .labels
            .insert(
                SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(),
                base.sha256.clone(),
            );
        base_topology.validate().unwrap();
        consensus
            .lock()
            .propose_topology(base_topology.clone())
            .unwrap();
        assert_eq!(
            consensus.lock().committed_topology(),
            &base_topology,
            "the single-voter metadata quorum must commit the base advertisement"
        );
        *published.write() = base_topology;

        let mut desired = crate::BicDb::open_with_config(
            root.path().join("desired"),
            crate::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        desired.create_collection("items").unwrap();
        desired.create_collection("new_items").unwrap();
        let bundle = desired.cluster_schema_bundle().unwrap();
        let signing_key = SigningKey::from_bytes(&[21; 32]);
        let signed_bundle = SignedClusterSchemaBundle {
            format_version: crate::SIGNED_CLUSTER_SCHEMA_BUNDLE_FORMAT_VERSION,
            signature_ed25519_hex: hex::encode(
                signing_key
                    .sign(&bundle.signing_message().unwrap())
                    .to_bytes(),
            ),
            signer_key_id: "release-key".to_string(),
            bundle,
        };
        let service = Mutex::new(
            ClusterDataNodeService::open_with_schema_trust(
                cluster_id,
                node_id.clone(),
                root.path().join("data"),
                Arc::clone(&db),
                BTreeMap::from([(
                    "release-key".to_string(),
                    signing_key.verifying_key().to_bytes(),
                )]),
                crate::ClusterSchemaStageLimits::default(),
                false,
            )
            .unwrap(),
        );

        let request = ClusterDataRequest::StageSignedSchemaBundle {
            signed_bundle: signed_bundle.clone(),
        };
        let wrong_leader = ClusterNodeId::new("n2").unwrap();
        let error = dispatch_cluster_data_request(
            &service,
            None,
            Some(&consensus),
            None,
            &published,
            &wrong_leader,
            request.clone(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("is not metadata leader"));

        let first = dispatch_cluster_data_request(
            &service,
            None,
            Some(&consensus),
            None,
            &published,
            &node_id,
            request.clone(),
        )
        .unwrap();
        let second = dispatch_cluster_data_request(
            &service,
            None,
            Some(&consensus),
            None,
            &published,
            &node_id,
            request,
        )
        .unwrap();
        let (first, second) = match (first, second) {
            (ClusterDataResponse::SchemaStage(first), ClusterDataResponse::SchemaStage(second)) => {
                (first, second)
            }
            responses => panic!("unexpected responses: {responses:?}"),
        };
        first.validate_for(&signed_bundle).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.base_fingerprint_sha256, base.sha256);
        assert_eq!(
            db.read().schema_compatibility_fingerprint().unwrap(),
            base,
            "remote staging must not activate schema"
        );
        assert!(db.read().get("items", "kept").unwrap().is_some());

        let rollout_id = Uuid::now_v7();
        let activation = ClusterDataRequest::AdvanceSignedSchemaActivation {
            rollout_id,
            stage_id: first.stage_id,
            base_fingerprint_sha256: base.sha256.clone(),
            target_fingerprint_sha256: signed_bundle.bundle.fingerprint.sha256.clone(),
        };
        let error = dispatch_cluster_data_request(
            &service,
            None,
            Some(&consensus),
            None,
            &published,
            &node_id,
            activation.clone(),
        )
        .unwrap_err();
        assert!(error.to_string().contains(
            "do not share the exact committed strict fence or additive compatibility window"
        ));

        let mut window = published.read().clone();
        window
            .open_schema_compatibility_window(
                &BTreeSet::from([node_id.clone()]),
                &base.sha256,
                &signed_bundle.bundle.fingerprint.sha256,
                node_id.clone(),
                10,
            )
            .unwrap();
        consensus.lock().propose_topology(window.clone()).unwrap();
        assert_eq!(consensus.lock().committed_topology(), &window);
        *published.write() = window;

        let wrong_base = ClusterDataRequest::AdvanceSignedSchemaActivation {
            rollout_id,
            stage_id: first.stage_id,
            base_fingerprint_sha256: "cc".repeat(32),
            target_fingerprint_sha256: signed_bundle.bundle.fingerprint.sha256.clone(),
        };
        let error = dispatch_cluster_data_request(
            &service,
            None,
            Some(&consensus),
            None,
            &published,
            &node_id,
            wrong_base,
        )
        .unwrap_err();
        assert!(error.to_string().contains(
            "do not share the exact committed strict fence or additive compatibility window"
        ));

        let response = dispatch_cluster_data_request(
            &service,
            None,
            Some(&consensus),
            None,
            &published,
            &node_id,
            activation,
        )
        .unwrap();
        let receipt = match response {
            ClusterDataResponse::SchemaActivation(receipt) => receipt,
            response => panic!("unexpected response: {response:?}"),
        };
        receipt
            .validate_for(&node_id, rollout_id, &first, &signed_bundle)
            .unwrap();
        assert!(receipt.complete());
        assert_eq!(
            db.read().schema_compatibility_fingerprint().unwrap(),
            signed_bundle.bundle.fingerprint
        );
        assert!(db.read().get("items", "kept").unwrap().is_some());
        let reopened_cluster_id = service.lock().cluster_id().clone();
        drop(service);
        let service = Mutex::new(
            ClusterDataNodeService::open_with_schema_trust(
                reopened_cluster_id,
                node_id.clone(),
                root.path().join("data"),
                Arc::clone(&db),
                BTreeMap::from([(
                    "release-key".to_string(),
                    signing_key.verifying_key().to_bytes(),
                )]),
                crate::ClusterSchemaStageLimits::default(),
                false,
            )
            .unwrap(),
        );
        let range_id = published.read().range_for_token(0).unwrap().id;
        require_local_schema_compatibility(&service, &published, range_id).unwrap();

        let finalization = ClusterDataRequest::FinalizeSignedSchemaActivation {
            activation: receipt.clone(),
        };
        let error = dispatch_cluster_data_request(
            &service,
            None,
            Some(&consensus),
            None,
            &published,
            &node_id,
            finalization.clone(),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("is not fenced at the completed target"));

        let mut promoted = published.read().clone();
        promoted
            .promote_schema_compatibility_window(
                &BTreeSet::from([node_id.clone()]),
                &base.sha256,
                &signed_bundle.bundle.fingerprint.sha256,
                node_id.clone(),
                11,
            )
            .unwrap();
        consensus.lock().propose_topology(promoted.clone()).unwrap();
        assert_eq!(consensus.lock().committed_topology(), &promoted);
        *published.write() = promoted;
        let error = dispatch_cluster_data_request(
            &service,
            None,
            Some(&consensus),
            None,
            &published,
            &wrong_leader,
            finalization.clone(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("is not metadata leader"));
        let first_finalization = dispatch_cluster_data_request(
            &service,
            None,
            Some(&consensus),
            None,
            &published,
            &node_id,
            finalization.clone(),
        )
        .unwrap();
        let retried_finalization = dispatch_cluster_data_request(
            &service,
            None,
            Some(&consensus),
            None,
            &published,
            &node_id,
            finalization,
        )
        .unwrap();
        let (first_finalization, retried_finalization) =
            match (first_finalization, retried_finalization) {
                (
                    ClusterDataResponse::SchemaFinalization(first),
                    ClusterDataResponse::SchemaFinalization(retried),
                ) => (first, retried),
                responses => panic!("unexpected responses: {responses:?}"),
            };
        first_finalization.validate_for(&node_id, &receipt).unwrap();
        assert_eq!(first_finalization, retried_finalization);
        assert!(!root
            .path()
            .join("data")
            .join(crate::DEFAULT_CLUSTER_SCHEMA_STAGE)
            .exists());
        assert!(!root
            .path()
            .join("data")
            .join(crate::DEFAULT_CLUSTER_SCHEMA_ACTIVATION)
            .exists());
        assert!(root
            .path()
            .join("data")
            .join(crate::DEFAULT_CLUSTER_SCHEMA_FINALIZATION)
            .exists());
    }

    #[test]
    fn data_rpc_schema_fence_checks_advertisement_and_live_database() {
        let root = tempfile::tempdir().unwrap();
        let node_id = ClusterNodeId::new("n1").unwrap();
        let cluster_id = ClusterId::new("schema-fence-rpc").unwrap();
        let config = crate::distribution::DistributionConfig {
            enabled: true,
            cluster_id: cluster_id.clone(),
            node_id: node_id.clone(),
            node_address: "127.0.0.1:9444".to_string(),
            replication_factor: 1,
            initial_ranges: 1,
            ..crate::distribution::DistributionConfig::default()
        };
        let store = crate::distribution::DistributionStore::initialize_at(
            root.path().join("control"),
            config,
            false,
            1,
        )
        .unwrap();
        let mut topology = store.topology().clone();
        let range_id = topology.range_for_token(0).unwrap().id;
        let db = Arc::new(RwLock::new(
            crate::BicDb::open_with_config(
                root.path().join("data"),
                crate::DbConfig::default().with_fsync(false),
            )
            .unwrap(),
        ));
        db.write().create_collection("items").unwrap();
        let actual = db.read().schema_compatibility_fingerprint().unwrap();
        let service = Mutex::new(
            ClusterDataNodeService::open(
                cluster_id,
                node_id.clone(),
                root.path().join("data"),
                db,
                false,
            )
            .unwrap(),
        );
        topology
            .nodes
            .get_mut(&node_id)
            .unwrap()
            .labels
            .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "f".repeat(64));
        let published = RwLock::new(topology);
        let error = require_local_schema_compatibility(&service, &published, range_id).unwrap_err();
        assert!(error.to_string().contains("local"), "{error}");

        published
            .write()
            .nodes
            .get_mut(&node_id)
            .unwrap()
            .labels
            .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), actual.sha256);
        require_local_schema_compatibility(&service, &published, range_id).unwrap();
    }

    #[test]
    fn bound_member_rpc_identity_must_match_presented_leaf_certificate() {
        let caller_id = ClusterNodeId::new("n1").unwrap();
        let caller = node("n1").with_tls_certificate_sha256(CERT_A).unwrap();

        authorize_cluster_data_caller(
            &caller_id,
            Some(&caller),
            &ClusterDataRequest::Ping,
            Some(CERT_A),
            false,
        )
        .unwrap();
        let error = authorize_cluster_data_caller(
            &caller_id,
            Some(&caller),
            &ClusterDataRequest::Ping,
            Some(CERT_B),
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not match membership"));
    }

    #[test]
    fn range_digest_rpc_is_bound_to_member_mtls_identity() {
        let caller_id = ClusterNodeId::new("n1").unwrap();
        let caller = node("n1").with_tls_certificate_sha256(CERT_A).unwrap();
        let session_id = Uuid::new_v4();
        let advance = ClusterDataRequest::AdvanceRangeDigest {
            range_id: RangeId::new(1).unwrap(),
            range_epoch: 1,
            session_id,
            expected_checksum_sha256: None,
            limits: RangeDigestLimits::default(),
        };
        authorize_cluster_data_caller(&caller_id, Some(&caller), &advance, Some(CERT_A), false)
            .unwrap();
        let error =
            authorize_cluster_data_caller(&caller_id, Some(&caller), &advance, Some(CERT_B), false)
                .unwrap_err();
        assert!(error.to_string().contains("does not match membership"));
        assert_eq!(
            advance.range_id_for_schema_fence(),
            Some(RangeId::new(1).unwrap())
        );

        let export = ClusterDataRequest::ExportRangeDigestBucket {
            range_id: RangeId::new(1).unwrap(),
            range_epoch: 1,
            session_id,
            bucket: 7,
            resume_after_key: Some("patients\0patient-00".to_string()),
            limits: RangeDigestLimits::default(),
        };
        authorize_cluster_data_caller(&caller_id, Some(&caller), &export, Some(CERT_A), false)
            .unwrap();
        let error =
            authorize_cluster_data_caller(&caller_id, Some(&caller), &export, Some(CERT_B), false)
                .unwrap_err();
        assert!(error.to_string().contains("does not match membership"));
        assert_eq!(
            export.range_id_for_schema_fence(),
            Some(RangeId::new(1).unwrap())
        );
    }

    #[test]
    fn legacy_member_is_fenced_until_matching_bind_heartbeat() {
        let caller_id = ClusterNodeId::new("n1").unwrap();
        let caller = node("n1");

        let error = authorize_cluster_data_caller(
            &caller_id,
            Some(&caller),
            &ClusterDataRequest::Ping,
            Some(CERT_A),
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must bind"));
        let error = authorize_cluster_data_caller(
            &caller_id,
            Some(&caller),
            &heartbeat(Some(CERT_B)),
            Some(CERT_A),
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must bind"));
        authorize_cluster_data_caller(
            &caller_id,
            Some(&caller),
            &heartbeat(Some(CERT_A)),
            Some(CERT_A),
            false,
        )
        .unwrap();
    }

    #[test]
    fn bootstrap_identity_can_only_register_itself_with_presented_certificate() {
        let caller_id = ClusterNodeId::new("n2").unwrap();
        let learner = node("n2")
            .with_tls_certificate_sha256(CERT_A)
            .unwrap()
            .as_metadata_learner();
        let registration = ClusterDataRequest::RegisterMetadataLearner {
            node: learner,
            now_ms: 2,
        };

        authorize_cluster_data_caller(
            &caller_id,
            None,
            &ClusterDataRequest::FetchBootstrapSnapshot,
            Some(CERT_A),
            false,
        )
        .unwrap();
        authorize_cluster_data_caller(&caller_id, None, &registration, Some(CERT_A), false)
            .unwrap();
        let error =
            authorize_cluster_data_caller(&caller_id, None, &registration, Some(CERT_B), false)
                .unwrap_err();
        assert!(error.to_string().contains("not bound"));
        let error = authorize_cluster_data_caller(
            &caller_id,
            None,
            &ClusterDataRequest::Ping,
            Some(CERT_A),
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("not a member"));
    }

    #[test]
    fn staged_rotation_accepts_overlap_but_only_new_heartbeat_activates() {
        let caller_id = ClusterNodeId::new("n1").unwrap();
        let mut caller = node("n1").with_tls_certificate_sha256(CERT_A).unwrap();
        caller.pending_tls_certificate_sha256 = Some(CERT_B.to_string());

        authorize_cluster_data_caller(
            &caller_id,
            Some(&caller),
            &ClusterDataRequest::Ping,
            Some(CERT_B),
            false,
        )
        .unwrap();
        authorize_cluster_data_caller(
            &caller_id,
            Some(&caller),
            &heartbeat(Some(CERT_B)),
            Some(CERT_B),
            false,
        )
        .unwrap();
        let error = authorize_cluster_data_caller(
            &caller_id,
            Some(&caller),
            &heartbeat(Some(CERT_A)),
            Some(CERT_B),
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("heartbeat TLS fingerprint"));

        let stage = ClusterDataRequest::StageTlsCertificateRotation {
            node_id: caller_id.clone(),
            next_tls_certificate_sha256:
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string(),
            now_ms: 3,
        };
        let error =
            authorize_cluster_data_caller(&caller_id, Some(&caller), &stage, Some(CERT_B), false)
                .unwrap_err();
        assert!(error.to_string().contains("active certificate"));
        authorize_cluster_data_caller(
            &caller_id,
            Some(&caller),
            &ClusterDataRequest::AbortTlsCertificateRotation {
                node_id: caller_id.clone(),
                now_ms: 4,
            },
            Some(CERT_B),
            false,
        )
        .unwrap();
    }
}
