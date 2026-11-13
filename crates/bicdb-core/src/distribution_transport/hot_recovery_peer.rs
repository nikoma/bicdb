//! Sender-local peer IO provenance and the existing sealed recovery replies.
//! Neither a network failure nor a phase receipt is quorum/admission authority.
use super::*;
use crate::distribution_range_recovery::{
    NativeCompletionObservation, NativeCompletionRequest, NativeCompletionStage,
    NativeReadyObservation, NativeReadyRequest, RecoveryEpochFenceRequest,
    RecoveryFenceObservation, RecoveryFenceObservationPhase, RecoveryHistoricalCommandRequest,
    RecoveryHistoryObservation,
};
use std::io;

#[derive(Clone, Copy, Debug)]
pub(super) enum SocketStage {
    Connect,
    Exchange,
}

/// Only the actual transport socket path can construct this provenance.
/// Its original error/kind remain inspectable; it cannot be decoded from wire
/// data or constructed by the controller from an arbitrary `BicDbError::Io`.
#[derive(Debug)]
pub(crate) struct HotRecoveryPeerFailure {
    source: io::Error,
    stage: SocketStage,
}
impl std::fmt::Display for HotRecoveryPeerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for HotRecoveryPeerFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl HotRecoveryPeerFailure {
    pub(crate) fn from_error(error: &BicDbError) -> Option<&Self> {
        match error {
            BicDbError::Io(error) => error.get_ref()?.downcast_ref::<Self>(),
            _ => None,
        }
    }
    pub(crate) fn kind(&self) -> io::ErrorKind {
        self.source.kind()
    }
    pub(crate) fn may_have_dispatched(&self) -> bool {
        matches!(self.stage, SocketStage::Exchange)
    }
}
pub(super) fn mark_socket_failure(
    error: io::Error,
    enabled: bool,
    stage: SocketStage,
) -> BicDbError {
    let kind = error.kind();
    // InvalidData/permission/JSON/pin/identity/semantic rejection is fatal.
    // WouldBlock here comes only from the actual bounded socket exchange;
    // arbitrary application IO does not pass this private constructor.
    if enabled
        && matches!(
            kind,
            io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::TimedOut
                | io::ErrorKind::WouldBlock
        )
    {
        io::Error::new(
            kind,
            HotRecoveryPeerFailure {
                source: error,
                stage,
            },
        )
        .into()
    } else {
        error.into()
    }
}

/// Actual request DATA only. The sole success path still requires the same
/// authenticated envelope and existing sealed observation constructors.
pub(crate) enum HotRecoveryPhaseRequest {
    Fence(RecoveryEpochFenceRequest),
    Historical {
        request: RecoveryHistoricalCommandRequest,
        apply: bool,
    },
    Prepare(NativeReadyRequest),
    Complete {
        request: NativeCompletionRequest,
        activate: bool,
    },
}
/// Not Clone/Serde and no raw constructor. This is a scheduling receipt only;
/// the final activated/open + fresh enrolled QQ publisher remains unchanged.
pub(crate) struct HotRecoveryPhaseReceipt {
    _actual_node: ClusterNodeId,
}
impl TcpClusterRelocationTransport {
    pub(crate) fn hot_recovery_phase_observation(
        &self,
        destination: &ClusterNodeId,
        mut request: HotRecoveryPhaseRequest,
        expected_index: u64,
        network_budget: Duration,
        parent: &CancellationToken,
    ) -> Result<HotRecoveryPhaseReceipt> {
        parent.check()?;
        if network_budget.is_zero() || network_budget > Duration::from_secs(5) {
            return Err(transport_error(
                "hot peer network budget must be 1..=5000ms",
            ));
        }
        let call_parent = parent.child_with_deadline(Instant::now() + network_budget);
        let remaining_ms = call_parent
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .map(|d| d.as_millis())
            .filter(|ms| *ms > 0)
            .ok_or(BicDbError::QueryTimedOut)?;
        let cap = remaining_ms.min(5000) as u64;
        match &mut request {
            HotRecoveryPhaseRequest::Fence(r) => r.budget_ms = r.budget_ms.min(cap),
            HotRecoveryPhaseRequest::Historical { request: r, .. } => {
                r.budget_ms = r.budget_ms.min(cap)
            }
            HotRecoveryPhaseRequest::Prepare(r) => r.budget_ms = r.budget_ms.min(cap),
            HotRecoveryPhaseRequest::Complete { request: r, .. } => {
                r.budget_ms = r.budget_ms.min(cap)
            }
        }
        let (token, payload) = match &request {
            HotRecoveryPhaseRequest::Fence(r) => (
                r.cancellation(&call_parent)?,
                ClusterDataRequest::RecoveryEpochFence { request: r.clone() },
            ),
            HotRecoveryPhaseRequest::Historical { request: r, apply } => (
                r.cancellation(&call_parent)?,
                if *apply {
                    ClusterDataRequest::RecoveryApplyHistorical { request: r.clone() }
                } else {
                    ClusterDataRequest::RecoveryCertifyHistorical { request: r.clone() }
                },
            ),
            HotRecoveryPhaseRequest::Prepare(r) => (
                r.cancellation(&call_parent)?,
                ClusterDataRequest::PrepareNativeReady { request: r.clone() },
            ),
            HotRecoveryPhaseRequest::Complete {
                request: r,
                activate,
            } => (
                r.cancellation(&call_parent)?,
                if *activate {
                    ClusterDataRequest::ActivateNativeRecovery { request: r.clone() }
                } else {
                    ClusterDataRequest::CompleteNativeRecovery { request: r.clone() }
                },
            ),
        };
        crate::distribution_range_consensus::validate_native_json_size(
            &ClusterDataRequestEnvelope {
                expected_native_destination: None,
                protocol_version: CLUSTER_DATA_PROTOCOL_VERSION,
                cluster_id: self.cluster_id.clone(),
                caller_node_id: self.caller_node_id.clone(),
                destination_node_id: destination.clone(),
                request_id: u64::MAX,
                request: &payload,
            },
            self.config.max_frame_bytes,
        )?;
        let result =
            self.call_with_native_destination_peer_transport(&token, destination, payload, true);
        // Original phase parent cancellation/expiry always wins. A shorter
        // actual peer timeout carries no successful vote/late observation.
        parent.check()?;
        let (response, peer, _) = result?;
        match (request, response) {
            (HotRecoveryPhaseRequest::Fence(r), ClusterDataResponse::RecoveryEpochFence(reply)) => {
                if reply.phase != RecoveryFenceObservationPhase::ActiveRecovery {
                    return Err(transport_error(
                        "epoch fence endpoint returned a non-active authority phase",
                    ));
                }
                RecoveryFenceObservation::from_transport(
                    AuthenticatedRecoveryFenceReply { reply, peer },
                    &r.scope,
                    token.deadline().unwrap(),
                )?
                .require_live()?;
            }
            (
                HotRecoveryPhaseRequest::Historical { request: r, .. },
                ClusterDataResponse::RecoveryFencedProbe(reply),
            ) => {
                RecoveryHistoryObservation::from_transport(
                    AuthenticatedRecoveryProbeReply { reply, peer },
                    &r.probe()?,
                    token.deadline().unwrap(),
                )?
                .fence()
                .require_live()?;
            }
            (HotRecoveryPhaseRequest::Prepare(r), ClusterDataResponse::NativeReady(reply)) => {
                let observation = NativeReadyObservation::from_transport(
                    AuthenticatedNativeReadyReply { reply, peer },
                    &r,
                    token.deadline().unwrap(),
                )?;
                observation.require_live()?;
                if observation.reply().range_index != expected_index {
                    return Err(transport_error(
                        "hot native preparation changed selected point",
                    ));
                }
            }
            (
                HotRecoveryPhaseRequest::Complete {
                    request: r,
                    activate,
                },
                ClusterDataResponse::NativeCompletion(reply),
            ) => {
                let observation = NativeCompletionObservation::from_transport(
                    AuthenticatedNativeCompletionReply { reply, peer },
                    &r,
                    token.deadline().unwrap(),
                )?;
                observation.require_live()?;
                if observation.reply().range_index != expected_index
                    || (activate
                        && observation.reply().stage != NativeCompletionStage::ActivatedAnchor)
                {
                    return Err(transport_error(
                        "hot native completion/open changed selected point",
                    ));
                }
            }
            (_, response) => return Err(unexpected_response("actual hot native phase", response)),
        }
        // A late/canceled producer result never becomes a scheduling success.
        parent.check()?;
        token.check()?;
        Ok(HotRecoveryPhaseReceipt {
            _actual_node: destination.clone(),
        })
    }
}
