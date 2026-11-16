//! Read-only probe of one member before and around a range move.
//!
//! The coordinator of a move asks a member what it is for one range, before
//! the learner allocation is proposed and again before an abort is decided.
//! The request is identity DATA (range id and epoch). The receiver decides
//! everything from its own state:
//! - the range and its epoch are read from the receiver's own published
//!   committed configuration; an unknown range or another epoch is refused;
//! - the authenticated mTLS caller must be the leader of that range in that
//!   configuration; no other member is answered;
//! - a registered replica receiver whose admission is live answers that it is
//!   one; an original data owner that is a voter of the range answers the
//!   durable epoch of its own range log, whether it holds an unfinished
//!   native command of the range and whether it holds an unfinished normal
//!   recovery of the range.
//!
//! The probe creates and changes nothing, takes no fence, and its reply is
//! historical DATA: it grants no role, quorum or permission to anyone. The
//! voter's epoch is the value its own service reports in-process; unlike the
//! range-write progress request it is not tied to the installed writer
//! policy naming the asked descriptor, because it admits no write. That is
//! what lets the coordinator read it after the learner allocation changed
//! the committed descriptor, which is exactly when an abort needs it.
use super::*;
use crate::distribution::RangeReplicaRole;

/// What the answering member is for the asked range.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NativeRangeMoveProbeRoleV1 {
    /// An original data owner that is a voter of the range, with the durable
    /// epoch of its own range log and whether its database holds an
    /// unfinished native command of the range (a write that was accepted and
    /// neither finished nor resolved). A source Close refuses such a voter.
    /// `unfinished_recovery`: its range catalogue holds a normal recovery of
    /// the range that is fenced and not yet activated. Such a recovery can
    /// only finish under the range configuration it was started for, and a
    /// source Close refuses the voter while it stands.
    Voter {
        durable_epoch: u64,
        unfinished_command: bool,
        #[serde(default)]
        unfinished_recovery: bool,
    },
    /// A registered replica receiver whose admission is live.
    RegisteredReplicaReceiver,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRangeMoveProbeReplyV1 {
    pub node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub role: NativeRangeMoveProbeRoleV1,
    /// The record behind `unfinished_recovery`, so that the coordinator's
    /// refusal can name it. Diagnostic DATA only; the refusal itself follows
    /// the role's flag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unfinished_recovery_record:
        Option<crate::distribution_range_consensus::UnfinishedNormalRecoveryV1>,
}

/// Receiver side. `initial` and `registered` are the listener's own immutable
/// mode; both were proved live by the caller of this function.
pub(super) fn answer(
    peer: &MetadataReadParticipant,
    local: &ClusterNodeId,
    topology: &RwLock<ClusterTopology>,
    service: &Mutex<ClusterDataNodeService>,
    initial: bool,
    registered: bool,
    range_id: RangeId,
    range_epoch: u64,
) -> Result<NativeRangeMoveProbeReplyV1> {
    let range = topology
        .read_recursive()
        .range_by_id(range_id)
        .filter(|range| range.epoch == range_epoch)
        .cloned()
        .ok_or_else(|| {
            transport_error(format!(
                "range move probe: unknown or stale range {range_id} epoch {range_epoch} in this member's committed configuration"
            ))
        })?;
    if peer.node_id() != &range.leader {
        return Err(transport_error(format!(
            "range move probe: {} is not the leader of range {range_id} epoch {range_epoch} in this member's committed configuration",
            peer.node_id()
        )));
    }
    let mut unfinished_recovery_record = None;
    let role = if registered {
        NativeRangeMoveProbeRoleV1::RegisteredReplicaReceiver
    } else if initial {
        if !range
            .replicas
            .iter()
            .any(|replica| &replica.node_id == local && replica.role == RangeReplicaRole::Voter)
        {
            return Err(transport_error(format!(
                "range move probe: this member is a data owner but not a voter of range {range_id}, and it is not a registered replica receiver"
            )));
        }
        let (durable_epoch, unfinished_command, record) = service
            .try_lock_for(Duration::from_millis(500))
            .ok_or_else(|| transport_error("range move probe: data service busy; repeat"))?
            .range_move_voter_facts_for_leader(&range, peer.node_id())?;
        let unfinished_recovery = record.is_some();
        unfinished_recovery_record = record;
        NativeRangeMoveProbeRoleV1::Voter {
            durable_epoch,
            unfinished_command,
            unfinished_recovery,
        }
    } else {
        return Err(transport_error(
            "range move probe: this member is neither a data owner nor a registered replica receiver",
        ));
    };
    Ok(NativeRangeMoveProbeReplyV1 {
        node_id: local.clone(),
        range_id,
        range_epoch,
        role,
        unfinished_recovery_record,
    })
}

impl TcpClusterRelocationTransport {
    /// Ask `destination` what it is for the range. Sent as this node; the
    /// reply is accepted only from the authenticated destination and only for
    /// the asked range and epoch.
    pub(crate) fn native_range_move_probe(
        &self,
        destination: &ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        parent: &CancellationToken,
    ) -> Result<NativeRangeMoveProbeReplyV1> {
        let (response, peer) = self.call_with_read_participant_cancellable(
            parent,
            destination,
            ClusterDataRequest::NativeRangeMoveProbe {
                range_id,
                range_epoch,
            },
        )?;
        let reply = match response {
            ClusterDataResponse::NativeRangeMoveProbe(reply) => reply,
            other => return Err(unexpected_response("native range move probe", other)),
        };
        if peer.node_id() != destination
            || &reply.node_id != destination
            || reply.range_id != range_id
            || reply.range_epoch != range_epoch
        {
            return Err(transport_error(
                "range move probe reply names another member, range or epoch",
            ));
        }
        Ok(reply)
    }
}
