//! Distinct current-voter epoch-fence acknowledgments. A completed round proves
//! that a data majority has stopped ordinary old-session mutations; it grants
//! neither native visibility nor a committed per-command decision or read permit.
use std::collections::BTreeMap;
use std::time::Instant;

use super::{invalid, RecoveryFenceObservation, RecoveryMemberIdentity, RecoveryRoleAdmission};
use crate::distribution::{ClusterNodeId, RangeDescriptor, RangeReplicaRole};
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_consensus::{RangeWriteCommand, RecoveryFenceScope};
use crate::Result;

const MAX_VOTERS: usize = 64;
const MAX_OBSERVATION_TEXT: usize = 16 * 1024;

/// No caller-supplied counters, duplicate-voter counting or decoded DTOs can
/// construct this round's observations. Each observation has already traversed
/// an authenticated transport or the actual local receiver's drain/persist path.
pub(crate) struct RecoveryFenceQuorumRound {
    role: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
    observations: BTreeMap<ClusterNodeId, RecoveryFenceObservation>,
}

impl RecoveryFenceQuorumRound {
    pub(crate) fn begin(role: RecoveryRoleAdmission) -> Result<Self> {
        role.require_live_request()?;
        let members = role.member_credentials();
        if members.is_empty()
            || members.len() > MAX_VOTERS
            || members.len()
                != role
                    .current_range()
                    .replicas
                    .iter()
                    .filter(|replica| replica.role == RangeReplicaRole::Voter)
                    .count()
        {
            return Err(invalid("invalid or oversized current recovery voter set"));
        }
        let scope = RecoveryFenceScope::from_role(&role)?;
        Ok(Self {
            role,
            scope,
            observations: BTreeMap::new(),
        })
    }

    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn required_quorum(&self) -> usize {
        self.role.member_credentials().len() / 2 + 1
    }
    pub(crate) fn has_observation(&self, node: &ClusterNodeId) -> bool {
        self.observations.contains_key(node)
    }

    pub(crate) fn record(&mut self, observation: RecoveryFenceObservation) -> Result<()> {
        self.role.require_live_request()?;
        observation.require_live()?;
        // Recipients obtain independent fresh metadata contexts. Their durable
        // recovery session must match; their context UUID need not equal ours.
        observation.scope().require_role(&self.role)?;
        if !self.scope.same_session(observation.scope()) {
            return Err(invalid(
                "epoch-fence reply belongs to another recovery session",
            ));
        }
        if self.observations.contains_key(observation.node_id()) {
            return Err(invalid("duplicate recovery fence voter"));
        }
        let member = self
            .role
            .member_credentials()
            .get(observation.node_id())
            .ok_or_else(|| invalid("recovery fence observer is not a current data voter"))?;
        let peer = observation.authenticated_peer();
        if peer.node_id() != observation.node_id()
            || peer.member_incarnation() != member.incarnation
            || observation.member_incarnation() != member.incarnation
        {
            return Err(invalid(
                "recovery fence peer incarnation differs from fresh membership",
            ));
        }
        let has_pins = member.active_certificate_sha256.is_some()
            || member.pending_certificate_sha256.is_some();
        if has_pins {
            let actual = peer
                .certificate_sha256()
                .ok_or_else(|| invalid("recovery fence peer has no actual certificate"))?;
            if member.active_certificate_sha256.as_deref() != Some(actual)
                && member.pending_certificate_sha256.as_deref() != Some(actual)
            {
                return Err(invalid(
                    "recovery fence peer certificate is no longer authorized",
                ));
            }
        } else if !peer.plaintext_loopback() {
            return Err(invalid(
                "unpinned recovery peer requires explicit loopback development mode",
            ));
        }
        let progress = observation.progress();
        let extra_handoff_epoch = match self.role.purpose() {
            MetadataRecoveryRolePurpose::NormalRecovery => None,
            MetadataRecoveryRolePurpose::CommittedHandoff { .. } => {
                self.role.current_range().epoch.checked_add(2)
            }
        };
        if progress.node_id != *observation.node_id()
            || progress.range_id != self.scope.range_id
            || (progress.current_epoch != self.scope.range_epoch
                && Some(progress.current_epoch) != extra_handoff_epoch)
            || progress.resolved_through > progress.last_index
            || progress.compacted_through > progress.resolved_through
            || observation.store_open_id().is_nil()
            || observation.native_database_incarnation().is_empty()
            || observation.native_database_incarnation().len() > MAX_OBSERVATION_TEXT
        {
            return Err(invalid(
                "recovery fence log identity, epoch or watermarks are invalid",
            ));
        }
        let native = observation.native();
        if native.range_id != self.scope.range_id
            || native
                .native_base_index
                .zip(native.native_applied_index)
                .is_some_and(|(base, applied)| base > applied)
            || native.pending_command_index.is_some_and(|index| index == 0)
            || native
                .materialized_pending_index
                .is_some_and(|index| native.pending_command_index != Some(index))
            || native
                .pending_command_id
                .as_ref()
                .is_some_and(|id| id.is_empty() || id.len() > MAX_OBSERVATION_TEXT)
            || native
                .diagnostic
                .as_ref()
                .is_some_and(|message| message.len() > MAX_OBSERVATION_TEXT)
            || native.native_receipt_sha256.as_ref().is_some_and(|sha| {
                sha.len() != 64 || !sha.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        {
            return Err(invalid("recovery fence native observation is malformed"));
        }
        // Unknown/invalidated native floors can join a logical fence quorum.
        // They must be repaired before native-ready certification; None is
        // never converted to verified empty genesis or a snapshot floor.
        self.observations
            .insert(observation.node_id().clone(), observation);
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<FreshRangeRecoveryAuthority> {
        self.role.require_live_request()?;
        if self.observations.len() < self.required_quorum() {
            return Err(invalid(
                "no distinct current-data-voter epoch-fence majority",
            ));
        }
        // A consumed successful reply proves a DURABLE refusal. Its short RPC
        // timeout is not a lease expiry and cannot shorten this original role
        // deadline. New operations still require independent fresh role checks.
        Ok(FreshRangeRecoveryAuthority {
            role: self.role,
            scope: self.scope,
            observations: self.observations,
        })
    }
}

/// Actual current-data majority fencing, not native-ready or strong-read
/// authority. Receiver-side mutation still requires its independently fresh
/// role, owned freeze and an actual distinct per-command certificate quorum.
pub(crate) struct FreshRangeRecoveryAuthority {
    role: RecoveryRoleAdmission,
    scope: RecoveryFenceScope,
    observations: BTreeMap<ClusterNodeId, RecoveryFenceObservation>,
}
impl FreshRangeRecoveryAuthority {
    pub(crate) fn require_live(&self) -> Result<()> {
        self.role.require_live_request()
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        &self.role
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn current_range(&self) -> &RangeDescriptor {
        self.role.current_range()
    }
    pub(crate) fn member_credentials(&self) -> &BTreeMap<ClusterNodeId, RecoveryMemberIdentity> {
        self.role.member_credentials()
    }
    pub(crate) fn observations(&self) -> &BTreeMap<ClusterNodeId, RecoveryFenceObservation> {
        &self.observations
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.role.deadline()
    }
    pub(crate) fn require_historical_scope(&self, command: &RangeWriteCommand) -> Result<()> {
        self.require_live()?;
        self.role.require_historical_command_scope(command)
    }
    /// A conservative observation bound, NOT a materialized/certified cut.
    /// Native images can cover more history than a relocated raw log catalog.
    pub(crate) fn highest_observed_index(&self) -> u64 {
        self.observations
            .values()
            .flat_map(|observation| {
                let native = observation.native();
                [
                    Some(observation.progress().last_index),
                    native.native_base_index,
                    native.native_applied_index,
                    native.pending_command_index,
                    native.materialized_pending_index,
                ]
                .into_iter()
                .flatten()
            })
            .max()
            .unwrap_or(0)
    }
    /// Candidates for subsequent ACTUAL lineage/history verification. These
    /// status observations alone cannot authorize source capture or erase data.
    pub(crate) fn clean_source_candidates(&self) -> Vec<&RecoveryFenceObservation> {
        let mut candidates: Vec<_> = self
            .observations
            .values()
            .filter(|observation| {
                let native = observation.native();
                native.factory_initialization_complete
                    && !native.invalidated
                    && native.native_applied_index.is_some()
                    && native.native_receipt_sha256.is_some()
                    && native.pending_command_index.is_none()
                    && native.materialized_pending_index.is_none()
            })
            .collect();
        candidates.sort_by(|left, right| {
            right
                .native()
                .native_applied_index
                .cmp(&left.native().native_applied_index)
                .then_with(|| left.node_id().cmp(right.node_id()))
        });
        candidates
    }
}
