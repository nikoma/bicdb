//! A remote participant's own catalogue confirmation of a certified joint
//! decision. See the catalogue side for the rule; this is the service entry
//! that reads this member's own protected native decision record.
//!
//! A member that holds NO protected native decision record of the committed
//! decision (it missed the publication of a round that reached only a
//! majority) is refused here. It is never given a record: the only writers
//! of that record are the publication sinks, which run under the original
//! decision phase's participant admission, and no such admission exists once
//! the decision is committed. Bringing such a member up to date needs its
//! own protocol; until then its bridge preparation (or source retirement)
//! refuses with the text below and writes nothing.
use super::*;
use crate::distribution::{NativeJointHandoffStageV1, RelocationId};
use crate::distribution_consensus::HostMetadataReadObservation;

impl ClusterDataNodeService {
    /// Confirm, into this member's own range catalogue, the joint decision
    /// that ITS OWN committed metadata read `own` records for `relocation`,
    /// from ITS OWN protected native decision record. Returns whether the
    /// catalogue was written; an already confirmed decision writes nothing.
    pub(crate) fn confirm_committed_native_joint_decision(
        service: &Arc<Mutex<Self>>,
        own: &HostMetadataReadObservation,
        business: Uuid,
        relocation: RelocationId,
        range: RangeId,
        parent: &CancellationToken,
    ) -> Result<bool> {
        own.require_live_request(business)?;
        let committed = own
            .committed_topology(business)?
            .relocations
            .get(&relocation)
            .and_then(|plan| plan.native_joint_handoff_v1.as_ref())
            .ok_or_else(|| {
                data_error("joint decision confirmation has no committed handoff record")
            })?
            .decision_sha256()
            .to_owned();
        Self::confirm_native_joint_decision_named(
            service, own, business, relocation, range, &committed, parent,
        )
    }
    /// `decision_sha256` selects this member's protected native record. It
    /// authorises nothing: the catalogue side compares the record it selects
    /// with the committed handoff record of `own` again.
    fn confirm_native_joint_decision_named(
        service: &Arc<Mutex<Self>>,
        own: &HostMetadataReadObservation,
        business: Uuid,
        relocation: RelocationId,
        range: RangeId,
        decision_sha256: &str,
        parent: &CancellationToken,
    ) -> Result<bool> {
        parent.check()?;
        own.require_live_request(business)?;
        // The attached metadata store is observed before any service, DB or
        // catalogue guard is held.
        let point = Self::capture_actual_joint_metadata_point(service, own, business, parent)?;
        if let Some(point) = &point {
            point.require_external_current()?;
        }
        let mut actual = service
            .try_lock_for(std::time::Duration::from_secs(1))
            .ok_or_else(|| data_error("joint decision confirmation service busy"))?;
        let database_arc = actual.db.clone();
        let database = checked_database_read(&database_arc, &actual.storage_policy)?;
        let receipt = actual.own_native_joint_decision(
            &database,
            own,
            business,
            relocation,
            range,
            decision_sha256,
        )?;
        parent.check()?;
        let written = actual
            .range_writes
            .current_mut()
            .confirm_committed_native_joint_decision(
                &database,
                own,
                business,
                point.as_ref(),
                &receipt,
            )?;
        parent.check()?;
        Ok(written)
    }
    /// This member's own protected native decision record named
    /// `decision_sha256`, for a relocation whose handoff THIS member's own
    /// committed metadata read records as certified. Read-only.
    fn own_native_joint_decision(
        &self,
        database: &crate::BicDb,
        own: &HostMetadataReadObservation,
        business: Uuid,
        relocation: RelocationId,
        range: RangeId,
        decision_sha256: &str,
    ) -> Result<crate::db::NativeJointDecisionReceipt> {
        if own.requester_node_id() != &self.node_id {
            return Err(data_error(
                "joint decision confirmation metadata read is not this member's own",
            ));
        }
        let topology = own.committed_topology(business)?;
        let plan = topology
            .relocations
            .get(&relocation)
            .ok_or_else(|| data_error("joint decision confirmation relocation is not committed"))?;
        let record = plan.native_joint_handoff_v1.as_ref().ok_or_else(|| {
            data_error("joint decision confirmation has no committed handoff record")
        })?;
        let local = topology
            .nodes
            .get(&self.node_id)
            .ok_or_else(|| data_error("joint decision confirmation local member absent"))?;
        if plan.range_id != range
            || record.stage() < NativeJointHandoffStageV1::Certified
            || local.incarnation != own.requester_incarnation()
        {
            return Err(data_error(
                "joint decision confirmation is not a certified handoff of this range in own metadata",
            ));
        }
        let bootstrap = topology.range_image_bootstrap_id.ok_or_else(|| {
            data_error("joint decision confirmation metadata has no native bootstrap")
        })?;
        self.require_native_catalog_database(database)?;
        let native = database.native_recovery_admission(
            &self.cluster_id,
            bootstrap,
            &self.node_id,
            local.incarnation,
        )?;
        database
            .read_native_joint_decision(&native, range, decision_sha256)
            .map_err(|error| {
                data_error(format!(
                    "joint decision confirmation refused: this member holds no protected native decision record of range {range} for decision {decision_sha256} ({error}); a member confirms only a decision it durably recorded itself, and no catch-up for a member that missed the decision publication is implemented"
                ))
            })
    }
    /// The body of the decision that this member's own committed metadata
    /// read `own` records for `relocation`, from this member's own protected
    /// native decision record: the reader refuses a record whose body
    /// checksum is not the committed digest. Historical DATA; it grants
    /// nothing and writes nothing.
    pub(crate) fn committed_native_joint_decision_body(
        service: &Arc<Mutex<Self>>,
        own: &HostMetadataReadObservation,
        business: Uuid,
        relocation: RelocationId,
        range: RangeId,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_handoff::NativeJointDecisionData> {
        parent.check()?;
        own.require_live_request(business)?;
        let committed = own
            .committed_topology(business)?
            .relocations
            .get(&relocation)
            .and_then(|plan| plan.native_joint_handoff_v1.as_ref())
            .ok_or_else(|| {
                data_error("joint decision confirmation has no committed handoff record")
            })?
            .decision_sha256()
            .to_owned();
        let actual = service
            .try_lock_for(std::time::Duration::from_secs(1))
            .ok_or_else(|| data_error("joint decision confirmation service busy"))?;
        let database_arc = actual.db.clone();
        let database = checked_database_read(&database_arc, &actual.storage_policy)?;
        let receipt = actual
            .own_native_joint_decision(&database, own, business, relocation, range, &committed)?;
        let data = receipt.data().clone();
        if data.checksum_sha256() != committed {
            return Err(data_error(
                "own native decision body is not the committed decision",
            ));
        }
        parent.check()?;
        Ok(data)
    }
    /// Select another native record than the committed one. Every check of
    /// the confirmation stays real.
    #[cfg(test)]
    pub(crate) fn test_confirm_native_joint_decision_named(
        service: &Arc<Mutex<Self>>,
        own: &HostMetadataReadObservation,
        business: Uuid,
        relocation: RelocationId,
        range: RangeId,
        decision_sha256: &str,
        parent: &CancellationToken,
    ) -> Result<bool> {
        Self::confirm_native_joint_decision_named(
            service,
            own,
            business,
            relocation,
            range,
            decision_sha256,
            parent,
        )
    }
    /// The two decision digests this member's catalogue holds for `range`.
    #[cfg(test)]
    pub(crate) fn test_catalogue_joint_decision(
        &self,
        range: RangeId,
    ) -> Option<(Option<String>, Option<String>)> {
        self.range_writes.test_catalogue_joint_decision(range)
    }
}
