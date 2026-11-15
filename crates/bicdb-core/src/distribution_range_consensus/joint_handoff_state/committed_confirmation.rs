//! Catalogue confirmation of a joint decision by a member that took part in
//! it over the remote decision round.
//!
//! The remote round writes the member's protected native decision record and
//! deliberately nothing else. The in-process route confirms the decision into
//! the range catalogue inside the publication itself, under the publishing
//! plan and role. A remote participant has neither, and needs neither: once
//! the decision is certified, ITS OWN committed metadata names the decision,
//! and ITS OWN protected native record holds the body. This member confirms
//! exactly when both agree. Nothing is taken from a request or a peer.
//!
//! It records an acknowledgement of a decision that is already committed. It
//! is never a vote: no quorum counts it, and it grants no serving, open or
//! release permission by itself.
use super::*;
use crate::distribution::NativeJointHandoffStageV1;
use crate::distribution_consensus::HostMetadataReadObservation;
use std::sync::Arc;

impl RangeWriteStore {
    /// Confirm the decision of `receipt` into this member's own catalogue, or
    /// find it already confirmed. Returns whether the catalogue was written.
    ///
    /// Proofs, all this member's own:
    /// - `receipt` is this store's member's protected native decision record,
    ///   re-read as current;
    /// - `own` is this member's live committed metadata read, in which the
    ///   relocation of the decision records a certified handoff with exactly
    ///   the record's decision digest, session, point and configurations;
    /// - the attached metadata store point `point`, where the member has one.
    ///
    /// A catalogue that already holds the same two digests is left untouched.
    /// A catalogue that holds another decision, session or scope refuses.
    pub(crate) fn confirm_committed_native_joint_decision(
        &mut self,
        database: &crate::BicDb,
        own: &HostMetadataReadObservation,
        business: Uuid,
        point: Option<&Arc<crate::distribution_data::ActualNativeJointMetadataPoint>>,
        receipt: &crate::db::NativeJointDecisionReceipt,
    ) -> Result<bool> {
        own.require_live_request(business)?;
        match point {
            Some(point) => {
                point.require_request(own, business)?;
                point.require_native_store_point(database, self)?;
            }
            None if receipt.native_metadata_initialization_version() == 0 => {}
            None => {
                return Err(range_write_error(
                    "joint decision confirmation requires the actual current metadata store point",
                ))
            }
        }
        receipt.require_current(database)?;
        let data = receipt.data();
        let range = data.old_range().id;
        let local = self.catalog.node_id.clone();
        let topology = own.committed_topology(business)?;
        data.require_metadata_domain(topology, own.restore_activation(business)?)?;
        let relocation = topology
            .relocations
            .get(&data.promise().relocation_id)
            .ok_or_else(|| {
                range_write_error("joint decision confirmation relocation is not committed")
            })?;
        let record = relocation.native_joint_handoff_v1.as_ref().ok_or_else(|| {
            range_write_error("joint decision confirmation has no committed handoff record")
        })?;
        let member = data
            .new_members()
            .get(&local)
            .or_else(|| data.old_members().get(&local))
            .ok_or_else(|| {
                range_write_error(
                    "joint decision confirmation member is outside the decided configurations",
                )
            })?;
        let (native_term, native_index) = receipt.metadata_anchor();
        if receipt.local_node_id() != &local
            || own.requester_node_id() != &local
            || own.requester_incarnation() != member.incarnation
            || topology.cluster_id != self.catalog.cluster_id
            || topology
                .nodes
                .get(&local)
                .is_none_or(|node| node.incarnation != member.incarnation)
            || relocation.range_id != range
            || record.stage() < NativeJointHandoffStageV1::Certified
            || record.decision_sha256() != data.checksum_sha256()
            || record.session_id() != data.promise().session_id
            || record.nonce() != data.promise().nonce
            || record.range_index() != data.range_index()
            || record.logical_genesis_sha256() != data.logical_genesis_sha256()
            || record.logical_history_sha256() != data.logical_history_sha256()
            || record.old_configuration_sha256() != data.promise().old_config_sha256
            || record.new_configuration_sha256() != data.promise().new_config_sha256
            || own.term() < native_term
            || own.read_index() < native_index
        {
            return Err(range_write_error(
                "joint decision confirmation: own committed metadata and own native decision record disagree",
            ));
        }
        if database.actual_native_writer_enforcement_version()? != 1
            || !crate::format::native_joint_data_handoff_active(&self.root)?
        {
            return Err(range_write_error(
                "joint decision confirmation differs from actual local native domain",
            ));
        }
        let scope = data.plan_sha256()?;
        let current = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|states| states.get(&range));
        if let Some(current) = current {
            if current.scope_sha256 != scope
                || current.session_id != data.promise().session_id
                || current.nonce != data.promise().nonce
                || current
                    .global_decision_sha256
                    .as_ref()
                    .is_some_and(|v| v != receipt.decision_sha256())
                || current
                    .global_decision_native_sha256
                    .as_ref()
                    .is_some_and(|v| v != receipt.checksum_sha256())
            {
                return Err(range_write_error(
                    "different native joint decision cannot replace pending local session",
                ));
            }
            if current.global_decision_sha256.as_deref() == Some(receipt.decision_sha256())
                && current.global_decision_native_sha256.as_deref()
                    == Some(receipt.checksum_sha256())
            {
                return Ok(false);
            }
        }
        // The write. An uncertain catalogue refuses: nothing is confirmed
        // over it. The host identity is the installed writer's, or the live
        // Registered owner of exactly this range.
        self.require_native_catalog_durability(database.data_path())?;
        self.require_native_joint_host_base(database, range)?;
        own.require_live_request(business)?;
        let previous = self.catalog.clone();
        super::super::image_install_certification::ensure_installing_certification_maps(
            &mut self.catalog,
        );
        self.catalog
            .sparse_committed_decisions
            .get_or_insert_with(BTreeMap::new);
        let state = self
            .catalog
            .native_joint_handoffs
            .get_or_insert_with(BTreeMap::new)
            .entry(range)
            .or_insert(NativeJointLocalState {
                scope_sha256: scope,
                session_id: data.promise().session_id,
                nonce: data.promise().nonce,
                old_closed_native_sha256: None,
                global_decision_sha256: None,
                global_decision_native_sha256: None,
                serving_activation: None,
                source_retirement: None,
            });
        state.global_decision_sha256 = Some(receipt.decision_sha256().into());
        state.global_decision_native_sha256 = Some(receipt.checksum_sha256().into());
        if let Err(error) = state.validate() {
            self.catalog = previous;
            return Err(error);
        }
        self.catalog.format_version = self
            .catalog
            .format_version
            .max(NATIVE_JOINT_HANDOFF_CATALOG_FORMAT);
        // An ambiguous write leaves the previous memory state; a retry
        // re-proves everything and writes the identical two digests again.
        if let Err(error) = self.persist_native_joint(database, range) {
            self.catalog = previous;
            return Err(error);
        }
        own.require_live_request(business)?;
        receipt.require_current(database)?;
        Ok(true)
    }
    /// The two decision digests the catalogue holds for `range`.
    #[cfg(test)]
    pub(crate) fn test_catalogue_joint_decision(
        &self,
        range: RangeId,
    ) -> Option<(Option<String>, Option<String>)> {
        self.catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|states| states.get(&range))
            .map(|state| {
                (
                    state.global_decision_sha256.clone(),
                    state.global_decision_native_sha256.clone(),
                )
            })
    }
}
