//! Finite direct range-leader calls. All native owners stay on recipients.
//! Authenticated replies remain DATA; this module cannot certify metadata or
//! create a local NativeJointDurableDecision, Open, release or Serving grant.
use super::*;
use crate::distribution_range_handoff::{
    NativeRemoteDecisionReceiptV1, NativeRemoteNewFactSetV1, NativeRemoteNewOperationV1 as NewOp,
    NativeRemoteNewRequestV1, NativeRemoteNewRound,
};
use crate::distribution_range_image::NativeRemoteImageManifestV1;

#[derive(Default)]
pub(super) struct FirstNativeDecision {
    pub(super) manifest: Option<NativeRemoteImageManifestV1>,
    phase: Option<Uuid>,
    facts: Option<NativeRemoteNewFactSetV1>,
    pub(super) pending: Option<(ClusterNodeId, NativeRemoteNewRequestV1)>,
    prepared: BTreeSet<ClusterNodeId>,
    last_resolution: Option<serde_json::Value>,
    receipts: BTreeMap<ClusterNodeId, NativeRemoteDecisionReceiptV1>,
}
impl NativeFirstCoordinator {
    pub(super) fn native_ready(&self) -> Result<()> {
        self.process.check()?;
        if self.pending_receive.is_some()
            || self.pending_control.is_some()
            || self.inflight_intent.is_some()
        {
            return Err(invalid(
                "FIRST prior operation unresolved before remote NEW/DECISION",
            ));
        }
        if self.closed.len() != self.plan.old_members().len() {
            return Err(invalid("FIRST remote NEW requires original ALL-OLD cut"));
        }
        Ok(())
    }
    pub(super) fn native_manifest(&self) -> Result<NativeRemoteImageManifestV1> {
        let mf = self
            .native_decision
            .manifest
            .clone()
            .ok_or_else(|| invalid("FIRST original target-issued MF absent"))?;
        mf.require_shape()?;
        crate::distribution_range_image::NativeSourceExportBindingV1::from_manifest(&mf)?
            .require_plan(&self.plan)?;
        if Some(mf.target_attempt) != self.target_attempt {
            return Err(invalid("FIRST remote NEW changed original A"));
        }
        Ok(mf)
    }
    pub(super) fn native_request(
        &self,
        operation: NewOp,
        token: &CancellationToken,
    ) -> Result<NativeRemoteNewRequestV1> {
        let request = NativeRemoteNewRequestV1 {
            version: 1,
            manifest: self.native_manifest()?,
            phase_id: self
                .native_decision
                .phase
                .ok_or_else(|| invalid("FIRST native phase absent"))?,
            operation,
            business: Uuid::now_v7(),
            challenge: Uuid::now_v7(),
            budget_ms: remaining_ms(token)?,
        };
        request.require_shape()?;
        Ok(request)
    }
    pub(super) fn native_exchange(
        &mut self,
        node: &ClusterNodeId,
        request: &NativeRemoteNewRequestV1,
        own: &HostMetadataReadObservation,
        business: Uuid,
        token: &CancellationToken,
        observing: bool,
    ) -> Result<crate::distribution_transport::AuthenticatedNativeRemoteNewFrame> {
        // Bounded comparison DATA, retained before dispatch; no remote owner
        // or decoded request can become a quorum or local native permission.
        self.reserve_intent(serde_json::json!({"node":node,"phase":request.phase_id,
            "MF":request.manifest.checksum_sha256,"operation":request.operation,"business":request.business,
            "challenge":request.challenge,"budget_ms":request.budget_ms}))?;
        if !observing {
            self.native_decision.pending = Some((node.clone(), request.clone()));
        }
        let result = self
            .backend
            .transport
            .request_remote_native_new(node, request, own, business, token);
        self.record_return(serde_json::json!({"returned_ok":result.is_ok(),
            "actual_reply":result.as_ref().ok().map(|r|r.reply()),
            "error":result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
        let actual = result?;
        actual.require_request_live()?;
        token.check()?;
        Ok(actual)
    }
    pub(super) fn collect_native_new(
        &mut self,
        token: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.native_ready()?;
        if self.native_decision.pending.is_some() || self.native_decision.facts.is_some() {
            return Err(invalid(
                "FIRST original native NEW collection already started; no replacement",
            ));
        }
        let mf = self.native_manifest()?;
        // A fresh actual range read issues this collection's plan. The old
        // FIRST Begin request is never resurrected/extended for later phases.
        let (own, business) = self.own(token)?;
        let leader = self
            .backend
            .provider
            .current_metadata_leader_hint(token)?
            .ok_or_else(|| invalid("FIRST metadata leader absent"))?;
        let read_business = Uuid::now_v7();
        let observed = self.backend.transport.acquire_metadata_read_observation(
            &leader,
            read_business,
            mf.capture_range.id,
            mf.capture_range.epoch,
            Duration::from_millis(remaining_ms(token)?),
            token,
        )?;
        let plan_round = NativeJointOldRound::begin(observed, read_business, mf.relocation_id)?;
        let plan = plan_round.plan();
        drop(plan_round);
        let phase = *self.native_decision.phase.get_or_insert_with(Uuid::now_v7);
        let mut round = NativeRemoteNewRound::begin(plan, mf.clone(), phase, token)?;
        let mut nodes = mf.old_members.keys().cloned().collect::<Vec<_>>();
        if !nodes.contains(&mf.target_id) {
            nodes.push(mf.target_id.clone());
        }
        for node in nodes {
            let request = self.native_request(NewOp::Materialize, token)?;
            let actual = self.native_exchange(&node, &request, &own, business, token, false)?;
            if mf.new_members.contains_key(&node) {
                round.record(actual)?;
            }
            self.native_decision.pending = None;
            self.pending_original_return = None;
        }
        let actual = round.finish()?;
        let facts = actual.facts().clone();
        // Only historical immutable DATA survives this finite collection.
        // Each recipient independently fetches live NEW evidence at use.
        self.native_decision.facts = Some(facts.clone());
        Ok(serde_json::json!({"phase":phase,"facts":facts,"local_quorum_or_serving":false}))
    }
    pub(super) fn native_member(
        &mut self,
        node: ClusterNodeId,
        publish: bool,
        observe: bool,
        token: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.native_ready()?;
        let mf = self.native_manifest()?;
        if !mf.old_members.contains_key(&node) && !mf.new_members.contains_key(&node) {
            return Err(invalid(
                "FIRST remote native participant outside original fixed scope",
            ));
        }
        let facts = self
            .native_decision
            .facts
            .clone()
            .ok_or_else(|| invalid("FIRST genuine remote NEW collection absent"))?;
        if observe {
            let (original_node, original) = self
                .native_decision
                .pending
                .as_ref()
                .ok_or_else(|| invalid("FIRST no unresolved native DECISION"))?;
            if original_node != &node
                || !matches!(&original.operation,NewOp::PublishDecision {facts: old} if old==&facts)
            {
                return Err(invalid(
                    "FIRST readonly native observation cannot resolve another operation",
                ));
            }
        } else if self.native_decision.pending.is_some() {
            return Err(invalid(
                "FIRST prior remote native outcome unresolved; mutation replay refused",
            ));
        }
        if publish && !observe {
            let required =
                mf.old_members.len() + usize::from(!mf.old_members.contains_key(&mf.target_id));
            if self.native_decision.prepared.len() != required {
                return Err(invalid(
                    "FIRST native DECISION requires original ALL participant pin handoffs",
                ));
            }
            if self.native_decision.receipts.contains_key(&node) {
                return Err(invalid(
                    "FIRST native DECISION already observed; no new publication",
                ));
            }
        } else if !publish && self.native_decision.prepared.contains(&node) {
            return Err(invalid(
                "FIRST native participant already prepared; no replacement",
            ));
        }
        let operation = if observe {
            NewOp::ObserveKnownDecision { facts }
        } else if publish {
            NewOp::PublishDecision { facts }
        } else {
            NewOp::PrepareDecision { facts }
        };
        let request = self.native_request(operation, token)?;
        let (own, business) = self.own(token)?;
        let actual = self.native_exchange(&node, &request, &own, business, token, observe)?;
        let reply = actual.reply();
        if publish {
            let receipt = reply
                .native_decision
                .clone()
                .ok_or_else(|| invalid("FIRST native DECISION readback absent"))?;
            self.native_decision.receipts.insert(node, receipt);
        } else {
            self.native_decision.prepared.insert(node);
        }
        // Preserve original unknown return in the hash chain. This later
        // successful authenticated readback is not old RPC/fsync success.
        if observe {
            let original = self
                .native_decision
                .pending
                .as_ref()
                .expect("same original native request");
            let resolution = serde_json::json!({"original_recipient":original.0,
                "original_request_sha256":hex::encode(Sha256::digest(original.1.exact_bytes()?)),
                "original_wire_return":self.pending_original_return,
                "actual_readonly_receipt":reply.native_decision,"old_call_relabelled_success":false});
            crate::distribution_range_image::bounded_json(&resolution, 4 * 1024 * 1024)?;
            self.native_decision.last_resolution = Some(resolution);
        }
        self.native_decision.pending = None;
        self.pending_original_return = None;
        Ok(
            serde_json::json!({"actual_reply":reply,"native_receipts":self.native_decision.receipts,
            "catalogue_global_quorum_open_release_serving":false}),
        )
    }
}

#[cfg(test)]
impl FirstNativeDecision {
    pub(super) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"phase":self.phase,"facts":self.facts,"pending":self.pending,
            "prepared":self.prepared,"receipts":self.receipts,"last_resolution":self.last_resolution,
            "global_quorum_or_serving":false})
    }
}

impl FirstNativeDecision {
    pub(super) fn catalogue_intent(
        &self,
    ) -> Result<super::super::first_catalogue_certification::NativeFirstCatalogueCertificationIntent>
    {
        let manifest = self
            .manifest
            .clone()
            .ok_or_else(|| invalid("FIRST actual target MF absent"))?;
        let facts = self
            .facts
            .clone()
            .ok_or_else(|| invalid("FIRST actual NEW collection absent"))?;
        let first = self
            .receipts
            .values()
            .next()
            .ok_or_else(|| invalid("FIRST actual original native return absent"))?;
        if self
            .receipts
            .values()
            .any(|r| r.decision_sha256 != first.decision_sha256)
        {
            return Err(invalid(
                "FIRST recipients returned mixed complete native decisions",
            ));
        }
        Ok(
            super::super::first_catalogue_certification::NativeFirstCatalogueCertificationIntent {
                manifest,
                facts,
                decision_sha256: first.decision_sha256.clone(),
            },
        )
    }
    pub(super) fn retain_catalogue_resolution(&mut self, value: serde_json::Value) {
        self.last_resolution = Some(value);
    }
}
