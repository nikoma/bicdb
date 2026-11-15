//! Finite catalogue continuation. Returned copies are correlation DATA;
//! the actual metadata owner independently fetches its own opaque frames.
use super::*;
use crate::distribution_range_handoff::NativeRemoteNewOperationV1 as NewOp;
impl NativeFirstCoordinator {
    pub(super) fn catalogue_member(
        &mut self,
        node: ClusterNodeId,
        retry: bool,
        observe: bool,
        token: &CancellationToken,
    ) -> Result<serde_json::Value> {
        self.native_ready()?;
        let intent = self.native_decision.catalogue_intent()?;
        let mf = &intent.manifest;
        if !mf.old_members.contains_key(&node) && !mf.new_members.contains_key(&node) {
            return Err(invalid(
                "FIRST catalogue recipient outside original OLD/NEW scope",
            ));
        }
        if let Some((original_node, request)) = &self.native_decision.pending {
            if (!retry && !observe)
                || original_node != &node
                || !matches!(&request.operation,NewOp::PublishCatalogue {facts} if facts==&intent.facts)
            {
                return Err(invalid(
                    "FIRST catalogue continuation cannot resolve another native operation",
                ));
            }
        } else if retry || observe {
            return Err(invalid(
                "FIRST catalogue continuation needs original unresolved publish",
            ));
        }
        let operation = if observe {
            NewOp::ObserveCatalogue {
                facts: intent.facts.clone(),
            }
        } else {
            NewOp::PublishCatalogue {
                facts: intent.facts.clone(),
            }
        };
        let request = self.native_request(operation, token)?;
        let (own, business) = self.own(token)?;
        let actual =
            self.native_exchange(&node, &request, &own, business, token, retry || observe)?;
        let reply = actual.reply();
        let catalogue = reply
            .catalogue_decision
            .as_ref()
            .ok_or_else(|| invalid("FIRST actual recipient catalogue readback absent"))?;
        if catalogue.native.decision_sha256 != intent.decision_sha256 {
            return Err(invalid("FIRST catalogue changed original native decision"));
        }
        let result = serde_json::json!({"actual_reply":reply,"catalogue_only":true,"localQQ_Open_release_Serving":false});
        if retry || observe {
            let original = self.native_decision.pending.as_ref().unwrap();
            let resolution = serde_json::json!({"original_node":original.0,"original_request":original.1,
                "original_wire_return":self.pending_original_return,"fresh_catalogue_return":result,
                "original_call_relabelled_success":false});
            crate::distribution_range_image::bounded_json(&resolution, 4 * 1024 * 1024)?;
            self.native_decision.retain_catalogue_resolution(resolution);
        }
        self.native_decision.pending = None;
        self.pending_original_return = None;
        Ok(result)
    }
}
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn first_catalogue_certification_intent(
        &self,
        range: crate::RangeId,
    ) -> Result<super::super::first_catalogue_certification::NativeFirstCatalogueCertificationIntent>
    {
        let owner = self
            .first_coordinators
            .get(&range)
            .ok_or_else(|| invalid("FIRST original coordinator absent"))?;
        owner.native_ready()?;
        if owner.native_decision.pending.is_some() {
            return Err(invalid(
                "FIRST native/catalogue outcome unresolved before certification intent",
            ));
        }
        owner.native_decision.catalogue_intent()
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    /// Actual pinned original frames only. The negative expected hash is DATA;
    /// it cannot construct a valid frame or any native/caller permission.
    pub(in crate::distribution_native_host) fn test_first_catalogue_collection_refusal(
        &mut self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        use crate::distribution_range_handoff::{NativeJointOldRound, NativeRemoteCatalogueRound};
        let owner = self
            .first_coordinators
            .get_mut(&range)
            .ok_or_else(|| invalid("actual FIRST coordinator absent"))?;
        let intent = owner.native_decision.catalogue_intent()?;
        let caller = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let target = intent.manifest.target_id.clone();
        let (own, business) = owner.own(&caller)?;
        let request = owner.native_request(
            NewOp::ObserveCatalogue {
                facts: intent.facts.clone(),
            },
            &caller,
        )?;
        let first = owner
            .backend
            .transport
            .request_remote_native_catalogue(&target, &request, &own, business, &caller)?;
        first.require_request_live()?;
        let before = first.catalogue().clone();
        let build = |token: &CancellationToken,
                     expected: String|
         -> Result<NativeRemoteCatalogueRound> {
            let b = Uuid::now_v7();
            let leader = owner
                .backend
                .provider
                .current_metadata_leader_hint(token)?
                .ok_or_else(|| invalid("actual metadata leader absent"))?;
            let observation = owner.backend.transport.acquire_metadata_read_observation(
                &leader,
                b,
                intent.manifest.capture_range.id,
                intent.manifest.capture_range.epoch,
                Duration::from_secs(5),
                token,
            )?;
            let plan =
                NativeJointOldRound::begin(observation, b, intent.manifest.relocation_id)?.plan();
            NativeRemoteCatalogueRound::begin(
                plan,
                intent.manifest.clone(),
                intent.facts.clone(),
                expected,
                token,
            )
        };
        let (error, reached) = match mode {
            1 => {
                let mut round = build(&caller, intent.decision_sha256.clone())?;
                round.record(first)?;
                let (fresh, b) = owner.own(&caller)?;
                let second_request = owner.native_request(
                    NewOp::ObserveCatalogue {
                        facts: intent.facts.clone(),
                    },
                    &caller,
                )?;
                let second = owner.backend.transport.request_remote_native_catalogue(
                    &target,
                    &second_request,
                    &fresh,
                    b,
                    &caller,
                )?;
                let result = round.record(second);
                (
                    result
                        .err()
                        .ok_or_else(|| invalid("duplicate actual frame unexpectedly counted"))?,
                    "duplicate_actual_recipient",
                )
            }
            2 => {
                let wrong = if intent.decision_sha256 == "0".repeat(64) {
                    "1".repeat(64)
                } else {
                    "0".repeat(64)
                };
                let mut round = build(&caller, wrong)?;
                let result = round.record(first);
                (
                    result.err().ok_or_else(|| {
                        invalid("actual frame counted against contradictory expected D")
                    })?,
                    "actual_frame_contradicts_expected_complete_D",
                )
            }
            3 => {
                // Original callback expires naturally. A DISTINCT later real
                // request issues the new collection; no token is rewritten.
                while first.require_request_live().is_ok() {
                    parent.check()?;
                    std::thread::sleep(Duration::from_millis(2));
                }
                let fresh = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
                let mut round = build(&fresh, intent.decision_sha256.clone())?;
                let result = round.record(first);
                fresh.check()?;
                (
                    result
                        .err()
                        .ok_or_else(|| invalid("expired actual frame unexpectedly counted"))?,
                    "expired_original_pinned_frame_new_actual_collection",
                )
            }
            _ => return Err(invalid("unknown actual catalogue collection negative mode")),
        };
        Ok(
            serde_json::json!({"actual_control_boundary":reached,"count_refused":true,
            "actual_original_catalogue":before,"error":crate::distribution_native_host::remote_first::error_data(&error),
            "raw_frame_fabricated":false,"localQQ_Open_release_Serving":false}),
        )
    }
}
