//! Test-only comparison DATA at an actual successfully activated allocation.
//! Altered claims are negative inputs, never modified records or authority.
use super::*;
impl BicDb {
    pub(in crate::db::range_image_native) fn test_allocation_activation_domain_comparison(
        &self,
        writer: &Arc<NativeWriterHostState>,
        range: crate::RangeId,
        mode: u8,
    ) -> Result<(
        Result<()>,
        [u64; 3],
        [u64; 3],
        RangeDescriptor,
        RangeDescriptor,
        RangeDescriptor,
        RangeDescriptor,
        String,
        String,
        String,
        [u64; 2],
    )> {
        let _journal = self.try_native_image_journal()?;
        let record: NativeActivationDecisionRecord = self
            .read_native_range_image_record(&decision_key(range))?
            .ok_or_else(|| invalid("actual allocation activation decision absent"))?;
        let native = self.native_recovery_admission(
            &record.scope.cluster_id,
            record.scope.bootstrap_id,
            &record.local_node_id,
            record.local_member_incarnation,
        )?;
        let receipt = NativeActivationDecisionReceipt {
            native,
            record: record.clone(),
        };
        receipt.require_current_under_journal(self)?;
        let lineage = self.current_native_range_source_lineage(range)?;
        let effective = canonical(lineage.native_write_range()?);
        let mut metadata_claim = record.range.clone();
        let mut native_claim = effective.clone();
        match mode {
            0 | 5 => {} // 5 supplies an OTHER real retained writer, not a DTO.
            1 => metadata_claim
                .replicas
                .retain(|r| r.role == RangeReplicaRole::Voter),
            2 => {
                metadata_claim
                    .replicas
                    .iter_mut()
                    .find(|r| r.role == RangeReplicaRole::Learner)
                    .ok_or_else(|| invalid("actual learner prerequisite missing"))?
                    .role = RangeReplicaRole::Voter
            }
            3 => {
                metadata_claim.epoch = metadata_claim
                    .epoch
                    .checked_add(1)
                    .ok_or_else(|| invalid("actual epoch exhausted"))?
            }
            4 => {
                let actual_learner = metadata_claim
                    .replicas
                    .iter()
                    .find(|r| r.role == RangeReplicaRole::Learner)
                    .ok_or_else(|| invalid("actual learner prerequisite missing"))?
                    .clone();
                metadata_claim.replicas.push(actual_learner);
            }
            6 => {
                native_claim.epoch = native_claim
                    .epoch
                    .checked_add(1)
                    .ok_or_else(|| invalid("actual native epoch exhausted"))?
            }
            _ => return Err(invalid("unknown actual allocation comparison test mode")),
        }
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("actual allocation pager absent"))?;
        let clock = || {
            [
                paged.store().transactions().next_xid(),
                paged.store().wal().next_lsn(),
                paged.store().wal().durable_lsn(),
            ]
        };
        let before = clock();
        // SAME production projector, with a real retained owner. Its Result
        // stays original and is formatted only after this scope unwinds.
        let actual = writer
            .allocation_serving_domain(self, &metadata_claim)
            .and_then(|domain| domain.require_native_match(self, &native_claim));
        let after = clock();
        let actual_after: NativeActivationDecisionRecord = self
            .read_native_range_image_record(&decision_key(range))?
            .ok_or_else(|| invalid("actual activation decision disappeared after comparison"))?;
        if actual_after != record {
            return Err(invalid(
                "actual activation decision changed during comparison",
            ));
        }
        Ok((
            actual,
            before,
            after,
            record.range,
            effective,
            metadata_claim,
            native_claim,
            record.checksum_sha256,
            actual_after.checksum_sha256,
            record.scope.range_configuration_sha256,
            [record.native_page_xid, record.range_index],
        ))
    }
}
