//! Core-only no-effect boundary after the real admission stack has unwound.
use super::*;
use crate::distribution_range_consensus::NativePrepareOnlyRejection;

/// Rejection is typed and retains the actual originating mutation lease. It
/// cannot be reconstructed from an error string, a raw Aborted entry or rows.
#[derive(Debug)]
pub enum NativeCommitAdmissionResult {
    Accepted(CommitAdmissionTicket),
    CommitMayHavePersisted {
        attempt: crate::distribution_range_consensus::NativeUncertainAdmissionRejection,
        error: BicDbError,
    },
    PrepareOnlyRejected {
        rejection: NativePrepareOnlyRejection,
        error: BicDbError,
    },
}
#[derive(Debug)]
pub struct NativeTransactionNoEffect {
    transaction_id: u64,
    database_incarnation: String,
    actual_mutations_sha256: String,
}
impl NativeTransactionNoEffect {
    pub(crate) fn require_rejection(
        &self,
        db: &BicDb,
        rejected: &NativePrepareOnlyRejection,
    ) -> Result<()> {
        db.check_recovery_state()?;
        if self.transaction_id != rejected.command().transaction_id
            || self.database_incarnation != db.incarnation_id()
            || self.database_incarnation != rejected.database_incarnation()
            || self.actual_mutations_sha256
                != admission_mutations_sha256(&rejected.command().mutations)?
        {
            return Err(BicDbError::Cluster(
                "native no-effect cleanup identity differs".into(),
            ));
        }
        rejected.require_mutation(&db.range_mutations)
    }
}
impl BicDb {
    pub(super) fn finish_native_prepare_rejection(
        &self,
        tx: &mut Transaction,
        error: BicDbError,
    ) -> Result<u64> {
        let (admission, rejected) = tx
            .native_prepare_only_rejection
            .take()
            .expect("typed prepare-only rejection checked by caller");
        if tx.committed_seq.is_some()
            || tx.commit_admission_ticket.is_some()
            || tx.native_admission_recovery_release.is_some()
            || tx.state != TxState::Pending
        {
            tx.receipt_native_admission_unknown();
            return Err(BicDbError::CommitOutcomeUnknown {
            sequence: tx.committed_seq, transaction_id: Some(tx.id.0), incarnation: self.incarnation.clone(),
            detail: "prepare-only cleanup reached an admitted/WAL attempt; no no-effect proof issued".into(),
        });
        }
        let publication = (|| -> Result<()> {
            // The inner commit has returned: page/unique/exclusion/postgres commit
            // guards have actually dropped. Release the actual locks and every
            // deferred effect/write buffer before publishing the no-effect intent.
            let actual_mutations_sha256 = actual_transaction_mutations_sha256(tx)?;
            if actual_mutations_sha256 != admission_mutations_sha256(&rejected.command().mutations)?
            {
                return Err(BicDbError::Cluster(
                    "prepare-only rejection does not describe actual Core buffered mutations"
                        .into(),
                ));
            }
            self.rollback_transaction_without_outcome_receipt(tx)?;
            tx.clear_native_rejected_effect_buffers();
            let no_effect = NativeTransactionNoEffect {
                transaction_id: tx.id.0,
                database_incarnation: self.incarnation.clone(),
                actual_mutations_sha256,
            };
            no_effect.require_rejection(self, &rejected)?;
            admission.native_prepare_only_cleaned(rejected, &no_effect, self)
        })();
        match publication {
            Ok(()) => {
                tx.receipt_aborted();
                Err(error)
            }
            Err(publication) => {
                tx.receipt_native_admission_unknown();
                Err(BicDbError::CommitOutcomeUnknown {
                    sequence: None, transaction_id: Some(tx.id.0), incarnation: self.incarnation.clone(),
                    detail: format!("actual local cleanup completed but durable no-effect intent is uncertain: {publication}"),
                })
            }
        }
    }
}

impl Transaction {
    fn clear_native_rejected_effect_buffers(&mut self) {
        self.rollback_write_receipts(0);
        self.writes.clear();
        self.write_index.clear();
        self.prepared_wal = PreparedWal::default();
        self.pending_broker_publishes.get_mut().clear();
        self.pending_record_audit_events.clear();
        self.imported_record_audit_events.clear();
        self.deferred_hooks.get_mut().clear();
        self.constraint_modes.get_mut().clear();
        self.commit_validators.clear();
        self.mutation_grants.clear();
        self.postgres_serializable = None;
        self.native_wal_retention = None;
        self.range_mutation_claim = None;
        self.database_mutation_permit = None;
    }
}
impl BicDb {
    pub(super) fn finish_native_uncertain_admission(
        &self,
        tx: &mut Transaction,
        error: BicDbError,
    ) -> Result<u64> {
        let attempt = tx
            .native_uncertain_admission
            .take()
            .expect("typed uncertain admission checked");
        tx.receipt_native_admission_unknown();
        let binding = actual_transaction_mutations_sha256(tx)
            .and_then(|hash| attempt.require_attempt(self, tx.id.0, &hash));
        let cleanup = if tx.state == TxState::Pending
            && tx.committed_seq.is_none()
            && tx.commit_admission_ticket.is_none()
            && tx.native_admission_recovery_release.is_none()
        {
            self.rollback_transaction_without_outcome_receipt(tx)
                .map(|()| tx.clear_native_rejected_effect_buffers())
        } else {
            Err(BicDbError::Cluster(
                "uncertain admission crossed its actual before-WAL cleanup boundary".into(),
            ))
        };
        // The lease is held through real lock/state/buffer cleanup. This emits
        // neither CoreNoEffect nor Aborted, and never removes an actual decision.
        drop(attempt);
        tx.receipt_native_admission_unknown();
        Err(BicDbError::CommitOutcomeUnknown {
            sequence: tx.committed_seq,
            transaction_id: Some(tx.id.0),
            incarnation: self.incarnation.clone(),
            detail: format!(
                "{error}; actual before-WAL cleanup binding={:?}, cleanup={:?}",
                binding.err(),
                cleanup.err()
            ),
        })
    }
}
