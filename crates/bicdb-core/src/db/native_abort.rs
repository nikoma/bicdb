//! Core-only no-effect boundary after the real admission stack has unwound.
use super::*;
use crate::distribution_range_consensus::NativePrepareOnlyRejection;

/// Rejection is typed and retains the actual originating mutation lease. It
/// cannot be reconstructed from an error string, a raw Aborted entry or rows.
#[derive(Debug)]
pub enum NativeCommitAdmissionResult {
    Accepted(CommitAdmissionTicket),
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
            tx.rollback_write_receipts(0);
            tx.writes.clear();
            tx.write_index.clear();
            tx.prepared_wal = PreparedWal::default();
            tx.pending_broker_publishes.get_mut().clear();
            tx.pending_record_audit_events.clear();
            tx.imported_record_audit_events.clear();
            tx.deferred_hooks.get_mut().clear();
            tx.constraint_modes.get_mut().clear();
            tx.commit_validators.clear();
            tx.mutation_grants.clear();
            tx.postgres_serializable = None;
            tx.native_wal_retention = None;
            tx.range_mutation_claim = None;
            tx.database_mutation_permit = None;
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
