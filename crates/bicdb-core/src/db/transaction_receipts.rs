//! Read-only receipts for transaction outcomes and buffered write survival.
//!
//! Receipts contain no database pointer or mutation authority. They are local
//! observations, not durable commit certificates, and cannot resolve an
//! uncertain decision across database recovery.
use super::{BicDbError, Result, Transaction, TransactionId, TxState};
use parking_lot::Mutex;
use std::sync::{Arc, Weak};

/// The lifecycle of the transaction which produced an execution value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionOutcome {
    Pending,
    /// A commit decision was enqueued. Record application is not complete.
    Committing {
        sequence: u64,
    },
    /// Record application and the configured WAL durability boundary completed.
    Committed {
        sequence: u64,
    },
    Aborted,
    /// A decision may have persisted but the live database requires recovery.
    /// Callers must not infer that the writes were rolled back.
    RecoveryRequired {
        sequence: u64,
    },
}

#[derive(Debug)]
pub(super) struct OutcomeState {
    transaction_id: TransactionId,
    outcome: Mutex<TransactionOutcome>,
}

/// A cloneable observation which remains usable after its transaction is gone.
/// Constructed only by a transaction; it grants no read or write permission.
#[derive(Clone, Debug)]
pub struct TransactionOutcomeObserver(Arc<OutcomeState>);

impl TransactionOutcomeObserver {
    pub fn transaction_id(&self) -> TransactionId {
        self.0.transaction_id
    }

    pub fn outcome(&self) -> TransactionOutcome {
        *self.0.outcome.lock()
    }

    /// Transaction numbers alone are not identities across database handles.
    pub fn same_transaction(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// The exact buffered-write prefix retained when a receipt was invalidated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransactionWriteRollback {
    pub retained_write_len: usize,
    /// Highest receipt number allocated before this rollback. Later writes
    /// cannot masquerade as members of the retained historical prefix.
    pub receipt_limit: u64,
}

#[derive(Debug)]
struct WriteState {
    id: u64,
    write_len: usize,
    rollback: Mutex<Option<TransactionWriteRollback>>,
}

/// An observation of one particular buffered write, not its mutable log slot.
/// The receipt number is unique within its transaction and is never reused
/// after rollback. A rolled-back receipt stays rolled back after later commits.
#[derive(Clone, Debug)]
pub struct TransactionWriteReceipt {
    transaction: TransactionOutcomeObserver,
    write: Arc<WriteState>,
}

impl TransactionWriteReceipt {
    pub fn transaction(&self) -> &TransactionOutcomeObserver {
        &self.transaction
    }

    pub fn id(&self) -> u64 {
        self.write.id
    }

    /// Buffered prefix ending with the observed write, captured at registration.
    pub fn write_len(&self) -> usize {
        self.write.write_len
    }

    /// A missing boundary does not prove commitment: inspect the transaction
    /// outcome as well. Superseding writes are not savepoint rollbacks.
    pub fn rollback_boundary(&self) -> Option<TransactionWriteRollback> {
        *self.write.rollback.lock()
    }
}

#[derive(Debug)]
pub(super) struct ReceiptJournal {
    last_id: u64,
    writes: Vec<Weak<WriteState>>,
    prune_at: usize,
}

impl Default for ReceiptJournal {
    fn default() -> Self {
        Self {
            last_id: 0,
            writes: Vec::new(),
            prune_at: 64,
        }
    }
}

impl Transaction {
    /// Observe this transaction without extending its database lifetime.
    /// Ordinary transactions allocate no receipt state unless observed.
    pub fn observe_outcome(&self) -> TransactionOutcomeObserver {
        TransactionOutcomeObserver(Arc::clone(self.outcome_observer.get_or_init(|| {
            let outcome = match self.state {
                TxState::Pending => TransactionOutcome::Pending,
                TxState::Aborted => TransactionOutcome::Aborted,
                TxState::Committed => TransactionOutcome::Committed {
                    sequence: self
                        .committed_seq
                        .expect("committed transaction has a visibility sequence"),
                },
            };
            Arc::new(OutcomeState {
                transaction_id: self.id,
                outcome: Mutex::new(outcome),
            })
        })))
    }

    /// Observe the most recent buffered write for this record. Registration
    /// itself performs no write and acquires no row lock. A receipt created
    /// after a savepoint for a write before that savepoint still survives it.
    pub fn observe_record_write(
        &mut self,
        collection: &str,
        record_id: &str,
    ) -> Result<TransactionWriteReceipt> {
        if self.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        let write_len = self
            .write_index
            .get(collection)
            .and_then(|writes| writes.get(record_id))
            .map(|position| position + 1)
            .ok_or_else(|| {
                BicDbError::TransactionConflict("record has no buffered write to observe".into())
            })?;
        let transaction = self.observe_outcome();
        let journal = self
            .write_receipts
            .get_or_insert_with(|| Box::new(ReceiptJournal::default()));
        let id = journal.last_id.checked_add(1).ok_or_else(|| {
            BicDbError::TransactionConflict("transaction receipt number exhausted".into())
        })?;
        journal.last_id = id;
        // Expired handles do not retain their payloads. Amortized pruning also
        // bounds weak-control-block retention when values are short-lived.
        if journal.writes.len() >= journal.prune_at {
            journal.writes.retain(|write| write.strong_count() != 0);
            journal.prune_at = journal.writes.len().max(32).saturating_mul(2);
        }
        let write = Arc::new(WriteState {
            id,
            write_len,
            rollback: Mutex::new(None),
        });
        journal.writes.push(Arc::downgrade(&write));
        Ok(TransactionWriteReceipt { transaction, write })
    }

    pub(super) fn rollback_write_receipts(&mut self, retained_write_len: usize) {
        let Some(journal) = self.write_receipts.as_mut() else {
            return;
        };
        let boundary = TransactionWriteRollback {
            retained_write_len,
            receipt_limit: journal.last_id,
        };
        journal.writes.retain(|entry| {
            let Some(write) = entry.upgrade() else {
                return false;
            };
            if write.write_len > retained_write_len {
                *write.rollback.lock() = Some(boundary);
                false
            } else {
                true
            }
        });
        journal.prune_at = journal.writes.len().max(32).saturating_mul(2);
    }

    pub(super) fn receipt_commit_started(&self, sequence: u64) {
        if let Some(observer) = self.outcome_observer.get() {
            *observer.outcome.lock() = TransactionOutcome::Committing { sequence };
        }
    }

    pub(super) fn receipt_commit_finished(&self, sequence: u64) {
        if let Some(observer) = self.outcome_observer.get() {
            *observer.outcome.lock() = TransactionOutcome::Committed { sequence };
        }
    }

    pub(super) fn receipt_recovery_required(&self, sequence: u64) {
        // Preserve uncertainty even if the first observer is requested only
        // after a buffered commit returned an error.
        *self.observe_outcome().0.outcome.lock() =
            TransactionOutcome::RecoveryRequired { sequence };
    }

    pub(super) fn receipt_aborted(&self) {
        if let Some(observer) = self.outcome_observer.get() {
            let mut outcome = observer.outcome.lock();
            *outcome = match *outcome {
                TransactionOutcome::Pending => TransactionOutcome::Aborted,
                TransactionOutcome::Committing { sequence } => {
                    TransactionOutcome::RecoveryRequired { sequence }
                }
                other => other,
            };
        }
    }
}
