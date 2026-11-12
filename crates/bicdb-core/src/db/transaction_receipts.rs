//! Read-only receipts for transaction outcomes and buffered write survival.
//!
//! Receipts contain no database pointer or mutation authority. They are local
//! observations, not durable commit certificates, and cannot resolve an
//! uncertain decision across database recovery.
use super::{BicDbError, Result, Transaction, TransactionId, TxState};
use parking_lot::Mutex;
use std::collections::HashMap;
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
    /// The native distributed admission may have persisted a Commit decision
    /// before this DB allocated any local application-WAL sequence. This is
    /// neither an Aborted outcome nor a visibility floor.
    RecoveryRequiredBeforeLocalCommit,
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
    tags: Mutex<HashMap<(u128, u128), Vec<WriteTag>>>,
}

#[derive(Debug)]
struct WriteTag {
    receipt: TransactionWriteReceipt,
    value: u64,
}

impl Default for ReceiptJournal {
    fn default() -> Self {
        Self {
            last_id: 0,
            writes: Vec::new(),
            prune_at: 64,
            tags: Mutex::new(HashMap::new()),
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

    /// Attach small, transaction-local derived state to an actual buffered
    /// record write. Namespace/key are opaque integers; the tag contains no
    /// callbacks, database pointers or mutation authority. Registration neither
    /// writes a row nor acquires a lock. It follows the observed write even if
    /// registered after a savepoint for an earlier write.
    ///
    /// Tags are not WAL data or commit certificates. They are released on a
    /// terminal outcome and cannot be transferred between transactions. A
    /// repeated annotation of the same key/write replaces its previous value;
    /// earlier write versions remain available for savepoint rollback.
    pub fn annotate_record_write(
        &mut self,
        collection: &str,
        record_id: &str,
        namespace: u128,
        key: u128,
        value: u64,
    ) -> Result<TransactionWriteReceipt> {
        let receipt = self.observe_record_write(collection, record_id)?;
        let notes = self
            .write_receipts
            .as_mut()
            .expect("registered receipt")
            .tags
            .get_mut();
        let history = notes.entry((namespace, key)).or_default();
        history.retain(|note| note.receipt.rollback_boundary().is_none());
        if history
            .last()
            .is_some_and(|last| last.receipt.write_len() > receipt.write_len())
        {
            return Err(BicDbError::TransactionConflict(
                "write tags must follow buffered write order".into(),
            ));
        }
        if history
            .last()
            .is_some_and(|last| last.receipt.write_len() == receipt.write_len())
        {
            history.pop();
        }
        history.push(WriteTag {
            receipt: receipt.clone(),
            value,
        });
        Ok(receipt)
    }

    /// Return the newest surviving tag in this pending transaction. A missing
    /// tag is not a statement about durable data; callers must still inspect
    /// the current row and enforce their own authorization and lock contracts.
    pub fn record_write_tag(&self, namespace: u128, key: u128) -> Result<Option<u64>> {
        if self.state != TxState::Pending
            || self
                .outcome_observer
                .get()
                .is_some_and(|observer| *observer.outcome.lock() != TransactionOutcome::Pending)
        {
            return Err(BicDbError::TransactionNotPending);
        }
        Ok(self.write_receipts.as_ref().and_then(|journal| {
            journal
                .tags
                .lock()
                .get(&(namespace, key))
                .and_then(|history| {
                    history
                        .iter()
                        .rev()
                        .find(|note| note.receipt.rollback_boundary().is_none())
                        .map(|note| note.value)
                })
        }))
    }

    fn clear_write_tags(&self) {
        if let Some(journal) = self.write_receipts.as_ref() {
            *journal.tags.lock() = HashMap::new();
        }
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
        let tags = journal.tags.get_mut();
        tags.retain(|_, history| {
            history.retain(|note| note.receipt.rollback_boundary().is_none());
            !history.is_empty()
        });
        if tags.capacity() > tags.len().max(32).saturating_mul(4) {
            tags.shrink_to(tags.len().max(32));
        }
    }

    pub(super) fn receipt_commit_started(&self, sequence: u64) {
        if let Some(observer) = self.outcome_observer.get() {
            *observer.outcome.lock() = TransactionOutcome::Committing { sequence };
        }
    }

    pub(super) fn receipt_commit_finished(&self, sequence: u64) {
        self.clear_write_tags();
        if let Some(observer) = self.outcome_observer.get() {
            *observer.outcome.lock() = TransactionOutcome::Committed { sequence };
        }
    }

    pub(super) fn receipt_recovery_required(&self, sequence: u64) {
        self.clear_write_tags();
        // Preserve uncertainty even if the first observer is requested only
        // after a buffered commit returned an error.
        *self.observe_outcome().0.outcome.lock() =
            TransactionOutcome::RecoveryRequired { sequence };
    }

    pub(super) fn receipt_native_admission_unknown(&self) {
        self.clear_write_tags();
        *self.observe_outcome().0.outcome.lock() =
            TransactionOutcome::RecoveryRequiredBeforeLocalCommit;
    }

    /// The commit was withdrawn before any byte of it was written: it did
    /// not commit.
    pub(super) fn receipt_commit_withdrawn(&self) {
        self.clear_write_tags();
        if let Some(observer) = self.outcome_observer.get() {
            *observer.outcome.lock() = TransactionOutcome::Aborted;
        }
    }

    pub(super) fn receipt_aborted(&self) {
        self.clear_write_tags();
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
