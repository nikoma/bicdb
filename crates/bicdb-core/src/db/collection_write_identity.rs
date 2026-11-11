//! Transaction observations bind to an exact live collection incarnation.
use super::*;

impl Transaction {
    /// A logical name can be dropped/reused without retaining its old backing.
    /// Pin the existing runtime capability, rather than a name, path or address
    /// that a replacement can inherit. Catalog-only moves retain this token.
    pub(super) fn bind_collection_incarnation(&self, collection: &str) -> Result<()> {
        let db = self.db_ref();
        db.check_recovery_state()?;
        let result = (|| {
            self.catalog_admission.bind(
                self.id,
                self.catalog_actor_scope,
                &self.collection_incarnations,
                collection,
                || {
                    db.ensure_collection(collection)?;
                    Ok(Arc::clone(&db.collection_state(collection)?.incarnation))
                },
            )
        })();
        db.check_recovery_state()?;
        result
    }
}

impl BicDb {
    /// Run before any repair, index mutation, WAL decision or record apply.
    /// Read-only observations need not remain in the catalog at COMMIT, but
    /// every buffered write must still target its original live backing.
    pub(super) fn validate_transaction_collection_incarnations(
        &self,
        tx: &Transaction,
    ) -> Result<()> {
        self.check_recovery_state()?;
        let result = (|| {
            let bindings = tx.collection_incarnations.lock();
            for collection in tx.write_index.keys() {
                let current = self.collection_state(collection).ok();
                let same = bindings.get(collection).is_some_and(|expected| {
                    current
                        .as_ref()
                        .is_some_and(|state| Arc::ptr_eq(expected, &state.incarnation))
                });
                if !same {
                    return Err(BicDbError::TransactionConflict(format!(
                        "buffered write collection `{collection}` no longer has its original identity"
                    )));
                }
            }
            Ok(())
        })();
        self.check_recovery_state()?;
        result
    }
}
