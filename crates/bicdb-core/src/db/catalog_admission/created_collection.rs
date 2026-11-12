//! One live owner's undo capability for a namespace Native actually created.
use super::*;
use std::sync::atomic::AtomicBool;

/// An opaque, local creation witness. It is neither serializable nor a lookup
/// by SQL name/UUID. Clones share consumption; the strong incarnation witness
/// prevents allocator/name reuse from redirecting an old undo to a new backing.
#[derive(Clone, Debug)]
pub struct CatalogCreatedCollectionUndo {
    inner: Arc<CreatedCollection>,
}

#[derive(Debug)]
struct CreatedCollection {
    scope: CatalogScopeToken,
    incarnation: Arc<()>,
    namespace: Option<Uuid>,
    storage: String,
    consumed: AtomicBool,
}

fn conflict(message: &str) -> BicDbError {
    BicDbError::TransactionConflict(message.into())
}

impl BicDb {
    /// Create a fresh staging namespace for this pending native owner and
    /// reserve both its staging and intended logical spelling. The host must
    /// separately persist its DDL intent before this call and reserve every
    /// metadata dependency. Ordinary database execution locking still applies.
    /// There is deliberately no API to mint a witness for an existing backing.
    pub fn create_collection_for_transaction_undo(
        &mut self,
        owner: &Transaction,
        staging: &str,
        logical: &str,
    ) -> Result<CatalogCreatedCollectionUndo> {
        self.ensure_writable("create owned catalog namespace")?;
        if !Arc::ptr_eq(&self.catalog_admission, &owner.catalog_admission)
            || owner.state != TxState::Pending
            || owner.catalog_actor_scope.is_some()
        {
            return Err(conflict(
                "creation requires this database's live business owner",
            ));
        }
        validate_collection_name(staging)?;
        validate_collection_name(logical)?;
        let already_observed = {
            let bindings = owner.collection_incarnations.lock();
            bindings.contains_key(staging) || bindings.contains_key(logical)
        };
        if self.collections.contains_key(staging)
            || self.collections.contains_key(logical)
            || self.storage_name_for_new(staging).is_some()
            || already_observed
        {
            return Err(conflict(
                "creation requires unobserved, unused namespace names",
            ));
        }
        let scope = owner.reserve_catalog_collections(&[staging.into(), logical.into()])?;
        if let Err(error) = self.create_collection(staging) {
            // No binding is forgotten on failure. A partial durable operation
            // retains the host's independent durable intent for startup undo.
            let _ = owner.release_catalog_scope(&scope);
            return Err(error);
        }
        let (incarnation, namespace, storage) = {
            let current = self.collection_state(staging)?;
            (
                Arc::clone(&current.incarnation),
                current.meta.local_namespace_id,
                current.meta.storage().to_string(),
            )
        };
        {
            let mut state = self.catalog_admission.state.write();
            self.catalog_admission.validate_token(&state, &scope)?;
            // Names were reserved before creation, so no other owner could
            // observe the new namespace before its physical identity is added.
            state
                .scopes
                .get_mut(&scope.serial)
                .unwrap()
                .incarnations
                .push(Arc::clone(&incarnation));
        }
        Ok(CatalogCreatedCollectionUndo {
            inner: Arc::new(CreatedCollection {
                scope,
                incarnation,
                namespace,
                storage,
                consumed: AtomicBool::new(false),
            }),
        })
    }

    /// Read-only identity preflight for trusted host undo, including full
    /// rollback after the business owner has gone. This grants no mutation or
    /// observation-retirement authority and never synthesizes a missing proof.
    pub fn check_created_collection_undo_identity(
        &self,
        receipt: &CatalogCreatedCollectionUndo,
    ) -> Result<String> {
        self.check_recovery_state()?;
        let proof = &receipt.inner;
        if proof.consumed.load(AtomicOrdering::Acquire)
            || !Weak::ptr_eq(
                &proof.scope.admission,
                &Arc::downgrade(&self.catalog_admission),
            )
        {
            return Err(conflict("creation undo proof is foreign or consumed"));
        }
        let mut found = None;
        for (name, current) in &self.collections {
            let current = current.read();
            if Arc::ptr_eq(&current.incarnation, &proof.incarnation) {
                if found.is_some()
                    || current.meta.storage() != proof.storage
                    || current.meta.local_namespace_id != proof.namespace
                {
                    return Err(conflict(
                        "creation undo backing identity changed or is ambiguous",
                    ));
                }
                found = Some(name.clone());
            }
        }
        found.ok_or_else(|| conflict("creation undo backing is missing or replaced"))
    }

    /// Validate before the host changes any associated metadata. Surviving
    /// writes and row locks forbid removal; rollback must release them first.
    pub fn check_created_collection_undo_owner(
        &self,
        owner: &Transaction,
        receipt: &CatalogCreatedCollectionUndo,
    ) -> Result<String> {
        let name = self.check_created_collection_undo_identity(receipt)?;
        if !Arc::ptr_eq(&self.catalog_admission, &owner.catalog_admission) {
            return Err(conflict("creation undo has a foreign live owner"));
        }
        owner.check_catalog_scope_owner(&receipt.inner.scope)?;
        {
            let state = self.catalog_admission.state.read();
            if self
                .catalog_admission
                .has_overlapping_children(&state, receipt.inner.scope.serial)
            {
                return Err(conflict(
                    "creation undo still has active internal catalog work",
                ));
            }
        }
        let bindings = owner.collection_incarnations.lock();
        if bindings
            .get(&name)
            .is_some_and(|binding| !Arc::ptr_eq(&binding.incarnation, &receipt.inner.incarnation))
        {
            return Err(conflict(
                "creation undo owner observed a different namespace",
            ));
        }
        let affected = |candidate: &str| {
            candidate == name
                || bindings.get(candidate).is_some_and(|binding| {
                    Arc::ptr_eq(&binding.incarnation, &receipt.inner.incarnation)
                })
        };
        if owner.writes.iter().any(|write| affected(&write.collection))
            || owner
                .write_index
                .iter()
                .any(|(collection, rows)| affected(collection) && !rows.is_empty())
            || owner
                .locked_keys
                .lock()
                .held
                .iter()
                .any(|(collection, _)| affected(collection))
        {
            return Err(conflict(
                "creation undo has surviving pending writes or row locks",
            ));
        }
        Ok(name)
    }

    /// Remove only the Native-created incarnation and retire only its creator's
    /// matching observations. All checks precede durable removal. Failed or
    /// foreign proofs leave every binding intact; generic savepoint rollback
    /// continues to preserve ordinary observations.
    pub fn undo_created_collection(
        &mut self,
        owner: &mut Transaction,
        receipt: &CatalogCreatedCollectionUndo,
    ) -> Result<()> {
        self.ensure_writable("undo owned catalog namespace")?;
        let name = self.check_created_collection_undo_owner(owner, receipt)?;
        if !self.drop_collection(&name)? {
            return Err(conflict("creation undo did not remove its backing"));
        }
        receipt.inner.consumed.store(true, AtomicOrdering::Release);
        let mut bindings = owner.collection_incarnations.lock();
        let removed = bindings
            .iter()
            .filter_map(|(name, binding)| {
                Arc::ptr_eq(&binding.incarnation, &receipt.inner.incarnation)
                    .then_some(name.clone())
            })
            .collect::<Vec<_>>();
        let mut versions = owner.observed_record_versions.lock();
        let mut images = owner.read_images.lock();
        for name in removed {
            bindings.remove(&name);
            versions.remove(&name);
            if let Some(rows) = images.rows.remove(&name) {
                images.len -= rows.len();
            }
        }
        drop(images);
        drop(versions);
        drop(bindings);
        // This private token is never handed out, retained or mutated by SQL.
        // The live owner and absence of children were checked before removal;
        // finalization below cannot fail after the durable effect succeeded.
        let mut state = self.catalog_admission.state.write();
        state.scopes.remove(&receipt.inner.scope.serial);
        self.catalog_admission
            .active
            .store(state.scopes.len(), AtomicOrdering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(mode: StorageMode) -> DbConfig {
        DbConfig::default().with_fsync(true).with_storage_mode(mode)
    }

    fn observations(owner: &Transaction) -> Vec<(String, Arc<()>)> {
        let mut result = owner
            .collection_incarnations
            .lock()
            .iter()
            .map(|(name, binding)| (name.clone(), Arc::clone(&binding.incarnation)))
            .collect::<Vec<_>>();
        result.sort_by(|left, right| left.0.cmp(&right.0));
        result
    }

    fn unchanged(owner: &Transaction, before: &[(String, Arc<()>)]) {
        let after = observations(owner);
        assert_eq!(before.len(), after.len());
        for ((before_name, before_identity), (after_name, after_identity)) in
            before.iter().zip(after)
        {
            assert_eq!(before_name, &after_name);
            assert!(Arc::ptr_eq(before_identity, &after_identity));
        }
    }

    #[test]
    fn created_namespace_cycles_preserve_other_observations_pending_work_and_acknowledgements() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let config = config(mode);
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            db.activate_local_namespace_identity_v1().unwrap();
            for name in ["pending", "observed", "independent"] {
                db.create_collection(name).unwrap();
            }
            db.insert("observed", Record::new("old")).unwrap();
            let mut owner = db.begin_transaction().unwrap();
            owner.insert("pending", Record::new("retained")).unwrap();
            assert!(owner.get("observed", "old").unwrap().is_some());
            let original_observations = observations(&owner);
            let mut independent = db.begin_transaction().unwrap();
            independent
                .insert("independent", Record::new("external-ack"))
                .unwrap();
            independent.commit().unwrap();
            let mut namespaces = BTreeSet::new();
            for cycle in 0..3 {
                let mark = owner.rollback_mark();
                let staging = format!("stage_{cycle}");
                let receipt = db
                    .create_collection_for_transaction_undo(&owner, &staging, "replacement")
                    .unwrap();
                namespaces.insert(receipt.inner.namespace.unwrap());
                db.rename_collection(&staging, "replacement").unwrap();
                owner
                    .insert("replacement", Record::new("temporary"))
                    .unwrap();
                assert!(db.undo_created_collection(&mut owner, &receipt).is_err());
                assert!(owner.get("replacement", "temporary").unwrap().is_some());
                owner.rollback_to_mark(mark).unwrap();
                db.undo_created_collection(&mut owner, &receipt).unwrap();
                unchanged(&owner, &original_observations);
                assert_eq!(owner.write_len(), 1);
                assert!(owner.get("pending", "retained").unwrap().is_some());
                assert!(db.get("independent", "external-ack").unwrap().is_some());
            }
            assert_eq!(namespaces.len(), 3);
            // An ordinary earlier read must still reject a later same-name
            // replacement. A generic savepoint reset would lose this witness.
            db.drop_collection("observed").unwrap();
            db.create_collection("observed").unwrap();
            assert!(owner.get("observed", "old").is_err());
            owner.commit().unwrap();
            db.close().unwrap();
            for _ in 0..2 {
                let mut reopened = BicDb::open_with_config(root.path(), config.clone()).unwrap();
                assert!(reopened.get("pending", "retained").unwrap().is_some());
                assert!(reopened
                    .get("independent", "external-ack")
                    .unwrap()
                    .is_some());
                assert!(!reopened
                    .collections()
                    .iter()
                    .any(|row| row.name == "replacement" || row.name.starts_with("stage_")));
                reopened.close().unwrap();
            }
        }
    }

    #[test]
    fn wrong_owner_database_missing_replaced_and_consumed_proofs_leave_bindings_intact() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let foreign_root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
            let mut foreign = BicDb::open_with_config(foreign_root.path(), config(mode)).unwrap();
            db.activate_local_namespace_identity_v1().unwrap();
            let mut owner = db.begin_transaction().unwrap();
            let proof = db
                .create_collection_for_transaction_undo(&owner, "stage", "replacement")
                .unwrap();
            db.rename_collection("stage", "replacement").unwrap();
            assert!(owner.get("replacement", "none").unwrap().is_none());
            let before = observations(&owner);
            let mut other = db.begin_transaction().unwrap();
            assert!(db.undo_created_collection(&mut other, &proof).is_err());
            unchanged(&owner, &before);
            assert!(foreign.undo_created_collection(&mut owner, &proof).is_err());
            unchanged(&owner, &before);
            let mut foreign_owner = foreign.begin_transaction().unwrap();
            assert!(db
                .undo_created_collection(&mut foreign_owner, &proof)
                .is_err());
            unchanged(&owner, &before);
            foreign_owner.rollback().unwrap();
            other.rollback().unwrap();
            let clone = proof.clone();
            db.undo_created_collection(&mut owner, &proof).unwrap();
            let next = db
                .create_collection_for_transaction_undo(&owner, "stage", "replacement")
                .unwrap();
            db.rename_collection("stage", "replacement").unwrap();
            assert!(owner.get("replacement", "none").unwrap().is_none());
            let before = observations(&owner);
            assert!(db.undo_created_collection(&mut owner, &clone).is_err());
            unchanged(&owner, &before);
            // A live capability cannot be manufactured for an existing name.
            assert!(db
                .create_collection_for_transaction_undo(&owner, "stage2", "replacement")
                .is_err());
            unchanged(&owner, &before);
            db.drop_collection("replacement").unwrap();
            assert!(db.undo_created_collection(&mut owner, &next).is_err());
            unchanged(&owner, &before);
            db.create_collection("replacement").unwrap();
            assert!(db.undo_created_collection(&mut owner, &next).is_err());
            unchanged(&owner, &before);
            assert!(owner.get("replacement", "none").is_err());
            owner.rollback().unwrap();
            foreign.close().unwrap();
            db.close().unwrap();
        }
    }

    #[test]
    fn surviving_lock_without_buffered_write_refuses_removal_until_exact_rollback() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config(mode)).unwrap();
            let mut owner = db.begin_transaction().unwrap();
            let proof = db
                .create_collection_for_transaction_undo(&owner, "stage", "replacement")
                .unwrap();
            db.rename_collection("stage", "replacement").unwrap();
            owner.get("replacement", "locked").unwrap();
            let mark = owner.rollback_mark();
            db.lock_tx_record_with_attempts(
                owner.id,
                "replacement",
                "locked",
                0,
                false,
                false,
                &owner.locked_keys,
            )
            .unwrap();
            assert_eq!(owner.write_len(), 0);
            assert_eq!(owner.lock_len(), 1);
            let before = observations(&owner);
            assert!(db.undo_created_collection(&mut owner, &proof).is_err());
            unchanged(&owner, &before);
            assert!(db.check_created_collection_undo_identity(&proof).is_ok());
            owner.rollback_to_mark(mark).unwrap();
            db.undo_created_collection(&mut owner, &proof).unwrap();
            owner.commit().unwrap();
            db.close().unwrap();
        }
    }
}
