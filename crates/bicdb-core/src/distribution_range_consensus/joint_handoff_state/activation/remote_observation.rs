//! Current healthy catalogue DATA from a real native read scope. An existing
//! activation additionally requires its retained actual verified IO owner.
use super::*;
impl RangeWriteStore {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn observe_remote_open_catalogue(
        &self,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
        native: &Arc<crate::db::NativeJointServingBridgeReceipt>,
        domain: &crate::db::NativeJointWriterDomain,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<(String, Option<String>, u64)> {
        scope.require_native(native)?;
        let db = scope.database();
        self.require_native_catalog_durability(db.data_path())?;
        self.require_current_catalogue_io_returned_ok()?;
        let generation = self.current_catalogue_write_generation();
        self.require_native_joint_host_base(db, native.data().old_range().id)?;
        let fact = self.observe_native_joint_activation_fact_with_scope(scope, native.clone())?;
        let activation = if let Some(fact) = fact {
            self.require_joint_open_phase_not_released(&fact)?;
            self.require_known_activation_read_projection(db, native, domain, frozen)?;
            Some(fact.activation_sha256()?)
        } else {
            None
        };
        // Existing per-head bound. These bytes prove the actual current
        // healthy head matches memory, never an earlier failed fsync result.
        let mut bytes = Vec::new();
        open_range_catalog_no_follow(&self.root.join(DEFAULT_RANGE_WRITE_LOG))?
            .take(64 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "remote Open read exceeds existing64MiB catalogue head",
            ));
        }
        if serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()? != self.catalog {
            return Err(range_write_error(
                "remote Open actual current catalogue disk/memory differ",
            ));
        }
        self.require_current_catalogue_io_generation(generation)?;
        Ok((hex::encode(Sha256::digest(&bytes)), activation, generation))
    }
}
