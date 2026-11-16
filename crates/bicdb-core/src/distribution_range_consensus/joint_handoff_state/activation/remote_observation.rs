//! Current healthy catalogue DATA from a real native read scope. An existing
//! activation additionally requires its retained actual verified IO owner.
use super::*;
impl RangeWriteStore {
    /// Distinct post-Open read under a genuine native historical scope and
    /// actual release-state observation. This never resolves uncertain IO or
    /// treats an old owner/candidate as the current full head.
    #[cfg(feature = "tls-replication")]
    pub(crate) fn observe_remote_release_catalogue(
        &self,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
        fact: &NativeJointCatalogueActivationFact,
    ) -> Result<(String, String, u64)> {
        fact.require_current_with_historical_cut_scope(scope, self)?;
        self.require_native_catalog_durability(scope.database().data_path())?;
        self.require_current_catalogue_io_returned_ok()?;
        let generation = self.current_catalogue_write_generation();
        let mut bytes = Vec::new();
        open_range_catalog_no_follow(&self.root.join(DEFAULT_RANGE_WRITE_LOG))?
            .take(64 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(range_write_error(
                "remote release read exceeds existing64MiB catalogue head",
            ));
        }
        if serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()? != self.catalog {
            return Err(range_write_error(
                "remote release actual current catalogue disk/memory differ",
            ));
        }
        fact.require_current_with_historical_cut_scope(scope, self)?;
        self.require_current_catalogue_io_generation(generation)?;
        Ok((
            hex::encode(Sha256::digest(&bytes)),
            fact.activation_sha256()?,
            generation,
        ))
    }

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
