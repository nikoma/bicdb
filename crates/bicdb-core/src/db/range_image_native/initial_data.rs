//! INITIAL-only selected-range eligibility. This is neither a zero counter nor
//! a generic genesis/restore factory: the actual retained initial factory and
//! current native anchor must still be the ORIGINAL protected genesis.
use super::*;

pub(crate) struct NativeInitialDataGenesis {
    birth: NativeDatabaseBirth,
    database_incarnation: String,
    root: PathBuf,
    range: RangeDescriptor,
    receipt: String,
    history: String,
}
fn stable(range: &RangeDescriptor) -> RangeDescriptor {
    let mut range = range.clone();
    range.approximate_bytes = 0;
    range.approximate_qps = 0;
    range.replicas.sort_by_key(|replica| replica.id);
    range
}
impl NativeInitialDataGenesis {
    pub(crate) fn range(&self) -> &RangeDescriptor {
        &self.range
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.history
    }
    pub(crate) fn require_current(
        &self,
        database: &BicDb,
        topology: &crate::ClusterTopology,
    ) -> Result<()> {
        database.check_recovery_state()?;
        let birth = database
            .read_native_birth()?
            .ok_or_else(|| invalid("initial data birth missing"))?;
        let current = topology
            .range_by_id(self.range.id)
            .ok_or_else(|| invalid("initial range missing"))?;
        if birth != self.birth
            || database.incarnation_id() != self.database_incarnation
            || database.path.canonicalize()? != self.root
            || stable(current) != self.range
            || topology.cluster_id != birth.spec.cluster_id
            || topology.range_image_bootstrap_id != Some(birth.spec.bootstrap_id)
            || birth.format_version != 2
            || birth.metadata_initialization_version != Some(1)
            || birth.abort_protocol_version != Some(1)
        {
            return Err(invalid(
                "initial data factory/range/domain changed; use native recovery",
            ));
        }
        for replica in &self.range.replicas {
            let original = birth
                .spec
                .initial_members
                .get(&replica.node_id)
                .ok_or_else(|| invalid("initial data voter is not an original member"))?;
            let member = topology
                .nodes
                .get(&replica.node_id)
                .ok_or_else(|| invalid("initial data voter absent"))?;
            if member.incarnation != original.incarnation
                || member.tls_certificate_sha256 != original.tls_certificate_sha256
                || member.pending_tls_certificate_sha256 != original.pending_tls_certificate_sha256
                || member.lifecycle != crate::distribution::ClusterNodeLifecycle::Active
            {
                return Err(invalid("initial data voter identity changed"));
            }
        }
        let source = database.current_native_range_source_lineage(self.range.id)?;
        if !matches!(&source._anchor, NativeRangeImageAnchor::Genesis(_))
            || source.prefix.is_some()
            || source.range_index() != 0
            || stable(source.base_range()) != self.range
            || source.native_receipt_sha256() != self.receipt
            || source.logical_genesis_sha256() != self.history
            || source.logical_history_sha256() != self.history
        {
            return Err(invalid("initial data requires ORIGINAL protected native genesis; nonempty/image recovery is separate"));
        }
        Ok(())
    }
}
impl BicDb {
    pub(crate) fn observe_initial_data_genesis(
        &self,
        factory: &FreshRangeGenesisAuthority,
        range: &RangeDescriptor,
        topology: &crate::ClusterTopology,
    ) -> Result<NativeInitialDataGenesis> {
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("initial data has no birth"))?;
        if birth != factory.birth
            || self.path.canonicalize()? != factory.root
            || !birth
                .spec
                .initial_ranges
                .iter()
                .any(|r| stable(r) == stable(range))
        {
            return Err(invalid(
                "initial data requires actual retained initial range factory",
            ));
        }
        // This actual native admission checks ordered READY and its complete
        // immutable genesis inventory; arbitrary R0 or an empty raw log cannot.
        self.native_recovery_admission(
            &birth.spec.cluster_id,
            birth.spec.bootstrap_id,
            &birth.spec.node_id,
            birth.spec.member_incarnation,
        )?;
        let source = self.current_native_range_source_lineage(range.id)?;
        let observed = NativeInitialDataGenesis {
            birth,
            database_incarnation: self.incarnation_id().to_string(),
            root: factory.root.clone(),
            range: stable(range),
            receipt: source.native_receipt_sha256().into(),
            history: source.logical_genesis_sha256().into(),
        };
        observed.require_current(self, topology)?;
        Ok(observed)
    }
}
