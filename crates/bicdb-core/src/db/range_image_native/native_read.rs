//! Actual local native physical read state. No source freeze, read quorum,
//! metadata authority or MVCC snapshot is created by this observer.
use super::*;
use crate::distribution_native_read::{canonical_range, NativeRangeReadScope};
use crate::distribution_range_consensus::{NativeRangeReadCatalogue, NativeReadCatalogueDecision};
use crate::distribution_range_mutation::NativeRangeReadStamp;

/// Target-LOCAL data visibility bound from actual native receipts. No public
/// constructor/Deserialize: a numeric R/source sequence cannot become this.
#[derive(Debug)]
pub(crate) struct NativeReadLocalDbSequence(u64);
impl NativeReadLocalDbSequence {
    pub(crate) fn get(&self) -> u64 {
        self.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct PhysicalPoint {
    database_incarnation: String,
    birth_id: Uuid,
    birth_sha256: String,
    node_id: ClusterNodeId,
    member_incarnation: u64,
    range_index: u64,
    h0: String,
    h: String,
    receipt: String,
    local_b: u64,
    history_generation: Uuid,
    global_generation: Uuid,
    range_generation: Uuid,
    serving_commitment: Option<String>,
}
#[derive(Debug)]
pub(crate) struct NativeRangeReadPhysical {
    native: NativeRecoveryAdmission,
    scope: NativeRangeReadScope,
    point: PhysicalPoint,
    local_b: NativeReadLocalDbSequence,
    stamp: NativeRangeReadStamp,
    directory: std::sync::Weak<bicdb_page::DirectoryLock>,
    health: Arc<bicdb_page::DurabilityHealth>,
    store_open_id: Uuid,
    catalogue_sha256: String,
    cancellation: CancellationToken,
}
impl NativeRangeReadPhysical {
    /// Compare a lower peer point with actual immutable local history. This
    /// does not manufacture a local visibility bound or admit that peer.
    pub(crate) fn require_ancestor(
        &self,
        db: &BicDb,
        index: u64,
        h0: &str,
        history: &str,
    ) -> Result<()> {
        self.require_lifetime()?;
        self.native.require_current(db)?;
        let _journal = db.try_native_image_journal()?;
        if h0 != self.point.h0 || index > self.point.range_index {
            return Err(invalid("peer point is outside actual local native history"));
        }
        let expected = if index == 0 {
            self.point.h0.clone()
        } else {
            db.read_native_history_entry(self.scope.range.id, self.point.history_generation, index)?
                .commitment_sha256
        };
        if expected != history {
            return Err(invalid(
                "lower peer conflicts with actual local native history",
            ));
        }
        self.require_lifetime()
    }
    pub(crate) fn local_visibility(&self) -> &NativeReadLocalDbSequence {
        &self.local_b
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.point.range_index
    }
    pub(crate) fn native_admission(&self) -> &NativeRecoveryAdmission {
        &self.native
    }
    pub(crate) fn require_lifetime(&self) -> Result<()> {
        self.cancellation.check()?;
        self.stamp.require_current()?;
        if self.directory.upgrade().is_none() {
            return Err(invalid("native read database lifetime ended"));
        }
        self.health.check().map_err(crate::error::durability_error)
    }
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        catalogue: &NativeRangeReadCatalogue,
    ) -> Result<()> {
        self.require_lifetime()?;
        self.require_catalogue_binding(catalogue)?;
        self.native.require_current(db)?;
        let _journal = db.try_native_image_journal()?;
        let point = db.native_read_physical_point(&self.scope, catalogue, &self.cancellation)?;
        if point != self.point {
            return Err(invalid("actual native read physical point changed"));
        }
        self.require_lifetime()
    }
    pub(crate) fn facts(
        &self,
        challenge: Uuid,
        catalogue: &NativeRangeReadCatalogue,
    ) -> Result<crate::distribution_native_read::NativeRangeReadFacts> {
        self.require_lifetime()?;
        self.require_catalogue_binding(catalogue)?;
        Ok(crate::distribution_native_read::NativeRangeReadFacts {
            evidence_version: crate::distribution_native_read::NATIVE_RANGE_READ_VERSION,
            native_writer_enforcement_version: self.native.writer_enforcement_version(),
            scope_sha256: self.scope.checksum_sha256()?,
            challenge,
            node_id: self.point.node_id.clone(),
            member_incarnation: self.point.member_incarnation,
            store_open_id: catalogue.store_open_id(),
            native_database_incarnation: self.point.database_incarnation.clone(),
            native_birth_id: self.point.birth_id,
            native_birth_sha256: self.point.birth_sha256.clone(),
            range_index: self.point.range_index,
            logical_genesis_sha256: self.point.h0.clone(),
            logical_history_sha256: self.point.h.clone(),
            native_receipt_sha256: self.point.receipt.clone(),
            local_visibility_db_sequence: self.local_b.get(),
            native_history_generation: self.point.history_generation,
            native_global_image_generation: self.point.global_generation,
            native_range_image_generation: self.point.range_generation,
            native_serving_record_commitment_sha256: self.point.serving_commitment.clone(),
            catalogue_sha256: catalogue.checksum_sha256().into(),
        })
    }
    pub(crate) fn matches_scope(&self, scope: &NativeRangeReadScope) -> Result<bool> {
        Ok(self.scope.checksum_sha256()? == scope.checksum_sha256()?)
    }
    fn require_catalogue_binding(&self, catalogue: &NativeRangeReadCatalogue) -> Result<()> {
        if self.store_open_id != catalogue.store_open_id()
            || self.catalogue_sha256 != catalogue.checksum_sha256()
            || self.point.range_index != catalogue.range_index()
        {
            return Err(invalid(
                "actual native physical/catalogue observations are not the same point",
            ));
        }
        Ok(())
    }
}
impl BicDb {
    fn native_read_physical_point(
        &self,
        scope: &NativeRangeReadScope,
        catalogue: &NativeRangeReadCatalogue,
        cancellation: &CancellationToken,
    ) -> Result<PhysicalPoint> {
        cancellation.check()?;
        scope.validate()?;
        self.check_recovery_state()?;
        let fingerprint = self.schema_compatibility_fingerprint()?;
        let lineage = self.current_native_range_source_lineage(scope.range.id)?;
        let actual_members = lineage.native_write_members()?;
        if lineage.cluster_id() != &scope.cluster_id
            || lineage.bootstrap_id() != scope.bootstrap_id
            || canonical_range(lineage.native_write_range()?) != canonical_range(&scope.range)
            || lineage.schema_sha256() != scope.schema_sha256
            || fingerprint.sha256 != scope.schema_sha256
            || lineage.restore_activation() != scope.restore_activation.as_ref()
            || !scope
                .members
                .get(lineage.local_node_id())
                .is_some_and(|m| m.incarnation == lineage.local_member_incarnation())
            || scope.members.iter().any(|(node, m)| {
                actual_members.get(node).is_none_or(|a| {
                    a.incarnation != m.incarnation
                        || a.tls_certificate_sha256 != m.active_certificate_sha256
                        || a.pending_tls_certificate_sha256 != m.pending_certificate_sha256
                })
            })
            || (catalogue.is_recovered() != (lineage.serving.is_some() || lineage.joint.is_some()))
            || (!catalogue.is_recovered() && lineage.base.installed)
            || catalogue.range_index() != lineage.range_index()
        {
            return Err(invalid(
                "native read scope has no matching actual physical/current-serving domain",
            ));
        }
        for (index, expected) in catalogue.decisions() {
            cancellation.check()?;
            let actual = self.read_native_history_entry(
                scope.range.id,
                lineage.history_generation(),
                *index,
            )?;
            let matches = match (expected, &actual.outcome) {
                (
                    NativeReadCatalogueDecision::Applied(expected),
                    NativeLogicalHistoryOutcomeV1::Applied {
                        command_checksum_sha256,
                    },
                ) => expected == command_checksum_sha256,
                (NativeReadCatalogueDecision::Aborted, NativeLogicalHistoryOutcomeV1::Aborted) => {
                    true
                }
                _ => false,
            };
            if !matches {
                return Err(invalid(
                    "actual native read history contradicts retained raw/sparse decision",
                ));
            }
        }
        let serving_commitment = if let Some(joint) = &lineage.joint {
            Some(joint.native_bridge_sha256().to_owned())
        } else {
            lineage
                .serving
                .as_ref()
                .map(|s| {
                    Ok::<_, BicDbError>(hex::encode(sha2::Sha256::digest(serde_json::to_vec(&(
                        "bicdb-native-read-serving-v1",
                        s,
                    ))?)))
                })
                .transpose()?
        };
        Ok(PhysicalPoint {
            database_incarnation: self.incarnation.clone(),
            birth_id: lineage.birth.birth_id,
            birth_sha256: lineage.birth.checksum_sha256.clone(),
            node_id: lineage.local_node_id().clone(),
            member_incarnation: lineage.local_member_incarnation(),
            range_index: lineage.range_index(),
            h0: lineage.logical_genesis_sha256().into(),
            h: lineage.logical_history_sha256().into(),
            receipt: lineage.native_receipt_sha256().into(),
            local_b: lineage.actual_local_visibility_sequence(),
            history_generation: lineage.history_generation(),
            global_generation: lineage.base.global_image_generation,
            range_generation: lineage.base.range_image_generation,
            serving_commitment,
        })
    }
    pub(crate) fn observe_native_range_read_physical(
        &self,
        scope: &NativeRangeReadScope,
        catalogue: &NativeRangeReadCatalogue,
        native: NativeRecoveryAdmission,
        parent: &CancellationToken,
    ) -> Result<NativeRangeReadPhysical> {
        parent.check()?;
        scope.validate()?;
        let stamp = self
            .range_mutation_gate()
            .observe_native_read_stamp(scope.range.id)?;
        let _journal = self.try_native_image_journal()?;
        native.require_current(self)?;
        let point = self.native_read_physical_point(scope, catalogue, parent)?;
        if native.birth_id() != point.birth_id || native.birth_sha256() != point.birth_sha256 {
            return Err(invalid(
                "actual native read birth changed during observation",
            ));
        }
        let directory = Arc::downgrade(
            self._directory_lock
                .as_ref()
                .ok_or_else(|| invalid("native read has no actual directory lifetime"))?,
        );
        let result = NativeRangeReadPhysical {
            native,
            scope: scope.clone(),
            local_b: NativeReadLocalDbSequence(point.local_b),
            point,
            stamp,
            directory,
            health: Arc::clone(&self.durability),
            store_open_id: catalogue.store_open_id(),
            catalogue_sha256: catalogue.checksum_sha256().into(),
            cancellation: parent.child(),
        };
        result.require_lifetime()?;
        Ok(result)
    }
}
