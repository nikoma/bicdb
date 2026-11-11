//! Durable adoption of an ACTUALLY verified physical image. This catalogue
//! snapshot is not membership activation, a serving epoch or strong-read proof.
use super::*;
use crate::distribution_range_image_install::VerifiedNativeRangeImageInstall;

pub(super) const NATIVE_IMAGE_CATALOG_FORMAT: u32 = 6;
const MAX_NATIVE_BASES: usize = 65_536;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct NativeImageCatalogueBase {
    format_version: u32,
    cluster_id: ClusterId,
    node_id: ClusterNodeId,
    bootstrap_id: Uuid,
    range_id: RangeId,
    capture_epoch: u64,
    range_index: u64,
    logical_genesis_sha256: String,
    logical_history_sha256: String,
    native_receipt_sha256: String,
    native_birth_id: Uuid,
    native_birth_sha256: String,
    attempt_id: Uuid,
    session_id: Uuid,
    nonce: Uuid,
    intent_sha256: String,
}
impl NativeImageCatalogueBase {
    fn validate(&self) -> Result<()> {
        let digest =
            |value: &str| value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit());
        if self.format_version != 1
            || self.capture_epoch == 0
            || self.bootstrap_id.is_nil()
            || self.native_birth_id.is_nil()
            || self.attempt_id.is_nil()
            || self.session_id.is_nil()
            || self.nonce.is_nil()
            || !digest(&self.logical_genesis_sha256)
            || !digest(&self.logical_history_sha256)
            || !digest(&self.native_receipt_sha256)
            || !digest(&self.native_birth_sha256)
            || !digest(&self.intent_sha256)
            || (self.range_index == 0 && self.logical_genesis_sha256 != self.logical_history_sha256)
        {
            return Err(range_write_error("malformed native image catalogue base"));
        }
        Ok(())
    }
}

pub(super) fn validate_catalog_native_image_bases(catalog: &RangeWriteCatalog) -> Result<()> {
    if catalog.native_image_bases.len() > MAX_NATIVE_BASES {
        return Err(range_write_error(
            "native image catalogue base bound exceeded",
        ));
    }
    for (range_id, base) in &catalog.native_image_bases {
        base.validate()?;
        let log = catalog
            .ranges
            .get(range_id)
            .ok_or_else(|| range_write_error("native image catalogue base has no range log"))?;
        if catalog.format_version < NATIVE_IMAGE_CATALOG_FORMAT
            || base.range_id != *range_id
            || base.cluster_id != catalog.cluster_id
            || base.node_id != catalog.node_id
            || log.current_epoch < base.capture_epoch
            || log.compacted_through < base.range_index
            || log.resolved_through < base.range_index
        {
            return Err(range_write_error(
                "native image catalogue base disagrees with durable log identity/floor",
            ));
        }
    }
    Ok(())
}

/// Issued only after the actual image proof has been revalidated and the new
/// catalogue has been fsynced. The verified image must remain owned alongside
/// it; this receipt alone authorizes no caller to rekey a freeze or serve data.
#[derive(Debug)]
pub(crate) struct NativeImageCatalogueAdoptionReceipt {
    store_open_id: Uuid,
    native_database_incarnation: String,
    base: NativeImageCatalogueBase,
}
impl NativeImageCatalogueAdoptionReceipt {
    pub(crate) fn range_id(&self) -> RangeId {
        self.base.range_id
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.base.range_index
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.base.attempt_id
    }
    pub(crate) fn session_id(&self) -> Uuid {
        self.base.session_id
    }
    pub(crate) fn nonce(&self) -> Uuid {
        self.base.nonce
    }
    pub(crate) fn native_database_incarnation(&self) -> &str {
        &self.native_database_incarnation
    }
    pub(crate) fn native_receipt_sha256(&self) -> &str {
        &self.base.native_receipt_sha256
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.base.logical_history_sha256
    }

    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        verified: &VerifiedNativeRangeImageInstall,
        role: &NativeImageInstallRole<'_>,
    ) -> Result<()> {
        verified.require_current(store, role)?;
        self.require_binding_current(store, verified.base(), verified.fence())
    }
    pub(crate) fn require_materialized_current(
        &self,
        store: &RangeWriteStore,
        database: &crate::BicDb,
        admission: &crate::db::NativeRecoveryAdmission,
        physical: &crate::db::NativeInstalledRangeImageBase,
        fence: &TargetImageInstallFence,
        role: &NativeImageInstallRole<'_>,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
    ) -> Result<()> {
        store.validate_native_image_install_fence(fence, database, admission, role, frozen)?;
        physical.require_current(database, admission)?;
        self.require_binding_current(store, physical, fence)
    }
    fn require_binding_current(
        &self,
        store: &RangeWriteStore,
        physical: &crate::db::NativeInstalledRangeImageBase,
        fence: &TargetImageInstallFence,
    ) -> Result<()> {
        store.require_durable_recovery_catalog()?;
        let expected = NativeImageCatalogueBase::from_materialized(physical, fence)?;
        if self.store_open_id != store.open_id
            || self.native_database_incarnation != fence.database_incarnation()
            || self.base != expected
            || store.catalog.native_image_bases.get(&self.base.range_id) != Some(&self.base)
        {
            return Err(range_write_error(
                "native catalogue adoption belongs to another actual image/store open",
            ));
        }
        let log = store
            .catalog
            .ranges
            .get(&self.base.range_id)
            .ok_or_else(|| range_write_error("adopted catalogue range disappeared"))?;
        if log.last_index != self.base.range_index
            || log.resolved_through != self.base.range_index
            || log.compacted_through != self.base.range_index
            || !log.entries.is_empty()
        {
            return Err(range_write_error(
                "adopted image catalogue is no longer the exact snapshot boundary",
            ));
        }
        Ok(())
    }
}
impl NativeImageCatalogueBase {
    fn from_actual(verified: &VerifiedNativeRangeImageInstall) -> Result<Self> {
        Self::from_materialized(verified.base(), verified.fence())
    }
    fn from_materialized(
        physical: &crate::db::NativeInstalledRangeImageBase,
        fence: &TargetImageInstallFence,
    ) -> Result<Self> {
        let base = Self {
            format_version: 1,
            cluster_id: physical.cluster_id().clone(),
            node_id: physical.local_node_id().clone(),
            bootstrap_id: physical.bootstrap_id(),
            range_id: physical.capture_range().id,
            capture_epoch: physical.capture_range().epoch,
            range_index: physical.range_index(),
            logical_genesis_sha256: physical.logical_genesis_sha256().to_string(),
            logical_history_sha256: physical.logical_history_sha256().to_string(),
            native_receipt_sha256: physical.native_receipt_sha256().to_string(),
            native_birth_id: physical.birth_id(),
            native_birth_sha256: physical.birth_sha256().to_string(),
            attempt_id: physical.attempt_id(),
            session_id: physical.session_id(),
            nonce: physical.nonce(),
            intent_sha256: physical.intent_sha256().to_string(),
        };
        base.validate()?;
        if physical.history_verified_through() != base.range_index
            || fence.range().id != base.range_id
            || fence.range().epoch != base.capture_epoch
            || fence.source_range_index() != base.range_index
            || fence.attempt_id() != base.attempt_id
            || fence.intent_sha256() != base.intent_sha256
            || fence.session_id() != base.session_id
            || fence.nonce() != base.nonce
        {
            return Err(range_write_error(
                "actual native image and catalogue intent are not the same complete attempt",
            ));
        }
        Ok(base)
    }
}

impl RangeWriteStore {
    /// Absence of adoption for THIS attempt only, never a native empty/floor
    /// claim. An older completed base may legitimately remain unchanged.
    pub(crate) fn require_no_native_image_catalogue_adoption_for_attempt(
        &self,
        range_id: RangeId,
        attempt_id: Uuid,
    ) -> Result<()> {
        self.require_durable_recovery_catalog()?;
        if attempt_id.is_nil()
            || self
                .catalog
                .native_image_bases
                .get(&range_id)
                .is_some_and(|base| base.attempt_id == attempt_id)
        {
            return Err(range_write_error(
                "this native image attempt already adopted catalogue state",
            ));
        }
        Ok(())
    }
    /// A verified native image, not a number/DTO, is the only input that can
    /// establish this snapshot floor. Preserve every existing refusal record.
    pub(crate) fn adopt_verified_native_image_catalogue(
        &mut self,
        verified: &VerifiedNativeRangeImageInstall,
        role: &NativeImageInstallRole<'_>,
    ) -> Result<NativeImageCatalogueAdoptionReceipt> {
        self.adopt_verified_native_image_catalogue_with(
            verified,
            role,
            crate::storage::write_atomic,
        )
    }
    #[cfg(test)]
    pub(crate) fn test_adopt_native_image_catalogue_after_durable_rename_failure(
        &mut self,
        verified: &VerifiedNativeRangeImageInstall,
        role: &NativeImageInstallRole<'_>,
    ) -> Result<NativeImageCatalogueAdoptionReceipt> {
        self.adopt_verified_native_image_catalogue_with(verified, role, |path, bytes, fsync| {
            crate::storage::write_atomic(path, bytes, fsync)?;
            Err(range_write_error(
                "injected catalogue acknowledgment error after actual durable rename",
            ))
        })
    }
    fn adopt_verified_native_image_catalogue_with(
        &mut self,
        verified: &VerifiedNativeRangeImageInstall,
        role: &NativeImageInstallRole<'_>,
        writer: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<NativeImageCatalogueAdoptionReceipt> {
        verified.require_current(self, role)?;
        let base = NativeImageCatalogueBase::from_actual(verified)?;
        if &base.cluster_id != role.role().cluster_id()
            || &base.node_id != role.role().local_node_id()
            || base.bootstrap_id != role.role().bootstrap_id()
            || !self
                .catalog
                .image_install_intents
                .contains_key(&base.range_id)
        {
            return Err(range_write_error(
                "actual catalogue image adoption has another identity or no refusal intent",
            ));
        }
        self.validate_install_local_log(verified.fence().range(), base.range_index)?;
        if let Some(old) = self.catalog.native_image_bases.get(&base.range_id) {
            if old.range_index > base.range_index
                || (old.attempt_id == base.attempt_id && old != &base)
            {
                return Err(range_write_error(
                    "native image catalogue base regressed or changed one completed attempt",
                ));
            }
        } else if self.catalog.native_image_bases.len() >= MAX_NATIVE_BASES {
            return Err(range_write_error(
                "native image catalogue base bound reached",
            ));
        }
        let current_epoch = self
            .catalog
            .ranges
            .get(&base.range_id)
            .map_or(base.capture_epoch, |log| {
                log.current_epoch.max(base.capture_epoch)
            });
        self.catalog.ranges.insert(
            base.range_id,
            RangeWriteLog {
                current_epoch,
                last_index: base.range_index,
                resolved_through: base.range_index,
                compacted_through: base.range_index,
                entries: BTreeMap::new(),
            },
        );
        self.catalog.format_version = self.catalog.format_version.max(NATIVE_IMAGE_CATALOG_FORMAT);
        self.catalog
            .native_image_bases
            .insert(base.range_id, base.clone());
        // On rename/fsync ambiguity retain both the conservative snapshot and
        // refusal, but issue NO receipt until a successful complete re-fsync.
        self.persist_recovery_catalog_with(writer)?;
        verified.require_current(self, role)?;
        let receipt = NativeImageCatalogueAdoptionReceipt {
            store_open_id: self.open_id,
            native_database_incarnation: verified.fence().database_incarnation().to_string(),
            base,
        };
        receipt.require_current(self, verified, role)?;
        Ok(receipt)
    }
}
