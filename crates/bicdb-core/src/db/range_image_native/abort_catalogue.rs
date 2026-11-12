//! From-birth range-catalogue identity. A missing file after Registered is
//! never a new store, even at native R0. Pending cannot expose any admission.
use super::*;
const KEY: &str = "abort-range-catalogue:identity";
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct NativeAbortCatalogueIdentity {
    version: u32,
    store_id: Uuid,
    birth_id: Uuid,
    birth_sha256: String,
    registered: bool,
    checksum_sha256: String,
}
impl NativeAbortCatalogueIdentity {
    fn checksum(&self) -> Result<String> {
        let mut v = self.clone();
        v.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &("native-abort-range-catalogue-identity-v1", v),
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self, birth: &NativeDatabaseBirth) -> Result<()> {
        if self.version != 1
            || self.store_id.is_nil()
            || self.birth_id != birth.birth_id
            || self.birth_sha256 != birth.checksum_sha256
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid(
                "native abort range catalogue identity is damaged/foreign",
            ));
        }
        Ok(())
    }
}
impl BicDb {
    pub(super) fn initialize_native_abort_catalogue(
        &self,
        birth: &NativeDatabaseBirth,
    ) -> Result<()> {
        if birth.abort_protocol_version != Some(1) {
            return Ok(());
        }
        if !crate::format::native_abort_history_active(&self.path)? {
            return Err(invalid(
                "abort-aware factory lost its from-birth reader fence",
            ));
        }
        let _journal = self.try_native_image_journal()?;
        let mut identity =
            match self.read_native_range_image_record::<NativeAbortCatalogueIdentity>(KEY)? {
                Some(record) => {
                    record.validate(birth)?;
                    record
                }
                None => {
                    // Actual still-initializing factory only. A ready/admitted DB
                    // can never create an identity over an older/missing store.
                    if self
                        .read_native_range_image_record::<NativeInitializationComplete>(READY_KEY)?
                        .is_some()
                    {
                        return Err(invalid(
                            "ready native DB cannot acquire abort initialization retroactively",
                        ));
                    }
                    let mut record = NativeAbortCatalogueIdentity {
                        version: 1,
                        store_id: Uuid::now_v7(),
                        birth_id: birth.birth_id,
                        birth_sha256: birth.checksum_sha256.clone(),
                        registered: false,
                        checksum_sha256: String::new(),
                    };
                    record.checksum_sha256 = record.checksum()?;
                    self.write_native_range_image_record(KEY, &record)?;
                    record
                }
            };
        if identity.registered {
            return crate::distribution_range_consensus::RangeWriteStore::require_abort_catalogue_identity(
                &self.path, &birth.spec.cluster_id, &birth.spec.node_id, identity.store_id,
            );
        }
        crate::distribution_range_consensus::RangeWriteStore::initialize_abort_catalogue_identity(
            &self.path,
            &birth.spec.cluster_id,
            &birth.spec.node_id,
            identity.store_id,
            birth.format_version == 2,
        )?;
        identity.registered = true;
        identity.checksum_sha256 = identity.checksum()?;
        self.write_native_range_image_record(KEY, &identity)?;
        let stored: NativeAbortCatalogueIdentity = self
            .read_native_range_image_record(KEY)?
            .ok_or_else(|| invalid("native abort catalogue registration missing"))?;
        if stored != identity {
            return Err(invalid("native abort catalogue registration changed"));
        }
        Ok(())
    }
    pub(crate) fn native_abort_catalogue_store_id(&self) -> Result<Option<Uuid>> {
        self.check_recovery_state()?;
        let Some(birth) = self.read_native_birth()? else {
            return Ok(None);
        };
        if birth.abort_protocol_version != Some(1) {
            return Ok(None);
        }
        if !crate::format::native_abort_history_active(&self.path)? {
            return Err(invalid("abort protocol birth lacks reader fence"));
        }
        let record: NativeAbortCatalogueIdentity =
            self.read_native_range_image_record(KEY)?.ok_or_else(|| {
                invalid("abort-aware native birth lacks protected catalogue identity")
            })?;
        record.validate(&birth)?;
        if !record.registered {
            return Err(invalid("abort catalogue initialization incomplete"));
        }
        crate::distribution_range_consensus::RangeWriteStore::require_abort_catalogue_identity(
            &self.path,
            &birth.spec.cluster_id,
            &birth.spec.node_id,
            record.store_id,
        )?;
        Ok(Some(record.store_id))
    }
}
