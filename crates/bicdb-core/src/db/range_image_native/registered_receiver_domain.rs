//! Original never-exposed native domain constraint, never a floor/read grant.
use super::*;
impl BicDb {
    pub(crate) fn require_original_registered_receiver_domain(
        &self,
        native: &NativeRecoveryAdmission,
    ) -> Result<()> {
        let journal = self.try_native_image_journal()?;
        self.require_original_registered_receiver_domain_with_journal(native, &journal)
    }
    pub(super) fn require_original_registered_receiver_domain_with_journal(
        &self,
        native: &NativeRecoveryAdmission,
        journal: &NativeImageJournalGuard<'_>,
    ) -> Result<()> {
        self.require_original_registered_receiver_birth_with_journal(native, journal)?;
        // This is the complete protected native namespace, not user rows and
        // not the lossy prefix diagnostic. Any unknown/historical native fact
        // is conservative refusal, including a durable invalidation marker.
        self.require_original_registered_receiver_inventory()
    }
    pub(super) fn require_original_registered_receiver_birth_with_journal(
        &self,
        native: &NativeRecoveryAdmission,
        journal: &NativeImageJournalGuard<'_>,
    ) -> Result<()> {
        journal.require_database(self)?;
        native.require_current(self)?;
        let birth = self
            .read_native_birth()?
            .ok_or_else(|| invalid("registered receiver original birth missing"))?;
        let ready: NativeInitializationComplete = self
            .read_native_range_image_record(READY_KEY)?
            .ok_or_else(|| {
            invalid("registered receiver actual original initialization missing")
        })?;
        if !birth.spec.initial_ranges.is_empty()
            || native.birth_id() != birth.birth_id
            || native.birth_sha256() != birth.checksum_sha256
            || ready.birth_id != birth.birth_id
            || ready.birth_sha256 != birth.checksum_sha256
            || !ready.genesis_sha256.is_empty()
            || self.current_commit_seq() != 0
        {
            return Err(invalid(
                "registered receiver is not the original no-INITIAL-range/native domain",
            ));
        }
        self.check_recovery_state()
    }
    fn require_original_registered_receiver_inventory(&self) -> Result<()> {
        self.require_registered_receiver_inventory_with_extra_keys(&[])
    }
    pub(super) fn require_registered_receiver_inventory_with_extra_keys(
        &self,
        extra: &[String],
    ) -> Result<()> {
        if extra.len() > 2 {
            return Err(invalid("Registered proposal inventory bound exceeded"));
        }
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("registered receiver actual page store absent"))?;
        let namespace = crate::paged_collection::RANGE_IMAGE_NATIVE_NS_V1;
        let snapshot = paged.latest_snapshot();
        let cursor = paged
            .store()
            .scan_from(&snapshot, &namespace)
            .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
        let mut expected = vec![
            BIRTH_KEY,
            READY_KEY,
            "metadata-store:initialized",
            "abort-range-catalogue:identity",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        expected.extend(extra.iter().cloned());
        let mut found = BTreeSet::new();
        for entry in cursor {
            let (key, value) = entry.map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            if !key.starts_with(&namespace) {
                break;
            }
            if key.len() > 256 || value.len() > MAX_NATIVE_RECEIPT_BYTES {
                return Err(invalid(
                    "registered receiver native domain inventory bound or unexpected record",
                ));
            }
            let key = std::str::from_utf8(&key[namespace.len()..])
                .map_err(|_| invalid("registered receiver native record key malformed"))?;
            if !expected.iter().any(|expected| expected == key) {
                return Err(invalid("registered receiver has an existing native domain/invalidation/install/history record; continuation unsupported"));
            }
            if found.len() >= expected.len() || !found.insert(key.to_owned()) {
                return Err(invalid(
                    "registered receiver native inventory duplicate/bound",
                ));
            }
        }
        if found.len() != expected.len() || expected.iter().any(|k| !found.contains(k)) {
            return Err(invalid("registered receiver lost original native birth/READY/Registered/catalogue record custody"));
        }
        // Native identity/checksum and namespace checks for these known
        // records are performed by the real admission + owned-MStore proof;
        // this inventory can neither create them nor substitute a lower bound.
        self.check_recovery_state()
    }
    #[cfg(test)]
    pub(crate) fn test_invalidate_original_registered_receiver(
        &self,
        range: RangeId,
    ) -> Result<serde_json::Value> {
        let _journal = self.try_native_image_journal()?;
        let key = format!("range-invalidation:{}", range.get());
        if self
            .read_native_range_image_record::<serde_json::Value>(&key)?
            .is_some()
        {
            return Err(invalid(
                "actual receiver already has a durable invalidation",
            ));
        }
        self.invalidate_native_range_images_before_mutation(
            Some(range),
            "actual registered receiver invalidation control",
        )?;
        self.read_native_range_image_record(&key)?
            .ok_or_else(|| invalid("actual durable invalidation did not persist"))
    }
}
