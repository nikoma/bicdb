//! Actual normal-serving current/archive fault, under the real read journal.
use super::*;

#[derive(Clone, Copy)]
enum RecordFault {
    Current,
    Archive,
}
impl BicDb {
    fn damage_actual_normal_serving_read_record(
        &self,
        scope: &NativeServingObservationScope<'_>,
        fault: RecordFault,
    ) -> Result<()> {
        let actual = scope.lineage(self)?;
        let expected = actual
            .serving
            .as_ref()
            .ok_or_else(|| invalid("normal serving fault has no ACTUAL serving record"))?;
        if !matches!(
            &expected.scope.purpose,
            MetadataRecoveryRolePurpose::NormalRecovery
        ) || expected.joint_decision_sha256.is_some()
        {
            return Err(invalid(
                "normal serving fault requires actual NormalRecovery producer",
            ));
        }
        let current_key = key(expected.range.id);
        let archive_key = history_key(expected.range.id, &expected.checksum_sha256);
        let current: NativeRecoveredServingConfiguration = self
            .read_native_range_image_record(&current_key)?
            .ok_or_else(|| invalid("normal serving fault current absent BEFORE cut"))?;
        let archive: NativeRecoveredServingConfiguration = self
            .read_native_range_image_record(&archive_key)?
            .ok_or_else(|| invalid("normal serving fault archive absent BEFORE cut"))?;
        current.validate_historical_floor(&actual.birth, &actual.base)?;
        archive.validate_historical_floor(&actual.birth, &actual.base)?;
        if current != *expected || archive != current {
            return Err(invalid(
                "normal serving fault requires actual current == archive",
            ));
        }
        let damaged = match fault {
            RecordFault::Current => &current_key,
            RecordFault::Archive => &archive_key,
        };
        self.in_paged_transaction(|paged, xid| {
            paged
                .store()
                .delete(xid, &native_key(damaged))
                .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("normal serving fault has no actual native WAL store"))?;
        let after_current: Option<NativeRecoveredServingConfiguration> =
            self.read_native_range_image_record(&current_key)?;
        let after_archive: Option<NativeRecoveredServingConfiguration> =
            self.read_native_range_image_record(&archive_key)?;
        let exact = match fault {
            RecordFault::Current => {
                after_current.is_none() && after_archive.as_ref() == Some(&archive)
            }
            RecordFault::Archive => {
                after_current.as_ref() == Some(&current) && after_archive.is_none()
            }
        };
        if !exact {
            return Err(invalid(
                "normal serving fault changed more than its exact selected record",
            ));
        }
        Ok(())
    }
    pub(crate) fn test_remove_serving_read_normal_current(
        &self,
        scope: &NativeServingObservationScope<'_>,
    ) -> Result<()> {
        self.damage_actual_normal_serving_read_record(scope, RecordFault::Current)
    }
    pub(crate) fn test_remove_serving_read_normal_archive(
        &self,
        scope: &NativeServingObservationScope<'_>,
    ) -> Result<()> {
        self.damage_actual_normal_serving_read_record(scope, RecordFault::Archive)
    }
}
