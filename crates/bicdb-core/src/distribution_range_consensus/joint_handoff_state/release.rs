//! Complete current LOCAL catalogue for a decided joint release continuation.
//! Does not certify a new head or grant ordinary reads/writes.
use super::*;

pub(crate) struct NativeJointReleaseCatalogue {
    obligations: TargetImageCatalogHistoryObligations,
    decisions: BTreeMap<u64, NativeReadCatalogueDecision>,
    native_r: u64,
}
impl NativeJointReleaseCatalogue {
    pub(crate) fn decisions(&self) -> &BTreeMap<u64, NativeReadCatalogueDecision> {
        &self.decisions
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.obligations.range_id()
    }
    pub(crate) fn native_r(&self) -> u64 {
        self.native_r
    }
    pub(crate) fn require_current(&self, store: &RangeWriteStore) -> Result<()> {
        self.obligations.require_current(store)
    }
}
impl RangeWriteStore {
    pub(crate) fn observe_native_joint_release_catalogue(
        &self,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
        fact: &NativeJointCatalogueActivationFact,
    ) -> Result<NativeJointReleaseCatalogue> {
        fact.require_current_with_scope(scope, self)?;
        let range = scope.native().data().old_range().id;
        let actual = self.progress(range);
        self.require_actual_abort_controls_covered(
            scope.database(),
            range,
            scope.current_range_index(),
        )?;
        if actual.current_epoch != scope.native().data().activation_range().epoch
            || actual.last_index != scope.current_range_index()
            || actual.resolved_through != scope.current_range_index()
            || actual.compacted_through > actual.resolved_through
        {
            return Err(range_write_error("joint release has unresolved/ahead/higher local obligations; actual replay required"));
        }
        let result = NativeJointReleaseCatalogue {
            obligations: self.image_install_catalog_history_obligations(range)?,
            decisions: self
                .collect_native_resolved_decisions(range, scope.current_range_index())?,
            native_r: scope.current_range_index(),
        };
        scope.require_complete_catalogue(&result)?;
        result.require_current(self)?;
        Ok(result)
    }
    pub(crate) fn observe_native_joint_open_continuity(
        &self,
        fact: &NativeJointCatalogueActivationFact,
    ) -> Result<crate::db::NativeJointOpenContinuity> {
        let range = fact.native().data().old_range().id;
        let guard = self
            .native_joint_activated_opens
            .get(&range)
            .ok_or_else(|| range_write_error("joint release lacks same-open actual guard"))?;
        guard.observe_actual_continuity(self.open_id, &fact.activation_sha256()?)
    }
    pub(crate) fn require_native_joint_open_continuity(
        &self,
        fact: &NativeJointCatalogueActivationFact,
        continuity: &crate::db::NativeJointOpenContinuity,
    ) -> Result<()> {
        let range = fact.native().data().old_range().id;
        let guard = self
            .native_joint_activated_opens
            .get(&range)
            .ok_or_else(|| range_write_error("joint actual open guard disappeared"))?;
        continuity.require_current(guard, self.open_id, &fact.activation_sha256()?)
    }
}
