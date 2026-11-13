//! Shared necessary lifetime/identity refusals ONLY. Each actual owner remains
//! distinct; this enum cannot convert Assigned data into INITIAL genesis or
//! replace request-specific roles, quorums, native facts or serving permission.
use super::*;
#[derive(Clone)]
pub(crate) enum NativeDataOwnerAdmission {
    Initial(Arc<NativeInitialDataAdmission>),
    Assigned(Arc<NativeAssignedDataAdmission>),
}
impl NativeDataOwnerAdmission {
    pub(crate) fn require_live(&self) -> Result<()> {
        match self {
            Self::Initial(a) => a.require_live(),
            Self::Assigned(a) => a.require_live(),
        }
    }
    pub(crate) fn require_listener(
        &self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        runtime: &Arc<crate::distribution_range_recovery::RangeRecoveryRuntime>,
    ) -> Result<()> {
        match self {
            Self::Initial(a) => a.require_listener(service, runtime),
            Self::Assigned(a) => a.require_listener(service, runtime),
        }
    }
    pub(crate) fn require_routing_service(
        &self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        store: Uuid,
    ) -> Result<()> {
        match self {
            Self::Initial(a) => a.require_routing_service(service, store),
            Self::Assigned(a) => a.require_routing_service(service, store),
        }
    }
}
