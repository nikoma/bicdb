//! Dedicated immutable initial-DATA listener. An old metadata-only listener
//! never broadens when fields/providers/Weak owners change later.
use super::*;
use crate::distribution_native_host::NativeInitialDataAdmission;

#[allow(clippy::too_many_arguments)]
pub(crate) fn start_cluster_initial_native_data_server(
    address: SocketAddr,
    config: ClusterNetworkTransportConfig,
    topology: Arc<RwLock<ClusterTopology>>,
    service: Arc<Mutex<ClusterDataNodeService>>,
    metadata: Arc<Mutex<MetadataConsensusStore>>,
    provider: Arc<MetadataReadProvider>,
    runtime: Arc<crate::distribution_range_recovery::RangeRecoveryRuntime>,
    admission: Arc<NativeInitialDataAdmission>,
) -> Result<ClusterDataServerHandle> {
    admission.require_listener(&service, &runtime)?;
    if config.dev_localhost_plaintext {
        return Err(transport_error("initial DATA requires mutual TLS"));
    }
    start_cluster_data_server_with_native_mode(
        address,
        config,
        topology,
        service,
        None,
        Some(metadata),
        None,
        Some(provider),
        Some(runtime),
        Some(NativeDataListenerMode::Initial(admission)),
    )
}

pub(super) fn require_request(
    admission: &NativeInitialDataAdmission,
    request: &ClusterDataRequest,
) -> Result<()> {
    admission.require_live()?;
    match request {
        ClusterDataRequest::NativeRemoteImageSourceFrame { .. }
        | ClusterDataRequest::NativeRemoteOld { .. }
        | ClusterDataRequest::NativeRemoteNew { .. }
        | ClusterDataRequest::NativeSourceExportRetirement { .. }
        | ClusterDataRequest::NativeSourceDiscovery { .. }
        | ClusterDataRequest::NativeDecisionSourceProvenance { .. } => admission.require_initial_source_hosting(),
        ClusterDataRequest::Ping
        | ClusterDataRequest::RegisterMetadataLearner { .. }
        | ClusterDataRequest::FetchBootstrapSnapshot
        | ClusterDataRequest::ExportSchemaBundle { .. }
        | ClusterDataRequest::MetadataVote { .. }
        | ClusterDataRequest::MetadataAppend { .. }
        | ClusterDataRequest::MetadataReadProbe { .. }
        | ClusterDataRequest::MetadataReadAcquire { .. }
        | ClusterDataRequest::HostMetadataReadAcquire { .. }
        | ClusterDataRequest::AcquireMetadataRecoveryRole { .. }
        | ClusterDataRequest::RecoveryEpochFence { .. }
        | ClusterDataRequest::RecoveryFencedProbe { .. }
        | ClusterDataRequest::RecoveryCertifyHistorical { .. }
        | ClusterDataRequest::RecoveryCertifySparseHistorical { .. }
        | ClusterDataRequest::RecoveryApplyHistorical { .. }
        | ClusterDataRequest::PrepareNativeReady { .. }
        | ClusterDataRequest::ObserveNativeReady { .. }
        | ClusterDataRequest::CompleteNativeRecovery { .. }
        | ClusterDataRequest::ActivateNativeRecovery { .. }
        | ClusterDataRequest::ObserveNativeCompletion { .. }
        | ClusterDataRequest::ObserveCurrentNativeRange { .. }
        | ClusterDataRequest::ObserveNativeAbortCandidate { .. }
        | ClusterDataRequest::PromiseNativeAbort { .. }
        | ClusterDataRequest::ObserveNativeAbortPromise { .. }
        | ClusterDataRequest::ResolveNativeAbort { .. }
        | ClusterDataRequest::PrepareRangeWrite { .. }
        | ClusterDataRequest::CommitRangeWrite { .. }
        | ClusterDataRequest::CertifyRangeWrite { .. }
        | ClusterDataRequest::AbortRangeWrite { .. }
        | ClusterDataRequest::RecertifyNativeOrigin { .. }
        | ClusterDataRequest::MaterializeNativeCompleted { .. }
        | ClusterDataRequest::ApplyNativeOrigin { .. }
        | ClusterDataRequest::ObserveNativeOrigin { .. }
        | ClusterDataRequest::RangeWriteProgress { .. }
        | ClusterDataRequest::ProbeRangeWrite { .. }
        // Typed Get executes the existing missing-dispatcher refusal until
        // publication; it NEVER falls through to an ordinary DB Get.
        | ClusterDataRequest::NativeEventGet(_) => Ok(()),
        ClusterDataRequest::ApplicationEvent(event) => match &event.operation {
            crate::ClusterEventOperation::Append { .. } | crate::ClusterEventOperation::Get { .. } => {
                if !admission.application_published()? {
                    return Err(transport_error("initial DATA receiver is pending local leader readiness; retry within the original request budget"));
                }
                Ok(())
            }
            _ => Err(transport_error("initial native DATA supports point Append/Get only; scan/SQL/effects remain unsupported")),
        },
        _ => Err(transport_error("initial native DATA refuses generic snapshot/import/repair/cleanup and unsupported operations")),
    }
}
