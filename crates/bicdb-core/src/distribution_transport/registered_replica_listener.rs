//! Immutable registered replica-only mode. It never broadens old metadata
//! listeners and never grants APP/Strong/source/cut authority from registration.
use super::*;
use crate::distribution_native_host::NativeRegisteredReplicaAdmission;
#[allow(clippy::too_many_arguments)]
pub(crate) fn start_cluster_registered_replica_server(
    address: SocketAddr,
    config: ClusterNetworkTransportConfig,
    topology: Arc<RwLock<ClusterTopology>>,
    service: Arc<Mutex<ClusterDataNodeService>>,
    metadata: Arc<Mutex<MetadataConsensusStore>>,
    provider: Arc<MetadataReadProvider>,
    runtime: Arc<crate::distribution_range_recovery::RangeRecoveryRuntime>,
    admission: Arc<NativeRegisteredReplicaAdmission>,
) -> Result<ClusterDataServerHandle> {
    admission.require_listener(&service, &runtime)?;
    if config.dev_localhost_plaintext
        || config
            .tls
            .as_ref()
            .is_none_or(|tls| !tls.require_client_cert || tls.dev_localhost_plaintext)
    {
        return Err(transport_error(
            "registered replica receiver requires mutual TLS",
        ));
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
        Some(NativeDataListenerMode::RegisteredReplica(admission)),
    )
}
pub(super) fn require_request(request: &ClusterDataRequest) -> Result<()> {
    match request {
        ClusterDataRequest::Ping | ClusterDataRequest::RegisterMetadataLearner {..}
        | ClusterDataRequest::FetchBootstrapSnapshot | ClusterDataRequest::ExportSchemaBundle {..}
        | ClusterDataRequest::MetadataVote {..} | ClusterDataRequest::MetadataAppend {..}
        | ClusterDataRequest::MetadataReadProbe {..} | ClusterDataRequest::MetadataReadAcquire {..}
        | ClusterDataRequest::HostMetadataReadAcquire {..}
        | ClusterDataRequest::AcquireMetadataRecoveryRole {..}
        | ClusterDataRequest::RecoveryEpochFence {..} | ClusterDataRequest::RecoveryFencedProbe {..}
        | ClusterDataRequest::RecoveryCertifyHistorical {..} | ClusterDataRequest::RecoveryCertifySparseHistorical {..}
        | ClusterDataRequest::RecoveryApplyHistorical {..} | ClusterDataRequest::PrepareNativeReady {..}
        | ClusterDataRequest::ObserveNativeReady {..} | ClusterDataRequest::CompleteNativeRecovery {..}
        | ClusterDataRequest::ObserveNativeCompletion {..} | ClusterDataRequest::ObserveCurrentNativeRange {..}
        | ClusterDataRequest::ObserveNativeAbortCandidate {..} | ClusterDataRequest::PromiseNativeAbort {..}
        | ClusterDataRequest::ObserveNativeAbortPromise {..} | ClusterDataRequest::ResolveNativeAbort {..}
        | ClusterDataRequest::ApplyNativeOrigin {..} | ClusterDataRequest::ObserveNativeOrigin {..}
        | ClusterDataRequest::RangeWriteProgress {..} | ClusterDataRequest::ProbeRangeWrite {..}
        // Returns typed unavailable BEFORE latches, never a bare DB None.
        | ClusterDataRequest::NativeObserveRegisteredExportNew { .. }
        | ClusterDataRequest::NativeEventGet(_) => Ok(()),
        _ => Err(transport_error("registered native replica-only receiver refuses APP/Strong serving and generic snapshot/import/repair; actual placement/image/joint admission is still required")),
    }
}
