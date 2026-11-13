//! NEW immutable Assigned CLOSED mode. Installed policy is necessary but is
//! not a Ready/APP/Strong grant. Old Registered/control modes are unchanged.
use super::*;
use crate::distribution_native_host::NativeAssignedDataAdmission;
#[allow(clippy::too_many_arguments)]
pub(crate) fn start_cluster_assigned_closed_data_server(
    address: SocketAddr,
    config: ClusterNetworkTransportConfig,
    topology: Arc<RwLock<ClusterTopology>>,
    service: Arc<Mutex<ClusterDataNodeService>>,
    metadata: Arc<Mutex<MetadataConsensusStore>>,
    provider: Arc<MetadataReadProvider>,
    runtime: Arc<crate::distribution_range_recovery::RangeRecoveryRuntime>,
    admission: Arc<NativeAssignedDataAdmission>,
) -> Result<ClusterDataServerHandle> {
    admission.require_listener(&service, &runtime)?;
    if config.dev_localhost_plaintext
        || config
            .tls
            .as_ref()
            .is_none_or(|t| !t.require_client_cert || t.dev_localhost_plaintext)
    {
        return Err(transport_error(
            "Assigned CLOSED listener requires mutual TLS",
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
        Some(NativeDataListenerMode::AssignedClosed(admission)),
    )
}
pub(super) fn require_request(
    admission: &NativeAssignedDataAdmission,
    request: &ClusterDataRequest,
) -> Result<()> {
    admission.require_live()?;
    match request {
        ClusterDataRequest::ApplicationEvent(_) => Err(transport_error(
            "Assigned CLOSED policy is not application or Strong readiness",
        )),
        // This immutable phase preserves the exact prior replica request
        // surface. Every actual native receiver still consumes its own role,
        // decision and physical proof; no new mutator is enabled by policy.
        _ => registered_replica_listener::require_request(request),
    }
}
