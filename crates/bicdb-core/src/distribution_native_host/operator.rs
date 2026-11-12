//! Public metadata-only owner. Configuration DTOs never become birth/vote/data
//! proofs; every action consumes the existing actual native producer.
use super::*;
use crate::{ClusterNetworkTransportConfig, DistributionConfig, MetadataConsensusStatus};
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMetadataCollection {
    pub name: String,
    pub mode: CollectionMode,
}
/// Common initial configuration DATA. Every process must use identical data;
/// each process supplies its OWN network/TLS files and its own root separately.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMetadataBootstrapPlan {
    pub version: u32,
    pub configuration: DistributionConfig,
    pub members: Vec<crate::ClusterNode>,
    pub collections: Vec<NativeMetadataCollection>,
    pub created_at_ms: u64,
}
impl NativeMetadataBootstrapPlan {
    fn validated(&self, parent: &CancellationToken) -> Result<NativeClusterBootstrapPlan> {
        crate::distribution_range_consensus::validate_native_json_size(self, MAX_PLAN_BYTES)?;
        if self.version != 1 || self.created_at_ms == 0 {
            return Err(invalid("unsupported native metadata plan version/time"));
        }
        let mut configuration = self.configuration.clone();
        configuration.transport = ClusterNetworkTransportConfig::default();
        NativeClusterBootstrapPlan::new(
            &configuration,
            self.members.clone(),
            self.collections
                .iter()
                .map(|c| NativeBootstrapCollection {
                    name: c.name.clone(),
                    mode: c.mode.clone(),
                })
                .collect(),
            self.created_at_ms,
            parent,
        )
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeMetadataHostAction {
    Prepare,
    Launch,
    Elect,
    EnableControl,
    Heartbeat,
    Status,
    Stop,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeMetadataHostPhase {
    Provisional,
    Prepared,
    OwnedMetadata,
    MetadataControlTransition,
    MetadataControl,
    Stopped,
}
#[derive(Clone, Debug, Serialize)]
pub struct NativeMetadataHostStatus {
    pub version: u32,
    pub node_id: ClusterNodeId,
    pub phase: NativeMetadataHostPhase,
    pub metadata: Option<MetadataConsensusStatus>,
    pub data_serving: bool,
    pub data_refusal: &'static str,
}

/// No DB/service/store/coordinator accessors, Clone or deserialization. This
/// owner is intentionally fresh-only; existing roots need a separate strict
/// cold lifecycle and can never be retrofitted through this constructor.
pub struct NativeMetadataHost {
    provisional: NativeProvisionalHost,
    permit: Option<NativeInitialConsensusPermit>,
    launch: Option<NativeInitialConsensusLaunch>,
    listener: Option<crate::distribution_transport::ClusterDataServerHandle>,
    control: Option<NativeMetadataControlHost>,
    network: ClusterNetworkTransportConfig,
    template: DistributionConfig,
    phase: NativeMetadataHostPhase,
}
impl NativeMetadataHost {
    pub fn start_fresh_metadata_only(
        root: &Path,
        plan: NativeMetadataBootstrapPlan,
        local: ClusterNodeId,
        local_address: &str,
        network: ClusterNetworkTransportConfig,
        paged_buffer_pool_bytes: u64,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        let internal = plan.validated(parent)?;
        network.validate()?;
        if root.exists()
            || !(8 * 1024 * 1024..=1024 * 1024 * 1024).contains(&paged_buffer_pool_bytes)
            || network.dev_localhost_plaintext
        {
            return Err(invalid("metadata-only fresh startup refuses an existing root, unsupported memory, or plaintext"));
        }
        let tls = network
            .tls
            .as_ref()
            .ok_or_else(|| invalid("own native TLS configuration required"))?;
        if !tls.require_client_cert {
            return Err(invalid("native metadata host requires mutual TLS"));
        }
        let pin = crate::replication_transport::replication_certificate_sha256(&tls.cert_path)?;
        let member = internal
            .topology
            .nodes
            .get(&local)
            .ok_or_else(|| invalid("local identity absent from common plan"))?;
        if member.address != local_address
            || member.tls_certificate_sha256.as_deref() != Some(pin.as_str())
        {
            return Err(invalid(
                "own native TLS leaf differs from planned local identity",
            ));
        }
        // Validate endpoint shape before ANY factory/filesystem effects.
        let address: std::net::SocketAddr = member
            .address
            .parse()
            .map_err(|_| invalid("metadata host requires numeric planned address"))?;
        if address.port() == 0 {
            return Err(invalid("metadata host requires nonzero planned port"));
        }
        let mut template = plan.configuration;
        template.node_id = member.id.clone();
        template.node_incarnation = member.incarnation;
        template.node_address = member.address.clone();
        template.node_tls_certificate_sha256 = member.tls_certificate_sha256.clone();
        template.node_capacity_bytes = member.capacity_bytes;
        template.transport = network.clone(); // NEVER seed-local TLS paths.
        template.validate()?;
        let _validated_tls = crate::TcpClusterRelocationTransport::new(
            internal.topology.cluster_id.clone(),
            local.clone(),
            Arc::new(RwLock::new(internal.topology.clone())),
            network.clone(),
        )?;
        let db = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(paged_buffer_pool_bytes);
        let seed = Arc::new(NativeProvisionalNode::create(
            root, db, internal, &local, parent,
        )?);
        let provisional = NativeProvisionalHost::start(seed, network.clone(), parent)?;
        Ok(Self {
            provisional,
            permit: None,
            launch: None,
            listener: None,
            control: None,
            network,
            template,
            phase: NativeMetadataHostPhase::Provisional,
        })
    }
    pub fn status(&self, parent: &CancellationToken) -> Result<NativeMetadataHostStatus> {
        parent.check()?;
        let metadata = if let Some(control) = &self.control {
            Some(control.metadata_status(parent)?)
        } else if let Some(launch) = &self.launch {
            let actual = launch
                .service
                .try_lock()
                .ok_or_else(|| invalid("native operator status service busy"))?
                .listener_open_guard();
            let store = launch.lock_initial_metadata(parent)?;
            store.require_listener_database(&actual)?;
            Some(store.status())
        } else {
            None
        };
        Ok(NativeMetadataHostStatus {
            version: 1,
            node_id: self.template.node_id.clone(),
            phase: self.phase,
            metadata,
            data_serving: false,
            data_refusal: "metadata-only host: data/read/SQL/promotion endpoints are not installed",
        })
    }
    /// Each finite action preserves its original caller deadline, including
    /// all network/IO/drain work. Failures retain actual owners for retry.
    pub fn apply(
        &mut self,
        action: NativeMetadataHostAction,
        parent: &CancellationToken,
    ) -> Result<NativeMetadataHostStatus> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        token.check()?;
        if self.phase == NativeMetadataHostPhase::Stopped
            && !matches!(
                action,
                NativeMetadataHostAction::Status | NativeMetadataHostAction::Stop
            )
        {
            return Err(invalid(
                "stopped native host cannot be restarted through fresh startup",
            ));
        }
        match action {
            NativeMetadataHostAction::Prepare => {
                if self.launch.is_some() || self.control.is_some() {
                    return Err(invalid(
                        "initial preparation cannot replace exposed metadata",
                    ));
                }
                let configuration = self.provisional.observe_initial_configuration(&token)?;
                self.permit = Some(
                    self.provisional
                        .prepare_common_initial_configuration(&configuration, &token)?,
                );
                self.phase = NativeMetadataHostPhase::Prepared;
            }
            NativeMetadataHostAction::Launch => {
                if self.control.is_some() {
                    return Err(invalid("native control already owns metadata"));
                }
                if self.launch.is_none() {
                    let permit = self
                        .permit
                        .as_ref()
                        .ok_or_else(|| invalid("actual all-C initial preparation required"))?;
                    self.launch = Some(self.provisional.finish_initial_consensus(permit, &token)?);
                    self.phase = NativeMetadataHostPhase::OwnedMetadata;
                    self.permit.take();
                }
                if self.listener.is_none() {
                    self.listener = Some(
                        self.launch
                            .as_ref()
                            .unwrap()
                            .start_metadata_listener(self.network.clone(), &token)?,
                    );
                }
            }
            NativeMetadataHostAction::Elect => {
                if self.listener.is_none() || self.control.is_some() {
                    return Err(invalid(
                        "election requires the owned initial metadata listener",
                    ));
                }
                self.launch
                    .as_ref()
                    .ok_or_else(|| invalid("owned initial metadata absent"))?
                    .elect_initial_metadata(&token)?;
            }
            NativeMetadataHostAction::EnableControl => {
                if self.control.is_none() {
                    if self.launch.is_none() || self.listener.is_none() {
                        return Err(invalid(
                            "owned running metadata listener required before control transition",
                        ));
                    }
                    // Pure ownership transfer BEFORE any fallible drain/bind.
                    self.control = Some(NativeMetadataControlHost::new(
                        self.launch.take().unwrap(),
                        self.listener.take().unwrap(),
                        self.network.clone(),
                        self.template.clone(),
                    ));
                    self.phase = NativeMetadataHostPhase::MetadataControlTransition;
                }
                self.control.as_mut().unwrap().advance(&token)?;
                self.phase = NativeMetadataHostPhase::MetadataControl;
            }
            NativeMetadataHostAction::Heartbeat => {
                if let Some(control) = &self.control {
                    control.heartbeat(&token)?;
                } else {
                    self.launch
                        .as_ref()
                        .ok_or_else(|| invalid("owned metadata absent"))?
                        .heartbeat_initial_metadata(&token)?;
                }
            }
            NativeMetadataHostAction::Stop => {
                if let Some(control) = self.control.as_mut() {
                    control.stop(&token)?;
                }
                if let Some(listener) = self.listener.as_mut() {
                    listener.stop_and_drain(&token)?;
                }
                self.listener.take();
                self.provisional.stop_retained(&token)?;
                self.phase = NativeMetadataHostPhase::Stopped;
            }
            NativeMetadataHostAction::Status => {}
        }
        token.check()?;
        self.status(&token)
    }
}
