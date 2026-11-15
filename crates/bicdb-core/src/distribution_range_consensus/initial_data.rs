//! Initial DATA readiness is a real selected-range protected-genesis protocol,
//! not a raw-zero setter or general repair. Existing recovery APIs stay intact.
use super::*;
use crate::db::{InstalledNativeWriterHostGuard, NativeInitialDataGenesis};
use crate::distribution_range_recovery::RangeRecoveryRuntime;
use crate::TcpClusterRelocationTransport;

impl RangeWriteCoordinator<TcpClusterRelocationTransport> {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn new_assigned_schema_fenced(
        cluster: ClusterId,
        local: ClusterNodeId,
        topology: Arc<RwLock<ClusterTopology>>,
        service: Arc<Mutex<ClusterDataNodeService>>,
        transport: Arc<TcpClusterRelocationTransport>,
        origin: &crate::distribution_native_host::NativeAssignedDataPolicyOrigin,
    ) -> Result<Self> {
        origin.require_live()?;
        let mut coordinator = Self::with_repair_limits_scheduling(
            cluster,
            local,
            topology,
            service,
            transport,
            RangeWriteRepairLimits::default(),
            true,
        )?;
        coordinator.require_schema_compatibility = true;
        Ok(coordinator)
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn new_registered_serving_schema_fenced(
        cluster: ClusterId,
        local: ClusterNodeId,
        topology: Arc<RwLock<ClusterTopology>>,
        service: Arc<Mutex<ClusterDataNodeService>>,
        transport: Arc<TcpClusterRelocationTransport>,
        origin: &crate::distribution_native_host::NativeRegisteredServingDataPolicyOrigin,
    ) -> Result<Self> {
        origin.require_live()?;
        let mut coordinator = Self::with_repair_limits_scheduling(
            cluster,
            local,
            topology,
            service,
            transport,
            RangeWriteRepairLimits::default(),
            true,
        )?;
        coordinator.require_schema_compatibility = true;
        Ok(coordinator)
    }
    pub(crate) fn new_initial_schema_fenced(
        cluster: ClusterId,
        local: ClusterNodeId,
        topology: Arc<RwLock<ClusterTopology>>,
        service: Arc<Mutex<ClusterDataNodeService>>,
        transport: Arc<TcpClusterRelocationTransport>,
    ) -> Result<Self> {
        let mut coordinator = Self::with_repair_limits_scheduling(
            cluster,
            local,
            topology,
            service,
            transport,
            RangeWriteRepairLimits::default(),
            true,
        )?;
        coordinator.require_schema_compatibility = true;
        Ok(coordinator)
    }
    pub(crate) fn recover_initial_native_genesis(
        self: &Arc<Self>,
        runtime: &Arc<RangeRecoveryRuntime>,
        host: &Arc<InstalledNativeWriterHostGuard>,
        genesis: &NativeInitialDataGenesis,
        metadata_leader: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if !self.native_image_mode || genesis.range().leader != self.local_node_id {
            return Err(range_write_error(
                "initial DATA recovery requires actual local native leader",
            ));
        }
        runtime.require_point_read_service(&self.local_service, &self.local_node_id)?;
        // Keep this SAME reservation through readiness publication and its
        // final cancellation check/rollback. No APP can ACK in between.
        let _reservation = self.try_reserve_range(genesis.range().id)?;
        let range = genesis.range();
        self.recovered_epochs.lock().remove(&range.id);
        let remaining = || -> Result<Duration> {
            parent.check()?;
            parent
                .deadline()
                .and_then(|d| d.checked_duration_since(Instant::now()))
                .filter(|d| !d.is_zero())
                .map(|d| d.min(Duration::from_secs(5)))
                .ok_or(BicDbError::QueryTimedOut)
        };
        // Reject an unbounded caller before any same-owner lock wait.
        remaining()?;
        // Retain the actual original open/Store slot before a first READ can
        // encounter the metadata leader's control-to-DATA listener transition.
        // Neither a socket failure nor a new READ can replace these owners.
        let (open, slot) = {
            let service = loop {
                parent.check()?;
                if let Some(service) = self.local_service.try_lock_for(Duration::from_millis(5)) {
                    break service;
                }
            };
            (
                service.listener_open_guard(),
                service.native_replica_listener_store_open_guard(),
            )
        };
        let require_original = || -> Result<()> {
            parent.check()?;
            open.require_live()?;
            slot.require_live()?;
            runtime.require_point_read_service(&self.local_service, &self.local_node_id)?;
            let service = loop {
                parent.check()?;
                if let Some(service) = self.local_service.try_lock_for(Duration::from_millis(5)) {
                    break service;
                }
            };
            let database = service.database();
            let database = database
                .try_read()
                .ok_or_else(|| range_write_error("initial first READ original DB busy"))?;
            open.require_database(&database)?;
            if service.native_initial_listener_store_open_id() != slot.id() {
                return Err(range_write_error(
                    "initial first READ original Store changed",
                ));
            }
            host.require_for_coordinator(&database, range, self)?;
            let topology = self
                .topology
                .try_read()
                .ok_or_else(|| range_write_error("initial first READ original routing busy"))?;
            genesis.require_current(&database, &topology)?;
            parent.check()
        };
        #[cfg(test)]
        self.transport.test_before_initial_genesis_read(parent)?;
        let (first, business) = loop {
            require_original()?;
            let business = Uuid::now_v7();
            let actual = self
                .transport
                .acquire_initial_genesis_metadata_read_observation(
                    metadata_leader,
                    business,
                    range.id,
                    range.epoch,
                    remaining()?,
                    parent,
                );
            #[cfg(test)]
            self.transport
                .test_initial_genesis_read_return(&actual, business, range, parent)?;
            match actual {
                Ok(first) => {
                    require_original()?;
                    break (first, business);
                }
                Err(error) => {
                    #[cfg(test)]
                    initial_read_failure(
                        "first_metadata",
                        &self.local_node_id,
                        range,
                        parent,
                        &error,
                    );
                    let transient =
                        crate::distribution_transport::HotRecoveryPeerFailure::from_error(&error)
                            .is_some_and(|failure| {
                                matches!(
                                    failure.kind(),
                                    std::io::ErrorKind::ConnectionReset
                                        | std::io::ErrorKind::ConnectionRefused
                                        | std::io::ErrorKind::ConnectionAborted
                                        | std::io::ErrorKind::BrokenPipe
                                        | std::io::ErrorKind::UnexpectedEof
                                )
                            });
                    if !transient {
                        return Err(error);
                    }
                    require_original()?;
                    // Pacing and every new authenticated READ consume the SAME
                    // original absolute action. No native/host install repeats.
                    std::thread::sleep(Duration::from_millis(2));
                    parent.check()?;
                }
            }
        };
        {
            let service = loop {
                parent.check()?;
                if let Some(service) = self.local_service.try_lock_for(Duration::from_millis(5)) {
                    break service;
                }
            };
            let database = service.database();
            let database = database
                .try_read()
                .ok_or_else(|| range_write_error("initial DATA database busy"))?;
            host.require_for_coordinator(&database, range, self)?;
            genesis.require_current(&database, first.committed_topology(business)?)?;
        }
        // Actual peer birth/incarnation/pins and full native R/H/catalogue/
        // pending/control checks come from the existing sealed read producer.
        // A decoded list, raw progress0, or NativeOriginStatus cannot replace it.
        let collected = runtime
            .acquire_native_read_candidate(first, business, Uuid::now_v7(), parent)
            .map_err(|error| {
                #[cfg(test)]
                initial_read_failure(
                    "actual_native_read_candidate",
                    &self.local_node_id,
                    range,
                    parent,
                    &error,
                );
                error
            })?
            .into_initial_data_genesis(genesis)
            .map_err(|error| {
                #[cfg(test)]
                initial_read_failure(
                    "initial_genesis_comparison",
                    &self.local_node_id,
                    range,
                    parent,
                    &error,
                );
                error
            })?;
        let final_metadata = self
            .transport
            .acquire_metadata_read_observation(
                metadata_leader,
                business,
                range.id,
                range.epoch,
                remaining()?,
                parent,
            )
            .map_err(|error| {
                #[cfg(test)]
                initial_read_failure("final_metadata", &self.local_node_id, range, parent, &error);
                error
            })?;
        let service = loop {
            parent.check()?;
            if let Some(service) = self.local_service.try_lock_for(Duration::from_millis(5)) {
                break service;
            }
        };
        collected.require_current(&service, &final_metadata, business)?;
        let database = service.database();
        let database = database
            .try_read()
            .ok_or_else(|| range_write_error("initial DATA database busy at final boundary"))?;
        genesis.require_current(&database, final_metadata.committed_topology(business)?)?;
        host.require_for_coordinator(&database, range, self)?;
        {
            let topology = self
                .topology
                .try_read()
                .ok_or_else(|| range_write_error("initial DATA routing publication busy"))?;
            genesis.require_current(&database, &topology)?;
        }
        collected.require_current(&service, &final_metadata, business)?;
        parent.check()?;
        // This grant follows the complete actual initial-factory/native QQ/
        // post-MQ producer above. Nonzero/history/image/joint recovery never
        // enters this branch. The owned reservation prevents an admission race.
        self.recovered_epochs.lock().insert(range.id, range.epoch);
        if let Err(error) = parent
            .check()
            .and_then(|_| final_metadata.require_live_request(business))
        {
            self.recovered_epochs.lock().remove(&range.id);
            return Err(error);
        }
        Ok(())
    }

    /// Diagnostic only; ordinary admission independently rechecks this state
    /// plus its actual native prefix/configuration and installed host policy.
    pub(crate) fn initial_data_leader_admission_observed(&self, range: &RangeDescriptor) -> bool {
        range.leader == self.local_node_id
            && self.topology.try_read().is_some_and(|topology| {
                topology.range_by_id(range.id).is_some_and(|current| {
                    crate::distribution_native_read::canonical_range(current)
                        == crate::distribution_native_read::canonical_range(range)
                })
            })
            && self.recovered_epochs.lock().get(&range.id) == Some(&range.epoch)
    }
    /// Exact stored marker DATA for reached-cut tests; no topology try-read
    /// can turn contention into a misleading absent-marker observation.
    #[cfg(test)]
    pub(crate) fn test_initial_genesis_recovered_epochs(&self) -> BTreeMap<RangeId, u64> {
        self.recovered_epochs.lock().clone()
    }
}

#[cfg(test)]
fn initial_read_failure(
    stage: &str,
    node: &ClusterNodeId,
    range: &RangeDescriptor,
    parent: &CancellationToken,
    error: &BicDbError,
) {
    use std::io::Write as _;
    // Keep this projection local: host-private protocol helpers do not become
    // public for logging. Formatting/writing DATA must not replace the error.
    struct Prefix {
        text: String,
        complete: bool,
    }
    impl std::fmt::Write for Prefix {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            if !self.complete {
                return Ok(());
            }
            let mut end = (4096 - self.text.len()).min(text.len());
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            self.text.push_str(&text[..end]);
            self.complete &= end == text.len();
            Ok(())
        }
    }
    let mut prefix = Prefix {
        text: String::new(),
        complete: true,
    };
    let formatted = std::fmt::write(&mut prefix, format_args!("{error}")).is_ok();
    let io_kind = match error {
        BicDbError::Io(io) => Some(format!("{:?}", io.kind())),
        _ => None,
    };
    let io_os_code = match error {
        BicDbError::Io(io) => io.raw_os_error(),
        _ => None,
    };
    let event = serde_json::json!({
        "stage":stage,"node":node,"range":range.id,"epoch":range.epoch,
        "actual_original_remaining_us":parent.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_micros()),
        "actual_original_cancelled":parent.is_cancel_requested(),
        "error_kind":format!("{:?}",std::mem::discriminant(error)),
        "io_kind":io_kind,"io_os_code":io_os_code,
        "display_prefix":prefix.text,"prefix_incomplete":!prefix.complete,
        "display_format_complete":formatted,
        "diagnostic_only_not_authority_or_no_effect":true,
    });
    let _ = writeln!(
        std::io::stderr().lock(),
        "INITIAL_NATIVE_READ_FAILURE {}",
        event
    );
}
