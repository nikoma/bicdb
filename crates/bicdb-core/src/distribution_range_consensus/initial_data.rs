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
        let business = Uuid::now_v7();
        let first = self.transport.acquire_metadata_read_observation(
            metadata_leader,
            business,
            range.id,
            range.epoch,
            remaining()?,
            parent,
        )?;
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
            .acquire_native_read_candidate(first, business, Uuid::now_v7(), parent)?
            .into_initial_data_genesis(genesis)?;
        let final_metadata = self.transport.acquire_metadata_read_observation(
            metadata_leader,
            business,
            range.id,
            range.epoch,
            remaining()?,
            parent,
        )?;
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

    /// Reopened INITIAL owner only: the ordinary leader recovery of the
    /// ranges this node leads, one range at a time, in a NEW process that
    /// holds no recovered epoch. Exactly `recover_local_leadership` for each
    /// range, preceded by one more local refusal: a protected writer-start of
    /// this node above its native prefix is an unfinished command even when
    /// the earlier process ended before writing its raw log entry, and it is
    /// left to the native abort/tail recovery. Returns the recovered count;
    /// `Err` lists every range that stays unrecovered.
    pub(crate) fn recover_reopened_local_leadership(&self, max_ranges: usize) -> Result<usize> {
        self.require_expected_native_recovery_store()?;
        if !self.native_image_mode || max_ranges == 0 || max_ranges > 1_024 {
            return Err(range_write_error(
                "reopened leader recovery requires a native store and a limit in 1..=1024",
            ));
        }
        let mut ranges = {
            let topology = self.topology.read_recursive();
            let recovered = self.recovered_epochs.lock();
            topology
                .ranges
                .values()
                .filter(|range| {
                    range.leader == self.local_node_id
                        && recovered.get(&range.id) != Some(&range.epoch)
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        ranges.sort_by_key(|range| range.id);
        ranges.truncate(max_ranges);
        let mut completed = 0usize;
        let mut failures = Vec::new();
        for range in ranges {
            let unfinished = (|| -> Result<Option<u64>> {
                let service = self
                    .local_service
                    .try_lock_for(Duration::from_millis(250))
                    .ok_or_else(|| range_write_error("reopened leader recovery service busy"))?;
                let database = service.database();
                let database = database
                    .try_read()
                    .ok_or_else(|| range_write_error("reopened leader recovery database busy"))?;
                service.unfinished_native_writer_start(&database, &range)
            })();
            let outcome = match unfinished {
                Err(error) => Err(error),
                Ok(Some(index)) => Err(range_write_error(format!(
                    "native writer-start of an unfinished command at index {index} requires native abort/tail recovery; ordinary leader recovery is not attempted"
                ))),
                Ok(None) => self
                    .resolve_reopened_native_completed_raw_entry(&range)
                    .and_then(|()| self.recover_one_range_leadership(&range)),
            };
            match outcome {
                Ok(()) => {
                    self.recovered_epochs.lock().insert(range.id, range.epoch);
                    completed = completed.saturating_add(1);
                }
                Err(error) => failures.push(format!("{}: {error}", range.id)),
            }
        }
        let topology = self.topology.read_recursive();
        self.recovered_epochs.lock().retain(|range_id, epoch| {
            topology
                .range_by_id(*range_id)
                .is_some_and(|range| range.leader == self.local_node_id && range.epoch == *epoch)
        });
        if !failures.is_empty() {
            return Err(range_write_error(format!(
                "range leadership recovery deferred for {} range(s): {}",
                failures.len(),
                failures.join(" | ")
            )));
        }
        Ok(completed)
    }

    /// Reopened INITIAL leader only: when its earlier process ended after
    /// the native completion of its own certified command and before the
    /// raw Applied mark, record that raw resolution from its own durable
    /// certificate and native history (see
    /// `resolve_reopened_native_completed_raw_entry`). Any other state is
    /// untouched; the native-prefix refusal of the ordinary leader recovery
    /// is not relaxed: it is simply no longer reached for this state, since
    /// the raw log then ends exactly at the native prefix.
    fn resolve_reopened_native_completed_raw_entry(&self, range: &RangeDescriptor) -> Result<()> {
        let mutation = self.range_mutations.try_enter(range.id)?;
        let authority = &RangeMutationAuthority::Mutation(&mutation);
        let _reservation = self.try_reserve_range(range.id)?;
        let mut service = self
            .local_service
            .try_lock_for(Duration::from_millis(250))
            .ok_or_else(|| range_write_error("reopened leader raw resolution service busy"))?;
        let database = service.database();
        let database = database
            .try_read()
            .ok_or_else(|| range_write_error("reopened leader raw resolution database busy"))?;
        service.resolve_reopened_native_completed_raw_entry(&database, range, authority)?;
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
}
