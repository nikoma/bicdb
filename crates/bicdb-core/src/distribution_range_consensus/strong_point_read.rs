//! Internal point-read facade using the ACTUAL installed coordinator's range
//! admission map. This reservation is not a native mutation/freeze permit.
use super::*;
use crate::db::InstalledNativeWriterHostGuard;
use crate::distribution_consensus::MetadataReadObservation;
use crate::distribution_native_read::{NativeStrongPointReadOutcome, NativeStrongPointReadRequest};
use crate::distribution_range_recovery::RangeRecoveryRuntime;
use crate::TcpClusterRelocationTransport;

pub(crate) struct NativePointReadReservation<'a, T: RangeWriteTransport> {
    _reservation: RangeAdmissionReservation<'a, T>,
}

impl<T: RangeWriteTransport> RangeWriteCoordinator<T> {
    /// Publication is not a read permit. Check the REAL hook/DB/coordinator;
    /// every request still acquires its own metadata/data/snapshot protocol.
    pub(crate) fn validate_strong_event_dispatch_install(
        self: &Arc<Self>,
        runtime: &RangeRecoveryRuntime,
        host: &crate::db::InstalledNativeWriterHostGuard,
    ) -> Result<()> {
        self.require_strong_point_runtime(runtime)?;
        let ranges = self
            .topology
            .read()
            .ranges
            .values()
            .map(crate::distribution_native_read::canonical_range)
            .collect::<Vec<_>>();
        if ranges.is_empty() {
            return Err(range_write_error(
                "installed event writer has no range configuration",
            ));
        }
        let service = self
            .local_service
            .try_lock()
            .ok_or_else(|| range_write_error("service busy publishing Strong dispatcher"))?;
        let database = service.database();
        let database = database
            .try_read()
            .ok_or_else(|| range_write_error("database busy publishing Strong dispatcher"))?;
        for range in ranges {
            host.require_for_coordinator(&database, &range, self)?;
        }
        Ok(())
    }

    pub(crate) fn strong_point_read(
        self: &Arc<Self>,
        runtime: &Arc<RangeRecoveryRuntime>,
        host: Option<Arc<InstalledNativeWriterHostGuard>>,
        metadata_transport: Arc<TcpClusterRelocationTransport>,
        first_metadata: MetadataReadObservation,
        request: NativeStrongPointReadRequest,
        parent: &CancellationToken,
    ) -> NativeStrongPointReadOutcome {
        runtime.strong_point_read(
            Arc::clone(self),
            host,
            metadata_transport,
            first_metadata,
            request,
            parent,
        )
    }
    pub(crate) fn require_strong_point_runtime(
        &self,
        runtime: &RangeRecoveryRuntime,
    ) -> Result<()> {
        if !self.native_image_mode {
            return Err(range_write_error(
                "strong point read requires an actual native coordinator",
            ));
        }
        runtime.require_point_read_service(&self.local_service, &self.local_node_id)
    }
    pub(crate) fn require_strong_point_metadata(
        &self,
        first: &MetadataReadObservation,
        request: &NativeStrongPointReadRequest,
    ) -> Result<RangeDescriptor> {
        let scope = request.require_metadata(first, &self.local_node_id)?;
        let topology = self.topology.read();
        let current = topology.range_by_id(scope.range.id).ok_or_else(|| {
            range_write_error("strong point route absent from installed coordinator")
        })?;
        if topology.cluster_id != scope.cluster_id
            || topology.range_image_bootstrap_id != Some(scope.bootstrap_id)
            || crate::distribution_native_read::canonical_range(current) != scope.range
        {
            return Err(range_write_error(
                "installed coordinator route differs from actual fresh metadata",
            ));
        }
        let committed = first.committed_topology(request.business_request_id)?;
        for (id, member) in &scope.members {
            let cached = topology.nodes.get(id).ok_or_else(|| {
                range_write_error("installed coordinator lacks current read voter")
            })?;
            if cached.incarnation != member.incarnation
                || cached.tls_certificate_sha256 != member.active_certificate_sha256
                || cached.pending_tls_certificate_sha256 != member.pending_certificate_sha256
                || cached.native_store_birth_v1 != committed.nodes[id].native_store_birth_v1
            {
                return Err(range_write_error(
                    "installed coordinator voter identity differs from actual metadata",
                ));
            }
        }
        first.require_live_request(request.business_request_id)?;
        Ok(scope.range)
    }
    pub(crate) fn reserve_native_point_read(
        &self,
        range_id: RangeId,
        parent: &CancellationToken,
    ) -> Result<NativePointReadReservation<'_, T>> {
        parent.check()?;
        let deadline = parent.deadline().ok_or_else(|| {
            range_write_error("point range reservation requires original deadline")
        })?;
        let mut active = self.active_ranges.lock();
        loop {
            parent.check()?;
            // Necessary scheduling refusal only. BOTH paths hold active_ranges
            // before the waiter map, so retrying readers cannot overtake an
            // actual queued hot repair once an existing holder exits.
            if self.hot_recovery_waiters.lock().contains_key(&range_id) {
                return Err(range_write_error("selected native range has queued hot recovery; retry within original read budget"));
            }
            if !active.contains_key(&range_id) {
                let reservation_id = format!("point-read:{}", Uuid::now_v7());
                active.insert(range_id, reservation_id.clone());
                return Ok(NativePointReadReservation {
                    _reservation: RangeAdmissionReservation {
                        coordinator: self,
                        range_id,
                        reservation_id,
                        release_on_drop: true,
                    },
                });
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| range_write_error("point range reservation expired"))?;
            self.active_range_changed
                .wait_for(&mut active, remaining.min(Duration::from_millis(5)));
        }
    }
}
