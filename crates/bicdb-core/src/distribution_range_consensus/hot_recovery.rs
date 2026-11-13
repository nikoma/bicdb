//! HOT original-origin coordination. Reservations exclude APP/Strong while
//! fresh opaque authority is consumed; no raw progress setter is exposed.
use super::*;
use crate::db::InstalledNativeWriterHostGuard;
use crate::distribution_range_recovery::{RangeRecoveryRuntime, RecoveryRoleAdmission};
use crate::TcpClusterRelocationTransport;

pub(crate) struct NativeHotRangeReservation<'a> {
    actual: RangeAdmissionReservation<'a, TcpClusterRelocationTransport>,
    range: RangeDescriptor,
    store_open: Uuid,
    store_slot: crate::distribution_data::RangeWriteServiceSlotGuard,
    _waiter: NativeHotWaiter<'a>,
}
impl NativeHotRangeReservation<'_> {
    /// Necessary original-slot refusal, never range/read/write permission.
    pub(crate) fn require_actual_store_slot(&self) -> Result<()> {
        self.store_slot.require_live()?;
        if self.store_slot.id() != self.store_open {
            return Err(range_write_error(
                "hot repair original service Store slot/open changed",
            ));
        }
        Ok(())
    }
}
struct NativeHotWaiter<'a> {
    coordinator: &'a RangeWriteCoordinator<TcpClusterRelocationTransport>,
    range: RangeId,
    owner: Uuid,
}
impl Drop for NativeHotWaiter<'_> {
    fn drop(&mut self) {
        // SAME lock order as the reader and enqueue path. No engine/network
        // guard is held by this bounded attempt's memory-only cleanup.
        let _active = self.coordinator.active_ranges.lock();
        let mut waiters = self.coordinator.hot_recovery_waiters.lock();
        if waiters.get(&self.range) == Some(&self.owner) {
            waiters.remove(&self.range);
            self.coordinator.active_range_changed.notify_all();
        }
    }
}
impl RangeWriteCoordinator<TcpClusterRelocationTransport> {
    pub(crate) fn reserve_hot_native_recovery<'a>(
        self: &'a Arc<Self>,
        runtime: &Arc<RangeRecoveryRuntime>,
        host: &Arc<InstalledNativeWriterHostGuard>,
        range: &RangeDescriptor,
        parent: &CancellationToken,
    ) -> Result<NativeHotRangeReservation<'a>> {
        parent.check()?;
        if !self.native_image_mode || range.leader != self.local_node_id {
            return Err(range_write_error(
                "hot recovery requires actual local native origin",
            ));
        }
        runtime.require_point_read_service(&self.local_service, &self.local_node_id)?;
        let service = loop {
            parent.check()?;
            if let Some(s) = self.local_service.try_lock_for(Duration::from_millis(5)) {
                break s;
            }
        };
        let db = service.database();
        let db = db
            .try_read()
            .ok_or_else(|| range_write_error("hot recovery actual DB busy"))?;
        host.require_for_coordinator(&db, range, self)?;
        service.require_native_writer_host_for_range(&db, range)?;
        let store_slot = service.native_replica_listener_store_open_guard();
        let store_open = service.native_initial_listener_store_open_id();
        store_slot.require_live()?;
        if store_slot.id() != store_open {
            return Err(range_write_error(
                "hot repair actual service Store slot/open differs",
            ));
        }
        parent.check()?;
        drop(db);
        drop(service);
        let deadline = parent.deadline().ok_or_else(|| {
            range_write_error("hot recovery reservation requires original deadline")
        })?;
        let owner = Uuid::now_v7();
        {
            let _active = self.active_ranges.lock();
            let mut waiters = self.hot_recovery_waiters.lock();
            if waiters.contains_key(&range.id) || waiters.len() >= 64 {
                return Err(range_write_error(
                    "actual hot recovery waiter busy or bounded capacity exhausted",
                ));
            }
            waiters.insert(range.id, owner);
        }
        let waiter = NativeHotWaiter {
            coordinator: self.as_ref(),
            range: range.id,
            owner,
        };
        // Declared AFTER waiter: unwind also drops active before waiter Drop.
        let mut active = self.active_ranges.lock();
        // On cancellation/error drop active BEFORE waiter cleanup reacquires
        // the same mutex. Explicitly scope the wait result to preserve order.
        let acquired =
            (|| -> Result<RangeAdmissionReservation<'a, TcpClusterRelocationTransport>> {
                loop {
                    parent.check()?;
                    store_slot.require_live()?;
                    if !active.contains_key(&range.id) {
                        let reservation_id = format!("hot-recovery:{}", Uuid::now_v7());
                        active.insert(range.id, reservation_id.clone());
                        return Ok(RangeAdmissionReservation {
                            coordinator: self.as_ref(),
                            range_id: range.id,
                            reservation_id,
                            release_on_drop: true,
                        });
                    }
                    let remaining = deadline
                        .checked_duration_since(Instant::now())
                        .ok_or(crate::BicDbError::QueryTimedOut)?;
                    self.active_range_changed
                        .wait_for(&mut active, remaining.min(Duration::from_millis(5)));
                }
            })();
        drop(active);
        let actual = acquired?;
        Ok(NativeHotRangeReservation {
            actual,
            range: crate::distribution_native_read::canonical_range(range),
            store_open,
            store_slot,
            _waiter: waiter,
        })
    }
    fn require_hot_reservation(
        &self,
        held: &NativeHotRangeReservation<'_>,
        role: &RecoveryRoleAdmission,
    ) -> Result<()> {
        held.require_actual_store_slot()?;
        if !std::ptr::eq(held.actual.coordinator, self)
            || held.actual.range_id != role.current_range().id
            || held.range != crate::distribution_native_read::canonical_range(role.current_range())
            || self.active_ranges.lock().get(&held.actual.range_id)
                != Some(&held.actual.reservation_id)
        {
            return Err(range_write_error(
                "hot repair lost the SAME original coordinator/range reservation",
            ));
        }
        Ok(())
    }
    pub(crate) fn publish_hot_native_recovery(
        self: &Arc<Self>,
        runtime: &Arc<RangeRecoveryRuntime>,
        host: &Arc<InstalledNativeWriterHostGuard>,
        held: &NativeHotRangeReservation<'_>,
        role: &RecoveryRoleAdmission,
        scope: &RecoveryFenceScope,
        command: &RangeWriteCommand,
        parent: &CancellationToken,
    ) -> Result<()> {
        let token = parent.child_with_deadline(role.deadline());
        token.check()?;
        scope.require_role(role)?;
        self.require_hot_reservation(held, role)?;
        runtime.require_point_read_service(&self.local_service, &self.local_node_id)?;
        let remaining = || -> Result<Duration> {
            token.check()?;
            token
                .deadline()
                .and_then(|d| d.checked_duration_since(Instant::now()))
                .filter(|d| !d.is_zero())
                .map(|d| d.min(Duration::from_secs(5)))
                .ok_or(BicDbError::QueryTimedOut)
        };
        let metadata_leader = runtime
            .event_metadata_leader_hint(&token)?
            .ok_or_else(|| range_write_error("hot recovery current metadata leader unknown"))?;
        let business = Uuid::now_v7();
        let first = self.transport.acquire_metadata_read_observation(
            &metadata_leader,
            business,
            scope.range_id,
            scope.range_epoch,
            remaining()?,
            &token,
        )?;
        self.require_hot_reservation(held, role)?;
        let candidate =
            runtime.acquire_native_read_candidate(first, business, Uuid::now_v7(), &token)?;
        self.require_hot_reservation(held, role)?;
        let collected = {
            let service = loop {
                token.check()?;
                if let Some(s) = self.local_service.try_lock_for(Duration::from_millis(5)) {
                    break s;
                }
            };
            let db = service.database();
            let db = db.try_read().ok_or_else(|| {
                range_write_error("hot recovery DB busy validating original writer")
            })?;
            host.require_for_coordinator(&db, role.current_range(), self)?;
            drop(db);
            candidate.into_hot_recovered_open(&service, role, scope, command, &token)?
        };
        // No DB/service latch crosses either network boundary.
        self.require_hot_reservation(held, role)?;
        let final_metadata = self.transport.acquire_metadata_read_observation(
            &metadata_leader,
            business,
            scope.range_id,
            scope.range_epoch,
            remaining()?,
            &token,
        )?;
        self.require_hot_reservation(held, role)?;
        let service = loop {
            token.check()?;
            if let Some(s) = self.local_service.try_lock_for(Duration::from_millis(5)) {
                break s;
            }
        };
        collected.require_current(&service, &final_metadata, business)?;
        let db = service.database();
        let db = db.try_read().ok_or_else(|| {
            range_write_error("hot recovery DB busy before recovered publication")
        })?;
        host.require_for_coordinator(&db, role.current_range(), self)?;
        self.require_hot_reservation(held, role)?;
        let topology = self
            .topology
            .try_read()
            .ok_or_else(|| range_write_error("hot recovery topology publication busy"))?;
        if topology
            .range_by_id(scope.range_id)
            .is_none_or(|r| crate::distribution_native_read::canonical_range(r) != held.range)
        {
            return Err(range_write_error(
                "hot repair original range changed before publication",
            ));
        }
        token.check()?;
        role.require_live_request()?;
        self.recovered_epochs
            .lock()
            .insert(scope.range_id, scope.range_epoch);
        // SAME reservation still excludes every APP ACK until this last check.
        if let Err(error) = token
            .check()
            .and_then(|_| final_metadata.require_live_request(business))
            .and_then(|_| role.require_live_request())
            .and_then(|_| held.require_actual_store_slot())
            .and_then(|_| host.require_for_coordinator(&db, role.current_range(), self))
        {
            self.recovered_epochs.lock().remove(&scope.range_id);
            return Err(error);
        }
        Ok(())
    }
}
#[cfg(test)]
impl RangeWriteCoordinator<TcpClusterRelocationTransport> {
    pub(crate) fn test_hot_waiter_and_active(&self, range: RangeId) -> (bool, Option<String>) {
        let active = self.active_ranges.lock();
        (
            self.hot_recovery_waiters.lock().contains_key(&range),
            active.get(&range).cloned(),
        )
    }
}
