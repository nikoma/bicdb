//! Internal, bounded consuming protocol. The public host/gateway adapter is a
//! separate integration: no legacy Get fallback or decoded permission exists.
use super::*;
use crate::db::InstalledNativeWriterHostGuard;
use crate::distribution_consensus::MetadataReadObservation;
use crate::distribution_native_read::{
    NativeStrongPointReadOutcome, NativeStrongPointReadRequest, NativeStrongPointReadUnavailable,
    NativeStrongReadStage,
};
use crate::distribution_range_consensus::{RangeWriteCoordinator, RangeWriteTransport};
use crate::TcpClusterRelocationTransport;
use std::sync::mpsc;

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct NativePointSnapshotBarrier {
    entered: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
}
#[cfg(test)]
impl NativePointSnapshotBarrier {
    pub(crate) fn entered(&self) -> bool {
        self.entered.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub(crate) fn release(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
impl RangeRecoveryRuntime {
    pub(crate) fn point_read_local_node_id(&self) -> &ClusterNodeId {
        &self.local_node_id
    }
    pub(crate) fn attach_native_event_dispatcher(
        self: &Arc<Self>,
        dispatcher: &Arc<crate::distribution_native_read::NativeStrongEventDispatcher>,
    ) -> Result<()> {
        dispatcher.require_runtime(self)?;
        *self.event_dispatcher.lock() = Arc::downgrade(dispatcher);
        Ok(())
    }
    pub(crate) fn native_event_dispatcher(
        &self,
    ) -> Option<Arc<crate::distribution_native_read::NativeStrongEventDispatcher>> {
        self.event_dispatcher.lock().upgrade()
    }
    pub(crate) fn event_metadata_leader_hint(
        &self,
        token: &CancellationToken,
    ) -> Result<Option<ClusterNodeId>> {
        self.provider.current_metadata_leader_hint(token)
    }

    #[cfg(test)]
    pub(crate) fn test_hold_point_after_actual_snapshot(&self) -> Arc<NativePointSnapshotBarrier> {
        let barrier = Arc::new(NativePointSnapshotBarrier::default());
        *self.test_point_snapshot_barrier.lock() = Some(Arc::clone(&barrier));
        barrier
    }

    pub(crate) fn require_point_read_service(
        &self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        node: &ClusterNodeId,
    ) -> Result<()> {
        if !Arc::ptr_eq(service, &self.service) || node != &self.local_node_id {
            return Err(invalid(
                "point reader uses another actual coordinator/service",
            ));
        }
        Ok(())
    }
    pub(crate) fn strong_point_read<T: RangeWriteTransport>(
        self: &Arc<Self>,
        coordinator: Arc<RangeWriteCoordinator<T>>,
        host: Option<Arc<InstalledNativeWriterHostGuard>>,
        metadata_transport: Arc<TcpClusterRelocationTransport>,
        first_metadata: MetadataReadObservation,
        request: NativeStrongPointReadRequest,
        parent: &CancellationToken,
    ) -> NativeStrongPointReadOutcome {
        use NativeStrongPointReadOutcome::{Observed, Unavailable};
        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let _strong_context = crate::distribution_native_read::trace::Context::enter(
            &self.local_node_id,
            request.business_request_id,
            request.challenge,
            request.range_id,
            request.range_epoch,
            parent.deadline(),
        );
        #[cfg(test)]
        let _strong_call = crate::distribution_native_read::trace::Span::current("strong-caller");
        // STRONG_TRACE_END

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        crate::distribution_native_read::trace::Span::detail(
            "strong-original-allowances",
            format_args!(
                "request_budget_ms={} first_metadata_deadline_us={:?}",
                request.budget_ms,
                crate::distribution_native_read::trace::deadline_us(Some(
                    first_metadata.deadline()
                ))
            ),
        );
        // STRONG_TRACE_END
        let Some(host) = host else {
            return Unavailable(NativeStrongPointReadUnavailable::new(
                NativeStrongReadStage::WriterAdmission,
                invalid("actual installed enforced-writer policy is missing"),
            ));
        };

        let token = match request.cancellation(parent) {
            Ok(t) => t.child_with_deadline(first_metadata.deadline()),
            Err(error) => {
                return Unavailable(NativeStrongPointReadUnavailable::new(
                    NativeStrongReadStage::Request,
                    error,
                ))
            }
        };
        if let Err(error) = coordinator.require_strong_point_runtime(self) {
            return Unavailable(NativeStrongPointReadUnavailable::new(
                NativeStrongReadStage::WriterAdmission,
                error,
            ));
        }
        let operation = match self.try_reserve_point_context(request.range_id) {
            Ok(operation) => operation,
            Err(error) => {
                return Unavailable(NativeStrongPointReadUnavailable::new(
                    NativeStrongReadStage::Reservation,
                    error,
                ))
            }
        };
        let runtime = Arc::clone(self);
        let worker_token = token.clone();
        let (tx, rx) = mpsc::sync_channel(1);

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let strong_queued = Instant::now();
        // STRONG_TRACE_END
        let spawned = std::thread::Builder::new()
            .name("native-strong-point".into())
            .spawn(move || {
                // STRONG_TRACE_BEGIN
                #[cfg(test)]
                let _strong_context = crate::distribution_native_read::trace::Context::enter(
                    &runtime.local_node_id,
                    request.business_request_id,
                    request.challenge,
                    request.range_id,
                    request.range_epoch,
                    worker_token.deadline(),
                );
                #[cfg(test)]
                let _strong_worker =
                    crate::distribution_native_read::trace::Span::current("strong-worker");
                #[cfg(test)]
                crate::distribution_native_read::trace::Span::detail(
                    "strong-worker",
                    format_args!("queued_us={}", strong_queued.elapsed().as_micros()),
                );
                // STRONG_TRACE_END

                let result = (|| {
                    let check = |stage, error| NativeStrongPointReadUnavailable::new(stage, error);
                    worker_token
                        .check()
                        .map_err(|e| check(NativeStrongReadStage::Canceled, e))?;
                    // No DB/service latch is held while waiting on the ACTUAL
                    // same map that serializes native origin/background recovery.

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    let strong_reservation =
                        crate::distribution_native_read::trace::Span::current("strong-reservation");
                    // STRONG_TRACE_END
                    let _reservation = coordinator
                        .reserve_native_point_read(request.range_id, &worker_token)
                        .map_err(|e| check(NativeStrongReadStage::Reservation, e))?;

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    drop(strong_reservation);
                    // STRONG_TRACE_END
                    let range = coordinator
                        .require_strong_point_metadata(&first_metadata, &request)
                        .map_err(|e| check(NativeStrongReadStage::Route, e))?;
                    {
                        // STRONG_TRACE_BEGIN
                        #[cfg(test)]
                        let strong_writer_service_wait =
                            crate::distribution_native_read::trace::Span::current(
                                "strong-writer-service-wait",
                            );
                        // STRONG_TRACE_END

                        let service = loop {
                            worker_token
                                .check()
                                .map_err(|e| check(NativeStrongReadStage::Canceled, e))?;
                            if let Some(s) = runtime.service.try_lock_for(LATCH_POLL) {
                                break s;
                            }
                        };

                        // STRONG_TRACE_BEGIN
                        #[cfg(test)]
                        drop(strong_writer_service_wait);
                        // STRONG_TRACE_END
                        let database = service.database();
                        let database = database.try_read().ok_or_else(|| {
                            check(
                                NativeStrongReadStage::WriterAdmission,
                                invalid("database busy validating actual installed point writer"),
                            )
                        })?;
                        host.require_for_coordinator(&database, &range, &coordinator)
                            .map_err(|e| check(NativeStrongReadStage::WriterAdmission, e))?;
                    }
                    let metadata_leader = first_metadata.metadata_leader_id().clone();
                    // Reuse ONE operation, including its local probe/final IO.
                    // No recursive fifth context or new recovery session is made.

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    let strong_data_quorum =
                        crate::distribution_native_read::trace::Span::current("strong-data-quorum");
                    // STRONG_TRACE_END
                    let candidate = runtime
                        .acquire_native_read_candidate_with_operation(
                            first_metadata,
                            request.business_request_id,
                            request.challenge,
                            &worker_token,
                            Arc::clone(&operation),
                        )
                        .map_err(|e| check(NativeStrongReadStage::DataQuorum, e))?;

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    drop(strong_data_quorum);
                    // STRONG_TRACE_END

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    let strong_birth_enrollment =
                        crate::distribution_native_read::trace::Span::current(
                            "strong-birth-enrollment",
                        );
                    // STRONG_TRACE_END
                    let enrolled = candidate
                        .check_enrolled_voter_births()
                        .map_err(|e| check(NativeStrongReadStage::WriterAdmission, e))?;

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    drop(strong_birth_enrollment);
                    // STRONG_TRACE_END

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    let strong_snapshot =
                        crate::distribution_native_read::trace::Span::current("strong-snapshot");
                    // STRONG_TRACE_END
                    let pending = {
                        let service = loop {
                            worker_token
                                .check()
                                .map_err(|e| check(NativeStrongReadStage::Canceled, e))?;
                            if let Some(s) = runtime.service.try_lock_for(LATCH_POLL) {
                                break s;
                            }
                        };
                        enrolled
                            .capture_actual_point_read(
                                &service,
                                &coordinator,
                                Arc::clone(&host),
                                &request,
                                &worker_token,
                            )
                            .map_err(|e| check(NativeStrongReadStage::Snapshot, e))?
                    };

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    drop(strong_snapshot);
                    // STRONG_TRACE_END
                    #[cfg(test)]
                    if let Some(barrier) = runtime.test_point_snapshot_barrier.lock().take() {
                        barrier
                            .entered
                            .store(true, std::sync::atomic::Ordering::SeqCst);
                        // Actual snapshot has been captured; no DB/service latch
                        // crosses this one-shot, bounded test return barrier.
                        while !barrier.released.load(std::sync::atomic::Ordering::SeqCst) {
                            worker_token
                                .check()
                                .map_err(|e| check(NativeStrongReadStage::Canceled, e))?;
                            std::thread::sleep(LATCH_POLL);
                        }
                    }
                    // This is genuinely new generic range-leader MQ acquired
                    // AFTER the owned value; no service/DB latch crosses the call.
                    worker_token
                        .check()
                        .map_err(|e| check(NativeStrongReadStage::Canceled, e))?;
                    let remaining = worker_token
                        .deadline()
                        .and_then(|d| d.checked_duration_since(Instant::now()))
                        .ok_or_else(|| {
                            check(
                                NativeStrongReadStage::FinalMetadata,
                                invalid("original point budget expired"),
                            )
                        })?;

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    let strong_final_metadata =
                        crate::distribution_native_read::trace::Span::current(
                            "strong-final-metadata",
                        );
                    // STRONG_TRACE_END
                    let final_metadata = metadata_transport
                        .acquire_metadata_read_observation(
                            &metadata_leader,
                            request.business_request_id,
                            request.range_id,
                            request.range_epoch,
                            remaining.min(Duration::from_secs(5)),
                            &worker_token,
                        )
                        .map_err(|e| check(NativeStrongReadStage::FinalMetadata, e))?;
                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    drop(strong_final_metadata);
                    #[cfg(test)]
                    let strong_final_service_wait =
                        crate::distribution_native_read::trace::Span::current(
                            "strong-final-service-wait",
                        );
                    // STRONG_TRACE_END

                    let service = loop {
                        worker_token
                            .check()
                            .map_err(|e| check(NativeStrongReadStage::Canceled, e))?;
                        if let Some(s) = runtime.service.try_lock_for(LATCH_POLL) {
                            break s;
                        }
                    };

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    drop(strong_final_service_wait);
                    // STRONG_TRACE_END
                    coordinator
                        .require_strong_point_runtime(&runtime)
                        .map_err(|e| check(NativeStrongReadStage::FinalCurrentness, e))?;

                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    let _strong_final_currentness =
                        crate::distribution_native_read::trace::Span::current(
                            "strong-final-currentness",
                        );
                    // STRONG_TRACE_END
                    pending
                        .finalize(final_metadata, &service, &coordinator, &worker_token)
                        .map_err(|e| check(NativeStrongReadStage::FinalCurrentness, e))
                })();
                // All actual local IO occurs on this worker. Its operation stays
                // owned even when the caller abandons this bounded result channel.

                // STRONG_TRACE_BEGIN
                #[cfg(test)]
                crate::distribution_native_read::trace::Span::detail(
                    "strong-outcome",
                    format_args!(
                        "ok={} unavailable={:?}",
                        result.is_ok(),
                        result.as_ref().err()
                    ),
                );
                // STRONG_TRACE_END
                drop(operation);
                let _ = tx.send(match result {
                    Ok(value) => Observed(value),
                    Err(error) => Unavailable(error),
                });
            });
        if let Err(error) = spawned {
            return Unavailable(NativeStrongPointReadUnavailable::new(
                NativeStrongReadStage::Reservation,
                error,
            ));
        }
        loop {
            if let Err(error) = token.check() {
                // STRONG_TRACE_BEGIN
                #[cfg(test)]
                crate::distribution_native_read::trace::Span::detail(
                    "strong-caller-token-refusal",
                    format_args!("error={error}"),
                );
                // STRONG_TRACE_END

                return Unavailable(NativeStrongPointReadUnavailable::new(
                    NativeStrongReadStage::Canceled,
                    error,
                ));
            }
            match rx.recv_timeout(LATCH_POLL) {
                Ok(value) => {
                    // STRONG_TRACE_BEGIN
                    #[cfg(test)]
                    crate::distribution_native_read::trace::Span::detail(
                        "strong-caller-result",
                        format_args!(
                            "outcome={:?}",
                            match &value {
                                Observed(_) => "observed",
                                Unavailable(_) => "unavailable",
                            }
                        ),
                    );
                    // STRONG_TRACE_END

                    if let Err(error) = token.check() {
                        return Unavailable(NativeStrongPointReadUnavailable::new(
                            NativeStrongReadStage::Canceled,
                            error,
                        ));
                    }
                    return value;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Unavailable(NativeStrongPointReadUnavailable::new(
                        NativeStrongReadStage::Reservation,
                        invalid("owned strong point worker disconnected"),
                    ))
                }
            }
        }
    }
}
