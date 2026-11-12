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

impl RangeRecoveryRuntime {
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
        let spawned = std::thread::Builder::new()
            .name("native-strong-point".into())
            .spawn(move || {
                let result = (|| {
                    let check = |stage, error| NativeStrongPointReadUnavailable::new(stage, error);
                    worker_token
                        .check()
                        .map_err(|e| check(NativeStrongReadStage::Canceled, e))?;
                    // No DB/service latch is held while waiting on the ACTUAL
                    // same map that serializes native origin/background recovery.
                    let _reservation = coordinator
                        .reserve_native_point_read(request.range_id, &worker_token)
                        .map_err(|e| check(NativeStrongReadStage::Reservation, e))?;
                    let range = coordinator
                        .require_strong_point_metadata(&first_metadata, &request)
                        .map_err(|e| check(NativeStrongReadStage::Route, e))?;
                    {
                        let service = loop {
                            worker_token
                                .check()
                                .map_err(|e| check(NativeStrongReadStage::Canceled, e))?;
                            if let Some(s) = runtime.service.try_lock_for(LATCH_POLL) {
                                break s;
                            }
                        };
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
                    let candidate = runtime
                        .acquire_native_read_candidate_with_operation(
                            first_metadata,
                            request.business_request_id,
                            request.challenge,
                            &worker_token,
                            Arc::clone(&operation),
                        )
                        .map_err(|e| check(NativeStrongReadStage::DataQuorum, e))?;
                    let enrolled = candidate
                        .check_enrolled_voter_births()
                        .map_err(|e| check(NativeStrongReadStage::WriterAdmission, e))?;
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
                    let service = loop {
                        worker_token
                            .check()
                            .map_err(|e| check(NativeStrongReadStage::Canceled, e))?;
                        if let Some(s) = runtime.service.try_lock_for(LATCH_POLL) {
                            break s;
                        }
                    };
                    coordinator
                        .require_strong_point_runtime(&runtime)
                        .map_err(|e| check(NativeStrongReadStage::FinalCurrentness, e))?;
                    pending
                        .finalize(final_metadata, &service, &coordinator, &worker_token)
                        .map_err(|e| check(NativeStrongReadStage::FinalCurrentness, e))
                })();
                // All actual local IO occurs on this worker. Its operation stays
                // owned even when the caller abandons this bounded result channel.
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
                return Unavailable(NativeStrongPointReadUnavailable::new(
                    NativeStrongReadStage::Canceled,
                    error,
                ));
            }
            match rx.recv_timeout(LATCH_POLL) {
                Ok(value) => {
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
