//! Installable BEFORE-listener slot. Persistent bindings are weak; only active
//! bounded workers retain their real coordinator/DB lifetimes. No slot/bool or
//! HostMQ routing observation is a Strong read permission.
use super::*;
use crate::db::InstalledNativeWriterHostGuard;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_range_consensus::RangeWriteCoordinator;
use crate::distribution_range_recovery::RangeRecoveryRuntime;
use crate::{ClusterDataNodeService, TcpClusterRelocationTransport};
use parking_lot::Mutex;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc, Weak,
};

type Coordinator = RangeWriteCoordinator<TcpClusterRelocationTransport>;
#[derive(Debug)]
struct Binding {
    generation: Uuid,
    coordinator: Weak<Coordinator>,
    host: Weak<InstalledNativeWriterHostGuard>,
}
#[derive(Debug)]
pub(crate) struct NativeStrongEventDispatcher {
    service: Weak<Mutex<ClusterDataNodeService>>,
    runtime: Weak<RangeRecoveryRuntime>,
    transport: Weak<TcpClusterRelocationTransport>,
    topology: Weak<parking_lot::RwLock<crate::ClusterTopology>>,
    local: ClusterNodeId,
    binding: Mutex<Option<Binding>>,
    active: Arc<AtomicUsize>,
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl NativeStrongEventDispatcher {
    pub(crate) fn new(
        service: &Arc<Mutex<ClusterDataNodeService>>,
        runtime: &Arc<RangeRecoveryRuntime>,
        transport: &Arc<TcpClusterRelocationTransport>,
        topology: &Arc<parking_lot::RwLock<crate::ClusterTopology>>,
    ) -> Result<Arc<Self>> {
        runtime.require_point_read_service(service, runtime.point_read_local_node_id())?;
        let local = runtime.point_read_local_node_id().clone();
        if transport.local_metadata_read_participant().node_id() != &local {
            return Err(invalid(
                "Strong event transport is another actual local identity",
            ));
        }
        let out = Arc::new(Self {
            service: Arc::downgrade(service),
            runtime: Arc::downgrade(runtime),
            transport: Arc::downgrade(transport),
            topology: Arc::downgrade(topology),
            local,
            binding: Mutex::new(None),
            active: Arc::new(AtomicUsize::new(0)),
        });
        runtime.attach_native_event_dispatcher(&out)?;
        Ok(out)
    }
    pub(crate) fn require_runtime(&self, runtime: &Arc<RangeRecoveryRuntime>) -> Result<()> {
        let actual = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("Strong dispatcher runtime ended"))?;
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("Strong dispatcher service ended"))?;
        if !Arc::ptr_eq(&actual, runtime) {
            return Err(invalid("Strong dispatcher uses another runtime"));
        }
        runtime.require_point_read_service(&service, &self.local)
    }
    pub(crate) fn publish(
        self: &Arc<Self>,
        coordinator: &Arc<Coordinator>,
        host: &Arc<InstalledNativeWriterHostGuard>,
    ) -> Result<Uuid> {
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("Strong dispatcher runtime ended"))?;
        self.require_runtime(&runtime)?;
        coordinator.validate_strong_event_dispatch_install(&runtime, host)?;
        let generation = Uuid::now_v7();
        *self.binding.lock() = Some(Binding {
            generation,
            coordinator: Arc::downgrade(coordinator),
            host: Arc::downgrade(host),
        });
        Ok(generation)
    }
    pub(crate) fn clear(&self) {
        *self.binding.lock() = None;
    }
    fn snapshot(&self) -> Result<(Uuid, Arc<Coordinator>, Arc<InstalledNativeWriterHostGuard>)> {
        let binding = self
            .binding
            .try_lock()
            .ok_or_else(|| invalid("Strong dispatcher publication busy"))?;
        let b = binding
            .as_ref()
            .ok_or_else(|| invalid("Strong dispatcher is not published"))?;
        Ok((
            b.generation,
            b.coordinator
                .upgrade()
                .ok_or_else(|| invalid("installed Strong coordinator ended"))?,
            b.host
                .upgrade()
                .ok_or_else(|| invalid("installed Strong writer guard ended"))?,
        ))
    }
    fn require_generation(self: &Arc<Self>, generation: Uuid) -> Result<()> {
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("Strong dispatcher runtime ended"))?;
        if runtime
            .native_event_dispatcher()
            .is_none_or(|d| !Arc::ptr_eq(&d, self))
            || self
                .binding
                .lock()
                .as_ref()
                .is_none_or(|b| b.generation != generation)
        {
            return Err(invalid(
                "Strong dispatcher registration changed during read",
            ));
        }
        Ok(())
    }
    pub(crate) fn dispatch(
        self: &Arc<Self>,
        request: NativeEventGetRequest,
        parent: &CancellationToken,
    ) -> NativeEventGetReply {
        let token = match request.cancellation(parent) {
            Ok(t) => t,
            Err(e) => {
                return NativeEventGetReply::unavailable(
                    &request,
                    NativeEventGetUnavailableCode::InvalidRequest,
                    None,
                    e,
                )
            }
        };
        let (generation, coordinator, host) = match self.snapshot() {
            Ok(s) => s,
            Err(e) => {
                return NativeEventGetReply::unavailable(
                    &request,
                    NativeEventGetUnavailableCode::MissingWriter,
                    None,
                    e,
                )
            }
        };
        if self
            .active
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < 4).then_some(n + 1)
            })
            .is_err()
        {
            return NativeEventGetReply::unavailable(
                &request,
                NativeEventGetUnavailableCode::Busy,
                None,
                "Strong endpoint capacity is retained by actual workers",
            );
        }
        let active = Active(Arc::clone(&self.active));
        let dispatcher = Arc::clone(self);
        let worker_request = request.clone();
        let worker_token = token.clone();
        let (tx, rx) = mpsc::sync_channel(1);
        let spawned = std::thread::Builder::new()
            .name("native-event-get".into())
            .spawn(move || {
                let _active = active;
                let result = dispatcher.execute(
                    &worker_request,
                    generation,
                    coordinator,
                    host,
                    &worker_token,
                );
                let _ = tx.send(result);
            });
        if let Err(e) = spawned {
            return NativeEventGetReply::unavailable(
                &request,
                NativeEventGetUnavailableCode::Busy,
                None,
                e,
            );
        }
        loop {
            if let Err(e) = token.check() {
                return NativeEventGetReply::unavailable(
                    &request,
                    NativeEventGetUnavailableCode::Canceled,
                    None,
                    e,
                );
            }
            match rx.recv_timeout(Duration::from_millis(5)) {
                Ok(reply) => {
                    if let Err(e) = token.check() {
                        return NativeEventGetReply::unavailable(
                            &request,
                            NativeEventGetUnavailableCode::Canceled,
                            None,
                            e,
                        );
                    }
                    return reply;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return NativeEventGetReply::unavailable(
                        &request,
                        NativeEventGetUnavailableCode::Busy,
                        None,
                        "Strong endpoint worker disconnected",
                    )
                }
            }
        }
    }
    fn execute(
        self: &Arc<Self>,
        request: &NativeEventGetRequest,
        generation: Uuid,
        coordinator: Arc<Coordinator>,
        host: Arc<InstalledNativeWriterHostGuard>,
        token: &CancellationToken,
    ) -> NativeEventGetReply {
        let fail = |code, route, error: &dyn std::fmt::Display| {
            NativeEventGetReply::unavailable(request, code, route, error)
        };
        let runtime = match self.runtime.upgrade() {
            Some(r) => r,
            None => {
                return fail(
                    NativeEventGetUnavailableCode::MissingDispatcher,
                    None,
                    &"runtime ended",
                )
            }
        };
        let transport = match self.transport.upgrade() {
            Some(t) => t,
            None => {
                return fail(
                    NativeEventGetUnavailableCode::MissingDispatcher,
                    None,
                    &"metadata transport ended",
                )
            }
        };
        let host_metadata =
            match self.fresh_host_route(&runtime, &transport, request.business_request_id, token) {
                Ok(m) => m,
                Err(e) => {
                    return fail(NativeEventGetUnavailableCode::MetadataUnavailable, None, &e)
                }
            };
        let id = match request.validate() {
            Ok(id) => id,
            Err(e) => return fail(NativeEventGetUnavailableCode::InvalidRequest, None, &e),
        };
        let topology = match host_metadata.committed_topology(request.business_request_id) {
            Ok(t) => t,
            Err(e) => return fail(NativeEventGetUnavailableCode::MetadataUnavailable, None, &e),
        };
        let range = match topology.range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id) {
            Ok(r) => r.clone(),
            Err(e) => return fail(NativeEventGetUnavailableCode::InvalidRequest, None, &e),
        };
        let route = NativeEventRoute {
            generation: topology.generation,
            range_id: range.id,
            range_epoch: range.epoch,
            leader: range.leader.clone(),
        };
        if range.leader != self.local
            || range.id != request.range_id
            || range.epoch != request.range_epoch
        {
            return NativeEventGetReply::new(
                request,
                NativeEventGetResult::Redirect {
                    route,
                    metadata_context_id: host_metadata.context_id(),
                    metadata_term: host_metadata.term(),
                    metadata_read_index: host_metadata.read_index(),
                },
            );
        }
        let remaining = match token
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
        {
            Some(d) => d,
            None => {
                return fail(
                    NativeEventGetUnavailableCode::Canceled,
                    Some(route),
                    &"original Get budget expired",
                )
            }
        };
        // HostMQ is routing DATA ONLY. This independent fresh leader-only MQ
        // is mandatory before the existing consuming Strong read protocol.
        let first = match transport.acquire_metadata_read_observation(
            host_metadata.metadata_leader_id(),
            request.business_request_id,
            range.id,
            range.epoch,
            remaining.min(Duration::from_secs(5)),
            token,
        ) {
            Ok(m) => m,
            Err(e) => {
                return fail(
                    NativeEventGetUnavailableCode::MetadataUnavailable,
                    Some(route),
                    &e,
                )
            }
        };
        let outcome = coordinator.strong_point_read(
            &runtime,
            Some(host),
            transport,
            first,
            NativeStrongPointReadRequest {
                business_request_id: request.business_request_id,
                challenge: request.challenge,
                range_id: range.id,
                range_epoch: range.epoch,
                collection: crate::CLUSTER_EVENTS_COLLECTION.into(),
                id: id.clone(),
                budget_ms: u64::try_from(remaining.as_millis())
                    .unwrap_or(5000)
                    .clamp(1, 5000),
            },
            token,
        );
        match outcome {
            NativeStrongPointReadOutcome::Unavailable(e) => fail(
                NativeEventGetUnavailableCode::NativeRead { stage: e.stage },
                Some(route),
                &e.detail,
            ),
            NativeStrongPointReadOutcome::Observed(value) => {
                if let Err(e) = self
                    .require_generation(generation)
                    .and_then(|_| token.check())
                {
                    return fail(
                        NativeEventGetUnavailableCode::MissingWriter,
                        Some(route),
                        &e,
                    );
                }
                let audit = value.audit().clone();
                let record = value.into_owned_record();
                if record.as_ref().is_some_and(|row| row.id != id) {
                    return fail(
                        NativeEventGetUnavailableCode::InvalidRequest,
                        Some(route),
                        &"actual stored event record ID differs from canonical namespace/key",
                    );
                }
                let value = NativeEventValue::from(record.map(|r| r.metadata.clone()));
                let reply = NativeEventGetReply::new(
                    request,
                    NativeEventGetResult::Value {
                        value,
                        route: NativeEventRoute {
                            generation: audit.final_metadata_generation,
                            ..route
                        },
                        audit,
                    },
                );
                if let Err(e) = reply.validate_for(request) {
                    return fail(NativeEventGetUnavailableCode::InvalidRequest, None, &e);
                }
                reply
            }
        }
    }
    fn fresh_host_route(
        &self,
        runtime: &RangeRecoveryRuntime,
        transport: &TcpClusterRelocationTransport,
        business: Uuid,
        token: &CancellationToken,
    ) -> Result<HostMetadataReadObservation> {
        token.check()?;
        let hint = runtime.event_metadata_leader_hint(token)?;
        let topology = self
            .topology
            .upgrade()
            .ok_or_else(|| invalid("routing topology ended"))?;
        let mut members = topology
            .read()
            .metadata_voters()
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(hint) = hint {
            members.retain(|id| id != &hint);
            members.insert(0, hint);
        }
        let mut last = invalid("fresh HostMQ route unavailable");
        for member in members.into_iter().take(64) {
            token.check()?;
            let remaining = token
                .deadline()
                .and_then(|d| d.checked_duration_since(Instant::now()))
                .ok_or_else(|| invalid("original Get route budget expired"))?;
            match transport.acquire_host_metadata_observation(
                &member,
                business,
                remaining.min(Duration::from_millis(700)),
                token,
            ) {
                Ok(observed) => {
                    observed.require_live_request(business)?;
                    return Ok(observed);
                }
                Err(error) => last = error,
            }
        }
        Err(last)
    }
}
