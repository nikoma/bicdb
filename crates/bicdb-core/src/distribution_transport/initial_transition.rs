//! Test-only synchronization on actual listener/socket/read-return paths.
//! No Role, OWN, native owner, freeze, service or raw cancellation token is
//! exported. The hooks never fabricate a transport or producer result.
use super::*;
use parking_lot::Condvar;

#[derive(Default)]
struct State {
    before_drain: bool,
    drain_arrived: bool,
    drain_release: bool,
    accept_pause: bool,
    accept_arrived: bool,
    accept_release: bool,
    auth_peer: Option<ClusterNodeId>,
    auth_business: Option<Uuid>,
    auth_arrived: bool,
    auth_wait_active: bool,
    auth_deadline: Option<Instant>,
    auth_release: bool,
    auth_data: Option<serde_json::Value>,
    connect_peer: Option<ClusterNodeId>,
    connect_genesis: bool,
    connect_arrived: bool,
    connect_release: bool,
    connect_data: Option<serde_json::Value>,
    shutdown: bool,
    accept_ended: bool,
    reads: Vec<serde_json::Value>,
    reads_omitted: u64,
    actual_installs: u64,
    before_genesis: bool,
    genesis_arrived: bool,
    genesis_release: bool,
    pause_genesis_failure: bool,
    genesis_failure_arrived: bool,
    genesis_failure_release: bool,
    genesis_reads: Vec<serde_json::Value>,
    genesis_reads_omitted: u64,
}
#[derive(Default)]
pub(super) struct Hooks {
    enabled: AtomicBool,
    state: Mutex<State>,
    changed: Condvar,
}
pub(crate) struct InitialTransitionControl {
    actual: Arc<Hooks>,
    active: Option<Arc<AtomicUsize>>,
}
impl Drop for InitialTransitionControl {
    fn drop(&mut self) {
        self.release_all();
    }
}
impl InitialTransitionControl {
    pub(crate) fn pause_before_drain(&self) {
        let mut s = self.actual.state.lock();
        s.before_drain = true;
    }
    pub(crate) fn pause_accept(&self) {
        let mut s = self.actual.state.lock();
        s.accept_pause = true;
    }
    pub(crate) fn pause_authenticated_host_read(&self, peer: ClusterNodeId) {
        self.actual.state.lock().auth_peer = Some(peer);
    }
    pub(crate) fn pause_authenticated_host_read_request(
        &self,
        peer: ClusterNodeId,
        business: Uuid,
    ) {
        let mut s = self.actual.state.lock();
        s.auth_peer = Some(peer);
        s.auth_business = Some(business);
    }
    pub(crate) fn pause_connected_host_read(&self, peer: ClusterNodeId) {
        let mut s = self.actual.state.lock();
        s.connect_peer = Some(peer);
        s.connect_genesis = false;
    }
    pub(crate) fn pause_before_genesis_read(&self) {
        self.actual.state.lock().before_genesis = true;
    }
    pub(crate) fn release_genesis_read(&self) {
        self.actual.state.lock().genesis_release = true;
        self.actual.changed.notify_all();
    }
    pub(crate) fn pause_connected_genesis_read(&self, peer: ClusterNodeId) {
        let mut s = self.actual.state.lock();
        s.connect_peer = Some(peer);
        s.connect_genesis = true;
    }
    pub(crate) fn pause_after_genesis_socket_failure(&self) {
        self.actual.state.lock().pause_genesis_failure = true;
    }
    pub(crate) fn release_drain(&self) {
        self.actual.state.lock().drain_release = true;
        self.actual.changed.notify_all();
    }
    pub(crate) fn release_auth(&self) {
        self.actual.state.lock().auth_release = true;
        self.actual.changed.notify_all();
    }
    pub(crate) fn release_connect(&self) {
        self.actual.state.lock().connect_release = true;
        self.actual.changed.notify_all();
    }
    pub(crate) fn release_all(&self) {
        let mut s = self.actual.state.lock();
        s.drain_release = true;
        s.auth_release = true;
        s.connect_release = true;
        s.accept_release = true;
        s.genesis_release = true;
        s.genesis_failure_release = true;
        self.actual.changed.notify_all();
    }
    pub(crate) fn wait(&self, event: &str, parent: &CancellationToken) -> Result<()> {
        let mut s = self.actual.state.lock();
        loop {
            parent.check()?;
            let ready = match event {
                "drain" => s.drain_arrived,
                "accept" => s.accept_arrived,
                "auth" => s.auth_arrived,
                "connect" => s.connect_arrived,
                "shutdown" => s.shutdown,
                "accept_ended" => s.accept_ended,
                "read_return" => !s.reads.is_empty(),
                "read_success" => s.reads.iter().any(|r| r["returned_ok"] == true),
                "genesis" => s.genesis_arrived,
                "genesis_failure" => s.genesis_failure_arrived,
                "genesis_success" => s.genesis_reads.iter().any(|r| r["returned_ok"] == true),
                "idle" => self
                    .active
                    .as_ref()
                    .is_some_and(|v| v.load(Ordering::SeqCst) == 0),
                _ => return Err(transport_error("unknown initial transition test event")),
            };
            if ready {
                return Ok(());
            }
            self.actual
                .changed
                .wait_for(&mut s, Duration::from_millis(2));
        }
    }
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        let s = self.actual.state.lock();
        serde_json::json!({"before_drain":s.drain_arrived,"accept_paused":s.accept_arrived,"authenticated_read_paused":s.auth_arrived,
            "connected_before_TLS":s.connect_arrived,"actual_shutdown":s.shutdown,"actual_accept_ended":s.accept_ended,
            "actual_active_connections":self.active.as_ref().map(|v|v.load(Ordering::SeqCst)),
            "actual_authenticated_request":s.auth_data,"actual_connected_request":s.connect_data,
            "actual_read_returns":s.reads,"omitted":s.reads_omitted,"actual_retained_enrollment_installs":s.actual_installs,
            "actual_genesis_before_first_read":s.genesis_arrived,
            "actual_genesis_failure_paused":s.genesis_failure_arrived,
            "actual_genesis_read_returns":s.genesis_reads,"genesis_omitted":s.genesis_reads_omitted,
            "authority_returned":false})
    }
}
impl Hooks {
    pub(super) fn control(
        actual: &Arc<Self>,
        active: Option<Arc<AtomicUsize>>,
    ) -> InitialTransitionControl {
        actual.enabled.store(true, Ordering::Release);
        InitialTransitionControl {
            actual: actual.clone(),
            active,
        }
    }
    pub(super) fn control_snapshot(&self, active: &AtomicUsize) -> serde_json::Value {
        let s = self.state.lock();
        serde_json::json!({"actual_authenticated_request":s.auth_data,
            "actual_authenticated_handler_paused":s.auth_wait_active && !s.auth_release,
            "actual_authenticated_handler_remaining_us":s.auth_deadline.map(|d|d.saturating_duration_since(Instant::now()).as_micros()),
            "actual_active_connections":active.load(Ordering::SeqCst),
            "actual_shutdown":s.shutdown,"actual_accept_ended":s.accept_ended,
            "authority_returned":false})
    }
    pub(super) fn before_accept(&self, shutdown: &AtomicBool) {
        if !self.enabled.load(Ordering::Acquire) {
            return;
        }
        let mut s = self.state.lock();
        if !s.accept_pause {
            return;
        }
        s.accept_arrived = true;
        self.changed.notify_all();
        while !s.accept_release && !shutdown.load(Ordering::SeqCst) {
            self.changed.wait_for(&mut s, Duration::from_millis(2));
        }
    }
    pub(super) fn before_drain(&self, parent: &CancellationToken) -> Result<()> {
        if !self.enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut s = self.state.lock();
        if !s.before_drain {
            return Ok(());
        }
        s.drain_arrived = true;
        self.changed.notify_all();
        while !s.drain_release {
            parent.check()?;
            self.changed.wait_for(&mut s, Duration::from_millis(2));
        }
        parent.check()
    }
    pub(super) fn authenticated_host_read(
        &self,
        peer: &ClusterNodeId,
        request: &ClusterDataRequest,
        deadline: Instant,
    ) -> Result<()> {
        if !self.enabled.load(Ordering::Acquire)
            || !matches!(request, ClusterDataRequest::HostMetadataReadAcquire { .. })
        {
            return Ok(());
        }
        let mut s = self.state.lock();
        if s.auth_peer.as_ref() != Some(peer) || s.auth_arrived {
            return Ok(());
        }
        if s.auth_business.is_some_and(|expected| !matches!(request,
            ClusterDataRequest::HostMetadataReadAcquire { request } if request.read_request_id==expected)) {
            return Ok(());
        }
        s.auth_arrived = true;
        s.auth_wait_active = true;
        s.auth_deadline = Some(deadline);
        if let ClusterDataRequest::HostMetadataReadAcquire { request } = request {
            s.auth_data = Some(
                serde_json::json!({"actual_acquisition":request.acquisition_id,"actual_business":request.read_request_id,
                "actual_requester":request.requester_node_id,"actual_requester_incarnation":request.requester_incarnation,
                "actual_server_remaining_us":deadline.saturating_duration_since(Instant::now()).as_micros(),"TLS_authenticated":true}),
            );
        }
        self.changed.notify_all();
        while !s.auth_release && Instant::now() < deadline {
            self.changed.wait_for(&mut s, Duration::from_millis(2));
        }
        s.auth_wait_active = false;
        // The real provider retains the SAME original request_deadline. An
        // expired request receives its normal real refusal; no renewed token.
        Ok(())
    }
    pub(super) fn connected_host_read(
        &self,
        peer: &ClusterNodeId,
        request: &ClusterDataRequest,
        parent: &CancellationToken,
        deadline: Instant,
    ) -> Result<()> {
        if !self.enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        let genesis = matches!(request, ClusterDataRequest::MetadataReadAcquire { .. });
        if !genesis && !matches!(request, ClusterDataRequest::HostMetadataReadAcquire { .. }) {
            return Ok(());
        }
        let mut s = self.state.lock();
        if s.connect_peer.as_ref() != Some(peer)
            || s.connect_genesis != genesis
            || s.connect_arrived
        {
            return Ok(());
        }
        s.connect_arrived = true;
        if let ClusterDataRequest::HostMetadataReadAcquire { request } = request {
            s.connect_data = Some(
                serde_json::json!({"actual_acquisition":request.acquisition_id,"actual_business":request.read_request_id,
                "actual_requester":request.requester_node_id,"actual_requester_incarnation":request.requester_incarnation,
                "original_exchange_remaining_us":deadline.saturating_duration_since(Instant::now()).as_micros(),"peer_TLS_not_yet_authenticated":true}),
            );
        }
        if let ClusterDataRequest::MetadataReadAcquire { request } = request {
            s.connect_data = Some(
                serde_json::json!({"actual_request_kind":"MetadataReadAcquire",
                "actual_acquisition":request.acquisition_id,"actual_business":request.read_request_id,
                "actual_requester":request.requester_node_id,"actual_requester_incarnation":request.requester_incarnation,
                "actual_range":request.range_id,"actual_epoch":request.range_epoch,
                "original_exchange_remaining_us":deadline.saturating_duration_since(Instant::now()).as_micros(),"peer_TLS_not_yet_authenticated":true}),
            );
        }
        self.changed.notify_all();
        while !s.connect_release {
            parent.check()?;
            if Instant::now() >= deadline {
                return Err(BicDbError::QueryTimedOut);
            }
            self.changed.wait_for(&mut s, Duration::from_millis(2));
        }
        parent.check()
    }
    pub(super) fn shutdown(&self) {
        if self.enabled.load(Ordering::Acquire) {
            self.state.lock().shutdown = true;
            self.changed.notify_all();
        }
    }
    pub(super) fn accept_ended(&self) {
        if self.enabled.load(Ordering::Acquire) {
            self.state.lock().accept_ended = true;
            self.changed.notify_all();
        }
    }
    pub(super) fn installed(&self) {
        if self.enabled.load(Ordering::Acquire) {
            let mut s = self.state.lock();
            s.actual_installs = s.actual_installs.saturating_add(1);
        }
    }
    pub(super) fn read_return(
        &self,
        actual: &Result<HostMetadataReadObservation>,
        parent: &CancellationToken,
    ) {
        if !self.enabled.load(Ordering::Acquire) {
            return;
        }
        let now = Instant::now();
        let failure = actual
            .as_ref()
            .err()
            .and_then(HotRecoveryPeerFailure::from_error);
        // Error text is unnecessary here. This reports actual local socket
        // provenance, original-parent clocks and successful observation DATA.
        let value = serde_json::json!({"returned_ok":actual.is_ok(),"typed_socket_failure":failure.is_some(),
            "actual_socket_kind":failure.map(|f|format!("{:?}",f.kind())),
            "may_have_dispatched":failure.map(|f|f.may_have_dispatched()),
            "original_parent_remaining_us":parent.deadline().map(|d|d.saturating_duration_since(now).as_micros()),
            "original_parent_cancelled":parent.is_cancel_requested(),
            "actual_success_context":actual.as_ref().ok().map(|o|o.context_id()),
            "actual_success_deadline_remaining_us":actual.as_ref().ok().map(|o|o.deadline().saturating_duration_since(now).as_micros())});
        let mut s = self.state.lock();
        if s.reads.len() < 32 {
            s.reads.push(value);
        } else {
            s.reads_omitted = s.reads_omitted.saturating_add(1);
        }
        self.changed.notify_all();
    }
    fn before_genesis_read(&self, parent: &CancellationToken) -> Result<()> {
        if !self.enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut s = self.state.lock();
        if !s.before_genesis {
            return Ok(());
        }
        s.genesis_arrived = true;
        self.changed.notify_all();
        while !s.genesis_release {
            parent.check()?;
            self.changed.wait_for(&mut s, Duration::from_millis(2));
        }
        parent.check()
    }
    fn genesis_read_return(
        &self,
        actual: &Result<MetadataReadObservation>,
        business: Uuid,
        range: &RangeDescriptor,
        parent: &CancellationToken,
    ) -> Result<()> {
        if !self.enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        let failure = actual
            .as_ref()
            .err()
            .and_then(HotRecoveryPeerFailure::from_error);
        let now = Instant::now();
        let value = serde_json::json!({"actual_request_kind":"MetadataReadAcquire",
            "actual_business":business,"actual_range":range.id,"actual_epoch":range.epoch,
            "returned_ok":actual.is_ok(),"typed_socket_failure":failure.is_some(),
            "actual_socket_kind":failure.map(|f|format!("{:?}",f.kind())),
            "actual_original_socket_error":failure.map(ToString::to_string),
            "may_have_dispatched":failure.map(|f|f.may_have_dispatched()),
            "actual_error_discriminant":actual.as_ref().err().map(|e|format!("{:?}",std::mem::discriminant(e))),
            "original_parent_remaining_us":parent.deadline().map(|d|d.saturating_duration_since(now).as_micros()),
            "original_parent_cancelled":parent.is_cancel_requested(),
            "original_parent_deadline":format!("{:?}",parent.deadline()),
            "actual_success_context":actual.as_ref().ok().map(|o|o.context_id()),
            "actual_success_acquisition":actual.as_ref().ok().map(|o|o.request().acquisition_id),
            "actual_success_deadline_remaining_us":actual.as_ref().ok().map(|o|o.deadline().saturating_duration_since(now).as_micros())});
        let mut s = self.state.lock();
        if s.genesis_reads.len() < 32 {
            s.genesis_reads.push(value);
        } else {
            s.genesis_reads_omitted = s.genesis_reads_omitted.saturating_add(1);
        }
        let retryable = failure.is_some_and(|failure| {
            matches!(
                failure.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
            )
        });
        if retryable && s.pause_genesis_failure && !s.genesis_failure_arrived {
            s.genesis_failure_arrived = true;
            self.changed.notify_all();
            while !s.genesis_failure_release {
                parent.check()?;
                self.changed.wait_for(&mut s, Duration::from_millis(2));
            }
            parent.check()?;
        }
        self.changed.notify_all();
        Ok(())
    }
}
impl ClusterDataServerHandle {
    pub(crate) fn test_initial_transition_control(&self) -> InitialTransitionControl {
        Hooks::control(&self.initial_transition, Some(self.active.clone()))
    }
    pub(crate) fn test_before_initial_data_drain(&self, parent: &CancellationToken) -> Result<()> {
        self.initial_transition.before_drain(parent)
    }
}
impl TcpClusterRelocationTransport {
    pub(crate) fn test_before_initial_genesis_read(
        &self,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.initial_transition.before_genesis_read(parent)
    }
    pub(crate) fn test_initial_genesis_read_return(
        &self,
        actual: &Result<MetadataReadObservation>,
        business: Uuid,
        range: &RangeDescriptor,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.initial_transition
            .genesis_read_return(actual, business, range, parent)
    }
    pub(crate) fn test_initial_transition_control(&self) -> InitialTransitionControl {
        Hooks::control(&self.initial_transition, None)
    }
    pub(crate) fn test_initial_enrollment_installed(&self) {
        self.initial_transition.installed();
    }
    pub(crate) fn test_initial_read_return(
        &self,
        actual: &Result<HostMetadataReadObservation>,
        parent: &CancellationToken,
    ) {
        self.initial_transition.read_return(actual, parent)
    }
}
