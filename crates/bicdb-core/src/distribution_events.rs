//! Range-local, idempotent application events over the member-authenticated
//! data plane. Events are immutable; reads are bounded and eventually consistent.
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{distribution_key_token, BicDb, BicDbError, RangeDescriptor, RangeId, Record, Result};

pub const CLUSTER_EVENTS_COLLECTION: &str = "__bic_cluster_events";
pub const MAX_CLUSTER_EVENT_BYTES: usize = 64 * 1024;
pub const MAX_CLUSTER_EVENT_PAGE_ROWS: usize = 128;

fn invalid(message: &str) -> BicDbError {
    BicDbError::Cluster(message.to_owned())
}

fn component(value: &str, max: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err(invalid(
            "event names must use bounded ASCII letters, digits, '-', '_' or '.'",
        ));
    }
    Ok(())
}

pub fn cluster_event_record_id(namespace: &str, key: &str) -> Result<String> {
    component(namespace, 128)?;
    component(key, 256)?;
    Ok(format!("{namespace}/{key}"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClusterEventOperation {
    Append {
        namespace: String,
        key: String,
        value: Value,
    },
    Get {
        namespace: String,
        key: String,
    },
    /// The cursor is the last examined key, including records filtered out by
    /// range. Restart from None to see insertions before an earlier cursor.
    Scan {
        namespace: String,
        after: Option<String>,
        limit: usize,
    },
}

impl ClusterEventOperation {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Append {
                namespace,
                key,
                value,
            } => {
                cluster_event_record_id(namespace, key)?;
                if serde_json::to_vec(value)?.len() > MAX_CLUSTER_EVENT_BYTES {
                    return Err(invalid("event value exceeds 64 KiB"));
                }
            }
            Self::Get { namespace, key } => {
                cluster_event_record_id(namespace, key)?;
            }
            Self::Scan {
                namespace,
                after,
                limit,
            } => {
                component(namespace, 128)?;
                if let Some(after) = after {
                    component(after, 256)?;
                }
                if *limit == 0 || *limit > MAX_CLUSTER_EVENT_PAGE_ROWS {
                    return Err(invalid("event page limit must be between 1 and 128"));
                }
            }
        }
        Ok(())
    }

    pub fn point_record_id(&self) -> Result<Option<String>> {
        self.validate()?;
        match self {
            Self::Append { namespace, key, .. } | Self::Get { namespace, key } => {
                Ok(Some(cluster_event_record_id(namespace, key)?))
            }
            Self::Scan { .. } => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterEventRequest {
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub operation: ClusterEventOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClusterEvent {
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ClusterEventResponse {
    Appended {
        inserted: bool,
    },
    Value {
        value: Option<Value>,
    },
    Page {
        events: Vec<ClusterEvent>,
        next: Option<String>,
    },
}

/// Cursor for an eventually consistent walk over the current range set.
/// Membership changes preserve range IDs. A fresh walk discovers events added
/// before an earlier cursor; consumers deduplicate by their stable event keys.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterEventCursor {
    pub range_id: RangeId,
    pub after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClusterEventPage {
    pub events: Vec<ClusterEvent>,
    pub next: Option<ClusterEventCursor>,
}

#[cfg(feature = "tls-replication")]
mod client {
    use super::*;
    use crate::{
        ClusterNodeId, ClusterTopology, DistributionConfig, TcpClusterRelocationTransport,
    };
    use parking_lot::RwLock;
    use std::sync::Arc;

    /// What one bounded discovery round established. Only `Unanswered` says
    /// anything about the members: `Interrupted` means the caller's own
    /// deadline ended the round before every member had been asked, which is
    /// a statement about the remaining budget and never about reachability.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Discovery {
        Observed,
        Unanswered,
        Interrupted,
    }

    /// Outcome of waiting on the attempts of one request that are in flight.
    enum Waited {
        Answered(ClusterEventResponse),
        /// The attempt the caller is waiting for ended with an error.
        Failed,
        /// The wait ended first; the attempt is still in flight.
        Pending,
    }

    /// Attempts of ONE request that were dispatched and have not reported.
    /// Reaching the end of an attempt's slice of the budget only lets the
    /// request go on to discovery and the next attempt; it does not hang up
    /// on a member that accepted the request and is still executing it.
    struct InFlight {
        replies: std::sync::mpsc::Receiver<(usize, Result<ClusterEventResponse>)>,
        attempts: Vec<(usize, crate::CancellationToken)>,
    }

    impl InFlight {
        /// Wait until `until` for an answer from any attempt, or for the
        /// error of attempt `awaited`. Errors of other attempts are recorded
        /// and the wait continues.
        fn wait(
            &mut self,
            awaited: Option<usize>,
            until: std::time::Instant,
            last_error: &mut BicDbError,
        ) -> Waited {
            loop {
                if self.attempts.is_empty() {
                    return Waited::Pending;
                }
                let remaining = until.saturating_duration_since(std::time::Instant::now());
                let (attempt, reply) = match self.replies.recv_timeout(remaining) {
                    Ok(reply) => reply,
                    Err(_) => return Waited::Pending,
                };
                self.attempts.retain(|(id, _)| *id != attempt);
                match reply {
                    Ok(response) => return Waited::Answered(response),
                    Err(error) => {
                        *last_error = error;
                        if awaited == Some(attempt) {
                            return Waited::Failed;
                        }
                    }
                }
            }
        }

        /// Stop listening. One attempt at a time, so two ending exchanges
        /// never record their diagnostics concurrently. Every attempt thread
        /// reports exactly once; a cancelled socket exchange ends within its
        /// polling interval and a connect within its own slice.
        fn hang_up(&mut self) {
            while let Some((_, token)) = self.attempts.first() {
                token.cancel();
                match self.replies.recv() {
                    Ok((attempt, _)) => self.attempts.retain(|(id, _)| *id != attempt),
                    Err(_) => break,
                }
            }
        }
    }

    /// Trusted host-side client. Membership certificates stay with this host;
    /// never provide them, this client, or arbitrary namespaces to guest code.
    pub struct ClusterEventClient {
        topology: Arc<RwLock<ClusterTopology>>,
        transport: TcpClusterRelocationTransport,
        request_timeout: std::time::Duration,
        // Discovery ordering hint only: members whose discovery exchange
        // recently consumed its whole slice without an answer. They are asked
        // last and only while no other member answered in the same refresh.
        silent_members:
            parking_lot::Mutex<std::collections::BTreeMap<ClusterNodeId, std::time::Instant>>,
    }

    impl ClusterEventClient {
        pub fn new(config: &DistributionConfig, topology: ClusterTopology) -> Result<Self> {
            let topology = Arc::new(RwLock::new(topology));
            let transport = TcpClusterRelocationTransport::new(
                config.cluster_id.clone(),
                config.node_id.clone(),
                Arc::clone(&topology),
                config.transport.clone(),
            )?;
            Ok(Self {
                topology,
                transport,
                request_timeout: std::time::Duration::from_secs(30),
                silent_members: parking_lot::Mutex::new(std::collections::BTreeMap::new()),
            })
        }

        /// Bound route discovery, retries and their TCP/TLS calls with one
        /// absolute deadline. Hostname resolution remains outside the network
        /// deadline; numeric member addresses avoid that resolver limitation.
        pub fn with_request_timeout(mut self, timeout: std::time::Duration) -> Result<Self> {
            if timeout.is_zero() || timeout > std::time::Duration::from_secs(300) {
                return Err(invalid(
                    "event request timeout must be positive and at most 300 seconds",
                ));
            }
            self.request_timeout = timeout;
            Ok(self)
        }

        /// Journal exact actual Event/discovery RPC evidence through the
        /// existing transport. This opt-in may refuse on evidence failure; it
        /// cannot change replies, routing, retries or admission authority.
        pub fn with_rpc_evidence_observer(
            mut self,
            observer: Arc<dyn crate::ClusterRpcEvidenceObserver>,
            limits: crate::ClusterRpcEvidenceLimits,
        ) -> Result<Self> {
            self.transport = self
                .transport
                .with_rpc_evidence_observer(observer, limits)?;
            Ok(self)
        }

        /// Discover a newer committed topology from known members. Equal
        /// generations with conflicting contents are rejected by the transport.
        pub fn refresh(&self) -> Result<()> {
            self.refresh_until(std::time::Instant::now() + self.request_timeout)
        }

        fn refresh_until(&self, deadline: std::time::Instant) -> Result<()> {
            match self.refresh_members_until(deadline)? {
                Discovery::Observed => Ok(()),
                Discovery::Unanswered => Err(invalid("no event cluster member is reachable")),
                Discovery::Interrupted => Err(invalid(
                    "event cluster discovery did not finish within the request budget",
                )),
            }
        }

        fn refresh_members_until(&self, deadline: std::time::Instant) -> Result<Discovery> {
            self.transport.require_rpc_evidence_current()?;
            let cancellation =
                crate::CancellationToken::uncancelable().child_with_deadline(deadline);
            let mut members: Vec<ClusterNodeId> =
                self.topology.read().nodes.keys().cloned().collect();
            // A member that recently stayed silent for its whole discovery
            // slice is asked LAST, and not at all once another member answered
            // in this refresh: one silent member must not take a second of
            // every retry away from the route that is actually serving. It is
            // still asked whenever nobody else answers, and again after the
            // bounded hint expires.
            let silent = {
                let now = std::time::Instant::now();
                let mut silent = self.silent_members.lock();
                silent.retain(|_, until| now < *until);
                silent
                    .keys()
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>()
            };
            members.sort_by_key(|member| silent.contains(member));
            let mut observed = false;
            // True only while every member asked so far had its whole
            // exchange inside the caller's deadline.
            let mut complete = true;
            for member in members {
                if std::time::Instant::now() >= deadline {
                    complete = false;
                    break;
                }
                if observed && silent.contains(&member) {
                    continue;
                }
                let member_started = std::time::Instant::now();
                // Committed snapshots are available from followers too. A
                // stale client may know only the former leader, so requiring
                // leader authority here can prevent learning its replacement.
                // One silent member must leave time to ask the others. Both
                // discovery forms share this member's slice of the original
                // request deadline, including the legacy fallback.
                let member_cancellation = cancellation.child_with_deadline(
                    deadline.min(std::time::Instant::now() + std::time::Duration::from_secs(1)),
                );
                let topology = match self
                    .transport
                    .fetch_metadata_snapshot_cancellable(&member_cancellation, &member)
                {
                    Ok(snapshot) => {
                        snapshot.validate()?;
                        Some(snapshot.topology)
                    }
                    // Legacy non-consensus controllers expose only this read.
                    Err(_) if std::time::Instant::now() < deadline => self
                        .transport
                        .fetch_topology_cancellable(&member_cancellation, &member)
                        .ok(),
                    Err(_) => None,
                };
                if let Some(topology) = topology {
                    self.transport.install_topology(topology)?;
                    observed = true;
                    self.silent_members.lock().remove(&member);
                } else {
                    if std::time::Instant::now() >= deadline {
                        // This member's exchange was cut short by the
                        // caller's deadline: that does not show the member
                        // to be unreachable.
                        complete = false;
                    }
                    if member_started.elapsed() >= std::time::Duration::from_millis(500) {
                        self.silent_members.lock().insert(
                            member.clone(),
                            std::time::Instant::now() + std::time::Duration::from_secs(10),
                        );
                    }
                }
            }
            self.transport.require_rpc_evidence_current()?;
            Ok(if observed {
                Discovery::Observed
            } else if complete {
                Discovery::Unanswered
            } else {
                Discovery::Interrupted
            })
        }

        fn request(
            &self,
            operation: ClusterEventOperation,
            scan_range: Option<RangeId>,
        ) -> Result<ClusterEventResponse> {
            operation.validate()?;
            if let ClusterEventOperation::Get { namespace, key } = &operation {
                return self.get_request(
                    namespace,
                    key,
                    crate::distribution_native_read::NativeEventReadMode::DeploymentDefault,
                );
            }
            let deadline = std::time::Instant::now() + self.request_timeout;
            let cancellation =
                crate::CancellationToken::uncancelable().child_with_deadline(deadline);
            std::thread::scope(|scope| {
                let (reports, replies) = std::sync::mpsc::channel();
                let mut in_flight = InFlight {
                    replies,
                    attempts: Vec::new(),
                };
                let result = self.request_attempts(
                    scope,
                    &cancellation,
                    deadline,
                    &operation,
                    scan_range,
                    &reports,
                    &mut in_flight,
                );
                // Every RPC of this request ends before the request returns.
                drop(reports);
                in_flight.hang_up();
                result
            })
        }

        #[allow(clippy::too_many_arguments)]
        fn request_attempts<'scope, 'env>(
            &'env self,
            scope: &'scope std::thread::Scope<'scope, 'env>,
            cancellation: &crate::CancellationToken,
            deadline: std::time::Instant,
            operation: &ClusterEventOperation,
            scan_range: Option<RangeId>,
            reports: &std::sync::mpsc::Sender<(usize, Result<ClusterEventResponse>)>,
            in_flight: &mut InFlight,
        ) -> Result<ClusterEventResponse> {
            let mut last_error = invalid("event cluster request failed");
            for attempt in 0..5 {
                self.transport.require_rpc_evidence_current()?;
                if std::time::Instant::now() >= deadline {
                    break;
                }
                if attempt > 0 {
                    let now = std::time::Instant::now();
                    let pause = std::time::Duration::from_millis(100 << (attempt - 1))
                        .min(deadline.saturating_duration_since(now));
                    // An earlier attempt that is still executing may answer
                    // during the pause; a pause with nothing in flight is the
                    // plain backoff.
                    if let Waited::Answered(response) =
                        in_flight.wait(None, now + pause, &mut last_error)
                    {
                        return Ok(response);
                    }
                    std::thread::sleep(
                        (now + pause).saturating_duration_since(std::time::Instant::now()),
                    );
                    // A discovery outage during an election consumes a retry,
                    // rather than aborting the remaining request budget. Invalid
                    // or conflicting topologies still propagate through `?`.
                    let discovery = self.refresh_members_until(deadline)?;
                    if let Waited::Answered(response) =
                        in_flight.wait(None, std::time::Instant::now(), &mut last_error)
                    {
                        return Ok(response);
                    }
                    match discovery {
                        Discovery::Observed => {}
                        Discovery::Unanswered => {
                            last_error = invalid("no event cluster member is reachable");
                            continue;
                        }
                        // The budget ended the round. Keep the error of the
                        // last actual attempt: no member was shown to be
                        // unreachable.
                        Discovery::Interrupted => break,
                    }
                }
                if std::time::Instant::now() >= deadline {
                    break;
                }
                let (destination, request) = {
                    let topology = self.topology.read();
                    let range = if let Some(id) = scan_range {
                        topology
                            .range_by_id(id)
                            .ok_or_else(|| invalid("unknown event cursor range"))?
                    } else {
                        let id = operation
                            .point_record_id()?
                            .ok_or_else(|| invalid("missing event point key"))?;
                        topology.range_for_key(CLUSTER_EVENTS_COLLECTION, &id)?
                    };
                    (
                        range.leader.clone(),
                        ClusterEventRequest {
                            range_id: range.id,
                            range_epoch: range.epoch,
                            operation: operation.clone(),
                        },
                    )
                };
                // Reserve part of the remaining budget for route discovery
                // after a silent old leader. Otherwise every new request can
                // exhaust its whole budget on the same stale route forever.
                // The slice bounds how long this request WAITS on the route
                // before asking the other members, and how long the attempt
                // may spend connecting. It does not hang up on a member that
                // accepted the request: a leader that is alive and still
                // committing answers this same attempt, instead of having
                // its work abandoned and repeated by a retry that then has
                // even less of the budget.
                let now = std::time::Instant::now();
                let slice_end = now + deadline.saturating_duration_since(now) / 2;
                let token = cancellation.child();
                let exchange = {
                    let token = token.clone();
                    let reports = reports.clone();
                    move || {
                        let reply = self.transport.application_event_listening(
                            &token,
                            slice_end,
                            &destination,
                            request,
                        );
                        let _ = reports.send((attempt, reply));
                    }
                };
                match std::thread::Builder::new()
                    .name("event-request-attempt".into())
                    .spawn_scoped(scope, exchange)
                {
                    Ok(_) => in_flight.attempts.push((attempt, token)),
                    Err(error) => {
                        last_error = error.into();
                        continue;
                    }
                }
                if let Waited::Answered(response) =
                    in_flight.wait(Some(attempt), slice_end, &mut last_error)
                {
                    return Ok(response);
                }
            }
            // The attempts are spent or the budget is nearly so. Whatever is
            // still in flight may yet answer inside the caller's deadline.
            if let Waited::Answered(response) = in_flight.wait(None, deadline, &mut last_error) {
                return Ok(response);
            }
            self.transport.require_rpc_evidence_current()?;
            Err(last_error)
        }

        fn get_request(
            &self,
            namespace: &str,
            key: &str,
            mode: crate::distribution_native_read::NativeEventReadMode,
        ) -> Result<ClusterEventResponse> {
            use crate::distribution_native_read::{
                NativeEventGetRequest, NativeEventGetResult, NativeEventRoute,
            };
            let id = cluster_event_record_id(namespace, key)?;
            let deadline = std::time::Instant::now() + self.request_timeout;
            let parent = crate::CancellationToken::uncancelable().child_with_deadline(deadline);
            let business = uuid::Uuid::now_v7();
            let mut hint: Option<NativeEventRoute> = None;
            let mut retry_after = std::time::Duration::from_millis(50);
            let mut last = invalid("typed event Get unavailable");
            // A finite bound plus the SAME absolute request deadline. Typed
            // route/unavailable results never become a successful empty row.
            for attempt in 0..128 {
                self.transport.require_rpc_evidence_current()?;
                parent.check()?;
                if attempt > 0 {
                    let until = (std::time::Instant::now() + retry_after).min(deadline);
                    while std::time::Instant::now() < until {
                        parent.check()?;
                        std::thread::sleep(
                            until
                                .saturating_duration_since(std::time::Instant::now())
                                .min(std::time::Duration::from_millis(5)),
                        );
                    }
                    match self.refresh_members_until(deadline)? {
                        Discovery::Observed => {}
                        Discovery::Unanswered => {
                            last = invalid("no member reached during typed Get discovery");
                            continue;
                        }
                        // The budget ended the round; keep the last actual
                        // typed result instead of reporting unreachability.
                        Discovery::Interrupted => continue,
                    }
                }
                parent.check()?;
                let (destination, range_id, range_epoch) = {
                    let topology = self.topology.read();
                    let current = topology.range_for_key(CLUSTER_EVENTS_COLLECTION, &id)?;
                    match hint.as_ref().filter(|h| {
                        h.generation >= topology.generation
                            && topology.nodes.contains_key(&h.leader)
                    }) {
                        Some(h) => (h.leader.clone(), h.range_id, h.range_epoch),
                        None => (current.leader.clone(), current.id, current.epoch),
                    }
                };
                let now = std::time::Instant::now();
                let attempt_end = (now + deadline.saturating_duration_since(now) / 2)
                    .min(now + std::time::Duration::from_secs(5));
                let cancellation = parent.child_with_deadline(attempt_end);
                let request = NativeEventGetRequest {
                    version: 1,
                    mode,
                    business_request_id: business,
                    challenge: uuid::Uuid::now_v7(),
                    range_id,
                    range_epoch,
                    namespace: namespace.into(),
                    key: key.into(),
                    budget_ms: u64::try_from(
                        attempt_end.saturating_duration_since(now).as_millis(),
                    )
                    .unwrap_or(5000)
                    .clamp(1, 5000),
                };
                match self.transport.native_event_get_cancellable(
                    &cancellation,
                    &destination,
                    &request,
                ) {
                    Ok(reply) => {
                        match reply.result {
                            NativeEventGetResult::Value { value, .. }
                            | NativeEventGetResult::LegacyValue { value } => {
                                parent.check()?;
                                return Ok(ClusterEventResponse::Value {
                                    value: value.into_option(),
                                });
                            }
                            NativeEventGetResult::Redirect { route, .. } => {
                                hint = Some(route);
                                retry_after = std::time::Duration::from_millis(25);
                                last=invalid("typed event Get redirected; original request deadline retained");
                            }
                            NativeEventGetResult::Unavailable {
                                code,
                                route,
                                retry_after_ms,
                                detail,
                            } => {
                                if matches!(code,crate::distribution_native_read::NativeEventGetUnavailableCode::LegacyUnsupported) {
                                    return Err(invalid("explicit Strong Get is unsupported by this unenforced deployment"));
                                }
                                if route.is_some() {
                                    hint = route;
                                }
                                let backoff = 25u64.saturating_mul(1u64 << attempt.min(4));
                                retry_after = std::time::Duration::from_millis(
                                    retry_after_ms.max(backoff).clamp(5, 250),
                                );
                                last = invalid(&format!(
                                    "typed event Get unavailable ({code:?}): {detail}"
                                ));
                            }
                        }
                    }
                    Err(error) => last = error,
                }
            }
            Err(last)
        }

        /// Return true for a newly committed event, false for an identical
        /// retry. Reusing a key with different content is an error. A transport
        /// failure may have an unknown outcome; retry with the exact same key.
        pub fn append(&self, namespace: &str, key: &str, value: Value) -> Result<bool> {
            match self.request(
                ClusterEventOperation::Append {
                    namespace: namespace.into(),
                    key: key.into(),
                    value,
                },
                None,
            )? {
                ClusterEventResponse::Appended { inserted } => Ok(inserted),
                _ => Err(invalid("unexpected event append response")),
            }
        }

        /// Born-v2 enrolled receivers return only actual Strong results;
        /// born-v1 receivers retain their explicitly eventual behavior. Scan
        /// remains eventual. Unsupported protocol peers refuse typed requests.
        pub fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>> {
            match self.request(
                ClusterEventOperation::Get {
                    namespace: namespace.into(),
                    key: key.into(),
                },
                None,
            )? {
                ClusterEventResponse::Value { value } => Ok(value),
                _ => Err(invalid("unexpected event get response")),
            }
        }

        /// Require the enrolled born-v2 protocol. Legacy values and absences
        /// are rejected explicitly; unsupported peers are never downgraded.
        pub fn get_strong(&self, namespace: &str, key: &str) -> Result<Option<Value>> {
            match self.get_request(
                namespace,
                key,
                crate::distribution_native_read::NativeEventReadMode::Strong,
            )? {
                ClusterEventResponse::Value { value } => Ok(value),
                _ => Err(invalid("unexpected typed Strong Get result")),
            }
        }

        /// Read at most one bounded range page. An empty page may still have
        /// a continuation. There is no global ordering or cross-range snapshot.
        pub fn scan(
            &self,
            namespace: &str,
            cursor: Option<ClusterEventCursor>,
            limit: usize,
        ) -> Result<ClusterEventPage> {
            let range_ids: Vec<RangeId> = self
                .topology
                .read()
                .ranges
                .values()
                .map(|range| range.id)
                .collect();
            let cursor = cursor.unwrap_or(ClusterEventCursor {
                range_id: *range_ids
                    .first()
                    .ok_or_else(|| invalid("event cluster has no ranges"))?,
                after: None,
            });
            let index = range_ids
                .iter()
                .position(|id| *id == cursor.range_id)
                .ok_or_else(|| invalid("unknown event cursor range"))?;
            match self.request(
                ClusterEventOperation::Scan {
                    namespace: namespace.into(),
                    after: cursor.after,
                    limit,
                },
                Some(cursor.range_id),
            )? {
                ClusterEventResponse::Page { events, next } => {
                    let next = next
                        .map(|key| ClusterEventCursor {
                            range_id: cursor.range_id,
                            after: Some(key),
                        })
                        .or_else(|| {
                            range_ids.get(index + 1).map(|id| ClusterEventCursor {
                                range_id: *id,
                                after: None,
                            })
                        });
                    Ok(ClusterEventPage { events, next })
                }
                _ => Err(invalid("unexpected event scan response")),
            }
        }
    }
}

#[cfg(feature = "tls-replication")]
pub use client::ClusterEventClient;

pub(crate) fn execute_cluster_event(
    db: &BicDb,
    range: &RangeDescriptor,
    operation: ClusterEventOperation,
) -> Result<ClusterEventResponse> {
    operation.validate()?;
    if !db.is_server_paged() {
        return Err(invalid("cluster events require bounded paged storage"));
    }
    if let Some(id) = operation.point_record_id()? {
        if !range.contains_token(distribution_key_token(CLUSTER_EVENTS_COLLECTION, &id)) {
            return Err(invalid("event key is outside the requested range"));
        }
    }
    match operation {
        ClusterEventOperation::Append {
            namespace,
            key,
            value,
        } => {
            // Commit admission is mandatory: the engine verifies the current
            // leader, recovered epoch and schema and replicates to a quorum.
            // Do not turn this RPC into a bypass for standalone mutations.
            if !db.commit_admission_ready() {
                return Err(invalid("event write admission is not ready"));
            }
            let id = cluster_event_record_id(&namespace, &key)?;
            // A matching local row is not an acknowledged cluster decision:
            // an earlier call may have applied locally but failed certificate
            // quorum. Re-admit an identical mutation before acknowledging a
            // retry, including after process reopen. Reserve the exact current
            // row/version instead of inventing a global snapshot floor; a
            // contiguous snapshot may lag this range's already-applied row.
            if db.get(CLUSTER_EVENTS_COLLECTION, &id)?.is_some() {
                let mut tx = db.begin_transaction()?;
                tx.set_snapshot_isolation(false, 0)?;
                let reserved =
                    tx.read_committed_update_record_nowait(CLUSTER_EVENTS_COLLECTION, &id)?;
                let (previous, statement_snapshot) = match reserved {
                    Some((Some(previous), sequence)) => (previous, sequence),
                    Some((None, _)) => return Err(invalid("event changed while reserving retry")),
                    None => (
                        tx.get(CLUSTER_EVENTS_COLLECTION, &id)?
                            .ok_or_else(|| invalid("event changed while reserving retry"))?
                            .as_ref()
                            .clone(),
                        0,
                    ),
                };
                if previous.metadata != value {
                    return Err(invalid("event key already exists with a different value"));
                }
                tx.write_upserts_with_statement_snapshots(
                    CLUSTER_EVENTS_COLLECTION,
                    [(previous, statement_snapshot)],
                )?;
                tx.commit()?;
                return Ok(ClusterEventResponse::Appended { inserted: false });
            }
            let mut tx = db.begin_transaction()?;
            tx.insert(
                CLUSTER_EVENTS_COLLECTION,
                Record::new(id).with_metadata(value),
            )?;
            tx.commit()?;
            Ok(ClusterEventResponse::Appended { inserted: true })
        }
        ClusterEventOperation::Get { namespace, key } => {
            if db.actual_native_writer_enforcement_version()? == 1 {
                return Err(invalid("strong_point_read_unavailable: born-v2 Get requires the actual enrolled Strong dispatcher"));
            }
            let id = cluster_event_record_id(&namespace, &key)?;
            let value = db
                .get(CLUSTER_EVENTS_COLLECTION, &id)?
                .map(|r| r.metadata.clone());
            Ok(ClusterEventResponse::Value { value })
        }
        ClusterEventOperation::Scan {
            namespace,
            after,
            limit,
        } => {
            let prefix = format!("{namespace}/");
            let start = after.as_ref().map(|key| format!("{prefix}{key}"));
            // Account for framing/record overhead above the 64 KiB value cap.
            let batch = db.scan_collection_batch_after(
                CLUSTER_EVENTS_COLLECTION,
                Some(start.as_deref().unwrap_or(&prefix)),
                limit,
                1024 * 1024,
            )?;
            let mut events = Vec::new();
            let mut next = None;
            for record in batch {
                let Some(key) = record.id.strip_prefix(&prefix) else {
                    next = None;
                    break;
                };
                next = Some(key.to_owned());
                if range.contains_token(distribution_key_token(
                    CLUSTER_EVENTS_COLLECTION,
                    &record.id,
                )) {
                    events.push(ClusterEvent {
                        key: key.to_owned(),
                        value: record.metadata,
                    });
                }
            }
            Ok(ClusterEventResponse::Page { events, next })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DbConfig, DistributionConfig, DistributionStore, StorageMode};
    use serde_json::json;

    #[test]
    fn an_empty_schema_recipient_can_export_the_next_range_snapshot() {
        let source_root = tempfile::tempdir().unwrap();
        let target_root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged);
        let mut source = BicDb::open_with_config(source_root.path(), config.clone()).unwrap();
        source.create_collection(CLUSTER_EVENTS_COLLECTION).unwrap();
        let bundle = source.cluster_schema_bundle().unwrap();
        let mut target = BicDb::open_with_config(target_root.path(), config).unwrap();
        target.install_cluster_schema_bundle(&bundle).unwrap();
        let store = DistributionStore::initialize_at(
            source_root.path(),
            DistributionConfig {
                enabled: true,
                initial_ranges: 1,
                replication_factor: 1,
                ..Default::default()
            },
            false,
            1000,
        )
        .unwrap();
        let range = store.topology().ranges.values().next().unwrap();
        let mut rows = 0;
        target
            .for_each_range_snapshot_batch(
                CLUSTER_EVENTS_COLLECTION,
                range,
                None,
                &crate::RangeSnapshotOptions::default(),
                |batch| {
                    rows += batch.records.len();
                    Ok(true)
                },
            )
            .unwrap();
        assert_eq!(rows, 0);
        target
            .insert(
                CLUSTER_EVENTS_COLLECTION,
                Record::new("a/one").with_metadata(json!({"saved":true})),
            )
            .unwrap();
        target
            .for_each_range_snapshot_batch(
                CLUSTER_EVENTS_COLLECTION,
                range,
                None,
                &crate::RangeSnapshotOptions::default(),
                |batch| {
                    rows += batch.records.len();
                    Ok(true)
                },
            )
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn bounded_pages_do_not_cross_namespace_and_writes_require_admission() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        )
        .unwrap();
        db.create_collection(CLUSTER_EVENTS_COLLECTION).unwrap();
        for id in ["a/one", "a/three", "a/two", "b/private"] {
            db.insert(
                CLUSTER_EVENTS_COLLECTION,
                Record::new(id).with_metadata(json!({"id":id})),
            )
            .unwrap();
        }
        let store = DistributionStore::initialize_at(
            root.path(),
            DistributionConfig {
                enabled: true,
                initial_ranges: 1,
                replication_factor: 1,
                ..Default::default()
            },
            false,
            1000,
        )
        .unwrap();
        let range = store.topology().ranges.values().next().unwrap();
        let first = execute_cluster_event(
            &db,
            range,
            ClusterEventOperation::Scan {
                namespace: "a".into(),
                after: None,
                limit: 2,
            },
        )
        .unwrap();
        let ClusterEventResponse::Page { events, next } = first else {
            panic!()
        };
        assert_eq!(events.len(), 2);
        let second = execute_cluster_event(
            &db,
            range,
            ClusterEventOperation::Scan {
                namespace: "a".into(),
                after: next,
                limit: 2,
            },
        )
        .unwrap();
        let ClusterEventResponse::Page { events, next } = second else {
            panic!()
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].key, "two");
        assert!(next.is_none());
        assert!(execute_cluster_event(
            &db,
            range,
            ClusterEventOperation::Append {
                namespace: "a".into(),
                key: "new".into(),
                value: json!({"saved":true}),
            }
        )
        .unwrap_err()
        .to_string()
        .contains("admission is not ready"));
        assert!(db
            .get(CLUSTER_EVENTS_COLLECTION, "a/new")
            .unwrap()
            .is_none());
        assert!(ClusterEventOperation::Scan {
            namespace: "a".into(),
            after: None,
            limit: 129
        }
        .validate()
        .is_err());
        assert!(cluster_event_record_id("a", "../b/private").is_err());
        assert!(ClusterEventOperation::Append {
            namespace: "a".into(),
            key: "key".into(),
            value: json!("x".repeat(MAX_CLUSTER_EVENT_BYTES))
        }
        .validate()
        .is_err());
    }
}
