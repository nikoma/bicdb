//! Stream broker (Slice 1): durable queue consumption with consumer groups,
//! ack/nack, retry with visibility timeouts, and per-group dead-letter queues,
//! layered on the existing append-only [`EventStream`].
//!
//! Design: the broker is event-sourced on the same durable segment that holds
//! the queue messages. Every state transition — group creation, a delivery
//! batch, an ack, a nack, a dead-letter — is itself an appended control event,
//! so broker state survives restarts by replay (see
//! [`BrokerState::rebuild`]), inherits the stream's fsync/compression/
//! encryption configuration, and is fully auditable. The in-memory projection
//! keeps only *actionable* delivery records (in-flight or awaiting
//! redelivery); acked and dead-lettered deliveries are dropped from memory —
//! their history remains in the log and, for dead letters, in the DLQ stream.
//!
//! Streams used per queue `q` and group `g`:
//! - `queue:q` — the messages themselves (shared with the [`EventQueue`]
//!   facade; plain queue messages are consumable with default broker fields).
//! - `broker:q/g` — control events for one consumer group.
//! - `dlq:q/g` — dead-lettered message copies for one consumer group.
//!
//! Semantics:
//! - Distinct groups each receive every message once; consumers inside one
//!   group compete for messages.
//! - A new group starts at the beginning of the retained queue history.
//! - Delivery preserves publish order per group; a delayed message
//!   (`available_at` in the future) at the head of the undelivered range
//!   pauses fresh delivery for that group until it becomes available, so
//!   ordering is not reshuffled around it. Redeliveries are not blocked.
//! - `attempts` counts deliveries. When a redelivery would exceed the
//!   message's `max_attempts`, the message is dead-lettered instead.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::error::{BicDbError, Result};
use crate::event::{queue_stream, Event, EventStream, StoredEvent};

/// Default per-message delivery-attempt ceiling when the publisher does not
/// set one explicitly.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 16;

pub(crate) const QUEUE_MESSAGE_EVENT: &str = "QueueMessage";
const GROUP_CREATED_EVENT: &str = "BrokerGroupCreated";
const DELIVERED_EVENT: &str = "BrokerDelivered";
const ACKED_EVENT: &str = "BrokerAcked";
const NACKED_EVENT: &str = "BrokerNacked";
const DEAD_LETTERED_EVENT: &str = "BrokerDeadLettered";
const QUEUE_TRIMMED_EVENT: &str = "BrokerQueueTrimmed";
const QUEUE_CONFIGURED_EVENT: &str = "BrokerQueueConfigured";
const REDRIVEN_EVENT: &str = "BrokerRedriven";
/// Event type of the message copies appended to a group's DLQ stream.
pub const DEAD_LETTER_MESSAGE_EVENT: &str = "DeadLetteredMessage";

const BROKER_STREAM_PREFIX: &str = "broker:";
const DLQ_STREAM_PREFIX: &str = "dlq:";

fn control_stream(queue: &str, group: &str) -> String {
    format!("{BROKER_STREAM_PREFIX}{queue}/{group}")
}

fn dlq_stream(queue: &str, group: &str) -> String {
    format!("{DLQ_STREAM_PREFIX}{queue}/{group}")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

/// Broker names (queues, groups, consumers) allow the stream-name charset
/// minus `/`, which is reserved as the queue/group separator in control and
/// DLQ stream names.
pub(crate) fn validate_broker_name(kind: &str, name: &str) -> Result<()> {
    if name.is_empty()
        || !name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || byte == b'_'
                || byte == b'-'
                || byte == b'.'
                || byte == b':'
        })
    {
        return Err(BicDbError::Broker(format!(
            "invalid {kind} name `{name}`: use ASCII letters, digits, '-', '_', '.', or ':'"
        )));
    }
    Ok(())
}

/// Options for [`Broker::publish_with`].
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PublishOptions {
    /// Application headers carried alongside the payload.
    pub headers: Value,
    /// When set, republishing with the same key on the same queue is a no-op
    /// that returns the original message id (`deduplicated: true`).
    pub idempotency_key: Option<String>,
    /// Delay before the message becomes available for delivery.
    pub delay_ms: Option<u64>,
    /// Per-message delivery-attempt ceiling (defaults to
    /// [`DEFAULT_MAX_ATTEMPTS`]).
    pub max_attempts: Option<u32>,
}

/// A publish buffered inside an open transaction, applied when the
/// transaction commits (see `Transaction::buffer_broker_publish`). The
/// message id is pre-allocated so callers get a stable receipt up front.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingBrokerPublish {
    pub queue: String,
    pub payload: Value,
    pub options: PublishOptions,
    pub message_id: Uuid,
}

/// Result of a publish.
#[derive(Clone, Debug, PartialEq)]
pub struct PublishReceipt {
    pub message_id: Uuid,
    /// Per-queue logical sequence number (0-based publish order). Stable
    /// across restarts and segment compaction, unlike raw frame offsets.
    pub sequence: u64,
    /// True when an idempotency key suppressed a duplicate publish; the id
    /// and sequence then refer to the original message.
    pub deduplicated: bool,
}

/// Options for [`Broker::consume`].
#[derive(Clone, Copy, Debug)]
pub struct ConsumeOptions {
    pub max_messages: usize,
    /// How long a delivered message stays invisible to other consumers of the
    /// same group before it is considered abandoned and redelivered.
    pub visibility_timeout_ms: u64,
}

impl Default for ConsumeOptions {
    fn default() -> Self {
        Self {
            max_messages: 1,
            visibility_timeout_ms: 30_000,
        }
    }
}

/// Options for [`Broker::nack`].
#[derive(Clone, Debug, Default)]
pub struct NackOptions {
    /// Requeue for redelivery (subject to `max_attempts`); when false the
    /// message is dead-lettered immediately.
    pub requeue: bool,
    /// Delay before the requeued message becomes deliverable again.
    pub delay_ms: Option<u64>,
    /// Consumer-reported error, recorded on the delivery and in the DLQ copy.
    pub error: Option<String>,
}

/// A message handed to a consumer by [`Broker::consume`].
#[derive(Clone, Debug, PartialEq)]
pub struct BrokerMessage {
    pub queue: String,
    pub group: String,
    pub message_id: Uuid,
    /// Per-queue logical sequence number (0-based publish order).
    pub sequence: u64,
    pub payload: Value,
    pub headers: Value,
    pub idempotency_key: Option<String>,
    /// Unix milliseconds.
    pub created_at: i64,
    /// Unix milliseconds; when this delivery attempt was handed out.
    pub delivered_at: i64,
    /// Unix milliseconds; after this instant the message may be redelivered
    /// to another consumer of the group unless acked first.
    pub visibility_deadline: i64,
    /// 1-based delivery attempt counter (this delivery included).
    pub attempts: u32,
    pub max_attempts: u32,
}

/// Durable per-group summary, exposed via [`Broker::group_info`].
#[derive(Clone, Debug, PartialEq)]
pub struct ConsumerGroupInfo {
    pub queue: String,
    pub group: String,
    /// Unix milliseconds.
    pub created_at: i64,
    /// Highest queue sequence number delivered to this group, if any.
    pub last_delivered_sequence: Option<u64>,
    /// Deliveries currently handed out and not yet acked/expired.
    pub in_flight: usize,
    /// Deliveries awaiting redelivery (nack-requeued or visibility-expired).
    pub pending_redelivery: usize,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum DeliveryStatus {
    /// Handed to a consumer; invisible to the group until the deadline.
    InFlight { visibility_deadline: i64 },
    /// Requeued (nack or visibility expiry); deliverable once `available_at`
    /// passes.
    Available { available_at: i64 },
}

#[derive(Clone, Debug)]
struct Delivery {
    sequence: u64,
    consumer_id: String,
    status: DeliveryStatus,
    attempts: u32,
    delivered_at: i64,
    last_error: Option<String>,
}

#[derive(Clone, Debug)]
struct GroupState {
    created_at: i64,
    /// Next queue sequence number to examine for fresh deliveries.
    next_sequence: u64,
    /// Actionable deliveries only (in-flight or awaiting redelivery), keyed
    /// by message id. Acked and dead-lettered deliveries are removed.
    deliveries: HashMap<Uuid, Delivery>,
    /// First sequence not yet settled (acked or dead-lettered) by this
    /// group; everything below it is settled. Gates settled trimming.
    settled_floor: u64,
    /// Settled sequences at or above the floor (out-of-order settlement).
    settled_above: std::collections::BTreeSet<u64>,
    /// DLQ copies (by DLQ event id) already redriven back onto the queue,
    /// so a second redrive pass skips them.
    redriven: std::collections::HashSet<Uuid>,
}

impl GroupState {
    fn new(created_at: i64, settled_floor: u64) -> Self {
        Self {
            created_at,
            next_sequence: 0,
            deliveries: HashMap::new(),
            settled_floor,
            settled_above: std::collections::BTreeSet::new(),
            redriven: std::collections::HashSet::new(),
        }
    }

    fn settle(&mut self, sequence: u64) {
        if sequence < self.settled_floor {
            return;
        }
        if sequence == self.settled_floor {
            self.settled_floor += 1;
            while self.settled_above.remove(&self.settled_floor) {
                self.settled_floor += 1;
            }
        } else {
            self.settled_above.insert(sequence);
        }
    }

    /// Applies a queue trim through `through`: removed messages can no
    /// longer be delivered, so their deliveries drop and the settled floor
    /// jumps past them.
    fn apply_trim(&mut self, through: u64) {
        self.deliveries
            .retain(|_, delivery| delivery.sequence > through);
        if self.settled_floor <= through {
            self.settled_floor = through.saturating_add(1);
        }
        self.settled_above = self.settled_above.split_off(&self.settled_floor);
        while self.settled_above.remove(&self.settled_floor) {
            self.settled_floor += 1;
        }
        self.next_sequence = self.next_sequence.max(through.saturating_add(1));
    }
}

/// One queue's retained messages, ordered by sequence. Sequences come from
/// an explicit counter (not vector positions) so they survive trimming; the
/// durable floor is carried by `BrokerQueueTrimmed` markers in the log.
#[derive(Debug, Default)]
struct QueueMessages {
    /// (sequence, message id), ascending; the trimmed prefix is absent.
    entries: Vec<(u64, Uuid)>,
    /// Sequence assigned to the next published message.
    next_sequence: u64,
}

/// Durable per-queue configuration, set via [`Broker::configure_queue`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueueConfig {
    /// Default delivery-attempt ceiling for messages published without an
    /// explicit `max_attempts` (falls back to [`DEFAULT_MAX_ATTEMPTS`]).
    pub default_max_attempts: Option<u32>,
    /// Retention: keep at most this many messages; older ones may be trimmed
    /// by [`Broker::trim`] even if unsettled (explicit data-loss policy).
    pub retention_max_messages: Option<u64>,
    /// Retention: messages older than this may be trimmed by
    /// [`Broker::trim`] even if unsettled (explicit data-loss policy).
    pub retention_max_age_ms: Option<i64>,
    /// Roles allowed to publish from restricted (security-context) SQL
    /// sessions. `None` = unrestricted. Trusted sessions always pass.
    pub publish_roles: Option<std::collections::BTreeSet<String>>,
    /// Roles allowed to consume/ack/nack/inspect from restricted SQL
    /// sessions. `None` = unrestricted.
    pub consume_roles: Option<std::collections::BTreeSet<String>>,
    /// Roles allowed to run administrative operations (configure, trim,
    /// redrive, purge) from restricted SQL sessions. Unlike publish/consume,
    /// restricted sessions are DENIED admin operations unless explicitly
    /// granted here (so a restricted user can never bootstrap an ACL).
    pub admin_roles: Option<std::collections::BTreeSet<String>>,
}

/// Access classes checked by [`Broker::authorize`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BrokerAccess {
    Publish,
    Consume,
    Admin,
}

/// Result of a [`Broker::trim`] pass.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TrimReport {
    /// Messages removed from the retained log.
    pub removed_messages: usize,
    /// Broker control events removed alongside them.
    pub removed_control_events: usize,
    /// Highest sequence trimmed, if anything was trimmed.
    pub through_sequence: Option<u64>,
    /// Segment bytes before and after the rewrite (0/0 when nothing trimmed).
    pub bytes_before: u64,
    pub bytes_after: u64,
}

/// Options for [`Broker::drain`].
#[derive(Clone, Copy, Debug)]
pub struct DrainOptions {
    /// Messages fetched per consume round.
    pub batch_size: usize,
    /// Visibility timeout applied to each round's deliveries.
    pub visibility_timeout_ms: u64,
    /// Stop after handling this many messages (0 = drain until empty).
    pub max_messages: usize,
    /// Redelivery delay applied when the handler rejects a message.
    pub nack_delay_ms: Option<u64>,
}

impl Default for DrainOptions {
    fn default() -> Self {
        Self {
            batch_size: 32,
            visibility_timeout_ms: 30_000,
            max_messages: 0,
            nack_delay_ms: None,
        }
    }
}

/// Result of a [`Broker::drain`] run.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DrainReport {
    /// Messages handed to the handler.
    pub delivered: usize,
    /// Messages the handler accepted (acked).
    pub acked: usize,
    /// Messages the handler rejected (nacked; retried or dead-lettered per
    /// the usual attempt accounting).
    pub nacked: usize,
}

/// A retained message as seen by [`Broker::peek`] — a read-only view that
/// does not deliver, so it never affects group state or attempt counters.
#[derive(Clone, Debug, PartialEq)]
pub struct PeekedMessage {
    pub sequence: u64,
    pub message_id: Uuid,
    pub payload: Value,
    pub headers: Value,
    pub idempotency_key: Option<String>,
    /// Unix milliseconds.
    pub created_at: i64,
    /// Unix milliseconds; in the future for delayed messages.
    pub available_at: i64,
    pub max_attempts: u32,
}

/// Point-in-time broker observability snapshot; see [`Broker::stats`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BrokerStats {
    pub queues: Vec<QueueStats>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueueStats {
    pub queue: String,
    /// Messages currently retained in the log (after trims).
    pub retained_messages: usize,
    /// Sequence the next published message will receive.
    pub next_sequence: u64,
    /// Whether a durable queue configuration exists.
    pub configured: bool,
    pub groups: Vec<GroupStats>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupStats {
    pub group: String,
    /// Unix milliseconds.
    pub created_at: i64,
    pub last_delivered_sequence: Option<u64>,
    pub in_flight: usize,
    pub pending_redelivery: usize,
    /// Every sequence below this is settled (acked or dead-lettered).
    pub settled_floor: u64,
    /// Retained messages not yet delivered to this group.
    pub lag: usize,
    /// Dead-letter copies currently in the group's DLQ stream.
    pub dead_letters: usize,
}

/// In-memory projection of the broker control events. Owned by
/// [`EventStream`]; rebuilt on open by replaying the log.
#[derive(Debug, Default)]
pub(crate) struct BrokerState {
    /// queue -> retained messages + sequence counter
    queues: HashMap<String, QueueMessages>,
    /// queue -> group -> state
    groups: HashMap<String, HashMap<String, GroupState>>,
    /// (queue, idempotency key) -> (message id, sequence) of the original.
    /// The dedup window equals the retention window: keys of trimmed
    /// messages are forgotten.
    idempotency: HashMap<(String, String), (Uuid, u64)>,
    /// queue -> durable configuration (latest write wins)
    configs: HashMap<String, QueueConfig>,
}

/// Where a broker operation came from.
///
/// Provenance and identity are separate facts. Collapsing them into
/// `Option<&SecurityContext>` is what made an unidentified SQL caller more
/// powerful than an identified one: `None` meant both "no authenticated broker
/// identity" and "trusted embedded host", and pgwire produced the former while
/// the gate read the latter.
#[derive(Clone, Copy, Debug)]
pub enum BrokerCaller<'a> {
    /// The embedded Rust API. The host process is the trust boundary, so this
    /// passes unconditionally — and it must be stated, never inferred.
    TrustedHost,
    /// A SQL statement. `context` is the authenticated broker identity when the
    /// server established one; its absence NEVER means trusted. `superuser` is
    /// the effective SQL role's status, used as an authorization fact.
    Sql {
        context: Option<&'a crate::db::SecurityContext>,
        superuser: bool,
    },
}

impl BrokerState {
    /// Replays the ordered event log into broker state. Used at open; the
    /// live paths apply the same transitions incrementally as they append.
    pub(crate) fn rebuild(events: &[StoredEvent]) -> Self {
        let mut state = Self::default();
        for stored in events {
            state.apply(stored);
        }
        state
    }

    /// Applies one appended event to the projection. Must stay in lockstep
    /// with what the `Broker` methods append so that replay reproduces the
    /// live state exactly.
    pub(crate) fn apply(&mut self, stored: &StoredEvent) {
        let event = &stored.event;
        if let Some(queue) = event.stream.strip_prefix("queue:") {
            if event.event_type == QUEUE_MESSAGE_EVENT {
                let messages = self.queues.entry(queue.to_string()).or_default();
                let sequence = messages.next_sequence;
                messages.entries.push((sequence, event.id));
                messages.next_sequence += 1;
                if let Some(key) = broker_meta(&event.metadata)
                    .and_then(|meta| meta.get("idempotency_key"))
                    .and_then(Value::as_str)
                {
                    self.idempotency
                        .entry((queue.to_string(), key.to_string()))
                        .or_insert((event.id, sequence));
                }
            }
            return;
        }
        if !event.stream.starts_with(BROKER_STREAM_PREFIX) {
            return;
        }
        let payload = &event.payload;
        let Some(queue) = payload.get("queue").and_then(Value::as_str) else {
            return;
        };
        // Queue-level control events carry no group.
        match event.event_type.as_str() {
            QUEUE_TRIMMED_EVENT => {
                let through = payload
                    .get("through_sequence")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let messages = self.queues.entry(queue.to_string()).or_default();
                messages.entries.retain(|(sequence, _)| *sequence > through);
                messages.next_sequence = messages.next_sequence.max(through.saturating_add(1));
                if let Some(groups) = self.groups.get_mut(queue) {
                    for group_state in groups.values_mut() {
                        group_state.apply_trim(through);
                    }
                }
                return;
            }
            QUEUE_CONFIGURED_EVENT => {
                self.configs.insert(
                    queue.to_string(),
                    queue_config_from_json(payload.get("config").unwrap_or(&Value::Null)),
                );
                return;
            }
            _ => {}
        }
        let Some(group) = payload.get("group").and_then(Value::as_str) else {
            return;
        };
        match event.event_type.as_str() {
            GROUP_CREATED_EVENT => {
                let created_at = payload
                    .get("created_at_ms")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                let settled_floor = self
                    .queues
                    .get(queue)
                    .map(|messages| {
                        messages
                            .entries
                            .first()
                            .map(|(sequence, _)| *sequence)
                            .unwrap_or(messages.next_sequence)
                    })
                    .unwrap_or_default();
                self.groups
                    .entry(queue.to_string())
                    .or_default()
                    .entry(group.to_string())
                    .or_insert_with(|| GroupState::new(created_at, settled_floor));
            }
            DELIVERED_EVENT => {
                let Some(state) = self.group_mut(queue, group) else {
                    return;
                };
                let consumer_id = payload
                    .get("consumer_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let delivered_at = payload
                    .get("delivered_at_ms")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                let visibility_deadline = payload
                    .get("visibility_deadline_ms")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                for entry in payload
                    .get("deliveries")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    let Some(message_id) = entry
                        .get("message_id")
                        .and_then(Value::as_str)
                        .and_then(|raw| Uuid::parse_str(raw).ok())
                    else {
                        continue;
                    };
                    let sequence = entry.get("sequence").and_then(Value::as_u64).unwrap_or(0);
                    let attempts =
                        entry.get("attempts").and_then(Value::as_u64).unwrap_or(1) as u32;
                    state.deliveries.insert(
                        message_id,
                        Delivery {
                            sequence,
                            consumer_id: consumer_id.clone(),
                            status: DeliveryStatus::InFlight {
                                visibility_deadline,
                            },
                            attempts,
                            delivered_at,
                            last_error: None,
                        },
                    );
                    state.next_sequence = state.next_sequence.max(sequence.saturating_add(1));
                }
            }
            ACKED_EVENT => {
                if let (Some(state), Some(message_id)) =
                    (self.group_mut(queue, group), payload_message_id(payload))
                {
                    let sequence = state
                        .deliveries
                        .remove(&message_id)
                        .map(|delivery| delivery.sequence)
                        .or_else(|| payload.get("sequence").and_then(Value::as_u64));
                    if let Some(sequence) = sequence {
                        state.settle(sequence);
                    }
                }
            }
            NACKED_EVENT => {
                let Some(state) = self.group_mut(queue, group) else {
                    return;
                };
                let Some(message_id) = payload_message_id(payload) else {
                    return;
                };
                if let Some(delivery) = state.deliveries.get_mut(&message_id) {
                    delivery.status = DeliveryStatus::Available {
                        available_at: payload
                            .get("available_at_ms")
                            .and_then(Value::as_i64)
                            .unwrap_or_default(),
                    };
                    delivery.last_error = payload
                        .get("error")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                }
            }
            DEAD_LETTERED_EVENT => {
                if let (Some(state), Some(message_id)) =
                    (self.group_mut(queue, group), payload_message_id(payload))
                {
                    let sequence = state
                        .deliveries
                        .remove(&message_id)
                        .map(|delivery| delivery.sequence)
                        .or_else(|| payload.get("sequence").and_then(Value::as_u64));
                    if let Some(sequence) = sequence {
                        state.settle(sequence);
                    }
                }
            }
            REDRIVEN_EVENT => {
                let Some(state) = self.group_mut(queue, group) else {
                    return;
                };
                for id in payload
                    .get("dlq_event_ids")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|raw| raw.as_str().and_then(|raw| Uuid::parse_str(raw).ok()))
                {
                    state.redriven.insert(id);
                }
            }
            _ => {}
        }
    }

    fn group_mut(&mut self, queue: &str, group: &str) -> Option<&mut GroupState> {
        self.groups.get_mut(queue)?.get_mut(group)
    }

    fn group(&self, queue: &str, group: &str) -> Option<&GroupState> {
        self.groups.get(queue)?.get(group)
    }
}

fn roles_from_json(value: Option<&Value>) -> Option<std::collections::BTreeSet<String>> {
    value.and_then(Value::as_array).map(|roles| {
        roles
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    })
}

fn roles_to_json(roles: &Option<std::collections::BTreeSet<String>>) -> Value {
    match roles {
        Some(roles) => Value::Array(roles.iter().cloned().map(Value::String).collect()),
        None => Value::Null,
    }
}

fn queue_config_from_json(value: &Value) -> QueueConfig {
    QueueConfig {
        default_max_attempts: value
            .get("default_max_attempts")
            .and_then(Value::as_u64)
            .map(|raw| (raw as u32).max(1)),
        retention_max_messages: value.get("retention_max_messages").and_then(Value::as_u64),
        retention_max_age_ms: value.get("retention_max_age_ms").and_then(Value::as_i64),
        publish_roles: roles_from_json(value.get("publish_roles")),
        consume_roles: roles_from_json(value.get("consume_roles")),
        admin_roles: roles_from_json(value.get("admin_roles")),
    }
}

fn queue_config_to_json(config: &QueueConfig) -> Value {
    json!({
        "default_max_attempts": config.default_max_attempts,
        "retention_max_messages": config.retention_max_messages,
        "retention_max_age_ms": config.retention_max_age_ms,
        "publish_roles": roles_to_json(&config.publish_roles),
        "consume_roles": roles_to_json(&config.consume_roles),
        "admin_roles": roles_to_json(&config.admin_roles),
    })
}

/// Removes references to trimmed messages from one group-level control
/// event. A delivered batch can contain both removed and surviving messages,
/// so retaining or dropping the whole event would make replay reconstruct
/// ghost in-flight deliveries for the removed entries.
fn retain_trimmed_control_event(
    event: &mut Event,
    removed: &std::collections::HashSet<Uuid>,
) -> bool {
    match event.event_type.as_str() {
        DELIVERED_EVENT => {
            let Some(entries) = event
                .payload
                .get_mut("deliveries")
                .and_then(Value::as_array_mut)
            else {
                return false;
            };
            entries.retain(|entry| {
                !entry
                    .get("message_id")
                    .and_then(Value::as_str)
                    .and_then(|raw| Uuid::parse_str(raw).ok())
                    .is_some_and(|id| removed.contains(&id))
            });
            !entries.is_empty()
        }
        ACKED_EVENT | NACKED_EVENT | DEAD_LETTERED_EVENT => {
            !payload_message_id(&event.payload).is_some_and(|id| removed.contains(&id))
        }
        // GroupCreated, Redriven, and unknown types always survive.
        _ => true,
    }
}

fn payload_message_id(payload: &Value) -> Option<Uuid> {
    payload
        .get("message_id")
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

fn broker_meta(metadata: &Value) -> Option<&Value> {
    metadata.get("broker")
}

/// Broker-relevant fields of one stored queue message. Messages published
/// through the plain [`EventQueue`] facade (no broker metadata) get defaults,
/// so both publish paths interoperate.
struct MessageFields {
    headers: Value,
    idempotency_key: Option<String>,
    created_at: i64,
    available_at: i64,
    max_attempts: u32,
}

fn message_fields(event: &Event) -> MessageFields {
    let meta = broker_meta(&event.metadata);
    let get_i64 = |key: &str| meta.and_then(|m| m.get(key)).and_then(Value::as_i64);
    MessageFields {
        headers: meta
            .and_then(|m| m.get("headers"))
            .cloned()
            .unwrap_or(Value::Null),
        idempotency_key: meta
            .and_then(|m| m.get("idempotency_key"))
            .and_then(Value::as_str)
            .map(str::to_string),
        created_at: get_i64("created_at_ms").unwrap_or(event.timestamp.saturating_mul(1000)),
        available_at: get_i64("available_at_ms").unwrap_or(event.timestamp.saturating_mul(1000)),
        max_attempts: meta
            .and_then(|m| m.get("max_attempts"))
            .and_then(Value::as_u64)
            .map(|raw| (raw as u32).max(1))
            .unwrap_or(DEFAULT_MAX_ATTEMPTS),
    }
}

/// Durable-queue facade over an [`EventStream`]. Obtain via
/// [`EventStream::broker`] or `BicDb::broker`.
pub struct Broker<'a> {
    stream: &'a mut EventStream,
}

impl<'a> Broker<'a> {
    pub(crate) fn new(stream: &'a mut EventStream) -> Self {
        Self { stream }
    }

    /// Publishes a message with plain headers. See [`Self::publish_with`].
    pub fn publish(
        &mut self,
        queue: &str,
        payload: Value,
        headers: Value,
    ) -> Result<PublishReceipt> {
        self.publish_with(
            queue,
            payload,
            PublishOptions {
                headers,
                ..PublishOptions::default()
            },
        )
    }

    /// Publishes a message onto `queue`'s durable stream. With an idempotency
    /// key, a duplicate publish returns the original receipt instead of
    /// appending a second copy.
    pub fn publish_with(
        &mut self,
        queue: &str,
        payload: Value,
        options: PublishOptions,
    ) -> Result<PublishReceipt> {
        self.publish_inner(queue, payload, options, None)
    }

    /// Applies a transaction-buffered publish with its pre-allocated message
    /// id (idempotent: a replayed id is a no-op thanks to event-id dedup).
    /// Skips the opportunistic retention trim to keep commit paths lean.
    pub(crate) fn publish_prepared(
        &mut self,
        pending: &PendingBrokerPublish,
    ) -> Result<PublishReceipt> {
        self.publish_inner(
            &pending.queue,
            pending.payload.clone(),
            pending.options.clone(),
            Some(pending.message_id),
        )
    }

    fn publish_inner(
        &mut self,
        queue: &str,
        payload: Value,
        options: PublishOptions,
        prepared_id: Option<Uuid>,
    ) -> Result<PublishReceipt> {
        validate_broker_name("queue", queue)?;
        if let Some(key) = options.idempotency_key.as_deref() {
            if let Some(&(message_id, sequence)) = self
                .stream
                .broker_state
                .idempotency
                .get(&(queue.to_string(), key.to_string()))
            {
                return Ok(PublishReceipt {
                    message_id,
                    sequence,
                    deduplicated: true,
                });
            }
        }
        // Opportunistic retention enforcement, before the append so a trim
        // failure cannot leave a publish half-reported. Skipped for prepared
        // (commit-time) publishes to keep the commit path lean.
        if prepared_id.is_none() && self.publish_trim_due(queue) {
            self.trim(queue)?;
        }
        let now = now_ms();
        let available_at = now.saturating_add(options.delay_ms.unwrap_or(0) as i64);
        let max_attempts = options
            .max_attempts
            .or_else(|| {
                self.stream
                    .broker_state
                    .configs
                    .get(queue)
                    .and_then(|config| config.default_max_attempts)
            })
            .unwrap_or(DEFAULT_MAX_ATTEMPTS)
            .max(1);
        let mut broker_meta = json!({
            "headers": options.headers,
            "created_at_ms": now,
            "available_at_ms": available_at,
            "max_attempts": max_attempts,
        });
        if let Some(key) = options.idempotency_key.as_deref() {
            broker_meta["idempotency_key"] = Value::String(key.to_string());
        }
        let mut event = Event::new(queue_stream(queue), QUEUE_MESSAGE_EVENT, payload)
            .with_metadata(json!({ "broker": broker_meta }));
        if let Some(id) = prepared_id {
            event = event.with_id(id);
        }
        let message_id = event.id;
        // `append` routes through `EventStream::append_event`, which applies
        // the event to `broker_state` (registering the message sequence and
        // idempotency key).
        self.stream.append(event)?;
        let sequence = self
            .stream
            .broker_state
            .queues
            .get(queue)
            .map(|messages| messages.next_sequence.saturating_sub(1))
            .unwrap_or_default();
        Ok(PublishReceipt {
            message_id,
            sequence,
            deduplicated: false,
        })
    }

    /// Delivers up to `max_messages` to `consumer_id` of `group`, creating the
    /// group durably on first use. Redeliveries (nack-requeued or
    /// visibility-expired, in sequence order) are served before fresh messages;
    /// a redelivery that would exceed `max_attempts` is dead-lettered instead.
    /// Delivered messages are invisible to the group until acked, nacked, or
    /// `visibility_timeout_ms` elapses.
    pub fn consume(
        &mut self,
        queue: &str,
        group: &str,
        consumer_id: &str,
        options: ConsumeOptions,
    ) -> Result<Vec<BrokerMessage>> {
        validate_broker_name("queue", queue)?;
        validate_broker_name("group", group)?;
        validate_broker_name("consumer", consumer_id)?;
        let now = now_ms();
        self.ensure_group(queue, group, now)?;
        if options.max_messages == 0 {
            return Ok(Vec::new());
        }
        let visibility_deadline = now.saturating_add(options.visibility_timeout_ms as i64);

        // Phase 1 (read-only): decide redeliveries, expiry dead-letters, and
        // fresh deliveries against the current projection.
        let group_state = self
            .stream
            .broker_state
            .group(queue, group)
            .expect("group ensured above");
        let mut due: Vec<(u64, Uuid)> = group_state
            .deliveries
            .iter()
            .filter_map(|(id, delivery)| match delivery.status {
                DeliveryStatus::Available { available_at } if available_at <= now => {
                    Some((delivery.sequence, *id))
                }
                DeliveryStatus::InFlight {
                    visibility_deadline,
                } if visibility_deadline <= now => Some((delivery.sequence, *id)),
                _ => None,
            })
            .collect();
        due.sort_unstable();

        // (message_id, sequence, attempts) chosen for this delivery batch.
        let mut batch: Vec<(Uuid, u64, u32)> = Vec::new();
        // (message_id, sequence, attempts, reason, last_error) to dead-letter.
        let mut dead: Vec<(Uuid, u64, u32, String, Option<String>)> = Vec::new();
        for (sequence, message_id) in due {
            let delivery = &group_state.deliveries[&message_id];
            let max_attempts = self
                .stream
                .event_by_id(&message_id)
                .map(|stored| message_fields(&stored.event).max_attempts)
                .unwrap_or(DEFAULT_MAX_ATTEMPTS);
            let next_attempt = delivery.attempts.saturating_add(1);
            if next_attempt > max_attempts {
                let reason = if matches!(delivery.status, DeliveryStatus::InFlight { .. }) {
                    format!(
                        "visibility timeout expired on final attempt {} of {max_attempts}",
                        delivery.attempts
                    )
                } else {
                    format!("exceeded max_attempts {max_attempts}")
                };
                dead.push((
                    message_id,
                    sequence,
                    delivery.attempts,
                    reason,
                    delivery.last_error.clone(),
                ));
            } else if batch.len() < options.max_messages {
                batch.push((message_id, sequence, next_attempt));
            }
        }

        let next_sequence = group_state.next_sequence;
        if batch.len() < options.max_messages {
            let entries = self
                .stream
                .broker_state
                .queues
                .get(queue)
                .map(|messages| messages.entries.as_slice())
                .unwrap_or_default();
            let start = entries.partition_point(|(sequence, _)| *sequence < next_sequence);
            for (sequence, message_id) in &entries[start..] {
                if batch.len() >= options.max_messages {
                    break;
                }
                let Some(stored) = self.stream.event_by_id(message_id) else {
                    continue;
                };
                let fields = message_fields(&stored.event);
                if fields.available_at > now {
                    // Preserve per-group ordering: stop fresh delivery at the
                    // first not-yet-available message instead of skipping it.
                    break;
                }
                batch.push((*message_id, *sequence, 1));
            }
        }

        // Phase 2 (append + apply): durably record the transitions, then
        // mutate the projection. Dead letters first so a crash between the
        // two appends redelivers rather than double-delivers.
        for (message_id, sequence, attempts, reason, last_error) in dead {
            self.append_dead_letter(
                queue, group, message_id, sequence, attempts, &reason, last_error, now,
            )?;
        }
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        let deliveries_json = batch
            .iter()
            .map(|(message_id, sequence, attempts)| {
                json!({
                    "message_id": message_id.to_string(),
                    "sequence": sequence,
                    "attempts": attempts,
                })
            })
            .collect::<Vec<_>>();
        self.append_control(
            queue,
            group,
            DELIVERED_EVENT,
            json!({
                "queue": queue,
                "group": group,
                "consumer_id": consumer_id,
                "delivered_at_ms": now,
                "visibility_deadline_ms": visibility_deadline,
                "deliveries": deliveries_json,
            }),
        )?;

        let mut messages = Vec::with_capacity(batch.len());
        for (message_id, sequence, attempts) in batch {
            let Some(stored) = self.stream.event_by_id(&message_id) else {
                continue;
            };
            let fields = message_fields(&stored.event);
            messages.push(BrokerMessage {
                queue: queue.to_string(),
                group: group.to_string(),
                message_id,
                sequence,
                payload: stored.event.payload.clone(),
                headers: fields.headers,
                idempotency_key: fields.idempotency_key,
                created_at: fields.created_at,
                delivered_at: now,
                visibility_deadline,
                attempts,
                max_attempts: fields.max_attempts,
            });
        }
        Ok(messages)
    }

    /// Acknowledges a delivered message: it will never be redelivered to this
    /// group. Errors if the delivery is not in flight for `consumer_id`.
    pub fn ack(
        &mut self,
        queue: &str,
        group: &str,
        consumer_id: &str,
        message_id: Uuid,
    ) -> Result<()> {
        self.check_in_flight(queue, group, consumer_id, message_id)?;
        let sequence = self
            .stream
            .broker_state
            .group(queue, group)
            .and_then(|state| state.deliveries.get(&message_id))
            .map(|delivery| delivery.sequence);
        self.append_control(
            queue,
            group,
            ACKED_EVENT,
            json!({
                "queue": queue,
                "group": group,
                "consumer_id": consumer_id,
                "message_id": message_id.to_string(),
                "sequence": sequence,
                "acked_at_ms": now_ms(),
            }),
        )
    }

    /// Negatively acknowledges a delivered message. With `requeue` it becomes
    /// available again after `delay_ms` (subject to `max_attempts`); without
    /// `requeue` — or when attempts are exhausted — it is dead-lettered.
    pub fn nack(
        &mut self,
        queue: &str,
        group: &str,
        consumer_id: &str,
        message_id: Uuid,
        options: NackOptions,
    ) -> Result<()> {
        self.check_in_flight(queue, group, consumer_id, message_id)?;
        let now = now_ms();
        let delivery = &self
            .stream
            .broker_state
            .group(queue, group)
            .expect("checked")
            .deliveries[&message_id];
        let attempts = delivery.attempts;
        let sequence = delivery.sequence;
        let max_attempts = self
            .stream
            .event_by_id(&message_id)
            .map(|stored| message_fields(&stored.event).max_attempts)
            .unwrap_or(DEFAULT_MAX_ATTEMPTS);
        if !options.requeue || attempts >= max_attempts {
            let reason = if options.requeue {
                format!("exceeded max_attempts {max_attempts}")
            } else {
                "nacked without requeue".to_string()
            };
            return self.append_dead_letter(
                queue,
                group,
                message_id,
                sequence,
                attempts,
                &reason,
                options.error,
                now,
            );
        }
        self.append_control(
            queue,
            group,
            NACKED_EVENT,
            json!({
                "queue": queue,
                "group": group,
                "consumer_id": consumer_id,
                "message_id": message_id.to_string(),
                "sequence": sequence,
                "requeue": true,
                "available_at_ms": now.saturating_add(options.delay_ms.unwrap_or(0) as i64),
                "error": options.error,
                "nacked_at_ms": now,
            }),
        )
    }

    /// Dead-lettered message copies for one group, oldest first.
    pub fn dead_letters(&self, queue: &str, group: &str) -> Vec<StoredEvent> {
        self.stream.read(&dlq_stream(queue, group))
    }

    /// Durable state summary for one consumer group, if it exists.
    pub fn group_info(&self, queue: &str, group: &str) -> Option<ConsumerGroupInfo> {
        let state = self.stream.broker_state.group(queue, group)?;
        let mut in_flight = 0;
        let mut pending = 0;
        for delivery in state.deliveries.values() {
            match delivery.status {
                DeliveryStatus::InFlight { .. } => in_flight += 1,
                DeliveryStatus::Available { .. } => pending += 1,
            }
        }
        Some(ConsumerGroupInfo {
            queue: queue.to_string(),
            group: group.to_string(),
            created_at: state.created_at,
            last_delivered_sequence: state.next_sequence.checked_sub(1),
            in_flight,
            pending_redelivery: pending,
        })
    }

    /// Pumps deliverable messages through `handler`: each accepted message
    /// (`Ok`) is acked, each rejected one (`Err(reason)`) is nacked with
    /// requeue (dead-lettering once attempts are exhausted, as usual). Runs
    /// until the group has nothing deliverable or `max_messages` is reached.
    /// Combine with an [`EventStream::subscribe`] wake signal on the
    /// `queue:<name>` stream for push-style workers.
    pub fn drain<F>(
        &mut self,
        queue: &str,
        group: &str,
        consumer_id: &str,
        options: DrainOptions,
        mut handler: F,
    ) -> Result<DrainReport>
    where
        F: FnMut(&BrokerMessage) -> std::result::Result<(), String>,
    {
        let mut report = DrainReport::default();
        let batch_size = options.batch_size.max(1);
        loop {
            let budget = if options.max_messages == 0 {
                batch_size
            } else {
                batch_size.min(options.max_messages - report.delivered)
            };
            if budget == 0 {
                break;
            }
            let batch = self.consume(
                queue,
                group,
                consumer_id,
                ConsumeOptions {
                    max_messages: budget,
                    visibility_timeout_ms: options.visibility_timeout_ms,
                },
            )?;
            if batch.is_empty() {
                break;
            }
            for message in &batch {
                report.delivered += 1;
                match handler(message) {
                    Ok(()) => {
                        self.ack(queue, group, consumer_id, message.message_id)?;
                        report.acked += 1;
                    }
                    Err(reason) => {
                        self.nack(
                            queue,
                            group,
                            consumer_id,
                            message.message_id,
                            NackOptions {
                                requeue: true,
                                delay_ms: options.nack_delay_ms,
                                error: Some(reason),
                            },
                        )?;
                        report.nacked += 1;
                    }
                }
            }
        }
        Ok(report)
    }

    /// Whether a consume call for this group would hand out at least one
    /// message right now. Cheap poll for worker loops. A group that does not
    /// exist yet is deliverable when the queue retains any available message.
    pub fn has_deliverable(&self, queue: &str, group: &str) -> bool {
        let now = now_ms();
        let state = &self.stream.broker_state;
        let Some(messages) = state.queues.get(queue) else {
            return false;
        };
        let group_state = state.group(queue, group);
        if let Some(group_state) = group_state {
            let due = group_state
                .deliveries
                .values()
                .any(|delivery| match delivery.status {
                    DeliveryStatus::Available { available_at } => available_at <= now,
                    DeliveryStatus::InFlight {
                        visibility_deadline,
                    } => visibility_deadline <= now,
                });
            if due {
                return true;
            }
        }
        let cursor = group_state.map(|state| state.next_sequence).unwrap_or(0);
        let start = messages
            .entries
            .partition_point(|(sequence, _)| *sequence < cursor);
        // Head-of-line semantics: only the first undelivered message counts.
        messages.entries.get(start).is_some_and(|(_, message_id)| {
            self.stream
                .event_by_id(message_id)
                .is_some_and(|stored| message_fields(&stored.event).available_at <= now)
        })
    }

    /// Read-only view of up to `max` retained messages of `queue` starting
    /// at `from_sequence` (ascending). Never delivers: group cursors,
    /// visibility, and attempt counters are untouched — observability only.
    pub fn peek(&self, queue: &str, from_sequence: u64, max: usize) -> Vec<PeekedMessage> {
        let Some(messages) = self.stream.broker_state.queues.get(queue) else {
            return Vec::new();
        };
        let start = messages
            .entries
            .partition_point(|(sequence, _)| *sequence < from_sequence);
        messages.entries[start..]
            .iter()
            .take(max)
            .filter_map(|(sequence, message_id)| {
                let stored = self.stream.event_by_id(message_id)?;
                let fields = message_fields(&stored.event);
                Some(PeekedMessage {
                    sequence: *sequence,
                    message_id: *message_id,
                    payload: stored.event.payload.clone(),
                    headers: fields.headers,
                    idempotency_key: fields.idempotency_key,
                    created_at: fields.created_at,
                    available_at: fields.available_at,
                    max_attempts: fields.max_attempts,
                })
            })
            .collect()
    }

    /// Point-in-time snapshot of every queue and group the broker knows
    /// about, for dashboards and health checks.
    pub fn stats(&self) -> BrokerStats {
        let state = &self.stream.broker_state;
        let mut queues: Vec<QueueStats> = state
            .queues
            .iter()
            .map(|(queue, messages)| {
                let mut groups: Vec<GroupStats> = state
                    .groups
                    .get(queue)
                    .into_iter()
                    .flatten()
                    .map(|(group, group_state)| {
                        let lag = messages
                            .entries
                            .partition_point(|(sequence, _)| *sequence < group_state.next_sequence);
                        GroupStats {
                            group: group.clone(),
                            created_at: group_state.created_at,
                            last_delivered_sequence: group_state.next_sequence.checked_sub(1),
                            in_flight: group_state
                                .deliveries
                                .values()
                                .filter(|delivery| {
                                    matches!(delivery.status, DeliveryStatus::InFlight { .. })
                                })
                                .count(),
                            pending_redelivery: group_state
                                .deliveries
                                .values()
                                .filter(|delivery| {
                                    matches!(delivery.status, DeliveryStatus::Available { .. })
                                })
                                .count(),
                            settled_floor: group_state.settled_floor,
                            lag: messages.entries.len() - lag,
                            dead_letters: self
                                .stream
                                .stored_events()
                                .iter()
                                .filter(|stored| {
                                    stored.event.stream == dlq_stream(queue, group)
                                        && stored.event.event_type == DEAD_LETTER_MESSAGE_EVENT
                                })
                                .count(),
                        }
                    })
                    .collect();
                groups.sort_by(|a, b| a.group.cmp(&b.group));
                QueueStats {
                    queue: queue.clone(),
                    retained_messages: messages.entries.len(),
                    next_sequence: messages.next_sequence,
                    configured: state.configs.contains_key(queue),
                    groups,
                }
            })
            .collect();
        queues.sort_by(|a, b| a.queue.cmp(&b.queue));
        BrokerStats { queues }
    }

    /// Authorizes one access class against the queue's durable ACL.
    ///
    /// The caller states its PROVENANCE explicitly. This used to be inferred
    /// from `Option<&SecurityContext>`, where `None` was documented as a
    /// trusted caller — but that single value was carrying two unrelated
    /// facts: "which identity is this" and "did this arrive over the network".
    /// The embedded Rust API and an unauthenticated pgwire session both
    /// produced `None`, so every queue ACL was inert for ordinary SQL clients
    /// whenever the server had no configured security context, and an
    /// unidentified caller ended up with MORE authority than an identified one
    /// holding no roles. Provenance is now a separate, explicit input, and
    /// absence of an identity is never read as trust.
    ///
    /// A caller with a bypass policy always passes. Otherwise `Publish` and
    /// `Consume` pass when the queue declares no ACL for that class or when
    /// one of the caller's roles is granted; `Admin` is denied unless a role
    /// is explicitly granted via `admin_roles`, so an unprivileged caller can
    /// never bootstrap an ACL onto a queue it does not administer. A superuser
    /// SQL role passes as it does at every other gate in the engine — that is
    /// an authorization fact about a known identity, not a guess about where
    /// the call came from.
    pub fn authorize(
        &self,
        queue: &str,
        access: BrokerAccess,
        caller: BrokerCaller<'_>,
    ) -> Result<()> {
        let (context, superuser) = match caller {
            // The host process IS the trust boundary for the embedded API.
            BrokerCaller::TrustedHost => return Ok(()),
            BrokerCaller::Sql { context, superuser } => (context, superuser),
        };
        if context.is_some_and(|context| context.bypass_policy.is_some()) {
            return Ok(());
        }
        // A security context, when present, IS the broker identity, and the
        // SQL role's superuser status adds nothing to it. This matters: a
        // session opened with `new_secure` carries no SQL identity GUC, so its
        // effective role falls back to the bootstrap role and would otherwise
        // read as a superuser — turning every carrier session into a broker
        // superuser. Without a context the SQL role is the only identity there
        // is, and only a superuser is privileged; that is the path the
        // embedded host and the bootstrap role use to declare an ACL at all.
        if context.is_none() && superuser {
            return Ok(());
        }
        let config = self.stream.broker_state.configs.get(queue);
        let required = config.and_then(|config| match access {
            BrokerAccess::Publish => config.publish_roles.as_ref(),
            BrokerAccess::Consume => config.consume_roles.as_ref(),
            BrokerAccess::Admin => config.admin_roles.as_ref(),
        });
        let allowed = match (required, access) {
            // A declared ACL binds every SQL caller. An absent identity cannot
            // satisfy a role requirement — this is the case that was inert.
            (Some(roles), _) => {
                context.is_some_and(|context| roles.iter().any(|role| context.roles.contains(role)))
            }
            (None, BrokerAccess::Admin) => false,
            (None, _) => true,
        };
        if allowed {
            Ok(())
        } else {
            let who = context
                .map(|context| context.user_id.as_str())
                .unwrap_or("(no broker identity)");
            Err(BicDbError::Broker(format!(
                "permission denied: user `{who}` lacks {access:?} access to queue `{queue}`"
            )))
        }
    }

    /// Durably sets the queue's configuration (latest write wins).
    pub fn configure_queue(&mut self, queue: &str, config: QueueConfig) -> Result<()> {
        validate_broker_name("queue", queue)?;
        self.stream.append(Event::new(
            format!("{BROKER_STREAM_PREFIX}{queue}"),
            QUEUE_CONFIGURED_EVENT,
            json!({
                "queue": queue,
                "config": queue_config_to_json(&config),
                "at_ms": now_ms(),
            }),
        ))?;
        Ok(())
    }

    /// The queue's durable configuration, if one was ever set.
    pub fn queue_config(&self, queue: &str) -> Option<QueueConfig> {
        self.stream.broker_state.configs.get(queue).cloned()
    }

    /// Trims the queue's retained history and rewrites the segment.
    ///
    /// Always trims the prefix that **every** consumer group has settled
    /// (acked or dead-lettered). When the queue is configured with
    /// `retention_max_messages` / `retention_max_age_ms`, those bounds may
    /// trim further — including unsettled messages (explicit data-loss
    /// policy; affected in-flight deliveries are dropped and group cursors
    /// skip past). A queue with no groups is only trimmed by configured
    /// retention bounds. Sequences are never reused: a durable trim marker
    /// carries the floor across restarts.
    ///
    /// Note: idempotency keys of trimmed messages are forgotten (the dedup
    /// window equals the retention window). Dead-letter copies live in
    /// their own streams and are not affected; see
    /// [`Self::purge_dead_letters`].
    pub fn trim(&mut self, queue: &str) -> Result<TrimReport> {
        validate_broker_name("queue", queue)?;
        let now = now_ms();
        let state = &self.stream.broker_state;
        let Some(messages) = state.queues.get(queue) else {
            return Ok(TrimReport::default());
        };
        if messages.entries.is_empty() {
            return Ok(TrimReport::default());
        }

        // Settled bound: everything below the slowest group's floor.
        let settled_through = state
            .groups
            .get(queue)
            .filter(|groups| !groups.is_empty())
            .map(|groups| {
                groups
                    .values()
                    .map(|group| group.settled_floor)
                    .min()
                    .unwrap_or(0)
            })
            .and_then(|floor| floor.checked_sub(1));

        // Configured retention bounds (may exceed the settled bound).
        let config = state.configs.get(queue);
        let mut policy_through: Option<u64> = None;
        if let Some(max_messages) = config.and_then(|config| config.retention_max_messages) {
            let retained = messages.entries.len() as u64;
            if retained > max_messages {
                let cut = (retained - max_messages) as usize;
                let through = messages.entries[cut - 1].0;
                policy_through = Some(policy_through.map_or(through, |t| t.max(through)));
            }
        }
        if let Some(max_age_ms) = config.and_then(|config| config.retention_max_age_ms) {
            let cutoff = now.saturating_sub(max_age_ms);
            let mut through = None;
            for (sequence, message_id) in &messages.entries {
                let created_at = self
                    .stream
                    .event_by_id(message_id)
                    .map(|stored| message_fields(&stored.event).created_at)
                    .unwrap_or(i64::MAX);
                if created_at < cutoff {
                    through = Some(*sequence);
                } else {
                    break;
                }
            }
            if let Some(through) = through {
                policy_through = Some(policy_through.map_or(through, |t| t.max(through)));
            }
        }

        let Some(through) = settled_through.into_iter().chain(policy_through).max() else {
            return Ok(TrimReport::default());
        };
        let removed_ids: std::collections::HashSet<Uuid> = messages
            .entries
            .iter()
            .take_while(|(sequence, _)| *sequence <= through)
            .map(|(_, message_id)| *message_id)
            .collect();
        if removed_ids.is_empty() {
            return Ok(TrimReport::default());
        }

        // Build the surviving event list. The trim marker replaces the first
        // removed event so that on replay it applies BEFORE any surviving
        // message of this queue (removed messages form a sequence prefix, so
        // they all precede the survivors in the log).
        let queue_stream_name = queue_stream(queue);
        let group_prefix = format!("{BROKER_STREAM_PREFIX}{queue}/");
        let queue_level_stream = format!("{BROKER_STREAM_PREFIX}{queue}");
        let marker = Event::new(
            queue_level_stream.clone(),
            QUEUE_TRIMMED_EVENT,
            json!({
                "queue": queue,
                "through_sequence": through,
                "at_ms": now,
            }),
        );
        let mut removed_messages = 0usize;
        let mut removed_control = 0usize;
        let mut marker_placed = false;
        let (bytes_before, bytes_after) = self.stream.rewrite_events_in_place(|event| {
            if event.stream.starts_with(&group_prefix) {
                if retain_trimmed_control_event(event, &removed_ids) {
                    return true;
                }
                removed_control += 1;
                if !marker_placed {
                    *event = marker.clone();
                    marker_placed = true;
                    return true;
                }
                return false;
            }
            let drop = if event.stream == queue_stream_name
                && event.event_type == QUEUE_MESSAGE_EVENT
            {
                removed_ids.contains(&event.id)
            } else if event.stream == queue_level_stream && event.event_type == QUEUE_TRIMMED_EVENT
            {
                // Older markers with a lower-or-equal floor are superseded.
                event
                    .payload
                    .get("through_sequence")
                    .and_then(Value::as_u64)
                    .is_some_and(|old| old <= through)
            } else {
                false
            };
            if drop {
                if event.event_type == QUEUE_MESSAGE_EVENT {
                    removed_messages += 1;
                } else {
                    removed_control += 1;
                }
                if !marker_placed {
                    *event = marker.clone();
                    marker_placed = true;
                    true
                } else {
                    false
                }
            } else {
                true
            }
        })?;
        debug_assert!(marker_placed);
        Ok(TrimReport {
            removed_messages,
            removed_control_events: removed_control,
            through_sequence: Some(through),
            bytes_before,
            bytes_after,
        })
    }

    /// Trims every queue that has a durable retention configuration.
    /// Invoked automatically during database compaction; queues without a
    /// configuration are left untouched (their history is retained for
    /// future consumer groups unless [`Self::trim`] is called explicitly).
    pub fn trim_configured(&mut self) -> Result<Vec<(String, TrimReport)>> {
        let queues: Vec<String> = self
            .stream
            .broker_state
            .configs
            .iter()
            .filter(|(_, config)| {
                config.retention_max_messages.is_some() || config.retention_max_age_ms.is_some()
            })
            .map(|(queue, _)| queue.clone())
            .collect();
        let mut reports = Vec::new();
        for queue in queues {
            let report = self.trim(&queue)?;
            if report.removed_messages > 0 || report.removed_control_events > 0 {
                reports.push((queue, report));
            }
        }
        Ok(reports)
    }

    /// Whether an opportunistic publish-time trim should run: the queue has
    /// a message-count retention bound and has grown past twice that bound
    /// (hysteresis so the segment rewrite amortizes over many publishes).
    fn publish_trim_due(&self, queue: &str) -> bool {
        let state = &self.stream.broker_state;
        let Some(max_messages) = state
            .configs
            .get(queue)
            .and_then(|config| config.retention_max_messages)
        else {
            return false;
        };
        state
            .queues
            .get(queue)
            .is_some_and(|messages| messages.entries.len() as u64 > max_messages.saturating_mul(2))
    }

    /// Republishes up to `max` not-yet-redriven dead letters of the group
    /// back onto the queue as fresh messages (new ids, attempt counters
    /// reset, headers preserved). Each DLQ copy is redriven at most once;
    /// the copies stay in the DLQ stream for audit until purged.
    pub fn redrive_dead_letters(
        &mut self,
        queue: &str,
        group: &str,
        max: usize,
    ) -> Result<Vec<PublishReceipt>> {
        validate_broker_name("queue", queue)?;
        validate_broker_name("group", group)?;
        if max == 0 {
            return Ok(Vec::new());
        }
        let already = self
            .stream
            .broker_state
            .group(queue, group)
            .map(|state| state.redriven.clone())
            .unwrap_or_default();
        let candidates: Vec<(Uuid, Value, Value)> = self
            .stream
            .read(&dlq_stream(queue, group))
            .into_iter()
            .filter(|stored| stored.event.event_type == DEAD_LETTER_MESSAGE_EVENT)
            .filter(|stored| !already.contains(&stored.event.id))
            .take(max)
            .map(|stored| {
                let headers = broker_meta(&stored.event.metadata)
                    .and_then(|meta| meta.get("headers"))
                    .cloned()
                    .unwrap_or(Value::Null);
                (stored.event.id, stored.event.payload, headers)
            })
            .collect();
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let mut receipts = Vec::with_capacity(candidates.len());
        let mut redriven_ids = Vec::with_capacity(candidates.len());
        for (dlq_event_id, payload, headers) in candidates {
            receipts.push(self.publish_with(
                queue,
                payload,
                PublishOptions {
                    headers,
                    ..PublishOptions::default()
                },
            )?);
            redriven_ids.push(dlq_event_id.to_string());
        }
        self.append_control(
            queue,
            group,
            REDRIVEN_EVENT,
            json!({
                "queue": queue,
                "group": group,
                "dlq_event_ids": redriven_ids,
                "at_ms": now_ms(),
            }),
        )?;
        Ok(receipts)
    }

    /// Removes the group's dead-letter copies (and their redrive markers)
    /// from the log entirely, rewriting the segment. Returns how many dead
    /// letters were purged.
    pub fn purge_dead_letters(&mut self, queue: &str, group: &str) -> Result<usize> {
        validate_broker_name("queue", queue)?;
        validate_broker_name("group", group)?;
        let dlq = dlq_stream(queue, group);
        let control = control_stream(queue, group);
        let mut purged = 0usize;
        let mut survivors: Vec<Event> = Vec::with_capacity(self.stream.stored_events().len());
        for stored in self.stream.stored_events() {
            let event = &stored.event;
            if event.stream == dlq {
                if event.event_type == DEAD_LETTER_MESSAGE_EVENT {
                    purged += 1;
                }
                continue;
            }
            if event.stream == control && event.event_type == REDRIVEN_EVENT {
                continue;
            }
            survivors.push(event.clone());
        }
        if purged == 0 {
            return Ok(0);
        }
        self.stream.rewrite_events(survivors)?;
        Ok(purged)
    }

    fn ensure_group(&mut self, queue: &str, group: &str, now: i64) -> Result<()> {
        if self.stream.broker_state.group(queue, group).is_some() {
            return Ok(());
        }
        self.append_control(
            queue,
            group,
            GROUP_CREATED_EVENT,
            json!({
                "queue": queue,
                "group": group,
                "created_at_ms": now,
            }),
        )
    }

    fn check_in_flight(
        &self,
        queue: &str,
        group: &str,
        consumer_id: &str,
        message_id: Uuid,
    ) -> Result<()> {
        let delivery = self
            .stream
            .broker_state
            .group(queue, group)
            .and_then(|state| state.deliveries.get(&message_id))
            .ok_or_else(|| {
                BicDbError::Broker(format!(
                    "message {message_id} has no active delivery for group `{group}` on queue `{queue}`"
                ))
            })?;
        if !matches!(delivery.status, DeliveryStatus::InFlight { .. }) {
            return Err(BicDbError::Broker(format!(
                "message {message_id} is not in flight for group `{group}` on queue `{queue}`"
            )));
        }
        if delivery.consumer_id != consumer_id {
            return Err(BicDbError::Broker(format!(
                "message {message_id} is in flight for consumer `{}`, not `{consumer_id}`",
                delivery.consumer_id
            )));
        }
        Ok(())
    }

    /// Appends one control event; `EventStream::append_event` applies it to
    /// the in-memory projection, keeping live state and replay in lockstep.
    fn append_control(
        &mut self,
        queue: &str,
        group: &str,
        event_type: &str,
        payload: Value,
    ) -> Result<()> {
        self.stream.append(Event::new(
            control_stream(queue, group),
            event_type,
            payload,
        ))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn append_dead_letter(
        &mut self,
        queue: &str,
        group: &str,
        message_id: Uuid,
        sequence: u64,
        attempts: u32,
        reason: &str,
        last_error: Option<String>,
        now: i64,
    ) -> Result<()> {
        // Copy the message into the group's DLQ stream first, then record the
        // control transition; replay tolerates a crash in between (the DLQ
        // copy is idempotent by content, the delivery stays actionable until
        // the control event lands).
        if let Some(stored) = self.stream.event_by_id(&message_id) {
            let fields = message_fields(&stored.event);
            let payload = stored.event.payload.clone();
            let dead_letter_meta = json!({
                "original_message_id": message_id.to_string(),
                "queue": queue,
                "group": group,
                "sequence": sequence,
                "attempts": attempts,
                "reason": reason,
                "last_error": last_error,
                "headers": fields.headers,
                "dead_lettered_at_ms": now,
            });
            self.stream.append(
                Event::new(dlq_stream(queue, group), DEAD_LETTER_MESSAGE_EVENT, payload)
                    .with_metadata(json!({ "broker": dead_letter_meta })),
            )?;
        }
        self.append_control(
            queue,
            group,
            DEAD_LETTERED_EVENT,
            json!({
                "queue": queue,
                "group": group,
                "message_id": message_id.to_string(),
                "sequence": sequence,
                "attempts": attempts,
                "reason": reason,
                "at_ms": now,
            }),
        )
    }
}
