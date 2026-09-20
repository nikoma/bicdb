use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::broker::{Broker, BrokerState};
use crate::encryption::EncryptionRuntime;
use crate::error::{BicDbError, Result};
use crate::storage::{self, CompressionConfig, FrameKind, SegmentReadMode};

pub const DEFAULT_EVENTS_DIR: &str = "events";
pub const DEFAULT_EVENTS_SEGMENT: &str = "events.seg";
pub const RECORD_AUDIT_STREAM: &str = "bicdb.records";
/// Replicated pairwise clock-offset observations measured by live mesh
/// sessions. Because they replicate like any other events, every converged
/// replica derives the identical clock table — which is what lets the
/// conflict resolver use timing evidence without breaking determinism.
pub const CLOCK_OBSERVATION_STREAM: &str = "bicdb.clock_observations";
pub const SPATIAL_AUDIT_STREAM: &str = "bicdb.spatial";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub id: Uuid,
    pub stream: String,
    pub event_type: String,
    pub timestamp: i64,
    pub payload: Value,
    pub metadata: Value,
}

impl Event {
    pub fn new(stream: impl Into<String>, event_type: impl Into<String>, payload: Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            stream: stream.into(),
            event_type: event_type.into(),
            timestamp: unix_timestamp(),
            payload,
            metadata: Value::Object(Default::default()),
        }
    }

    pub fn with_id(mut self, id: Uuid) -> Self {
        self.id = id;
        self
    }

    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StoredEvent {
    pub offset: u64,
    pub event: Event,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionId(Uuid);

type SyncHandler = Arc<dyn Fn(&StoredEvent) + Send + Sync + 'static>;

struct Subscriber {
    id: SubscriptionId,
    filter: SubscriberFilter,
    kind: SubscriberKind,
}

enum SubscriberFilter {
    Exact(String),
    Prefix(String),
}

impl SubscriberFilter {
    fn matches(&self, stream: &str) -> bool {
        match self {
            Self::Exact(name) => stream == name,
            Self::Prefix(prefix) => stream.starts_with(prefix.as_str()),
        }
    }
}

enum SubscriberKind {
    Sync(SyncHandler),
    Async(mpsc::Sender<StoredEvent>),
}

pub trait EventProjection: Default {
    fn apply(&mut self, event: &StoredEvent) -> Result<()>;
}

pub struct EventStream {
    path: PathBuf,
    fsync: bool,
    read_mode: SegmentReadMode,
    compression: CompressionConfig,
    encryption: EncryptionRuntime,
    events: Vec<StoredEvent>,
    event_indexes: HashMap<Uuid, usize>,
    subscribers: Vec<Subscriber>,
    queue_offsets: HashMap<String, u64>,
    /// Durable-broker projection, rebuilt from the log at open and kept in
    /// lockstep by `append_event`. See the `broker` module.
    pub(crate) broker_state: BrokerState,
}

impl fmt::Debug for EventStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventStream")
            .field("path", &self.path)
            .field("fsync", &self.fsync)
            .field("read_mode", &self.read_mode)
            .field("compression", &self.compression)
            .field("encryption", &self.encryption)
            .field("events", &self.events.len())
            .field("subscribers", &self.subscribers.len())
            .field("queue_offsets", &self.queue_offsets)
            .finish()
    }
}

pub struct EventQueue<'a> {
    name: String,
    stream: &'a mut EventStream,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeviceView {
    latest_by_device: BTreeMap<String, Value>,
}

impl EventStream {
    pub(crate) fn events_iter(&self) -> impl Iterator<Item = &StoredEvent> {
        self.events.iter()
    }

    pub fn open(
        path: impl AsRef<Path>,
        fsync: bool,
        read_mode: SegmentReadMode,
        compression: CompressionConfig,
    ) -> Result<Self> {
        Self::open_with_encryption(
            path,
            fsync,
            read_mode,
            compression,
            EncryptionRuntime::disabled(),
        )
    }

    pub(crate) fn open_with_encryption(
        path: impl AsRef<Path>,
        fsync: bool,
        read_mode: SegmentReadMode,
        compression: CompressionConfig,
        encryption: EncryptionRuntime,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Stream the segment: each frame parses, applies, and drops before
        // the next is read. The previous recovery held the raw file AND every
        // decoded payload simultaneously — on a multi-gigabyte broker log
        // that peak alone outgrew the container before projections were even
        // built. (`read_mode` still selects the verification/import readers;
        // recovery itself is always the bounded streaming walk.)
        let _ = read_mode;
        let mut events = Vec::new();
        let mut event_indexes = HashMap::new();
        let mut broker_state = BrokerState::default();
        let _truncated_bytes =
            storage::stream_frames(&path, FrameKind::Event, &encryption, |offset, payload| {
                let event: Event = serde_json::from_slice(&payload)?;
                validate_event(&event)?;
                if event_indexes.contains_key(&event.id) {
                    return Ok(());
                }
                let index = events.len();
                event_indexes.insert(event.id, index);
                let stored = StoredEvent { offset, event };
                broker_state.apply(&stored);
                events.push(stored);
                Ok(())
            })?;
        Ok(Self {
            path,
            fsync,
            read_mode,
            compression,
            encryption,
            events,
            event_indexes,
            subscribers: Vec::new(),
            queue_offsets: HashMap::new(),
            broker_state,
        })
    }

    pub fn append(&mut self, event: Event) -> Result<u64> {
        let (stored, inserted) = self.append_event(event)?;
        if inserted {
            self.notify_subscribers(&stored);
        }
        Ok(stored.offset)
    }

    pub(crate) fn append_imported(&mut self, event: Event) -> Result<bool> {
        let (stored, inserted) = self.append_event(event)?;
        if inserted {
            self.notify_subscribers(&stored);
        }
        Ok(inserted)
    }

    pub fn read(&self, stream_name: &str) -> Vec<StoredEvent> {
        self.events
            .iter()
            .filter(|event| event.event.stream == stream_name)
            .cloned()
            .collect()
    }

    pub fn read_since(&self, offset: u64) -> Vec<StoredEvent> {
        self.events
            .iter()
            .filter(|event| event.offset >= offset)
            .cloned()
            .collect()
    }

    /// Highest offset in one stream without cloning the stream. Following a
    /// stream needs the head cheaply; `read(...).last()` clones every event
    /// to find it.
    pub fn stream_head_offset(&self, stream_name: &str) -> u64 {
        self.events
            .iter()
            .rev()
            .find(|event| event.event.stream == stream_name)
            .map(|event| event.offset)
            .unwrap_or(0)
    }

    pub fn read_stream_since(&self, stream_name: &str, offset: u64) -> Vec<StoredEvent> {
        self.events
            .iter()
            .filter(|event| event.event.stream == stream_name && event.offset >= offset)
            .cloned()
            .collect()
    }

    pub fn replay(&self, stream_name: &str) -> Vec<StoredEvent> {
        self.read(stream_name)
    }

    pub fn replay_from(&self, stream_name: &str, offset: u64) -> Vec<StoredEvent> {
        self.read_stream_since(stream_name, offset)
    }

    pub fn subscribe<F>(&mut self, stream_name: &str, handler: F) -> SubscriptionId
    where
        F: Fn(&StoredEvent) + Send + Sync + 'static,
    {
        let id = SubscriptionId(Uuid::new_v4());
        self.subscribers.push(Subscriber {
            id,
            filter: SubscriberFilter::Exact(stream_name.to_string()),
            kind: SubscriberKind::Sync(Arc::new(handler)),
        });
        id
    }

    /// Subscribes a synchronous handler to every stream whose name starts
    /// with `prefix` (e.g. `queue:` to observe all broker queue publishes).
    /// The handler runs inline on the appending thread; keep it cheap.
    pub fn subscribe_prefix<F>(&mut self, prefix: &str, handler: F) -> SubscriptionId
    where
        F: Fn(&StoredEvent) + Send + Sync + 'static,
    {
        let id = SubscriptionId(Uuid::new_v4());
        self.subscribers.push(Subscriber {
            id,
            filter: SubscriberFilter::Prefix(prefix.to_string()),
            kind: SubscriberKind::Sync(Arc::new(handler)),
        });
        id
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn subscribe_async<F>(&mut self, stream_name: &str, handler: F) -> SubscriptionId
    where
        F: Fn(StoredEvent) + Send + 'static,
    {
        let id = SubscriptionId(Uuid::new_v4());
        let (sender, receiver) = mpsc::channel::<StoredEvent>();
        thread::spawn(move || {
            for event in receiver {
                handler(event);
            }
        });
        self.subscribers.push(Subscriber {
            id,
            filter: SubscriberFilter::Exact(stream_name.to_string()),
            kind: SubscriberKind::Async(sender),
        });
        id
    }

    /// wasm has no background threads; "async" delivery degrades to inline
    /// delivery on the appending thread, preserving the at-least-once contract.
    #[cfg(target_arch = "wasm32")]
    pub fn subscribe_async<F>(&mut self, stream_name: &str, handler: F) -> SubscriptionId
    where
        F: Fn(StoredEvent) + Send + Sync + 'static,
    {
        let id = SubscriptionId(Uuid::new_v4());
        self.subscribers.push(Subscriber {
            id,
            filter: SubscriberFilter::Exact(stream_name.to_string()),
            kind: SubscriberKind::Sync(Arc::new(move |event: &StoredEvent| handler(event.clone()))),
        });
        id
    }

    pub fn rebuild_projection<P>(&self, stream_name: &str) -> Result<P>
    where
        P: EventProjection,
    {
        self.rebuild_projection_from(stream_name, 0)
    }

    pub fn rebuild_projection_from<P>(&self, stream_name: &str, offset: u64) -> Result<P>
    where
        P: EventProjection,
    {
        let mut projection = P::default();
        for event in self.read_stream_since(stream_name, offset) {
            projection.apply(&event)?;
        }
        Ok(projection)
    }

    pub fn rebuild_projection_at<P>(&self, stream_name: &str, timestamp: i64) -> Result<P>
    where
        P: EventProjection,
    {
        let mut projection = P::default();
        for event in self.read(stream_name) {
            if event.event.timestamp <= timestamp {
                projection.apply(&event)?;
            }
        }
        Ok(projection)
    }

    pub fn queue(&mut self, name: &str) -> EventQueue<'_> {
        EventQueue {
            name: name.to_string(),
            stream: self,
        }
    }

    /// Durable-broker facade: consumer groups, ack/nack, redelivery, DLQ.
    pub fn broker(&mut self) -> Broker<'_> {
        Broker::new(self)
    }

    /// Whether an event id is already in the log — the spoof-proof "is this
    /// an echo?" test for sync direction policies (a client re-pushing
    /// server-authored events after compaction carries only ids the server
    /// already has; forged events cannot).
    pub fn contains_event(&self, id: &Uuid) -> bool {
        self.event_indexes.contains_key(id)
    }

    pub(crate) fn event_by_id(&self, id: &Uuid) -> Option<&StoredEvent> {
        self.event_indexes
            .get(id)
            .and_then(|index| self.events.get(*index))
    }

    pub(crate) fn stored_events(&self) -> &[StoredEvent] {
        &self.events
    }

    /// Rewrites the segment to contain exactly `events` (in order), then
    /// rebuilds the in-memory indexes and broker projection from the new
    /// contents. Used by broker trimming/purging; the rewrite goes through
    /// the same tmp-file + rename path as compaction, so a crash leaves
    /// either the old or the new segment intact. Returns (bytes_before,
    /// bytes_after).
    pub(crate) fn rewrite_events(&mut self, events: Vec<Event>) -> Result<(u64, u64)> {
        let bytes_before = match fs::metadata(&self.path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        let offsets = crate::db::rewrite_frame_file_iter(
            &self.path,
            FrameKind::Event,
            events
                .iter()
                .map(|event| serde_json::to_vec(event).map_err(Into::into)),
            self.fsync,
            &self.compression,
            &self.encryption,
        )?;
        self.events.clear();
        self.event_indexes.clear();
        for (event, offset) in events.into_iter().zip(offsets) {
            let stored = StoredEvent { offset, event };
            self.event_indexes
                .insert(stored.event.id, self.events.len());
            self.events.push(stored);
        }
        self.broker_state = BrokerState::rebuild(&self.events);
        let bytes_after = match fs::metadata(&self.path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        Ok((bytes_before, bytes_after))
    }

    /// Rewrites the segment after mutating/filtering the resident events in
    /// place. This keeps large payload allocations owned by their surviving
    /// events instead of cloning the whole log before the rewrite.
    pub(crate) fn rewrite_events_in_place(
        &mut self,
        mut retain: impl FnMut(&mut Event) -> bool,
    ) -> Result<(u64, u64)> {
        self.events.retain_mut(|stored| retain(&mut stored.event));
        let result = self.compact();
        match result {
            Ok(report) => {
                self.broker_state = BrokerState::rebuild(&self.events);
                Ok(report)
            }
            Err(error) => {
                // The atomic rewrite can fail before or after its rename.
                // Re-read whichever complete segment is on disk so the
                // in-memory stream never remains partially filtered.
                if let Ok(recovered) = Self::open_with_encryption(
                    &self.path,
                    self.fsync,
                    self.read_mode,
                    self.compression.clone(),
                    self.encryption.clone(),
                ) {
                    self.events = recovered.events;
                    self.event_indexes = recovered.event_indexes;
                    self.broker_state = recovered.broker_state;
                }
                Err(error)
            }
        }
    }

    pub fn flush(&self) -> Result<()> {
        storage::sync_file(&self.path)
    }

    pub(crate) fn compact(&mut self) -> Result<(u64, u64)> {
        let bytes_before = match fs::metadata(&self.path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };

        // One serialized payload in flight at a time: the previous rewrite
        // cloned every event AND held every serialized payload while the new
        // file was written — three copies of the log.
        let offsets = crate::db::rewrite_frame_file_iter(
            &self.path,
            FrameKind::Event,
            self.events
                .iter()
                .map(|stored| serde_json::to_vec(&stored.event).map_err(Into::into)),
            self.fsync,
            &self.compression,
            &self.encryption,
        )?;
        let events = std::mem::take(&mut self.events);
        self.event_indexes.clear();
        for (stored, offset) in events.into_iter().zip(offsets) {
            let stored = StoredEvent {
                offset,
                event: stored.event,
            };
            self.event_indexes
                .insert(stored.event.id, self.events.len());
            self.events.push(stored);
        }

        let bytes_after = match fs::metadata(&self.path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        Ok((bytes_before, bytes_after))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read_mode(&self) -> SegmentReadMode {
        self.read_mode
    }

    fn append_event(&mut self, event: Event) -> Result<(StoredEvent, bool)> {
        validate_event(&event)?;
        if let Some(index) = self.event_indexes.get(&event.id).copied() {
            return Ok((self.events[index].clone(), false));
        }

        let payload = serde_json::to_vec(&event)?;
        let offsets = storage::append_frames(
            &self.path,
            FrameKind::Event,
            &[payload],
            self.fsync,
            &self.compression,
            &self.encryption,
        )?;
        let stored = StoredEvent {
            offset: offsets[0],
            event,
        };
        self.event_indexes
            .insert(stored.event.id, self.events.len());
        self.events.push(stored.clone());
        self.broker_state.apply(&stored);
        Ok((stored, true))
    }

    fn notify_subscribers(&mut self, event: &StoredEvent) {
        let mut closed = Vec::new();
        for subscriber in &self.subscribers {
            if !subscriber.filter.matches(&event.event.stream) {
                continue;
            }

            match &subscriber.kind {
                SubscriberKind::Sync(handler) => handler(event),
                SubscriberKind::Async(sender) => {
                    if sender.send(event.clone()).is_err() {
                        closed.push(subscriber.id);
                    }
                }
            }
        }

        if !closed.is_empty() {
            self.subscribers
                .retain(|subscriber| !closed.contains(&subscriber.id));
        }
    }

    fn consume_queue(&mut self, queue: &str, max_messages: usize) -> Vec<StoredEvent> {
        if max_messages == 0 {
            return Vec::new();
        }

        let stream = queue_stream(queue);
        let next_offset = self.queue_offsets.get(queue).copied().unwrap_or(0);
        let messages = self
            .events
            .iter()
            .filter(|event| {
                event.event.stream == stream
                    && event.event.event_type == "QueueMessage"
                    && event.offset >= next_offset
            })
            .take(max_messages)
            .cloned()
            .collect::<Vec<_>>();

        if let Some(last) = messages.last() {
            self.queue_offsets
                .insert(queue.to_string(), last.offset.saturating_add(1));
        }

        messages
    }
}

impl EventQueue<'_> {
    pub fn publish(&mut self, payload: Value, metadata: Value) -> Result<u64> {
        self.stream.append(
            Event::new(queue_stream(&self.name), "QueueMessage", payload).with_metadata(metadata),
        )
    }

    pub fn consume(&mut self, max_messages: usize) -> Result<Vec<StoredEvent>> {
        Ok(self.stream.consume_queue(&self.name, max_messages))
    }
}

impl DeviceView {
    pub fn latest(&self, device_id: &str) -> Option<&Value> {
        self.latest_by_device.get(device_id)
    }

    pub fn len(&self) -> usize {
        self.latest_by_device.len()
    }

    pub fn is_empty(&self) -> bool {
        self.latest_by_device.is_empty()
    }
}

impl EventProjection for DeviceView {
    fn apply(&mut self, event: &StoredEvent) -> Result<()> {
        if event.event.event_type == "DeviceMeasurementReceived" {
            upsert_payload(
                &mut self.latest_by_device,
                &event.event.payload,
                "device_id",
            )?;
        }
        Ok(())
    }
}

fn validate_event(event: &Event) -> Result<()> {
    validate_stream_name(&event.stream)?;
    if event.event_type.trim().is_empty() {
        return Err(BicDbError::InvalidEvent(
            "event_type must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_stream_name(stream: &str) -> Result<()> {
    if stream.is_empty()
        || !stream.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || byte == b'_'
                || byte == b'-'
                || byte == b'.'
                || byte == b':'
                || byte == b'/'
        })
    {
        return Err(BicDbError::InvalidStreamName(stream.to_string()));
    }
    Ok(())
}

pub(crate) fn queue_stream(queue: &str) -> String {
    format!("queue:{queue}")
}

fn upsert_payload(
    values: &mut BTreeMap<String, Value>,
    payload: &Value,
    primary_key: &str,
) -> Result<()> {
    let id = payload
        .get(primary_key)
        .or_else(|| payload.get("id"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            BicDbError::ProjectionError(format!("event payload is missing `{primary_key}`"))
        })?
        .to_string();

    let Some(existing) = values.get_mut(&id) else {
        values.insert(id, payload.clone());
        return Ok(());
    };

    merge_object(existing, payload);
    Ok(())
}

fn merge_object(existing: &mut Value, patch: &Value) {
    let (Some(existing), Some(patch)) = (existing.as_object_mut(), patch.as_object()) else {
        *existing = patch.clone();
        return;
    };

    for (key, value) in patch {
        existing.insert(key.clone(), value.clone());
    }
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_place_rewrite_keeps_survivor_payload_allocation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(DEFAULT_EVENTS_SEGMENT);
        let mut stream = EventStream::open(
            path,
            false,
            SegmentReadMode::Buffered,
            CompressionConfig::disabled(),
        )
        .unwrap();
        stream
            .append(Event::new("test", "drop", Value::Null))
            .unwrap();
        stream
            .append(Event::new(
                "test",
                "keep",
                serde_json::json!({"body": "x".repeat(1_000_000)}),
            ))
            .unwrap();

        let payload_before = stream.events[1].event.payload["body"]
            .as_str()
            .unwrap()
            .as_ptr();
        stream
            .rewrite_events_in_place(|event| event.event_type != "drop")
            .unwrap();
        let payload_after = stream.events[0].event.payload["body"]
            .as_str()
            .unwrap()
            .as_ptr();

        assert_eq!(payload_after, payload_before);
    }
}
