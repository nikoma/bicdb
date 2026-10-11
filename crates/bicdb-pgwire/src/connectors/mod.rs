//! Connectors of `bicdb serve --connectors FILE`: move messages between the
//! database and external messaging systems.
//!
//! - A **sink** drains a broker queue to an external system (a Redis stream,
//!   list or channel, or an AMQP exchange). It consumes the queue as its own
//!   consumer group and acknowledges a message only after the external system
//!   confirmed it, so delivery is at-least-once; failures are retried with a
//!   delay and dead-lettered by the broker after the message's attempt limit.
//!   Together with `broker_publish_on_commit` in a trigger this is a
//!   transactional outbox: a row change becomes an external message if and
//!   only if its transaction commits.
//! - A **source** drains an external queue (a Redis stream consumer group, or
//!   an AMQP queue) into the database: messages are written in batches, one
//!   transaction per batch, into a table, through a function, or onto a
//!   broker queue, and acknowledged upstream only after the batch committed
//!   (and, with synchronous replication, after the standbys confirmed it). A
//!   bursty writer can fill the external queue while the database drains it
//!   at its own pace (`max_per_second`).
//!
//! Connectors run only while this node is the primary.
use super::*;
use bicdb_core::{BrokerMessage, ConsumeOptions, NackOptions, PublishOptions};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Instant;

mod amqp;
mod redis;

// ------------------------------------------------------------------ config

/// The contents of a connectors file.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConnectorsConfig {
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SinkConfig {
    pub name: String,
    /// Broker queue to drain.
    pub queue: String,
    pub target: SinkTarget,
    /// Messages taken from the queue per round.
    #[serde(default = "default_sink_batch")]
    pub batch: usize,
    /// Delay before a failed delivery is retried.
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SinkTarget {
    Redis(RedisEndpoint),
    Amqp(AmqpPublishTarget),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RedisEndpoint {
    /// `redis://` or `rediss://` URL.
    #[serde(default)]
    pub url: Option<String>,
    /// Environment variable holding the URL (keeps credentials out of the file).
    #[serde(default)]
    pub url_env: Option<String>,
    /// Stream key: sinks append with XADD, sources read with a consumer group.
    #[serde(default)]
    pub stream: Option<String>,
    /// Sink only: PUBLISH to this channel.
    #[serde(default)]
    pub channel: Option<String>,
    /// Sink only: LPUSH onto this list.
    #[serde(default)]
    pub list: Option<String>,
    /// Sink with a stream: approximate MAXLEN trim.
    #[serde(default)]
    pub maxlen: Option<u64>,
    /// Source only: consumer group (created if missing; default "bicdb").
    #[serde(default)]
    pub group: Option<String>,
    /// Source only: where a new consumer group starts: "0" (everything in
    /// the stream, the default) or "$" (only new entries).
    #[serde(default)]
    pub start: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AmqpPublishTarget {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub url_env: Option<String>,
    /// Exchange to publish to ("" is the default exchange).
    #[serde(default)]
    pub exchange: String,
    /// Routing key (with the default exchange: the queue name).
    #[serde(default)]
    pub routing_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AmqpConsumeSource {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub url_env: Option<String>,
    /// Queue to consume (must exist).
    pub queue: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub name: String,
    pub from: SourceOrigin,
    pub into: SourceDestination,
    /// Messages written per transaction.
    #[serde(default = "default_source_batch")]
    pub batch: usize,
    /// Longest wait for a batch to fill before writing what arrived.
    #[serde(default = "default_flush_ms")]
    pub flush_ms: u64,
    /// Upper bound on messages written per second (0 = no bound).
    #[serde(default)]
    pub max_per_second: u64,
    /// Delay before retrying after a failed write or connection loss.
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceOrigin {
    Redis(RedisEndpoint),
    Amqp(AmqpConsumeSource),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceDestination {
    /// Rows into a table: message fields map to columns by name.
    Table {
        table: String,
        /// Column that receives the message key (Redis entry id, AMQP
        /// message id) when the message does not set it.
        #[serde(default)]
        key: Option<String>,
        /// Column that receives the whole message as JSON.
        #[serde(default)]
        payload_column: Option<String>,
        /// "ignore" (default) skips a row whose key already exists;
        /// "update" overwrites it (needs `key`).
        #[serde(default)]
        on_conflict: Option<String>,
    },
    /// `SELECT function(message::jsonb)` per message.
    Function { function: String },
    /// Onto a broker queue (idempotent by message key).
    Queue { queue: String },
}

fn default_sink_batch() -> usize {
    100
}
fn default_source_batch() -> usize {
    500
}
fn default_flush_ms() -> u64 {
    200
}
fn default_retry_delay_ms() -> u64 {
    1_000
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn resolve_url(url: &Option<String>, url_env: &Option<String>, what: &str) -> Result<String> {
    match (url, url_env) {
        (Some(url), None) => Ok(url.clone()),
        (None, Some(name)) => std::env::var(name).map_err(|_| {
            PgWireError::Server(format!("{what}: environment variable {name} is not set"))
        }),
        _ => Err(PgWireError::Server(format!(
            "{what}: set exactly one of url and url_env"
        ))),
    }
}

impl ConnectorsConfig {
    /// Reads and validates a connectors file.
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let config: Self = serde_json::from_str(&text).map_err(|error| {
            PgWireError::Server(format!("connectors file {}: {error}", path.display()))
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        let mut names = BTreeSet::new();
        let fail = |message: String| Err(PgWireError::Server(message));
        for sink in &self.sinks {
            if !valid_name(&sink.name) || !names.insert(sink.name.clone()) {
                return fail(format!(
                    "connector name `{}` is invalid or repeated",
                    sink.name
                ));
            }
            if sink.batch == 0 {
                return fail(format!("sink {}: batch must be at least 1", sink.name));
            }
            match &sink.target {
                SinkTarget::Redis(redis) => {
                    resolve_url(&redis.url, &redis.url_env, &sink.name)?;
                    let targets = [&redis.stream, &redis.channel, &redis.list]
                        .iter()
                        .filter(|target| target.is_some())
                        .count();
                    if targets != 1 {
                        return fail(format!(
                            "sink {}: set exactly one of stream, channel and list",
                            sink.name
                        ));
                    }
                    if redis.group.is_some() || redis.start.is_some() {
                        return fail(format!(
                            "sink {}: group and start apply to sources only",
                            sink.name
                        ));
                    }
                }
                SinkTarget::Amqp(amqp) => {
                    resolve_url(&amqp.url, &amqp.url_env, &sink.name)?;
                }
            }
        }
        for source in &self.sources {
            if !valid_name(&source.name) || !names.insert(source.name.clone()) {
                return fail(format!(
                    "connector name `{}` is invalid or repeated",
                    source.name
                ));
            }
            if source.batch == 0 {
                return fail(format!("source {}: batch must be at least 1", source.name));
            }
            match &source.from {
                SourceOrigin::Redis(redis) => {
                    resolve_url(&redis.url, &redis.url_env, &source.name)?;
                    if redis.stream.is_none() || redis.channel.is_some() || redis.list.is_some() {
                        return fail(format!(
                            "source {}: Redis sources read a stream (set stream)",
                            source.name
                        ));
                    }
                    if let Some(start) = &redis.start {
                        if start != "0" && start != "$" {
                            return fail(format!(
                                "source {}: start is \"0\" or \"$\"",
                                source.name
                            ));
                        }
                    }
                }
                SourceOrigin::Amqp(amqp) => {
                    resolve_url(&amqp.url, &amqp.url_env, &source.name)?;
                }
            }
            if let SourceDestination::Table {
                key, on_conflict, ..
            } = &source.into
            {
                match on_conflict.as_deref() {
                    None | Some("ignore") => {}
                    Some("update") if key.is_some() => {}
                    Some("update") => {
                        return fail(format!(
                            "source {}: on_conflict \"update\" needs key",
                            source.name
                        ))
                    }
                    Some(other) => {
                        return fail(format!(
                            "source {}: on_conflict is \"ignore\" or \"update\", not {other}",
                            source.name
                        ))
                    }
                }
            }
        }
        Ok(())
    }
}

// ------------------------------------------------------------------ status

/// What one connector has done, for `bicdb_connectors` and metrics.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ConnectorStatus {
    pub name: String,
    /// "sink" or "source".
    pub kind: String,
    /// "redis" or "amqp".
    pub system: String,
    /// "starting", "running", "idle", "standby", "retrying".
    pub state: String,
    pub messages: u64,
    pub failures: u64,
    pub last_error: Option<String>,
    /// Unix milliseconds of the last message moved.
    pub last_message_at: Option<i64>,
}

pub(crate) type ConnectorRegistry = Arc<Mutex<BTreeMap<String, ConnectorStatus>>>;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

#[derive(Clone)]
struct StatusHandle {
    registry: ConnectorRegistry,
    name: String,
}

impl StatusHandle {
    fn new(registry: &ConnectorRegistry, name: &str, kind: &str, system: &str) -> Self {
        registry.lock().unwrap().insert(
            name.to_string(),
            ConnectorStatus {
                name: name.to_string(),
                kind: kind.to_string(),
                system: system.to_string(),
                state: "starting".to_string(),
                ..ConnectorStatus::default()
            },
        );
        Self {
            registry: Arc::clone(registry),
            name: name.to_string(),
        }
    }

    fn update(&self, change: impl FnOnce(&mut ConnectorStatus)) {
        if let Some(status) = self.registry.lock().unwrap().get_mut(&self.name) {
            change(status);
        }
    }

    fn state(&self, state: &str) {
        self.update(|status| {
            if status.state != state {
                status.state = state.to_string();
            }
        });
    }

    fn moved(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.update(|status| {
            status.messages += count as u64;
            status.last_message_at = Some(now_ms());
            status.state = "running".to_string();
        });
    }

    fn failed(&self, error: &str) {
        let mut log = false;
        self.update(|status| {
            status.failures += 1;
            log = status.last_error.as_deref() != Some(error);
            status.last_error = Some(error.to_string());
            status.state = "retrying".to_string();
        });
        if log {
            eprintln!("bicdb connector {}: {error}", self.name);
        }
    }
}

/// Status of every connector of `server`.
pub(crate) fn statuses(server: &PgWireServer) -> Vec<ConnectorStatus> {
    server
        .connectors
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect()
}

// ------------------------------------------------------------------ start

/// Starts the configured connectors of `server`.
pub(crate) fn start(server: Arc<PgWireServer>) -> Result<()> {
    let Some(config) = server.config.connectors.clone() else {
        return Ok(());
    };
    // In cluster mode every database starts its background tasks; the
    // connectors belong to the default database and run once.
    if let Some(cluster) = server.cluster.as_ref().and_then(std::sync::Weak::upgrade) {
        let default_directory = {
            let manifest = cluster.manifest.lock().unwrap();
            manifest
                .databases
                .get(&manifest.default_database)
                .map(|entry| entry.directory.clone())
        };
        let is_default = default_directory
            .is_some_and(|directory| server.path == cluster.root.join("databases").join(directory));
        if !is_default {
            return Ok(());
        }
    }
    for sink in config.sinks {
        let target: Box<dyn SinkTransport> = match &sink.target {
            SinkTarget::Redis(redis) => Box::new(redis::RedisSink::new(redis, &sink.name)?),
            SinkTarget::Amqp(amqp) => Box::new(amqp::AmqpSink::new(amqp, &sink.name)?),
        };
        let status = StatusHandle::new(&server.connectors, &sink.name, "sink", target.system());
        let server = server.clone();
        std::thread::Builder::new()
            .name(format!("bicdb-sink-{}", sink.name))
            .spawn(move || run_sink(server, sink, target, status))?;
    }
    for source in config.sources {
        let reader: Box<dyn SourceTransport> = match &source.from {
            SourceOrigin::Redis(redis) => Box::new(redis::RedisSource::new(redis, &source.name)?),
            SourceOrigin::Amqp(amqp) => Box::new(amqp::AmqpSource::new(amqp, &source.name)?),
        };
        let status = StatusHandle::new(&server.connectors, &source.name, "source", reader.system());
        let server = server.clone();
        std::thread::Builder::new()
            .name(format!("bicdb-source-{}", source.name))
            .spawn(move || run_source(server, source, reader, status))?;
    }
    Ok(())
}

/// Whether this node may move messages: connectors run on the primary only.
fn may_run(server: &Arc<PgWireServer>) -> bool {
    server
        .read_db()
        .is_ok_and(|db| !db.is_standby_role() && !db.is_read_only_standby())
}

fn sleep_unless_shutdown(server: &Arc<PgWireServer>, duration: Duration) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline && !server.is_shutdown_requested() {
        std::thread::sleep(Duration::from_millis(50).min(duration));
    }
}

// ------------------------------------------------------------------ sinks

/// The message a sink delivers.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Outgoing {
    /// Stable across redeliveries: the broker message id.
    pub id: String,
    pub queue: String,
    pub sequence: u64,
    pub payload: Value,
    pub headers: Value,
    pub created_at: i64,
}

impl Outgoing {
    fn from_broker(message: &BrokerMessage) -> Self {
        Self {
            id: message.message_id.to_string(),
            queue: message.queue.clone(),
            sequence: message.sequence,
            payload: message.payload.clone(),
            headers: message.headers.clone(),
            created_at: message.created_at,
        }
    }

    /// The JSON envelope used where the target has no fields of its own.
    pub(crate) fn envelope(&self) -> Value {
        serde_json::json!({
            "id": self.id,
            "queue": self.queue,
            "sequence": self.sequence,
            "payload": self.payload,
            "headers": self.headers,
            "created_at": self.created_at,
        })
    }
}

/// An external system messages are delivered to.
pub(crate) trait SinkTransport: Send {
    fn system(&self) -> &'static str;
    /// Delivers one message; returns once the external system confirmed it.
    fn deliver(&mut self, message: &Outgoing) -> std::result::Result<(), String>;
    /// Drops the connection after a failure (reconnects on next use).
    fn reset(&mut self);
}

fn run_sink(
    server: Arc<PgWireServer>,
    config: SinkConfig,
    mut target: Box<dyn SinkTransport>,
    status: StatusHandle,
) {
    let group = format!("connector-{}", config.name);
    let consumer = format!("connector-{}", config.name);
    let mut idle = Duration::from_millis(50);
    while !server.is_shutdown_requested() {
        if !may_run(&server) {
            status.state("standby");
            sleep_unless_shutdown(&server, Duration::from_secs(1));
            continue;
        }
        let consumed = server
            .read_db()
            .map_err(|error| error.to_string())
            .and_then(|db| {
                db.with_broker(|broker| {
                    broker.consume(
                        &config.queue,
                        &group,
                        &consumer,
                        ConsumeOptions {
                            max_messages: config.batch,
                            visibility_timeout_ms: 60_000,
                        },
                    )
                })
                .map_err(|error| error.to_string())
            });
        let messages = match consumed {
            Ok(messages) => messages,
            Err(error) => {
                status.failed(&error);
                sleep_unless_shutdown(&server, Duration::from_millis(config.retry_delay_ms));
                continue;
            }
        };
        if messages.is_empty() {
            status.state("idle");
            sleep_unless_shutdown(&server, idle);
            idle = (idle * 2).min(Duration::from_millis(500));
            continue;
        }
        idle = Duration::from_millis(50);
        let mut delivered = 0;
        let mut failure = None;
        for message in &messages {
            let settle = match &failure {
                None => match target.deliver(&Outgoing::from_broker(message)) {
                    Ok(()) => {
                        delivered += 1;
                        Ok(())
                    }
                    Err(error) => {
                        target.reset();
                        failure = Some(error.clone());
                        Err(error)
                    }
                },
                // After one failure the rest of the batch is retried later,
                // in order.
                Some(error) => Err(error.clone()),
            };
            let settled = server
                .read_db()
                .map_err(|error| error.to_string())
                .and_then(|db| {
                    db.with_broker(|broker| match &settle {
                        Ok(()) => broker.ack(&config.queue, &group, &consumer, message.message_id),
                        Err(error) => broker.nack(
                            &config.queue,
                            &group,
                            &consumer,
                            message.message_id,
                            NackOptions {
                                requeue: true,
                                delay_ms: Some(config.retry_delay_ms),
                                error: Some(error.clone()),
                            },
                        ),
                    })
                    .map_err(|error| error.to_string())
                });
            if let Err(error) = settled {
                status.failed(&format!("settling a delivery: {error}"));
            }
        }
        status.moved(delivered);
        if let Some(error) = failure {
            status.failed(&error);
            sleep_unless_shutdown(&server, Duration::from_millis(config.retry_delay_ms));
        }
    }
}

// ------------------------------------------------------------------ sources

/// A message read from an external queue.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Incoming {
    /// Stable identity when the system provides one (Redis entry id, AMQP
    /// message id), used for idempotent writes.
    pub key: Option<String>,
    /// The message as a JSON object.
    pub body: Map<String, Value>,
    /// Transport-specific acknowledgement token.
    pub token: String,
}

/// An external system messages are read from.
pub(crate) trait SourceTransport: Send {
    fn system(&self) -> &'static str;
    /// Up to `max` messages, waiting at most `wait` for the first.
    fn fetch(&mut self, max: usize, wait: Duration) -> std::result::Result<Vec<Incoming>, String>;
    /// Acknowledges messages durably written.
    fn ack(&mut self, messages: &[Incoming]) -> std::result::Result<(), String>;
    /// Returns unwritten messages for redelivery and drops the connection.
    fn reset(&mut self, unwritten: &[Incoming]);
}

/// Bounds the rate messages are written at.
struct RateLimit {
    per_second: u64,
    window_start: Instant,
    in_window: u64,
}

impl RateLimit {
    fn new(per_second: u64) -> Self {
        Self {
            per_second,
            window_start: Instant::now(),
            in_window: 0,
        }
    }

    /// How many messages may be written now (blocking until at least one).
    fn allowance(&mut self, server: &Arc<PgWireServer>, want: usize) -> usize {
        if self.per_second == 0 {
            return want;
        }
        loop {
            if self.window_start.elapsed() >= Duration::from_secs(1) {
                self.window_start = Instant::now();
                self.in_window = 0;
            }
            let left = self.per_second.saturating_sub(self.in_window);
            if left > 0 {
                let granted = (want as u64).min(left) as usize;
                self.in_window += granted as u64;
                return granted;
            }
            let rest = Duration::from_secs(1).saturating_sub(self.window_start.elapsed());
            sleep_unless_shutdown(server, rest);
            if server.is_shutdown_requested() {
                return 0;
            }
        }
    }
}

fn run_source(
    server: Arc<PgWireServer>,
    config: SourceConfig,
    mut reader: Box<dyn SourceTransport>,
    status: StatusHandle,
) {
    let mut writer = BatchWriter::new(&config);
    let mut limit = RateLimit::new(config.max_per_second);
    let mut pending: VecDeque<Incoming> = VecDeque::new();
    while !server.is_shutdown_requested() {
        if !may_run(&server) {
            if !pending.is_empty() {
                reader.reset(&pending.drain(..).collect::<Vec<_>>());
            }
            status.state("standby");
            sleep_unless_shutdown(&server, Duration::from_secs(1));
            continue;
        }
        // Fill a batch, waiting at most flush_ms for it.
        let deadline = Instant::now() + Duration::from_millis(config.flush_ms);
        let mut failed = None;
        while pending.len() < config.batch && !server.is_shutdown_requested() {
            let wait = deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() && !pending.is_empty() {
                break;
            }
            match reader.fetch(
                config.batch - pending.len(),
                wait.max(Duration::from_millis(10)),
            ) {
                Ok(messages) if messages.is_empty() => {
                    if Instant::now() >= deadline {
                        break;
                    }
                }
                Ok(messages) => pending.extend(messages),
                Err(error) => {
                    failed = Some(error);
                    break;
                }
            }
        }
        if let Some(error) = failed {
            status.failed(&error);
            reader.reset(&pending.drain(..).collect::<Vec<_>>());
            sleep_unless_shutdown(&server, Duration::from_millis(config.retry_delay_ms));
            continue;
        }
        if pending.is_empty() {
            status.state("idle");
            continue;
        }
        let allowed = limit.allowance(&server, pending.len());
        if allowed == 0 {
            continue;
        }
        let batch = pending.drain(..allowed).collect::<Vec<_>>();
        match writer.write(&server, &batch) {
            Ok(()) => match reader.ack(&batch) {
                Ok(()) => status.moved(batch.len()),
                Err(error) => {
                    // Written but not acknowledged: redelivered, and the
                    // idempotent write absorbs the repeat.
                    status.failed(&format!("acknowledging: {error}"));
                    let mut unwritten = batch;
                    unwritten.extend(pending.drain(..));
                    reader.reset(&unwritten);
                    sleep_unless_shutdown(&server, Duration::from_millis(config.retry_delay_ms));
                }
            },
            Err(error) => {
                status.failed(&error);
                let mut unwritten = batch;
                unwritten.extend(pending.drain(..));
                reader.reset(&unwritten);
                sleep_unless_shutdown(&server, Duration::from_millis(config.retry_delay_ms));
            }
        }
    }
}

// ------------------------------------------------------------------ writes

/// A SQL literal for a JSON value; the column's type converts it.
fn sql_literal(value: &Value) -> String {
    let quote = |text: &str| format!("'{}'", text.replace('\'', "''"));
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => quote(text),
        other => quote(&other.to_string()),
    }
}

fn quote_identifier_path(name: &str) -> String {
    name.split('.')
        .map(quote_sql_identifier)
        .collect::<Vec<_>>()
        .join(".")
}

struct BatchWriter {
    destination: SourceDestination,
    source: String,
    /// Lowercased column names of the destination table, once known.
    columns: Option<BTreeSet<String>>,
}

impl BatchWriter {
    fn new(config: &SourceConfig) -> Self {
        Self {
            destination: config.into.clone(),
            source: config.name.clone(),
            columns: None,
        }
    }

    /// Writes `batch` in one transaction (broker queues: one idempotent
    /// publish per message) and waits for synchronous standbys.
    fn write(
        &mut self,
        server: &Arc<PgWireServer>,
        batch: &[Incoming],
    ) -> std::result::Result<(), String> {
        let result = self.write_inner(server, batch);
        if result.is_err() {
            // The table may have changed: look its columns up again.
            self.columns = None;
        }
        result
    }

    fn write_inner(
        &mut self,
        server: &Arc<PgWireServer>,
        batch: &[Incoming],
    ) -> std::result::Result<(), String> {
        if let SourceDestination::Queue { queue } = &self.destination {
            let db = server.read_db().map_err(|error| error.to_string())?;
            for message in batch {
                let idempotency_key = message
                    .key
                    .as_ref()
                    .map(|key| format!("{}:{key}", self.source));
                db.with_broker(|broker| {
                    broker.publish_with(
                        queue,
                        Value::Object(message.body.clone()),
                        PublishOptions {
                            headers: serde_json::json!({ "source": self.source }),
                            idempotency_key,
                            ..PublishOptions::default()
                        },
                    )
                })
                .map_err(|error| error.to_string())?;
            }
            return Ok(());
        }
        let statements = self.statements(server, batch)?;
        {
            let admission = server
                .try_admit_write()
                .map_err(|error| error.to_string())?;
            let mut db = server
                .write_db_with_admission(&admission)
                .map_err(|error| error.to_string())?;
            let mut session = sql_session_for_server(server, &mut db);
            session
                .execute("BEGIN")
                .map_err(|error| error.to_string())?;
            for statement in &statements {
                if let Err(error) = session.execute(statement) {
                    let _ = session.execute("ROLLBACK");
                    return Err(error.to_string());
                }
            }
            session
                .execute("COMMIT")
                .map_err(|error| error.to_string())?;
        }
        // Acknowledge upstream only once the batch is as durable as any
        // client commit: confirmed by synchronous standbys when configured.
        if let Some(seq) = bicdb_core::take_thread_commit_seq() {
            server
                .sync_quorum
                .wait_for(seq)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    fn statements(
        &mut self,
        server: &Arc<PgWireServer>,
        batch: &[Incoming],
    ) -> std::result::Result<Vec<String>, String> {
        match self.destination.clone() {
            SourceDestination::Function { function } => Ok(batch
                .iter()
                .map(|message| {
                    format!(
                        "SELECT {}({}::jsonb)",
                        quote_identifier_path(&function),
                        sql_literal(&Value::String(
                            Value::Object(message.body.clone()).to_string()
                        ))
                    )
                })
                .collect()),
            SourceDestination::Table {
                table,
                key,
                payload_column,
                on_conflict,
            } => {
                let columns = self.table_columns(server, &table)?;
                // Rows grouped by their column list: one multi-row INSERT each.
                let mut groups: BTreeMap<Vec<String>, Vec<Vec<String>>> = BTreeMap::new();
                for message in batch {
                    let mut row: BTreeMap<String, String> = BTreeMap::new();
                    for (field, value) in &message.body {
                        let column = field.to_ascii_lowercase();
                        if columns.contains(&column) {
                            row.insert(column, sql_literal(value));
                        }
                    }
                    if let Some(key) = &key {
                        let column = key.to_ascii_lowercase();
                        if !row.contains_key(&column) {
                            let Some(value) = &message.key else {
                                return Err(format!(
                                    "message has no {key} field and the transport gave it no id"
                                ));
                            };
                            row.insert(column, sql_literal(&Value::String(value.clone())));
                        }
                    }
                    if let Some(column) = &payload_column {
                        row.insert(
                            column.to_ascii_lowercase(),
                            sql_literal(&Value::String(
                                Value::Object(message.body.clone()).to_string(),
                            )),
                        );
                    }
                    if row.is_empty() {
                        return Err(format!(
                            "message fields match no column of {table}: {}",
                            Value::Object(message.body.clone())
                        ));
                    }
                    let (names, values): (Vec<_>, Vec<_>) = row.into_iter().unzip();
                    groups.entry(names).or_default().push(values);
                }
                let conflict = match (on_conflict.as_deref(), &key) {
                    (Some("update"), Some(key)) => Some(key.to_ascii_lowercase()),
                    _ => None,
                };
                Ok(groups
                    .into_iter()
                    .map(|(names, rows)| {
                        let tail = match &conflict {
                            Some(key) => {
                                let updates = names
                                    .iter()
                                    .filter(|name| *name != key)
                                    .map(|name| {
                                        let name = quote_sql_identifier(name);
                                        format!("{name} = EXCLUDED.{name}")
                                    })
                                    .collect::<Vec<_>>();
                                if updates.is_empty() {
                                    " ON CONFLICT DO NOTHING".to_string()
                                } else {
                                    format!(
                                        " ON CONFLICT ({}) DO UPDATE SET {}",
                                        quote_sql_identifier(key),
                                        updates.join(", ")
                                    )
                                }
                            }
                            None => " ON CONFLICT DO NOTHING".to_string(),
                        };
                        format!(
                            "INSERT INTO {} ({}) VALUES {}{tail}",
                            quote_identifier_path(&table),
                            names
                                .iter()
                                .map(|name| quote_sql_identifier(name))
                                .collect::<Vec<_>>()
                                .join(", "),
                            rows.iter()
                                .map(|values| format!("({})", values.join(", ")))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })
                    .collect())
            }
            SourceDestination::Queue { .. } => Ok(Vec::new()),
        }
    }

    fn table_columns(
        &mut self,
        server: &Arc<PgWireServer>,
        table: &str,
    ) -> std::result::Result<BTreeSet<String>, String> {
        if let Some(columns) = &self.columns {
            return Ok(columns.clone());
        }
        let (schema, name) = match table.split_once('.') {
            Some((schema, name)) => (schema.to_ascii_lowercase(), name.to_ascii_lowercase()),
            None => ("public".to_string(), table.to_ascii_lowercase()),
        };
        let result = {
            let admission = server
                .try_admit_write()
                .map_err(|error| error.to_string())?;
            let mut db = server
                .write_db_with_admission(&admission)
                .map_err(|error| error.to_string())?;
            let mut session = sql_session_for_server(server, &mut db);
            session
                .execute(&format!(
                    "SELECT column_name FROM information_schema.columns \
                     WHERE table_schema = {} AND table_name = {}",
                    sql_literal(&Value::String(schema)),
                    sql_literal(&Value::String(name))
                ))
                .map_err(|error| error.to_string())?
        };
        let columns = result
            .rows
            .into_iter()
            .filter_map(|row| row.into_iter().next())
            .map(|value| value.to_cell().to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        if columns.is_empty() {
            return Err(format!("table {table} does not exist"));
        }
        self.columns = Some(columns.clone());
        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_quote_text_and_pass_numbers() {
        assert_eq!(sql_literal(&Value::String("it's".into())), "'it''s'");
        assert_eq!(sql_literal(&serde_json::json!(4.5)), "4.5");
        assert_eq!(sql_literal(&Value::Null), "NULL");
        assert_eq!(sql_literal(&serde_json::json!({"a": 1})), "'{\"a\":1}'");
    }

    #[test]
    fn configuration_is_validated() {
        let config: ConnectorsConfig = serde_json::from_value(serde_json::json!({
            "sinks": [{"name": "out", "queue": "orders",
                       "target": {"redis": {"url": "redis://127.0.0.1:6379", "stream": "s"}}}],
            "sources": [{"name": "in", "from": {"redis": {"url": "redis://h", "stream": "s"}},
                         "into": {"table": {"table": "t", "key": "id", "on_conflict": "update"}}}]
        }))
        .unwrap();
        config.validate().unwrap();
        let mut repeated = config.clone();
        repeated.sources[0].name = "out".into();
        assert!(repeated.validate().is_err());
        let mut two_targets = config.clone();
        if let SinkTarget::Redis(redis) = &mut two_targets.sinks[0].target {
            redis.list = Some("l".into());
        }
        assert!(two_targets.validate().is_err());
        let mut update_without_key = config;
        update_without_key.sources[0].into = SourceDestination::Table {
            table: "t".into(),
            key: None,
            payload_column: None,
            on_conflict: Some("update".into()),
        };
        assert!(update_without_key.validate().is_err());
    }
}
