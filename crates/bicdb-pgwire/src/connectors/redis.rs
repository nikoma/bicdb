//! Redis transports: a sink that appends to a stream (XADD), publishes to a
//! channel or pushes onto a list, and a source that reads a stream through a
//! consumer group (XREADGROUP, XACK after the database commit). A source
//! re-reads its own pending entries after every reconnect, so entries
//! delivered before a crash or failed write are not lost.
use super::*;
use ::redis::{Client, Connection, Value as RedisValue};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

fn open(url: &str) -> std::result::Result<Connection, String> {
    let client = Client::open(url).map_err(|error| format!("redis url: {error}"))?;
    let connection = client
        .get_connection_with_timeout(CONNECT_TIMEOUT)
        .map_err(|error| format!("connecting to redis: {error}"))?;
    connection
        .set_write_timeout(Some(COMMAND_TIMEOUT))
        .map_err(|error| error.to_string())?;
    connection
        .set_read_timeout(Some(COMMAND_TIMEOUT))
        .map_err(|error| error.to_string())?;
    Ok(connection)
}

fn text(value: &RedisValue) -> Option<String> {
    match value {
        RedisValue::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        RedisValue::SimpleString(text) => Some(text.clone()),
        RedisValue::Int(number) => Some(number.to_string()),
        _ => None,
    }
}

// ------------------------------------------------------------------ sink

pub(crate) struct RedisSink {
    url: String,
    endpoint: RedisEndpoint,
    connection: Option<Connection>,
}

impl RedisSink {
    pub(crate) fn new(endpoint: &RedisEndpoint, name: &str) -> Result<Self> {
        Ok(Self {
            url: resolve_url(&endpoint.url, &endpoint.url_env, name)?,
            endpoint: endpoint.clone(),
            connection: None,
        })
    }
}

impl SinkTransport for RedisSink {
    fn system(&self) -> &'static str {
        "redis"
    }

    fn deliver(&mut self, message: &Outgoing) -> std::result::Result<(), String> {
        if self.connection.is_none() {
            self.connection = Some(open(&self.url)?);
        }
        let connection = self.connection.as_mut().expect("connected above");
        let mut command;
        if let Some(stream) = &self.endpoint.stream {
            command = ::redis::cmd("XADD");
            command.arg(stream);
            if let Some(maxlen) = self.endpoint.maxlen {
                command.arg("MAXLEN").arg("~").arg(maxlen);
            }
            command
                .arg("*")
                .arg("id")
                .arg(&message.id)
                .arg("queue")
                .arg(&message.queue)
                .arg("sequence")
                .arg(message.sequence)
                .arg("payload")
                .arg(message.payload.to_string())
                .arg("headers")
                .arg(message.headers.to_string());
        } else if let Some(channel) = &self.endpoint.channel {
            command = ::redis::cmd("PUBLISH");
            command.arg(channel).arg(message.envelope().to_string());
        } else {
            let list = self.endpoint.list.as_deref().unwrap_or_default();
            command = ::redis::cmd("LPUSH");
            command.arg(list).arg(message.envelope().to_string());
        }
        command
            .query::<RedisValue>(connection)
            .map(|_| ())
            .map_err(|error| format!("redis: {error}"))
    }

    fn reset(&mut self) {
        self.connection = None;
    }
}

// ------------------------------------------------------------------ source

pub(crate) struct RedisSource {
    url: String,
    stream: String,
    group: String,
    consumer: String,
    start: String,
    connection: Option<Connection>,
    /// Re-reading this consumer's pending entries before new ones.
    recovering: bool,
    /// While recovering: pending entries after this id are read next.
    pending_cursor: String,
}

impl RedisSource {
    pub(crate) fn new(endpoint: &RedisEndpoint, name: &str) -> Result<Self> {
        Ok(Self {
            url: resolve_url(&endpoint.url, &endpoint.url_env, name)?,
            stream: endpoint.stream.clone().unwrap_or_default(),
            group: endpoint
                .group
                .clone()
                .unwrap_or_else(|| "bicdb".to_string()),
            // Stable, so a restart re-reads what this source left pending.
            consumer: format!("bicdb-{name}"),
            start: endpoint.start.clone().unwrap_or_else(|| "0".to_string()),
            connection: None,
            recovering: true,
            pending_cursor: "0".to_string(),
        })
    }

    fn connect(&mut self) -> std::result::Result<&mut Connection, String> {
        if self.connection.is_none() {
            let mut connection = open(&self.url)?;
            let created = ::redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(&self.stream)
                .arg(&self.group)
                .arg(&self.start)
                .arg("MKSTREAM")
                .query::<RedisValue>(&mut connection);
            if let Err(error) = created {
                if !error.to_string().contains("BUSYGROUP") {
                    return Err(format!("creating consumer group: {error}"));
                }
            }
            self.connection = Some(connection);
            self.recovering = true;
            self.pending_cursor = "0".to_string();
        }
        Ok(self.connection.as_mut().expect("connected above"))
    }

    /// Turns a stream entry into a message: a `payload` field holding a
    /// JSON object is the message (what a BicDB sink writes); otherwise the
    /// fields are. An `id` field is the message key, else the entry id.
    fn message(entry_id: String, fields: &[RedisValue]) -> Incoming {
        let mut body = Map::new();
        for pair in fields.chunks(2) {
            if let [field, value] = pair {
                if let (Some(field), Some(value)) = (text(field), text(value)) {
                    body.insert(field, Value::String(value));
                }
            }
        }
        let key = body
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| Some(entry_id.clone()));
        if let Some(Value::Object(payload)) = body
            .get("payload")
            .and_then(Value::as_str)
            .and_then(|text| serde_json::from_str::<Value>(text).ok())
        {
            body = payload;
        }
        Incoming {
            key,
            body,
            token: entry_id,
        }
    }
}

impl SourceTransport for RedisSource {
    fn system(&self) -> &'static str {
        "redis"
    }

    fn fetch(&mut self, max: usize, wait: Duration) -> std::result::Result<Vec<Incoming>, String> {
        let (stream, group, consumer) = (
            self.stream.clone(),
            self.group.clone(),
            self.consumer.clone(),
        );
        self.connect()?;
        let recovering = self.recovering;
        let cursor = self.pending_cursor.clone();
        let connection = self.connection.as_mut().expect("connected above");
        let mut command = ::redis::cmd("XREADGROUP");
        command
            .arg("GROUP")
            .arg(&group)
            .arg(&consumer)
            .arg("COUNT")
            .arg(max.max(1));
        if !recovering {
            command.arg("BLOCK").arg(wait.as_millis().max(1) as u64);
        }
        command
            .arg("STREAMS")
            .arg(&stream)
            .arg(if recovering { cursor.as_str() } else { ">" });
        let reply = command
            .query::<RedisValue>(connection)
            .map_err(|error| format!("redis: {error}"))?;
        let mut messages = Vec::new();
        let mut deleted = Vec::new();
        if let RedisValue::Array(streams) = reply {
            for stream_reply in streams {
                let RedisValue::Array(parts) = stream_reply else {
                    continue;
                };
                let Some(RedisValue::Array(entries)) = parts.get(1) else {
                    continue;
                };
                for entry in entries {
                    let RedisValue::Array(entry) = entry else {
                        continue;
                    };
                    let Some(entry_id) = entry.first().and_then(text) else {
                        continue;
                    };
                    if recovering {
                        self.pending_cursor = entry_id.clone();
                    }
                    match entry.get(1) {
                        Some(RedisValue::Array(fields)) => {
                            messages.push(Self::message(entry_id, fields))
                        }
                        // A pending entry trimmed from the stream: nothing
                        // to write, settle it.
                        _ => deleted.push(entry_id),
                    }
                }
            }
        }
        if !deleted.is_empty() {
            let connection = self.connect()?;
            ::redis::cmd("XACK")
                .arg(&stream)
                .arg(&group)
                .arg(&deleted)
                .query::<RedisValue>(connection)
                .map_err(|error| format!("redis: {error}"))?;
        }
        if recovering && messages.is_empty() && deleted.is_empty() {
            self.recovering = false;
        }
        Ok(messages)
    }

    fn ack(&mut self, messages: &[Incoming]) -> std::result::Result<(), String> {
        if messages.is_empty() {
            return Ok(());
        }
        let (stream, group) = (self.stream.clone(), self.group.clone());
        let connection = self.connect()?;
        ::redis::cmd("XACK")
            .arg(&stream)
            .arg(&group)
            .arg(
                messages
                    .iter()
                    .map(|message| &message.token)
                    .collect::<Vec<_>>(),
            )
            .query::<RedisValue>(connection)
            .map(|_| ())
            .map_err(|error| format!("redis: {error}"))
    }

    fn reset(&mut self, _unwritten: &[Incoming]) {
        // Unacknowledged entries stay pending for this consumer and are read
        // again first after reconnecting.
        self.connection = None;
        self.recovering = true;
        self.pending_cursor = "0".to_string();
    }
}
