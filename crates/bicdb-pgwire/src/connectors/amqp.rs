//! AMQP 0-9-1 transports (RabbitMQ and compatible brokers): a sink that
//! publishes with publisher confirms (mandatory, so an unroutable message is
//! a failure, not a silent drop), and a source that consumes a queue with
//! manual acknowledgements and a prefetch of one batch. Messages a source
//! has not acknowledged are redelivered by the AMQP broker after a reconnect.
use super::*;
use futures_core::Stream;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions,
};
use lapin::types::{AMQPValue, FieldTable};
use lapin::Confirmation;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Consumer};
use std::pin::Pin;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

fn runtime() -> std::result::Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("amqp runtime: {error}"))
}

async fn open(url: &str) -> std::result::Result<(Connection, Channel), String> {
    let connection = tokio::time::timeout(
        CONNECT_TIMEOUT,
        Connection::connect(url, ConnectionProperties::default()),
    )
    .await
    .map_err(|_| "connecting to amqp timed out".to_string())?
    .map_err(|error| format!("connecting to amqp: {error}"))?;
    let channel = connection
        .create_channel()
        .await
        .map_err(|error| format!("amqp channel: {error}"))?;
    Ok((connection, channel))
}

// ------------------------------------------------------------------ sink

pub(crate) struct AmqpSink {
    url: String,
    exchange: String,
    routing_key: String,
    runtime: Option<tokio::runtime::Runtime>,
    connected: Option<(Connection, Channel)>,
}

impl AmqpSink {
    pub(crate) fn new(target: &AmqpPublishTarget, name: &str) -> Result<Self> {
        Ok(Self {
            url: resolve_url(&target.url, &target.url_env, name)?,
            exchange: target.exchange.clone(),
            routing_key: target.routing_key.clone(),
            runtime: None,
            connected: None,
        })
    }
}

/// AMQP headers for a message: the broker identity plus the application's
/// own headers (strings, numbers and booleans; anything else as JSON text).
fn amqp_headers(message: &Outgoing) -> FieldTable {
    let mut table = FieldTable::default();
    table.insert(
        "bicdb-queue".into(),
        AMQPValue::LongString(message.queue.clone().into()),
    );
    table.insert(
        "bicdb-sequence".into(),
        AMQPValue::LongLongInt(message.sequence as i64),
    );
    if let Value::Object(headers) = &message.headers {
        for (key, value) in headers {
            let value = match value {
                Value::String(text) => AMQPValue::LongString(text.clone().into()),
                Value::Bool(flag) => AMQPValue::Boolean(*flag),
                Value::Number(number) if number.is_i64() => {
                    AMQPValue::LongLongInt(number.as_i64().unwrap_or_default())
                }
                other => AMQPValue::LongString(other.to_string().into()),
            };
            table.insert(key.clone().into(), value);
        }
    }
    table
}

impl SinkTransport for AmqpSink {
    fn system(&self) -> &'static str {
        "amqp"
    }

    fn deliver(&mut self, message: &Outgoing) -> std::result::Result<(), String> {
        if self.runtime.is_none() {
            self.runtime = Some(runtime()?);
        }
        let runtime = self.runtime.as_ref().expect("created above");
        let url = self.url.clone();
        if self.connected.is_none() {
            let connected = runtime.block_on(async {
                let (connection, channel) = open(&url).await?;
                channel
                    .confirm_select(ConfirmSelectOptions::default())
                    .await
                    .map_err(|error| format!("amqp confirms: {error}"))?;
                Ok::<_, String>((connection, channel))
            })?;
            self.connected = Some(connected);
        }
        let (_, channel) = self.connected.as_ref().expect("connected above");
        let body = message.payload.to_string();
        let properties = BasicProperties::default()
            .with_message_id(message.id.clone().into())
            .with_content_type("application/json".into())
            .with_delivery_mode(2)
            .with_timestamp((message.created_at / 1000).max(0) as u64)
            .with_headers(amqp_headers(message));
        let (exchange, routing_key) = (self.exchange.clone(), self.routing_key.clone());
        runtime.block_on(async {
            let confirm = channel
                .basic_publish(
                    exchange.into(),
                    routing_key.into(),
                    BasicPublishOptions {
                        mandatory: true,
                        ..BasicPublishOptions::default()
                    },
                    body.as_bytes(),
                    properties,
                )
                .await
                .map_err(|error| format!("amqp publish: {error}"))?;
            match tokio::time::timeout(CONNECT_TIMEOUT, confirm)
                .await
                .map_err(|_| "amqp publish confirm timed out".to_string())?
                .map_err(|error| format!("amqp publish: {error}"))?
            {
                Confirmation::Ack(None) => Ok(()),
                Confirmation::Ack(Some(_)) => Err(
                    "amqp: the message was unroutable (no queue bound to the exchange and routing key)"
                        .to_string(),
                ),
                Confirmation::Nack(_) => Err("amqp: the broker rejected the message".to_string()),
                Confirmation::NotRequested => Ok(()),
            }
        })
    }

    fn reset(&mut self) {
        self.connected = None;
    }
}

// ------------------------------------------------------------------ source

pub(crate) struct AmqpSource {
    url: String,
    queue: String,
    consumer_tag: String,
    runtime: Option<tokio::runtime::Runtime>,
    connected: Option<(Connection, Channel, Consumer)>,
    prefetch: u16,
}

impl AmqpSource {
    pub(crate) fn new(origin: &AmqpConsumeSource, name: &str) -> Result<Self> {
        Ok(Self {
            url: resolve_url(&origin.url, &origin.url_env, name)?,
            queue: origin.queue.clone(),
            consumer_tag: format!("bicdb-{name}"),
            runtime: None,
            connected: None,
            prefetch: 0,
        })
    }

    fn incoming(delivery: lapin::message::Delivery) -> Incoming {
        let body = match serde_json::from_slice::<Value>(&delivery.data) {
            Ok(Value::Object(object)) => object,
            Ok(other) => {
                let mut object = Map::new();
                object.insert("body".to_string(), other);
                object
            }
            Err(_) => {
                let mut object = Map::new();
                object.insert(
                    "body".to_string(),
                    Value::String(String::from_utf8_lossy(&delivery.data).into_owned()),
                );
                object
            }
        };
        Incoming {
            key: delivery
                .properties
                .message_id()
                .as_ref()
                .map(|id| id.to_string()),
            body,
            token: delivery.delivery_tag.to_string(),
        }
    }
}

impl SourceTransport for AmqpSource {
    fn system(&self) -> &'static str {
        "amqp"
    }

    fn fetch(&mut self, max: usize, wait: Duration) -> std::result::Result<Vec<Incoming>, String> {
        if self.runtime.is_none() {
            self.runtime = Some(runtime()?);
        }
        let runtime = self.runtime.as_ref().expect("created above");
        let prefetch = max.clamp(1, u16::MAX as usize) as u16;
        if self.connected.is_none() {
            let (url, queue, tag) = (
                self.url.clone(),
                self.queue.clone(),
                self.consumer_tag.clone(),
            );
            let connected = runtime.block_on(async {
                let (connection, channel) = open(&url).await?;
                channel
                    .basic_qos(prefetch, BasicQosOptions::default())
                    .await
                    .map_err(|error| format!("amqp qos: {error}"))?;
                let consumer = channel
                    .basic_consume(
                        queue.into(),
                        tag.into(),
                        BasicConsumeOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                    .map_err(|error| format!("amqp consume: {error}"))?;
                Ok::<_, String>((connection, channel, consumer))
            })?;
            self.connected = Some(connected);
            self.prefetch = prefetch;
        }
        let (_, _, consumer) = self.connected.as_mut().expect("connected above");
        runtime.block_on(async {
            let mut messages = Vec::new();
            let deadline = tokio::time::Instant::now() + wait;
            while messages.len() < max {
                // After the first message, take only what is already there.
                let until = if messages.is_empty() {
                    deadline
                } else {
                    tokio::time::Instant::now() + Duration::from_millis(1)
                };
                let next = tokio::time::timeout_at(
                    until,
                    std::future::poll_fn(|context| Pin::new(&mut *consumer).poll_next(context)),
                )
                .await;
                match next {
                    Err(_) => break,
                    Ok(None) => return Err("amqp consumer was cancelled".to_string()),
                    Ok(Some(Err(error))) => return Err(format!("amqp: {error}")),
                    Ok(Some(Ok(delivery))) => messages.push(Self::incoming(delivery)),
                }
            }
            Ok(messages)
        })
    }

    fn ack(&mut self, messages: &[Incoming]) -> std::result::Result<(), String> {
        let Some(last) = messages
            .iter()
            .filter_map(|message| message.token.parse::<u64>().ok())
            .max()
        else {
            return Ok(());
        };
        let (Some(runtime), Some((_, channel, _))) = (&self.runtime, &self.connected) else {
            return Err("amqp: not connected".to_string());
        };
        // Deliveries on a channel are acknowledged in order: everything up
        // to the batch's last tag was in this batch.
        runtime
            .block_on(channel.basic_ack(last, BasicAckOptions { multiple: true }))
            .map_err(|error| format!("amqp ack: {error}"))
    }

    fn reset(&mut self, _unwritten: &[Incoming]) {
        // Closing the channel returns every unacknowledged delivery to the
        // queue.
        self.connected = None;
    }
}
