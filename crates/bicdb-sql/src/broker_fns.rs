//! SQL surface for the stream broker (Slice 3): scalar functions callable
//! from any SQL context — including over pgwire from external clients — that
//! delegate to `BicDb::with_broker`.
//!
//! Broker operations are durable immediately and are NOT transactional: they
//! do not roll back with an enclosing SQL transaction (like `nextval`).
//! Composite results (receipts, message batches, reports) are returned as
//! `jsonb` values so external clients need no bespoke row types.

use crate::*;
use bicdb_core::{
    BrokerAccess, BrokerCaller, BrokerMessage, ConsumeOptions as BrokerConsumeOptions,
    NackOptions as BrokerNackOptions, PublishOptions as BrokerPublishOptions, PublishReceipt,
    QueueConfig,
};
use serde_json::json;
use serde_json::Value as JsonValue;

pub(crate) fn broker_function_pg_type(name: &str) -> Option<&'static str> {
    match name.strip_prefix("pg_catalog.").unwrap_or(name) {
        "broker_ack" | "broker_nack" | "broker_configure_queue" => Some("bool"),
        "broker_purge_dead_letters" => Some("int8"),
        "broker_publish"
        | "broker_publish_with"
        | "broker_publish_on_commit"
        | "broker_publish_with_on_commit"
        | "broker_consume"
        | "broker_group_info"
        | "broker_dead_letters"
        | "broker_queue_config"
        | "broker_trim"
        | "broker_redrive_dead_letters"
        | "broker_peek"
        | "broker_stats" => Some("jsonb"),
        _ => None,
    }
}

/// Dispatches `broker_*` SQL functions. Returns `Ok(None)` for unrelated
/// names so callers fall through to the next function family.
pub(crate) fn eval_broker_function_value(
    db: &BicDb,
    name: &str,
    args: &[SqlValue],
    caller: BrokerCaller<'_>,
    tx: Option<&bicdb_core::Transaction>,
) -> Result<Option<SqlValue>> {
    let authorize = |queue: &str, access: BrokerAccess| -> Result<()> {
        db.with_broker(|broker| broker.authorize(queue, access, caller))
            .map_err(SqlError::from)
    };
    match name {
        "broker_publish" => {
            expect_args(name, args, 2, 3)?;
            let queue = text_arg(name, args, 0)?;
            let payload = json_arg(name, args, 1)?;
            let headers = args
                .get(2)
                .map(|_| json_arg(name, args, 2))
                .transpose()?
                .unwrap_or(JsonValue::Null);
            authorize(&queue, BrokerAccess::Publish)?;
            let receipt = db
                .with_broker(|broker| broker.publish(&queue, payload, headers))
                .map_err(SqlError::from)?;
            Ok(Some(SqlValue::Json(receipt_to_json(&receipt))))
        }
        "broker_publish_with" => {
            expect_args(name, args, 3, 3)?;
            let queue = text_arg(name, args, 0)?;
            let payload = json_arg(name, args, 1)?;
            let options = json_arg(name, args, 2)?;
            let publish_options = BrokerPublishOptions {
                headers: options.get("headers").cloned().unwrap_or(JsonValue::Null),
                idempotency_key: options
                    .get("idempotency_key")
                    .and_then(JsonValue::as_str)
                    .map(str::to_string),
                delay_ms: options.get("delay_ms").and_then(JsonValue::as_u64),
                max_attempts: options
                    .get("max_attempts")
                    .and_then(JsonValue::as_u64)
                    .map(|raw| raw as u32),
            };
            authorize(&queue, BrokerAccess::Publish)?;
            let receipt = db
                .with_broker(|broker| broker.publish_with(&queue, payload, publish_options))
                .map_err(SqlError::from)?;
            Ok(Some(SqlValue::Json(receipt_to_json(&receipt))))
        }
        "broker_publish_on_commit" => {
            expect_args(name, args, 2, 3)?;
            let queue = text_arg(name, args, 0)?;
            let payload = json_arg(name, args, 1)?;
            let headers = args
                .get(2)
                .map(|_| json_arg(name, args, 2))
                .transpose()?
                .unwrap_or(JsonValue::Null);
            authorize(&queue, BrokerAccess::Publish)?;
            let options = BrokerPublishOptions {
                headers,
                ..BrokerPublishOptions::default()
            };
            deferred_or_immediate_publish(db, tx, &queue, payload, options)
        }
        "broker_publish_with_on_commit" => {
            expect_args(name, args, 3, 3)?;
            let queue = text_arg(name, args, 0)?;
            let payload = json_arg(name, args, 1)?;
            let options = json_arg(name, args, 2)?;
            authorize(&queue, BrokerAccess::Publish)?;
            let options = publish_options_from_json(&options);
            deferred_or_immediate_publish(db, tx, &queue, payload, options)
        }
        "broker_consume" => {
            expect_args(name, args, 3, 5)?;
            let queue = text_arg(name, args, 0)?;
            let group = text_arg(name, args, 1)?;
            let consumer = text_arg(name, args, 2)?;
            let max_messages = optional_int_arg(name, args, 3)?.unwrap_or(1).max(0) as usize;
            let visibility_timeout_ms =
                optional_int_arg(name, args, 4)?.unwrap_or(30_000).max(0) as u64;
            authorize(&queue, BrokerAccess::Consume)?;
            let batch = db
                .with_broker(|broker| {
                    broker.consume(
                        &queue,
                        &group,
                        &consumer,
                        BrokerConsumeOptions {
                            max_messages,
                            visibility_timeout_ms,
                        },
                    )
                })
                .map_err(SqlError::from)?;
            Ok(Some(SqlValue::Json(JsonValue::Array(
                batch.iter().map(message_to_json).collect(),
            ))))
        }
        "broker_ack" => {
            expect_args(name, args, 4, 4)?;
            let queue = text_arg(name, args, 0)?;
            let group = text_arg(name, args, 1)?;
            let consumer = text_arg(name, args, 2)?;
            let message_id = message_id_arg(name, args, 3)?;
            authorize(&queue, BrokerAccess::Consume)?;
            db.with_broker(|broker| broker.ack(&queue, &group, &consumer, message_id))
                .map_err(SqlError::from)?;
            Ok(Some(SqlValue::Bool(true)))
        }
        "broker_nack" => {
            expect_args(name, args, 4, 7)?;
            let queue = text_arg(name, args, 0)?;
            let group = text_arg(name, args, 1)?;
            let consumer = text_arg(name, args, 2)?;
            let message_id = message_id_arg(name, args, 3)?;
            let requeue = match args.get(4) {
                None | Some(SqlValue::Null) => true,
                Some(SqlValue::Bool(value)) => *value,
                Some(_) => {
                    return Err(SqlError::InvalidSql(format!(
                        "{name} requeue argument must be boolean"
                    )))
                }
            };
            let delay_ms = optional_int_arg(name, args, 5)?.map(|raw| raw.max(0) as u64);
            let error = match args.get(6) {
                None | Some(SqlValue::Null) => None,
                Some(SqlValue::String(text)) => Some(text.clone()),
                Some(other) => Some(other.to_cell()),
            };
            authorize(&queue, BrokerAccess::Consume)?;
            db.with_broker(|broker| {
                broker.nack(
                    &queue,
                    &group,
                    &consumer,
                    message_id,
                    BrokerNackOptions {
                        requeue,
                        delay_ms,
                        error,
                    },
                )
            })
            .map_err(SqlError::from)?;
            Ok(Some(SqlValue::Bool(true)))
        }
        "broker_group_info" => {
            expect_args(name, args, 2, 2)?;
            let queue = text_arg(name, args, 0)?;
            let group = text_arg(name, args, 1)?;
            authorize(&queue, BrokerAccess::Consume)?;
            let info = db.with_broker(|broker| broker.group_info(&queue, &group));
            Ok(Some(match info {
                Some(info) => SqlValue::Json(json!({
                    "queue": info.queue,
                    "group": info.group,
                    "created_at_ms": info.created_at,
                    "last_delivered_sequence": info.last_delivered_sequence,
                    "in_flight": info.in_flight,
                    "pending_redelivery": info.pending_redelivery,
                })),
                None => SqlValue::Null,
            }))
        }
        "broker_dead_letters" => {
            expect_args(name, args, 2, 2)?;
            let queue = text_arg(name, args, 0)?;
            let group = text_arg(name, args, 1)?;
            authorize(&queue, BrokerAccess::Consume)?;
            let dead = db.with_broker(|broker| broker.dead_letters(&queue, &group));
            Ok(Some(SqlValue::Json(JsonValue::Array(
                dead.iter()
                    .map(|stored| {
                        json!({
                            "dlq_event_id": stored.event.id.to_string(),
                            "payload": stored.event.payload,
                            "broker": stored.event.metadata.get("broker").cloned()
                                .unwrap_or(JsonValue::Null),
                        })
                    })
                    .collect(),
            ))))
        }
        "broker_configure_queue" => {
            expect_args(name, args, 2, 2)?;
            let queue = text_arg(name, args, 0)?;
            let config = json_arg(name, args, 1)?;
            authorize(&queue, BrokerAccess::Admin)?;
            let queue_config = QueueConfig {
                default_max_attempts: config
                    .get("default_max_attempts")
                    .and_then(JsonValue::as_u64)
                    .map(|raw| raw as u32),
                retention_max_messages: config
                    .get("retention_max_messages")
                    .and_then(JsonValue::as_u64),
                retention_max_age_ms: config
                    .get("retention_max_age_ms")
                    .and_then(JsonValue::as_i64),
                publish_roles: json_roles(config.get("publish_roles")),
                consume_roles: json_roles(config.get("consume_roles")),
                admin_roles: json_roles(config.get("admin_roles")),
            };
            db.with_broker(|broker| broker.configure_queue(&queue, queue_config))
                .map_err(SqlError::from)?;
            Ok(Some(SqlValue::Bool(true)))
        }
        "broker_queue_config" => {
            expect_args(name, args, 1, 1)?;
            let queue = text_arg(name, args, 0)?;
            authorize(&queue, BrokerAccess::Admin)?;
            let config = db.with_broker(|broker| broker.queue_config(&queue));
            Ok(Some(match config {
                Some(config) => SqlValue::Json(json!({
                    "default_max_attempts": config.default_max_attempts,
                    "retention_max_messages": config.retention_max_messages,
                    "retention_max_age_ms": config.retention_max_age_ms,
                    "publish_roles": config.publish_roles,
                    "consume_roles": config.consume_roles,
                    "admin_roles": config.admin_roles,
                })),
                None => SqlValue::Null,
            }))
        }
        "broker_trim" => {
            expect_args(name, args, 1, 1)?;
            let queue = text_arg(name, args, 0)?;
            authorize(&queue, BrokerAccess::Admin)?;
            let report = db
                .with_broker(|broker| broker.trim(&queue))
                .map_err(SqlError::from)?;
            Ok(Some(SqlValue::Json(json!({
                "removed_messages": report.removed_messages,
                "removed_control_events": report.removed_control_events,
                "through_sequence": report.through_sequence,
                "bytes_before": report.bytes_before,
                "bytes_after": report.bytes_after,
            }))))
        }
        "broker_redrive_dead_letters" => {
            expect_args(name, args, 2, 3)?;
            let queue = text_arg(name, args, 0)?;
            let group = text_arg(name, args, 1)?;
            let max = optional_int_arg(name, args, 2)?.unwrap_or(100).max(0) as usize;
            authorize(&queue, BrokerAccess::Admin)?;
            let receipts = db
                .with_broker(|broker| broker.redrive_dead_letters(&queue, &group, max))
                .map_err(SqlError::from)?;
            Ok(Some(SqlValue::Json(JsonValue::Array(
                receipts.iter().map(receipt_to_json).collect(),
            ))))
        }
        "broker_peek" => {
            expect_args(name, args, 1, 3)?;
            let queue = text_arg(name, args, 0)?;
            let from_sequence = optional_int_arg(name, args, 1)?.unwrap_or(0).max(0) as u64;
            let max = optional_int_arg(name, args, 2)?.unwrap_or(50).max(0) as usize;
            authorize(&queue, BrokerAccess::Consume)?;
            let messages = db.with_broker(|broker| broker.peek(&queue, from_sequence, max));
            Ok(Some(SqlValue::Json(JsonValue::Array(
                messages
                    .iter()
                    .map(|message| {
                        json!({
                            "sequence": message.sequence,
                            "message_id": message.message_id.to_string(),
                            "payload": message.payload,
                            "headers": message.headers,
                            "idempotency_key": message.idempotency_key,
                            "created_at_ms": message.created_at,
                            "available_at_ms": message.available_at,
                            "max_attempts": message.max_attempts,
                        })
                    })
                    .collect(),
            ))))
        }
        "broker_stats" => {
            expect_args(name, args, 0, 0)?;
            let mut stats = db.with_broker(|broker| broker.stats());
            // Every SQL caller sees only the queues it may consume from. This
            // was gated on `security_context.is_some()`, so a session without
            // one — which is every ordinary pgwire client on a server with no
            // configured context — enumerated every queue in the broker.
            // `authorize` itself decides what a trusted host may see.
            stats.queues.retain(|queue| {
                db.with_broker(|broker| {
                    broker
                        .authorize(&queue.queue, BrokerAccess::Consume, caller)
                        .is_ok()
                })
            });
            Ok(Some(SqlValue::Json(json!({
                "queues": stats.queues.iter().map(|queue| json!({
                    "queue": queue.queue,
                    "retained_messages": queue.retained_messages,
                    "next_sequence": queue.next_sequence,
                    "configured": queue.configured,
                    "groups": queue.groups.iter().map(|group| json!({
                        "group": group.group,
                        "created_at_ms": group.created_at,
                        "last_delivered_sequence": group.last_delivered_sequence,
                        "in_flight": group.in_flight,
                        "pending_redelivery": group.pending_redelivery,
                        "settled_floor": group.settled_floor,
                        "lag": group.lag,
                        "dead_letters": group.dead_letters,
                    })).collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
            }))))
        }
        "broker_purge_dead_letters" => {
            expect_args(name, args, 2, 2)?;
            let queue = text_arg(name, args, 0)?;
            let group = text_arg(name, args, 1)?;
            authorize(&queue, BrokerAccess::Admin)?;
            let purged = db
                .with_broker(|broker| broker.purge_dead_letters(&queue, &group))
                .map_err(SqlError::from)?;
            Ok(Some(SqlValue::Int(purged as i64)))
        }
        _ => Ok(None),
    }
}

fn json_roles(value: Option<&JsonValue>) -> Option<std::collections::BTreeSet<String>> {
    value.and_then(JsonValue::as_array).map(|roles| {
        roles
            .iter()
            .filter_map(JsonValue::as_str)
            .map(str::to_string)
            .collect()
    })
}

fn expect_args(name: &str, args: &[SqlValue], min: usize, max: usize) -> Result<()> {
    if args.len() < min || args.len() > max {
        return Err(SqlError::InvalidSql(if min == max {
            format!("{name} expects {min} arguments, got {}", args.len())
        } else {
            format!(
                "{name} expects between {min} and {max} arguments, got {}",
                args.len()
            )
        }));
    }
    Ok(())
}

fn text_arg(name: &str, args: &[SqlValue], index: usize) -> Result<String> {
    match args.get(index) {
        Some(SqlValue::String(text)) => Ok(text.clone()),
        _ => Err(SqlError::InvalidSql(format!(
            "{name} argument {} must be text",
            index + 1
        ))),
    }
}

/// Accepts native json/jsonb values, JSON-encoded text, or NULL.
fn json_arg(name: &str, args: &[SqlValue], index: usize) -> Result<JsonValue> {
    match args.get(index) {
        Some(SqlValue::JsonText(value)) => Ok(value.parsed().clone()),
        Some(SqlValue::Json(value)) => Ok(value.clone()),
        Some(SqlValue::String(text)) => serde_json::from_str(text).map_err(|error| {
            SqlError::InvalidSql(format!(
                "{name} argument {} is not valid JSON: {error}",
                index + 1
            ))
        }),
        Some(SqlValue::Null) | None => Ok(JsonValue::Null),
        Some(_) => Err(SqlError::InvalidSql(format!(
            "{name} argument {} must be json or text",
            index + 1
        ))),
    }
}

fn optional_int_arg(name: &str, args: &[SqlValue], index: usize) -> Result<Option<i64>> {
    match args.get(index) {
        None | Some(SqlValue::Null) => Ok(None),
        Some(value) => sql_value_i64(value).map(Some).ok_or_else(|| {
            SqlError::InvalidSql(format!("{name} argument {} must be an integer", index + 1))
        }),
    }
}

fn message_id_arg(name: &str, args: &[SqlValue], index: usize) -> Result<uuid::Uuid> {
    let text = text_arg(name, args, index)?;
    uuid::Uuid::parse_str(&text).map_err(|_| {
        SqlError::InvalidSql(format!(
            "{name} argument {} must be a message id (uuid)",
            index + 1
        ))
    })
}

fn publish_options_from_json(options: &JsonValue) -> BrokerPublishOptions {
    BrokerPublishOptions {
        headers: options.get("headers").cloned().unwrap_or(JsonValue::Null),
        idempotency_key: options
            .get("idempotency_key")
            .and_then(JsonValue::as_str)
            .map(str::to_string),
        delay_ms: options.get("delay_ms").and_then(JsonValue::as_u64),
        max_attempts: options
            .get("max_attempts")
            .and_then(JsonValue::as_u64)
            .map(|raw| raw as u32),
    }
}

/// Inside an open transaction the publish is buffered onto it (applied at
/// COMMIT, dropped on ROLLBACK); in autocommit it publishes immediately.
/// Deferred receipts carry `"deferred": true` and no sequence (idempotency
/// deduplication also happens at commit time).
fn deferred_or_immediate_publish(
    db: &BicDb,
    tx: Option<&bicdb_core::Transaction>,
    queue: &str,
    payload: JsonValue,
    options: BrokerPublishOptions,
) -> Result<Option<SqlValue>> {
    match tx {
        Some(tx) => {
            let message_id = tx
                .buffer_broker_publish(queue, payload, options)
                .map_err(SqlError::from)?;
            Ok(Some(SqlValue::Json(json!({
                "message_id": message_id.to_string(),
                "sequence": JsonValue::Null,
                "deduplicated": false,
                "deferred": true,
            }))))
        }
        None => {
            let receipt = db
                .with_broker(|broker| broker.publish_with(queue, payload, options))
                .map_err(SqlError::from)?;
            let mut receipt = receipt_to_json(&receipt);
            receipt["deferred"] = JsonValue::Bool(false);
            Ok(Some(SqlValue::Json(receipt)))
        }
    }
}

fn receipt_to_json(receipt: &PublishReceipt) -> JsonValue {
    json!({
        "message_id": receipt.message_id.to_string(),
        "sequence": receipt.sequence,
        "deduplicated": receipt.deduplicated,
    })
}

fn message_to_json(message: &BrokerMessage) -> JsonValue {
    json!({
        "queue": message.queue,
        "group": message.group,
        "message_id": message.message_id.to_string(),
        "sequence": message.sequence,
        "payload": message.payload,
        "headers": message.headers,
        "idempotency_key": message.idempotency_key,
        "created_at_ms": message.created_at,
        "delivered_at_ms": message.delivered_at,
        "visibility_deadline_ms": message.visibility_deadline,
        "attempts": message.attempts,
        "max_attempts": message.max_attempts,
    })
}
