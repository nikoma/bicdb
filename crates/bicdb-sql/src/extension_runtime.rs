//! Native host integration for catalog-backed WASM extensions.
//!
//! HTTP servers can call [`sync_extension_runtime`] and then dispatch through
//! `bicdb_extension::host::ExtensionRuntime`. Event supervisors call
//! [`run_extension_event_once`] for each enabled binding; delivery state,
//! retries, and dead letters are owned by BicDB's durable broker.

use bicdb_core::{BicDb, ConsumeOptions, NackOptions};
use bicdb_extension::host::ExtensionRuntime;
use bicdb_extension::{
    EventBindingDefinition, EventSource, ExtensionInvocationResult, ExtensionState,
    InvocationContext,
};

use crate::{
    list_active_website_deployments, list_event_bindings, list_installed_extensions,
    list_rest_resources, Result, SqlError,
};

#[derive(Clone, Debug)]
pub enum ExtensionEventOutcome {
    Idle,
    Acked {
        message_id: String,
        attempts: u32,
        result: ExtensionInvocationResult,
    },
    Nacked {
        message_id: String,
        attempts: u32,
        dead_lettered: bool,
        error: String,
    },
}

/// Build and atomically publish a runtime snapshot from the durable catalog.
/// A failure leaves the runtime's prior snapshot available.
pub fn sync_extension_runtime(db: &BicDb, runtime: &ExtensionRuntime) -> Result<()> {
    let installations = list_installed_extensions(db)?;
    let active_extensions = installations
        .iter()
        .filter(|installation| installation.state == ExtensionState::Active)
        .map(|installation| installation.manifest.identity.name.to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    let resources = list_rest_resources(db)?
        .into_iter()
        .filter(|resource| active_extensions.contains(&resource.extension.to_ascii_lowercase()))
        .collect::<Vec<_>>();
    let websites = list_active_website_deployments(db)?;
    runtime
        .sync_catalog_with_websites(&installations, &resources, &websites)
        .map_err(extension_runtime_error)
}

/// Return enabled event bindings in stable catalog order.
pub fn active_extension_event_bindings(db: &BicDb) -> Result<Vec<EventBindingDefinition>> {
    let active_extensions = list_installed_extensions(db)?
        .into_iter()
        .filter(|installation| installation.state == ExtensionState::Active)
        .map(|installation| installation.manifest.identity.name.to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    Ok(list_event_bindings(db)?
        .into_iter()
        .filter(|binding| {
            binding.enabled && active_extensions.contains(&binding.extension.to_ascii_lowercase())
        })
        .collect())
}

/// Consume and execute at most one durable event.
///
/// A successful result is acknowledged. Explicit negative acknowledgements,
/// 429/5xx responses, traps, timeouts, and validation failures are nacked with
/// bounded retry and then dead-lettered by the broker.
pub fn run_extension_event_once(
    db: &BicDb,
    runtime: &ExtensionRuntime,
    binding: &EventBindingDefinition,
    consumer_id: &str,
) -> Result<ExtensionEventOutcome> {
    binding.validate().map_err(extension_runtime_error)?;
    if !binding.enabled {
        return Ok(ExtensionEventOutcome::Idle);
    }
    let (queue, group) = binding_queue_and_group(binding);
    let mut messages = db.with_broker(|broker| {
        broker.consume(
            queue,
            group.as_str(),
            consumer_id,
            ConsumeOptions {
                max_messages: 1,
                visibility_timeout_ms: binding.visibility_timeout_ms,
            },
        )
    })?;
    let Some(message) = messages.pop() else {
        return Ok(ExtensionEventOutcome::Idle);
    };
    let message_id = message.message_id.to_string();
    let context = InvocationContext {
        trace_id: Some(message_id.clone()),
        metadata: [
            ("queue".to_string(), queue.to_string()),
            ("group".to_string(), group.clone()),
            ("consumer_id".to_string(), consumer_id.to_string()),
            ("attempt".to_string(), message.attempts.to_string()),
        ]
        .into_iter()
        .collect(),
        ..InvocationContext::default()
    };
    let invocation = runtime.dispatch_event(binding, message_id.clone(), message.payload, context);

    match invocation {
        Ok(result)
            if result.ack
                && result.error.is_none()
                && result.status != 429
                && result.status < 500 =>
        {
            db.with_broker(|broker| {
                broker.ack(queue, group.as_str(), consumer_id, message.message_id)
            })?;
            Ok(ExtensionEventOutcome::Acked {
                message_id,
                attempts: message.attempts,
                result,
            })
        }
        Ok(result) => {
            let error = result.error.clone().unwrap_or_else(|| {
                format!(
                    "extension returned status {} with ack={}",
                    result.status, result.ack
                )
            });
            nack_extension_message(
                db,
                binding,
                queue,
                &group,
                consumer_id,
                message.message_id,
                message.attempts,
                message_id,
                error,
                result.retry_after_ms,
            )
        }
        Err(error) => nack_extension_message(
            db,
            binding,
            queue,
            &group,
            consumer_id,
            message.message_id,
            message.attempts,
            message_id,
            error.to_string(),
            None,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn nack_extension_message(
    db: &BicDb,
    binding: &EventBindingDefinition,
    queue: &str,
    group: &str,
    consumer_id: &str,
    broker_message_id: uuid::Uuid,
    attempts: u32,
    message_id: String,
    error: String,
    requested_delay_ms: Option<u64>,
) -> Result<ExtensionEventOutcome> {
    let requeue = attempts < binding.max_attempts;
    let backoff = 1000u64
        .saturating_mul(1u64 << attempts.saturating_sub(1).min(6))
        .min(60_000);
    db.with_broker(|broker| {
        broker.nack(
            queue,
            group,
            consumer_id,
            broker_message_id,
            NackOptions {
                requeue,
                delay_ms: requeue.then_some(requested_delay_ms.unwrap_or(backoff).min(300_000)),
                error: Some(error.clone()),
            },
        )
    })?;
    Ok(ExtensionEventOutcome::Nacked {
        message_id,
        attempts,
        dead_lettered: !requeue,
        error,
    })
}

fn binding_queue_and_group(binding: &EventBindingDefinition) -> (&str, String) {
    match &binding.source {
        EventSource::Database { .. } => (
            binding
                .delivery_queue
                .as_deref()
                .expect("validated database binding has a queue"),
            format!("extension:{}:{}", binding.extension, binding.name),
        ),
        EventSource::Queue { queue, group } => (queue, group.clone()),
    }
}

fn extension_runtime_error(error: bicdb_extension::ExtensionError) -> SqlError {
    SqlError::InvalidSql(error.to_string())
}
