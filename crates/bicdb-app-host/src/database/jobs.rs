//! The application database owns job identity, delivery leases and settlement.
//! The historical local broker is read only during the resumable migration.
use anyhow::{bail, ensure, Context, Result};
use bicdb_core::{BrokerDeliveryState, BrokerMessage, DEFAULT_MAX_ATTEMPTS};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::{json, Value};
use uuid::Uuid;

use super::{quote, Database, JOB_GROUP, JOB_QUEUE, OUTBOX};

const JOBS: &str = "__bic_platform_jobs";
const PAYLOADS: &str = "__bic_platform_job_payloads";
const KEYS: &str = "__bic_platform_job_keys";
const MIGRATION: &str = "__bic_platform_job_migration";

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}
fn text(row: &[SqlValue], position: usize) -> Result<&str> {
    match row.get(position) {
        Some(SqlValue::String(value)) => Ok(value),
        _ => bail!("invalid job text field"),
    }
}
fn integer(row: &[SqlValue], position: usize) -> Result<i64> {
    match row.get(position) {
        Some(SqlValue::Int(value)) => Ok(*value),
        _ => bail!("invalid job integer field"),
    }
}
fn transaction<T>(
    session: &mut SqlSession<'_>,
    work: impl FnOnce(&mut SqlSession<'_>) -> Result<T>,
) -> Result<T> {
    session.execute("BEGIN")?;
    let result = work(session);
    match result {
        Ok(value) => match session.execute("COMMIT") {
            Ok(_) => Ok(value),
            Err(error) => {
                let _ = session.execute("ROLLBACK");
                Err(error.into())
            }
        },
        Err(error) => {
            let _ = session.execute("ROLLBACK");
            Err(error)
        }
    }
}

fn lookup(session: &mut SqlSession<'_>, key: &str, payload: &Value) -> Result<Option<String>> {
    use sha2::{Digest, Sha256};
    let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(payload)?));
    let rows = session
        .execute(&format!(
            "SELECT id, digest FROM {KEYS} WHERE key = {}",
            quote(key)
        ))?
        .rows;
    if let Some(row) = rows.first() {
        ensure!(
            text(row, 1)? == digest,
            "job idempotency key reused with a different payload"
        );
        return Ok(Some(text(row, 0)?.into()));
    }
    Ok(None)
}
fn record_key(session: &mut SqlSession<'_>, key: &str, id: &str, payload: &Value) -> Result<()> {
    use sha2::{Digest, Sha256};
    let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(payload)?));
    session.execute(&format!(
        "INSERT INTO {KEYS} VALUES ({}, {}, {})",
        quote(key),
        quote(id),
        quote(&digest)
    ))?;
    Ok(())
}
fn insert(session: &mut SqlSession<'_>, id: &str, payload: &Value, timestamp: i64) -> Result<()> {
    let rows = session
        .execute(&format!("SELECT COALESCE(MAX(position), 0) FROM {JOBS}"))?
        .rows;
    let position = integer(&rows[0], 0)?
        .checked_add(1)
        .context("job position exhausted")?;
    session.execute(&format!(
        "INSERT INTO {PAYLOADS} VALUES ({}, {})",
        quote(id),
        quote(&serde_json::to_string(payload)?)
    ))?;
    session.execute(&format!("INSERT INTO {JOBS} VALUES ({}, {position}, {timestamp}, {timestamp}, 0, {DEFAULT_MAX_ATTEMPTS}, 'ready', '', 0, 0)", quote(id)))?;
    Ok(())
}

impl Database {
    pub(super) fn initialize_job_ledger(&mut self) -> Result<()> {
        let mut db = self.db.write();
        let mut session = SqlSession::new(&mut db);
        session.execute(&format!("CREATE TABLE IF NOT EXISTS {JOBS} (id TEXT PRIMARY KEY, position BIGINT NOT NULL, created_at BIGINT NOT NULL, available_at BIGINT NOT NULL, attempts BIGINT NOT NULL, max_attempts BIGINT NOT NULL, status TEXT NOT NULL, consumer TEXT NOT NULL, lease_until BIGINT NOT NULL, delivered_at BIGINT NOT NULL)"))?;
        session.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {PAYLOADS} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)"
        ))?;
        session.execute(&format!("CREATE TABLE IF NOT EXISTS {KEYS} (key TEXT PRIMARY KEY, id TEXT NOT NULL, digest TEXT NOT NULL)"))?;
        session.execute(&format!("CREATE TABLE IF NOT EXISTS {MIGRATION} (id TEXT PRIMARY KEY, cursor BIGINT NOT NULL, complete BOOL NOT NULL)"))?;
        let migration = session
            .execute(&format!(
                "SELECT cursor, complete FROM {MIGRATION} WHERE id = 'legacy-v1'"
            ))?
            .rows;
        if migration
            .first()
            .is_some_and(|row| row[1] == SqlValue::Bool(true))
        {
            return Ok(());
        }
        let mut cursor = migration
            .first()
            .map(|row| integer(row, 0))
            .transpose()?
            .unwrap_or(0);
        if migration.is_empty() {
            session.execute(&format!(
                "INSERT INTO {MIGRATION} VALUES ('legacy-v1', 0, FALSE)"
            ))?;
        }
        if !self.legacy_jobs_path.exists() {
            session.execute(&format!(
                "UPDATE {MIGRATION} SET complete = TRUE WHERE id = 'legacy-v1'"
            ))?;
            return Ok(());
        }
        let mut legacy = bicdb_core::BicDb::open_with_config(
            &self.legacy_jobs_path,
            bicdb_core::DbConfig::default().with_fsync(true),
        )?;
        // The old broker will never be delivered again by this host. Import
        // exact IDs and attempts, including outstanding leases and settled jobs.
        let dead = legacy.broker().dead_letter_attempts(JOB_QUEUE, JOB_GROUP);
        loop {
            let batch = legacy.broker().delivery_snapshot(
                JOB_QUEUE,
                JOB_GROUP,
                cursor.try_into()?,
                128,
                16 * 1024 * 1024,
            )?;
            if batch.is_empty() {
                session.execute(&format!(
                    "UPDATE {MIGRATION} SET complete = TRUE WHERE id = 'legacy-v1'"
                ))?;
                break;
            }
            transaction(&mut session, |session| {
                for delivery in &batch {
                    let message = &delivery.message;
                    let id = message.message_id.to_string();
                    let (status, consumer, lease, available) = match &delivery.state {
                        BrokerDeliveryState::Unseen => ("ready", "", 0, message.available_at),
                        BrokerDeliveryState::Available { available_at } => {
                            ("ready", "", 0, *available_at)
                        }
                        BrokerDeliveryState::InFlight {
                            consumer,
                            visibility_deadline,
                        } => (
                            "running",
                            consumer.as_str(),
                            *visibility_deadline,
                            message.available_at,
                        ),
                        BrokerDeliveryState::Settled => (
                            if dead.contains_key(&message.message_id) {
                                "dead"
                            } else {
                                "done"
                            },
                            "",
                            0,
                            message.available_at,
                        ),
                    };
                    let attempts = dead
                        .get(&message.message_id)
                        .copied()
                        .map(i64::from)
                        .unwrap_or(i64::from(delivery.attempts));
                    let position = i64::try_from(message.sequence)?
                        .checked_add(1)
                        .context("legacy job position exhausted")?;
                    session.execute(&format!(
                        "INSERT INTO {PAYLOADS} VALUES ({}, {})",
                        quote(&id),
                        quote(&serde_json::to_string(&message.payload)?)
                    ))?;
                    session.execute(&format!("INSERT INTO {JOBS} VALUES ({}, {position}, {}, {available}, {attempts}, {}, {}, {}, {lease}, {})", quote(&id), message.created_at, message.max_attempts, quote(status), quote(consumer), delivery.delivered_at))?;
                    if let Some(key) = &message.idempotency_key {
                        record_key(session, &format!("legacy:{key}"), &id, &message.payload)?;
                    }
                    cursor = position;
                }
                session.execute(&format!(
                    "UPDATE {MIGRATION} SET cursor = {cursor} WHERE id = 'legacy-v1'"
                ))?;
                Ok(())
            })?;
        }
        Ok(())
    }

    pub fn enqueue(&mut self, payload: Value, key: Option<String>) -> Result<Value> {
        self.ensure_authority()?;
        let result = self.enqueue_local(payload, key);
        self.acknowledge(result)
    }
    fn enqueue_local(&mut self, payload: Value, key: Option<String>) -> Result<Value> {
        ensure!(
            serde_json::to_vec(&payload)?.len() <= 8 * 1024 * 1024,
            "job payload exceeds 8 MiB"
        );
        if let Some(key) = &key {
            ensure!(
                !key.is_empty() && key.len() <= 256,
                "invalid job idempotency key"
            );
        }
        let mut db = self.db.write();
        let mut session = SqlSession::new(&mut db);
        transaction(&mut session, |session| {
            if let Some(key) = &key {
                if let Some(id) = lookup(session, &format!("send:{key}"), &payload)?.or(lookup(
                    session,
                    &format!("legacy:{key}"),
                    &payload,
                )?) {
                    return Ok(json!({"id":id,"deduplicated":true}));
                }
            }
            let id = Uuid::new_v4().to_string();
            insert(session, &id, &payload, now())?;
            if let Some(key) = &key {
                record_key(session, &format!("send:{key}"), &id, &payload)?;
            }
            Ok(json!({"id":id,"deduplicated":false}))
        })
    }

    /// Move each SQL outbox entry into the canonical job ledger in one database
    /// transaction. No local broker acknowledgement is needed to preserve it.
    pub fn drain_outbox(&mut self, limit: usize) -> Result<usize> {
        self.ensure_authority()?;
        let result = self.drain_outbox_local(limit);
        self.acknowledge(result)
    }
    fn drain_outbox_local(&mut self, limit: usize) -> Result<usize> {
        ensure!((1..=128).contains(&limit), "invalid outbox batch size");
        let mut db = self.db.write();
        let mut session = SqlSession::new(&mut db);
        let rows = session
            .execute(&format!(
                "SELECT id FROM {OUTBOX} ORDER BY position LIMIT {limit}"
            ))?
            .rows;
        if rows.is_empty() {
            return Ok(0);
        }
        transaction(&mut session, |session| {
            let mut count = 0;
            let mut bytes = 0usize;
            for row in &rows {
                let id = text(row, 0)?;
                let values = session
                    .execute(&format!(
                        "SELECT payload FROM {OUTBOX} WHERE id = {}",
                        quote(id)
                    ))?
                    .rows;
                let encoded = text(&values[0], 0)?;
                ensure!(encoded.len() <= 8 * 1024 * 1024, "outbox job exceeds 8 MiB");
                if bytes.saturating_add(encoded.len()) > 16 * 1024 * 1024 {
                    break;
                }
                bytes += encoded.len();
                let payload: Value = serde_json::from_str(encoded)?;
                if lookup(session, &format!("legacy:outbox:{id}"), &payload)?.is_none() {
                    insert(session, id, &payload, now())?;
                }
                session.execute(&format!("DELETE FROM {OUTBOX} WHERE id = {}", quote(id)))?;
                count += 1;
            }
            Ok(count)
        })
    }

    pub fn job_status(&mut self) -> Result<Value> {
        let timestamp = now();
        let mut db = self.db.write();
        let mut session = SqlSession::new(&mut db);
        let mut count = |predicate: &str| -> Result<i64> {
            integer(
                &session
                    .execute(&format!("SELECT COUNT(*) FROM {JOBS} WHERE {predicate}"))?
                    .rows[0],
                0,
            )
        };
        let in_flight = count(&format!("status = 'running' AND lease_until > {timestamp}"))?;
        let pending = count(&format!("(status = 'ready' AND attempts > 0) OR (status = 'running' AND lease_until <= {timestamp})"))?;
        let dead = count("status = 'dead'")?;
        let queued = count("status = 'ready' AND attempts = 0")?;
        let rows = session.execute(&format!("SELECT MAX(position) FROM {JOBS} WHERE attempts > 0 OR status = 'done' OR status = 'dead'"))?.rows;
        let delivered = match rows[0][0] {
            SqlValue::Int(value) => Some(value - 1),
            _ => None,
        };
        Ok(
            json!({"in_flight":in_flight,"pending_redelivery":pending,"queued":queued,"dead_letters":dead,"last_delivered_sequence":delivered}),
        )
    }

    pub fn consume(&mut self, consumer: &str, timeout_ms: u64) -> Result<Option<BrokerMessage>> {
        self.ensure_authority()?;
        let result = self.consume_local(consumer, timeout_ms);
        self.acknowledge(result)
    }
    fn consume_local(&mut self, consumer: &str, timeout_ms: u64) -> Result<Option<BrokerMessage>> {
        ensure!(
            !consumer.is_empty() && consumer.len() <= 256,
            "invalid job consumer"
        );
        ensure!(
            (1..=300_000).contains(&timeout_ms),
            "invalid job execution timeout"
        );
        let timestamp = now();
        let deadline = timestamp.saturating_add(timeout_ms as i64 + 10_000);
        let mut db = self.db.write();
        let mut session = SqlSession::new(&mut db);
        let rows = session.execute(&format!("SELECT id, position, created_at, attempts, max_attempts FROM {JOBS} WHERE (status = 'ready' OR (status = 'running' AND lease_until <= {timestamp})) AND available_at <= {timestamp} ORDER BY position LIMIT 128"))?.rows;
        if rows.is_empty() {
            return Ok(None);
        }
        transaction(&mut session, |session| {
            for row in rows {
                let id = text(&row, 0)?;
                let attempt = integer(&row, 3)?;
                let maximum = integer(&row, 4)?;
                if attempt >= maximum {
                    session.execute(&format!("UPDATE {JOBS} SET status = 'dead', consumer = '', lease_until = 0 WHERE id = {}", quote(id)))?;
                    continue;
                }
                let attempt = attempt.checked_add(1).context("job attempts exhausted")?;
                session.execute(&format!("UPDATE {JOBS} SET status = 'running', consumer = {}, lease_until = {deadline}, delivered_at = {timestamp}, attempts = {attempt} WHERE id = {}", quote(consumer), quote(id)))?;
                let values = session
                    .execute(&format!(
                        "SELECT payload FROM {PAYLOADS} WHERE id = {}",
                        quote(id)
                    ))?
                    .rows;
                return Ok(Some(BrokerMessage {
                    queue: JOB_QUEUE.into(),
                    group: JOB_GROUP.into(),
                    message_id: Uuid::parse_str(id)?,
                    sequence: (integer(&row, 1)? - 1).try_into()?,
                    payload: serde_json::from_str(text(&values[0], 0)?)?,
                    headers: json!({}),
                    idempotency_key: None,
                    created_at: integer(&row, 2)?,
                    delivered_at: timestamp,
                    visibility_deadline: deadline,
                    attempts: attempt.try_into()?,
                    max_attempts: maximum.try_into()?,
                }));
            }
            Ok(None)
        })
    }

    pub fn settle(&mut self, consumer: &str, message: &BrokerMessage, success: bool) -> Result<()> {
        self.ensure_authority()?;
        let result = self.settle_local(consumer, message, success);
        self.acknowledge(result)
    }
    fn settle_local(
        &mut self,
        consumer: &str,
        message: &BrokerMessage,
        success: bool,
    ) -> Result<()> {
        ensure!(
            message.queue == JOB_QUEUE && message.group == JOB_GROUP,
            "invalid application job scope"
        );
        let timestamp = now();
        let id = quote(&message.message_id.to_string());
        let mut db = self.db.write();
        let mut session = SqlSession::new(&mut db);
        transaction(&mut session, |session| {
            let rows = session
                .execute(&format!(
                    "SELECT consumer, attempts, lease_until, status, max_attempts FROM {JOBS} WHERE id = {id}"
                ))?
                .rows;
            let row = rows.first().context("job delivery not found")?;
            ensure!(
                text(row, 0)? == consumer
                    && integer(row, 1)? == i64::from(message.attempts)
                    && integer(row, 2)? == message.visibility_deadline
                    && integer(row, 2)? > timestamp
                    && text(row, 3)? == "running",
                "job delivery lease is stale or expired"
            );
            let status = if success {
                "done"
            } else if i64::from(message.attempts) >= integer(row, 4)? {
                "dead"
            } else {
                "ready"
            };
            session.execute(&format!("UPDATE {JOBS} SET status = '{status}', consumer = '', lease_until = 0, available_at = {} WHERE id = {id}", timestamp.saturating_add(1000)))?;
            Ok(())
        })
    }
}
