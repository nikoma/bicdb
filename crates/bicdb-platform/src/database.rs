use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::path::Path;

use anyhow::{bail, ensure, Context, Result};
use bicdb_core::{BicDb, BrokerMessage, ConsumeOptions, DbConfig, NackOptions, PublishOptions};
use bicdb_sql::{SqlResult, SqlSession, SqlValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

use crate::manifest::{Release, TableSync};

const MIGRATIONS: &str = "__bic_platform_migrations";
const RECEIPTS: &str = "__bic_platform_receipts";
pub const JOB_QUEUE: &str = "bic.platform.jobs";
pub const JOB_GROUP: &str = "application";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryBatch {
    pub statements: Vec<Query>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

pub struct Database {
    pub db: BicDb,
    jobs: BicDb,
    sync: BTreeMap<String, TableSync>,
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let mut db = BicDb::open_with_config(
            path.join("data"),
            DbConfig::default().with_fsync(true).with_audit_events(true),
        )?;
        let mut session = SqlSession::new(&mut db);
        session.execute(&format!("CREATE TABLE IF NOT EXISTS {MIGRATIONS} (position BIGINT PRIMARY KEY, name TEXT NOT NULL, digest TEXT NOT NULL)"))?;
        session.execute(&format!("CREATE TABLE IF NOT EXISTS {RECEIPTS} (key TEXT PRIMARY KEY, digest TEXT NOT NULL, result TEXT NOT NULL)"))?;
        drop(session);
        let jobs =
            BicDb::open_with_config(path.join("jobs"), DbConfig::default().with_fsync(true))?;
        Ok(Self {
            db,
            jobs,
            sync: BTreeMap::new(),
        })
    }

    pub fn prepare(&mut self, release: &Release) -> Result<()> {
        self.sync = release.manifest.sync.clone();
        self.migrate(release)?;
        self.configure_sync(&release.manifest.sync)
    }

    pub fn migrate(&mut self, release: &Release) -> Result<()> {
        let node = self.db.node_id();
        let mut session = SqlSession::new(&mut self.db);
        let applied = session
            .execute(&format!(
                "SELECT position, name, digest FROM {MIGRATIONS} ORDER BY position"
            ))?
            .rows;
        ensure!(
            release.manifest.migrations.len() >= applied.len(),
            "release omits previously applied migrations"
        );
        for (position, name) in release.manifest.migrations.iter().enumerate() {
            let source = release.decode(name)?;
            let digest = format!("{:x}", Sha256::digest(&source));
            if let Some(row) = applied.get(position) {
                ensure!(
                    row[1] == SqlValue::String(name.clone()) && row[2] == SqlValue::String(digest),
                    "an applied migration was reordered or modified"
                );
                continue;
            }
            let sql = std::str::from_utf8(&source)?;
            let statements =
                Parser::parse_sql(&PostgreSqlDialect {}, sql).context("parse migration")?;
            ensure!(!statements.is_empty(), "empty migration");
            Self::authorize_statements(&self.sync, &node, &statements)?;
            for statement in &statements {
                ensure!(
                    matches!(
                        statement,
                        Statement::CreateTable(_)
                            | Statement::CreateIndex(_)
                            | Statement::AlterTable(_)
                            | Statement::Insert(_)
                            | Statement::Update { .. }
                            | Statement::Delete(_)
                    ),
                    "unsupported migration statement"
                );
            }
            session.execute("BEGIN")?;
            let result = (|| -> Result<()> {
                session.execute(sql)?;
                session.execute(&format!(
                    "INSERT INTO {MIGRATIONS} VALUES ({position}, {}, {})",
                    quote(name),
                    quote(&digest)
                ))?;
                session.execute("COMMIT")?;
                Ok(())
            })();
            if result.is_err() {
                let _ = session.execute("ROLLBACK");
            }
            result?;
        }
        Ok(())
    }

    pub fn query(&mut self, batch: &QueryBatch) -> Result<Vec<SqlResult>> {
        ensure!(
            !batch.statements.is_empty() && batch.statements.len() <= 128,
            "invalid statement count"
        );
        let digest = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&batch.statements)?)
        );
        for query in &batch.statements {
            ensure!(
                query.sql.len() <= 128 * 1024 && query.params.len() <= 1024,
                "query too large"
            );
            let statements = Parser::parse_sql(&PostgreSqlDialect {}, &query.sql)?;
            Self::authorize_statements(&self.sync, &self.db.node_id(), &statements)?;
            ensure!(
                statements.len() == 1,
                "each query must contain exactly one statement"
            );
            ensure!(matches!(statements[0], Statement::Query(_) | Statement::Insert(_) | Statement::Update {..} | Statement::Delete(_)), "queries support SELECT, INSERT, UPDATE and DELETE; use migrations for schema changes");
            ensure!(
                !query.sql.to_ascii_lowercase().contains("__bic_platform_"),
                "platform metadata is reserved"
            );
        }
        if let Some(key) = &batch.idempotency_key {
            ensure!(
                !key.is_empty() && key.len() <= 256,
                "invalid idempotency key"
            );
            let mut session = SqlSession::new(&mut self.db);
            let rows = session
                .execute(&format!(
                    "SELECT digest, result FROM {RECEIPTS} WHERE key = {}",
                    quote(key)
                ))?
                .rows;
            if let Some(row) = rows.first() {
                ensure!(
                    row[0] == SqlValue::String(digest.clone()),
                    "idempotency key reused for a different transaction"
                );
                if let SqlValue::String(result) = &row[1] {
                    return Ok(serde_json::from_str(result)?);
                }
                bail!("invalid transaction receipt");
            }
        }
        // One database mutex covers receipt lookup, transaction and commit.
        let mut session = SqlSession::new(&mut self.db);
        session.execute("BEGIN")?;
        let mut results = Vec::new();
        for query in &batch.statements {
            let parameters = query
                .params
                .iter()
                .map(|v| match v {
                    Value::Null => SqlValue::Null,
                    Value::Bool(v) => SqlValue::Bool(*v),
                    Value::Number(v) => v.as_i64().map(SqlValue::Int).unwrap_or_else(|| {
                        v.as_f64()
                            .map(SqlValue::Float)
                            .unwrap_or_else(|| SqlValue::Json(Value::Number(v.clone())))
                    }),
                    Value::String(v) => SqlValue::String(v.clone()),
                    other => SqlValue::Json(other.clone()),
                })
                .collect();
            session = session.with_positional_parameters(parameters);
            match session.execute(&query.sql) {
                Ok(result) => results.push(result),
                Err(error) => {
                    let _ = session.execute("ROLLBACK");
                    return Err(error.into());
                }
            }
        }
        if let Some(key) = &batch.idempotency_key {
            let result = serde_json::to_string(&results)?;
            if result.len() > 1024 * 1024 {
                let _ = session.execute("ROLLBACK");
                bail!("idempotent result exceeds receipt limit");
            }
            if let Err(error) = session.execute(&format!(
                "INSERT INTO {RECEIPTS} VALUES ({}, {}, {})",
                quote(key),
                quote(&digest),
                quote(&result)
            )) {
                let _ = session.execute("ROLLBACK");
                return Err(error.into());
            }
        }
        if let Err(error) = session.execute("COMMIT") {
            let _ = session.execute("ROLLBACK");
            return Err(error.into());
        }
        Ok(results)
    }

    pub fn configure_sync(&mut self, policy: &BTreeMap<String, TableSync>) -> Result<()> {
        for collection in self.db.collections() {
            let enabled = policy.contains_key(&collection.name);
            if collection.mesh_sync_enabled != enabled {
                self.db
                    .set_collection_mesh_sync_enabled(&collection.name, enabled)?;
            }
        }
        for table in policy.keys() {
            ensure!(
                self.db.collections().iter().any(|c| &c.name == table),
                "sync table {table} does not exist"
            );
        }
        self.sync = policy.clone();
        Ok(())
    }

    fn authorize_statements(
        policy: &BTreeMap<String, TableSync>,
        node: &bicdb_core::NodeId,
        statements: &[Statement],
    ) -> Result<()> {
        let result = sqlparser::ast::visit_statements(&statements.to_vec(), |statement| {
            let mut targets = Vec::new();
            let append = match statement {
                Statement::Insert(insert) => {
                    targets.push(insert.table.to_string());
                    insert.on.is_none() && !insert.replace_into
                }
                Statement::Update(_) | Statement::Delete(_) => {
                    let _: ControlFlow<()> = sqlparser::ast::visit_relations(statement, |name| {
                        targets.push(name.to_string());
                        ControlFlow::Continue(())
                    });
                    false
                }
                _ => return ControlFlow::Continue(()),
            };
            for table in targets {
                let table = table.replace('"', "").to_ascii_lowercase();
                let table = table.strip_prefix("public.").unwrap_or(&table);
                if let Some(policy) = policy.get(table) {
                    let permitted = match policy {
                        TableSync::AppendOnly => append,
                        TableSync::SingleWriter { node: writer } => writer == node,
                    };
                    if !permitted {
                        return ControlFlow::Break(anyhow::anyhow!(
                            "sync policy refuses writes to {table} on this node"
                        ));
                    }
                }
            }
            ControlFlow::Continue(())
        });
        match result {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(error),
        }
    }

    pub fn conflicts(&self, table: &str) -> Result<Value> {
        ensure!(
            self.sync.contains_key(table),
            "table is not enabled for synchronization"
        );
        Ok(serde_json::to_value(self.db.list_record_conflicts(table)?)?)
    }

    pub fn enqueue(&mut self, payload: Value, key: Option<String>) -> Result<Value> {
        let receipt = self.jobs.broker().publish_with(
            JOB_QUEUE,
            payload,
            PublishOptions {
                idempotency_key: key,
                ..Default::default()
            },
        )?;
        Ok(serde_json::json!({"id": receipt.message_id, "deduplicated": receipt.deduplicated}))
    }

    pub fn job_status(&mut self) -> Value {
        let broker = self.jobs.broker();
        let group = broker.group_info(JOB_QUEUE, JOB_GROUP);
        serde_json::json!({
            "in_flight": group.as_ref().map(|g| g.in_flight).unwrap_or(0),
            "pending_redelivery": group.as_ref().map(|g| g.pending_redelivery).unwrap_or(0),
            "last_delivered_sequence": group.as_ref().and_then(|g| g.last_delivered_sequence),
            "dead_letters": broker.dead_letters(JOB_QUEUE, JOB_GROUP).len(),
        })
    }

    pub fn consume(&mut self, consumer: &str, timeout_ms: u64) -> Result<Option<BrokerMessage>> {
        Ok(self
            .jobs
            .broker()
            .consume(
                JOB_QUEUE,
                JOB_GROUP,
                consumer,
                ConsumeOptions {
                    max_messages: 1,
                    visibility_timeout_ms: timeout_ms + 10_000,
                },
            )?
            .pop())
    }

    pub fn settle(&mut self, consumer: &str, message: &BrokerMessage, success: bool) -> Result<()> {
        if success {
            self.jobs.broker().ack_delivery(consumer, message)?;
        } else {
            self.jobs.broker().nack_delivery(
                consumer,
                message,
                NackOptions {
                    requeue: true,
                    delay_ms: Some(1000),
                    error: Some("application handler failed".into()),
                },
            )?;
        }
        Ok(())
    }
}
