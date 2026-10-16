use bicdb_pgwire::{EmbeddedHaDatabase, HaServerConfig, HaWriterLease, PgWireConfig};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlResult, SqlSession, SqlValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

use crate::manifest::{Release, TableSync};

mod deployment;
mod jobs;
mod live;
pub use live::LiveQuery;
mod peer_grants;
mod preview;

const MIGRATIONS: &str = "__bic_platform_migrations";
const RECEIPTS: &str = "__bic_platform_receipts";
const OUTBOX: &str = "__bic_platform_outbox";
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
    /// Jobs persisted atomically with the SQL effects and delivered after commit.
    #[serde(default)]
    pub jobs: Vec<Value>,
}

pub struct Database {
    pub db: Arc<RwLock<BicDb>>,
    native: Option<Arc<EmbeddedHaDatabase>>,
    legacy_jobs_path: std::path::PathBuf,
    sync: BTreeMap<String, TableSync>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReplicaBinding {
    format: u32,
    cluster_id: String,
    node_id: String,
}
const REPLICA_BINDING: &str = "state-replica.json";

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        ensure!(
            !path.join(REPLICA_BINDING).exists(),
            "this application is bound to state replication; reopen with its replica configuration"
        );
        let db = BicDb::open_with_config(
            path.join("data"),
            DbConfig::default().with_fsync(true).with_audit_events(true),
        )?;
        let mut database = Self {
            db: Arc::new(RwLock::new(db)),
            native: None,
            legacy_jobs_path: path.join("jobs"),
            sync: BTreeMap::new(),
        };
        database.initialize()?;
        Ok(database)
    }

    /// Open a native state replica without exposing a SQL listener. Call
    /// prepare() once this replica holds a live primary lease. Streaming,
    /// promotion and restart follow the ordinary native HA lifecycle.
    pub fn open_replicated(path: &Path, ha: HaServerConfig) -> Result<Self> {
        let binding = ReplicaBinding {
            format: 1,
            cluster_id: ha.replication.cluster_id.clone(),
            node_id: ha.replication.node_id.clone(),
        };
        let binding_path = path.join(REPLICA_BINDING);
        let already_bound = binding_path.exists();
        if already_bound {
            let previous: ReplicaBinding =
                serde_json::from_slice(&crate::store::read_bounded(&binding_path, 16 * 1024)?)?;
            ensure!(
                previous == binding,
                "state replica identity differs from its persisted binding"
            );
        }
        let legacy = path.join("jobs");
        let native = Arc::new(EmbeddedHaDatabase::open_with_initializer(
            path.join("data"),
            PgWireConfig {
                fsync: true,
                high_availability: Some(ha),
                ..Default::default()
            },
            DbConfig::default().with_fsync(true).with_audit_events(true),
            move |db| {
                if legacy.exists() {
                    let migrated = SqlSession::new_shared(db)
                        .execute("SELECT complete FROM __bic_platform_job_migration WHERE id = 'legacy-v1'")
                        .is_ok_and(|result| result.rows.first().is_some_and(|row| row[0] == SqlValue::Bool(true)));
                    if !migrated {
                        return Err(bicdb_pgwire::PgWireError::Server("migrate the historical local job broker with the current standalone host before enabling state replication".into()));
                    }
                }
                // Historical standalone WAL can contain compact update patches
                // and omit empty collections. Checkpoint it before admitting
                // any peer, forcing new replicas to bootstrap the complete
                // durable image instead of replaying an incomplete old suffix.
                if !already_bound
                    && db.replication_role() == bicdb_core::NodeRole::Primary
                    && db.current_commit_seq() > 0
                {
                    db.compact()?;
                }
                crate::store::atomic_write(
                    &binding_path,
                    &serde_json::to_vec(&binding)
                        .map_err(|error| bicdb_pgwire::PgWireError::Server(error.to_string()))?,
                )
                .map_err(|error| bicdb_pgwire::PgWireError::Server(error.to_string()))?;
                Ok(())
            },
        )?);
        let database = Self {
            db: native.database(),
            native: Some(native),
            legacy_jobs_path: path.join("jobs"),
            sync: BTreeMap::new(),
        };
        Ok(database)
    }

    pub fn writer_lease(&self) -> Option<HaWriterLease> {
        self.native.as_ref().map(|host| host.writer_lease())
    }
    pub fn restart_required(&self) -> bool {
        self.native
            .as_ref()
            .is_some_and(|host| host.restart_required())
    }
    pub fn writable(&self) -> bool {
        self.writer_lease().is_none_or(|lease| lease.active())
            && !self.db.read().is_read_only_standby()
    }
    pub fn request_shutdown(&self) {
        if let Some(host) = &self.native {
            host.request_shutdown();
        }
    }

    fn ensure_authority(&self) -> Result<()> {
        ensure!(
            self.writer_lease().is_none_or(|lease| lease.active()),
            "state replica has no active writer authority"
        );
        Ok(())
    }

    fn acknowledge<T>(&self, result: Result<T>) -> Result<T> {
        let value = result?;
        if let Some(native) = &self.native {
            native.acknowledge_current()?;
        }
        Ok(value)
    }

    fn initialize(&mut self) -> Result<()> {
        self.initialize_table(
            MIGRATIONS,
            "position BIGINT PRIMARY KEY, name TEXT NOT NULL, digest TEXT NOT NULL",
        )?;
        self.initialize_table(
            RECEIPTS,
            "key TEXT PRIMARY KEY, digest TEXT NOT NULL, result TEXT NOT NULL",
        )?;
        self.initialize_table(
            OUTBOX,
            "id TEXT PRIMARY KEY, position BIGINT NOT NULL, payload TEXT NOT NULL",
        )?;
        self.initialize_job_ledger()
    }

    fn initialize_table(&self, table: &str, columns: &str) -> Result<()> {
        self.ensure_authority()?;
        let result = (|| -> Result<()> {
            let mut db = self.db.write();
            SqlSession::new(&mut db)
                .execute(&format!("CREATE TABLE IF NOT EXISTS {table} ({columns})"))?;
            Ok(())
        })();
        // Replication advances between independent private DDL operations.
        // Application migrations retain their own transactional boundaries.
        self.acknowledge(result)
    }

    pub fn prepare(&mut self, release: &Release) -> Result<()> {
        if self.native.is_some() {
            ensure!(
                self.writable(),
                "state replica has no active writer authority"
            );
            self.initialize()?;
        }
        self.sync = release.manifest.sync.clone();
        self.migrate(release)?;
        self.configure_sync(&release.manifest.sync)
    }

    pub fn migrate(&mut self, release: &Release) -> Result<()> {
        self.validate_replication_sync(&release.manifest.sync)?;
        self.ensure_authority()?;
        let result = self.migrate_local(release);
        self.acknowledge(result)
    }
    fn migrate_local(&mut self, release: &Release) -> Result<()> {
        let mut db = self.db.write();
        let node = db.node_id();
        let mut session = SqlSession::new(&mut db);
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
        self.query_with_deadline(
            batch,
            std::time::Instant::now() + std::time::Duration::from_secs(30),
        )
    }

    pub fn query_with_deadline(
        &mut self,
        batch: &QueryBatch,
        deadline: std::time::Instant,
    ) -> Result<Vec<SqlResult>> {
        self.ensure_authority()?;
        let result = self.query_with_deadline_local(batch, deadline);
        self.acknowledge(result)
    }
    fn query_with_deadline_local(
        &mut self,
        batch: &QueryBatch,
        deadline: std::time::Instant,
    ) -> Result<Vec<SqlResult>> {
        let db = self.db.read();
        let cancellation = bicdb_core::CancellationToken::new(
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Some(deadline),
        );
        cancellation.check()?;
        ensure!(
            (!batch.statements.is_empty() || !batch.jobs.is_empty())
                && batch.statements.len() <= 128
                && batch.jobs.len() <= 128,
            "invalid statement count"
        );
        for payload in &batch.jobs {
            ensure!(
                serde_json::to_vec(payload)?.len() <= 8 * 1024 * 1024,
                "job payload exceeds 8 MiB"
            );
        }
        // Retain the receipt digest of SQL-only requests across upgrades.
        let content = if batch.jobs.is_empty() {
            serde_json::to_vec(&batch.statements)?
        } else {
            serde_json::to_vec(
                &serde_json::json!({"statements":batch.statements,"jobs":batch.jobs}),
            )?
        };
        let digest = format!("{:x}", Sha256::digest(content));
        for query in &batch.statements {
            ensure!(
                query.sql.len() <= 128 * 1024 && query.params.len() <= 1024,
                "query too large"
            );
            let statements = Parser::parse_sql(&PostgreSqlDialect {}, &query.sql)?;
            Self::authorize_statements(&self.sync, &db.node_id(), &statements)?;
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
            let mut session = SqlSession::new_shared(&db).with_cancellation(cancellation.clone());
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
        let mut session = SqlSession::new_shared(&db).with_cancellation(cancellation.clone());
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
        let queued = (|| -> Result<()> {
            if batch.jobs.is_empty() {
                return Ok(());
            }
            let rows = session
                .execute(&format!("SELECT COALESCE(MAX(position), 0) FROM {OUTBOX}"))?
                .rows;
            let SqlValue::Int(position) = rows[0][0] else {
                bail!("invalid outbox position");
            };
            for (index, payload) in batch.jobs.iter().enumerate() {
                let id = uuid::Uuid::new_v4().to_string();
                let position = position
                    .checked_add(index as i64 + 1)
                    .context("outbox position exhausted")?;
                session.execute(&format!(
                    "INSERT INTO {OUTBOX} VALUES ({}, {position}, {})",
                    quote(&id),
                    quote(&serde_json::to_string(payload)?)
                ))?;
            }
            Ok(())
        })();
        if let Err(error) = queued {
            let _ = session.execute("ROLLBACK");
            return Err(error);
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
        self.validate_replication_sync(policy)?;
        self.ensure_authority()?;
        let result = self.configure_sync_local(policy);
        self.acknowledge(result)
    }

    fn validate_replication_sync(&self, policy: &BTreeMap<String, TableSync>) -> Result<()> {
        ensure!(
            self.native.is_none()
                || policy.is_empty()
                || self.db.read().replication_config().record_journal,
            "native edge synchronization requires journal-enabled state replication"
        );
        Ok(())
    }

    fn configure_sync_local(&mut self, policy: &BTreeMap<String, TableSync>) -> Result<()> {
        let mut db = self.db.write();
        for collection in db.collections() {
            let enabled = policy.contains_key(&collection.name);
            if collection.mesh_sync_enabled != enabled {
                db.set_collection_mesh_sync_enabled(&collection.name, enabled)?;
            }
        }
        for table in policy.keys() {
            ensure!(
                db.collections().iter().any(|c| &c.name == table),
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
        let public = sqlparser::ast::visit_relations(&statements.to_vec(), |name| {
            if name.0.iter().any(|part| match part {
                sqlparser::ast::ObjectNamePart::Identifier(id) => {
                    id.value.to_ascii_lowercase().starts_with("__bic")
                }
                _ => true,
            }) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        ensure!(public.is_continue(), "platform metadata is reserved");
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
        Ok(serde_json::to_value(
            self.db.read().list_record_conflicts(table)?,
        )?)
    }

    pub fn snapshot_jobs(&self, output: &Path, passphrase: &str) -> Result<()> {
        let jobs =
            BicDb::open_with_config(&self.legacy_jobs_path, DbConfig::default().with_fsync(true))?;
        bicdb_core::create_backup_from_open_database(
            &jobs,
            output,
            bicdb_core::BackupCreateOptions {
                passphrase: passphrase.into(),
                base_backup: None,
            },
        )?;
        Ok(())
    }
}
