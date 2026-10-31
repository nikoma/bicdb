//! Host-configured repositories, controlled by standalone superuser calls.
//! No SQL argument can supply a filesystem path or backup encryption key.
use super::*;
use bicdb_core::{
    BackupCreateOptions, OnlineBackupControl, OnlineBackupPhase, RepositoryBackupReport,
};
use std::collections::VecDeque;

#[derive(Clone)]
pub struct OnlineBackupConfig {
    pub directory: PathBuf,
    pub key: String,
}
impl std::fmt::Debug for OnlineBackupConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnlineBackupConfig")
            .field("directory", &self.directory)
            .field("key", &"<redacted>")
            .finish()
    }
}

type Outcome = std::result::Result<RepositoryBackupReport, String>;
#[derive(Debug)]
struct Job {
    id: Uuid,
    name: String,
    started_at: u64,
    control: OnlineBackupControl,
    outcome: Arc<Mutex<Option<Outcome>>>,
}

#[derive(Debug, Default)]
pub(crate) struct BackupJobs {
    jobs: VecDeque<Job>,
    restore_drill: bool,
}

impl BackupJobs {
    pub(crate) fn status(&self, id: Option<Uuid>) -> serde_json::Value {
        let rows = self
            .jobs
            .iter()
            .filter(|j| id.is_none_or(|id| j.id == id))
            .map(|job| {
                let outcome = job.outcome.lock().unwrap();
                let mut progress = job.control.progress();
                let phase = match outcome.as_ref() {
                    Some(Ok(_)) => OnlineBackupPhase::Complete,
                    Some(Err(_)) if progress.phase != OnlineBackupPhase::Cancelled => {
                        OnlineBackupPhase::Failed
                    }
                    None if matches!(
                        progress.phase,
                        OnlineBackupPhase::Complete
                            | OnlineBackupPhase::Failed
                            | OnlineBackupPhase::Cancelled
                    ) =>
                    {
                        OnlineBackupPhase::Publishing
                    }
                    _ => progress.phase,
                };
                progress.phase = phase;
                serde_json::json!({"id": job.id, "name": job.name, "started_at": job.started_at,
                "phase": phase, "progress": progress,
                "result": outcome.as_ref().and_then(|r| r.as_ref().ok()),
                "error": outcome.as_ref().and_then(|r| r.as_ref().err())})
            })
            .collect::<Vec<_>>();
        serde_json::json!(rows)
    }
}

pub(crate) fn cancel_all(server: &PgWireServer) {
    for job in &server.backup_jobs.lock().unwrap().jobs {
        job.control.cancel();
    }
}

fn name(value: &str) -> Result<String> {
    if value.is_empty()
        || value.len() > 128
        || value.starts_with('.')
        || value.contains("..")
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(PgWireError::QueryRejected("backup names require 1..128 ASCII letters, digits, dots, dashes or underscores; no paths".into()));
    }
    Ok(if value.ends_with(".bicbackup") {
        value.into()
    } else {
        format!("{value}.bicbackup")
    })
}

fn settings(server: &PgWireServer) -> Result<OnlineBackupConfig> {
    let mut config = server.config.online_backups.clone().or_else(|| {
        server.config.log_archive.as_ref().and_then(|c| c.base_backup_key.as_ref().map(|key| OnlineBackupConfig {
            directory: c.dir.clone(), key: key.clone(),
        }))
    }).ok_or_else(|| PgWireError::QueryRejected("configure --backup-repository and --backup-key-env, or an encrypted log archive, before starting online backups".into()))?;
    if server.cluster.is_some() {
        config.directory = config.directory.join(
            server
                .read_db()?
                .data_path()
                .file_name()
                .unwrap_or_default(),
        );
    }
    if config.key.is_empty() {
        return Err(PgWireError::QueryRejected(
            "backup encryption key is empty".into(),
        ));
    }
    server.config.online_backup_limits.validate()?;
    Ok(config)
}

fn start(
    server: &Arc<PgWireServer>,
    requested: &str,
    parent: Option<&str>,
) -> Result<serde_json::Value> {
    let config = settings(server)?;
    let name = name(requested)?;
    let parent = parent.map(name_from_parent).transpose()?;
    if parent.as_ref() == Some(&name) {
        return Err(PgWireError::QueryRejected(
            "backup and parent names must differ".into(),
        ));
    }
    let mut jobs = server.backup_jobs.lock().unwrap();
    if server.is_shutdown_requested() {
        return Err(PgWireError::QueryRejected("server is shutting down".into()));
    }
    if jobs.restore_drill
        || jobs
            .jobs
            .iter()
            .any(|job| job.outcome.lock().unwrap().is_none())
    {
        return Err(PgWireError::QueryRejected(
            "an online backup or restore drill is already running".into(),
        ));
    }
    while jobs.jobs.len() >= 64 {
        jobs.jobs.pop_front();
    }
    let permit = admit(server)?;
    let id = Uuid::new_v4();
    let control = OnlineBackupControl::default();
    let outcome = Arc::new(Mutex::new(None));
    let job = Job {
        id,
        name: name.clone(),
        started_at: unix_timestamp().max(0) as u64,
        control: control.clone(),
        outcome: Arc::clone(&outcome),
    };
    let path = config.directory.join(&name);
    let options = BackupCreateOptions {
        passphrase: config.key.clone(),
        base_backup: parent.map(|p| config.directory.join(p)),
    };
    let worker = BackgroundWorker::register(Arc::clone(server));
    let running = Arc::clone(server);
    let limits = server.config.online_backup_limits.clone();
    let spawn = thread::Builder::new()
        .name("bicdb-online-backup".into())
        .spawn(move || {
            let _worker = worker;
            let _permit = permit;
            let result = (|| {
                // The read guard is released at the end of this block. In
                // particular it is never retained across copy/encryption/upload.
                let capture = { running.read_db()?.capture_online_backup(limits)? };
                capture
                    .write_backup(path, options, control)
                    .map_err(PgWireError::from)
            })();
            *outcome.lock().unwrap() = Some(result.map_err(|e| e.to_string()));
        });
    if let Err(error) = spawn {
        return Err(error.into());
    }
    jobs.jobs.push_back(job);
    Ok(serde_json::json!({"accepted": true, "id": id, "name": name}))
}

fn name_from_parent(value: &str) -> Result<String> {
    name(value)
}

/// Automatic bases use the same single-job admission and shutdown cancellation
/// as operator requests. Busy is a normal retry, not a failed restore drill.
pub(crate) fn archive_base(
    server: &Arc<PgWireServer>,
    directory: &Path,
    key: &str,
) -> Result<Option<RepositoryBackupReport>> {
    let permit;
    let id = Uuid::new_v4();
    let control = OnlineBackupControl::default();
    let outcome = Arc::new(Mutex::new(None));
    {
        let mut jobs = server.backup_jobs.lock().unwrap();
        if jobs.restore_drill
            || jobs
                .jobs
                .iter()
                .any(|job| job.outcome.lock().unwrap().is_none())
        {
            return Ok(None);
        }
        if server.is_shutdown_requested() {
            return Ok(None);
        }
        // Defer first when another job owns the slot: reserving here keeps
        // a normal busy retry from reporting a resource-exhaustion failure.
        permit = admit(server)?;
        while jobs.jobs.len() >= 64 {
            jobs.jobs.pop_front();
        }
        jobs.jobs.push_back(Job {
            id,
            name: "automatic archive base".into(),
            started_at: unix_timestamp().max(0) as u64,
            control: control.clone(),
            outcome: Arc::clone(&outcome),
        });
    }
    let _permit = permit;
    let result = (|| -> Result<RepositoryBackupReport> {
        let capture = {
            server
                .read_db()?
                .capture_online_backup(server.config.online_backup_limits.clone())?
        };
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let path = directory.join(format!(
            "base-{:020}-{millis:020}-{id}.bicbackup",
            capture.commit_seq()
        ));
        {
            let mut jobs = server.backup_jobs.lock().unwrap();
            if let Some(job) = jobs.jobs.iter_mut().find(|j| j.id == id) {
                job.name = path.file_name().unwrap().to_string_lossy().into_owned();
            }
        }
        let parent = bicdb_core::log_archive::list_base_backups(directory)?
            .into_iter()
            .rev()
            .find(|base| bicdb_core::is_repository_backup(&base.path).unwrap_or(false))
            .map(|base| base.path);
        capture
            .write_backup(
                path,
                BackupCreateOptions {
                    passphrase: key.into(),
                    base_backup: parent,
                },
                control,
            )
            .map_err(PgWireError::from)
    })();
    *outcome.lock().unwrap() = Some(
        result
            .as_ref()
            .map(Clone::clone)
            .map_err(ToString::to_string),
    );
    result.map(Some)
}

pub(crate) fn execute(
    server: &Arc<PgWireServer>,
    state: &ConnectionState,
    sql: &str,
) -> Result<Option<SqlResult>> {
    use sqlparser::{
        ast::{
            Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr, Statement,
            Value,
        },
        dialect::PostgreSqlDialect,
        parser::Parser,
    };
    if !sql.to_ascii_lowercase().contains("bicdb_backup_") {
        return Ok(None);
    }
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|e| PgWireError::Protocol(e.to_string()))?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Ok(None);
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    let [SelectItem::UnnamedExpr(Expr::Function(function))] = select.projection.as_slice() else {
        return Ok(None);
    };
    let function_name = function.name.to_string();
    let operation = function_name.to_ascii_lowercase();
    if !matches!(
        operation.as_str(),
        "bicdb_backup_start" | "bicdb_backup_status" | "bicdb_backup_cancel"
    ) {
        return Ok(None);
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(PgWireError::Protocol(
            "backup calls need literal arguments".into(),
        ));
    };
    let args = arguments
        .args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) => match &value.value {
                Value::SingleQuotedString(value) => Ok(value.clone()),
                _ => Err(PgWireError::Protocol(
                    "backup arguments must be text literals".into(),
                )),
            },
            _ => Err(PgWireError::Protocol(
                "backup arguments must be text literals".into(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    let canonical = format!(
        "SELECT {function_name}({})",
        args.iter()
            .map(|s| format!("'{}'", s.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ")
    );
    if statements[0].to_string() != canonical
        || state.in_transaction
        || state.delegated_identity.is_some()
    {
        return Err(PgWireError::QueryRejected(
            "backup control requires a standalone SELECT outside a transaction and delegation"
                .into(),
        ));
    }
    let gucs = state.session_state.session_gucs();
    let role = gucs
        .get("role")
        .map(String::as_str)
        .filter(|role| !role.trim().is_empty() && !role.eq_ignore_ascii_case("none"))
        .or_else(|| {
            gucs.get("session_authorization")
                .map(String::as_str)
                .filter(|role| !role.trim().is_empty())
        })
        .unwrap_or(&state.user);
    if !server.user_is_admin(role) {
        return Err(PgWireError::BicDb(BicDbError::Authorization(
            "backup control requires a superuser".into(),
        )));
    }
    let value = match (operation.as_str(), args.as_slice()) {
        ("bicdb_backup_start", [name]) => start(server, name, None)?,
        ("bicdb_backup_start", [name, parent]) => start(server, name, Some(parent))?,
        ("bicdb_backup_status", []) => server.backup_jobs.lock().unwrap().status(None),
        ("bicdb_backup_status", [id]) => {
            let id = Uuid::parse_str(id).map_err(|_| PgWireError::QueryRejected("invalid backup job id".into()))?;
            server.backup_jobs.lock().unwrap().status(Some(id))
        },
        ("bicdb_backup_cancel", [id]) => {
            let id = Uuid::parse_str(id).map_err(|_| PgWireError::QueryRejected("invalid backup job id".into()))?;
            let jobs = server.backup_jobs.lock().unwrap();
            let job = jobs.jobs.iter().find(|j| j.id == id).ok_or_else(|| PgWireError::QueryRejected("unknown backup job".into()))?;
            let running = job.outcome.lock().unwrap().is_none();
            if running { job.control.cancel(); }
            serde_json::json!({"id": id, "cancellation_requested": running})
        },
        _ => return Err(PgWireError::Protocol("backup_start needs a name and optional parent; backup_status takes an optional job id; backup_cancel needs a job id".into())),
    };
    let json = serde_json::to_string(&value).map_err(|e| PgWireError::Server(e.to_string()))?;
    Ok(Some(
        SqlResult::new(vec![operation], vec![vec![SqlValue::String(json)]])
            .with_column_types(vec![Some("text".into())]),
    ))
}

pub(crate) fn metric_values(server: &PgWireServer) -> Vec<(&'static str, u64)> {
    let jobs = server.backup_jobs.lock().unwrap();
    let Some(job) = jobs.jobs.back() else {
        return vec![("bicdb_backup_running", 0)];
    };
    let outcome = job.outcome.lock().unwrap();
    let progress = job.control.progress();
    vec![
        ("bicdb_backup_running", u64::from(outcome.is_none())),
        (
            "bicdb_backup_last_success",
            u64::from(outcome.as_ref().is_some_and(|r| r.is_ok())),
        ),
        ("bicdb_backup_source_bytes_read", progress.source_bytes_read),
        ("bicdb_backup_uploaded_bytes", progress.uploaded_bytes),
        ("bicdb_backup_reused_bytes", progress.reused_bytes),
        ("bicdb_backup_files_copied", progress.files_copied),
        ("bicdb_backup_files_total", progress.files_total),
        (
            "bicdb_backup_page_extents_reused",
            outcome
                .as_ref()
                .and_then(|r| r.as_ref().ok())
                .map_or(0, |r| r.page_extents_reused),
        ),
        (
            "bicdb_backup_page_extents_copied",
            outcome
                .as_ref()
                .and_then(|r| r.as_ref().ok())
                .map_or(0, |r| r.page_extents_copied),
        ),
        (
            "bicdb_backup_coordination_milliseconds",
            outcome
                .as_ref()
                .and_then(|r| r.as_ref().ok())
                .map_or(0, |r| r.coordination_millis),
        ),
    ]
}

fn admit(server: &PgWireServer) -> Result<bicdb_core::ResourcePermit> {
    use bicdb_core::{ResourceDemand, ResourceLane};
    let limits = &server.config.online_backup_limits;
    let memory_bytes = (limits.max_manifest_bytes as u64)
        .saturating_mul(4)
        .saturating_add((limits.page_images.max_before_image_pages as u64).saturating_mul(96))
        .saturating_add((limits.max_files as u64).saturating_mul(512))
        .saturating_add(64 * 1024 * 1024);
    server
        .resource_governor
        .try_admit(
            ResourceLane::BackupRestore,
            ResourceDemand {
                memory_bytes,
                io_bytes: 8 * 1024 * 1024,
                cpu_slots: 1,
                io_charge_bytes: 1024 * 1024,
            },
            cluster_now_ms(),
        )
        .map_err(PgWireError::from)
}

#[cfg(test)]
mod archive_admission_tests {
    use super::*;
    #[test]
    fn busy_archive_defers_before_reserving_another_background_slot() {
        let root = tempfile::tempdir().unwrap();
        let mut config = PgWireConfig::default();
        config.fsync = false;
        config.automatic_paged_checkpoint = false;
        config
            .resource_governor
            .lanes
            .get_mut(&bicdb_core::ResourceLane::BackupRestore)
            .unwrap()
            .max_active = 1;
        let server = PgWireServer::open(root.path().join("db"), config).unwrap();
        let _active = admit(&server).unwrap();
        server.backup_jobs.lock().unwrap().jobs.push_back(Job {
            id: Uuid::new_v4(),
            name: "running operator backup".into(),
            started_at: 0,
            control: OnlineBackupControl::default(),
            outcome: Arc::new(Mutex::new(None)),
        });
        let result = archive_base(&server, &root.path().join("repository"), "key");
        assert!(
            matches!(result, Ok(None)),
            "busy archive must defer rather than fail resource admission: {result:?}"
        );
    }
}

/// Restore drills and immutable-object publication share repository ownership.
/// Reserve admission, rather than holding the jobs mutex over either operation.
pub(crate) struct RestoreDrillGuard(Arc<PgWireServer>);
impl Drop for RestoreDrillGuard {
    fn drop(&mut self) {
        self.0.backup_jobs.lock().unwrap().restore_drill = false;
    }
}
pub(crate) fn begin_restore_drill(server: &Arc<PgWireServer>) -> Option<RestoreDrillGuard> {
    let mut jobs = server.backup_jobs.lock().unwrap();
    if jobs.restore_drill
        || jobs
            .jobs
            .iter()
            .any(|job| job.outcome.lock().unwrap().is_none())
        || server.is_shutdown_requested()
    {
        return None;
    }
    jobs.restore_drill = true;
    Some(RestoreDrillGuard(Arc::clone(server)))
}

#[cfg(test)]
mod restore_drill_admission_tests {
    use super::*;
    #[test]
    fn restore_drill_reserves_admission_without_holding_the_jobs_mutex() {
        let root = tempfile::tempdir().unwrap();
        let mut config = PgWireConfig::default();
        config.fsync = false;
        config.online_backups = Some(OnlineBackupConfig {
            directory: root.path().join("repository"),
            key: "key".into(),
        });
        let server = PgWireServer::open(root.path().join("db"), config).unwrap();
        let guard = begin_restore_drill(&server).unwrap();
        assert!(begin_restore_drill(&server).is_none());
        assert!(start(&server, "blocked", None).is_err());
        assert!(matches!(
            archive_base(&server, &root.path().join("repository"), "key"),
            Ok(None)
        ));
        assert!(server
            .backup_jobs
            .lock()
            .unwrap()
            .status(None)
            .as_array()
            .unwrap()
            .is_empty());
        drop(guard);
        assert!(begin_restore_drill(&server).is_some());
        assert!(!root.path().join("repository").exists());
        server.request_shutdown();
    }
}
