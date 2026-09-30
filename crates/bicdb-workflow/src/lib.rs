//! Trusted embedding APIs for script publication and transactional scheduling.
//! Network-facing callers must authorize management and reconstruct execution
//! authority from trusted job context, never from the event's JSON fields.
use bicdb_core::{BicDb, PublishOptions, Record, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const VERSION_COLLECTION: &str = "bicdb_script_versions";
pub const ACTIVE_COLLECTION: &str = "bicdb_script_active";
pub const JOB_COLLECTION: &str = "bicdb_script_jobs";
pub const QUEUE: &str = "bicdb.workflows";

#[derive(Debug, thiserror::Error)]
#[error("Workflow rejected: {0}")]
pub struct Error(pub String);
type Result<T> = std::result::Result<T, Error>;
fn error(e: impl std::fmt::Display) -> Error {
    Error(e.to_string())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    Lua,
    TypeScript,
    JavaScript,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptVersion {
    pub tenant: String,
    pub workflow: String,
    pub version: String,
    pub language: Language,
    /// TypeScript publication stores JavaScript here; Lua retains Lua source.
    pub executable: String,
    pub executable_sha256: String,
    pub source_sha256: String,
    pub compiler: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobContext {
    pub tenant: String,
    pub principal: String,
    pub roles: Vec<String>,
    pub trace_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowJob {
    pub tenant: String,
    pub workflow: String,
    pub version: String,
    pub event_id: String,
    pub event: Value,
    pub context: JobContext,
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn key(parts: &[&str]) -> String {
    hash(&serde_json::to_vec(parts).expect("strings serialize"))
}
fn json_hash(value: &impl Serialize) -> Result<String> {
    fn canonical(value: Value) -> Value {
        match value {
            Value::Object(map) => Value::Object(
                map.into_iter()
                    .collect::<std::collections::BTreeMap<_, _>>()
                    .into_iter()
                    .map(|(key, value)| (key, canonical(value)))
                    .collect(),
            ),
            Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
            other => other,
        }
    }
    Ok(hash(
        &serde_json::to_vec(&canonical(serde_json::to_value(value).map_err(error)?))
            .map_err(error)?,
    ))
}
fn names(parts: &[&str]) -> Result<()> {
    if parts
        .iter()
        .any(|s| s.is_empty() || s.len() > 256 || s.chars().any(char::is_control))
    {
        return Err(Error("invalid workflow identity".into()));
    }
    Ok(())
}

pub fn initialize(db: &mut BicDb) -> Result<()> {
    for collection in [VERSION_COLLECTION, ACTIVE_COLLECTION, JOB_COLLECTION] {
        if !db.collections().iter().any(|meta| meta.name == collection) {
            db.create_collection(collection).map_err(error)?;
        }
    }
    Ok(())
}

pub fn publish_version(
    db: &mut BicDb,
    tenant: &str,
    workflow: &str,
    version: &str,
    language: Language,
    source: &str,
    limits: &bicdb_script::Limits,
) -> Result<ScriptVersion> {
    names(&[tenant, workflow, version])?;
    if source.len() > limits.source_bytes {
        return Err(Error("source limit exceeded".into()));
    }
    let (executable, compiler) = match language {
        Language::TypeScript => {
            let receipt = bicdb_script::compile_typescript(source, limits).map_err(error)?;
            (receipt.javascript, receipt.compiler)
        }
        Language::JavaScript => {
            // The TS parser accepts JS; publication validates without executing.
            let receipt = bicdb_script::compile_typescript(source, limits).map_err(error)?;
            if receipt.javascript.is_empty() {
                return Err(Error("empty script".into()));
            }
            (receipt.javascript, receipt.compiler)
        }
        Language::Lua => {
            bicdb_lua::validate(
                source.as_bytes(),
                &bicdb_lua::Limits {
                    source_bytes: limits.source_bytes,
                    memory_bytes: limits.memory_bytes,
                    ..Default::default()
                },
            )
            .map_err(error)?;
            (source.to_owned(), "lua/5.1".into())
        }
    };
    let script = ScriptVersion {
        tenant: tenant.into(),
        workflow: workflow.into(),
        version: version.into(),
        language,
        executable_sha256: hash(executable.as_bytes()),
        executable,
        source_sha256: hash(source.as_bytes()),
        compiler,
    };
    let id = key(&[tenant, workflow, version]);
    let mut tx = db.begin_transaction().map_err(error)?;
    if let Some(existing) = tx.get(VERSION_COLLECTION, &id).map_err(error)? {
        let existing: ScriptVersion =
            serde_json::from_value(existing.metadata.clone()).map_err(error)?;
        if existing.source_sha256 != script.source_sha256 || existing.language != script.language {
            return Err(Error("script versions are immutable".into()));
        }
        tx.rollback().map_err(error)?;
        return Ok(existing);
    }
    tx.insert(
        VERSION_COLLECTION,
        Record::new(id).with_metadata(serde_json::to_value(&script).map_err(error)?),
    )
    .map_err(error)?;
    tx.commit().map_err(error)?;
    Ok(script)
}

pub fn activate_version(
    tx: &mut Transaction,
    tenant: &str,
    workflow: &str,
    version: &str,
) -> Result<()> {
    names(&[tenant, workflow, version])?;
    if tx
        .get(VERSION_COLLECTION, &key(&[tenant, workflow, version]))
        .map_err(error)?
        .is_none()
    {
        return Err(Error("script version does not exist".into()));
    }
    tx.insert(
        ACTIVE_COLLECTION,
        Record::new(key(&[tenant, workflow])).with_metadata(serde_json::json!({"version":version})),
    )
    .map_err(error)
}

pub fn enqueue_on_commit(
    tx: &mut Transaction,
    workflow: &str,
    event_id: &str,
    event: Value,
    context: JobContext,
) -> Result<uuid::Uuid> {
    names(&[
        &context.tenant,
        workflow,
        event_id,
        &context.principal,
        &context.trace_id,
    ])?;
    if context.roles.len() > 128 || context.roles.iter().any(|r| r.is_empty() || r.len() > 256) {
        return Err(Error("invalid trusted roles".into()));
    }
    let job_key = key(&[&context.tenant, workflow, event_id]);
    let event_hash = json_hash(&event)?;
    if let Some(existing) = tx.get(JOB_COLLECTION, &job_key).map_err(error)? {
        if existing.metadata["event_sha256"] != event_hash
            || existing.metadata["principal"] != context.principal
        {
            return Err(Error(
                "event id reused with different payload or principal".into(),
            ));
        }
        return existing.metadata["message_id"]
            .as_str()
            .ok_or_else(|| Error("invalid stored job receipt".into()))?
            .parse()
            .map_err(error);
    }
    let active = tx
        .get(ACTIVE_COLLECTION, &key(&[&context.tenant, workflow]))
        .map_err(error)?
        .ok_or_else(|| Error("no active script version".into()))?;
    let version = active.metadata["version"]
        .as_str()
        .ok_or_else(|| Error("invalid active version".into()))?;
    let job = WorkflowJob {
        tenant: context.tenant.clone(),
        workflow: workflow.into(),
        version: version.into(),
        event_id: event_id.into(),
        event,
        context,
    };
    let payload = serde_json::to_value(&job).map_err(error)?;
    if serde_json::to_vec(&payload).map_err(error)?.len() > 1024 * 1024 {
        return Err(Error("job payload limit exceeded".into()));
    }
    let message_id = tx
        .buffer_broker_publish(
            QUEUE,
            payload,
            PublishOptions {
                idempotency_key: Some(job_key.clone()),
                max_attempts: Some(10),
                ..Default::default()
            },
        )
        .map_err(error)?;
    tx.insert(JOB_COLLECTION, Record::new(job_key).with_metadata(serde_json::json!({
        "event_sha256":event_hash, "principal":job.context.principal, "message_id":message_id.to_string(),
        "job_sha256":json_hash(&job)?
    }))).map_err(error)?;
    Ok(message_id)
}

pub fn load_job_script(db: &BicDb, job: &WorkflowJob) -> Result<ScriptVersion> {
    validate_job_receipt(db, job, None)?;
    names(&[&job.tenant, &job.workflow, &job.version])?;
    if job.context.tenant != job.tenant
        || job.context.principal.is_empty()
        || job.context.trace_id.is_empty()
    {
        return Err(Error("job context mismatch".into()));
    }
    let record = db
        .get(
            VERSION_COLLECTION,
            &key(&[&job.tenant, &job.workflow, &job.version]),
        )
        .map_err(error)?
        .ok_or_else(|| Error("pinned script version missing".into()))?;
    let script: ScriptVersion = serde_json::from_value(record.metadata.clone()).map_err(error)?;
    if script.tenant != job.tenant
        || script.workflow != job.workflow
        || script.version != job.version
        || hash(script.executable.as_bytes()) != script.executable_sha256
    {
        return Err(Error("script integrity or scope mismatch".into()));
    }
    Ok(script)
}

/// Generic broker publishers cannot mint workflow execution authority. Match
/// the native receipt committed with the booking; delivery workers also bind
/// its broker message ID so copied envelopes cannot start additional jobs.
pub fn validate_job_receipt(
    db: &BicDb,
    job: &WorkflowJob,
    message_id: Option<uuid::Uuid>,
) -> Result<()> {
    let receipt = db
        .get(
            JOB_COLLECTION,
            &key(&[&job.tenant, &job.workflow, &job.event_id]),
        )
        .map_err(error)?
        .ok_or_else(|| Error("trusted workflow job receipt missing".into()))?;
    if receipt.metadata["job_sha256"] != json_hash(job)?
        || message_id.is_some_and(|id| receipt.metadata["message_id"] != id.to_string())
    {
        return Err(Error("workflow job does not match trusted receipt".into()));
    }
    Ok(())
}
