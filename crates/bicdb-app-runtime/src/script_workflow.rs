//! Lua/JS workflows use the existing signed capability host, not a second
//! SQL/HTTP authorization implementation. Stored jobs are execution inputs;
//! the caller supplies a freshly authenticated and authorized CapabilityHost.
use crate::CapabilityHost;
use bicdb_extension::abi_v2::{
    DatabaseAction, DatabaseRequest, EgressRequest, HostCall, HostHandle, HostRequest, HostValue,
    IsolationLevel, SecretRequest, TransactionRequest,
};
use bicdb_extension::host::ApplicationHost;
use bicdb_workflow::{Language, ScriptVersion, WorkflowJob};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(test)]
#[path = "script_route_tests.rs"]
mod route_tests;
#[cfg(test)]
#[path = "script_workflow_tests.rs"]
mod tests;

#[derive(Clone, Debug, Default)]
pub struct ScriptWorkflowOptions {
    pub limits: bicdb_script::Limits,
    /// A declared egress policy from the signed application manifest.
    pub egress_policy: Option<String>,
}
#[derive(Clone, Debug, PartialEq)]
pub enum ScriptWorkflowOutcome {
    Completed(Value),
    Retry { delay_ms: u64 },
}

struct Host {
    capability: CapabilityHost,
    transaction: Option<HostHandle>,
    request_id: u64,
    retry: Option<u64>,
    /// Durable workflow jobs may request a retry; HTTP route handlers cannot.
    allow_retry: bool,
    options: ScriptWorkflowOptions,
}
type Result<T> = std::result::Result<T, String>;
fn now_ms() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_millis()
        .try_into()
        .map_err(|_| "clock overflow".into())
}
fn arguments(value: Value) -> Result<Vec<Value>> {
    match value {
        Value::Array(a) => Ok(a),
        Value::Object(o) if o.is_empty() => Ok(vec![]),
        _ => Err("arguments must be an array".into()),
    }
}
fn text(value: &Value) -> Result<String> {
    value
        .as_str()
        .filter(|s| s.len() <= 1024 * 1024)
        .map(str::to_owned)
        .ok_or_else(|| "expected bounded string".into())
}
impl Host {
    fn request(&mut self, request: HostRequest) -> Result<HostValue> {
        self.request_id += 1;
        let response = self.capability.call(HostCall {
            request_id: self.request_id,
            request,
        });
        if let Some(error) = response.error {
            return Err(format!("{}: {}", error.code, error.message));
        }
        response
            .value
            .ok_or_else(|| "empty capability response".into())
    }
    fn begin(&mut self) -> Result<HostHandle> {
        match self.request(HostRequest::Transaction(TransactionRequest::Begin {
            isolation: IsolationLevel::Serializable,
        }))? {
            HostValue::Handle(handle) => Ok(handle),
            _ => Err("invalid transaction response".into()),
        }
    }
    fn rollback(&mut self) -> Result<()> {
        if let Some(transaction) = self.transaction.take() {
            self.capability
                .rollback_script_transaction(transaction)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    fn sql(&mut self, method: &str, args: Vec<Value>) -> Result<Value> {
        if args.len() != 2 {
            return Err("SQL requires statement and bound parameters".into());
        }
        let statement = text(&args[0])?;
        let parameters = arguments(args[1].clone())?;
        let declaration = self
            .capability
            .application()
            .raw_sql
            .iter()
            .find(|decl| decl.id == statement || decl.sql.trim() == statement.trim())
            .cloned()
            .ok_or_else(|| "SQL statement is not declared in the signed manifest".to_string())?;
        if !method.ends_with("execute")
            && declaration.actions.iter().any(|a| {
                !matches!(
                    a,
                    DatabaseAction::Select | DatabaseAction::Lock | DatabaseAction::RawSql
                )
            })
        {
            return Err("read method cannot execute a mutation".into());
        }
        let owned = method.starts_with("db.");
        let transaction = if owned {
            if self.transaction.is_some() {
                return Err("use tx methods inside a transaction".into());
            }
            let handle = self.begin()?;
            self.transaction = Some(handle);
            handle
        } else {
            self.transaction
                .ok_or_else(|| "no active transaction".to_string())?
        };
        let result = self.request(HostRequest::Database(DatabaseRequest::RawSql {
            transaction,
            statement_id: declaration.id,
            parameters,
        }));
        let result = result.and_then(|value| match (method.rsplit('.').next(), value) {
            (Some("one"), HostValue::Rows(mut rows)) if rows.len() <= 1 => {
                Ok(rows.pop().unwrap_or(Value::Null))
            }
            (Some("scalar"), HostValue::Rows(mut rows)) if rows.len() <= 1 => {
                if rows.is_empty() {
                    return Ok(Value::Null);
                }
                let row = rows.pop().unwrap();
                let object = row
                    .as_object()
                    .filter(|o| o.len() == 1)
                    .ok_or_else(|| "scalar requires one column".to_string())?;
                Ok(object.values().next().unwrap().clone())
            }
            (Some("execute"), HostValue::U64(n)) => Ok(json!(n)),
            (Some("execute"), HostValue::Rows(rows)) => Ok(json!(rows)),
            _ => Err("unexpected SQL result shape".into()),
        });
        if owned {
            match result {
                Ok(value) => {
                    self.request(HostRequest::Transaction(TransactionRequest::Commit {
                        transaction,
                    }))?;
                    self.transaction = None;
                    Ok(value)
                }
                Err(error) => {
                    self.rollback()?;
                    Err(error)
                }
            }
        } else {
            result
        }
    }
    fn call(&mut self, method: &str, value: Value) -> Result<Value> {
        if serde_json::to_vec(&value).map_err(|e| e.to_string())?.len()
            > self
                .options
                .limits
                .result_bytes
                .min(self.options.limits.source_bytes)
        {
            return Err("workflow host argument limit exceeded".into());
        }
        if now_ms()? >= self.capability.actor().deadline_unix_ms {
            return Err("workflow deadline exceeded".into());
        }
        if self.retry.is_some() {
            return Err("retry already requested".into());
        }
        let args = arguments(value)?;
        match method {
            "db.one" | "db.scalar" | "db.execute" | "tx.one" | "tx.scalar" | "tx.execute" => {
                self.sql(method, args)
            }
            "db.begin" if args.is_empty() => {
                if self.transaction.is_some() {
                    return Err("nested transactions are forbidden".into());
                }
                self.transaction = Some(self.begin()?);
                Ok(Value::Null)
            }
            "db.commit" if args.is_empty() => {
                let transaction = self
                    .transaction
                    .ok_or_else(|| "no active transaction".to_string())?;
                self.request(HostRequest::Transaction(TransactionRequest::Commit {
                    transaction,
                }))?;
                self.transaction = None;
                Ok(Value::Null)
            }
            "db.rollback" if args.is_empty() => {
                self.rollback()?;
                Ok(Value::Null)
            }
            "jobs.retry" if args.len() == 1 => {
                if !self.allow_retry {
                    return Err("jobs.retry is only available to durable workflow jobs".into());
                }
                if self.transaction.is_some() {
                    return Err("retry cannot be requested inside a transaction".into());
                }
                let object = args[0]
                    .as_object()
                    .ok_or_else(|| "invalid retry options".to_string())?;
                if object.len() != 1 {
                    return Err("retry requires delay_seconds only".into());
                }
                let seconds = object["delay_seconds"]
                    .as_u64()
                    .filter(|n| *n > 0 && *n <= 86_400)
                    .ok_or_else(|| "retry delay must be 1..86400 seconds".to_string())?;
                self.retry = Some(seconds * 1000);
                Ok(Value::Null)
            }
            "secrets.get" if args.len() == 1 => {
                let handle = match self.request(HostRequest::Secret(SecretRequest::Open {
                    name: text(&args[0])?,
                    version: None,
                }))? {
                    HostValue::Handle(handle) => handle,
                    _ => return Err("invalid secret handle".into()),
                };
                match self.request(HostRequest::Secret(SecretRequest::ReadPlaintext {
                    secret: handle,
                }))? {
                    HostValue::Bytes(bytes) => String::from_utf8(bytes)
                        .map(Value::String)
                        .map_err(|e| e.to_string()),
                    HostValue::String(value) => Ok(Value::String(value)),
                    _ => Err("invalid secret value".into()),
                }
            }
            "http.post" if args.len() == 2 => {
                if self.transaction.is_some() {
                    return Err("HTTP is forbidden inside database transactions".into());
                }
                let policy = self
                    .options
                    .egress_policy
                    .clone()
                    .ok_or_else(|| "HTTP is not enabled".to_string())?;
                let options = args[1]
                    .as_object()
                    .ok_or_else(|| "invalid HTTP options".to_string())?;
                if options
                    .keys()
                    .any(|k| !matches!(k.as_str(), "headers" | "json" | "timeout_ms"))
                {
                    return Err("unknown HTTP option".into());
                }
                let timeout = options
                    .get("timeout_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(8000);
                if timeout == 0 || timeout > 30_000 {
                    return Err("HTTP timeout must be 1..30000 ms".into());
                }
                let mut headers = Vec::new();
                if let Some(value) = options.get("headers") {
                    for (name, value) in value
                        .as_object()
                        .ok_or_else(|| "invalid HTTP headers".to_string())?
                    {
                        headers.push((name.clone(), text(value)?));
                    }
                }
                headers.push(("Content-Type".into(), "application/json".into()));
                let body = serde_json::to_vec(options.get("json").unwrap_or(&Value::Null))
                    .map_err(|e| e.to_string())?;
                let deadline =
                    (now_ms()? + timeout as i64).min(self.capability.actor().deadline_unix_ms);
                match self.request(HostRequest::Egress(EgressRequest::Http {
                    policy,
                    method: "POST".into(),
                    url: text(&args[0])?,
                    headers,
                    body,
                    deadline_unix_ms: deadline,
                }))? {
                    HostValue::EgressResponse(response) => {
                        if response.body.len() > self.options.limits.result_bytes {
                            return Err("HTTP response limit exceeded".into());
                        }
                        Ok(
                            json!({"status":response.status,"body":String::from_utf8(response.body).map_err(|e|e.to_string())?}),
                        )
                    }
                    _ => Err("invalid HTTP response".into()),
                }
            }
            _ => Err("unsupported workflow host method or arguments".into()),
        }
    }
}

/// Execute a pinned script with fresh authority. Role snapshots in the job are
/// deliberately ignored; the supplied capability host must revalidate them.
pub fn execute_script_workflow(
    script: &ScriptVersion,
    job: &WorkflowJob,
    mut capability: CapabilityHost,
    mut options: ScriptWorkflowOptions,
) -> Result<ScriptWorkflowOutcome> {
    let actor = capability.actor();
    if actor.tenant_id.as_deref() != Some(job.tenant.as_str())
        || !(actor.user_id.as_deref() == Some(job.context.principal.as_str())
            || actor.service_id.as_deref() == Some(job.context.principal.as_str()))
        || job.context.tenant != job.tenant
        || script.tenant != job.tenant
        || script.workflow != job.workflow
        || script.version != job.version
        || format!("{:x}", Sha256::digest(script.executable.as_bytes())) != script.executable_sha256
    {
        return Err("workflow identity or tenant mismatch".into());
    }
    clamp_limits(&capability, &mut options.limits)?;
    if script.executable.len() > options.limits.source_bytes
        || serde_json::to_vec(&job.event)
            .map_err(|e| e.to_string())?
            .len()
            > options.limits.result_bytes.min(options.limits.source_bytes)
    {
        return Err("workflow input limit exceeded".into());
    }
    bind_deadline(&mut capability, &options.limits)?;
    let host = Rc::new(RefCell::new(Host {
        capability,
        transaction: None,
        request_id: 0,
        retry: None,
        allow_retry: true,
        options: options.clone(),
    }));
    let callback = host.clone();
    let lua_instructions = host
        .borrow()
        .capability
        .script_limits()
        .fuel
        .min(bicdb_lua::Limits::default().instructions);
    let result = match script.language {
        Language::TypeScript | Language::JavaScript => bicdb_script::execute(
            &script.executable,
            &job.event,
            &options.limits,
            move |method, args| callback.borrow_mut().call(method, args),
        )
        .map_err(|e| e.to_string()),
        Language::Lua => bicdb_lua::execute_workflow(
            &script.executable,
            &job.event,
            &bicdb_lua::Limits {
                memory_bytes: options.limits.memory_bytes,
                timeout: options.limits.timeout,
                source_bytes: options.limits.source_bytes.saturating_add(8192),
                instructions: lua_instructions,
            },
            move |method, args| callback.borrow_mut().call(method, args),
        )
        .map_err(|e| e.to_string()),
    };
    let mut host = host.borrow_mut();
    // Always clean up on VM interruption, errors, and scripts catching errors.
    let left_open = host.transaction.is_some();
    host.rollback()?;
    if let Some(delay_ms) = host.retry {
        return Ok(ScriptWorkflowOutcome::Retry { delay_ms });
    }
    if left_open {
        return Err("workflow left a transaction open".into());
    }
    result.and_then(|value| {
        if now_ms()? >= host.capability.actor().deadline_unix_ms {
            return Err("workflow deadline exceeded".into());
        }
        if serde_json::to_vec(&value).map_err(|e| e.to_string())?.len()
            > options.limits.result_bytes
        {
            return Err("workflow result limit exceeded".into());
        }
        Ok(ScriptWorkflowOutcome::Completed(value))
    })
}

/// First line of an engine error, bounded so diagnostics never echo stacks.
pub(crate) fn bounded_error_text(message: &str) -> String {
    let line = message.lines().next().unwrap_or_default().trim();
    let mut end = line.len().min(512);
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    line[..end].to_string()
}

/// Narrow script limits to the caller deadline and the signed manifest limits.
fn clamp_limits(capability: &CapabilityHost, limits: &mut bicdb_script::Limits) -> Result<()> {
    let remaining = capability.actor().deadline_unix_ms - now_ms()?;
    if remaining <= 0 {
        return Err("workflow deadline exceeded".into());
    }
    limits.timeout = limits
        .timeout
        .min(std::time::Duration::from_millis(remaining as u64));
    let signed_limits = capability.script_limits();
    limits.timeout = limits
        .timeout
        .min(std::time::Duration::from_millis(signed_limits.timeout_ms));
    limits.memory_bytes = limits
        .memory_bytes
        .min(signed_limits.memory_bytes.try_into().unwrap_or(usize::MAX));
    limits.source_bytes = limits.source_bytes.min(
        signed_limits
            .max_input_bytes
            .try_into()
            .unwrap_or(usize::MAX),
    );
    limits.result_bytes = limits.result_bytes.min(
        signed_limits
            .max_output_bytes
            .try_into()
            .unwrap_or(usize::MAX),
    );
    Ok(())
}

/// Bind the capability host's deadline to the clamped script timeout.
fn bind_deadline(capability: &mut CapabilityHost, limits: &bicdb_script::Limits) -> Result<i64> {
    let timeout_ms: i64 = limits.timeout.as_millis().try_into().unwrap_or(i64::MAX);
    let deadline = capability
        .actor()
        .deadline_unix_ms
        .min(now_ms()?.saturating_add(timeout_ms));
    capability.replace_actor_deadline_unix_ms(deadline);
    Ok(deadline)
}

/// Execute a signed package script route handler for one authenticated
/// caller. The capability host is built from the caller's actor, so declared
/// SQL runs under that caller's row-level security; an open transaction is
/// always rolled back. Engine errors are reduced to their first line.
pub(crate) fn execute_script_route(
    javascript: &str,
    export: &str,
    request: &Value,
    mut capability: CapabilityHost,
    mut options: ScriptWorkflowOptions,
) -> crate::Result<Value> {
    use crate::AppRuntimeError;
    let failed = |message: String| {
        AppRuntimeError::Extension(bicdb_extension::ExtensionError::Runtime(format!(
            "script route `{export}` failed: {message}"
        )))
    };
    let timed_out =
        || AppRuntimeError::Timeout(format!("script route `{export}` exceeded its deadline"));
    clamp_limits(&capability, &mut options.limits).map_err(|_| timed_out())?;
    if javascript.len() > options.limits.source_bytes {
        return Err(failed(
            "compiled script exceeds the signed input limit".into(),
        ));
    }
    let deadline = bind_deadline(&mut capability, &options.limits).map_err(failed)?;
    let host = Rc::new(RefCell::new(Host {
        capability,
        transaction: None,
        request_id: 0,
        retry: None,
        allow_retry: false,
        options: options.clone(),
    }));
    let callback = host.clone();
    let result = bicdb_script::execute_entry(
        javascript,
        export,
        request,
        &options.limits,
        move |method, args| callback.borrow_mut().call(method, args),
    );
    let mut host = host.borrow_mut();
    let left_open = host.transaction.is_some();
    host.rollback().map_err(failed)?;
    let expired = || now_ms().map_or(true, |now| now >= deadline);
    let value = match result {
        Ok(value) => value,
        Err(_) if expired() => return Err(timed_out()),
        Err(error) => {
            return Err(failed(bounded_error_text(&error.0)));
        }
    };
    if left_open {
        return Err(failed("handler left a transaction open".into()));
    }
    if expired() {
        return Err(timed_out());
    }
    if serde_json::to_vec(&value)?.len() > options.limits.result_bytes {
        return Err(failed("result exceeds the signed output limit".into()));
    }
    Ok(value)
}

/// Execute and settle one durable broker delivery. The caller constructs the
/// capability host using current identity/role policy before this call. Errors
/// remain unacknowledged so the worker can classify retry or dead-letter policy.
pub fn execute_script_delivery(
    db: &mut bicdb_core::BicDb,
    consumer: &str,
    delivery: &bicdb_core::BrokerMessage,
    mut capability: CapabilityHost,
    options: ScriptWorkflowOptions,
) -> Result<ScriptWorkflowOutcome> {
    if delivery.queue != bicdb_workflow::QUEUE || delivery.visibility_deadline <= now_ms()? {
        return Err("invalid or expired workflow delivery".into());
    }
    db.broker()
        .validate_delivery(consumer, delivery)
        .map_err(|e| e.to_string())?;
    capability.replace_actor_deadline_unix_ms(
        capability
            .actor()
            .deadline_unix_ms
            .min(delivery.visibility_deadline),
    );
    let job: WorkflowJob =
        serde_json::from_value(delivery.payload.clone()).map_err(|e| e.to_string())?;
    bicdb_workflow::validate_job_receipt(db, &job, Some(delivery.message_id))
        .map_err(|e| e.to_string())?;
    let script = bicdb_workflow::load_job_script(db, &job).map_err(|e| e.to_string())?;
    let outcome = execute_script_workflow(&script, &job, capability, options)?;
    match &outcome {
        ScriptWorkflowOutcome::Completed(_) => db
            .broker()
            .ack_delivery(consumer, delivery)
            .map_err(|e| e.to_string())?,
        ScriptWorkflowOutcome::Retry { delay_ms } => db
            .broker()
            .nack_delivery(
                consumer,
                delivery,
                bicdb_core::NackOptions {
                    requeue: true,
                    delay_ms: Some(*delay_ms),
                    error: Some("workflow retry requested".into()),
                },
            )
            .map_err(|e| e.to_string())?,
    }
    Ok(outcome)
}
