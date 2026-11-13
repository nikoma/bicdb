//! TypeScript is compiled at publication; workers execute stored JavaScript.
use oxc_allocator::Allocator;
use oxc_codegen::Codegen;
use oxc_parser::Parser;
use oxc_semantic::SemanticBuilder;
use oxc_span::SourceType;
use oxc_transformer::{TransformOptions, Transformer};
use rquickjs::{
    function::Func, CatchResultExt, CaughtError, Constructor, Context, Ctx, Exception, Object,
    Promise, Runtime,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
#[error("Script rejected: {0}")]
pub struct ScriptError(pub String);

/// Trusted host failure. The optional origin is private to this invocation's
/// native caller and is never serialized into guest-visible JSON or properties.
#[derive(Debug)]
pub struct HostFailure {
    pub message: String,
    pub origin: Option<u64>,
}

/// An unhandled exception preserves a host origin only when it is the exact
/// exception object created for that host call. Messages and copied properties
/// cannot confer origin; caught failures followed by normal results remain normal.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct ExecutionFailure {
    pub error: ScriptError,
    pub host_origin: Option<u64>,
}

impl From<ScriptError> for ExecutionFailure {
    fn from(error: ScriptError) -> Self {
        Self {
            error,
            host_origin: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub source_bytes: usize,
    pub memory_bytes: usize,
    pub result_bytes: usize,
    pub host_calls: usize,
    pub pending_jobs: usize,
    pub timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            source_bytes: 1024 * 1024,
            memory_bytes: 64 * 1024 * 1024,
            result_bytes: 1024 * 1024,
            host_calls: 1000,
            pending_jobs: 10_000,
            timeout: Duration::from_secs(10),
        }
    }
}

/// Immutable compilation receipt. OXC checks syntax and binding semantics,
/// not TypeScript's full type system. Host boundaries validate values at runtime.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompiledScript {
    pub javascript: String,
    pub source_sha256: String,
    pub compiler: String,
}

pub fn compile_typescript(source: &str, limits: &Limits) -> Result<CompiledScript, ScriptError> {
    if source.len() > limits.source_bytes || limits.memory_bytes == 0 {
        return Err(ScriptError("source limit exceeded".into()));
    }
    let allocator = Allocator::default();
    let parsed = Parser::new(&allocator, source, SourceType::ts().with_module(false)).parse();
    if !parsed.diagnostics.is_empty() {
        return Err(ScriptError(format!(
            "TypeScript syntax: {:?}",
            parsed.diagnostics
        )));
    }
    let mut program = parsed.program;
    let semantic = SemanticBuilder::new().with_enum_eval(true).build(&program);
    if !semantic.diagnostics.is_empty() {
        return Err(ScriptError(format!(
            "TypeScript bindings: {:?}",
            semantic.diagnostics
        )));
    }
    let transformed = Transformer::new(
        &allocator,
        Path::new("workflow.ts"),
        &TransformOptions::default(),
    )
    .build_with_scoping(semantic.semantic.into_scoping(), &mut program);
    if !transformed.diagnostics.is_empty() {
        return Err(ScriptError(format!(
            "TypeScript transformation: {:?}",
            transformed.diagnostics
        )));
    }
    let javascript = Codegen::new().build(&program).code;
    if javascript.len() > limits.source_bytes {
        return Err(ScriptError("compiled source limit exceeded".into()));
    }
    // Compile in the same sandbox used by workers before accepting publication.
    let runtime = Runtime::new().map_err(|e| ScriptError(e.to_string()))?;
    runtime.set_memory_limit(limits.memory_bytes);
    let context = Context::full(&runtime).map_err(|e| ScriptError(e.to_string()))?;
    context.with(|ctx| {
        ctx.eval::<rquickjs::Function, _>(format!("(function() {{\n{javascript}\n}})"))
            .catch(&ctx)
            .map(|_| ())
            .map_err(|e| ScriptError(e.to_string()))
    })?;
    Ok(CompiledScript {
        javascript,
        source_sha256: format!("{:x}", Sha256::digest(source.as_bytes())),
        compiler: "oxc/0.152.0".into(),
    })
}

/// Fresh QuickJS VM without filesystem, network, timers, Node globals or module
/// loader. Every side effect passes through the trusted host, never event data.
/// The host must bound blocking calls and enforce capabilities and transactions.
pub fn execute(
    javascript: &str,
    event: &serde_json::Value,
    limits: &Limits,
    host: impl FnMut(&str, serde_json::Value) -> Result<serde_json::Value, String> + 'static,
) -> Result<serde_json::Value, ScriptError> {
    execute_entry(javascript, "workflow", event, limits, host)
}

/// True when `name` can name a plain global JavaScript function.
pub fn is_entry_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    name.len() <= 128
        && bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
        && !matches!(
            name,
            "event" | "call" | "sql" | "db" | "http" | "secrets" | "jobs" | "json" | "eval"
        )
}

/// Same sandbox as [`execute`], calling the global function `entry(event)`
/// instead of `workflow(event)`. The entry may return a value or a promise.
pub fn execute_entry(
    javascript: &str,
    entry: &str,
    event: &serde_json::Value,
    limits: &Limits,
    mut host: impl FnMut(&str, serde_json::Value) -> Result<serde_json::Value, String> + 'static,
) -> Result<serde_json::Value, ScriptError> {
    execute_entry_with_host_origins(javascript, entry, event, limits, move |method, args| {
        host(method, args).map_err(|message| HostFailure {
            message,
            origin: None,
        })
    })
    .map_err(|failure| failure.error)
}

/// Execute with native exception identity tracking. Guest JavaScript cannot mint
/// an origin by raising an error string, SQLSTATE, property, or copied exception.
pub fn execute_entry_with_host_origins(
    javascript: &str,
    entry: &str,
    event: &serde_json::Value,
    limits: &Limits,
    host: impl FnMut(&str, serde_json::Value) -> Result<serde_json::Value, HostFailure> + 'static,
) -> Result<serde_json::Value, ExecutionFailure> {
    if !is_entry_name(entry) {
        return Err(ScriptError("invalid entry function name".into()).into());
    }
    if javascript.len() > limits.source_bytes
        || limits.memory_bytes == 0
        || limits.timeout.is_zero()
        || limits.host_calls == 0
        || limits.pending_jobs == 0
    {
        return Err(ScriptError("invalid limits or oversized source".into()).into());
    }
    let deadline = Instant::now()
        .checked_add(limits.timeout)
        .ok_or_else(|| ScriptError("invalid timeout".into()))?;
    let runtime = Runtime::new().map_err(|e| ScriptError(e.to_string()))?;
    runtime.set_memory_limit(limits.memory_bytes);
    runtime.set_max_stack_size(512 * 1024);
    runtime.set_interrupt_handler(Some(Box::new(move || Instant::now() >= deadline)));
    let context = Context::full(&runtime).map_err(|e| ScriptError(e.to_string()))?;
    let host = RefCell::new(host);
    let event = serde_json::to_string(event).map_err(|e| ScriptError(e.to_string()))?;
    if event.len() > limits.result_bytes {
        return Err(ScriptError("event limit exceeded".into()).into());
    }
    context
        .with(|ctx| execute_entry_context(ctx, javascript, entry, &event, limits, deadline, host))
}

fn execute_entry_context<'js>(
    ctx: Ctx<'js>,
    javascript: &str,
    entry: &str,
    event: &str,
    limits: &Limits,
    deadline: Instant,
    host: RefCell<
        impl FnMut(&str, serde_json::Value) -> Result<serde_json::Value, HostFailure> + 'static,
    >,
) -> Result<serde_json::Value, ExecutionFailure> {
    let callback_limits = limits.clone();
    let origins: Rc<RefCell<Vec<(Exception<'js>, u64)>>> = Rc::new(RefCell::new(Vec::new()));
    let callback_origins = origins.clone();
    let error_constructor: Rc<RefCell<Option<Constructor<'js>>>> = Rc::new(RefCell::new(None));
    let callback_constructor = error_constructor.clone();
    let run = || -> rquickjs::Result<serde_json::Value> {
        // Capture the pristine intrinsic before evaluating any guest code.
        // Its constructor defines an own message data property. Ordinary Set
        // (including Exception::from_message) can invoke an inherited setter.
        *error_constructor.borrow_mut() = Some(ctx.globals().get("Error")?);
        ctx.globals().set("__event", event)?;
        let calls = RefCell::new(0usize);
        ctx.globals().set(
            "__host",
            Func::new(
                move |method: String, arguments: String| -> rquickjs::Result<String> {
                    let invoke = || -> Result<serde_json::Value, HostFailure> {
                        *calls.borrow_mut() += 1;
                        if *calls.borrow() > callback_limits.host_calls
                            || Instant::now() >= deadline
                        {
                            return Err(HostFailure {
                                message: "host call limit exceeded".into(),
                                origin: None,
                            });
                        }
                        if arguments.len() > callback_limits.result_bytes {
                            return Err(HostFailure {
                                message: "host argument limit exceeded".into(),
                                origin: None,
                            });
                        }
                        let value = (host.borrow_mut())(
                            &method,
                            serde_json::from_str(&arguments).map_err(|e| HostFailure {
                                message: e.to_string(),
                                origin: None,
                            })?,
                        )?;
                        if serde_json::to_vec(&value)
                            .map_err(|e| HostFailure {
                                message: e.to_string(),
                                origin: None,
                            })?
                            .len()
                            > callback_limits.result_bytes
                        {
                            return Err(HostFailure {
                                message: "host response limit exceeded".into(),
                                origin: None,
                            });
                        }
                        Ok(value)
                    };
                    match invoke() {
                        Ok(value) => Ok(serde_json::json!({"value":value}).to_string()),
                        Err(error) => {
                            let object: Object<'js> = callback_constructor
                                .borrow()
                                .as_ref()
                                .ok_or(rquickjs::Error::Unknown)?
                                .construct((&error.message,))?;
                            let exception =
                                Exception::from_object(object).ok_or(rquickjs::Error::Unknown)?;
                            if let Some(origin) = error.origin {
                                callback_origins
                                    .borrow_mut()
                                    .push((exception.clone(), origin));
                            }
                            Err(exception.throw())
                        }
                    }
                },
            ),
        )?;
        ctx.eval::<(), _>(include_str!("bindings.js"))?;
        ctx.eval::<(), _>(javascript.as_bytes())?;
        let promise: Promise = ctx.eval(format!(
            "Promise.resolve().then(() => {entry}(event)).then(value => JSON.stringify(value))"
        ))?;
        for _ in 0..limits.pending_jobs {
            if Instant::now() >= deadline {
                break;
            }
            if let Some(result) = promise.result::<String>() {
                let result = result?;
                if result.len() > limits.result_bytes {
                    return Err(rquickjs::Error::Unknown);
                }
                return serde_json::from_str(&result).map_err(|_| rquickjs::Error::Unknown);
            }
            if !ctx.execute_pending_job() {
                break;
            }
        }
        Err(rquickjs::Error::Unknown)
    };
    let result = run().catch(&ctx).map_err(|error| {
        let host_origin = match &error {
            CaughtError::Exception(exception) => origins
                .borrow()
                .iter()
                .find_map(|(original, origin)| (original == exception).then_some(*origin)),
            _ => None,
        };
        ExecutionFailure {
            // The native caller already retains the authoritative error. Do
            // not invoke guest-mutated message/stack getters for that error.
            error: ScriptError(if host_origin.is_some() {
                "native host call failed".into()
            } else {
                error.to_string()
            }),
            host_origin,
        }
    });
    // Native callback storage belongs to the VM. Drop retained exception values
    // before destroying the context so it cannot retain its own context via them.
    origins.borrow_mut().clear();
    error_constructor.borrow_mut().take();
    result
}
