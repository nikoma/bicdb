//! TypeScript is compiled at publication; workers execute stored JavaScript.
use oxc_allocator::Allocator;
use oxc_codegen::Codegen;
use oxc_parser::Parser;
use oxc_semantic::SemanticBuilder;
use oxc_span::SourceType;
use oxc_transformer::{TransformOptions, Transformer};
use rquickjs::{function::Func, CatchResultExt, Context, Promise, Runtime};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
#[error("Script rejected: {0}")]
pub struct ScriptError(pub String);

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
    if javascript.len() > limits.source_bytes
        || limits.memory_bytes == 0
        || limits.timeout.is_zero()
        || limits.host_calls == 0
        || limits.pending_jobs == 0
    {
        return Err(ScriptError("invalid limits or oversized source".into()));
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
    let callback_limits = limits.clone();
    let event = serde_json::to_string(event).map_err(|e| ScriptError(e.to_string()))?;
    if event.len() > limits.result_bytes {
        return Err(ScriptError("event limit exceeded".into()));
    }
    context.with(|ctx| {
        let run = || -> rquickjs::Result<serde_json::Value> {
            ctx.globals().set("__event", event)?;
            let calls = RefCell::new(0usize);
            ctx.globals().set("__host", Func::new(move |method: String, arguments: String| -> String {
                let invoke = || -> Result<serde_json::Value, String> {
                    *calls.borrow_mut() += 1;
                    if *calls.borrow() > callback_limits.host_calls || Instant::now() >= deadline {
                        return Err("host call limit exceeded".into());
                    }
                    if arguments.len() > callback_limits.result_bytes {
                        return Err("host argument limit exceeded".into());
                    }
                    let value = (host.borrow_mut())(&method, serde_json::from_str(&arguments).map_err(|e| e.to_string())?)?;
                    if serde_json::to_vec(&value).map_err(|e| e.to_string())?.len() > callback_limits.result_bytes {
                        return Err("host response limit exceeded".into());
                    }
                    Ok(value)
                };
                match invoke() {
                    Ok(value) => serde_json::json!({"value":value}).to_string(),
                    Err(error) => serde_json::json!({"error":error}).to_string(),
                }
            }))?;
            ctx.eval::<(), _>(include_str!("bindings.js"))?;
            ctx.eval::<(), _>(javascript.as_bytes())?;
            let promise: Promise = ctx.eval("Promise.resolve().then(() => workflow(event)).then(value => JSON.stringify(value))")?;
            for _ in 0..limits.pending_jobs {
                if Instant::now() >= deadline { break; }
                if let Some(result) = promise.result::<String>() {
                    let result = result?;
                    if result.len() > limits.result_bytes { return Err(rquickjs::Error::Unknown); }
                    return serde_json::from_str(&result).map_err(|_| rquickjs::Error::Unknown);
                }
                if !ctx.execute_pending_job() { break; }
            }
            Err(rquickjs::Error::Unknown)
        };
        run().catch(&ctx).map_err(|e| ScriptError(e.to_string()))
    })
}
