//! Sandboxed native host, content-addressed package store, and atomic runtime
//! snapshots for ABI-v1 WebAssembly extensions.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use sha2::{Digest, Sha256};
use wasmtime::{
    Caller, Config, Engine, ExternType, Instance, Linker, Memory, Module, PoolingAllocationConfig,
    Store, StoreLimits, StoreLimitsBuilder, TypedFunc, ValType,
};

use crate::{
    abi_v2::{ErrorClass, HostCall, HostCallResult, HostError},
    resolve_extension_order, ActiveWebsiteDeployment, EventBindingDefinition, ExtensionError,
    ExtensionInstallation, ExtensionInvocation, ExtensionInvocationResult, ExtensionManifest,
    ExtensionState, HttpMethod, InvocationContext, InvocationKind, RestResourceDefinition, Result,
    RouteAuth, EXTENSION_ABI_V2, EXTENSION_ABI_VERSION, MAX_MANIFEST_BYTES,
};

const ABI_VERSION_EXPORT: &str = "bicdb_extension_abi_version";
const MANIFEST_PTR_EXPORT: &str = "bicdb_extension_manifest_ptr";
const MANIFEST_LEN_EXPORT: &str = "bicdb_extension_manifest_len";
const ALLOC_EXPORT: &str = "bicdb_extension_alloc";
const DEALLOC_EXPORT: &str = "bicdb_extension_dealloc";
const INVOKE_EXPORT: &str = "bicdb_extension_invoke";
pub const ABI_V2_HOST_MODULE: &str = "bicdb:app/host";
pub const ABI_V2_HOST_CALL: &str = "call";
pub const ABI_V2_CALL_OK: u32 = 0;
pub const ABI_V2_CALL_BUFFER_TOO_SMALL: u32 = 1;
pub const ABI_V2_CALL_PROTOCOL_ERROR: u32 = 2;
const PACKAGE_SUFFIX: &str = ".wasm";
const TEMP_PREFIX: &str = ".bicdb-extension-";
const EPOCH_TICK_MS: u64 = 5;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct WasmHostConfig {
    pub max_module_bytes: usize,
    pub max_manifest_bytes: usize,
    pub max_memory_bytes: usize,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub max_host_call_bytes: usize,
    pub fuel_per_invocation: u64,
    pub max_timeout_ms: u64,
    /// Process-local ceiling for simultaneously allocated application Wasm
    /// instances when a shared pooled engine is used.
    pub max_pooled_instances: u32,
}

impl Default for WasmHostConfig {
    fn default() -> Self {
        Self {
            max_module_bytes: 64 * 1024 * 1024,
            max_manifest_bytes: MAX_MANIFEST_BYTES,
            max_memory_bytes: 64 * 1024 * 1024,
            max_input_bytes: 1024 * 1024,
            max_output_bytes: 4 * 1024 * 1024,
            max_host_call_bytes: 1024 * 1024,
            fuel_per_invocation: 10_000_000,
            max_timeout_ms: 10_000,
            max_pooled_instances: 256,
        }
    }
}

impl WasmHostConfig {
    pub fn validate(&self) -> Result<()> {
        if self.max_module_bytes == 0
            || self.max_manifest_bytes == 0
            || self.max_memory_bytes == 0
            || self.max_input_bytes == 0
            || self.max_output_bytes == 0
            || self.max_host_call_bytes == 0
            || self.fuel_per_invocation == 0
            || self.max_timeout_ms == 0
            || self.max_pooled_instances == 0
        {
            return Err(ExtensionError::InvalidManifest(
                "WASM host limits must all be positive".to_string(),
            ));
        }
        if self.max_input_bytes > self.max_memory_bytes
            || self.max_output_bytes > self.max_memory_bytes
            || self.max_host_call_bytes > self.max_memory_bytes
        {
            return Err(ExtensionError::InvalidManifest(
                "WASM input/output/host-call limits cannot exceed memory".to_string(),
            ));
        }
        Ok(())
    }
}

/// Invocation-local provider for ABI-v2 host capabilities.
///
/// Implementations own all native resources used by one invocation. Dropping
/// the provider must roll back open transactions and release every outstanding
/// handle. A provider never crosses invocations.
pub trait ApplicationHost: Send {
    fn call(&mut self, call: HostCall) -> HostCallResult;
}

struct StoreState {
    limits: StoreLimits,
    host: Option<Box<dyn ApplicationHost>>,
    max_host_call_bytes: usize,
}

/// Validated, reusable extension module.
///
/// Each invocation receives a fresh store and instance. This prevents mutable
/// globals or leaked linear-memory contents from crossing requests. Application
/// hosts compile modules into one explicitly bounded Wasmtime allocation pool;
/// standalone callers retain the on-demand allocator and per-module admission.
#[derive(Clone)]
pub struct WasmExtension {
    engine: Arc<EngineRuntime>,
    module: Module,
    config: WasmHostConfig,
    manifest: Arc<ExtensionManifest>,
    sha256: [u8; 32],
    active_invocations: Arc<AtomicU32>,
}

/// Reusable Wasmtime engine for a set of application modules.
///
/// Application hosts use [`WasmEngine::pooled`] once and compile every module
/// into that engine. The pooling allocator bounds concurrent instance slots
/// while Wasmtime scrubs mutable memory between fresh invocations. Standalone
/// callers of [`WasmExtension::load`] retain the on-demand allocator.
#[derive(Clone)]
pub struct WasmEngine {
    runtime: Arc<EngineRuntime>,
    config: WasmHostConfig,
    pooled: bool,
}

struct EngineRuntime {
    engine: Engine,
    pooled: bool,
}

impl EngineRuntime {
    fn start(engine: Engine, pooled: bool) -> Arc<Self> {
        let runtime = Arc::new(Self { engine, pooled });
        let weak = Arc::downgrade(&runtime);
        std::thread::Builder::new()
            .name("bicdb-wasm-epoch".to_string())
            .spawn(move || {
                while let Some(runtime) = weak.upgrade() {
                    std::thread::sleep(Duration::from_millis(EPOCH_TICK_MS));
                    runtime.engine.increment_epoch();
                }
            })
            .expect("spawn shared WASM epoch ticker");
        runtime
    }
}

impl std::fmt::Debug for WasmExtension {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WasmExtension")
            .field("name", &self.manifest.identity.name)
            .field("version", &self.manifest.identity.version)
            .field("sha256", &self.sha256_hex())
            .finish()
    }
}

impl WasmExtension {
    pub fn load(bytes: &[u8], config: WasmHostConfig) -> Result<Self> {
        let engine = WasmEngine::on_demand(config)?;
        Self::load_with_engine(bytes, &engine)
    }

    pub fn load_with_engine(bytes: &[u8], engine: &WasmEngine) -> Result<Self> {
        let config = engine.config.clone();
        config.validate()?;
        if bytes.len() > config.max_module_bytes {
            return Err(ExtensionError::ResourceLimit(format!(
                "module is {} bytes; host limit is {}",
                bytes.len(),
                config.max_module_bytes
            )));
        }
        let module = Module::new(&engine.runtime.engine, bytes)
            .map_err(|error| ExtensionError::Runtime(format!("compile module: {error}")))?;

        validate_imports_are_known(&module)?;

        let (mut store, instance) = instantiate(&engine.runtime.engine, &module, &config, None)?;
        let abi = typed::<(), u32>(&mut store, &instance, ABI_VERSION_EXPORT)?
            .call(&mut store, ())
            .map_err(runtime_call_error)?;
        if !matches!(abi, EXTENSION_ABI_VERSION | EXTENSION_ABI_V2) {
            return Err(ExtensionError::UnsupportedAbi {
                expected: EXTENSION_ABI_V2,
                actual: abi,
            });
        }
        validate_imports_for_abi(&module, abi)?;
        let manifest_pointer = typed::<(), u32>(&mut store, &instance, MANIFEST_PTR_EXPORT)?
            .call(&mut store, ())
            .map_err(runtime_call_error)?;
        let manifest_len = typed::<(), u32>(&mut store, &instance, MANIFEST_LEN_EXPORT)?
            .call(&mut store, ())
            .map_err(runtime_call_error)? as usize;
        if manifest_len == 0 || manifest_len > config.max_manifest_bytes {
            return Err(ExtensionError::ResourceLimit(format!(
                "manifest is {manifest_len} bytes; host limit is {}",
                config.max_manifest_bytes
            )));
        }
        let memory = memory(&mut store, &instance)?;
        let manifest_bytes = read_memory(
            &store,
            memory,
            manifest_pointer as usize,
            manifest_len,
            "manifest",
        )?;
        let manifest: ExtensionManifest = serde_json::from_slice(manifest_bytes)
            .map_err(|error| ExtensionError::InvalidPayload(format!("manifest JSON: {error}")))?;
        manifest.validate()?;
        if manifest.limits.memory_bytes as usize > config.max_memory_bytes
            || manifest.limits.max_input_bytes as usize > config.max_input_bytes
            || manifest.limits.max_output_bytes as usize > config.max_output_bytes
            || manifest.limits.fuel > config.fuel_per_invocation
            || manifest.limits.timeout_ms > config.max_timeout_ms
        {
            return Err(ExtensionError::ResourceLimit(
                "manifest requests limits above host policy".to_string(),
            ));
        }
        let sha256: [u8; 32] = Sha256::digest(bytes).into();
        Ok(Self {
            engine: Arc::clone(&engine.runtime),
            module,
            config,
            manifest: Arc::new(manifest),
            sha256,
            active_invocations: Arc::new(AtomicU32::new(0)),
        })
    }

    pub fn uses_pooled_engine(&self) -> bool {
        self.engine.pooled
    }

    pub fn manifest(&self) -> &ExtensionManifest {
        &self.manifest
    }

    pub fn sha256(&self) -> [u8; 32] {
        self.sha256
    }

    pub fn sha256_hex(&self) -> String {
        self.sha256
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    pub fn invoke(&self, invocation: &ExtensionInvocation) -> Result<ExtensionInvocationResult> {
        if self.manifest.identity.abi_version == EXTENSION_ABI_V2 {
            return Err(ExtensionError::Runtime(
                "ABI v2 invocation requires an invocation-scoped capability host".to_string(),
            ));
        }
        self.invoke_inner(invocation, None)
    }

    pub fn invoke_with_host(
        &self,
        invocation: &ExtensionInvocation,
        host: Box<dyn ApplicationHost>,
    ) -> Result<ExtensionInvocationResult> {
        if self.manifest.identity.abi_version != EXTENSION_ABI_V2 {
            return Err(ExtensionError::Runtime(
                "capability hosts are available only to ABI v2 modules".to_string(),
            ));
        }
        self.invoke_inner(invocation, Some(host))
    }

    fn invoke_inner(
        &self,
        invocation: &ExtensionInvocation,
        host: Option<Box<dyn ApplicationHost>>,
    ) -> Result<ExtensionInvocationResult> {
        let _admission = self.acquire_invocation()?;
        let input = serde_json::to_vec(invocation)
            .map_err(|error| ExtensionError::InvalidPayload(error.to_string()))?;
        let max_input = self
            .config
            .max_input_bytes
            .min(self.manifest.limits.max_input_bytes as usize);
        if input.len() > max_input {
            return Err(ExtensionError::ResourceLimit(format!(
                "invocation input is {} bytes; limit is {max_input}",
                input.len()
            )));
        }

        let (mut store, instance) =
            instantiate(&self.engine.engine, &self.module, &self.config, host)?;
        store
            .set_fuel(
                self.config
                    .fuel_per_invocation
                    .min(self.manifest.limits.fuel),
            )
            .map_err(|error| ExtensionError::Runtime(error.to_string()))?;
        let memory = memory(&mut store, &instance)?;
        let alloc = typed::<u32, u32>(&mut store, &instance, ALLOC_EXPORT)?;
        let dealloc = typed::<(u32, u32), ()>(&mut store, &instance, DEALLOC_EXPORT)?;
        let invoke = typed::<(u32, u32), u64>(&mut store, &instance, INVOKE_EXPORT)?;
        let input_len = u32::try_from(input.len())
            .map_err(|_| ExtensionError::ResourceLimit("input exceeds wasm32".to_string()))?;
        let input_pointer = alloc
            .call(&mut store, input_len)
            .map_err(runtime_call_error)?;
        write_memory(
            &mut store,
            memory,
            input_pointer as usize,
            &input,
            "invocation input",
        )?;
        let timeout = Duration::from_millis(
            self.config
                .max_timeout_ms
                .min(self.manifest.limits.timeout_ms),
        );
        let deadline_ticks = timeout
            .as_millis()
            .div_ceil(u128::from(EPOCH_TICK_MS))
            .max(1)
            .min(u128::from(u64::MAX)) as u64;
        store.set_epoch_deadline(deadline_ticks);
        let packed_result = invoke.call(&mut store, (input_pointer, input_len));
        let packed = packed_result.map_err(runtime_call_error)?;
        dealloc
            .call(&mut store, (input_pointer, input_len))
            .map_err(runtime_call_error)?;

        let output_pointer = (packed >> 32) as u32;
        let output_len = packed as u32;
        let max_output = self
            .config
            .max_output_bytes
            .min(self.manifest.limits.max_output_bytes as usize);
        if output_len as usize > max_output {
            return Err(ExtensionError::ResourceLimit(format!(
                "invocation output is {output_len} bytes; limit is {max_output}"
            )));
        }
        let output = read_memory(
            &store,
            memory,
            output_pointer as usize,
            output_len as usize,
            "invocation output",
        )?
        .to_vec();
        dealloc
            .call(&mut store, (output_pointer, output_len))
            .map_err(runtime_call_error)?;
        serde_json::from_slice(&output)
            .map_err(|error| ExtensionError::InvalidPayload(format!("result JSON: {error}")))
    }

    fn acquire_invocation(&self) -> Result<InvocationAdmission> {
        let limit = self.manifest.limits.max_concurrency;
        let mut active = self.active_invocations.load(Ordering::Acquire);
        loop {
            if active >= limit {
                return Err(ExtensionError::ResourceLimit(format!(
                    "extension `{}` reached its {}-invocation concurrency limit",
                    self.manifest.identity.name, limit
                )));
            }
            match self.active_invocations.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(InvocationAdmission {
                        active: self.active_invocations.clone(),
                    })
                }
                Err(current) => active = current,
            }
        }
    }
}

impl WasmEngine {
    pub fn on_demand(config: WasmHostConfig) -> Result<Self> {
        Self::new(config, false)
    }

    pub fn pooled(config: WasmHostConfig) -> Result<Self> {
        Self::new(config, true)
    }

    fn new(config: WasmHostConfig, pooled: bool) -> Result<Self> {
        config.validate()?;
        let mut engine_config = Config::new();
        engine_config.consume_fuel(true);
        engine_config.epoch_interruption(true);
        engine_config.wasm_threads(false);
        // Module compilation happens only on a bounded cache miss. Avoid
        // Wasmtime's process-global CPU-count-sized Rayon pool so application
        // hosting has a predictable thread ceiling.
        engine_config.parallel_compilation(false);
        // Current Rust WebAssembly output uses internal reference types. They
        // grant no host authority: every module import is still rejected.
        if pooled {
            let slots = config.max_pooled_instances;
            let mut allocation = PoolingAllocationConfig::default();
            allocation
                .total_core_instances(slots)
                .total_memories(slots)
                .total_tables(slots.saturating_mul(2))
                .max_memories_per_module(1)
                .max_tables_per_module(2)
                .max_memory_size(config.max_memory_bytes)
                .max_unused_warm_slots(slots.min(64));
            engine_config
                .memory_reservation(config.max_memory_bytes as u64)
                .allocation_strategy(allocation);
        }
        let engine = Engine::new(&engine_config)
            .map_err(|error| ExtensionError::Runtime(error.to_string()))?;
        Ok(Self {
            runtime: EngineRuntime::start(engine, pooled),
            config,
            pooled,
        })
    }

    pub fn max_pooled_instances(&self) -> Option<u32> {
        self.pooled.then_some(self.config.max_pooled_instances)
    }
}

struct InvocationAdmission {
    active: Arc<AtomicU32>,
}

impl Drop for InvocationAdmission {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

fn instantiate(
    engine: &Engine,
    module: &Module,
    config: &WasmHostConfig,
    host: Option<Box<dyn ApplicationHost>>,
) -> Result<(Store<StoreState>, Instance)> {
    let limits = StoreLimitsBuilder::new()
        .memory_size(config.max_memory_bytes)
        .instances(1)
        .tables(2)
        .build();
    let mut store = Store::new(
        engine,
        StoreState {
            limits,
            host,
            max_host_call_bytes: config.max_host_call_bytes,
        },
    );
    store.limiter(|state| &mut state.limits);
    // Manifest extraction is fuel-bounded. Give it a distant epoch deadline;
    // invocation replaces this with a one-tick watchdog deadline.
    store.set_epoch_deadline(1_000_000);
    store
        .set_fuel(config.fuel_per_invocation)
        .map_err(|error| ExtensionError::Runtime(error.to_string()))?;
    let mut linker = Linker::new(engine);
    linker
        .func_wrap(
            ABI_V2_HOST_MODULE,
            ABI_V2_HOST_CALL,
            |mut caller: Caller<'_, StoreState>,
             request_pointer: i32,
             request_len: i32,
             response_pointer: i32,
             response_capacity: i32|
             -> i64 {
                abi_v2_host_call(
                    &mut caller,
                    request_pointer,
                    request_len,
                    response_pointer,
                    response_capacity,
                )
            },
        )
        .map_err(|error| ExtensionError::Runtime(format!("link ABI v2 host: {error}")))?;
    let instance = linker
        .instantiate(&mut store, module)
        .map_err(|error| ExtensionError::Runtime(format!("instantiate module: {error}")))?;
    Ok((store, instance))
}

fn abi_v2_host_call(
    caller: &mut Caller<'_, StoreState>,
    request_pointer: i32,
    request_len: i32,
    response_pointer: i32,
    response_capacity: i32,
) -> i64 {
    let Some(request_pointer) = u32::try_from(request_pointer)
        .ok()
        .map(|value| value as usize)
    else {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    };
    let Some(request_len) = u32::try_from(request_len).ok().map(|value| value as usize) else {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    };
    let Some(response_pointer) = u32::try_from(response_pointer)
        .ok()
        .map(|value| value as usize)
    else {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    };
    let Some(response_capacity) = u32::try_from(response_capacity)
        .ok()
        .map(|value| value as usize)
    else {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    };
    let max_call_bytes = caller.data().max_host_call_bytes;
    if request_len == 0 || request_len > max_call_bytes || response_capacity > max_call_bytes {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    }
    let Some(memory) = caller
        .get_export("memory")
        .and_then(|export| export.into_memory())
    else {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    };
    let Some(request_end) = request_pointer.checked_add(request_len) else {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    };
    let Some(request_bytes) = memory.data(&*caller).get(request_pointer..request_end) else {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    };
    let Ok(call) = serde_json::from_slice::<HostCall>(request_bytes) else {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    };
    let request_id = call.request_id;
    let mut result = if let Some(host) = caller.data_mut().host.as_mut() {
        host.call(call)
    } else {
        HostCallResult::failure(
            request_id,
            HostError {
                code: "host_unavailable".to_string(),
                class: ErrorClass::Activation,
                message: "ABI v2 host capabilities are unavailable during module validation"
                    .to_string(),
                retryable: false,
                retry_after_ms: None,
                trace_id: "module-validation".to_string(),
            },
        )
    };
    if result.request_id != request_id || result.validate().is_err() {
        result = HostCallResult::failure(
            request_id,
            HostError {
                code: "invalid_host_response".to_string(),
                class: ErrorClass::Internal,
                message: "capability provider returned an invalid response".to_string(),
                retryable: false,
                retry_after_ms: None,
                trace_id: "host".to_string(),
            },
        );
    }
    let Ok(response) = serde_json::to_vec(&result) else {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    };
    if response.len() > max_call_bytes || response.len() > u32::MAX as usize {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    }
    if response.len() > response_capacity {
        return pack_host_call_result(ABI_V2_CALL_BUFFER_TOO_SMALL, response.len() as u32);
    }
    let Some(response_end) = response_pointer.checked_add(response.len()) else {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    };
    let Some(destination) = memory
        .data_mut(&mut *caller)
        .get_mut(response_pointer..response_end)
    else {
        return pack_host_call_result(ABI_V2_CALL_PROTOCOL_ERROR, 0);
    };
    destination.copy_from_slice(&response);
    pack_host_call_result(ABI_V2_CALL_OK, response.len() as u32)
}

fn pack_host_call_result(status: u32, len: u32) -> i64 {
    (((status as u64) << 32) | len as u64) as i64
}

fn validate_imports_are_known(module: &Module) -> Result<()> {
    for import in module.imports() {
        if import.module() != ABI_V2_HOST_MODULE || import.name() != ABI_V2_HOST_CALL {
            return Err(ExtensionError::InvalidManifest(format!(
                "extension import `{}.{}` is forbidden; extension modules may not import WASI or arbitrary host functions",
                import.module(),
                import.name()
            )));
        }
        let ExternType::Func(function) = import.ty() else {
            return Err(ExtensionError::InvalidManifest(
                "ABI v2 host import must be a function".to_string(),
            ));
        };
        let params = function.params().collect::<Vec<_>>();
        let results = function.results().collect::<Vec<_>>();
        if params.len() != 4
            || !params.iter().all(|value| matches!(value, ValType::I32))
            || results.len() != 1
            || !matches!(results.first(), Some(ValType::I64))
        {
            return Err(ExtensionError::InvalidManifest(
                "ABI v2 `bicdb:app/host.call` must have signature (i32, i32, i32, i32) -> i64"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_imports_for_abi(module: &Module, abi: u32) -> Result<()> {
    let imports = module.imports().count();
    match abi {
        EXTENSION_ABI_VERSION if imports == 0 => Ok(()),
        EXTENSION_ABI_VERSION => Err(ExtensionError::InvalidManifest(
            "ABI v1 extension modules may not import host functions".to_string(),
        )),
        EXTENSION_ABI_V2 if imports == 1 => Ok(()),
        EXTENSION_ABI_V2 => Err(ExtensionError::InvalidManifest(
            "ABI v2 extension must import exactly `bicdb:app/host.call`".to_string(),
        )),
        _ => unreachable!("ABI is validated before import policy"),
    }
}

fn typed<Params, Results>(
    store: &mut Store<StoreState>,
    instance: &Instance,
    name: &str,
) -> Result<TypedFunc<Params, Results>>
where
    Params: wasmtime::WasmParams,
    Results: wasmtime::WasmResults,
{
    instance
        .get_typed_func(store, name)
        .map_err(|error| ExtensionError::InvalidManifest(format!("missing `{name}`: {error}")))
}

fn memory(store: &mut Store<StoreState>, instance: &Instance) -> Result<Memory> {
    instance
        .get_memory(store, "memory")
        .ok_or_else(|| ExtensionError::InvalidManifest("missing exported `memory`".to_string()))
}

fn read_memory<'a>(
    store: &'a Store<StoreState>,
    memory: Memory,
    pointer: usize,
    len: usize,
    label: &str,
) -> Result<&'a [u8]> {
    let end = pointer
        .checked_add(len)
        .ok_or_else(|| ExtensionError::InvalidPayload(format!("{label} range overflow")))?;
    memory
        .data(store)
        .get(pointer..end)
        .ok_or_else(|| ExtensionError::InvalidPayload(format!("{label} is outside module memory")))
}

fn write_memory(
    store: &mut Store<StoreState>,
    memory: Memory,
    pointer: usize,
    bytes: &[u8],
    label: &str,
) -> Result<()> {
    memory
        .write(store, pointer, bytes)
        .map_err(|error| ExtensionError::InvalidPayload(format!("{label}: {error}")))
}

fn runtime_call_error(error: wasmtime::Error) -> ExtensionError {
    if error.to_string().contains("fuel") {
        ExtensionError::ResourceLimit("extension exhausted its fuel budget".to_string())
    } else if error.to_string().contains("epoch") || error.to_string().contains("interrupt") {
        ExtensionError::ResourceLimit("extension exceeded its wall-clock deadline".to_string())
    } else {
        ExtensionError::Runtime(error.to_string())
    }
}

/// Content-addressed package storage rooted below a BicDB data directory.
///
/// Complete modules are immutable `<sha256>.wasm` files. New bytes are synced
/// through a same-directory temporary file and atomically renamed, so a crash
/// can leave only a removable temp file and never a partial published module.
#[derive(Clone, Debug)]
pub struct ExtensionPackageStore {
    root: PathBuf,
}

impl ExtensionPackageStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(package_io_error)?;
        let store = Self { root };
        store.remove_incomplete()?;
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn package_path(&self, sha256: &str) -> Result<PathBuf> {
        validate_package_hash(sha256)?;
        Ok(self.root.join(format!("{sha256}{PACKAGE_SUFFIX}")))
    }

    /// Install module bytes and return their lowercase SHA-256. Reinstalling
    /// identical bytes is idempotent and verifies the existing package.
    pub fn install(&self, bytes: &[u8], max_module_bytes: usize) -> Result<String> {
        if bytes.is_empty() || bytes.len() > max_module_bytes {
            return Err(ExtensionError::ResourceLimit(format!(
                "module is {} bytes; host limit is {max_module_bytes}",
                bytes.len()
            )));
        }
        let sha256 = sha256_hex(bytes);
        let destination = self.package_path(&sha256)?;
        if destination.exists() {
            let _ = self.read_verified(&sha256, max_module_bytes)?;
            return Ok(sha256);
        }

        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = self.root.join(format!(
            "{TEMP_PREFIX}{}-{sequence}.tmp",
            std::process::id()
        ));
        let install_result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(package_io_error)?;
            file.write_all(bytes).map_err(package_io_error)?;
            file.sync_all().map_err(package_io_error)?;
            match fs::rename(&temporary, &destination) {
                Ok(()) => {}
                Err(_error) if destination.exists() => {
                    let _ = fs::remove_file(&temporary);
                    let _ = self.read_verified(&sha256, max_module_bytes)?;
                    return Ok(sha256.clone());
                }
                Err(error) => return Err(package_io_error(error)),
            }
            sync_directory(&self.root)?;
            Ok(sha256.clone())
        })();
        if install_result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        install_result
    }

    pub fn read_verified(&self, sha256: &str, max_module_bytes: usize) -> Result<Vec<u8>> {
        let path = self.package_path(sha256)?;
        let metadata = fs::symlink_metadata(&path).map_err(package_io_error)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(ExtensionError::Runtime(format!(
                "extension package `{sha256}` is not a regular file"
            )));
        }
        if metadata.len() == 0 || metadata.len() > max_module_bytes as u64 {
            return Err(ExtensionError::ResourceLimit(format!(
                "package `{sha256}` is {} bytes; host limit is {max_module_bytes}",
                metadata.len()
            )));
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        File::open(&path)
            .and_then(|mut file| file.read_to_end(&mut bytes))
            .map_err(package_io_error)?;
        let actual = sha256_hex(&bytes);
        if actual != sha256 {
            return Err(ExtensionError::Runtime(format!(
                "extension package hash mismatch: expected {sha256}, found {actual}"
            )));
        }
        Ok(bytes)
    }

    /// Remove crash-left temp files only. Published packages are never touched.
    pub fn remove_incomplete(&self) -> Result<usize> {
        let mut removed = 0;
        for entry in fs::read_dir(&self.root).map_err(package_io_error)? {
            let entry = entry.map_err(package_io_error)?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if !name.starts_with(TEMP_PREFIX) || !name.ends_with(".tmp") {
                continue;
            }
            let metadata = fs::symlink_metadata(entry.path()).map_err(package_io_error)?;
            if metadata.is_file() || metadata.file_type().is_symlink() {
                fs::remove_file(entry.path()).map_err(package_io_error)?;
                removed += 1;
            }
        }
        if removed > 0 {
            sync_directory(&self.root)?;
        }
        Ok(removed)
    }

    /// Explicit garbage collection. Only valid published-package filenames
    /// absent from `retained` are removed; unknown files are preserved.
    pub fn remove_unreferenced(&self, retained: &BTreeSet<String>) -> Result<usize> {
        for hash in retained {
            validate_package_hash(hash)?;
        }
        let mut removed = 0;
        for entry in fs::read_dir(&self.root).map_err(package_io_error)? {
            let entry = entry.map_err(package_io_error)?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(hash) = name.strip_suffix(PACKAGE_SUFFIX) else {
                continue;
            };
            if validate_package_hash(hash).is_ok() && !retained.contains(hash) {
                let metadata = fs::symlink_metadata(entry.path()).map_err(package_io_error)?;
                if metadata.is_file() && !metadata.file_type().is_symlink() {
                    fs::remove_file(entry.path()).map_err(package_io_error)?;
                    removed += 1;
                }
            }
        }
        if removed > 0 {
            sync_directory(&self.root)?;
        }
        Ok(removed)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RouteAuthorization {
    #[default]
    Anonymous,
    Authenticated,
    RowLevelSecurityChecked,
    Administrator,
}

impl RouteAuthorization {
    fn allows(self, required: RouteAuth) -> bool {
        match required {
            RouteAuth::Public => true,
            RouteAuth::Authenticated => !matches!(self, Self::Anonymous),
            RouteAuth::RowLevelSecurity => {
                matches!(self, Self::RowLevelSecurityChecked | Self::Administrator)
            }
            RouteAuth::Admin => matches!(self, Self::Administrator),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExtensionHttpRequest {
    pub id: String,
    pub method: HttpMethod,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub query: BTreeMap<String, String>,
    pub body: serde_json::Value,
    pub context: InvocationContext,
    pub authorization: RouteAuthorization,
}

#[derive(Clone)]
struct LoadedExtension {
    module: WasmExtension,
    active_invocations: Arc<AtomicU32>,
    max_concurrency: u32,
}

impl LoadedExtension {
    fn invoke(&self, invocation: &ExtensionInvocation) -> Result<ExtensionInvocationResult> {
        let permit =
            InvocationPermit::acquire(Arc::clone(&self.active_invocations), self.max_concurrency)?;
        let result = self.module.invoke(invocation);
        drop(permit);
        result
    }
}

struct InvocationPermit {
    counter: Arc<AtomicU32>,
}

impl InvocationPermit {
    fn acquire(counter: Arc<AtomicU32>, limit: u32) -> Result<Self> {
        let mut active = counter.load(Ordering::Acquire);
        loop {
            if active >= limit {
                return Err(ExtensionError::ResourceLimit(format!(
                    "extension concurrency limit {limit} is busy"
                )));
            }
            match counter.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(Self { counter }),
                Err(current) => active = current,
            }
        }
    }
}

impl Drop for InvocationPermit {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone)]
struct RouteBinding {
    definition: RestResourceDefinition,
    module: Arc<LoadedExtension>,
}

#[derive(Clone)]
struct WebsiteBinding {
    deployment: ActiveWebsiteDeployment,
    module: Arc<LoadedExtension>,
}

#[derive(Default)]
struct RuntimeSnapshot {
    modules: BTreeMap<String, Arc<LoadedExtension>>,
    routes: BTreeMap<(HttpMethod, String), RouteBinding>,
    websites: Vec<WebsiteBinding>,
}

/// Atomically replaceable extension runtime.
///
/// `sync_catalog` fully validates and loads a new snapshot before publishing
/// it. Existing requests retain their `Arc` to the previous snapshot/module,
/// so a failed deployment or concurrent upgrade never tears down a working
/// extension.
#[derive(Clone)]
pub struct ExtensionRuntime {
    packages: ExtensionPackageStore,
    config: WasmHostConfig,
    snapshot: Arc<RwLock<Arc<RuntimeSnapshot>>>,
}

impl std::fmt::Debug for ExtensionRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.snapshot.read().expect("extension snapshot poisoned");
        formatter
            .debug_struct("ExtensionRuntime")
            .field("package_root", &self.packages.root())
            .field("active_extensions", &snapshot.modules.len())
            .field("routes", &snapshot.routes.len())
            .field("websites", &snapshot.websites.len())
            .finish()
    }
}

impl ExtensionRuntime {
    pub fn new(packages: ExtensionPackageStore, config: WasmHostConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            packages,
            config,
            snapshot: Arc::new(RwLock::new(Arc::new(RuntimeSnapshot::default()))),
        })
    }

    pub fn package_store(&self) -> &ExtensionPackageStore {
        &self.packages
    }

    pub fn sync_catalog(
        &self,
        installations: &[ExtensionInstallation],
        resources: &[RestResourceDefinition],
    ) -> Result<()> {
        self.sync_catalog_with_websites(installations, resources, &[])
    }

    pub fn sync_catalog_with_websites(
        &self,
        installations: &[ExtensionInstallation],
        resources: &[RestResourceDefinition],
        websites: &[ActiveWebsiteDeployment],
    ) -> Result<()> {
        let mut next = RuntimeSnapshot::default();
        let roots = installations
            .iter()
            .filter(|installation| installation.state == ExtensionState::Active)
            .map(|installation| installation.manifest.identity.name.clone())
            .collect::<Vec<_>>();
        let order = resolve_extension_order(installations, &roots, true)?;
        for extension_name in order {
            let installation = installations
                .iter()
                .find(|installation| {
                    installation
                        .manifest
                        .identity
                        .name
                        .eq_ignore_ascii_case(&extension_name)
                })
                .expect("dependency resolver only returns installed extensions");
            let bytes = self
                .packages
                .read_verified(&installation.module_sha256, self.config.max_module_bytes)?;
            let module = WasmExtension::load(&bytes, self.config.clone())?;
            if module.sha256_hex() != installation.module_sha256 {
                return Err(ExtensionError::Runtime(format!(
                    "package hash changed while loading extension `{}`",
                    installation.manifest.identity.name
                )));
            }
            if module.manifest() != &installation.manifest {
                return Err(ExtensionError::InvalidManifest(format!(
                    "catalog manifest differs from package manifest for `{}`",
                    installation.manifest.identity.name
                )));
            }
            let name = installation.manifest.identity.name.to_ascii_lowercase();
            if next
                .modules
                .insert(
                    name.clone(),
                    Arc::new(LoadedExtension {
                        max_concurrency: module.manifest().limits.max_concurrency,
                        module,
                        active_invocations: Arc::new(AtomicU32::new(0)),
                    }),
                )
                .is_some()
            {
                return Err(ExtensionError::InvalidManifest(format!(
                    "duplicate active extension `{name}`"
                )));
            }
        }

        for resource in resources.iter().filter(|resource| resource.enabled) {
            resource.validate()?;
            let extension_name = resource.extension.to_ascii_lowercase();
            let module = next.modules.get(&extension_name).cloned().ok_or_else(|| {
                ExtensionError::InvalidManifest(format!(
                    "resource `{}` references inactive extension `{extension_name}`",
                    resource.name
                ))
            })?;
            if !module
                .module
                .manifest()
                .routes
                .iter()
                .any(|route| route.export == resource.export)
            {
                return Err(ExtensionError::InvalidManifest(format!(
                    "resource `{}` references undeclared export `{}`",
                    resource.name, resource.export
                )));
            }
            for method in &resource.methods {
                let key = (*method, resource.path.clone());
                if next
                    .routes
                    .insert(
                        key,
                        RouteBinding {
                            definition: resource.clone(),
                            module: Arc::clone(&module),
                        },
                    )
                    .is_some()
                {
                    return Err(ExtensionError::InvalidManifest(format!(
                        "duplicate route {method} {}",
                        resource.path
                    )));
                }
            }
        }

        let mut website_mounts = BTreeSet::new();
        for deployment in websites
            .iter()
            .filter(|deployment| deployment.definition.enabled)
        {
            deployment.validate()?;
            let definition = &deployment.definition;
            let extension_name = definition.extension.to_ascii_lowercase();
            let module = next.modules.get(&extension_name).cloned().ok_or_else(|| {
                ExtensionError::InvalidManifest(format!(
                    "website `{}` references inactive extension `{extension_name}`",
                    definition.name
                ))
            })?;
            if !module
                .module
                .manifest()
                .routes
                .iter()
                .any(|route| route.export == definition.export)
            {
                return Err(ExtensionError::InvalidManifest(format!(
                    "website `{}` references undeclared route export `{}`",
                    definition.name, definition.export
                )));
            }
            let mount_key = (
                definition
                    .host
                    .as_ref()
                    .map(|host| host.to_ascii_lowercase()),
                definition.mount_path.clone(),
            );
            if !website_mounts.insert(mount_key) {
                return Err(ExtensionError::InvalidManifest(format!(
                    "duplicate website mount {}{}",
                    definition
                        .host
                        .as_deref()
                        .map(|host| format!("{host}:"))
                        .unwrap_or_default(),
                    definition.mount_path
                )));
            }
            let encoded_bytes = serde_json::to_vec(deployment)
                .map_err(|error| ExtensionError::InvalidPayload(error.to_string()))?
                .len();
            let content_bytes = serde_json::to_vec(&deployment.release.content)
                .map_err(|error| ExtensionError::InvalidPayload(error.to_string()))?;
            if sha256_hex(&content_bytes) != deployment.release.content_sha256 {
                return Err(ExtensionError::InvalidManifest(format!(
                    "website `{}` release content SHA-256 does not match",
                    definition.name
                )));
            }
            let input_limit = self
                .config
                .max_input_bytes
                .min(module.module.manifest().limits.max_input_bytes as usize);
            if encoded_bytes.saturating_add(64 * 1024) > input_limit {
                return Err(ExtensionError::ResourceLimit(format!(
                    "website `{}` bundle is {encoded_bytes} bytes; renderer input limit is {input_limit}",
                    definition.name
                )));
            }
            next.websites.push(WebsiteBinding {
                deployment: deployment.clone(),
                module,
            });
        }
        *self.snapshot.write().expect("extension snapshot poisoned") = Arc::new(next);
        Ok(())
    }

    pub fn active_extensions(&self) -> Vec<String> {
        self.snapshot
            .read()
            .expect("extension snapshot poisoned")
            .modules
            .keys()
            .cloned()
            .collect()
    }

    /// Invoke a declared extension export directly. SQL/function, index, and
    /// observability adapters use this entry point. WASM storage providers are
    /// intentionally rejected: storage registrations require a separately
    /// trusted native adapter because they participate in durability.
    pub fn invoke(
        &self,
        extension: &str,
        invocation: &ExtensionInvocation,
    ) -> Result<ExtensionInvocationResult> {
        let snapshot = self
            .snapshot
            .read()
            .expect("extension snapshot poisoned")
            .clone();
        let module = snapshot
            .modules
            .get(&extension.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| {
                ExtensionError::Runtime(format!(
                    "extension `{extension}` is not active in this runtime"
                ))
            })?;
        let declared = match invocation.kind {
            InvocationKind::Function => module
                .module
                .manifest()
                .functions
                .iter()
                .any(|registration| registration.export == invocation.target),
            InvocationKind::Index => module
                .module
                .manifest()
                .indexes
                .iter()
                .any(|registration| registration.export == invocation.target),
            InvocationKind::Storage => {
                return Err(ExtensionError::Runtime(
                    "WASM storage invocation is forbidden; storage providers require a trusted native host adapter"
                        .to_string(),
                ))
            }
            InvocationKind::HttpRoute => module
                .module
                .manifest()
                .routes
                .iter()
                .any(|registration| registration.export == invocation.target),
            InvocationKind::DatabaseEvent | InvocationKind::QueueEvent => module
                .module
                .manifest()
                .subscriptions
                .iter()
                .any(|registration| registration.export == invocation.target),
            InvocationKind::Observability => module
                .module
                .manifest()
                .observability
                .iter()
                .any(|registration| registration.export == invocation.target),
        };
        if !declared {
            return Err(ExtensionError::InvalidManifest(format!(
                "extension `{extension}` did not declare {:?} export `{}`",
                invocation.kind, invocation.target
            )));
        }
        module.invoke(invocation)
    }

    pub fn dispatch_http(
        &self,
        request: ExtensionHttpRequest,
    ) -> Result<Option<ExtensionInvocationResult>> {
        let snapshot = self
            .snapshot
            .read()
            .expect("extension snapshot poisoned")
            .clone();
        if let Some(route) = snapshot
            .routes
            .get(&(request.method, request.path.clone()))
            .cloned()
        {
            if !request.authorization.allows(route.definition.auth) {
                return Err(ExtensionError::Runtime(format!(
                    "route {} {} requires {:?} authorization",
                    request.method, request.path, route.definition.auth
                )));
            }
            let invocation = ExtensionInvocation {
                id: request.id,
                kind: InvocationKind::HttpRoute,
                target: route.definition.export.clone(),
                payload: serde_json::json!({
                    "resource": route.definition,
                    "request": {
                        "method": request.method,
                        "path": request.path,
                        "headers": request.headers,
                        "query": request.query,
                        "body": request.body,
                    }
                }),
                context: request.context,
            };
            return route.module.invoke(&invocation).map(Some);
        }

        if !matches!(request.method, HttpMethod::Get | HttpMethod::Head) {
            return Ok(None);
        }
        let request_host = request
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("host"))
            .map(|(_, host)| normalized_request_host(host));
        let website = snapshot
            .websites
            .iter()
            .filter(|binding| {
                website_host_matches(
                    binding.deployment.definition.host.as_deref(),
                    request_host.as_deref(),
                ) && website_path_matches(&binding.deployment.definition.mount_path, &request.path)
            })
            .max_by_key(|binding| {
                (
                    binding.deployment.definition.host.is_some(),
                    binding.deployment.definition.mount_path.len(),
                )
            })
            .cloned();
        let Some(website) = website else {
            return Ok(None);
        };
        let definition = &website.deployment.definition;
        if !request.authorization.allows(definition.auth) {
            return Err(ExtensionError::Runtime(format!(
                "website {} {} requires {:?} authorization",
                request.method, request.path, definition.auth
            )));
        }
        let relative_path = if definition.mount_path == "/" {
            request.path.clone()
        } else {
            request
                .path
                .strip_prefix(&definition.mount_path)
                .filter(|path| !path.is_empty())
                .unwrap_or("/")
                .to_string()
        };
        let invocation = ExtensionInvocation {
            id: request.id,
            kind: InvocationKind::HttpRoute,
            target: definition.export.clone(),
            payload: serde_json::json!({
                "website": website.deployment,
                "request": {
                    "method": request.method,
                    "path": request.path,
                    "relative_path": relative_path,
                    "headers": request.headers,
                    "query": request.query,
                    "body": request.body,
                }
            }),
            context: request.context,
        };
        website.module.invoke(&invocation).map(Some)
    }

    pub fn dispatch_event(
        &self,
        binding: &EventBindingDefinition,
        id: String,
        payload: serde_json::Value,
        context: InvocationContext,
    ) -> Result<ExtensionInvocationResult> {
        binding.validate()?;
        if !binding.enabled {
            return Err(ExtensionError::Runtime(format!(
                "event subscription `{}` is disabled",
                binding.name
            )));
        }
        let snapshot = self
            .snapshot
            .read()
            .expect("extension snapshot poisoned")
            .clone();
        let module = snapshot
            .modules
            .get(&binding.extension.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| {
                ExtensionError::Runtime(format!(
                    "extension `{}` is not active in this runtime",
                    binding.extension
                ))
            })?;
        if !module
            .module
            .manifest()
            .subscriptions
            .iter()
            .any(|subscription| subscription.export == binding.export)
        {
            return Err(ExtensionError::InvalidManifest(format!(
                "event subscription `{}` references undeclared export `{}`",
                binding.name, binding.export
            )));
        }
        module.invoke(&ExtensionInvocation {
            id,
            kind: match binding.source {
                crate::EventSource::Database { .. } => InvocationKind::DatabaseEvent,
                crate::EventSource::Queue { .. } => InvocationKind::QueueEvent,
            },
            target: binding.export.clone(),
            payload,
            context,
        })
    }
}

fn normalized_request_host(value: &str) -> String {
    let value = value.trim().to_ascii_lowercase();
    if let Some((host, port)) = value.rsplit_once(':') {
        if !host.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) {
            return host.to_string();
        }
    }
    value
}

fn website_host_matches(configured: Option<&str>, requested: Option<&str>) -> bool {
    configured.is_none_or(|configured| {
        requested.is_some_and(|requested| configured.eq_ignore_ascii_case(requested))
    })
}

fn website_path_matches(mount: &str, path: &str) -> bool {
    mount == "/"
        || path == mount
        || path
            .strip_prefix(mount)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn validate_package_hash(value: &str) -> Result<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(ExtensionError::InvalidManifest(
            "package hash must be 64 lowercase hexadecimal characters".to_string(),
        ))
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn package_io_error(error: std::io::Error) -> ExtensionError {
    ExtensionError::Runtime(format!("extension package I/O: {error}"))
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(package_io_error)
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        ExtensionActivation, ExtensionCapability, ExtensionIdentity, ExtensionLimits,
        ExtensionPermissions, ExtensionRegistrar, ExtensionState, HttpMethod,
        HttpRouteRegistration, InvocationContext, InvocationKind, RestResourceDefinition,
        RouteAuth,
    };

    fn manifest_json() -> String {
        let mut registrar = ExtensionRegistrar::new(ExtensionIdentity {
            name: "instant_rest".to_string(),
            version: "1.0.0".to_string(),
            abi_version: EXTENSION_ABI_VERSION,
            description: "test".to_string(),
        });
        registrar
            .declare(ExtensionCapability::HttpRoutes)
            .register_route(HttpRouteRegistration {
                name: "health".to_string(),
                method: HttpMethod::Get,
                path: "/health".to_string(),
                export: "handle_health".to_string(),
                auth: RouteAuth::Public,
            })
            .permissions(ExtensionPermissions::default())
            .limits(ExtensionLimits::default());
        serde_json::to_string(&registrar.finish().unwrap()).unwrap()
    }

    fn fixed_result_module(manifest: &str) -> Vec<u8> {
        let escaped = manifest
            .as_bytes()
            .iter()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        let result = br#"{"status":200,"body":{"ok":true},"ack":true}"#;
        let result_escaped = result
            .iter()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        let result_pointer = 16_384u32;
        let wat = format!(
            r#"(module
                (memory (export "memory") 2 1024)
                (table 1 externref)
                (global $next (mut i32) (i32.const 32768))
                (data (i32.const 1024) "{escaped}")
                (data (i32.const {result_pointer}) "{result_escaped}")
                (func (export "bicdb_extension_abi_version") (result i32)
                    i32.const 1)
                (func (export "bicdb_extension_manifest_ptr") (result i32)
                    i32.const 1024)
                (func (export "bicdb_extension_manifest_len") (result i32)
                    i32.const {manifest_len})
                (func (export "bicdb_extension_alloc") (param $len i32) (result i32)
                    (local $ptr i32)
                    global.get $next
                    local.tee $ptr
                    local.get $len
                    i32.add
                    global.set $next
                    local.get $ptr)
                (func (export "bicdb_extension_dealloc") (param i32 i32))
                (func (export "bicdb_extension_invoke") (param i32 i32) (result i64)
                    i64.const {packed}))
            "#,
            manifest_len = manifest.len(),
            packed = ((result_pointer as u64) << 32) | result.len() as u64,
        );
        wat::parse_str(wat).unwrap()
    }

    #[test]
    fn loads_validates_hashes_and_invokes_an_import_free_reference_type_module() {
        let bytes = fixed_result_module(&manifest_json());
        let extension = WasmExtension::load(&bytes, WasmHostConfig::default()).unwrap();
        assert_eq!(extension.manifest().identity.name, "instant_rest");
        assert_eq!(extension.sha256_hex().len(), 64);
        let result = extension
            .invoke(&ExtensionInvocation {
                id: "request-1".to_string(),
                kind: InvocationKind::HttpRoute,
                target: "health".to_string(),
                payload: json!({}),
                context: InvocationContext::default(),
            })
            .unwrap();
        assert_eq!(result.status, 200);
        assert_eq!(result.body, json!({"ok": true}));
        assert!(result.ack);
    }

    #[test]
    fn rejects_wasi_or_any_other_import() {
        let bytes = wat::parse_str(
            r#"(module
                (import "wasi_snapshot_preview1" "fd_write"
                    (func (param i32 i32 i32 i32) (result i32))))"#,
        )
        .unwrap();
        assert!(WasmExtension::load(&bytes, WasmHostConfig::default())
            .unwrap_err()
            .to_string()
            .contains("may not import WASI"));
    }

    #[test]
    fn rejects_host_policy_overrides_from_the_manifest() {
        let mut manifest: ExtensionManifest = serde_json::from_str(&manifest_json()).unwrap();
        manifest.limits.memory_bytes = 128 * 1024 * 1024;
        let bytes = fixed_result_module(&serde_json::to_string(&manifest).unwrap());
        assert!(WasmExtension::load(&bytes, WasmHostConfig::default())
            .unwrap_err()
            .to_string()
            .contains("above host policy"));
    }

    #[test]
    fn package_store_publishes_atomically_and_cleans_only_temp_files() {
        let directory = tempfile::tempdir().unwrap();
        let store = ExtensionPackageStore::open(directory.path()).unwrap();
        let bytes = fixed_result_module(&manifest_json());
        let hash = store.install(&bytes, 1024 * 1024).unwrap();
        assert_eq!(store.read_verified(&hash, 1024 * 1024).unwrap(), bytes);
        assert_eq!(store.install(&bytes, 1024 * 1024).unwrap(), hash);

        let incomplete = directory.path().join(format!("{TEMP_PREFIX}crash.tmp"));
        fs::write(&incomplete, b"partial").unwrap();
        let unrelated = directory.path().join("operator-notes.txt");
        fs::write(&unrelated, b"keep").unwrap();
        assert_eq!(store.remove_incomplete().unwrap(), 1);
        assert!(!incomplete.exists());
        assert!(unrelated.exists());
        assert!(store.package_path(&hash).unwrap().exists());
    }

    #[test]
    fn runtime_snapshot_routes_requests_and_survives_failed_reload() {
        let directory = tempfile::tempdir().unwrap();
        let store = ExtensionPackageStore::open(directory.path()).unwrap();
        let bytes = fixed_result_module(&manifest_json());
        let hash = store.install(&bytes, 1024 * 1024).unwrap();
        let module = WasmExtension::load(&bytes, WasmHostConfig::default()).unwrap();
        let installation = ExtensionInstallation {
            manifest: module.manifest().clone(),
            module_sha256: hash,
            state: ExtensionState::Active,
            installed_at_ms: 1,
            activation: Some(ExtensionActivation {
                catalog_generation: 1,
                topology_generation: 0,
                ready_nodes: BTreeSet::from(["local".to_string()]),
                required_nodes: BTreeSet::from(["local".to_string()]),
                quorum_committed: false,
                activated_at_ms: 1,
            }),
            last_error: None,
        };
        let resource = RestResourceDefinition {
            name: "patients".to_string(),
            extension: "instant_rest".to_string(),
            relation: "public.patients".to_string(),
            path: "/patients".to_string(),
            export: "handle_health".to_string(),
            methods: BTreeSet::from([HttpMethod::Get]),
            auth: RouteAuth::Authenticated,
            openapi: true,
            enabled: true,
        };
        let runtime = ExtensionRuntime::new(store, WasmHostConfig::default()).unwrap();
        runtime
            .sync_catalog(
                std::slice::from_ref(&installation),
                std::slice::from_ref(&resource),
            )
            .unwrap();
        assert_eq!(runtime.active_extensions(), vec!["instant_rest"]);
        assert!(runtime
            .dispatch_http(ExtensionHttpRequest {
                id: "anonymous".to_string(),
                method: HttpMethod::Get,
                path: "/patients".to_string(),
                headers: BTreeMap::new(),
                query: BTreeMap::new(),
                body: serde_json::Value::Null,
                context: InvocationContext::default(),
                authorization: RouteAuthorization::Anonymous,
            })
            .is_err());
        let response = runtime
            .dispatch_http(ExtensionHttpRequest {
                id: "authenticated".to_string(),
                method: HttpMethod::Get,
                path: "/patients".to_string(),
                headers: BTreeMap::new(),
                query: BTreeMap::new(),
                body: serde_json::Value::Null,
                context: InvocationContext::default(),
                authorization: RouteAuthorization::Authenticated,
            })
            .unwrap()
            .unwrap();
        assert_eq!(response.status, 200);

        let mut invalid = installation;
        invalid.module_sha256 = "b".repeat(64);
        assert!(runtime
            .sync_catalog(&[invalid], std::slice::from_ref(&resource))
            .is_err());
        assert_eq!(runtime.active_extensions(), vec!["instant_rest"]);
    }
}
