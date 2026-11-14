use bicdb_core::{
    cluster_event_record_id, BicDbError, ClusterEventClient, ClusterRpcEvidenceLimits,
    ClusterTopology, DistributionConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::time::Instant;
use uuid::Uuid;

#[path = "external.rs"]
mod external;
#[path = "journal.rs"]
mod journal;
use journal::{Journal, Observer};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    run_id: Uuid,
    phase: String,
    operations: Vec<Operation>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Operation {
    Append {
        logical_id: String,
        namespace: String,
        key: String,
        value: Value,
    },
    StrongRead {
        logical_id: String,
        namespace: String,
        key: String,
        expected: Value,
        acknowledged_by: Option<String>,
    },
}

impl Operation {
    fn identity(&self) -> (&str, &str, &str, &Value) {
        match self {
            Self::Append {
                logical_id,
                namespace,
                key,
                value,
            } => (logical_id, namespace, key, value),
            Self::StrongRead {
                logical_id,
                namespace,
                key,
                expected,
                ..
            } => (logical_id, namespace, key, expected),
        }
    }
}

fn invalid(message: &str) -> Box<dyn Error> {
    io::Error::new(io::ErrorKind::InvalidInput, message).into()
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(invalid("input exceeds explicit byte bound"));
    }
    Ok(bytes)
}

fn hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn retain_input(directory: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(directory.join(name))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn binary_hash() -> Result<String> {
    let mut file = File::open(std::env::current_exe()?)?;
    let mut hash = Sha256::new();
    let mut bytes = [0; 65_536];
    loop {
        let n = file.read(&mut bytes)?;
        if n == 0 {
            break;
        }
        hash.update(&bytes[..n]);
    }
    Ok(hex::encode(hash.finalize()))
}

fn validate(plan: &Plan) -> Result<()> {
    if plan.run_id.is_nil()
        || plan.phase.is_empty()
        || plan.phase.len() > 128
        || plan.operations.is_empty()
        || plan.operations.len() > 1024
    {
        return Err(invalid(
            "nonempty phase, run UUID and 1..1024 operations required",
        ));
    }
    let mut ids = BTreeSet::new();
    let mut writes = BTreeMap::new();
    let mut write_keys = BTreeSet::new();
    for operation in &plan.operations {
        let (id, namespace, key, value) = operation.identity();
        cluster_event_record_id(namespace, key)?;
        if [id, namespace, key]
            .iter()
            .any(|s| s.is_empty() || s.len() > 4096)
            || !ids.insert(id)
            || serde_json::to_vec(value)?.len() > 32_768
        {
            return Err(invalid(
                "invalid or duplicate operation identity / oversized payload",
            ));
        }
        match operation {
            Operation::Append { .. } => {
                if !write_keys.insert((namespace, key)) {
                    return Err(invalid("duplicate append key; no outer write replay"));
                }
                writes.insert(id, (namespace, key, value));
            }
            Operation::StrongRead {
                acknowledged_by: Some(ack),
                ..
            } => {
                if writes.get(ack.as_str()) != Some(&(namespace, key, value)) {
                    return Err(invalid("ACK reference must match an earlier exact append"));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn error_record(error: &BicDbError) -> Value {
    let mut sources = Vec::new();
    let mut source = error.source();
    while let Some(current) = source {
        sources.push(current.to_string());
        source = current.source();
    }
    json!({"display":error.to_string(),"debug":format!("{error:?}"),"source_chain":sources})
}

fn boundary_failure(stage: &str, id: &str, observed: Value, error: &dyn Error) -> Box<dyn Error> {
    // The journal may now be fenced/unwritable. Preserve the actual SDK outcome
    // in the returned error for the independent stderr/exit witness instead of
    // replacing it with a generic second "journal fenced" error. This cannot
    // turn an SDK failure into a reply or prove no success from missing output.
    let mut chain = vec![error.to_string()];
    let mut source = error.source();
    while let Some(current) = source {
        chain.push(current.to_string());
        source = current.source();
    }
    io::Error::other(
        json!({"logical_id":id,"evidence_failure_stage":stage,
      "actual_sdk_observation":observed,"evidence_error_chain":chain,
      "effect_unknown_unless_actual_sdk_ok":true,"timeout_is_rollback":false,
      "missing_output_never_proves_no_sdk_ok":true,"producer_fenced":true,
      "independent_stderr_capture_required":true})
        .to_string(),
    )
    .into()
}

pub fn run() -> Result<i32> {
    let source_claim = option_env!("BICDB_EXPERIMENT_SOURCE_SHA")
        .filter(|s| s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| {
            invalid("build must set BICDB_EXPERIMENT_SOURCE_SHA to its exact source commit")
        })?;
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 4 && args.len() != 5 {
        return Err(invalid("usage: native_external_ledger CONFIG_JSON TOPOLOGY_JSON PLAN_JSON NEW_OUTPUT_DIRECTORY [EXTERNAL_SINK_JSON]"));
    }
    let config_bytes = read_bounded(Path::new(&args[0]), 1024 * 1024)?;
    let topology_bytes = read_bounded(Path::new(&args[1]), 16 * 1024 * 1024)?;
    let plan_bytes = read_bounded(Path::new(&args[2]), 40 * 1024 * 1024)?;
    let config: DistributionConfig = serde_json::from_slice(&config_bytes)?;
    let topology: ClusterTopology = serde_json::from_slice(&topology_bytes)?;
    let plan: Plan = serde_json::from_slice(&plan_bytes)?;
    validate(&plan)?;
    if config.transport.dev_localhost_plaintext {
        return Err(invalid("experiment client requires authenticated TLS"));
    }
    let output = Path::new(&args[3]);
    let journal = Journal::create(output, plan.run_id)?;
    retain_input(output, "config.json", &config_bytes)?;
    retain_input(output, "topology.json", &topology_bytes)?;
    retain_input(output, "plan.json", &plan_bytes)?;
    File::open(output)?.sync_all()?;
    let binary = binary_hash()?;
    if args.len() == 5 {
        let member_tls = config
            .transport
            .tls
            .as_ref()
            .ok_or_else(|| invalid("external evidence requires actual member TLS"))?;
        let sink = external::External::start(
            Path::new(&args[4]),
            output,
            plan.run_id,
            &config.node_id.to_string(),
            member_tls,
            source_claim,
            &binary,
            &[
                ("config.json", &config_bytes),
                ("topology.json", &topology_bytes),
                ("plan.json", &plan_bytes),
            ],
        )?;
        journal.attach_external(sink)?;
    }
    journal.append(
        "run_started",
        json!({
            "phase":plan.phase,"operations":plan.operations.len(),
            "client_binary_sha256":binary,"core_version":env!("CARGO_PKG_VERSION"),
        "build_source_claim":source_claim,"debug_assertions":cfg!(debug_assertions),
        "caller_member_identity":config.node_id,
            "config_sha256":hash(&config_bytes),"topology_sha256":hash(&topology_bytes),
            "plan_sha256":hash(&plan_bytes),"initial_topology":topology,
            "client_path":"stock ClusterEventClient, sequential, one unchanged instance",
            "authorization_contract":"trusted member API; APP requires Active metadata voter, not ordinary application authentication",
            "typed_read_mode":"Strong; no legacy fallback","request_timeout":"SDK default 30s",
            "rpc_evidence_limits":{"max_records":65536,"max_bytes":1073741824u64},
            "external_sink":journal.external_description()?,
            "ack_corpus":"all known or possible SDK/server successes AND all attempted/uncertain writes; wrapper ACK never narrows reconciliation",
            "qualification_status":"not_qualified","throughput_benchmark":false,
        }),
    )?;
    let observer = Observer::new(journal.clone());
    let client = ClusterEventClient::new(&config, topology)?.with_rpc_evidence_observer(
        observer.clone(),
        ClusterRpcEvidenceLimits {
            max_records: 65_536,
            max_bytes: 1024 * 1024 * 1024,
        },
    )?;
    let mut acknowledged = BTreeSet::new();
    let mut outcomes: BTreeMap<String, usize> = BTreeMap::new();
    let mut finished = 0;
    let mut fenced = false;
    for operation in &plan.operations {
        let (id, namespace, key, value) = operation.identity();
        let logical_started = Instant::now();
        journal.append(
            "logical_attempt",
            json!({
                "command":operation,"payload_sha256":hash(&serde_json::to_vec(value)?),
                "canonical_record_id":cluster_event_record_id(namespace,key)?,
                "payload_hash_encoding":"serde_json::to_vec(Value) UTF-8",
            }),
        )?;
        observer.set_active(Some(id))?;
        let started = Instant::now();
        let result = match operation {
            Operation::Append { .. } => client
                .append(namespace, key, value.clone())
                .map(|inserted| json!({"inserted":inserted})),
            Operation::StrongRead { .. } => client
                .get_strong(namespace, key)
                .map(|v| json!({"found":v.is_some(),"value":v})),
        };
        let sdk_elapsed_ns = started.elapsed().as_nanos();
        observer.set_active(None).map_err(|error| {
            boundary_failure(
                "attribution_cleanup",
                id,
                match &result {
                    Ok(reply) => json!({"sdk_return":"Ok","reply":reply}),
                    Err(sdk_error) => json!({"sdk_return":"Err","error":error_record(sdk_error)}),
                },
                &error,
            )
        })?;
        let (outcome, detail) = match result {
            Ok(reply) => match operation {
                Operation::Append { .. } => {
                    let ack = journal.append(
                        "sdk_ack_observed",
                        json!({
                            "logical_id":id,"reply":reply,"sdk_elapsed_ns":sdk_elapsed_ns,
                        }),
                    ).map_err(|error| boundary_failure("explicit_sdk_ack_record", id,
                      json!({"sdk_return":"Ok","reply":reply,"sdk_elapsed_ns":sdk_elapsed_ns,
                        "explicit_ack_record_durability":"uncertain; SDK success remains in required ACK corpus"}), &error))?;
                    acknowledged.insert(id.as_str());
                    (
                        "acknowledged",
                        json!({"sdk_reply":reply,"durable_ack_receipt":ack}),
                    )
                }
                Operation::StrongRead {
                    expected,
                    acknowledged_by,
                    ..
                } => {
                    let ack_observed = acknowledged_by
                        .as_ref()
                        .is_some_and(|id| acknowledged.contains(id.as_str()));
                    let outcome = if reply["found"] == true && reply["value"] == *expected {
                        "exact_match"
                    } else if reply["found"] == false && ack_observed {
                        "acknowledged_value_absent"
                    } else if reply["found"] == false {
                        "expected_value_absent"
                    } else {
                        "payload_mismatch"
                    };
                    (
                        outcome,
                        json!({"sdk_reply":reply,"prior_ack_observed":ack_observed}),
                    )
                }
            },
            Err(error) => {
                fenced = matches!(&error, BicDbError::RpcEvidenceFailure { .. });
                let outcome = if matches!(operation, Operation::Append { .. }) {
                    "write_error_unresolved"
                } else {
                    "read_error"
                };
                (
                    outcome,
                    json!({"error":error_record(&error),"observer_fenced":fenced,
                    "timeout_is_rollback":false,"outer_write_retry":false}),
                )
            }
        };
        journal.append("logical_terminal", json!({
            "logical_id":id,"outcome":outcome,"detail":detail,"sdk_elapsed_ns":sdk_elapsed_ns,
            "client_elapsed_before_terminal_fsync_ns":logical_started.elapsed().as_nanos(),
            "latency_scope":"SDK call includes passive RPC evidence fsync; excludes terminal/ACK ledger fsync",
        })).map_err(|error| boundary_failure("logical_terminal", id,
          json!({"outcome":outcome,"detail":detail}), &error))?;
        *outcomes.entry(outcome.to_owned()).or_default() += 1;
        finished += 1;
        if fenced {
            break;
        }
    }
    let successful = finished == plan.operations.len()
        && outcomes
            .keys()
            .all(|v| v == "acknowledged" || v == "exact_match");
    let summary = json!({
        "plan_completed":finished==plan.operations.len(),"planned_operations":plan.operations.len(),
        "terminal_operations":finished,"outcomes":outcomes,"observer_fenced":fenced,
        "unattempted_logical_ids":plan.operations.iter().skip(finished).map(|op|op.identity().0).collect::<Vec<_>>(),
        "all_operations_succeeded":successful,"qualification_status":"not_qualified",
        "post_convergence_capacity_demonstrated":false,
    });
    let receipt = journal.append("run_completed", summary.clone())?;
    // Only an independently retained successful process exit/stdout receipt can
    // prove the trailing completion append returned; visible bytes alone cannot.
    println!(
        "{}",
        json!({"summary":summary,"completion_receipt":receipt})
    );
    Ok(if fenced {
        2
    } else if successful {
        0
    } else {
        1
    })
}
