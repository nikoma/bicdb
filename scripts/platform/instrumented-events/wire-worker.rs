//! Test-only member-side, single-RPC TLS bridge. No retry or application routing.
//! Linked to the frozen BicDB core; private keys remain on the member host.
use bicdb_core::replication_transport::{
    build_client_tls, client_tls_stream, replication_certificate_sha256,
};
use bicdb_core::{
    load_distribution_config, ClusterBootstrapSnapshot, ClusterEventRequest, ClusterTopology,
    DistributionStore, CLUSTER_DATA_PROTOCOL_VERSION,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    io::{self, BufRead, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
type AnyError = Box<dyn Error + Send + Sync>;
fn ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn fail(message: &str) -> AnyError {
    io::Error::new(io::ErrorKind::InvalidData, message).into()
}
fn error_json(error: &(dyn Error + 'static)) -> Value {
    let mut chain = Vec::new();
    let mut current = Some(error);
    while let Some(e) = current {
        chain.push(json!({"display":e.to_string(),"debug":format!("{e:?}"),"raw_os_error":e.downcast_ref::<io::Error>().and_then(|x|x.raw_os_error())}));
        current = e.source();
    }
    json!({"display":error.to_string(),"debug":format!("{error:?}"),"chain":chain})
}
fn frame(
    stream: &mut (impl Read + Write),
    request: &[u8],
    limit: usize,
    stage: &mut String,
) -> Result<Vec<u8>, AnyError> {
    if request.is_empty() || request.len() > limit || request.len() > u32::MAX as usize {
        return Err(fail("request exceeds frame limit"));
    }
    *stage = "frame_write".into();
    stream.write_all(&(request.len() as u32).to_be_bytes())?;
    stream.write_all(request)?;
    stream.flush()?;
    *stage = "frame_read_header".into();
    let mut header = [0; 4];
    stream.read_exact(&mut header)?;
    let len = u32::from_be_bytes(header) as usize;
    if len == 0 || len > limit {
        return Err(fail("response exceeds frame limit"));
    }
    *stage = "frame_read_body".into();
    let mut body = vec![0; len];
    stream.read_exact(&mut body)?;
    Ok(body)
}
fn main() -> Result<(), AnyError> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        return Err(fail("usage: wire-worker MEMBER_ROOT EXACT_TEST_NAMESPACE"));
    }
    let root = Path::new(&args[1]);
    let namespace = &args[2];
    bicdb_core::cluster_event_record_id(namespace, "validation")?;
    let config = load_distribution_config(root)?;
    let store = DistributionStore::open(root, config.clone(), true)?;
    let mut topology = store.topology().clone();
    drop(store);
    let tls = if config.transport.dev_localhost_plaintext {
        None
    } else {
        Some(build_client_tls(
            config
                .transport
                .tls
                .as_ref()
                .ok_or_else(|| fail("missing TLS config"))?,
        )?)
    };
    let fingerprint = if tls.is_some() {
        Some(replication_certificate_sha256(
            &config.transport.tls.as_ref().unwrap().cert_path,
        )?)
    } else {
        None
    };
    let own = topology
        .nodes
        .get(&config.node_id)
        .ok_or_else(|| fail("caller absent from topology"))?;
    if own.tls_certificate_sha256.is_some()
        && !fingerprint
            .as_deref()
            .is_some_and(|v| own.accepts_tls_certificate_sha256(v))
    {
        return Err(fail("caller certificate not bound to membership"));
    }
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut output = io::stdout().lock();
    writeln!(
        output,
        "{}",
        json!({"kind":"ready","wire_worker_sha256":hash(&std::fs::read(std::env::current_exe()?)?),"policy_source":"b05385d4b033edf0db03f3eccf4bdd86f7c8eddb","linked_core_source":env!("BICDB_TRACE_CORE_SOURCE"),"linked_core_version":env!("BICDB_TRACE_CORE_VERSION"),"worker_pid":std::process::id(),"unix_ns":ns(),"cluster_id":config.cluster_id,"caller_node_id":config.node_id,"namespace":namespace,"topology":topology,"transport":{"connect_timeout_ms":config.transport.connect_timeout_ms,"io_timeout_ms":config.transport.io_timeout_ms,"max_frame_bytes":config.transport.max_frame_bytes,"dev_localhost_plaintext":config.transport.dev_localhost_plaintext},"private_keys_exported":false,"client_policy":"controller-owned frozen461 policy; worker performs exactly one wire RPC; no retries"})
    )?;
    output.flush()?;
    let mut line = String::new();
    let mut sequence = 0u64;
    loop {
        line.clear();
        let size = input.read_line(&mut line)?;
        if size == 0 {
            break;
        }
        if size > config.transport.max_frame_bytes * 2 {
            return Err(fail("controller command exceeds bound"));
        }
        let command: Value = serde_json::from_str(&line)?;
        if command.get("action").and_then(Value::as_str) == Some("stop") {
            break;
        }
        sequence += 1;
        let request_id = command
            .get("wire_request_id")
            .and_then(Value::as_u64)
            .unwrap_or(sequence);
        let started = ns();
        let clock = Instant::now();
        let mut record = json!({"kind":"wire_rpc_result","completed_response_body_retained":false,"partial_network_frame_bytes_retained":false,"rpc_id":command["rpc_id"],"wire_request_id":request_id,"worker_local_rpc_sequence":sequence,"worker_pid":std::process::id(),"caller_node_id":config.node_id,"started_unix_ns":started});
        let mut stage = "validate_command".to_string();
        let result = (|| -> Result<(), AnyError> {
            if let Some(value) = command.get("topology") {
                let next: ClusterTopology = serde_json::from_value(value.clone())?;
                next.validate()?;
                if next.cluster_id != config.cluster_id {
                    return Err(fail("controller topology cluster mismatch"));
                }
                if next.generation == topology.generation && next != topology {
                    return Err(fail("conflicting topology at equal generation"));
                }
                if next.generation > topology.generation {
                    topology = next;
                }
            }
            let destination = command["destination_node_id"]
                .as_str()
                .ok_or_else(|| fail("missing destination"))?;
            let node = topology
                .nodes
                .iter()
                .find(|(id, _)| id.to_string() == destination)
                .map(|(_, n)| n)
                .ok_or_else(|| fail("destination missing from installed topology"))?;
            if format!("{:?}", node.lifecycle) == "Decommissioned" {
                return Err(fail("destination decommissioned"));
            }
            let request = command["request"].clone();
            if let Some(event) = request.get("ApplicationEvent") {
                let parsed: ClusterEventRequest = serde_json::from_value(event.clone())?;
                parsed.operation.validate()?;
                let operation = &event["operation"];
                let kind = operation["operation"].as_str().unwrap_or("");
                if !matches!(kind, "append" | "get")
                    || operation["namespace"].as_str() != Some(namespace)
                {
                    return Err(fail(
                        "wire worker only permits append/get in exact test namespace",
                    ));
                }
            } else if request.get("RangeWriteProgress").is_some() {
                let range = &request["RangeWriteProgress"];
                if range["range_id"].as_u64().is_none() || range["range_epoch"].as_u64().is_none() {
                    return Err(fail("invalid read-only range progress request"));
                }
            } else if request != json!("FetchBootstrapSnapshot")
                && request != json!("FetchTopology")
            {
                return Err(fail("wire worker rejects administrative/unsupported RPC"));
            }
            let connect = command["connect_timeout_ms"]
                .as_u64()
                .unwrap_or(config.transport.connect_timeout_ms);
            let io_ms = command["io_timeout_ms"]
                .as_u64()
                .unwrap_or(config.transport.io_timeout_ms);
            if connect == 0 || connect > 10000 || io_ms == 0 || io_ms > 30000 {
                return Err(fail("test RPC timeout outside bounded limits"));
            }
            let envelope = json!({"protocol_version":CLUSTER_DATA_PROTOCOL_VERSION,"cluster_id":config.cluster_id,"caller_node_id":config.node_id,"destination_node_id":destination,"request_id":request_id,"request":request});
            let bytes = serde_json::to_vec(&envelope)?;
            record["request_envelope"] = envelope;
            record["request_body_sha256"] = json!(hash(&bytes));
            record["transport_topology_generation"] = json!(topology.generation);
            record["logical_destination_address"] = json!(node.address);
            record["connect_timeout_ms"] = json!(connect);
            record["io_timeout_ms"] = json!(io_ms);
            stage = "resolve_address".into();
            let address = node
                .address
                .to_socket_addrs()?
                .next()
                .ok_or_else(|| fail("empty destination address"))?;
            record["resolved_destination_address"] = json!(address.to_string());
            if config.transport.dev_localhost_plaintext && !address.ip().is_loopback() {
                return Err(fail("plaintext restricted to loopback"));
            }
            stage = "tcp_connect".into();
            let before = Instant::now();
            let socket = TcpStream::connect_timeout(&address, Duration::from_millis(connect))?;
            record["tcp_connect_elapsed_ns"] = json!(before.elapsed().as_nanos());
            socket.set_read_timeout(Some(Duration::from_millis(io_ms)))?;
            socket.set_write_timeout(Some(Duration::from_millis(io_ms)))?;
            socket.set_nodelay(true)?;
            let body = if let Some(tls) = &tls {
                stage = "tls_handshake".into();
                let name = node
                    .address
                    .rsplit_once(':')
                    .ok_or_else(|| fail("invalid logical TLS address"))?
                    .0
                    .trim_matches(['[', ']']);
                let mut stream = client_tls_stream(Arc::clone(&tls.config), name, socket)?;
                let before = Instant::now();
                while stream.conn.is_handshaking() {
                    stream.conn.complete_io(&mut stream.sock)?;
                }
                let peer = stream
                    .conn
                    .peer_certificates()
                    .and_then(|v| v.first())
                    .ok_or_else(|| fail("missing server certificate"))?;
                let presented = hash(peer.as_ref());
                record["tls_peer_certificate_sha256"] = json!(presented);
                record["tls_handshake_elapsed_ns"] = json!(before.elapsed().as_nanos());
                if node
                    .tls_certificate_sha256
                    .as_deref()
                    .is_some_and(|active| {
                        active != presented
                            && node.pending_tls_certificate_sha256.as_deref() != Some(&presented)
                    })
                {
                    return Err(fail("destination TLS certificate not bound to membership"));
                }
                frame(
                    &mut stream,
                    &bytes,
                    config.transport.max_frame_bytes,
                    &mut stage,
                )?
            } else {
                let mut stream = socket;
                frame(
                    &mut stream,
                    &bytes,
                    config.transport.max_frame_bytes,
                    &mut stage,
                )?
            };
            record["response_body_sha256"] = json!(hash(&body));
            stage = "response_utf8".into();
            let raw = String::from_utf8(body)?;
            record["response_body_raw"] = json!(raw);
            record["completed_response_body_retained"] = json!(true);
            stage = "response_json".into();
            let response: Value = serde_json::from_str(&raw)?;
            record["response_envelope"] = response.clone();
            stage = "response_identity".into();
            if response["protocol_version"] != CLUSTER_DATA_PROTOCOL_VERSION
                || response["cluster_id"] != config.cluster_id.to_string()
                || response["source_node_id"] != destination
                || response["request_id"] != request_id
            {
                return Err(fail("response identity/protocol mismatch"));
            }
            if let Some(event) = response.pointer("/response/Ok/ApplicationEvent") {
                let _: bicdb_core::ClusterEventResponse = serde_json::from_value(event.clone())?;
            }
            if let Some(snapshot) = response.pointer("/response/Ok/BootstrapSnapshot") {
                stage = "discovery_decode".into();
                let snapshot: ClusterBootstrapSnapshot = serde_json::from_value(snapshot.clone())?;
                stage = "discovery_validate".into();
                snapshot.validate()?;
                record["discovery_topology_validated"] = json!(true);
            }
            if let Some(value) = response.pointer("/response/Ok/Topology") {
                stage = "discovery_decode".into();
                let t: ClusterTopology = serde_json::from_value(value.clone())?;
                stage = "discovery_validate".into();
                t.validate()?;
                if t.cluster_id != config.cluster_id {
                    return Err(fail("discovered topology cluster mismatch"));
                }
                record["discovery_topology_validated"] = json!(true);
            }
            stage = "complete".into();
            Ok(())
        })();
        record["completed_unix_ns"] = json!(ns());
        record["elapsed_ns"] = json!(clock.elapsed().as_nanos());
        record["last_stage"] = json!(stage);
        record["transport_success"] = json!(result.is_ok());
        if let Err(error) = result {
            record["error"] = error_json(error.as_ref());
        }
        writeln!(output, "{record}")?;
        output.flush()?;
    }
    Ok(())
}
