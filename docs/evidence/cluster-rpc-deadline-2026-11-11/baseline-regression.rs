//! Actual loopback transport regression; no database replication or HA claim.
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bicdb_core::{
    ClusterId, ClusterNetworkTransportConfig, ClusterNodeId, DistributionConfig, DistributionStore,
    TcpClusterRelocationTransport,
};
use parking_lot::RwLock;
use serde_json::{json, Value};

#[test]
fn slow_progress_does_not_extend_a_cluster_rpc_budget() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let network = ClusterNetworkTransportConfig {
        dev_localhost_plaintext: true,
        connect_timeout_ms: 100,
        io_timeout_ms: 80,
        ..Default::default()
    };
    let node = ClusterNodeId::new("n0").unwrap();
    let config = DistributionConfig {
        enabled: true,
        cluster_id: ClusterId::new("rpc-deadline-regression").unwrap(),
        node_id: node.clone(),
        node_address: listener.local_addr().unwrap().to_string(),
        initial_ranges: 1,
        replication_factor: 1,
        transport: network.clone(),
        ..Default::default()
    };
    let store = DistributionStore::initialize_at(root.path(), config.clone(), false, 1).unwrap();
    let topology = Arc::new(RwLock::new(store.topology().clone()));
    let transport =
        TcpClusterRelocationTransport::new(config.cluster_id, node.clone(), topology, network)
            .unwrap();
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut header = [0; 4];
        stream.read_exact(&mut header).unwrap();
        let mut bytes = vec![0; u32::from_be_bytes(header) as usize];
        stream.read_exact(&mut bytes).unwrap();
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        let response = serde_json::to_vec(&json!({
            "protocol_version": request["protocol_version"],
            "cluster_id": request["cluster_id"],
            "source_node_id": request["destination_node_id"],
            "request_id": request["request_id"],
            "response": {"Ok": "Pong"}
        }))
        .unwrap();
        let mut frame = (response.len() as u32).to_be_bytes().to_vec();
        frame.extend(response);
        for chunk in frame.chunks(8) {
            if stream.write_all(chunk).is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });
    let started = Instant::now();
    let result = transport.ping(&node);
    let elapsed = started.elapsed();
    peer.join().unwrap();
    println!(
        "{}",
        json!({"result":format!("{result:?}"),"elapsed_ms":elapsed.as_secs_f64()*1000.0,"io_budget_ms":80,"peer_chunk_interval_ms":20,"physical_ha_qualification":false})
    );
    assert!(
        result.is_err(),
        "a valid but slow-progress response exceeded the whole-request budget: {result:?}"
    );
    assert!(elapsed < Duration::from_secs(2));
}
