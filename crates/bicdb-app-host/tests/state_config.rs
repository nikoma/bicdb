use bicdb_app_host::state::Group;
use serde_json::json;

fn group() -> Group {
    serde_json::from_value(json!({
        "cluster_id":"test", "node_id":"n1", "initial_primary":"n1", "dev_localhost_plaintext":true,
        "nodes":{
            "n1":{"replication":"127.0.0.1:5941","endpoint":"http://127.0.0.1:8788"},
            "n2":{"replication":"127.0.0.1:5942","endpoint":"http://127.0.0.1:8789"},
            "n3":{"replication":"127.0.0.1:5943","endpoint":"http://127.0.0.1:8790"}
        }
    }))
    .unwrap()
}

#[test]
fn state_configuration_requires_a_unique_majority_and_pinned_transport() {
    let valid = group();
    valid.validate().unwrap();
    let config = valid.replication().unwrap();
    assert!(matches!(
        config.role,
        bicdb_pgwire::HaServerRole::Primary {
            synchronous_standbys: 1,
            on_timeout: bicdb_core::SyncTimeoutPolicy::Fail,
            ..
        }
    ));
    let mut missing = valid.clone();
    missing.node_id = "n4".into();
    assert!(missing.replication().is_err());
    let mut even = valid.clone();
    even.nodes.remove("n3");
    assert!(even.validate().is_err());
    let mut duplicate = valid.clone();
    duplicate.nodes.get_mut("n3").unwrap().replication = valid.nodes["n2"].replication;
    assert!(duplicate.validate().is_err());
    let mut remote = valid.clone();
    remote.nodes.get_mut("n3").unwrap().replication = "10.0.0.3:5943".parse().unwrap();
    assert!(remote.validate().is_err());
    let mut http = valid.clone();
    http.nodes.get_mut("n3").unwrap().endpoint = "http://remote.example".into();
    assert!(http.validate().is_err());
    let mut unpinned = valid;
    unpinned.dev_localhost_plaintext = false;
    unpinned.tls=Some(serde_json::from_value(json!({"certificate":"node.crt","key":"node.key","ca":"ca.crt","server_name":"state.internal"})).unwrap());
    assert!(unpinned.validate().is_err());
    for node in unpinned.nodes.values_mut() {
        node.certificate_sha256 = Some("a".repeat(64));
    }
    unpinned.validate().unwrap();
}
