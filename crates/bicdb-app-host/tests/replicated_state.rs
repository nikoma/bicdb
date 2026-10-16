use std::{
    collections::BTreeMap,
    net::TcpListener,
    path::Path,
    time::{Duration, Instant},
};

use base64::Engine;
use bicdb_app_host::{
    database::{Database, Query, QueryBatch},
    manifest::{Manifest, Release, Resources, Runtime},
};
use bicdb_core::{BicDb, ReplicationConfig, ReplicationTlsConfig, SyncTimeoutPolicy};
use bicdb_pgwire::{AllowedStandby, FailoverConfig, HaPeer, HaServerConfig, HaServerRole};
use bicdb_sql::SqlSession;
use serde_json::json;

fn ports(count: usize) -> Vec<u16> {
    let listeners: Vec<_> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect()
}

fn config(index: usize, ports: &[u16]) -> HaServerConfig {
    let address = |peer: usize| format!("127.0.0.1:{}", ports[peer]);
    let id = |peer: usize| format!("n{peer}");
    let primary = HaServerRole::Primary {
        listen: address(index),
        allowed_standbys: (0..ports.len())
            .filter(|peer| *peer != index)
            .map(|peer| AllowedStandby {
                node_id: id(peer),
                certificate_sha256: None,
            })
            .collect(),
        synchronous_standbys: usize::from(ports.len() > 1),
        synchronous_timeout: Duration::from_secs(3),
        on_timeout: SyncTimeoutPolicy::Fail,
    };
    HaServerConfig {
        replication: ReplicationConfig {
            enabled: true,
            cluster_id: "application-state-test".into(),
            node_id: id(index),
            reconnect_backoff_ms: 100,
            tls: Some(ReplicationTlsConfig {
                cert_path: Default::default(),
                key_path: Default::default(),
                ca_path: Default::default(),
                require_client_cert: false,
                dev_localhost_plaintext: true,
            }),
            ..Default::default()
        },
        role: if index == 0 {
            primary.clone()
        } else {
            HaServerRole::Standby {
                primary: address(0),
                primary_server_name: "localhost".into(),
            }
        },
        advertise_sql: Some(address(index)),
        failover: (ports.len() > 1).then(|| FailoverConfig {
            peers: (0..ports.len())
                .filter(|peer| *peer != index)
                .map(|peer| HaPeer {
                    node_id: id(peer),
                    address: address(peer),
                })
                .collect(),
            timeout: Duration::from_secs(3),
            as_primary: primary,
            peer_server_name: "localhost".into(),
        }),
    }
}

fn open(root: &Path, index: usize, ports: &[u16]) -> Database {
    Database::open_replicated(&root.join(format!("n{index}")), config(index, ports)).unwrap()
}

#[track_caller]
fn wait(mut ready: impl FnMut() -> bool) {
    let started = Instant::now();
    while !ready() {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "state replica timed out"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn package() -> Release {
    Release {
        manifest: Manifest {
            format: 1,
            name: "counter".into(),
            version: "1".into(),
            runtime: Runtime::Node,
            isolation: Default::default(),
            placement: None,
            entrypoint: "app.mjs".into(),
            files: vec!["app.mjs".into(), "001.sql".into()],
            migrations: vec!["001.sql".into()],
            resources: Resources::default(),
            sync: BTreeMap::new(),
        },
        files: BTreeMap::from([
            ("app.mjs".into(), base64::engine::general_purpose::STANDARD.encode("export default {}")),
            ("001.sql".into(), base64::engine::general_purpose::STANDARD.encode(
                "CREATE TABLE effects (id INT PRIMARY KEY, value INT); INSERT INTO effects VALUES (1,0)"
            )),
        ]),
    }
}

fn batch(sql: &str, key: Option<&str>) -> QueryBatch {
    QueryBatch {
        statements: vec![Query {
            sql: sql.into(),
            params: vec![],
        }],
        idempotency_key: key.map(str::to_owned),
        jobs: vec![],
    }
}

#[test]
fn native_failover_preserves_migrations_outbox_job_leases_and_effect_receipts() {
    let root = tempfile::tempdir().unwrap();
    let ports = ports(3);
    let package = package();
    // Exercise an existing standalone application's upgrade, not just an empty
    // native database. Its historical migration is preserved during bootstrap.
    {
        let mut initial = Database::open(&root.path().join("n0")).unwrap();
        initial.prepare(&package).unwrap();
        initial
            .query(&batch("UPDATE effects SET value = 40", Some("pre-upgrade")))
            .unwrap();
    }
    let mut hosts: Vec<_> = (0..3)
        .map(|index| Some(open(root.path(), index, &ports)))
        .collect();
    wait(|| {
        for (index, host) in hosts.iter_mut().enumerate() {
            if host.as_ref().unwrap().restart_required() {
                host.take();
                *host = Some(open(root.path(), index, &ports));
            }
        }
        hosts[0].as_ref().unwrap().writable()
            && (1..3).all(|index| {
                SqlSession::new(&mut hosts[index].as_ref().unwrap().db.write())
                    .execute("SELECT value FROM effects")
                    .is_ok_and(|result| serde_json::to_value(result.rows).unwrap() == json!([[40]]))
            })
    });
    let request = {
        let mut request = batch(
            "UPDATE effects SET value = value + 1 RETURNING value",
            Some("request-1"),
        );
        request.jobs.push(json!({"work":"increment"}));
        request
    };
    let primary = hosts[0].as_mut().unwrap();
    primary.prepare(&package).unwrap();
    let first_result = primary.query(&request).unwrap();
    assert_eq!(
        serde_json::to_value(&first_result[0].rows).unwrap(),
        json!([[41]])
    );
    assert_eq!(primary.drain_outbox(32).unwrap(), 1);
    let first_delivery = primary.consume("worker", 100).unwrap().unwrap();
    let effect = batch(
        "UPDATE effects SET value = value + 1 RETURNING value",
        Some(&format!("job:{}", first_delivery.message_id)),
    );
    let effect_result = primary.query(&effect).unwrap();
    // Crash between effect commit and job settlement. Make expiry deterministic
    // without waiting for a production visibility timeout.
    SqlSession::new(&mut primary.db.write())
        .execute("UPDATE __bic_platform_jobs SET lease_until = 0 WHERE status = 'running'")
        .unwrap();
    primary
        .query(&batch("SELECT value FROM effects", None))
        .unwrap(); // quorum barrier
    wait(|| {
        (1..3).all(|index| {
            SqlSession::new(&mut hosts[index].as_ref().unwrap().db.write())
                .execute("SELECT value FROM effects")
                .is_ok_and(|result| serde_json::to_value(result.rows).unwrap() == json!([[42]]))
        })
    });
    let old_lease = hosts[0].as_ref().unwrap().writer_lease().unwrap();
    hosts[0].as_ref().unwrap().request_shutdown();
    hosts[0].take();
    assert!(!old_lease.active());
    let mut elected = None;
    wait(|| {
        for (index, host) in hosts.iter_mut().enumerate().skip(1) {
            if host.as_ref().unwrap().restart_required() {
                host.take();
                *host = Some(open(root.path(), index, &ports));
            }
        }
        elected = (1..3).find(|index| {
            let candidate = hosts[*index].as_ref().unwrap();
            let status = candidate.db.read().ha_node_status();
            candidate.writable()
                && (1..3).filter(|peer| peer != index).all(|peer| {
                    let follower = hosts[peer].as_ref().unwrap().db.read().ha_node_status();
                    follower.primary_id == Some(format!("n{index}"))
                        && follower.data_epoch == status.epoch
                        && follower.epoch == status.epoch
                })
        });
        elected.is_some()
    });
    let primary = hosts[elected.unwrap()].as_mut().unwrap();
    primary.prepare(&package).unwrap();
    assert_eq!(primary.query(&request).unwrap(), first_result);
    assert_eq!(primary.drain_outbox(32).unwrap(), 0);
    let second_delivery = primary.consume("worker", 1000).unwrap().unwrap();
    assert_eq!(second_delivery.message_id, first_delivery.message_id);
    assert_eq!(second_delivery.attempts, 2);
    assert!(primary.settle("worker", &first_delivery, true).is_err());
    assert_eq!(primary.query(&effect).unwrap(), effect_result);
    primary.settle("worker", &second_delivery, true).unwrap();
    assert!(primary.consume("worker", 1000).unwrap().is_none());
    assert_eq!(
        serde_json::to_value(
            &primary
                .query(&batch("SELECT value FROM effects", None))
                .unwrap()[0]
                .rows
        )
        .unwrap(),
        json!([[42]])
    );
    for host in hosts.iter().flatten() {
        host.request_shutdown();
    }
    drop(hosts);
    assert!(
        Database::open(&root.path().join("n0")).is_err(),
        "standalone startup cannot bypass a replica binding"
    );
    let mut changed_identity = config(0, &ports);
    changed_identity.replication.node_id = "different-node".into();
    assert!(Database::open_replicated(&root.path().join("n0"), changed_identity).is_err());
}

#[test]
fn unmigrated_legacy_jobs_are_rejected_before_any_state_listener_starts() {
    let root = tempfile::tempdir().unwrap();
    let ports = ports(1);
    {
        let mut legacy = BicDb::open(root.path().join("jobs")).unwrap();
        legacy
            .broker()
            .publish(
                bicdb_app_host::database::JOB_QUEUE,
                json!({"work":1}),
                json!({}),
            )
            .unwrap();
    }
    assert!(Database::open_replicated(root.path(), config(0, &ports)).is_err());
    let listener = TcpListener::bind(("127.0.0.1", ports[0])).unwrap();
    drop(listener);
    assert!(!root.path().join("state-replica.json").exists());
    // A failed preflight leaves the historical standalone upgrade available.
    let mut database = Database::open(root.path()).unwrap();
    assert_eq!(database.job_status().unwrap()["queued"], 1);
    drop(database);
    let mut database = Database::open_replicated(root.path(), config(0, &ports)).unwrap();
    database.prepare(&package()).unwrap();
    assert_eq!(
        database.consume("worker", 1000).unwrap().unwrap().payload,
        json!({"work":1})
    );
    let mut sync_package = package();
    sync_package.manifest.sync.insert(
        "effects".into(),
        bicdb_app_host::manifest::TableSync::AppendOnly,
    );
    assert!(database.prepare(&sync_package).is_err());
    assert!(database.migrate(&sync_package).is_err());
    assert!(database
        .configure_sync(&sync_package.manifest.sync)
        .is_err());
}
