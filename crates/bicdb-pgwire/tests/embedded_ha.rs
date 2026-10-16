use std::{
    net::{TcpListener, TcpStream},
    path::Path,
    time::{Duration, Instant},
};

use bicdb_core::{DbConfig, NodeRole, ReplicationConfig, ReplicationTlsConfig, SyncTimeoutPolicy};
use bicdb_pgwire::{
    AllowedStandby, EmbeddedHaDatabase, FailoverConfig, HaPeer, HaServerConfig, HaServerRole,
    PgWireConfig,
};
use bicdb_sql::SqlSession;

fn port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn config(index: usize, ports: &[u16], failover: bool) -> PgWireConfig {
    let address = |index: usize| format!("127.0.0.1:{}", ports[index]);
    let id = |index: usize| format!("n{index}");
    let primary = HaServerRole::Primary {
        listen: address(index),
        allowed_standbys: (0..ports.len())
            .filter(|peer| *peer != index)
            .map(|peer| AllowedStandby {
                node_id: id(peer),
                certificate_sha256: None,
            })
            .collect(),
        synchronous_standbys: if ports.len() > 1 { 1 } else { 0 },
        synchronous_timeout: Duration::from_secs(3),
        on_timeout: SyncTimeoutPolicy::Fail,
    };
    let mut replication = ReplicationConfig {
        enabled: true,
        cluster_id: "embedded-test".into(),
        node_id: id(index),
        reconnect_backoff_ms: 100,
        ..Default::default()
    };
    replication.tls = Some(ReplicationTlsConfig {
        cert_path: Default::default(),
        key_path: Default::default(),
        ca_path: Default::default(),
        require_client_cert: false,
        dev_localhost_plaintext: true,
    });
    PgWireConfig {
        fsync: true,
        high_availability: Some(HaServerConfig {
            replication,
            role: if index == 0 {
                primary.clone()
            } else {
                HaServerRole::Standby {
                    primary: address(0),
                    primary_server_name: "localhost".into(),
                }
            },
            advertise_sql: Some(address(index)),
            failover: failover.then(|| FailoverConfig {
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
        }),
        ..Default::default()
    }
}
fn open(root: &Path, index: usize, ports: &[u16], failover: bool) -> EmbeddedHaDatabase {
    eprintln!(
        "opening n{index}, persisted={:?}",
        bicdb_core::read_persisted_ha_role(&root.join(format!("n{index}")))
    );
    let host = EmbeddedHaDatabase::open(
        root.join(format!("n{index}")),
        config(index, ports, failover),
        DbConfig::default(),
    )
    .unwrap();
    eprintln!(
        "opened n{index}, status={:?}",
        host.database().read().ha_node_status()
    );
    host
}
#[track_caller]
fn wait(mut condition: impl FnMut() -> bool) {
    let started = Instant::now();
    while !condition() {
        assert!(
            started.elapsed() < Duration::from_secs(25),
            "embedded HA condition timed out"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn shutdown_cancels_incomplete_peers_and_releases_the_replication_listener() {
    use std::io::{Read, Write};
    let root = tempfile::tempdir().unwrap();
    let ports = [port()];
    let host = open(root.path(), 0, &ports, false);
    let mut peer = TcpStream::connect(("127.0.0.1", ports[0])).unwrap();
    peer.write_all(&[0, 0]).unwrap(); // An incomplete replication frame.
    std::thread::sleep(Duration::from_millis(100));
    let lease = host.writer_lease();
    {
        let engine = host.database();
        let _busy = engine.write();
        host.request_shutdown();
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn({
            let lease = lease.clone();
            move || {
                send.send(lease.active()).unwrap();
            }
        });
        assert!(!receive
            .recv_timeout(Duration::from_secs(2))
            .expect("lease probe must not wait for the database lock"));
    }
    host.shutdown(Duration::from_secs(2)).unwrap();
    let listener = TcpListener::bind(("127.0.0.1", ports[0])).unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    assert!(peer.read(&mut [0; 1]).is_ok_and(|count| count == 0));
    drop(listener);
    drop(host);
    assert!(
        !lease.active(),
        "a lease probe must not keep the database alive"
    );
    let host = open(root.path(), 0, &ports, false);
    host.with_write(|db| {
        Ok(SqlSession::new(db).execute("CREATE TABLE reopened (id INT PRIMARY KEY)")?)
    })
    .unwrap();
}

#[test]
fn writer_probe_observes_direct_demotion_and_a_persisted_standby() {
    let root = tempfile::tempdir().unwrap();
    let ports = [port()];
    let host = open(root.path(), 0, &ports, false);
    let lease = host.writer_lease();
    assert!(lease.active());
    host.database().read().demote_to_standby().unwrap();
    assert!(!lease.active());
    drop(host);
    // The initial configuration must not override the persisted engine role
    // in an authority probe, even without automatic elections configured.
    let host = open(root.path(), 0, &ports, false);
    assert!(!host.writer_lease().active());
}

#[test]
fn native_handshake_refuses_old_versions_and_mismatched_resume_identity() {
    use bicdb_core::{
        replication::ReplicationFrame,
        replication_transport::{recv_frame, send_frame},
    };
    let root = tempfile::tempdir().unwrap();
    let ports = [port(), port()];
    let _host = open(root.path(), 0, &ports, false);
    let hello = |version| ReplicationFrame::Hello {
        protocol_version: version,
        cluster_id: "embedded-test".into(),
        node_id: "n1".into(),
        epoch: 0,
    };
    let connect = || {
        let stream = TcpStream::connect(("127.0.0.1", ports[0])).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
    };
    let mut peer = connect();
    send_frame(&mut peer, &hello(1), 65536).unwrap();
    assert!(
        matches!(recv_frame(&mut peer, 65536).unwrap(), ReplicationFrame::Error { message, .. } if message.contains("unsupported replication protocol"))
    );
    drop(peer);
    let mut peer = connect();
    send_frame(
        &mut peer,
        &hello(bicdb_pgwire::HA_REPLICATION_PROTOCOL_VERSION),
        65536,
    )
    .unwrap();
    send_frame(
        &mut peer,
        &ReplicationFrame::Resume {
            cluster_id: "embedded-test".into(),
            node_id: "other-node".into(),
            commit_seq: 0,
            data_epoch: 0,
            journal_identity: None,
        },
        65536,
    )
    .unwrap();
    assert!(
        matches!(recv_frame(&mut peer, 65536).unwrap(), ReplicationFrame::Error { message, .. } if message.contains("resume identity"))
    );
}

#[test]
fn embedded_quorum_survives_primary_loss_and_fences_a_returning_writer() {
    let root = tempfile::tempdir().unwrap();
    let ports = [port(), port(), port()];
    let mut hosts: Vec<_> = (0..3)
        .map(|index| Some(open(root.path(), index, &ports, true)))
        .collect();
    wait(|| hosts[0].as_ref().unwrap().writer_lease().active());
    hosts[0].as_ref().unwrap().with_write(|db| {
        SqlSession::new(db).execute("CREATE TABLE effects (id INT PRIMARY KEY, value INT); INSERT INTO effects VALUES (1,7)")?;
        Ok(())
    }).unwrap();
    wait(|| {
        (1..3).all(|index| {
            let data = hosts[index].as_ref().unwrap().database();
            let mut db = data.write();
            SqlSession::new(&mut db)
                .execute("SELECT value FROM effects")
                .is_ok_and(|result| result.rows.len() == 1)
        })
    });
    hosts[0]
        .take()
        .unwrap()
        .shutdown(Duration::from_secs(3))
        .unwrap();
    let mut primary = None;
    wait(|| {
        for (index, host) in hosts.iter_mut().enumerate().skip(1) {
            if host.as_ref().unwrap().restart_required() {
                host.take();
                *host = Some(open(root.path(), index, &ports, true));
            }
        }
        primary = (1..3).find(|index| {
            let candidate = hosts[*index].as_ref().unwrap();
            let status = candidate.database().read().ha_node_status();
            candidate.writer_lease().active()
                && (1..3).filter(|peer| peer != index).all(|peer| {
                    let follower = hosts[peer]
                        .as_ref()
                        .unwrap()
                        .database()
                        .read()
                        .ha_node_status();
                    follower.primary_id == Some(format!("n{index}"))
                        && follower.data_epoch == status.epoch
                        && follower.epoch == status.epoch
                })
        });
        primary.is_some()
    });
    let primary = primary.unwrap();
    let primary_epoch = hosts[primary]
        .as_ref()
        .unwrap()
        .database()
        .read()
        .ha_node_status()
        .epoch;
    eprintln!("new primary: n{primary}");
    hosts[primary]
        .as_ref()
        .unwrap()
        .with_write(|db| {
            SqlSession::new(db).execute("UPDATE effects SET value = 8 WHERE id = 1")?;
            Ok(())
        })
        .unwrap();
    eprintln!(
        "new primary after write: {:?}",
        hosts[primary]
            .as_ref()
            .unwrap()
            .database()
            .read()
            .ha_node_status()
    );
    hosts[0] = Some(open(root.path(), 0, &ports, true));
    wait(|| {
        if hosts[0].as_ref().unwrap().restart_required() {
            hosts[0].take();
            hosts[0] = Some(open(root.path(), 0, &ports, true));
        }
        let data = hosts[0].as_ref().unwrap().database();
        let mut db = data.write();
        db.replication_role() == NodeRole::Standby
            && SqlSession::new(&mut db)
                .execute("SELECT value FROM effects")
                .is_ok_and(|result| {
                    serde_json::to_value(result.rows).unwrap() == serde_json::json!([[8]])
                })
    });
    // Snapshot creation can temporarily outlast this test's short write lease.
    // Wait for live peer confirmations before submitting the next mutation.
    wait(|| hosts[primary].as_ref().unwrap().writer_lease().active());
    hosts[primary]
        .as_ref()
        .unwrap()
        .with_write(|db| {
            SqlSession::new(db).execute("UPDATE effects SET value = 9 WHERE id = 1")?;
            Ok(())
        })
        .unwrap();
    wait(|| {
        if hosts[0].as_ref().unwrap().restart_required() {
            hosts[0].take();
            hosts[0] = Some(open(root.path(), 0, &ports, true));
        }
        let data = hosts[0].as_ref().unwrap().database();
        let mut db = data.write();
        db.ha_node_status().data_epoch == primary_epoch
            && SqlSession::new(&mut db)
                .execute("SELECT value FROM effects")
                .is_ok_and(|result| {
                    serde_json::to_value(result.rows).unwrap() == serde_json::json!([[9]])
                })
    });
    assert!(hosts[0]
        .as_ref()
        .unwrap()
        .with_write(|db| {
            SqlSession::new(db).execute("UPDATE effects SET value = 99")?;
            Ok(())
        })
        .is_err());
    for host in hosts.iter().flatten() {
        host.request_shutdown();
    }
}

#[test]
fn journal_replicas_share_author_identity_and_preserve_it_after_promotion() {
    let root = tempfile::tempdir().unwrap();
    let ports = [port(), port(), port()];
    let open_journal = |index| {
        let mut configuration = config(index, &ports, true);
        configuration
            .high_availability
            .as_mut()
            .unwrap()
            .replication
            .record_journal = true;
        EmbeddedHaDatabase::open(
            root.path().join(format!("journal-n{index}")),
            configuration,
            DbConfig::default().with_audit_events(true),
        )
        .unwrap()
    };
    let mut hosts: Vec<_> = (0..3).map(|index| Some(open_journal(index))).collect();
    let identity = {
        let db = hosts[0].as_ref().unwrap().database();
        let db = db.read();
        (db.node_id(), db.mesh_verifying_key().unwrap())
    };
    wait(|| {
        for (index, host) in hosts.iter_mut().enumerate() {
            if host.as_ref().unwrap().restart_required() {
                host.take();
                *host = Some(open_journal(index));
            }
        }
        hosts.iter().flatten().all(|host| {
            let db = host.database();
            let db = db.read();
            (db.node_id(), db.mesh_verifying_key().unwrap()) == identity
        }) && hosts[0].as_ref().unwrap().writer_lease().active()
    });
    hosts[0]
        .as_ref()
        .unwrap()
        .with_write(|db| {
            SqlSession::new(db).execute("CREATE TABLE entries (id TEXT PRIMARY KEY, value INT)")?;
            db.set_collection_mesh_sync_enabled("entries", true)?;
            SqlSession::new(db).execute("INSERT INTO entries VALUES ('before', 1)")?;
            Ok(())
        })
        .unwrap();
    let original = hosts[0]
        .as_ref()
        .unwrap()
        .database()
        .write()
        .export_sync_bundle_delta(&Default::default())
        .unwrap();
    assert_eq!(original.event_count, 1);
    let original_envelope = original.events[0].envelope.clone();
    wait(|| {
        hosts.iter().flatten().all(|host| {
            host.database()
                .write()
                .export_sync_bundle_delta(&Default::default())
                .is_ok_and(|bundle| {
                    bundle
                        .events
                        .first()
                        .is_some_and(|event| event.envelope == original_envelope)
                })
        })
    });
    hosts[0]
        .take()
        .unwrap()
        .shutdown(Duration::from_secs(3))
        .unwrap();
    let mut primary = None;
    wait(|| {
        for (index, host) in hosts.iter_mut().enumerate().skip(1) {
            if host.as_ref().unwrap().restart_required() {
                host.take();
                *host = Some(open_journal(index));
            }
        }
        primary = (1..3).find(|index| hosts[*index].as_ref().unwrap().writer_lease().active());
        primary.is_some()
    });
    let primary = hosts[primary.unwrap()].as_ref().unwrap();
    primary
        .with_write(|db| {
            assert_eq!((db.node_id(), db.mesh_verifying_key().unwrap()), identity);
            SqlSession::new(db).execute("INSERT INTO entries VALUES ('after', 2)")?;
            Ok(())
        })
        .unwrap();
    let bundle = primary
        .database()
        .write()
        .export_sync_bundle_delta(&Default::default())
        .unwrap();
    assert_eq!(bundle.event_count, 2);
    assert!(bundle
        .events
        .iter()
        .any(|event| event.envelope == original_envelope));
    let later = bundle
        .events
        .iter()
        .find(|event| event.envelope.event_id != original_envelope.event_id)
        .unwrap();
    assert_eq!(later.envelope.node_id, original_envelope.node_id);
    assert!(later.envelope.sequence > original_envelope.sequence);
    // A peer pinned before the election accepts both the old and new writer.
    let mut edge = bicdb_core::BicDb::open(root.path().join("edge")).unwrap();
    edge.create_collection("entries").unwrap();
    edge.set_collection_mesh_sync_enabled("entries", true)
        .unwrap();
    edge.pin_node_key(&identity.0, &identity.1).unwrap();
    edge.import_sync_bundle_strict(bundle).unwrap();
    assert_eq!(edge.scan_collection("entries").unwrap().len(), 2);
    for host in hosts.iter().flatten() {
        host.request_shutdown();
    }
}
