use bicdb_app_host::manifest::Release;
use bicdb_core::{
    BicDb, DbConfig, DistributionConfig, DistributionStore, StorageMode, CLUSTER_EVENTS_COLLECTION,
};
use bicdb_pgwire::{BalancedDistributionHostService, PgWireConfig, PgWireServer};
use serde_json::{json, Value};
use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

struct Running(Child);
impl Drop for Running {
    fn drop(&mut self) {
        if self.0.try_wait().is_ok_and(|status| status.is_some()) {
            return;
        }
        // Let the worker remove its jailed VM and network allocation before
        // the temporary state directory goes away. SIGKILL skips that cleanup.
        unsafe {
            libc::kill(self.0.id() as i32, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if self.0.try_wait().is_ok_and(|status| status.is_some()) {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn deployed_notebook_uses_scoped_cluster_events_and_rejects_cross_application_access() {
    event_scenario(None);
}

#[test]
#[ignore = "requires Linux KVM, root and BICDB_TEST_MICROVM_CONFIG with pinned images"]
fn a_remote_microvm_uses_only_its_scoped_event_binding() {
    event_scenario(Some(
        std::env::var_os("BICDB_TEST_MICROVM_CONFIG")
            .expect("microVM configuration required")
            .into(),
    ));
}

fn event_scenario(microvm_config: Option<std::path::PathBuf>) {
    let cluster = tempfile::tempdir().unwrap();
    let platform = tempfile::tempdir().unwrap();
    let worker_root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(
            cluster.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        )
        .unwrap();
        db.create_collection(CLUSTER_EVENTS_COLLECTION).unwrap();
        db.close().unwrap();
    }
    DistributionStore::initialize_at(
        cluster.path(),
        DistributionConfig {
            enabled: true,
            replication_factor: 1,
            initial_ranges: 4,
            node_address: format!("127.0.0.1:{}", port()),
            metadata_election_timeout_ms: 300,
            metadata_heartbeat_interval_ms: 50,
            ..Default::default()
        },
        false,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    )
    .unwrap();
    let server = PgWireServer::open(
        cluster.path(),
        PgWireConfig {
            fsync: false,
            ..Default::default()
        },
    )
    .unwrap();
    server
        .install_host_service(Arc::new(BalancedDistributionHostService::default()))
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let serving = {
        let server = server.clone();
        std::thread::spawn(move || bicdb_pgwire::serve_existing_listener(server, listener).unwrap())
    };
    let config = platform.path().join("events.json");
    bicdb_app_host::store::write_synced(&config, &serde_json::to_vec(&json!({"format":1,"member_root":cluster.path(),"applications":{"notebook":"first-app","another":"second-app"}})).unwrap()).unwrap();
    let listen = port();
    let base = format!("http://127.0.0.1:{listen}");
    let worker_port = port();
    let worker_token = worker_root.path().join("management.token");
    let microvm_config = microvm_config.map(|path| {
        let mut config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let allocation = config["allocations"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .clone();
        config["allocations"] = json!({"another": allocation});
        let path = worker_root.path().join("microvm.json");
        bicdb_app_host::store::write_synced(&path, &serde_json::to_vec(&config).unwrap()).unwrap();
        path
    });
    let vm_args = microvm_config
        .as_ref()
        .map(|path| {
            vec![
                std::ffi::OsString::from("--microvm-config"),
                path.as_os_str().to_owned(),
            ]
        })
        .unwrap_or_default();
    bicdb_app_host::store::write_synced(
        &worker_token,
        b"event-worker-management-token-for-test-00000000",
    )
    .unwrap();
    let worker = Running(
        Command::new(env!("CARGO_BIN_EXE_bic"))
            .arg("--data")
            .arg(worker_root.path())
            .args([
                "worker",
                "--listen",
                &format!("127.0.0.1:{worker_port}"),
                "--state-endpoint",
                &base,
            ])
            .args(vm_args)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let fleet = platform.path().join("fleet.json");
    bicdb_app_host::store::write_synced(
        &fleet,
        &serde_json::to_vec(&json!({"format":1,"workers":{"worker":{
            "endpoint":format!("http://127.0.0.1:{worker_port}"),"token_file":worker_token,
        "region":"test","capacity":2,"isolation":if microvm_config.is_some() {"microvm"} else {"process"}
        }}}))
        .unwrap(),
    )
    .unwrap();
    let service = Running(
        Command::new(env!("CARGO_BIN_EXE_bic"))
            .arg("--data")
            .arg(platform.path())
            .args([
                "serve",
                "--listen",
                &format!("127.0.0.1:{listen}"),
                "--events-config",
            ])
            .arg(config)
            .arg("--fleet-config")
            .arg(fleet)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let http = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(25);
    while !http
        .get(format!("{base}/health"))
        .send()
        .is_ok_and(|r| r.status().is_success())
    {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(50));
    }
    let management = std::fs::read_to_string(platform.path().join("management.token")).unwrap();
    let mut release = Release::read(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/platform/notebook/bic.json"),
    )
    .unwrap();
    for name in ["notebook", "another"] {
        release.manifest.name = name.into();
        release.manifest.isolation = if name == "another" && microvm_config.is_some() {
            release.manifest.resources.memory_mb = 256;
            bicdb_app_host::manifest::Isolation::Microvm
        } else {
            bicdb_app_host::manifest::Isolation::Process
        };
        release.manifest.placement =
            (name == "another").then(|| bicdb_app_host::manifest::Placement {
                region: "test".into(),
            });
        let response = http
            .post(format!("{base}/v1/apps"))
            .bearer_auth(management.trim())
            .json(&release)
            .send()
            .unwrap();
        let status = response.status();
        let body = response.text().unwrap();
        let runtime_log = if status.is_success() {
            String::new()
        } else {
            bicdb_app_host::runtime::read_log(
                &worker_root
                    .path()
                    .join("apps")
                    .join(name)
                    .join("application.log"),
            )
            .unwrap_or_else(|error| format!("runtime log unavailable: {error}"))
        };
        assert!(status.is_success(), "{body}\n{runtime_log}");
    }
    release.manifest.name = "unallocated".into();
    assert!(!http
        .post(format!("{base}/v1/apps"))
        .bearer_auth(management.trim())
        .json(&release)
        .send()
        .unwrap()
        .status()
        .is_success());
    // A management token cannot be reused as a runtime SQL/event capability.
    assert_eq!(
        http.post(format!("{base}/internal/notebook/events"))
            .bearer_auth(management.trim())
            .json(&json!({"operation":"list","cursor":null,"limit":10}))
            .send()
            .unwrap()
            .status(),
        401
    );
    let room = "a".repeat(43);
    let html = http
        .get(format!("{base}/apps/notebook?view=notes"))
        .send()
        .unwrap();
    assert_eq!(html.url().path(), "/apps/notebook/");
    assert_eq!(html.url().query(), Some("view=notes"));
    assert!(html.text().unwrap().contains("Field Notebook"));
    for asset in ["browser.mjs", "client.mjs", "app.css", "sw.js"] {
        assert!(http
            .get(format!("{base}/apps/notebook/{asset}"))
            .send()
            .unwrap()
            .status()
            .is_success());
    }
    let note = json!({"id":"10000000-0000-4000-8000-000000000001","text":"Saved in the cluster","author":"Pat","created":42});
    let append = |note: &Value| {
        http.post(format!("{base}/apps/notebook/api/notes"))
            .bearer_auth(&room)
            .json(note)
            .send()
            .unwrap()
    };
    assert_eq!(
        append(&note)
            .error_for_status()
            .unwrap()
            .json::<Value>()
            .unwrap()["inserted"],
        true
    );
    assert_eq!(
        append(&note)
            .error_for_status()
            .unwrap()
            .json::<Value>()
            .unwrap()["inserted"],
        false
    );
    let mut changed = note.clone();
    changed["text"] = json!("changed");
    assert_eq!(append(&changed).status(), 409);
    let list = |app: &str, token: &str| {
        let mut cursor = Value::Null;
        let mut notes = Vec::new();
        for _ in 0..100 {
            let page: Value = http
                .post(format!("{base}/apps/{app}/api/page"))
                .bearer_auth(token)
                .json(&json!({"cursor":cursor}))
                .send()
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .unwrap();
            notes.extend(page["notes"].as_array().unwrap().iter().cloned());
            cursor = page["next"].clone();
            if cursor.is_null() {
                return notes;
            }
        }
        panic!("cursor did not terminate");
    };
    assert_eq!(list("notebook", &room), vec![note]);
    assert!(list("another", &room).is_empty());
    let remote_note = json!({"id":"10000000-0000-4000-8000-000000000002","text":"Saved by remote compute","author":"Lee","created":43});
    assert!(http
        .post(format!("{base}/apps/another/api/notes"))
        .bearer_auth(&room)
        .json(&remote_note)
        .send()
        .unwrap()
        .status()
        .is_success());
    assert_eq!(list("another", &room), vec![remote_note]);
    assert_eq!(list("notebook", &room).len(), 1);
    assert!(list("notebook", &"b".repeat(43)).is_empty());
    assert_eq!(
        http.post(format!("{base}/apps/notebook/api/page"))
            .json(&json!({}))
            .send()
            .unwrap()
            .status(),
        401
    );
    drop(service);
    drop(worker);
    server.request_shutdown();
    serving.join().unwrap();
}
