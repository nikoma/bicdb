//! Real Linux/KVM acceptance. This test requires explicit operator assets and
//! privilege; it never substitutes a process or a simulated VM.
use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use base64::Engine;
use bicdb_app_host::manifest::{Isolation, Release};
use serde_json::{json, Value};

struct Canary {
    port: u16,
    stopping: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Canary {
    fn start() -> Self {
        let listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let stopping = Arc::new(AtomicBool::new(false));
        let stop = stopping.clone();
        let thread = std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                if let Ok((mut stream, _)) = listener.accept() {
                    stream
                        .set_read_timeout(Some(Duration::from_millis(100)))
                        .unwrap();
                    let _ = stream.read(&mut [0u8; 4096]);
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    );
                } else {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        });
        Self {
            port,
            stopping,
            thread: Some(thread),
        }
    }
}
impl Drop for Canary {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        let _ = self.thread.take().unwrap().join();
    }
}

struct Service {
    child: Child,
    root: tempfile::TempDir,
    port: u16,
    config: PathBuf,
    client: reqwest::blocking::Client,
}
impl Drop for Service {
    fn drop(&mut self) {
        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGTERM);
        }
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            if self.child.try_wait().is_ok_and(|status| status.is_some()) {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Service {
    fn spawn(root: &std::path::Path, config: &std::path::Path, port: u16) -> Child {
        let log = File::create(root.join("supervisor-output.log")).unwrap();
        Command::new(env!("CARGO_BIN_EXE_bic"))
            .arg("--data")
            .arg(root)
            .args([
                "serve",
                "--listen",
                &format!("127.0.0.1:{port}"),
                "--microvm-config",
            ])
            .arg(config)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap()
    }
    fn new(config: PathBuf) -> Self {
        let root = tempfile::tempdir().unwrap();
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let child = Self::spawn(root.path(), &config, port);
        let service = Self {
            child,
            root,
            port,
            config,
            client: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap(),
        };
        service.ready();
        service
    }
    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
    fn ready(&self) {
        self.wait(|| {
            self.client
                .get(format!("{}/health", self.base()))
                .send()
                .is_ok_and(|r| r.status().is_success())
        });
    }
    fn wait(&self, mut condition: impl FnMut() -> bool) {
        let started = Instant::now();
        while !condition() {
            assert!(
                started.elapsed() < Duration::from_secs(45),
                "condition timed out; supervisor: {}; application: {}",
                self.logs("supervisor-output.log"),
                self.logs("apps/counter/application.log")
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    fn logs(&self, path: &str) -> String {
        std::fs::read_to_string(self.root.path().join(path)).unwrap_or_default()
    }
    fn token(&self) -> String {
        std::fs::read_to_string(self.root.path().join("management.token"))
            .unwrap()
            .trim()
            .into()
    }
    fn deploy(&self, release: &Release) -> reqwest::blocking::Response {
        self.client
            .post(format!("{}/v1/apps", self.base()))
            .bearer_auth(self.token())
            .json(release)
            .send()
            .unwrap()
    }
    fn counters(&self) -> Value {
        let response = self
            .client
            .get(format!("{}/apps/counter", self.base()))
            .send()
            .unwrap();
        assert!(
            response.status().is_success(),
            "counter request: {}; supervisor: {}; application: {}",
            response.text().unwrap(),
            self.logs("supervisor-output.log"),
            self.logs("apps/counter/application.log")
        );
        response.json().unwrap()
    }
}

#[test]
#[ignore = "requires Linux KVM, root and BICDB_TEST_MICROVM_CONFIG with pinned images"]
fn jailed_microvm_executes_scoped_database_jobs_and_recovers_after_supervisor_crash() {
    let config = PathBuf::from(
        std::env::var_os("BICDB_TEST_MICROVM_CONFIG").expect("set BICDB_TEST_MICROVM_CONFIG"),
    );
    let expected_uid = bicdb_app_host::microvm::Config::read(&config)
        .unwrap()
        .allocations["counter"]
        .uid
        .to_string();
    let host_config = bicdb_app_host::microvm::Config::read(&config).unwrap();
    let forbidden_host = Canary::start();
    let forbidden_url = format!(
        "http://{}:{}/",
        host_config.allocations["counter"].host_ip, forbidden_host.port
    );
    let mut service = Service::new(config);
    let marker = service.root.path().join("host-only-marker");
    std::fs::write(&marker, "host-private").unwrap();
    let mut release = Release::read(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/platform/counter/bic.json"),
    )
    .unwrap();
    release.manifest.isolation = Isolation::Microvm;
    release.manifest.resources.memory_mb = 256;
    let original = String::from_utf8(release.decode("app.mjs").unwrap()).unwrap();
    let source = format!("import fs from 'node:fs'; import os from 'node:os';\n{original}");
    let source = source.replace("const path = new URL(request.url).pathname;", &format!("const path = new URL(request.url).pathname; if (path === '/runtime') return Response.json({{uid:process.getuid(), kernel:os.release(), hostFileVisible:fs.existsSync({})}});", serde_json::to_string(&marker.to_string_lossy()).unwrap()));
    let source = source.replace("const path = new URL(request.url).pathname;", &format!("const path = new URL(request.url).pathname; if (path === '/network') {{ let reachable = false; try {{ reachable = (await fetch({}, {{signal:AbortSignal.timeout(250)}})).ok; }} catch {{}} return Response.json({{reachable}}); }}", serde_json::to_string(&forbidden_url).unwrap()));
    release.files.insert(
        "app.mjs".into(),
        base64::engine::general_purpose::STANDARD.encode(source),
    );
    let response = service.deploy(&release);
    assert!(
        response.status().is_success(),
        "{}; log: {}",
        response.text().unwrap(),
        service.logs("apps/counter/application.log")
    );
    eprintln!("phase: initial VM ready");
    let runtime: Value = service
        .client
        .get(format!("{}/apps/counter/runtime", service.base()))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(runtime["uid"], 1000);
    assert_eq!(runtime["hostFileVisible"], false);
    assert!(
        service
            .client
            .get(format!("http://127.0.0.1:{}/", forbidden_host.port))
            .send()
            .unwrap()
            .status()
            .is_success(),
        "host canary must be reachable outside the VM"
    );
    let network: Value = service
        .client
        .get(format!("{}/apps/counter/network", service.base()))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(
        network["reachable"], false,
        "guest must not reach other host services"
    );
    let process: Value = serde_json::from_slice(
        &std::fs::read(
            service
                .root
                .path()
                .join("apps/counter/microvm-process.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let status =
        std::fs::read_to_string(format!("/proc/{}/status", process["pid"].as_u64().unwrap()))
            .unwrap();
    assert!(
        status.lines().any(|line| line.starts_with("Uid:")
            && line.split_whitespace().nth(1) == Some(expected_uid.as_str())),
        "jailer must drop the host UID: {status}"
    );
    for _ in 0..2 {
        service
            .client
            .post(format!(
                "{}/apps/counter/increment-and-enqueue",
                service.base()
            ))
            .header("idempotency-key", "vm-transaction")
            .send()
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    eprintln!("phase: requests committed, awaiting job");
    service.wait(|| service.counters()[0]["value"] == 1);
    assert_eq!(service.counters()[1]["value"], 1);
    let mut downgrade = release.clone();
    downgrade.manifest.isolation = Isolation::Process;
    assert_eq!(service.deploy(&downgrade).status(), 400);
    eprintln!("phase: killing supervisor");
    service.child.kill().unwrap();
    service.child.wait().unwrap();
    service.child = Service::spawn(service.root.path(), &service.config, service.port);
    service.ready();
    eprintln!("phase: supervisor recovered");
    assert_eq!(
        service.counters(),
        json!([{"id":"jobs","value":1},{"id":"requests","value":1}])
    );
    assert!(
        !std::path::Path::new(&format!("/proc/{}", process["pid"].as_u64().unwrap())).exists(),
        "the previous VMM must be fenced before replacing it"
    );
    let response = service
        .client
        .post(format!("{}/v1/apps/counter/stop", service.base()))
        .bearer_auth(service.token())
        .send()
        .unwrap();
    assert!(response.status().is_success());
    assert_eq!(
        service
            .client
            .get(format!("{}/apps/counter", service.base()))
            .send()
            .unwrap()
            .status(),
        503
    );
    let response = service
        .client
        .post(format!("{}/v1/apps/counter/start", service.base()))
        .bearer_auth(service.token())
        .send()
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    assert_eq!(service.counters()[1]["value"], 1);
}
