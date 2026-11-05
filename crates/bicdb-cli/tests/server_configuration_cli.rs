use serde_json::Value;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_bicdb"));
    // Keep host configuration out of hermetic test subprocesses.
    for (key, _) in std::env::vars_os() {
        let name = key.to_string_lossy();
        if name.starts_with("BICDB_") {
            command.env_remove(key);
        }
    }
    command
}
fn inspect(file: &Path, args: &[&str]) -> Output {
    command()
        .arg("config")
        .args(args)
        .arg("--file")
        .arg(file)
        .output()
        .unwrap()
}
fn json(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn validate_and_show_do_not_create_database_and_reject_bad_tls() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("bicdb.toml");
    std::fs::write(&file, "version=1\n[server]\npath='unopened'\nport=5433\n").unwrap();
    let report = json(&inspect(&file, &["validate", "--json"]));
    assert_eq!(report["database_opened"], false);
    assert_eq!(report["listeners_bound"], false);
    let report = json(&inspect(&file, &["show", "--effective"]));
    assert_eq!(report["server"]["port"]["value"], 5433);
    assert_eq!(report["validation"], "not_performed");
    assert!(!root.path().join("unopened").exists());
    std::fs::write(
        &file,
        "version=1\n[server]\npath='unopened'\nmax_connections=0\n",
    )
    .unwrap();
    assert_eq!(inspect(&file, &["validate"]).status.code(), Some(2));
    assert!(!root.path().join("unopened").exists());
    std::fs::write(&file,"version=1\n[server]\npath='unopened'\nrequire_tls=true\ntls_cert='missing.pem'\ntls_key='missing-key.pem'\n").unwrap();
    assert_eq!(inspect(&file, &["validate"]).status.code(), Some(2));
    assert_eq!(
        json(&inspect(&file, &["show", "--effective"]))["validation"],
        "not_performed"
    );
    assert!(!root.path().join("unopened").exists());
}

#[tokio::test]
async fn running_diff_uses_frozen_startup_values_and_never_applies_changes() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("bicdb.toml");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let text = format!("version=1\n[server]\npath='database'\nport={port}\nmax_connections=20\n[engine]\nquery_work_memory_bytes=1048576\n");
    std::fs::write(&file, &text).unwrap();
    let child = command()
        .args(["serve", "--config"])
        .arg(&file)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut server = Server(child);
    let started = Instant::now();
    let (client, connection) = loop {
        match tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(port)
            .user("bicdb")
            .dbname("bicdb")
            .connect(tokio_postgres::NoTls)
            .await
        {
            Ok(connection) => break connection,
            Err(error) => {
                assert!(
                    server.0.try_wait().unwrap().is_none(),
                    "server exited: {error}"
                );
                assert!(
                    started.elapsed() < Duration::from_secs(30),
                    "startup timeout: {error}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    let task = tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(
            "CREATE TABLE config_smoke(id INT PRIMARY KEY); INSERT INTO config_smoke VALUES(7)",
        )
        .await
        .unwrap();
    assert_eq!(
        client
            .query_one("SELECT id FROM config_smoke", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        7
    );
    let text: String = client
        .query_one("SELECT * FROM bicdb_server_configuration", &[])
        .await
        .unwrap()
        .get(0);
    let snapshot: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(snapshot["validation"], "passed");
    assert_eq!(
        snapshot["engine"]["query_work_memory_bytes"]["value"],
        1048576
    );
    let unchanged = inspect(&file, &["diff", "--running"]);
    assert_eq!(json(&unchanged)["different"], false);
    std::fs::write(
        &file,
        std::fs::read_to_string(&file)
            .unwrap()
            .replace("max_connections=20", "max_connections=21"),
    )
    .unwrap();
    let drift = inspect(&file, &["diff", "--running"]);
    assert_eq!(
        drift.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&drift.stderr)
    );
    let report: Value = serde_json::from_slice(&drift.stdout).unwrap();
    assert_eq!(report["restart_required"], true);
    assert_eq!(report["applied"], false);
    assert_eq!(report["changes"][0]["setting"], "server.max_connections");
    let current: String = client
        .query_one("SELECT * FROM bicdb_server_configuration", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(current, text);
    drop(client);
    task.abort();
}

#[test]
fn running_diff_supports_verified_tls_scram_and_rejects_wrong_hostname() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("bicdb.toml");
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("../bicdb-pgwire/tests/fixtures/tls");
    let cert = fixtures.join("config-server.pem").canonicalize().unwrap();
    let ca = fixtures.join("sha256.pem").canonicalize().unwrap();
    let key = fixtures.join("config-server.key").canonicalize().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    bicdb_pgwire::create_user(
        root.path().join("database"),
        "bicdb",
        "synthetic-config-password",
    )
    .unwrap();
    std::fs::write(&file,format!("version=1\n[server]\npath='database'\nport={port}\nrequire_auth=true\nauth_method='scram-sha-256'\nrequire_tls=true\ntls_cert='{}'\ntls_key='{}'\n",cert.display(),key.display())).unwrap();
    let mut server = Server(
        command()
            .args(["serve", "--config"])
            .arg(&file)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let started = Instant::now();
    loop {
        // The recovery listener accepts TCP but declines TLS until the
        // database is open. Wait for protocol readiness, not just a port.
        if let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", port)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut response = [0_u8];
            if stream.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).is_ok()
                && stream.read_exact(&mut response).is_ok()
                && response == [b'S']
            {
                break;
            }
        }
        assert!(server.0.try_wait().unwrap().is_none(), "TLS server exited");
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "TLS server startup timeout"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let run = |hostname: &str| {
        command()
            .args(["config", "diff", "--running", "--file"])
            .arg(&file)
            .args(["--tls-ca"])
            .arg(&ca)
            .args([
                "--server-name",
                hostname,
                "--password-env",
                "CONFIG_TEST_PASSWORD",
            ])
            .env("CONFIG_TEST_PASSWORD", "synthetic-config-password")
            .output()
            .unwrap()
    };
    let output = run("localhost");
    assert_eq!(json(&output)["different"], false);
    let output = run("wrong.invalid");
    assert_eq!(output.status.code(), Some(2));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("synthetic-config-password"));
}
