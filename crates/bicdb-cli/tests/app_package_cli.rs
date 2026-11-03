//! Offline package keys and signing, plus the opt-in lifecycle API of a
//! running `bicdb app ... serve`.
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};

fn bicdb() -> Command {
    Command::new(env!("CARGO_BIN_EXE_bicdb"))
}

fn run(command: &mut Command) -> (bool, String, String) {
    let output = command.output().unwrap();
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// The unsigned package a JSON producer writes: hashes and signature empty.
fn unsigned_package(version: &str, marker: &str) -> Value {
    let bytes = |text: &str| json!(text.as_bytes());
    json!({
        "manifest": {
            "identity": {"name": "hello", "version": version, "abi_version": 2},
            "capabilities": ["http_routes"],
            "application": {
                "application_profile": "bicdb-application-v2",
                "package": {
                    "application": "hello", "version": version,
                    "package_sha256": "", "dependency_lock_sha256": "", "sbom_sha256": "",
                    "provenance_sha256": "", "signature_key_id": "", "signature_algorithm": "",
                    "signature": ""
                },
                "routes": [{
                    "name": "hello", "method": "GET", "template": "/hello",
                    "script": {"module": "api", "export": "hello"},
                    "public": true, "max_request_bytes": 1024, "max_response_bytes": 4096
                }]
            }
        },
        "modules": {},
        "scripts": {"api": format!(
            "function hello(request: any) {{ return {{ status: 200, body: {{ marker: \"{marker}\", path: request.path }} }}; }}"
        )},
        "dependency_lock": bytes(r#"{"format":1,"packages":[]}"#),
        "sbom": bytes(r#"{"bomFormat":"CycloneDX","specVersion":"1.5"}"#),
        "provenance": bytes(r#"{"builder":"hub"}"#)
    })
}

fn write_json(path: &Path, value: &Value) {
    std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
}

fn sign(dir: &Path, version: &str, marker: &str, key: &Path) -> std::path::PathBuf {
    let unsigned = dir.join(format!("hello-{version}.unsigned.json"));
    let signed = dir.join(format!("hello-{version}.json"));
    write_json(&unsigned, &unsigned_package(version, marker));
    let (ok, stdout, stderr) = run(bicdb()
        .args(["app-package", "sign"])
        .arg(&unsigned)
        .args(["--key-id", "release", "--signing-key"])
        .arg(key)
        .arg("--out")
        .arg(&signed));
    assert!(ok, "{stderr}");
    assert!(stdout.contains("package_sha256"), "{stdout}");
    signed
}

#[test]
fn keygen_sign_and_verify_signature_round_trip_and_reject_tampering() {
    let dir = tempfile::tempdir().unwrap();
    let (private, public) = (
        dir.path().join("release.key"),
        dir.path().join("release.pub"),
    );
    let keygen = |private: &Path, public: &Path| {
        run(bicdb()
            .args(["app-package", "keygen", "--out-private"])
            .arg(private)
            .arg("--out-public")
            .arg(public))
    };
    let (ok, _, stderr) = keygen(&private, &public);
    assert!(ok, "{stderr}");
    assert_eq!(std::fs::read_to_string(&private).unwrap().trim().len(), 64);
    assert_eq!(std::fs::read_to_string(&public).unwrap().trim().len(), 64);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&private).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0);
    }
    assert!(
        !keygen(&private, &public).0,
        "keygen must not overwrite keys"
    );

    let signed = sign(dir.path(), "1.0.0", "one", &private);
    let trusted = format!("release={}", public.display());
    let verify = |package: &Path| {
        run(bicdb()
            .arg("app")
            .arg(dir.path().join("db"))
            .args(["--trusted-key", &trusted, "verify-signature"])
            .arg(package))
    };
    let (ok, stdout, stderr) = verify(&signed);
    assert!(ok, "{stderr}");
    assert!(
        stdout.contains("\"signing_key_id\": \"release\""),
        "{stdout}"
    );

    let mut tampered: Value = serde_json::from_slice(&std::fs::read(&signed).unwrap()).unwrap();
    tampered["scripts"]["api"] = json!("function hello() { return { status: 500 }; }");
    let tampered_path = dir.path().join("tampered.json");
    write_json(&tampered_path, &tampered);
    let (ok, _, stderr) = verify(&tampered_path);
    assert!(!ok);
    assert!(stderr.contains("package hash mismatch"), "{stderr}");

    // A base64 seed is accepted and a different key is not trusted.
    let other = dir.path().join("other.key");
    std::fs::write(&other, "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=").unwrap();
    let foreign = sign(dir.path(), "9.0.0", "foreign", &other);
    let (ok, _, stderr) = verify(&foreign);
    assert!(!ok);
    assert!(stderr.contains("signature"), "{stderr}");
}

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn http(address: &str, method: &str, path: &str, token: Option<&str>, body: &[u8]) -> (u16, Value) {
    let mut stream = TcpStream::connect(address).unwrap();
    let authorization = token
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    write!(
        stream,
        "{method} {path} HTTP/1.0\r\nHost: {address}\r\nConnection: close\r\n{authorization}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(body).unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    let response = String::from_utf8_lossy(&response);
    let status = response[9..12].parse().unwrap();
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .unwrap_or("");
    (
        status,
        serde_json::from_str(body).unwrap_or(Value::String(body.to_string())),
    )
}

#[test]
fn running_server_installs_upgrades_and_rolls_back_through_the_management_api() {
    let dir = tempfile::tempdir().unwrap();
    let (private, public) = (
        dir.path().join("release.key"),
        dir.path().join("release.pub"),
    );
    assert!(
        run(bicdb()
            .args(["app-package", "keygen", "--out-private"])
            .arg(&private)
            .arg("--out-public")
            .arg(&public))
        .0
    );
    let token = "operator-token-0123456789abcdef0123456789";
    std::fs::write(dir.path().join("token"), token).unwrap();
    std::fs::write(dir.path().join("jwt.key"), [7_u8; 32]).unwrap();
    let mut child = bicdb()
        .arg("app")
        .arg(dir.path().join("db"))
        .arg("--trusted-key")
        .arg(format!("release={}", public.display()))
        .args([
            "serve",
            "--http-port",
            "0",
            "--pg-port",
            "0",
            "--jwt-hs256-key",
        ])
        .arg(dir.path().join("jwt.key"))
        .args(["--jwt-issuer", "test", "--jwt-audience", "test"])
        .arg("--management-token-file")
        .arg(dir.path().join("token"))
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let server = Server(child);
    let mut lines = BufReader::new(stdout).lines();
    let line = lines
        .by_ref()
        .map(Result::unwrap)
        .find(|line| line.contains("application HTTP listening"))
        .expect("serve banner");
    // Keep draining server output so the child never blocks on a full pipe.
    std::thread::spawn(move || for _ in lines {});
    let address = line
        .split("http://")
        .nth(1)
        .and_then(|rest| rest.split(';').next())
        .unwrap_or_else(|| panic!("unexpected serve banner: {line}"))
        .to_string();

    let first = std::fs::read(sign(dir.path(), "1.0.0", "one", &private)).unwrap();
    let second = std::fs::read(sign(dir.path(), "1.1.0", "two", &private)).unwrap();
    let unsigned = serde_json::to_vec(&unsigned_package("1.0.0", "evil")).unwrap();

    assert_eq!(
        http(&address, "POST", "/_bicdb/apps/install", None, &first).0,
        401
    );
    assert_eq!(
        http(
            &address,
            "POST",
            "/_bicdb/apps/install",
            Some("wrong-token-0123456789abcdef0123"),
            &first
        )
        .0,
        401
    );
    let (status, body) = http(
        &address,
        "POST",
        "/_bicdb/apps/install",
        Some(token),
        &unsigned,
    );
    assert_eq!(status, 400, "{body}");
    assert_eq!(http(&address, "GET", "/hello", None, b"").0, 404);

    let (status, body) = http(
        &address,
        "POST",
        "/_bicdb/apps/install",
        Some(token),
        &first,
    );
    assert_eq!(status, 200, "{body}");
    let (status, body) = http(&address, "GET", "/hello", None, b"");
    assert_eq!(
        (status, body["marker"].clone()),
        (200, json!("one")),
        "{body}"
    );

    let (status, body) = http(
        &address,
        "POST",
        "/_bicdb/apps/upgrade",
        Some(token),
        &second,
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        http(&address, "GET", "/hello", None, b"").1["marker"],
        "two"
    );
    let (status, apps) = http(&address, "GET", "/_bicdb/apps", Some(token), b"");
    assert_eq!(status, 200);
    assert_eq!(apps[0]["version"], "1.1.0");
    assert_eq!(apps[0]["history"], 1);

    let (status, _) = http(
        &address,
        "POST",
        "/_bicdb/apps/hello/rollback",
        Some(token),
        b"",
    );
    assert_eq!(status, 200);
    assert_eq!(
        http(&address, "GET", "/hello", None, b"").1["marker"],
        "one"
    );
    drop(server);
}
