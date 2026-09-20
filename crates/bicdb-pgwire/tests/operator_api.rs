use bicdb_pgwire::{operator::OperatorService, AuthMethod, PgWireConfig, PgWireServer};
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
use tokio_postgres::{Client, NoTls};

const TOKEN: &str = "public-test-only-operator-credential-123456789";
fn token_file(path: &Path) -> PathBuf {
    let path = path.join("operator-token");
    std::fs::write(&path, TOKEN).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    path
}
fn exchange(mut stream: impl Read + Write, token: &str, body: &[u8]) -> (u16, Value) {
    write!(stream, "POST /v1/logins HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
    stream.write_all(body).unwrap();
    stream.flush().unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).unwrap();
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap();
    let head = std::str::from_utf8(&bytes[..split]).unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, serde_json::from_slice(&bytes[split + 4..]).unwrap())
}
fn http(port: u16, token: &str, body: &Value) -> (u16, Value) {
    let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    exchange(stream, token, &serde_json::to_vec(body).unwrap())
}
struct Server {
    server: Arc<PgWireServer>,
    thread: Option<thread::JoinHandle<bicdb_pgwire::Result<()>>>,
    port: u16,
    operator: u16,
    tls: bool,
}
impl Server {
    fn start(path: &Path, tls: bool, auth: AuthMethod) -> Self {
        Self::start_with_operator(path, tls, auth, true)
    }
    fn start_with_operator(path: &Path, tls: bool, auth: AuthMethod, operator: bool) -> Self {
        let operator_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = operator_listener.local_addr().unwrap();
        drop(operator_listener);
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
        let server = PgWireServer::open(
            path,
            PgWireConfig {
                require_auth: true,
                auth_method: auth,
                tls_cert: tls.then(|| fixture.join("sha256.pem")),
                tls_key: tls.then(|| fixture.join("sha256.key")),
                ..PgWireConfig::default()
            },
        )
        .unwrap();
        if operator {
            server
                .install_host_service(Arc::new(
                    OperatorService::from_token_file(
                        address,
                        "host-operator".into(),
                        token_file(path),
                    )
                    .unwrap(),
                ))
                .unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let copy = server.clone();
        let handle = thread::spawn(move || bicdb_pgwire::serve_existing_listener(copy, listener));
        for _ in 0..200 {
            if TcpStream::connect(if operator {
                address
            } else {
                ([127, 0, 0, 1], port).into()
            })
            .is_ok()
            {
                return Self {
                    server,
                    thread: Some(handle),
                    port,
                    operator: address.port(),
                    tls,
                };
            }
            thread::sleep(Duration::from_millis(25));
        }
        panic!("operator did not start");
    }
    fn call(&self, body: Value) -> Value {
        let (status, value) = self.request(TOKEN, &body);
        assert_eq!(status, 200, "{value}");
        value
    }
    fn request(&self, token: &str, body: &Value) -> (u16, Value) {
        if !self.tls {
            return http(self.operator, token, body);
        }
        let stream = tls_connector()
            .connect(
                "localhost",
                TcpStream::connect(("127.0.0.1", self.operator)).unwrap(),
            )
            .unwrap();
        exchange(stream, token, &serde_json::to_vec(body).unwrap())
    }
    async fn connect(&self, user: &str, password: &str) -> Result<Client, tokio_postgres::Error> {
        let mut config = tokio_postgres::Config::new();
        config
            .host("localhost")
            .port(self.port)
            .user(user)
            .password(password)
            .dbname("bicdb");
        let client = if self.tls {
            let (client, connection) = config
                .connect(postgres_openssl::MakeTlsConnector::new(tls_connector()))
                .await?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        } else {
            config.host("127.0.0.1");
            let (client, connection) = config.connect(NoTls).await?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        };
        Ok(client)
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.server.request_shutdown();
        self.thread.take().unwrap().join().unwrap().unwrap();
    }
}
fn identity(user: &str) -> Value {
    json!({"user_id":user,"tenant_id":"tenant-one","workspace_id":"workspace-one","client_id":"client-one","roles":["reader"],"scopes":["records:read"]})
}
async fn current(client: &Client) -> Vec<String> {
    let row=client.query_one("SELECT current_setting('carrier.current_user', true), current_trusted_tenant(), current_setting('carrier.current_workspace', true), current_setting('carrier.current_roles', true), current_setting('carrier.current_scopes', true), current_setting('carrier.current_client', true)",&[]).await.unwrap();
    (0..6).map(|index| row.get::<_, String>(index)).collect()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn online_lifecycle_preserves_queries_and_immutable_sessions() {
    for auth in [AuthMethod::ScramSha256, AuthMethod::Cleartext] {
        let directory = tempfile::tempdir().unwrap();
        bicdb_pgwire::create_user(directory.path(), "bootstrap", "bootstrap-password").unwrap();
        let server = Server::start(
            directory.path(),
            matches!(auth, AuthMethod::Cleartext),
            auth,
        );
        let bootstrap = server
            .connect("bootstrap", "bootstrap-password")
            .await
            .unwrap();
        let running = Arc::new(AtomicBool::new(true));
        let count = Arc::new(AtomicUsize::new(0));
        let alive = running.clone();
        let queries = count.clone();
        let workload = tokio::spawn(async move {
            while alive.load(Ordering::SeqCst) {
                bootstrap.query_one("SELECT 1", &[]).await.unwrap();
                queries.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        });
        server.call(json!({"operation":"create","username":"app","password":"initial-test-password","identity":identity("principal-one")}));
        let old = server
            .connect("app", "initial-test-password")
            .await
            .unwrap();
        assert_eq!(
            current(&old).await,
            vec![
                "principal-one",
                "tenant-one",
                "workspace-one",
                "reader",
                "records:read",
                "client-one"
            ]
        );
        assert_eq!(server.request(TOKEN,&json!({"operation":"create","username":"app","password":"overwrite","identity":identity("bad")})).0,409);
        server.call(
            json!({"operation":"identity","username":"app","identity":identity("principal-two")}),
        );
        assert_eq!(current(&old).await[0], "principal-one");
        let rebound = server
            .connect("app", "initial-test-password")
            .await
            .unwrap();
        assert_eq!(current(&rebound).await[0], "principal-two");
        drop(rebound);
        server.call(
            json!({"operation":"password","username":"app","password":"rotated-test-password"}),
        );
        assert!(server
            .connect("app", "initial-test-password")
            .await
            .is_err());
        let new = server
            .connect("app", "rotated-test-password")
            .await
            .unwrap();
        assert_eq!(current(&new).await[0], "principal-two");
        drop(new);
        server.call(json!({"operation":"enabled","username":"app","enabled":false}));
        assert!(server
            .connect("app", "rotated-test-password")
            .await
            .is_err());
        // Even local password rotation must not re-enable a disabled login.
        bicdb_pgwire::create_user(directory.path(), "app", "third-test-password").unwrap();
        assert!(server.connect("app", "third-test-password").await.is_err());
        server.call(json!({"operation":"enabled","username":"app","enabled":true}));
        drop(server.connect("app", "third-test-password").await.unwrap());
        let metadata = server.call(json!({"operation":"list","limit":1}));
        assert_eq!(metadata["logins"][0]["identity"], identity("principal-two"));
        assert_eq!(metadata["next_after"], "app");
        let metadata = server.call(json!({"operation":"list","after":"app"}));
        assert_eq!(metadata["logins"][0]["username"], "bootstrap");
        bicdb_pgwire::set_user_delegation_policy(
            directory.path(),
            "app",
            Some(bicdb_pgwire::DelegationPolicy {
                signing_key: "test-signing-key-only-12345678901234567890".into(),
                tenants: vec!["tenant-one".into()],
            }),
        )
        .unwrap();
        server.call(json!({"operation":"revoke","username":"app"}));
        assert!(server.connect("app", "third-test-password").await.is_err());
        assert_eq!(current(&old).await[0], "principal-one");
        drop(old);
        let policies: Value = serde_json::from_slice(
            &std::fs::read(directory.path().join("server_delegation.json")).unwrap(),
        )
        .unwrap();
        assert!(policies.get("app").is_none());
        running.store(false, Ordering::SeqCst);
        workload.await.unwrap();
        assert!(count.load(Ordering::SeqCst) > 10);
        drop(server);
        let server = Server::start(
            directory.path(),
            matches!(auth, AuthMethod::Cleartext),
            auth,
        );
        assert!(server.connect("app", "third-test-password").await.is_err());
        server.call(json!({"operation":"create","username":"app","password":"new-generation-password","identity":identity("principal-three")}));
        drop(
            server
                .connect("app", "new-generation-password")
                .await
                .unwrap(),
        );
        let logs = std::fs::read_to_string(directory.path().join("server_operator.jsonl")).unwrap();
        for forbidden in [
            TOKEN,
            "initial-test-password",
            "rotated-test-password",
            "third-test-password",
            "new-generation-password",
            "salt_hex",
            "hash_hex",
            "scram_stored_key",
            "principal-one",
        ] {
            assert!(!logs.contains(forbidden), "log contains {forbidden}");
        }
        for line in logs.lines() {
            let event: Value = serde_json::from_str(line).unwrap();
            assert_eq!(event["actor"], "host-operator");
            assert!(event.get("operation").is_some());
            assert!(event.get("outcome").is_some());
        }
    }
}
#[tokio::test]
async fn operator_authentication_and_bounded_inputs_are_independent_of_sql_roles() {
    let directory = tempfile::tempdir().unwrap();
    {
        let mut db = bicdb_core::BicDb::open(directory.path()).unwrap();
        bicdb_sql::SqlSession::new(&mut db)
            .execute("CREATE ROLE root SUPERUSER")
            .unwrap();
    }
    bicdb_pgwire::create_user(directory.path(), "root", "sql-root-password").unwrap();
    let server = Server::start(directory.path(), false, AuthMethod::ScramSha256);
    let root = server.connect("root", "sql-root-password").await.unwrap();
    root.batch_execute("CREATE ROLE sql_operator SUPERUSER")
        .await
        .unwrap();
    for token in [
        "",
        "sql-root-password",
        "sql_operator",
        "wrong-operator-token-12345678901234567890",
    ] {
        assert_eq!(
            http(server.operator, token, &json!({"operation":"list"})).0,
            401
        );
    }
    for body in [
        json!({"operation":"list","limit":0}),
        json!({"operation":"list","limit":1001}),
        json!({"operation":"list","password":"secret"}),
        json!({"operation":"create","username":"app\nforged","password":"secret","identity":identity("one")}),
        json!({"operation":"create","username":"app","password":"x".repeat(40000),"identity":identity("one")}),
    ] {
        assert_eq!(http(server.operator, TOKEN, &body).0, 400);
    }
    assert!(!bicdb_pgwire::user_exists(directory.path(), "app").unwrap());
    let logs = std::fs::read_to_string(directory.path().join("server_operator.jsonl")).unwrap();
    assert!(!logs.contains("sql-root-password"));
    assert!(!logs.contains("forged"));
    assert!(logs.contains("unauthenticated"));
    drop(root);
}
#[test]
fn concurrent_operator_and_local_updates_preserve_record_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    bicdb_pgwire::create_user(directory.path(), "app", "first-password").unwrap();
    let server = Server::start(directory.path(), false, AuthMethod::ScramSha256);
    let port = server.operator;
    let path = directory.path().to_path_buf();
    let rotate = thread::spawn(move || {
        for i in 0..8 {
            bicdb_pgwire::create_user(&path, "app", &format!("local-password-{i}")).unwrap();
        }
    });
    for i in 0..8 {
        assert_eq!(http(port,TOKEN,&json!({"operation":"identity","username":"app","identity":identity(&format!("principal-{i}"))})).0,200);
    }
    rotate.join().unwrap();
    drop(server);
    let server = Server::start(directory.path(), false, AuthMethod::ScramSha256);
    let metadata = server.call(json!({"operation":"list"}));
    assert_eq!(metadata["logins"][0]["identity"], identity("principal-7"));
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let client = server.connect("app", "local-password-7").await.unwrap();
        assert_eq!(current(&client).await[0], "principal-7");
    });
}
#[test]
fn verified_tls_operator_request_and_private_token_file() {
    let directory = tempfile::tempdir().unwrap();
    bicdb_pgwire::create_user(directory.path(), "app", "test-password").unwrap();
    let token = token_file(directory.path());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(OperatorService::from_token_file(
            "127.0.0.1:0".parse().unwrap(),
            "operator".into(),
            &token
        )
        .is_err());
    }
    let server = Server::start(directory.path(), true, AuthMethod::ScramSha256);
    let mut connector =
        openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
    connector
        .set_ca_file(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls/sha256.pem"))
        .unwrap();
    let stream = connector
        .build()
        .connect(
            "localhost",
            TcpStream::connect(("127.0.0.1", server.operator)).unwrap(),
        )
        .unwrap();
    assert_eq!(exchange(stream, TOKEN, br#"{"operation":"list"}"#).0, 200);
}

fn tls_connector() -> openssl::ssl::SslConnector {
    let mut connector =
        openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
    connector
        .set_ca_file(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls/sha256.pem"))
        .unwrap();
    connector.build()
}

#[test]
fn failed_operation_logging_prevents_mutation() {
    let directory = tempfile::tempdir().unwrap();
    bicdb_pgwire::create_user(directory.path(), "app", "before-password").unwrap();
    let server = Server::start(directory.path(), false, AuthMethod::ScramSha256);
    let log = directory.path().join("server_operator.jsonl");
    std::fs::rename(&log, directory.path().join("saved-log")).unwrap();
    std::fs::create_dir(&log).unwrap();
    let (status, body) = http(
        server.operator,
        TOKEN,
        &json!({"operation":"password","username":"app","password":"after-password"}),
    );
    assert_eq!(status, 503);
    assert!(!body.to_string().contains("after-password"));
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        drop(server.connect("app", "before-password").await.unwrap());
        assert!(server.connect("app", "after-password").await.is_err());
    });
}

#[test]
fn a_stalled_scram_proof_cannot_authenticate_after_its_login_record_changes() {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    fn send(stream: &mut TcpStream, tag: u8, body: &[u8]) {
        stream.write_all(&[tag]).unwrap();
        stream
            .write_all(&((body.len() + 4) as u32).to_be_bytes())
            .unwrap();
        stream.write_all(body).unwrap();
        stream.flush().unwrap();
    }
    fn frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        let mut header = [0; 5];
        stream.read_exact(&mut header).unwrap();
        let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
        assert!((4..65536).contains(&length));
        let mut bytes = vec![0; length - 4];
        stream.read_exact(&mut bytes).unwrap();
        (header[0], bytes)
    }
    fn mac(key: &[u8], message: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(message);
        mac.finalize().into_bytes().to_vec()
    }
    for change in ["identity", "password", "enabled", "recreate"] {
        let directory = tempfile::tempdir().unwrap();
        bicdb_pgwire::create_user(directory.path(), "app", "test-password").unwrap();
        let server = Server::start(directory.path(), false, AuthMethod::ScramSha256);
        let mut stream = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let startup = b"\0\x03\0\0user\0app\0database\0bicdb\0\0";
        stream
            .write_all(&((startup.len() + 4) as u32).to_be_bytes())
            .unwrap();
        stream.write_all(startup).unwrap();
        assert_eq!(frame(&mut stream).0, b'R');
        let bare = "n=app,r=stalled-client-test-nonce";
        let first = format!("n,,{bare}");
        let mut initial = b"SCRAM-SHA-256\0".to_vec();
        initial.extend_from_slice(&(first.len() as u32).to_be_bytes());
        initial.extend_from_slice(first.as_bytes());
        send(&mut stream, b'p', &initial);
        let (tag, auth) = frame(&mut stream);
        assert_eq!(tag, b'R');
        assert_eq!(&auth[..4], &11u32.to_be_bytes());
        let server_first = std::str::from_utf8(&auth[4..]).unwrap();
        let attr = |prefix: &str| {
            server_first
                .split(',')
                .find_map(|part| part.strip_prefix(prefix))
                .unwrap()
        };
        let mut salted = [0; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(
            b"test-password",
            &BASE64.decode(attr("s=")).unwrap(),
            attr("i=").parse().unwrap(),
            &mut salted,
        );
        let final_bare = format!("c=biws,r={}", attr("r="));
        let message = format!("{bare},{server_first},{final_bare}");
        let client_key = mac(&salted, b"Client Key");
        let signature = mac(&Sha256::digest(&client_key), message.as_bytes());
        let proof = client_key
            .iter()
            .zip(signature)
            .map(|(a, b)| a ^ b)
            .collect::<Vec<_>>();
        match change {
            "identity" => {
                server.call(json!({"operation":"identity","username":"app","identity":identity("replacement-principal")}));
            }
            "password" => {
                server.call(json!({"operation":"password","username":"app","password":"replacement-password"}));
            }
            "enabled" => {
                server.call(json!({"operation":"enabled","username":"app","enabled":false}));
            }
            _ => {
                server.call(json!({"operation":"revoke","username":"app"}));
                server.call(json!({"operation":"create","username":"app","password":"test-password","identity":identity("replacement-principal")}));
            }
        }
        send(
            &mut stream,
            b'p',
            format!("{final_bare},p={}", BASE64.encode(proof)).as_bytes(),
        );
        let (mut tag, mut body) = frame(&mut stream);
        if tag == b'R' {
            assert_eq!(&body[..4], &12u32.to_be_bytes());
            (tag, body) = frame(&mut stream);
        }
        assert_eq!(tag, b'E', "{change} accepted the old proof");
        assert!(String::from_utf8_lossy(&body).contains("28P01"));
    }
}

#[tokio::test]
async fn sql_superuser_cannot_replace_operator_login_credentials() {
    let directory = tempfile::tempdir().unwrap();
    {
        let mut db = bicdb_core::BicDb::open(directory.path()).unwrap();
        bicdb_sql::SqlSession::new(&mut db)
            .execute("CREATE ROLE root SUPERUSER; CREATE ROLE protected_login LOGIN")
            .unwrap();
    }
    bicdb_pgwire::create_user(directory.path(), "root", "sql-root-password").unwrap();
    let server = Server::start(directory.path(), false, AuthMethod::ScramSha256);
    server.call(json!({"operation":"create", "username":"protected_login", "password":"original-password", "identity":identity("protected-principal")}));
    server.call(json!({"operation":"create", "username":"physical_only", "password":"original-password", "identity":identity("other-principal")}));
    // The persisted policy must also protect existing logins when the listener
    // is not configured on a later server using the same authentication catalog.
    async fn attempt_changes(server: &Server) {
        let root = server.connect("root", "sql-root-password").await.unwrap();
        for sql in [
            "ALTER ROLE protected_login PASSWORD 'replacement-password'",
            "ALTER USER protected_login PASSWORD NULL",
            "DROP ROLE protected_login",
            "CREATE ROLE physical_only LOGIN PASSWORD 'replacement-password'",
            "CREATE USER new_login PASSWORD 'replacement-password'",
        ] {
            let error = root.batch_execute(sql).await.expect_err(sql);
            assert_eq!(error.code().unwrap().code(), "42501", "{sql}: {error}");
        }
        root.batch_execute("CREATE ROLE permission_group NOLOGIN; GRANT permission_group TO protected_login; REVOKE permission_group FROM protected_login; DROP ROLE permission_group")
            .await.unwrap();
        let login = server
            .connect("protected_login", "original-password")
            .await
            .unwrap();
        assert_eq!(current(&login).await[0], "protected-principal");
        assert!(server
            .connect("protected_login", "replacement-password")
            .await
            .is_err());
        assert!(server
            .connect("new_login", "replacement-password")
            .await
            .is_err());
    }
    attempt_changes(&server).await;
    drop(server);
    let server =
        Server::start_with_operator(directory.path(), false, AuthMethod::ScramSha256, false);
    attempt_changes(&server).await;
    // Local host-authorized rotation remains available without the HTTP service.
    bicdb_pgwire::create_user(directory.path(), "protected_login", "host-rotated-password")
        .unwrap();
    let login = server
        .connect("protected_login", "host-rotated-password")
        .await
        .unwrap();
    assert_eq!(current(&login).await[0], "protected-principal");
}
