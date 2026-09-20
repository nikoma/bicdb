//! Real TLS clients exercise SCRAM-PLUS; fixture keys are public test-only keys.
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use bicdb_pgwire::{AuthMethod, ChannelBindingPolicy, PgWireConfig, PgWireServer};
use hmac::{Hmac, Mac};
use openssl::ssl::{SslConnector, SslMethod};
use pbkdf2::pbkdf2_hmac;
use postgres_openssl::MakeTlsConnector;
use sha2::{Digest, Sha256, Sha384};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::Duration,
};
use tokio_postgres::config::{ChannelBinding, SslMode};

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/tls")
        .join(name)
}

struct Server {
    server: Arc<PgWireServer>,
    thread: Option<thread::JoinHandle<bicdb_pgwire::Result<()>>>,
    port: u16,
    directory: tempfile::TempDir,
    cert: PathBuf,
}

impl Server {
    fn start(name: &str) -> Self {
        Self::start_with(Some(name), None)
    }
    fn start_with(name: Option<&str>, policy: Option<ChannelBindingPolicy>) -> Self {
        let tls_enabled = name.is_some();
        let name = name.unwrap_or("sha256");
        let directory = tempfile::tempdir().unwrap();
        let cert = fixture(&format!("{name}.pem"));
        let chain_path = directory.path().join("chain.pem");
        let mut chain = std::fs::read(&cert).unwrap();
        chain.extend(std::fs::read(fixture("sha256.pem")).unwrap());
        std::fs::write(&chain_path, chain).unwrap();
        bicdb_pgwire::create_user(directory.path(), "app", "test-password").unwrap();
        let server = PgWireServer::open(
            directory.path(),
            PgWireConfig {
                require_auth: true,
                auth_method: AuthMethod::ScramSha256,
                channel_binding: policy,
                tls_cert: tls_enabled.then_some(chain_path),
                tls_key: tls_enabled.then(|| fixture(&format!("{name}.key"))),
                require_tls: tls_enabled,
                ..PgWireConfig::default()
            },
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let copy = server.clone();
        let thread = Some(thread::spawn(move || {
            bicdb_pgwire::serve_existing_listener(copy, listener)
        }));
        Self {
            server,
            thread,
            port,
            directory,
            cert,
        }
    }
    fn connector(&self) -> openssl::ssl::SslConnectorBuilder {
        let mut connector = SslConnector::builder(SslMethod::tls()).unwrap();
        connector.set_ca_file(&self.cert).unwrap();
        connector
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.server.request_shutdown();
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap().unwrap();
        }
    }
}

#[tokio::test]
async fn tokio_postgres_requires_channel_binding_with_sha256_and_sha384_leaves() {
    for name in ["sha256", "sha384"] {
        let server = Server::start(name);
        // rustls already loaded the original chain. Auth must use that same
        // certificate snapshot even if an operator replaces its PEM on disk.
        std::fs::copy(
            fixture("sha256.pem"),
            server.directory.path().join("chain.pem"),
        )
        .unwrap();
        let mut config = tokio_postgres::Config::new();
        config
            .host("localhost")
            .port(server.port)
            .user("app")
            .password("test-password")
            .dbname("bicdb")
            .ssl_mode(SslMode::Require)
            .channel_binding(ChannelBinding::Require)
            .connect_timeout(Duration::from_secs(10));
        let (client, connection) = config
            .connect(MakeTlsConnector::new(server.connector().build()))
            .await
            .unwrap();
        let connection = tokio::spawn(connection);
        let row = client.query_one("SELECT 1", &[]).await.unwrap();
        assert_eq!(row.get::<_, i32>(0), 1, "{name}");
        drop(client);
        connection.await.unwrap().unwrap();
    }
}

#[test]
#[ignore = "requires the psql/libpq executable; run explicitly in interoperability checks"]
fn libpq_requires_channel_binding_with_sha256_and_sha384_leaves() {
    for name in ["sha256", "sha384"] {
        let server = Server::start(name);
        let output = std::process::Command::new("psql")
            .env("PGHOST", "localhost")
            .env("PGPORT", server.port.to_string())
            .env("PGUSER", "app")
            .env("PGPASSWORD", "test-password")
            .env("PGDATABASE", "bicdb")
            .env("PGSSLMODE", "verify-full")
            .env("PGSSLROOTCERT", &server.cert)
            .env("PGCHANNELBINDING", "require")
            .env("PGCONNECT_TIMEOUT", "10")
            .args(["-XAt", "-c", "SELECT 1"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "1");
    }
}

fn frame(stream: &mut impl Read) -> (u8, Vec<u8>) {
    let mut header = [0; 5];
    stream.read_exact(&mut header).unwrap();
    let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    assert!((4..=1_048_576).contains(&length));
    let mut payload = vec![0; length - 4];
    stream.read_exact(&mut payload).unwrap();
    (header[0], payload)
}
fn send(stream: &mut impl Write, tag: u8, payload: &[u8]) {
    stream.write_all(&[tag]).unwrap();
    stream
        .write_all(&((payload.len() + 4) as u32).to_be_bytes())
        .unwrap();
    stream.write_all(payload).unwrap();
    stream.flush().unwrap();
}
fn attr<'a>(text: &'a str, name: &str) -> &'a str {
    text.split(',')
        .find_map(|part| part.strip_prefix(name))
        .unwrap()
}
fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut h = Hmac::<Sha256>::new_from_slice(key).unwrap();
    h.update(data);
    h.finalize().into_bytes().to_vec()
}

#[test]
fn scram_plus_rejects_invalid_binding_and_proof_in_require_and_prefer_modes() {
    for policy in [ChannelBindingPolicy::Require, ChannelBindingPolicy::Prefer] {
        rejects_invalid_plus(policy, true);
        rejects_invalid_plus(policy, false);
    }
}

fn rejects_invalid_plus(policy: ChannelBindingPolicy, wrong_binding: bool) {
    let server = Server::start_with(Some("sha384"), Some(policy));
    let mut tls = start_tls_auth(&server);
    let (tag, auth) = frame(&mut tls);
    assert_eq!(tag, b'R');
    assert_eq!(&auth[..4], &10_u32.to_be_bytes());
    assert!(auth[4..].starts_with(b"SCRAM-SHA-256-PLUS\0"));
    let bare = "n=app,r=test-client-nonce";
    let first = format!("p=tls-server-end-point,,{bare}");
    let mut initial = b"SCRAM-SHA-256-PLUS\0".to_vec();
    initial.extend_from_slice(&(first.len() as u32).to_be_bytes());
    initial.extend_from_slice(first.as_bytes());
    send(&mut tls, b'p', &initial);
    let (tag, auth) = frame(&mut tls);
    assert_eq!(tag, b'R');
    assert_eq!(&auth[..4], &11_u32.to_be_bytes());
    let server_first = std::str::from_utf8(&auth[4..]).unwrap();
    let salt = BASE64.decode(attr(server_first, "s=")).unwrap();
    let iterations = attr(server_first, "i=").parse().unwrap();
    let mut salted = [0; 32];
    pbkdf2_hmac::<Sha256>(b"test-password", &salt, iterations, &mut salted);
    let pem = std::fs::read(&server.cert).unwrap();
    let der = rustls_pemfile::certs(&mut &pem[..])
        .next()
        .unwrap()
        .unwrap();
    let mut wrong = Sha384::digest(der.as_ref()).to_vec();
    if wrong_binding {
        wrong[0] ^= 1;
    }
    let mut binding = b"p=tls-server-end-point,,".to_vec();
    binding.extend(wrong);
    let final_bare = format!(
        "c={},r={}",
        BASE64.encode(binding),
        attr(server_first, "r=")
    );
    let message = format!("{bare},{server_first},{final_bare}");
    let client_key = hmac(&salted, b"Client Key");
    let signature = hmac(&Sha256::digest(&client_key), message.as_bytes());
    let mut proof = client_key
        .iter()
        .zip(signature)
        .map(|(a, b)| a ^ b)
        .collect::<Vec<_>>();
    if !wrong_binding {
        proof[0] ^= 1;
    }
    send(
        &mut tls,
        b'p',
        format!("{final_bare},p={}", BASE64.encode(proof)).as_bytes(),
    );
    let (tag, error) = frame(&mut tls);
    assert_eq!(tag, b'E');
    assert!(String::from_utf8_lossy(&error).contains("28P01"));
    let mut after_failure = [0];
    match tls.read(&mut after_failure) {
        Ok(0) => {}
        Err(error)
            if !matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) => {}
        other => panic!("failed PLUS exchange did not close the connection: {other:?}"),
    }
    drop(tls);
}

fn start_tls_auth(server: &Server) -> openssl::ssl::SslStream<TcpStream> {
    let mut tcp = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    tcp.set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    tcp.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).unwrap();
    let mut response = [0];
    tcp.read_exact(&mut response).unwrap();
    assert_eq!(response, [b'S']);
    let mut tls = server
        .connector()
        .build()
        .connect("localhost", tcp)
        .unwrap();
    let startup = b"\0\x03\0\0user\0app\0database\0bicdb\0\0";
    tls.write_all(&((startup.len() + 4) as u32).to_be_bytes())
        .unwrap();
    tls.write_all(startup).unwrap();
    tls.flush().unwrap();
    tls
}

#[tokio::test]
async fn channel_binding_policy_accepts_only_the_selected_client_modes() {
    use ChannelBindingPolicy::{Disable, Prefer, Require};
    for (policy, client_policy, succeeds) in [
        (Require, ChannelBinding::Require, true),
        (Require, ChannelBinding::Disable, false),
        (Prefer, ChannelBinding::Require, true),
        (Prefer, ChannelBinding::Disable, true),
        (Disable, ChannelBinding::Disable, true),
        (Disable, ChannelBinding::Require, false),
    ] {
        let server = Server::start_with(Some("sha384"), Some(policy));
        let mut config = tokio_postgres::Config::new();
        config
            .host("localhost")
            .port(server.port)
            .user("app")
            .password("test-password")
            .dbname("bicdb")
            .ssl_mode(SslMode::Require)
            .channel_binding(client_policy)
            .connect_timeout(Duration::from_secs(10));
        let result = config
            .connect(MakeTlsConnector::new(server.connector().build()))
            .await;
        if !succeeds {
            assert!(result.is_err(), "{policy:?}: {client_policy:?}");
            continue;
        }
        let (client, connection) = result.unwrap();
        let connection = tokio::spawn(connection);
        assert_eq!(
            client
                .query_one("SELECT 1", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            1
        );
        drop(client);
        connection.await.unwrap().unwrap();
    }
    for policy in [Prefer, Disable] {
        let server = Server::start_with(None, Some(policy));
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(server.port)
            .user("app")
            .password("test-password")
            .dbname("bicdb")
            .ssl_mode(SslMode::Disable)
            .channel_binding(ChannelBinding::Disable)
            .connect_timeout(Duration::from_secs(10));
        let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let connection = tokio::spawn(connection);
        assert_eq!(
            client
                .query_one("SELECT 1", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            1
        );
        drop(client);
        connection.await.unwrap().unwrap();
    }
}

#[test]
fn policy_advertisements_are_exact_and_gs2_downgrades_are_rejected() {
    use ChannelBindingPolicy::{Disable, Prefer, Require};
    for (policy, expected) in [
        (Require, b"SCRAM-SHA-256-PLUS\0\0".as_slice()),
        (Prefer, b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0".as_slice()),
        (Disable, b"SCRAM-SHA-256\0\0".as_slice()),
    ] {
        let server = Server::start_with(Some("sha256"), Some(policy));
        let mut tls = start_tls_auth(&server);
        let (tag, auth) = frame(&mut tls);
        assert_eq!(tag, b'R');
        assert_eq!(&auth[..4], &10_u32.to_be_bytes());
        assert_eq!(&auth[4..], expected);
        // A client saying 'y' believes the server did not offer binding.
        // Require rejects plain SCRAM; Prefer detects the downgrade;
        // Disable legitimately permits this plain mechanism.
        let first = "y,,n=app,r=client-policy-nonce";
        let mut initial = b"SCRAM-SHA-256\0".to_vec();
        initial.extend_from_slice(&(first.len() as u32).to_be_bytes());
        initial.extend_from_slice(first.as_bytes());
        send(&mut tls, b'p', &initial);
        let (tag, auth) = frame(&mut tls);
        if policy == Disable {
            assert_eq!(tag, b'R');
            assert_eq!(&auth[..4], &11_u32.to_be_bytes());
        } else {
            assert_eq!(tag, b'E');
            assert!(String::from_utf8_lossy(&auth).contains("28P01"));
        }
        drop(tls);
    }
}

#[test]
fn require_binding_rejects_incompatible_startup_configuration() {
    let directory = tempfile::tempdir().unwrap();
    let config = PgWireConfig {
        require_auth: true,
        channel_binding: Some(ChannelBindingPolicy::Require),
        ..PgWireConfig::default()
    };
    let error = PgWireServer::open(directory.path(), config.clone()).unwrap_err();
    assert!(error
        .to_string()
        .contains("channel_binding=require needs both tls_cert and tls_key"));
    let config = PgWireConfig {
        tls_cert: Some(fixture("sha256.pem")),
        tls_key: Some(fixture("sha256.key")),
        ..config
    };
    let error = PgWireServer::open(
        directory.path(),
        PgWireConfig {
            require_auth: false,
            ..config.clone()
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("channel_binding=require needs require_auth"));
    let error = PgWireServer::open(
        directory.path(),
        PgWireConfig {
            auth_method: AuthMethod::Cleartext,
            ..config
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("channel_binding policy requires auth_method scram-sha-256"));
}

#[tokio::test]
async fn unsupported_binding_hash_requires_an_explicit_plain_scram_policy() {
    let directory = tempfile::tempdir().unwrap();
    for policy in [None, Some(ChannelBindingPolicy::Require)] {
        let config = PgWireConfig {
            require_auth: true,
            channel_binding: policy,
            tls_cert: Some(fixture("ed25519.pem")),
            tls_key: Some(fixture("ed25519.key")),
            ..PgWireConfig::default()
        };
        let error = PgWireServer::open(directory.path(), config).unwrap_err();
        assert!(error
            .to_string()
            .contains("no supported tls-server-end-point hash"));
    }
    for policy in [ChannelBindingPolicy::Prefer, ChannelBindingPolicy::Disable] {
        let server = Server::start_with(Some("ed25519"), Some(policy));
        let mut config = tokio_postgres::Config::new();
        config
            .host("localhost")
            .port(server.port)
            .user("app")
            .password("test-password")
            .dbname("bicdb")
            .ssl_mode(SslMode::Require)
            .channel_binding(ChannelBinding::Disable)
            .connect_timeout(Duration::from_secs(10));
        let (client, connection) = config
            .connect(MakeTlsConnector::new(server.connector().build()))
            .await
            .unwrap();
        let connection = tokio::spawn(connection);
        assert_eq!(
            client
                .query_one("SELECT 1", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            1
        );
        drop(client);
        connection.await.unwrap().unwrap();
    }
}
