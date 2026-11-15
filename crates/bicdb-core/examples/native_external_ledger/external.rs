//! Example-only pinned mTLS sink. Never supplies database replies/authority.
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const MAX_HEADER: usize = 65_536;
const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    address: SocketAddr, // Numeric only: no DNS outside the absolute IO window.
    server_name: String,
    server_certificate_sha256: String,
    ca_path: PathBuf,
    cert_path: PathBuf,
    key_path: PathBuf,
    producer_id: Uuid,
    session_id: Uuid,
    io_timeout_ms: u64,
    key_free_inputs_reviewed: bool,
}

#[path = "receipt.rs"]
mod receipt;
use receipt::{decode_receipt, Receipt};

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn hash(raw: &[u8]) -> String {
    hex::encode(Sha256::digest(raw))
}
fn check_end(end: Instant) -> io::Result<Duration> {
    end.checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "original ledger IO deadline expired",
            )
        })
}
fn bounded(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    let mut raw = Vec::new();
    File::open(path)?
        .take(limit as u64 + 1)
        .read_to_end(&mut raw)?;
    if raw.len() > limit {
        return Err(invalid("ledger configuration/certificate byte bound"));
    }
    Ok(raw)
}
fn certs(path: &Path) -> io::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let raw = bounded(path, 1024 * 1024)?;
    let result = rustls_pemfile::certs(&mut BufReader::new(raw.as_slice()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if result.is_empty() {
        return Err(invalid("empty ledger certificate chain"));
    }
    Ok(result)
}

struct Socket {
    tcp: TcpStream,
    end: Instant,
}
impl Read for Socket {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.tcp.set_read_timeout(Some(check_end(self.end)?))?;
        self.tcp.read(buffer)
    }
}
impl Write for Socket {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.tcp.set_write_timeout(Some(check_end(self.end)?))?;
        self.tcp.write(buffer)
    }
    fn flush(&mut self) -> io::Result<()> {
        check_end(self.end)?;
        self.tcp.flush()
    }
}

pub struct External {
    config: Config,
    tls: Arc<ClientConfig>,
    client_sha256: String,
    run: Uuid,
    session: Uuid,
    receipts: File,
    receipt_sequence: u64,
    offset: u64,
    failed: bool,
}

impl External {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        config_path: &Path,
        output: &Path,
        run: Uuid,
        member: &str,
        member_tls: &bicdb_core::ReplicationTlsConfig,
        source: &str,
        binary: &str,
        inputs: &[(&str, &[u8])],
    ) -> io::Result<Self> {
        let config_raw = bounded(config_path, MAX_HEADER)?;
        let config: Config = serde_json::from_slice(&config_raw)?;
        if !(1..=30_000).contains(&config.io_timeout_ms)
            || !config.key_free_inputs_reviewed
            || config.producer_id.is_nil()
            || config.session_id.is_nil()
            || config.server_certificate_sha256.len() != 64
            || !config
                .server_certificate_sha256
                .bytes()
                .all(|c| c.is_ascii_hexdigit())
        {
            return Err(invalid("invalid ledger identity/bounds/input review"));
        }
        if std::fs::canonicalize(&config.key_path)? == std::fs::canonicalize(&member_tls.key_path)?
        {
            return Err(invalid(
                "ledger TLS key must be distinct from the DB member key",
            ));
        }
        let client_certs = certs(&config.cert_path)?;
        let client_sha256 = hash(client_certs[0].as_ref());
        let member_sha256 = hash(certs(&member_tls.cert_path)?[0].as_ref());
        if member_sha256 == client_sha256 || member_sha256 == config.server_certificate_sha256 {
            return Err(invalid(
                "ledger TLS identity must be distinct from DB membership",
            ));
        }
        let key_raw = bounded(&config.key_path, 1024 * 1024)?;
        let key = rustls_pemfile::private_key(&mut BufReader::new(key_raw.as_slice()))?
            .ok_or_else(|| invalid("missing ledger private key"))?;
        let mut roots = RootCertStore::empty();
        for cert in certs(&config.ca_path)? {
            roots.add(cert).map_err(io::Error::other)?;
        }
        let tls = ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?
        .with_root_certificates(roots)
        .with_client_auth_cert(client_certs, key)
        .map_err(io::Error::other)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let receipts = options.open(output.join("remote-receipts.jsonl"))?;
        receipts.sync_all()?;
        let mut input_options = OpenOptions::new();
        input_options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            input_options.mode(0o600);
        }
        let mut input_file = input_options.open(output.join("ledger-config.json"))?;
        input_file.write_all(&config_raw)?;
        input_file.sync_all()?;
        File::open(output)?.sync_all()?;
        let session = config.session_id;
        let mut this = Self {
            config,
            tls: Arc::new(tls),
            client_sha256,
            run,
            session,
            receipts,
            receipt_sequence: 0,
            offset: 0,
            failed: false,
        };
        let mut original_inputs = inputs.to_vec();
        original_inputs.push(("ledger-config.json", config_raw.as_slice()));
        let input_manifest: BTreeMap<_, _> = original_inputs
            .iter()
            .map(|(name, raw)| (*name, json!({"bytes":raw.len(),"sha256":hash(raw)})))
            .collect();
        let manifest = serde_json::to_vec(&json!({
            "format":1,"run_id":run,"producer_id":this.config.producer_id,
            "session_id":this.session,"inputs":input_manifest,"source_sha":source,
            "binary_sha256":binary,"member_node_id":member,"member_certificate_sha256":member_sha256,
            "ledger_client_certificate_sha256":this.client_sha256,
            "collector_certificate_sha256":this.config.server_certificate_sha256,
            "qualification_status":"not_qualified","throughput_benchmark":false,
        }))?;
        let end = this.deadline();
        this.send("start", json!({}), &manifest, end)?;
        for (name, raw) in original_inputs {
            // Explicit operator review is required too; this only catches PEM accidents.
            if raw
                .windows(b"PRIVATE KEY-----".len())
                .any(|part| part == b"PRIVATE KEY-----")
            {
                return Err(invalid(
                    "private-key bytes forbidden in external input custody",
                ));
            }
            let end = this.deadline();
            this.send("input", json!({"name":name}), raw, end)?;
        }
        Ok(this)
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.config.io_timeout_ms)
    }
    fn deadline(&self) -> Instant {
        Instant::now() + self.timeout()
    }
    pub fn description(&self) -> Value {
        json!({"mode":"external_fsync_required","run_id":self.run,
          "producer_id":self.config.producer_id,"session_id":self.session,
          "collector_certificate_sha256":self.config.server_certificate_sha256,
          "ledger_client_certificate_sha256":self.client_sha256,
          "io_timeout_ms":self.config.io_timeout_ms,"throughput_benchmark":false})
    }
    pub fn row(
        &mut self,
        raw: &[u8],
        sequence: u64,
        previous: &Option<String>,
        end: Instant,
    ) -> io::Result<()> {
        let offset = self
            .offset
            .checked_add(raw.len() as u64)
            .ok_or_else(|| invalid("ledger offset overflow"))?;
        let receipt = self.send(
            "row",
            json!({"sequence":sequence,
                    "previous_record_sha256":previous}),
            raw,
            end,
        )?;
        if receipt.sequence != Some(sequence)
            || receipt.record_sha256.as_deref() != Some(hash(&raw[..raw.len() - 1]).as_str())
            || receipt.previous_record_sha256 != *previous
            || receipt.durable_end_offset != Some(offset)
        {
            self.failed = true;
            return Err(invalid(
                "external row receipt offset/hash mismatch; producer fenced",
            ));
        }
        self.offset = offset;
        Ok(())
    }

    fn send(
        &mut self,
        action: &str,
        extra: Value,
        payload: &[u8],
        end: Instant,
    ) -> io::Result<Receipt> {
        if self.failed {
            return Err(invalid("external ledger fenced after ambiguity"));
        }
        let result = (|| {
            check_end(end)?;
            if payload.len() > MAX_PAYLOAD {
                return Err(invalid("external payload bound; no truncation"));
            }
            let mut header = json!({"format":1,"action":action,"run_id":self.run,
              "producer_id":self.config.producer_id,"session_id":self.session,
              "payload_sha256":hash(payload),"payload_bytes":payload.len()});
            header
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let raw_header = serde_json::to_vec(&header)?;
            if raw_header.len() > MAX_HEADER {
                return Err(invalid("external header bound"));
            }
            let tcp = TcpStream::connect_timeout(&self.config.address, check_end(end)?)?;
            let name =
                ServerName::try_from(self.config.server_name.clone()).map_err(io::Error::other)?;
            let connection =
                ClientConnection::new(self.tls.clone(), name).map_err(io::Error::other)?;
            let mut stream = StreamOwned::new(connection, Socket { tcp, end });
            while stream.conn.is_handshaking() {
                stream.conn.complete_io(&mut stream.sock)?;
            }
            let actual = stream
                .conn
                .peer_certificates()
                .and_then(|chain| chain.first())
                .ok_or_else(|| invalid("missing collector TLS identity"))?;
            if hash(actual.as_ref()) != self.config.server_certificate_sha256 {
                return Err(invalid("collector leaf fingerprint changed"));
            }
            check_end(end)?;
            stream.write_all(&(raw_header.len() as u32).to_be_bytes())?;
            stream.write_all(&raw_header)?;
            stream.write_all(&(payload.len() as u64).to_be_bytes())?;
            stream.write_all(payload)?;
            stream.flush()?;
            let mut length = [0; 4];
            stream.read_exact(&mut length)?;
            let length = u32::from_be_bytes(length) as usize;
            if length == 0 || length > MAX_HEADER {
                return Err(invalid("collector receipt bound"));
            }
            let mut raw = vec![0; length];
            stream.read_exact(&mut raw)?;
            check_end(end)?;
            let receipt = decode_receipt(&raw, action)?;
            if receipt.format != 1 || receipt.action != action || receipt.run_id != self.run
                || receipt.producer_id != self.config.producer_id || receipt.session_id != self.session
                || receipt.payload_sha256 != hash(payload) || receipt.payload_bytes != payload.len()
                || receipt.collector_certificate_sha256 != self.config.server_certificate_sha256
                || receipt.client_certificate_sha256 != self.client_sha256
                // Controller witnesses may interleave in the collector journal.
                || receipt.receipt_sequence <= self.receipt_sequence
                || receipt.name.as_deref() != header.get("name").and_then(Value::as_str)
            {
                return Err(invalid("collector receipt identity/hash/order mismatch"));
            }
            if action != "row"
                && (receipt.sequence.is_some()
                    || receipt.record_sha256.is_some()
                    || receipt.previous_record_sha256.is_some()
                    || receipt.durable_end_offset.is_some())
            {
                return Err(invalid("unexpected row fields in collector receipt"));
            }
            check_end(end)?;
            self.receipts.write_all(&raw)?;
            self.receipts.write_all(b"\n")?;
            self.receipts.sync_all()?;
            check_end(end)?; // A local fsync stall cannot become a late successful callback.
            self.receipt_sequence = receipt.receipt_sequence;
            Ok(receipt)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}
