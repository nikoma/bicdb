use std::fs::{self, File};
use std::io::{self, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rustls::client::ClientConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use sha2::{Digest, Sha256};

use crate::error::Result;
use crate::replication::{
    replication_error, CommitFrame, ReplicationConfig, ReplicationFrame, ReplicationTlsConfig,
};

const FRAME_HEADER_BYTES: usize = 4;
const SNAPSHOT_ARCHIVE_MAGIC: &[u8; 8] = b"BICRSNP1";
const SNAPSHOT_ARCHIVE_BUFFER: usize = 64 * 1024;

pub struct ReplicationServerTls {
    pub config: Arc<ServerConfig>,
}

pub struct ReplicationClientTls {
    pub config: Arc<ClientConfig>,
}

pub fn build_server_tls(tls: &ReplicationTlsConfig) -> Result<ReplicationServerTls> {
    let certs = load_certs(&tls.cert_path)?;
    let key = load_key(&tls.key_path)?;
    let mut roots = RootCertStore::empty();
    for cert in load_certs(&tls.ca_path)? {
        roots.add(cert).map_err(|error| {
            replication_error(format!("invalid replication CA certificate: {error}"))
        })?;
    }
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let verifier =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
            .build()
            .map_err(|error| {
                replication_error(format!("invalid replication client CA: {error}"))
            })?;
    // Select the provider on this config instead of relying on rustls'
    // process-wide auto-selection. A workspace binary may enable both
    // aws-lc-rs and ring through otherwise unrelated TLS clients.
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| {
            replication_error(format!(
                "invalid replication TLS protocol configuration: {error}"
            ))
        })?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|error| replication_error(format!("invalid replication TLS identity: {error}")))?;
    Ok(ReplicationServerTls {
        config: Arc::new(config),
    })
}

pub fn build_client_tls(tls: &ReplicationTlsConfig) -> Result<ReplicationClientTls> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(&tls.ca_path)? {
        roots.add(cert).map_err(|error| {
            replication_error(format!("invalid replication CA certificate: {error}"))
        })?;
    }
    let certs = load_certs(&tls.cert_path)?;
    let key = load_key(&tls.key_path)?;
    let config = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| {
        replication_error(format!(
            "invalid replication TLS protocol configuration: {error}"
        ))
    })?
    .with_root_certificates(roots)
    .with_client_auth_cert(certs, key)
    .map_err(|error| {
        replication_error(format!("invalid replication client TLS identity: {error}"))
    })?;
    Ok(ReplicationClientTls {
        config: Arc::new(config),
    })
}

/// SHA-256 of the leaf certificate's exact DER encoding. Cluster membership
/// binds this value to a node ID so a different CA-signed client cannot claim
/// that identity in an RPC envelope.
pub fn replication_certificate_sha256(path: impl AsRef<Path>) -> Result<String> {
    let certs = load_certs(path.as_ref())?;
    Ok(certificate_der_sha256(
        certs
            .first()
            .expect("load_certs rejects an empty certificate file")
            .as_ref(),
    ))
}

pub(crate) fn certificate_der_sha256(certificate_der: &[u8]) -> String {
    hex::encode(Sha256::digest(certificate_der))
}

pub fn validate_transport_config(config: &ReplicationConfig) -> Result<()> {
    config.validate()?;
    if !config.enabled {
        return Ok(());
    }
    if let Some(tls) = &config.tls {
        if !tls.dev_localhost_plaintext {
            let _ = build_server_tls(tls)?;
            let _ = build_client_tls(tls)?;
        }
    }
    Ok(())
}

pub fn serve_localhost_once<A, F>(addr: A, handler: F) -> Result<std::net::SocketAddr>
where
    A: ToSocketAddrs,
    F: FnOnce(TcpStream) -> Result<()> + Send + 'static,
{
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            let _ = handler(stream);
        }
    });
    Ok(local)
}

pub fn connect_localhost(addr: std::net::SocketAddr, timeout: Duration) -> Result<TcpStream> {
    if !addr.ip().is_loopback() {
        return Err(replication_error(
            "plaintext replication transport is localhost-only",
        ));
    }
    TcpStream::connect_timeout(&addr, timeout).map_err(Into::into)
}

pub fn send_frame<W: Write>(
    writer: &mut W,
    frame: &ReplicationFrame,
    max_frame_bytes: usize,
) -> Result<()> {
    let bytes = serde_json::to_vec(frame)?;
    if bytes.len() > max_frame_bytes {
        return Err(replication_error(format!(
            "replication frame exceeds max_frame_bytes: {} > {}",
            bytes.len(),
            max_frame_bytes
        )));
    }
    writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
    writer.write_all(&bytes)?;
    writer.flush()?;
    Ok(())
}

pub fn recv_frame<R: Read>(reader: &mut R, max_frame_bytes: usize) -> Result<ReplicationFrame> {
    let mut header = [0u8; FRAME_HEADER_BYTES];
    reader.read_exact(&mut header)?;
    let len = u32::from_be_bytes(header) as usize;
    if len > max_frame_bytes {
        return Err(replication_error(format!(
            "replication frame exceeds max_frame_bytes: {len} > {max_frame_bytes}"
        )));
    }
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn send_commit_batch<W: Write>(
    writer: &mut W,
    frames: &[CommitFrame],
    max_frame_bytes: usize,
) -> Result<()> {
    for frame in frames {
        send_frame(
            writer,
            &ReplicationFrame::Commit(frame.clone()),
            max_frame_bytes,
        )?;
    }
    Ok(())
}

pub fn snapshot_frames_from_reader<R: Read>(
    snapshot_id: &str,
    cluster_id: &str,
    source_node_id: &str,
    snapshot_commit_seq: u64,
    reader: &mut R,
    chunk_bytes: usize,
) -> Result<Vec<ReplicationFrame>> {
    if chunk_bytes == 0 {
        return Err(replication_error(
            "snapshot chunk_bytes must be greater than zero",
        ));
    }
    let mut frames = vec![ReplicationFrame::SnapshotStart {
        cluster_id: cluster_id.to_string(),
        source_node_id: source_node_id.to_string(),
        snapshot_id: snapshot_id.to_string(),
        snapshot_commit_seq,
    }];
    let mut offset = 0u64;
    let mut snapshot_hash = Sha256::new();
    let mut buffer = vec![0u8; chunk_bytes];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let bytes = buffer[..read].to_vec();
        snapshot_hash.update(&bytes);
        let checksum: [u8; 32] = Sha256::digest(&bytes).into();
        frames.push(ReplicationFrame::SnapshotChunk {
            snapshot_id: snapshot_id.to_string(),
            offset,
            bytes,
            checksum,
        });
        offset = offset.saturating_add(read as u64);
    }
    frames.push(ReplicationFrame::SnapshotEnd {
        snapshot_id: snapshot_id.to_string(),
        snapshot_commit_seq,
        snapshot_hash: snapshot_hash.finalize().into(),
    });
    Ok(frames)
}

pub fn restore_snapshot_frames_to_writer<W: Write>(
    frames: &[ReplicationFrame],
    writer: &mut W,
) -> Result<u64> {
    let Some(ReplicationFrame::SnapshotStart { snapshot_id, .. }) = frames.first() else {
        return Err(replication_error("snapshot restore missing SnapshotStart"));
    };
    let mut expected_offset = 0u64;
    let mut snapshot_hash = Sha256::new();
    let mut saw_end = false;
    for frame in &frames[1..] {
        match frame {
            ReplicationFrame::SnapshotChunk {
                snapshot_id: chunk_snapshot_id,
                offset,
                bytes,
                checksum,
            } => {
                if chunk_snapshot_id != snapshot_id {
                    return Err(replication_error("snapshot chunk id mismatch"));
                }
                if *offset != expected_offset {
                    return Err(replication_error(format!(
                        "snapshot chunk offset mismatch: expected {expected_offset}, got {offset}"
                    )));
                }
                let actual: [u8; 32] = Sha256::digest(bytes).into();
                if &actual != checksum {
                    return Err(replication_error("snapshot chunk checksum mismatch"));
                }
                writer.write_all(bytes)?;
                snapshot_hash.update(bytes);
                expected_offset = expected_offset.saturating_add(bytes.len() as u64);
            }
            ReplicationFrame::SnapshotEnd {
                snapshot_id: end_snapshot_id,
                snapshot_hash: expected_hash,
                ..
            } => {
                if end_snapshot_id != snapshot_id {
                    return Err(replication_error("snapshot end id mismatch"));
                }
                let actual: [u8; 32] = snapshot_hash.finalize().into();
                if &actual != expected_hash {
                    return Err(replication_error("snapshot hash mismatch"));
                }
                saw_end = true;
                break;
            }
            _ => return Err(replication_error("unexpected frame in snapshot restore")),
        }
    }
    if !saw_end {
        return Err(replication_error("snapshot restore missing SnapshotEnd"));
    }
    writer.flush()?;
    Ok(expected_offset)
}

pub fn write_snapshot_archive(root: &Path, writer: &mut impl Write) -> Result<()> {
    writer.write_all(SNAPSHOT_ARCHIVE_MAGIC)?;
    let mut files = Vec::new();
    collect_snapshot_files(root, root, &mut files)?;
    files.sort();
    let mut buffer = vec![0u8; SNAPSHOT_ARCHIVE_BUFFER];
    for relative in files {
        let path = root.join(&relative);
        let metadata = fs::metadata(&path)?;
        if !metadata.is_file() {
            continue;
        }
        let relative_bytes = relative.to_string_lossy().as_bytes().to_vec();
        writer.write_all(&(relative_bytes.len() as u32).to_be_bytes())?;
        writer.write_all(&metadata.len().to_be_bytes())?;
        writer.write_all(&relative_bytes)?;
        let mut file = File::open(&path)?;
        let mut remaining = metadata.len();
        while remaining > 0 {
            let max_read = buffer.len().min(remaining as usize);
            let read = file.read(&mut buffer[..max_read])?;
            if read == 0 {
                return Err(replication_error(format!(
                    "snapshot file {} ended before declared length",
                    path.display()
                )));
            }
            writer.write_all(&buffer[..read])?;
            remaining -= read as u64;
        }
    }
    writer.write_all(&0u32.to_be_bytes())?;
    writer.flush()?;
    Ok(())
}

pub fn restore_snapshot_archive_atomic(
    archive: &mut impl Read,
    target: &Path,
    force: bool,
) -> Result<()> {
    let staging = target.with_extension("replication-restore.tmp");
    match fs::remove_dir_all(&staging) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::create_dir_all(&staging)?;
    let result = restore_snapshot_archive_to_dir(archive, &staging).and_then(|_| {
        if target.exists() {
            if !force {
                return Err(replication_error(format!(
                    "snapshot restore target {} already exists; pass force to replace it",
                    target.display()
                )));
            }
            fs::remove_dir_all(target)?;
        }
        fs::rename(&staging, target)?;
        Ok(())
    });
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result
}

fn restore_snapshot_archive_to_dir(archive: &mut impl Read, target: &Path) -> Result<()> {
    let mut magic = [0u8; 8];
    archive.read_exact(&mut magic)?;
    if magic != *SNAPSHOT_ARCHIVE_MAGIC {
        return Err(replication_error("invalid replication snapshot archive"));
    }
    let mut buffer = vec![0u8; SNAPSHOT_ARCHIVE_BUFFER];
    loop {
        let mut path_len_bytes = [0u8; 4];
        archive.read_exact(&mut path_len_bytes)?;
        let path_len = u32::from_be_bytes(path_len_bytes) as usize;
        if path_len == 0 {
            break;
        }
        let mut file_len_bytes = [0u8; 8];
        archive.read_exact(&mut file_len_bytes)?;
        let mut relative_bytes = vec![0u8; path_len];
        archive.read_exact(&mut relative_bytes)?;
        let relative = snapshot_relative_path(&relative_bytes)?;
        let output_path = target.join(relative);
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut output = File::create(&output_path)?;
        let mut remaining = u64::from_be_bytes(file_len_bytes);
        while remaining > 0 {
            let max_read = buffer.len().min(remaining as usize);
            archive.read_exact(&mut buffer[..max_read])?;
            output.write_all(&buffer[..max_read])?;
            remaining -= max_read as u64;
        }
        output.sync_all()?;
    }
    Ok(())
}

fn collect_snapshot_files(root: &Path, dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_snapshot_files(root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| replication_error(format!("snapshot path error: {error}")))?;
            files.push(relative.to_path_buf());
        }
    }
    Ok(())
}

fn snapshot_relative_path(bytes: &[u8]) -> Result<PathBuf> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| replication_error("snapshot archive path is not valid UTF-8"))?;
    let path = PathBuf::from(text);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(replication_error("snapshot archive path escapes target"));
    }
    Ok(path)
}

/// SHA-256 of the leaf certificate a connected replication peer presented.
///
/// The mirror of the cluster transport's binding
/// (`distribution_transport.rs`): mTLS proves the peer holds SOME key the CA
/// signed, never WHICH node it is. Without pinning this fingerprint to the
/// node identity, any certificate inside the CA can claim any node ID.
/// Completes the handshake first, since `StreamOwned` defers it to the
/// first read.
pub fn peer_certificate_sha256(
    stream: &mut StreamOwned<ServerConnection, TcpStream>,
) -> Result<String> {
    while stream.conn.is_handshaking() {
        stream.conn.complete_io(&mut stream.sock).map_err(|error| {
            replication_error(format!("replication TLS handshake failed: {error}"))
        })?;
    }
    stream
        .conn
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .map(|certificate| certificate_der_sha256(certificate.as_ref()))
        .ok_or_else(|| replication_error("replication peer supplied no leaf certificate"))
}

pub fn server_tls_stream(
    config: Arc<ServerConfig>,
    stream: TcpStream,
) -> Result<StreamOwned<ServerConnection, TcpStream>> {
    let connection = ServerConnection::new(config)
        .map_err(|error| replication_error(format!("replication TLS accept failed: {error}")))?;
    Ok(StreamOwned::new(connection, stream))
}

pub fn client_tls_stream(
    config: Arc<ClientConfig>,
    server_name: &str,
    stream: TcpStream,
) -> Result<StreamOwned<ClientConnection, TcpStream>> {
    let server_name = ServerName::try_from(server_name.to_string())
        .map_err(|error| replication_error(format!("invalid replication server name: {error}")))?;
    let connection = ClientConnection::new(config, server_name)
        .map_err(|error| replication_error(format!("replication TLS connect failed: {error}")))?;
    Ok(StreamOwned::new(connection, stream))
}

fn load_certs(path: &std::path::Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let certs =
        rustls_pemfile::certs(&mut reader).collect::<std::result::Result<Vec<_>, io::Error>>()?;
    if certs.is_empty() {
        return Err(replication_error(format!(
            "replication certificate file {} contained no certificates",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_key(path: &std::path::Path) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| replication_error("replication key file contained no private key"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::RequestVote;
    use crate::replication::{CommitFrame, DEFAULT_REPLICATION_STREAM_ID};

    #[test]
    fn localhost_plaintext_rejects_non_loopback_address() {
        let addr = "192.0.2.1:9443".parse().unwrap();
        assert!(connect_localhost(addr, Duration::from_millis(1)).is_err());
    }

    #[test]
    fn frame_codec_roundtrips_commit_frame() {
        let commit = CommitFrame::new(
            "cluster",
            "node-a",
            DEFAULT_REPLICATION_STREAM_ID,
            1,
            9,
            123,
            Vec::new(),
        );
        let mut bytes = Vec::new();
        send_frame(
            &mut bytes,
            &ReplicationFrame::Commit(commit.clone()),
            1024 * 1024,
        )
        .unwrap();
        let decoded = recv_frame(&mut &bytes[..], 1024 * 1024).unwrap();
        assert_eq!(decoded, ReplicationFrame::Commit(commit));
    }

    #[test]
    fn frame_codec_roundtrips_consensus_vote_request() {
        let request = RequestVote {
            cluster_id: "cluster".to_string(),
            term: 3,
            candidate_id: "node-a".to_string(),
            last_log_index: 7,
            last_log_term: 2,
        };
        let mut bytes = Vec::new();
        send_frame(
            &mut bytes,
            &ReplicationFrame::ConsensusRequestVote(request.clone()),
            1024 * 1024,
        )
        .unwrap();
        let decoded = recv_frame(&mut &bytes[..], 1024 * 1024).unwrap();
        assert_eq!(decoded, ReplicationFrame::ConsensusRequestVote(request));
    }

    #[test]
    fn frame_codec_rejects_oversize_payload() {
        let commit = CommitFrame::new("cluster", "node-a", "s", 1, 9, 123, Vec::new());
        let mut bytes = Vec::new();
        assert!(send_frame(&mut bytes, &ReplicationFrame::Commit(commit), 4).is_err());
    }

    #[test]
    fn snapshot_frames_roundtrip_and_reject_corruption() {
        let input = b"abcdefghijklmnopqrstuvwxyz0123456789".to_vec();
        let frames =
            snapshot_frames_from_reader("snap-1", "cluster", "node-a", 7, &mut &input[..], 8)
                .unwrap();
        let mut restored = Vec::new();
        let bytes = restore_snapshot_frames_to_writer(&frames, &mut restored).unwrap();
        assert_eq!(bytes, input.len() as u64);
        assert_eq!(restored, input);

        let mut corrupted = frames.clone();
        let chunk = corrupted
            .iter_mut()
            .find_map(|frame| match frame {
                ReplicationFrame::SnapshotChunk { bytes, .. } => Some(bytes),
                _ => None,
            })
            .unwrap();
        chunk[0] ^= 0x01;
        assert!(restore_snapshot_frames_to_writer(&corrupted, &mut Vec::new()).is_err());
    }

    #[test]
    fn snapshot_archive_restores_atomically_and_rejects_corruption() {
        let source = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let target_path = target.path().join("db");
        std::fs::create_dir_all(source.path().join("nested")).unwrap();
        std::fs::write(source.path().join("root.txt"), b"root").unwrap();
        std::fs::write(source.path().join("nested").join("child.txt"), b"child").unwrap();

        let mut archive = Vec::new();
        write_snapshot_archive(source.path(), &mut archive).unwrap();
        restore_snapshot_archive_atomic(&mut &archive[..], &target_path, false).unwrap();
        assert_eq!(
            std::fs::read(target_path.join("root.txt")).unwrap(),
            b"root"
        );
        assert_eq!(
            std::fs::read(target_path.join("nested").join("child.txt")).unwrap(),
            b"child"
        );

        let mut corrupted = archive.clone();
        corrupted[0] ^= 0x01;
        assert!(restore_snapshot_archive_atomic(&mut &corrupted[..], &target_path, true).is_err());
        assert_eq!(
            std::fs::read(target_path.join("nested").join("child.txt")).unwrap(),
            b"child"
        );
    }
}
