//! Streaming replication and synchronous quorum commit for `bicdb serve`.
//!
//! A primary started with an HA configuration listens for standbys. Each
//! standby holds one long-lived connection: it sends `Hello` and the last
//! commit sequence number it applied, the primary streams every newer commit
//! (in order, gapless) followed by a `Heartbeat` that ends the batch, and the
//! standby answers with an `Ack` once the batch is durable on its side. The
//! primary records those acknowledgements in the database's [`SyncQuorum`],
//! where client commits wait when synchronous standbys are configured.
//!
//! A standby runs the follower loop inside its own server, so it keeps serving
//! read-only queries while it applies the stream.
//!
//! Standbys authenticate with mutual TLS: the claimed node id must be on the
//! allow-list and, when pinned, match the presented certificate. Plaintext is
//! accepted only for loopback development configurations.
use super::*;
use bicdb_core::replication::{ReplicationFrame, REPLICATION_PROTOCOL_VERSION};
use bicdb_core::{NodeRole, ReplicationCursor, SyncTimeoutPolicy};
use std::net::TcpStream;

/// Frames per round sent to one standby.
const BATCH_COMMITS: usize = 512;
/// Idle interval after which the primary sends a keepalive batch.
const KEEPALIVE: Duration = Duration::from_secs(1);
const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;

/// A standby allowed to connect, optionally bound to its certificate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllowedStandby {
    pub node_id: String,
    /// SHA-256 of the standby's leaf certificate (hex). Required with TLS.
    pub certificate_sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum HaServerRole {
    Primary {
        /// Replication listen address (host:port).
        listen: String,
        allowed_standbys: Vec<AllowedStandby>,
        /// Standbys that must confirm each commit before it is acknowledged
        /// (0 = asynchronous replication).
        synchronous_standbys: usize,
        /// How long a commit waits for its confirmations (zero = forever).
        synchronous_timeout: Duration,
        on_timeout: SyncTimeoutPolicy,
    },
    Standby {
        /// The primary's replication address (host:port).
        primary: String,
        /// TLS server name of the primary's certificate.
        primary_server_name: String,
    },
}

/// Streaming replication settings for a server.
#[derive(Clone, Debug, PartialEq)]
pub struct HaServerConfig {
    /// Cluster id, this node's id, and TLS material.
    pub replication: ReplicationConfig,
    pub role: HaServerRole,
    /// Address clients use to reach this node's SQL port; a primary announces
    /// it so standbys can point refused writes at it.
    pub advertise_sql: Option<String>,
}

impl HaServerConfig {
    /// The database-level replication configuration for this role: a primary
    /// logs full row images so frames can be built, a standby refuses
    /// ordinary writes.
    pub(crate) fn database_replication(&self) -> ReplicationConfig {
        let mut replication = self.replication.clone();
        replication.enabled = true;
        replication.mode = match self.role {
            HaServerRole::Primary { .. } => ReplicationMode::Primary,
            HaServerRole::Standby { .. } => ReplicationMode::Standby,
        };
        // The address replication traffic uses: validation allows plaintext
        // only when it is a loopback address.
        replication.listen_addr = Some(match &self.role {
            HaServerRole::Primary { listen, .. } => listen.clone(),
            HaServerRole::Standby { primary, .. } => primary.clone(),
        });
        replication
    }

    fn plaintext(&self) -> bool {
        self.replication
            .tls
            .as_ref()
            .is_none_or(|tls| tls.dev_localhost_plaintext)
    }
}

/// Starts the replication service for `server`'s HA role.
pub(crate) fn start(server: Arc<PgWireServer>, config: HaServerConfig) -> Result<()> {
    config
        .database_replication()
        .validate()
        .map_err(PgWireError::from)?;
    match &config.role {
        HaServerRole::Primary {
            listen,
            allowed_standbys,
            synchronous_standbys,
            synchronous_timeout,
            on_timeout,
        } => {
            if *synchronous_standbys > 0 && config.role_allowed_count() < *synchronous_standbys {
                return Err(PgWireError::Server(format!(
                    "{synchronous_standbys} synchronous standbys required but only {} are allowed to connect",
                    config.role_allowed_count()
                )));
            }
            server
                .sync_quorum
                .configure(*synchronous_standbys, *synchronous_timeout, *on_timeout);
            {
                let db = server.read_db()?;
                // Standbys removed from the allow-list stop pinning the log.
                let allowed = allowed_standbys
                    .iter()
                    .map(|standby| standby.node_id.clone())
                    .collect::<Vec<_>>();
                db.retain_standby_slots(&allowed)
                    .map_err(PgWireError::from)?;
                if db.replication_role() == NodeRole::Primary {
                    db.record_primary_sql_address(config.advertise_sql.clone())
                        .map_err(PgWireError::from)?;
                }
            }
            let listener = std::net::TcpListener::bind(listen)?;
            let tls = if config.plaintext() {
                None
            } else {
                let tls = config.replication.tls.as_ref().ok_or_else(|| {
                    PgWireError::Server("replication TLS configuration missing".to_string())
                })?;
                Some(
                    replication_transport::build_server_tls(tls)
                        .map_err(PgWireError::from)?
                        .config,
                )
            };
            std::thread::Builder::new()
                .name("bicdb-replication-listener".to_string())
                .spawn(move || accept_standbys(server, config, listener, tls))?;
        }
        HaServerRole::Standby { .. } => {
            std::thread::Builder::new()
                .name("bicdb-replication-follower".to_string())
                .spawn(move || follow_primary(server, config))?;
        }
    }
    Ok(())
}

impl HaServerConfig {
    fn role_allowed_count(&self) -> usize {
        match &self.role {
            HaServerRole::Primary {
                allowed_standbys, ..
            } => allowed_standbys.len(),
            HaServerRole::Standby { .. } => 0,
        }
    }
}

trait ReplicationStream: std::io::Read + std::io::Write + Send {}
impl<T: std::io::Read + std::io::Write + Send> ReplicationStream for T {}

fn accept_standbys(
    server: Arc<PgWireServer>,
    config: HaServerConfig,
    listener: std::net::TcpListener,
    tls: Option<Arc<rustls::ServerConfig>>,
) {
    for connection in listener.incoming() {
        if server.is_shutdown_requested() {
            return;
        }
        let Ok(socket) = connection else {
            continue;
        };
        let _ = socket.set_nodelay(true);
        let server = Arc::clone(&server);
        let config = config.clone();
        let tls = tls.clone();
        let _ = std::thread::Builder::new()
            .name("bicdb-replication-sender".to_string())
            .spawn(move || {
                let peer = socket.peer_addr().ok();
                if let Err(error) = serve_standby(&server, &config, socket, tls) {
                    eprintln!("bicdb replication: standby {peer:?} disconnected: {error}");
                }
            });
    }
}

fn send(mut stream: &mut dyn ReplicationStream, frame: &ReplicationFrame) -> Result<()> {
    replication_transport::send_frame(&mut stream, frame, MAX_FRAME_BYTES)
        .map_err(PgWireError::from)
}

fn receive(mut stream: &mut dyn ReplicationStream) -> Result<ReplicationFrame> {
    replication_transport::recv_frame(&mut stream, MAX_FRAME_BYTES).map_err(PgWireError::from)
}

fn refuse(stream: &mut dyn ReplicationStream, message: &str) -> PgWireError {
    let _ = send(
        stream,
        &ReplicationFrame::Error {
            code: "replication_refused".to_string(),
            message: message.to_string(),
        },
    );
    PgWireError::Server(format!("replication refused: {message}"))
}

/// Authenticates one standby and streams to it until the connection ends.
fn serve_standby(
    server: &Arc<PgWireServer>,
    config: &HaServerConfig,
    socket: TcpStream,
    tls: Option<Arc<rustls::ServerConfig>>,
) -> Result<()> {
    let HaServerRole::Primary {
        allowed_standbys, ..
    } = &config.role
    else {
        return Ok(());
    };
    let (mut stream, presented): (Box<dyn ReplicationStream>, Option<String>) = match tls {
        Some(tls) => {
            let mut stream =
                replication_transport::server_tls_stream(tls, socket).map_err(PgWireError::from)?;
            let presented = replication_transport::peer_certificate_sha256(&mut stream).ok();
            (Box::new(stream), presented)
        }
        None => {
            let local = socket.local_addr()?.ip();
            if !local.is_loopback() {
                return Err(PgWireError::Server(
                    "plaintext replication is accepted only on loopback".to_string(),
                ));
            }
            (Box::new(socket), None)
        }
    };
    let stream = stream.as_mut();

    let standby = match receive(stream)? {
        ReplicationFrame::Hello {
            protocol_version,
            cluster_id,
            node_id,
            epoch,
        } => {
            if protocol_version != REPLICATION_PROTOCOL_VERSION {
                return Err(refuse(stream, "unsupported replication protocol version"));
            }
            if cluster_id != config.replication.cluster_id {
                return Err(refuse(stream, "cluster_id mismatch"));
            }
            // Fail closed: an empty allow-list authorizes nobody.
            let Some(allowed) = allowed_standbys
                .iter()
                .find(|allowed| allowed.node_id == node_id)
            else {
                return Err(refuse(stream, "node is not allowed"));
            };
            match (&allowed.certificate_sha256, presented.as_deref()) {
                (Some(expected), Some(presented)) if expected == presented => {}
                (None, None) => {}
                (Some(_), _) => {
                    return Err(refuse(stream, "certificate does not match node identity"));
                }
                (None, Some(_)) => {
                    return Err(refuse(
                        stream,
                        "node identity is not pinned to a certificate",
                    ));
                }
            }
            superseded_check(server, stream, epoch)?;
            node_id
        }
        _ => return Err(refuse(stream, "expected Hello")),
    };
    let applied = match receive(stream)? {
        ReplicationFrame::Ack { commit_seq, .. } => commit_seq,
        _ => return Err(refuse(stream, "expected the standby's applied position")),
    };
    // The standby's position is durable on its side.
    server.sync_quorum.record_ack(&standby, applied);
    // Persist at once: a primary restart must keep retaining this standby's log.
    server
        .read_db()?
        .persist_standby_slots()
        .map_err(PgWireError::from)?;
    eprintln!("bicdb replication: standby {standby} connected at commit {applied}");

    let mut cursor = ReplicationCursor::new(applied);
    loop {
        if server.is_shutdown_requested() {
            return Ok(());
        }
        let (frames, epoch) = {
            let db = server.read_db()?;
            if db.replication_role() != NodeRole::Primary {
                return Err(refuse(stream, "this node is no longer the primary"));
            }
            let frames = match db.replication_frames_after(&mut cursor, BATCH_COMMITS) {
                Ok(frames) => frames,
                Err(error) => return Err(refuse(stream, &error.to_string())),
            };
            (frames, db.replication_epoch())
        };
        if frames.is_empty() {
            let durable = server
                .replication_feed
                .wait_beyond(cursor.sent_seq(), KEEPALIVE);
            if durable > cursor.sent_seq() {
                continue;
            }
        }
        for frame in frames {
            send(stream, &ReplicationFrame::Commit(frame))?;
        }
        send(
            stream,
            &ReplicationFrame::Heartbeat {
                cluster_id: config.replication.cluster_id.clone(),
                node_id: config.replication.node_id.clone(),
                last_commit_seq: cursor.sent_seq(),
                epoch,
                primary_sql_address: config.advertise_sql.clone(),
            },
        )?;
        match receive(stream)? {
            ReplicationFrame::Ack {
                commit_seq, epoch, ..
            } => {
                superseded_check(server, stream, epoch)?;
                server.sync_quorum.record_ack(&standby, commit_seq);
            }
            ReplicationFrame::Error { message, .. } => {
                return Err(PgWireError::Server(format!(
                    "standby {standby} reported: {message}"
                )));
            }
            other => {
                return Err(PgWireError::Server(format!(
                    "standby {standby} sent an unexpected frame: {other:?}"
                )));
            }
        }
    }
}

/// Fences this primary when a peer reports a newer epoch: another node was
/// promoted, so this one must stop acknowledging writes.
fn superseded_check(
    server: &Arc<PgWireServer>,
    stream: &mut dyn ReplicationStream,
    epoch: u64,
) -> Result<()> {
    if server
        .read_db()?
        .observe_replication_epoch(epoch)
        .map_err(PgWireError::from)?
    {
        return Err(refuse(
            stream,
            &format!("a newer primary (epoch {epoch}) exists; this node is fenced"),
        ));
    }
    Ok(())
}

/// Standby side: connect, apply the stream, acknowledge, reconnect on loss.
fn follow_primary(server: Arc<PgWireServer>, config: HaServerConfig) {
    let backoff = Duration::from_millis(config.replication.reconnect_backoff_ms.max(100));
    while !server.is_shutdown_requested() {
        if let Err(error) = follow_once(&server, &config) {
            eprintln!("bicdb replication: lost the primary: {error}");
        }
        std::thread::sleep(backoff);
    }
}

fn follow_once(server: &Arc<PgWireServer>, config: &HaServerConfig) -> Result<()> {
    let HaServerRole::Standby {
        primary,
        primary_server_name,
    } = &config.role
    else {
        return Ok(());
    };
    let socket = TcpStream::connect(primary)?;
    let _ = socket.set_nodelay(true);
    let mut stream: Box<dyn ReplicationStream> = if config.plaintext() {
        Box::new(socket)
    } else {
        let tls = config.replication.tls.as_ref().ok_or_else(|| {
            PgWireError::Server("replication TLS configuration missing".to_string())
        })?;
        let client = replication_transport::build_client_tls(tls).map_err(PgWireError::from)?;
        Box::new(
            replication_transport::client_tls_stream(client.config, primary_server_name, socket)
                .map_err(PgWireError::from)?,
        )
    };
    let stream = stream.as_mut();
    let (applied, epoch) = {
        let db = server.read_db()?;
        (db.last_applied_commit_seq(), db.replication_epoch())
    };
    send(
        stream,
        &ReplicationFrame::Hello {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            cluster_id: config.replication.cluster_id.clone(),
            node_id: config.replication.node_id.clone(),
            epoch,
        },
    )?;
    send(
        stream,
        &ReplicationFrame::Ack {
            cluster_id: config.replication.cluster_id.clone(),
            node_id: config.replication.node_id.clone(),
            commit_seq: applied,
            epoch,
        },
    )?;
    eprintln!("bicdb replication: following {primary} from commit {applied}");
    let mut batch = Vec::new();
    loop {
        match receive(stream)? {
            ReplicationFrame::Commit(frame) => batch.push(frame),
            ReplicationFrame::Heartbeat {
                epoch,
                primary_sql_address,
                ..
            } => {
                {
                    let db = server.read_db()?;
                    let known = db.replication_epoch();
                    if epoch < known {
                        // A superseded primary: its stream must not be applied.
                        return Err(refuse(
                            stream,
                            &format!("stale primary: epoch {epoch} < {known}"),
                        ));
                    }
                    db.observe_replication_epoch(epoch)
                        .map_err(PgWireError::from)?;
                    db.record_primary_sql_address(primary_sql_address)
                        .map_err(PgWireError::from)?;
                }
                let applied = if batch.is_empty() {
                    server.read_db()?.last_applied_commit_seq()
                } else {
                    let admission = server.try_admit_write()?;
                    let mut db = server.write_db_with_admission(&admission)?;
                    let report = db
                        .apply_replication_batch(&batch)
                        .map_err(PgWireError::from)?;
                    batch.clear();
                    report.last_applied_commit_seq
                };
                send(
                    stream,
                    &ReplicationFrame::Ack {
                        cluster_id: config.replication.cluster_id.clone(),
                        node_id: config.replication.node_id.clone(),
                        commit_seq: applied,
                        epoch,
                    },
                )?;
            }
            ReplicationFrame::Error { message, .. } => {
                return Err(PgWireError::Server(format!("primary refused: {message}")));
            }
            other => {
                return Err(PgWireError::Server(format!(
                    "primary sent an unexpected frame: {other:?}"
                )));
            }
        }
    }
}
