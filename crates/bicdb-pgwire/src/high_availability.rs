//! Streaming replication, synchronous quorum commit and automatic failover
//! for `bicdb serve`.
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
//! With automatic failover every node of the group also listens, and the
//! group elects a new primary when the old one goes silent:
//!
//! - Every primary term has an epoch. A standby that has not heard from a
//!   primary for the failover timeout stands for election in the next term
//!   and becomes primary when a majority of the group votes for it. A node
//!   votes once per term, only for a candidate whose commits are at least as
//!   recent as its own, and only once it has not heard from the primary for
//!   the failover timeout itself.
//! - A primary holds a write lease: it admits writes only while it heard from
//!   enough standbys to form a majority within half the failover timeout. A
//!   primary cut off from its group therefore stops writing before the group
//!   can elect a successor, and two primaries never accept writes at once.
//! - Promotion and demotion are recorded in the database directory and take
//!   effect through a restart of the server process. A former primary that
//!   learns of a newer epoch rejoins as a standby; if its history diverged, it
//!   is bootstrapped from a snapshot.
//!
//! Peers authenticate with mutual TLS: the claimed node id must be on the
//! allow-list and, when pinned, match the presented certificate. Plaintext is
//! accepted only for loopback development configurations.
use super::*;
use bicdb_core::replication::ReplicationFrame;
use bicdb_core::{NodeRole, ReplicationCursor, StandbyResume, SyncTimeoutPolicy};
use sha2::{Digest, Sha256};
use std::io::{Read as _, Write as _};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::AtomicBool;
use std::time::Instant;

/// Native HA handshake version. Version 3 understands journal-carrying commit
/// frames. All group members must upgrade before joining.
pub const HA_REPLICATION_PROTOCOL_VERSION: u32 = 3;

/// Frames per round sent to one standby.
const BATCH_COMMITS: usize = 512;
/// Idle interval after which the primary sends a keepalive batch.
const KEEPALIVE: Duration = Duration::from_secs(1);
const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;
/// Bytes per snapshot chunk frame.
const SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;
const NOT_PRIMARY: &str = "not_primary: this node is not the primary";
/// Shortest failover timeout: several keepalives must fit into it.
pub const MIN_FAILOVER_TIMEOUT: Duration = Duration::from_secs(3);

static RESTART_REQUESTED: AtomicBool = AtomicBool::new(false);

/// True once this node changed its replication role (a promotion, a
/// demotion, or a staged snapshot) and shut the server down: the process
/// should start again, and the next open runs with the new role.
pub fn restart_requested() -> bool {
    RESTART_REQUESTED.load(Ordering::SeqCst)
}

fn restart(server: &Arc<PgWireServer>, why: &str) {
    eprintln!("bicdb replication: {why}; restarting");
    RESTART_REQUESTED.store(true, Ordering::SeqCst);
    server.request_shutdown();
}

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

/// Another member of a group with automatic failover.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HaPeer {
    pub node_id: String,
    /// The peer's replication address (host:port).
    pub address: String,
}

/// Automatic failover settings.
#[derive(Clone, Debug, PartialEq)]
pub struct FailoverConfig {
    /// Every other member of the group.
    pub peers: Vec<HaPeer>,
    /// Silence after which standbys elect a new primary.
    pub timeout: Duration,
    /// How this node runs whenever it is the primary (a `Primary` role).
    pub as_primary: HaServerRole,
    /// TLS server name of the peers' certificates.
    pub peer_server_name: String,
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
    pub failover: Option<FailoverConfig>,
}

impl HaServerConfig {
    /// The database-level replication configuration for this role: a primary
    /// logs full row images so frames can be built, a standby refuses
    /// ordinary writes.
    pub fn database_replication(&self) -> ReplicationConfig {
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

    /// The role to run with: with automatic failover, the role recorded in
    /// the database directory (after a promotion or demotion) wins over the
    /// command line, which only sets up a new group.
    pub fn resolved(&self, recorded: (Option<NodeRole>, Option<String>)) -> HaServerConfig {
        let Some(failover) = &self.failover else {
            return self.clone();
        };
        let mut config = self.clone();
        match recorded.0 {
            Some(NodeRole::Primary) => config.role = failover.as_primary.clone(),
            Some(NodeRole::Standby | NodeRole::Fenced) => {
                let primary = recorded
                    .1
                    .as_deref()
                    .and_then(|id| failover.peers.iter().find(|peer| peer.node_id == id))
                    .map(|peer| peer.address.clone())
                    .or_else(|| match &self.role {
                        HaServerRole::Standby { primary, .. } => Some(primary.clone()),
                        HaServerRole::Primary { .. } => None,
                    })
                    .or_else(|| failover.peers.first().map(|peer| peer.address.clone()))
                    .unwrap_or_default();
                config.role = HaServerRole::Standby {
                    primary,
                    primary_server_name: failover.peer_server_name.clone(),
                };
            }
            None => {}
        }
        config
    }

    fn plaintext(&self) -> bool {
        self.replication
            .tls
            .as_ref()
            .is_none_or(|tls| tls.dev_localhost_plaintext)
    }

    /// The settings this node uses as a primary, if it can be one.
    fn primary_settings(&self) -> Option<&HaServerRole> {
        match (&self.role, &self.failover) {
            (HaServerRole::Primary { .. }, _) => Some(&self.role),
            (_, Some(failover)) => Some(&failover.as_primary),
            _ => None,
        }
    }

    fn allowed(&self) -> &[AllowedStandby] {
        match self.primary_settings() {
            Some(HaServerRole::Primary {
                allowed_standbys, ..
            }) => allowed_standbys,
            _ => &[],
        }
    }

    fn group_size(&self) -> usize {
        self.failover
            .as_ref()
            .map_or(1, |failover| failover.peers.len() + 1)
    }
}

/// What the replication threads of one server share.
struct HaRuntime {
    /// When this node last heard from a live primary (a standby).
    last_heard: Mutex<Instant>,
    connections: Mutex<std::collections::BTreeMap<uuid::Uuid, TcpStream>>,
    stopping: AtomicBool,
}

impl HaRuntime {
    fn track(self: &Arc<Self>, socket: &TcpStream, limit: usize) -> Result<Connection> {
        let mut connections = self.connections.lock().unwrap();
        if self.stopping.load(Ordering::Acquire) || connections.len() >= limit {
            return Err(PgWireError::Server(
                "replication connection capacity reached".into(),
            ));
        }
        let id = uuid::Uuid::new_v4();
        connections.insert(id, socket.try_clone()?);
        Ok(Connection {
            runtime: self.clone(),
            id,
        })
    }
    fn close_connections(&self) {
        self.stopping.store(true, Ordering::Release);
        for socket in self.connections.lock().unwrap().values() {
            let _ = socket.shutdown(std::net::Shutdown::Both);
        }
    }
    fn heard_now(&self) {
        *self.last_heard.lock().unwrap() = Instant::now();
    }

    fn silent_for(&self) -> Duration {
        self.last_heard.lock().unwrap().elapsed()
    }
}

struct Connection {
    runtime: Arc<HaRuntime>,
    id: uuid::Uuid,
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.runtime.connections.lock().unwrap().remove(&self.id);
    }
}

fn spawn_worker(
    server: &Arc<PgWireServer>,
    name: &str,
    work: impl FnOnce() + Send + 'static,
) -> Result<()> {
    // Register before spawning so shutdown cannot miss a thread that has not
    // started executing yet. The registration also covers accepted peers.
    let worker = BackgroundWorker::register(server.clone());
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            let _worker = worker;
            work();
        })?;
    Ok(())
}
fn peer_capacity(config: &HaServerConfig) -> usize {
    (config.allowed().len() * 4 + 8).min(256)
}

/// Starts the replication service for `server`'s HA role.
pub(crate) fn start(server: Arc<PgWireServer>, config: HaServerConfig) -> Result<()> {
    let recorded = {
        let db = server.read_db()?;
        bicdb_core::read_persisted_ha_role(db.data_path()).map_err(PgWireError::from)?
    };
    let config = config.resolved(recorded);
    config
        .database_replication()
        .validate()
        .map_err(PgWireError::from)?;
    let runtime = Arc::new(HaRuntime {
        last_heard: Mutex::new(Instant::now()),
        connections: Mutex::new(std::collections::BTreeMap::new()),
        stopping: AtomicBool::new(false),
    });
    struct StartupGuard {
        server: Arc<PgWireServer>,
        armed: bool,
    }
    impl Drop for StartupGuard {
        fn drop(&mut self) {
            if self.armed {
                self.server.request_shutdown();
            }
        }
    }
    let mut startup = StartupGuard {
        server: server.clone(),
        armed: true,
    };
    let cancel_server = server.clone();
    let cancel_runtime = runtime.clone();
    spawn_worker(&server, "bicdb-replication-shutdown", move || {
        while !cancel_server.is_shutdown_requested() {
            std::thread::sleep(Duration::from_millis(25));
        }
        cancel_runtime.close_connections();
    })?;
    if config.failover.is_some() && server.read_db()?.replication_role() == NodeRole::Fenced {
        // A fenced former primary rejoins its group as a standby.
        server
            .read_db()?
            .demote_to_standby()
            .map_err(PgWireError::from)?;
    }
    if let HaServerRole::Primary {
        synchronous_standbys,
        synchronous_timeout,
        on_timeout,
        ..
    } = &config.role
    {
        if *synchronous_standbys > 0 && config.allowed().len() < *synchronous_standbys {
            return Err(PgWireError::Server(format!(
                "{synchronous_standbys} synchronous standbys required but only {} are allowed to connect",
                config.allowed().len()
            )));
        }
        server
            .sync_quorum
            .configure(*synchronous_standbys, *synchronous_timeout, *on_timeout);
        if let Some(failover) = &config.failover {
            // A majority of the group: this node plus half of the others.
            server
                .sync_quorum
                .configure_lease(failover.timeout / 2, config.group_size() / 2);
        }
        let db = server.read_db()?;
        // Standbys removed from the allow-list stop pinning the log.
        let allowed = config
            .allowed()
            .iter()
            .map(|standby| standby.node_id.clone())
            .collect::<Vec<_>>();
        db.retain_standby_slots(&allowed)
            .map_err(PgWireError::from)?;
        if db.replication_role() == NodeRole::Primary {
            db.record_primary_sql_address(config.advertise_sql.clone())
                .map_err(PgWireError::from)?;
        }
        drop(db);
        {
            let admission = server.try_admit_write()?;
            server
                .write_db_with_admission(&admission)?
                .ensure_catalog_log()
                .map_err(PgWireError::from)?;
        }
        let announcer = server.clone();
        spawn_worker(&server, "bicdb-replication-catalog", move || {
            announce_index_catalog(announcer)
        })?;
    }
    if let Some(HaServerRole::Primary { listen, .. }) = config.primary_settings() {
        let listener = std::net::TcpListener::bind(listen)?;
        listener.set_nonblocking(true)?;
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
        let (server, config, runtime) = (server.clone(), config.clone(), runtime.clone());
        let owner = server.clone();
        spawn_worker(&owner, "bicdb-replication-listener", move || {
            accept_peers(server, config, runtime, listener, tls)
        })?;
    }
    match &config.role {
        HaServerRole::Primary { .. } => {
            if config.failover.is_some() {
                let owner = server.clone();
                spawn_worker(&owner, "bicdb-replication-lease", move || {
                    watch_primary_lease(server, config, runtime)
                })?;
            }
        }
        HaServerRole::Standby { .. } => {
            let owner = server.clone();
            spawn_worker(&owner, "bicdb-replication-follower", move || {
                follow_primary(server, config, runtime)
            })?;
        }
    }
    startup.armed = false;
    Ok(())
}

fn accept_peers(
    server: Arc<PgWireServer>,
    config: HaServerConfig,
    runtime: Arc<HaRuntime>,
    listener: std::net::TcpListener,
    tls: Option<Arc<rustls::ServerConfig>>,
) {
    while !server.is_shutdown_requested() {
        let socket = match listener.accept() {
            Ok((socket, _)) => socket,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(25));
                continue;
            }
            Err(error) => {
                eprintln!("bicdb replication accept failed: {error}");
                std::thread::sleep(Duration::from_millis(25));
                continue;
            }
        };
        let Ok(connection) = runtime.track(&socket, peer_capacity(&config)) else {
            continue;
        };
        let _ = socket.set_nodelay(true);
        let _ = socket.set_read_timeout(Some(Duration::from_secs(3)));
        let _ = socket.set_write_timeout(Some(Duration::from_secs(3)));
        let owner = server.clone();
        let server = server.clone();
        let config = config.clone();
        let runtime = runtime.clone();
        let tls = tls.clone();
        let _ = spawn_worker(&owner, "bicdb-replication-peer", move || {
            let _connection = connection;
            let peer = socket.peer_addr().ok();
            match serve_peer(&server, &config, &runtime, socket, tls) {
                Err(error)
                    if error.to_string().contains(NOT_PRIMARY)
                        || server.is_shutdown_requested() => {}
                Err(error) => eprintln!("bicdb replication: peer {peer:?} disconnected: {error}"),
                Ok(()) => {}
            }
        });
    }
    runtime.close_connections();
}

trait ReplicationStream: std::io::Read + std::io::Write + Send {}
impl<T: std::io::Read + std::io::Write + Send> ReplicationStream for T {}

struct TrackedStream {
    inner: Box<dyn ReplicationStream>,
    _connection: Connection,
}
impl Read for TrackedStream {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(bytes)
    }
}
impl Write for TrackedStream {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.inner.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn send(mut stream: &mut dyn ReplicationStream, frame: &ReplicationFrame) -> Result<()> {
    replication_transport::send_frame(&mut stream, frame, MAX_FRAME_BYTES)
        .map_err(PgWireError::from)
}

fn receive(mut stream: &mut dyn ReplicationStream) -> Result<ReplicationFrame> {
    replication_transport::recv_frame(&mut stream, MAX_FRAME_BYTES).map_err(PgWireError::from)
}

fn receive_control(mut stream: &mut dyn ReplicationStream) -> Result<ReplicationFrame> {
    replication_transport::recv_frame(&mut stream, 64 * 1024).map_err(PgWireError::from)
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

/// Authenticates one peer and serves what it asks for: a standby's stream, a
/// vote, or a primary's ping.
fn serve_peer(
    server: &Arc<PgWireServer>,
    config: &HaServerConfig,
    runtime: &HaRuntime,
    socket: TcpStream,
    tls: Option<Arc<rustls::ServerConfig>>,
) -> Result<()> {
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

    let peer = match receive_control(stream)? {
        ReplicationFrame::Hello {
            protocol_version,
            cluster_id,
            node_id,
            epoch,
        } => {
            if protocol_version != HA_REPLICATION_PROTOCOL_VERSION {
                return Err(refuse(stream, "unsupported replication protocol version"));
            }
            if cluster_id != config.replication.cluster_id {
                return Err(refuse(stream, "cluster_id mismatch"));
            }
            // Fail closed: an empty allow-list authorizes nobody.
            let Some(allowed) = config
                .allowed()
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
    match receive(stream)? {
        ReplicationFrame::Resume {
            cluster_id,
            node_id,
            commit_seq,
            data_epoch,
            journal_identity,
        } => {
            if cluster_id != config.replication.cluster_id || node_id != peer {
                return Err(refuse(
                    stream,
                    "resume identity does not match the authenticated peer",
                ));
            }
            if server.read_db()?.replication_role() != NodeRole::Primary {
                return Err(refuse(stream, NOT_PRIMARY));
            }
            if config.replication.record_journal != journal_identity.is_some() {
                return Err(refuse(
                    stream,
                    "record journal mode differs between replicas",
                ));
            }
            if let Some(identity) = journal_identity {
                let db = server.read_db()?;
                let local = (
                    db.node_id().to_string(),
                    db.mesh_verifying_key().ok_or_else(|| {
                        PgWireError::Server("record journal requires mesh signing".into())
                    })?,
                );
                if identity != local {
                    drop(db);
                    return send_snapshot(server, config, stream, &peer);
                }
            }
            stream_to_standby(server, config, stream, &peer, data_epoch, commit_seq)
        }
        ReplicationFrame::VoteRequest {
            candidate,
            term,
            data_epoch,
            last_commit_seq,
            ..
        } => {
            let db = server.read_db()?;
            let granted = if candidate != peer {
                false
            } else {
                match db.replication_role() {
                    // A primary that still holds its lease is alive.
                    NodeRole::Primary => {
                        server.sync_quorum.lease_refusal().is_some()
                            && db
                                .consider_vote(term, &candidate, data_epoch, last_commit_seq)
                                .map_err(PgWireError::from)?
                    }
                    // A standby that hears its primary does not replace it.
                    _ => {
                        let timeout = config.failover.as_ref().map(|failover| failover.timeout);
                        timeout.is_some_and(|timeout| runtime.silent_for() >= timeout)
                            && db
                                .consider_vote(term, &candidate, data_epoch, last_commit_seq)
                                .map_err(PgWireError::from)?
                    }
                }
            };
            eprintln!(
                "bicdb replication: vote for {candidate} in term {term}: {}",
                if granted { "granted" } else { "refused" }
            );
            send(
                stream,
                &ReplicationFrame::VoteResponse {
                    node_id: config.replication.node_id.clone(),
                    term,
                    granted,
                    epoch: db.replication_epoch(),
                },
            )
        }
        ReplicationFrame::Ping { epoch, .. } => {
            let db = server.read_db()?;
            if epoch >= db.replication_epoch() && db.replication_role() != NodeRole::Primary {
                // The current primary is alive even if its stream is not.
                runtime.heard_now();
            }
            send(
                stream,
                &ReplicationFrame::Pong {
                    node_id: config.replication.node_id.clone(),
                    epoch: db.replication_epoch(),
                    primary_id: db.ha_node_status().primary_id,
                },
            )
        }
        _ => Err(refuse(stream, "expected Resume, VoteRequest or Ping")),
    }
}

/// Streams commits to one standby until the connection ends.
fn stream_to_standby(
    server: &Arc<PgWireServer>,
    config: &HaServerConfig,
    stream: &mut dyn ReplicationStream,
    standby: &str,
    standby_epoch: u64,
    applied: u64,
) -> Result<()> {
    if server.read_db()?.replication_role() != NodeRole::Primary {
        return Err(refuse(stream, NOT_PRIMARY));
    }
    let resume = server
        .read_db()?
        .standby_resume(standby_epoch, applied)
        .map_err(PgWireError::from)?;
    if let StandbyResume::Snapshot(reason) = resume {
        eprintln!("bicdb replication: standby {standby} needs a snapshot: {reason}");
        return send_snapshot(server, config, stream, standby);
    }
    // The standby's position is durable on its side, and it is exact: a
    // standby that lost data reports less than it once confirmed.
    server.sync_quorum.set_ack(standby, applied);
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
        let (frames, status) = {
            let db = server.read_db()?;
            if db.replication_role() != NodeRole::Primary {
                return Err(refuse(stream, "this node is no longer the primary"));
            }
            let frames = match db.replication_frames_after(&mut cursor, BATCH_COMMITS) {
                Ok(frames) => frames,
                Err(error) => return Err(refuse(stream, &error.to_string())),
            };
            (frames, db.ha_node_status())
        };
        if frames.is_empty() {
            let durable = server
                .replication_feed
                .wait_beyond(cursor.sent_seq(), KEEPALIVE);
            if durable > cursor.sent_seq() {
                // A durable row commit may still be publishing its journal.
                // Keep the connection live without spinning on that prefix.
                std::thread::sleep(Duration::from_millis(50));
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
                epoch: status.epoch,
                primary_sql_address: config.advertise_sql.clone(),
                epoch_starts: status.epoch_starts.into_iter().collect(),
            },
        )?;
        match receive(stream)? {
            ReplicationFrame::Ack {
                commit_seq, epoch, ..
            } => {
                superseded_check(server, stream, epoch)?;
                server.sync_quorum.record_ack(standby, commit_seq);
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

/// Removes a file when dropped.
struct RemoveOnDrop(std::path::PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn sibling_file(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let mut name = path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    name.push(format!(".{suffix}"));
    path.with_file_name(name)
}

/// Sends a consistent snapshot of the database to a standby that cannot
/// continue from its own position; the standby installs it and reconnects.
fn send_snapshot(
    server: &Arc<PgWireServer>,
    config: &HaServerConfig,
    stream: &mut dyn ReplicationStream,
    standby: &str,
) -> Result<()> {
    let snapshot_id = uuid::Uuid::new_v4().simple().to_string();
    let key = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let (archive, snapshot_commit_seq) = {
        let db = server.read_db()?;
        // Keep the log after the snapshot point until the standby is back.
        server.sync_quorum.pin(standby, db.current_commit_seq());
        let archive = RemoveOnDrop(sibling_file(
            db.data_path(),
            &format!("snapshot-{snapshot_id}"),
        ));
        let seq = db
            .create_standby_snapshot(&archive.0, &key)
            .map_err(PgWireError::from)?;
        (archive, seq)
    };
    send(
        stream,
        &ReplicationFrame::SnapshotStart {
            cluster_id: config.replication.cluster_id.clone(),
            source_node_id: config.replication.node_id.clone(),
            snapshot_id: snapshot_id.clone(),
            snapshot_commit_seq,
            archive_key: key,
        },
    )?;
    let mut file = std::fs::File::open(&archive.0)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0u8; SNAPSHOT_CHUNK_BYTES];
    let mut offset = 0u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let bytes = buffer[..read].to_vec();
        hash.update(&bytes);
        send(
            stream,
            &ReplicationFrame::SnapshotChunk {
                snapshot_id: snapshot_id.clone(),
                offset,
                checksum: Sha256::digest(&bytes).into(),
                bytes,
            },
        )?;
        offset += read as u64;
    }
    send(
        stream,
        &ReplicationFrame::SnapshotEnd {
            snapshot_id,
            snapshot_commit_seq,
            snapshot_hash: hash.finalize().into(),
        },
    )?;
    eprintln!(
        "bicdb replication: sent standby {standby} a snapshot at commit {snapshot_commit_seq} ({offset} bytes)"
    );
    Ok(())
}

/// Standby side: receives a snapshot, stages it next to the database, and
/// shuts the server down so the next start installs it.
fn receive_snapshot(
    server: &Arc<PgWireServer>,
    stream: &mut dyn ReplicationStream,
    snapshot_id: &str,
    snapshot_commit_seq: u64,
    key: &str,
) -> Result<()> {
    let path = server.read_db()?.data_path().to_path_buf();
    eprintln!(
        "bicdb replication: receiving a snapshot of the primary at commit {snapshot_commit_seq}"
    );
    let archive = RemoveOnDrop(sibling_file(&path, "bootstrap-archive"));
    let mut file = std::fs::File::create(&archive.0)?;
    let mut hash = Sha256::new();
    let mut expected = 0u64;
    loop {
        match receive(stream)? {
            ReplicationFrame::SnapshotChunk {
                snapshot_id: id,
                offset,
                bytes,
                checksum,
            } => {
                let digest: [u8; 32] = Sha256::digest(&bytes).into();
                if id != snapshot_id || offset != expected || digest != checksum {
                    return Err(PgWireError::Server(
                        "snapshot chunk out of order or corrupt".to_string(),
                    ));
                }
                hash.update(&bytes);
                file.write_all(&bytes)?;
                expected += bytes.len() as u64;
            }
            ReplicationFrame::SnapshotEnd {
                snapshot_id: id,
                snapshot_hash,
                ..
            } => {
                let digest: [u8; 32] = hash.finalize().into();
                if id != snapshot_id || digest != snapshot_hash {
                    return Err(PgWireError::Server("snapshot hash mismatch".to_string()));
                }
                break;
            }
            other => {
                return Err(PgWireError::Server(format!(
                    "unexpected frame in a snapshot: {other:?}"
                )));
            }
        }
    }
    file.sync_all()?;
    drop(file);
    bicdb_core::stage_standby_bootstrap(&path, &archive.0, key, true).map_err(PgWireError::from)?;
    restart(
        server,
        &format!("snapshot staged ({expected} bytes); the next start installs it"),
    );
    Ok(())
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

/// Opens an authenticated replication connection to a peer.
fn connect_peer(
    config: &HaServerConfig,
    address: &str,
    server_name: &str,
    timeout: Option<Duration>,
    runtime: &Arc<HaRuntime>,
) -> Result<Box<dyn ReplicationStream>> {
    let timeout = Some(
        timeout
            .unwrap_or(Duration::from_secs(3))
            .min(Duration::from_secs(3)),
    );
    let socket = match timeout {
        Some(timeout) => {
            let target = address.to_socket_addrs()?.next().ok_or_else(|| {
                PgWireError::Server(format!("cannot resolve replication address {address}"))
            })?;
            let socket = TcpStream::connect_timeout(&target, timeout)?;
            socket.set_read_timeout(Some(timeout))?;
            socket.set_write_timeout(Some(timeout))?;
            socket
        }
        None => TcpStream::connect(address)?,
    };
    let _ = socket.set_nodelay(true);
    let connection = runtime.track(&socket, peer_capacity(config))?;
    if config.plaintext() {
        return Ok(Box::new(TrackedStream {
            inner: Box::new(socket),
            _connection: connection,
        }));
    }
    let tls =
        config.replication.tls.as_ref().ok_or_else(|| {
            PgWireError::Server("replication TLS configuration missing".to_string())
        })?;
    let client = replication_transport::build_client_tls(tls).map_err(PgWireError::from)?;
    Ok(Box::new(TrackedStream {
        inner: Box::new(
            replication_transport::client_tls_stream(client.config, server_name, socket)
                .map_err(PgWireError::from)?,
        ),
        _connection: connection,
    }))
}

fn hello(config: &HaServerConfig, epoch: u64) -> ReplicationFrame {
    ReplicationFrame::Hello {
        protocol_version: HA_REPLICATION_PROTOCOL_VERSION,
        cluster_id: config.replication.cluster_id.clone(),
        node_id: config.replication.node_id.clone(),
        epoch,
    }
}

/// Standby side: follow the primary, find a new one when it moves, and stand
/// for election when the group has none.
fn follow_primary(server: Arc<PgWireServer>, config: HaServerConfig, runtime: Arc<HaRuntime>) {
    let backoff = Duration::from_millis(config.replication.reconnect_backoff_ms.max(100));
    // Waiting a random extra of up to one more timeout keeps candidates from
    // splitting the vote.
    let jitter = |timeout: Duration| {
        let random = uuid::Uuid::new_v4().as_u128() as u64;
        Duration::from_millis(random % (timeout.as_millis() as u64).max(1))
    };
    let mut last_errors = std::collections::HashMap::<String, String>::new();
    let mut election_after = config
        .failover
        .as_ref()
        .map(|failover| failover.timeout + jitter(failover.timeout));
    while !server.is_shutdown_requested() {
        for (address, server_name) in follow_targets(&server, &config) {
            if server.is_shutdown_requested() {
                return;
            }
            match follow_once(&server, &config, &runtime, &address, &server_name) {
                Ok(()) => {
                    last_errors.remove(&address);
                }
                Err(error) => {
                    // Report each address's error once until it changes.
                    let error = error.to_string();
                    if last_errors.get(&address) != Some(&error) {
                        eprintln!("bicdb replication: no stream from {address}: {error}");
                        last_errors.insert(address, error);
                    }
                }
            }
        }
        if let (Some(failover), Some(after)) = (&config.failover, election_after) {
            if runtime.silent_for() >= after && !server.is_shutdown_requested() {
                if let Err(error) = run_election(&server, &config, failover, &runtime) {
                    eprintln!("bicdb replication: election failed: {error}");
                }
                election_after = Some(failover.timeout + jitter(failover.timeout));
                runtime.heard_now();
            }
        }
        std::thread::sleep(backoff);
    }
}

/// Where to look for the primary: the configured or last known primary
/// first, then every peer.
fn follow_targets(server: &Arc<PgWireServer>, config: &HaServerConfig) -> Vec<(String, String)> {
    let mut targets = Vec::new();
    if let HaServerRole::Standby {
        primary,
        primary_server_name,
    } = &config.role
    {
        targets.push((primary.clone(), primary_server_name.clone()));
    }
    if let Some(failover) = &config.failover {
        let known = server
            .read_db()
            .ok()
            .and_then(|db| db.ha_node_status().primary_id);
        let mut peers = failover.peers.iter().collect::<Vec<_>>();
        peers.sort_by_key(|peer| Some(&peer.node_id) != known.as_ref());
        for peer in peers {
            if targets.iter().all(|(address, _)| address != &peer.address) {
                targets.push((peer.address.clone(), failover.peer_server_name.clone()));
            }
        }
    }
    targets
}

fn follow_once(
    server: &Arc<PgWireServer>,
    config: &HaServerConfig,
    runtime: &Arc<HaRuntime>,
    primary: &str,
    server_name: &str,
) -> Result<()> {
    let connect_timeout = config
        .failover
        .as_ref()
        .map(|failover| failover.timeout / 2);
    let mut stream = connect_peer(config, primary, server_name, connect_timeout, runtime)?;
    let stream = stream.as_mut();
    let (applied, epoch, data_epoch, journal_identity) = {
        let db = server.read_db()?;
        (
            db.last_applied_commit_seq(),
            db.replication_epoch(),
            db.ha_node_status().data_epoch,
            if config.replication.record_journal {
                Some((
                    db.node_id().to_string(),
                    db.mesh_verifying_key().ok_or_else(|| {
                        PgWireError::Server("record journal requires mesh signing".into())
                    })?,
                ))
            } else {
                None
            },
        )
    };
    send(stream, &hello(config, epoch))?;
    send(
        stream,
        &ReplicationFrame::Resume {
            cluster_id: config.replication.cluster_id.clone(),
            node_id: config.replication.node_id.clone(),
            commit_seq: applied,
            data_epoch,
            journal_identity,
        },
    )?;
    let mut batch = Vec::new();
    let mut announced = false;
    loop {
        match receive(stream)? {
            ReplicationFrame::Commit(frame) => batch.push(frame),
            ReplicationFrame::Heartbeat {
                node_id,
                epoch,
                primary_sql_address,
                epoch_starts,
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
                    let epoch_starts = epoch_starts.into_iter().collect();
                    db.record_primary(&node_id, epoch, primary_sql_address, &epoch_starts)
                        .map_err(PgWireError::from)?;
                }
                runtime.heard_now();
                if !announced {
                    eprintln!(
                        "bicdb replication: following {node_id} at {primary} (epoch {epoch}) from commit {applied}"
                    );
                    announced = true;
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
            ReplicationFrame::SnapshotStart {
                snapshot_id,
                snapshot_commit_seq,
                archive_key,
                ..
            } => {
                return receive_snapshot(
                    server,
                    stream,
                    &snapshot_id,
                    snapshot_commit_seq,
                    &archive_key,
                );
            }
            ReplicationFrame::Error { message, .. } => {
                return Err(PgWireError::Server(format!("refused: {message}")));
            }
            other => {
                return Err(PgWireError::Server(format!(
                    "primary sent an unexpected frame: {other:?}"
                )));
            }
        }
    }
}

/// Stands for election in the next term; on winning, records the promotion
/// and restarts as the primary.
fn run_election(
    server: &Arc<PgWireServer>,
    config: &HaServerConfig,
    failover: &FailoverConfig,
    runtime: &Arc<HaRuntime>,
) -> Result<()> {
    let node_id = &config.replication.node_id;
    let (term, epoch, data_epoch, last_commit_seq) = {
        let db = server.read_db()?;
        let term = db.begin_candidacy(node_id).map_err(PgWireError::from)?;
        let status = db.ha_node_status();
        (
            term,
            status.epoch,
            status.data_epoch,
            db.last_applied_commit_seq(),
        )
    };
    eprintln!(
        "bicdb replication: no primary for {:?}; standing for election in term {term} at commit {last_commit_seq}",
        failover.timeout
    );
    let mut votes = 1;
    for peer in &failover.peers {
        if server.is_shutdown_requested() {
            return Ok(());
        }
        let response = (|| -> Result<ReplicationFrame> {
            let mut stream = connect_peer(
                config,
                &peer.address,
                &failover.peer_server_name,
                Some(failover.timeout / 2),
                runtime,
            )?;
            let stream = stream.as_mut();
            send(stream, &hello(config, epoch))?;
            send(
                stream,
                &ReplicationFrame::VoteRequest {
                    cluster_id: config.replication.cluster_id.clone(),
                    candidate: node_id.clone(),
                    term,
                    data_epoch,
                    last_commit_seq,
                },
            )?;
            receive(stream)
        })();
        match response {
            Ok(ReplicationFrame::VoteResponse { granted, epoch, .. }) => {
                if granted {
                    votes += 1;
                }
                server
                    .read_db()?
                    .observe_replication_epoch(epoch)
                    .map_err(PgWireError::from)?;
            }
            Ok(other) => eprintln!(
                "bicdb replication: {} answered the vote request with {other:?}",
                peer.node_id
            ),
            Err(error) => eprintln!("bicdb replication: no vote from {}: {error}", peer.node_id),
        }
    }
    let group = config.group_size();
    if votes * 2 <= group {
        eprintln!("bicdb replication: term {term} lost with {votes} of {group} votes");
        return Ok(());
    }
    let db = server.read_db()?;
    if db.replication_epoch() >= term {
        // Another node became primary in this term or later meanwhile.
        return Ok(());
    }
    db.promote_by_election(term, node_id, config.advertise_sql.clone())
        .map_err(PgWireError::from)?;
    drop(db);
    restart(
        server,
        &format!("won term {term} with {votes} of {group} votes; this node is now the primary"),
    );
    Ok(())
}

/// Primary side with automatic failover: notices that this node was fenced
/// (a newer primary exists) and rejoins as a standby. While the write lease
/// is lost, asks the group who its primary is.
fn watch_primary_lease(server: Arc<PgWireServer>, config: HaServerConfig, runtime: Arc<HaRuntime>) {
    let Some(failover) = config.failover.clone() else {
        return;
    };
    let mut last_probe = Instant::now();
    while !server.is_shutdown_requested() {
        std::thread::sleep(Duration::from_millis(250));
        let Ok(db) = server.read_db() else {
            continue;
        };
        if db.replication_role() == NodeRole::Fenced {
            if let Err(error) = db.demote_to_standby() {
                eprintln!("bicdb replication: demotion failed: {error}");
                continue;
            }
            drop(db);
            restart(
                &server,
                "a newer primary exists; this node rejoins the group as a standby",
            );
            return;
        }
        let epoch = db.replication_epoch();
        drop(db);
        if server.sync_quorum.lease_refusal().is_none()
            || last_probe.elapsed() < failover.timeout / 2
        {
            continue;
        }
        last_probe = Instant::now();
        for peer in &failover.peers {
            if server.is_shutdown_requested() {
                return;
            }
            let pong = (|| -> Result<ReplicationFrame> {
                let mut stream = connect_peer(
                    &config,
                    &peer.address,
                    &failover.peer_server_name,
                    Some(failover.timeout / 2),
                    &runtime,
                )?;
                let stream = stream.as_mut();
                send(stream, &hello(&config, epoch))?;
                send(
                    stream,
                    &ReplicationFrame::Ping {
                        cluster_id: config.replication.cluster_id.clone(),
                        node_id: config.replication.node_id.clone(),
                        epoch,
                    },
                )?;
                receive(stream)
            })();
            if let Ok(ReplicationFrame::Pong { epoch: theirs, .. }) = pong {
                if theirs > epoch {
                    if let Ok(db) = server.read_db() {
                        let _ = db.observe_replication_epoch(theirs);
                    }
                    break;
                }
            }
        }
    }
}

/// Primary: tells standbys about index changes, so they build the same
/// indexes from their own rows.
fn announce_index_catalog(server: Arc<PgWireServer>) {
    let mut last_error = None::<String>;
    while !server.is_shutdown_requested() {
        std::thread::sleep(KEEPALIVE);
        let announced = server
            .read_db()
            .map_err(|error| error.to_string())
            .and_then(|db| {
                if db.replication_role() != NodeRole::Primary {
                    return Ok(false);
                }
                db.announce_index_catalog()
                    .map_err(|error| error.to_string())
            });
        match announced {
            Ok(_) => last_error = None,
            Err(error) => {
                if last_error.as_ref() != Some(&error) {
                    eprintln!("bicdb replication: index catalog announcement failed: {error}");
                    last_error = Some(error);
                }
            }
        }
    }
}
