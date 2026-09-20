//! LAN mesh transport: automatic peer discovery + duplex sync sessions.
//!
//! A travel router with no Internet is full camp infrastructure: nodes
//! announce themselves with small UDP multicast beacons and sync over plain
//! TCP by handing the connected stream to the transport-neutral session in
//! [`crate::mesh`]. Discovery also accepts unicast beacons, so environments
//! without multicast (containers, odd VPNs) can point nodes at each other
//! directly — and [`sync_with_addr`] dials a known address with no discovery
//! at all.
//!
//! Everything here inherits the mesh recovery model: interruption is the
//! normal case, any dropped session is repaired by the next one, and no
//! transfer state is required for correctness. Shutting a node down
//! mid-session, killing the app, or walking out of Wi-Fi range are all
//! equivalent to "sync later".
//!
//! Pre-identity trust model: this transport carries the same bytes a file
//! bundle would. Run it on networks you trust (the camp router), and treat
//! the Phase 2 identity work (certificates, signed frames, session
//! encryption) as the upgrade path — the beacon `group` string is a
//! convenience filter, not a security boundary.

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bicdb_core::{BicDb, BicDbError, NodeId, Result};
use serde::{Deserialize, Serialize};

use crate::mesh::{
    run_mesh_initiator, run_mesh_initiator_shared, run_mesh_responder_shared, MeshSyncReport,
};

pub const LAN_BEACON_MAGIC: &str = "bicdb-mesh-beacon";
pub const LAN_BEACON_VERSION: u32 = 1;
pub const DEFAULT_MULTICAST_ADDR: SocketAddrV4 =
    SocketAddrV4::new(Ipv4Addr::new(239, 255, 71, 13), 47113);
const MAX_BEACON_BYTES: usize = 1024;
const SESSION_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const SESSION_IO_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LanBeacon {
    pub magic: String,
    pub version: u32,
    pub node_id: NodeId,
    pub tcp_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}

impl LanBeacon {
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let beacon: Self = serde_json::from_slice(bytes)?;
        if beacon.magic != LAN_BEACON_MAGIC {
            return Err(BicDbError::SyncBundle(format!(
                "not a bicdb mesh beacon (magic `{}`)",
                beacon.magic
            )));
        }
        if beacon.version != LAN_BEACON_VERSION {
            return Err(BicDbError::SyncBundle(format!(
                "unsupported mesh beacon version {}",
                beacon.version
            )));
        }
        Ok(beacon)
    }
}

#[derive(Clone, Debug)]
pub struct LanMeshConfig {
    /// TCP port for inbound sessions; 0 asks the OS for an ephemeral port
    /// (announced via beacons).
    pub tcp_port: u16,
    /// Multicast destination and discovery-listener port. A port of 0 asks
    /// the OS for an ephemeral discovery port and disables multicast
    /// announcements, which is useful for explicitly injected discovery.
    pub multicast_addr: SocketAddrV4,
    pub beacon_interval: Duration,
    /// Minimum time between sync attempts with the same peer.
    pub resync_interval: Duration,
    /// Only sync with peers announcing the same group (e.g. a camp id).
    /// A convenience filter for co-located deployments, not a security
    /// boundary.
    pub group: Option<String>,
}

impl Default for LanMeshConfig {
    fn default() -> Self {
        Self {
            tcp_port: 0,
            multicast_addr: DEFAULT_MULTICAST_ADDR,
            beacon_interval: Duration::from_secs(2),
            resync_interval: Duration::from_secs(10),
            group: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LanPeer {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub group: Option<String>,
    pub last_seen_unix_ms: i64,
    pub last_sync_unix_ms: Option<i64>,
    pub last_sync_error: Option<String>,
    pub sessions_completed: u64,
}

#[derive(Debug, Default)]
struct PeerTable {
    peers: BTreeMap<NodeId, PeerEntry>,
}

#[derive(Debug)]
struct PeerEntry {
    info: LanPeer,
    next_attempt_at: Instant,
}

/// A running LAN mesh node: beacon sender, discovery listener, inbound
/// session acceptor, and an outbound sync loop, each on its own thread.
pub struct LanMesh {
    shutdown: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
    local_addr: SocketAddr,
    discovery_addr: SocketAddr,
    node_id: NodeId,
    peers: Arc<Mutex<PeerTable>>,
}

impl LanMesh {
    pub fn spawn(db: Arc<Mutex<BicDb>>, config: LanMeshConfig) -> Result<LanMesh> {
        let node_id = lock_db(&db).node_id();
        let listener = TcpListener::bind(("0.0.0.0", config.tcp_port))?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;

        let discovery_socket = bind_discovery_socket(&config.multicast_addr)?;
        discovery_socket.set_read_timeout(Some(POLL_INTERVAL))?;
        let discovery_addr = discovery_socket.local_addr()?;

        let beacon_socket = UdpSocket::bind(("0.0.0.0", 0))?;
        beacon_socket.set_multicast_loop_v4(true)?;
        let beacon = LanBeacon {
            magic: LAN_BEACON_MAGIC.to_string(),
            version: LAN_BEACON_VERSION,
            node_id: node_id.clone(),
            tcp_port: local_addr.port(),
            group: config.group.clone(),
        }
        .encode()?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let peers = Arc::new(Mutex::new(PeerTable::default()));
        let mut threads = Vec::new();

        // Inbound sessions: accept, answer, hang up.
        {
            let db = Arc::clone(&db);
            let shutdown = Arc::clone(&shutdown);
            threads.push(std::thread::spawn(move || {
                while !shutdown.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let _ = stream.set_read_timeout(Some(SESSION_IO_TIMEOUT));
                            let _ = stream.set_write_timeout(Some(SESSION_IO_TIMEOUT));
                            let mut stream = stream;
                            let _ = run_mesh_responder_shared(&db, &mut stream);
                        }
                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            std::thread::sleep(POLL_INTERVAL);
                        }
                        Err(_) => std::thread::sleep(POLL_INTERVAL),
                    }
                }
            }));
        }

        // Beacon announcements.
        if config.multicast_addr.port() != 0 {
            let shutdown = Arc::clone(&shutdown);
            let multicast_addr = config.multicast_addr;
            let beacon_interval = config.beacon_interval;
            threads.push(std::thread::spawn(move || {
                while !shutdown.load(Ordering::Relaxed) {
                    let _ = beacon_socket.send_to(&beacon, multicast_addr);
                    let mut waited = Duration::ZERO;
                    while waited < beacon_interval && !shutdown.load(Ordering::Relaxed) {
                        std::thread::sleep(POLL_INTERVAL);
                        waited += POLL_INTERVAL;
                    }
                }
            }));
        }

        // Discovery + outbound sync loop.
        {
            let db = Arc::clone(&db);
            let shutdown = Arc::clone(&shutdown);
            let peers = Arc::clone(&peers);
            let own_node = node_id.clone();
            let group = config.group.clone();
            let resync_interval = config.resync_interval;
            threads.push(std::thread::spawn(move || {
                let mut buffer = [0_u8; MAX_BEACON_BYTES];
                while !shutdown.load(Ordering::Relaxed) {
                    match discovery_socket.recv_from(&mut buffer) {
                        Ok((length, source)) => {
                            if let Ok(beacon) = LanBeacon::decode(&buffer[..length]) {
                                observe_beacon(&peers, &own_node, &group, beacon, source);
                            }
                        }
                        Err(error)
                            if matches!(
                                error.kind(),
                                ErrorKind::WouldBlock | ErrorKind::TimedOut
                            ) => {}
                        Err(_) => std::thread::sleep(POLL_INTERVAL),
                    }
                    sync_due_peers(&db, &peers, resync_interval);
                }
            }));
        }

        Ok(LanMesh {
            shutdown,
            threads,
            local_addr,
            discovery_addr,
            node_id,
            peers,
        })
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// The bound TCP address inbound sessions arrive on (beacons announce
    /// its port).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The bound UDP address that receives discovery beacons. When the
    /// configured multicast port is 0, this exposes the ephemeral port the
    /// OS selected for explicitly injected discovery.
    pub fn discovery_addr(&self) -> SocketAddr {
        self.discovery_addr
    }

    pub fn peers(&self) -> Vec<LanPeer> {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .peers
            .values()
            .map(|entry| entry.info.clone())
            .collect()
    }

    pub fn shutdown(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        for thread in self.threads {
            let _ = thread.join();
        }
    }
}

/// Dials a known peer address and runs one initiator session — the
/// no-discovery path (manual IP entry, tests, wired links).
pub fn sync_with_addr(db: &mut BicDb, addr: SocketAddr) -> Result<MeshSyncReport> {
    let mut stream = TcpStream::connect_timeout(&addr, SESSION_CONNECT_TIMEOUT)?;
    stream.set_read_timeout(Some(SESSION_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(SESSION_IO_TIMEOUT))?;
    run_mesh_initiator(db, &mut stream)
}

fn sync_with_addr_shared(db: &Mutex<BicDb>, addr: SocketAddr) -> Result<MeshSyncReport> {
    let mut stream = TcpStream::connect_timeout(&addr, SESSION_CONNECT_TIMEOUT)?;
    stream.set_read_timeout(Some(SESSION_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(SESSION_IO_TIMEOUT))?;
    run_mesh_initiator_shared(db, &mut stream)
}

fn lock_db(db: &Arc<Mutex<BicDb>>) -> std::sync::MutexGuard<'_, BicDb> {
    db.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn bind_discovery_socket(multicast_addr: &SocketAddrV4) -> Result<UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, multicast_addr.port()).into())?;
    let socket: UdpSocket = socket.into();
    // Best effort: environments without multicast still receive unicast
    // beacons on the same socket.
    let _ = socket.join_multicast_v4(multicast_addr.ip(), &Ipv4Addr::UNSPECIFIED);
    Ok(socket)
}

fn observe_beacon(
    peers: &Arc<Mutex<PeerTable>>,
    own_node: &NodeId,
    own_group: &Option<String>,
    beacon: LanBeacon,
    source: SocketAddr,
) {
    if beacon.node_id == *own_node {
        return;
    }
    if own_group.is_some() && beacon.group != *own_group {
        return;
    }
    let addr = SocketAddr::new(source.ip(), beacon.tcp_port);
    let mut table = peers
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let entry = table
        .peers
        .entry(beacon.node_id.clone())
        .or_insert_with(|| PeerEntry {
            info: LanPeer {
                node_id: beacon.node_id.clone(),
                addr,
                group: beacon.group.clone(),
                last_seen_unix_ms: 0,
                last_sync_unix_ms: None,
                last_sync_error: None,
                sessions_completed: 0,
            },
            next_attempt_at: Instant::now(),
        });
    entry.info.addr = addr;
    entry.info.group = beacon.group;
    entry.info.last_seen_unix_ms = unix_ms();
}

fn sync_due_peers(db: &Arc<Mutex<BicDb>>, peers: &Arc<Mutex<PeerTable>>, resync: Duration) {
    let now = Instant::now();
    let due: Vec<(NodeId, SocketAddr)> = {
        let table = peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        table
            .peers
            .values()
            .filter(|entry| entry.next_attempt_at <= now)
            .map(|entry| (entry.info.node_id.clone(), entry.info.addr))
            .collect()
    };
    for (node_id, addr) in due {
        let outcome = sync_with_addr_shared(db, addr);
        let mut table = peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = table.peers.get_mut(&node_id) {
            entry.next_attempt_at = Instant::now() + resync;
            match outcome {
                Ok(_) => {
                    entry.info.last_sync_unix_ms = Some(unix_ms());
                    entry.info.last_sync_error = None;
                    entry.info.sessions_completed += 1;
                }
                Err(error) => {
                    entry.info.last_sync_error = Some(error.to_string());
                }
            }
        }
    }
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
