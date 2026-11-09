//! SQL activation happens after node startup and deployment, so releasing an
//! ephemeral reservation at startup leaves time for an outgoing socket to take
//! its port. Keep SQL ports outside the OS outgoing range and remember every
//! allocation for this test process, including after the OS reservation drops.
use std::{
    collections::BTreeSet,
    net::TcpListener,
    sync::{Mutex, OnceLock},
};

fn outgoing_range() -> (u16, u16) {
    #[cfg(target_os = "linux")]
    {
        static RANGE: OnceLock<(u16, u16)> = OnceLock::new();
        *RANGE.get_or_init(|| {
            let text = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
                .expect("read OS ephemeral range without changing host networking");
            let ports: Vec<u16> = text
                .split_whitespace()
                .map(|s| s.parse().unwrap())
                .collect();
            assert_eq!(ports.len(), 2);
            assert!(ports[0] <= ports[1]);
            (ports[0], ports[1])
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        (32_768, 65_535)
    }
}

pub fn reserve() -> TcpListener {
    static CLAIMED: OnceLock<Mutex<BTreeSet<u16>>> = OnceLock::new();
    let mut claimed = CLAIMED.get_or_init(Default::default).lock().unwrap();
    let (low, high) = outgoing_range();
    let start = (uuid::Uuid::new_v4().as_u128() % 55_536) as u32;
    for step in 0..55_536 {
        let port = (10_000 + (start + step) % 55_536) as u16;
        if (low..=high).contains(&port) || claimed.contains(&port) {
            continue;
        }
        if let Ok(listener) = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)) {
            claimed.insert(port);
            return listener;
        }
    }
    panic!("no unclaimed SQL fixture port outside the OS outgoing range");
}

#[test]
fn delayed_sql_reservations_are_outside_outgoing_range_and_not_reallocated() {
    let first = reserve();
    let port = first.local_addr().unwrap().port();
    let (low, high) = outgoing_range();
    assert!(!(low..=high).contains(&port));
    drop(first);
    // Logical ownership survives the socket handoff to the child process.
    let later = reserve();
    assert_ne!(later.local_addr().unwrap().port(), port);
    let destination = TcpListener::bind("127.0.0.1:0").unwrap();
    let clients: Vec<_> = (0..32)
        .map(|_| {
            let client = std::net::TcpStream::connect(destination.local_addr().unwrap()).unwrap();
            let accepted = destination.accept().unwrap().0;
            assert!((low..=high).contains(&client.local_addr().unwrap().port()));
            (client, accepted)
        })
        .collect();
    let sql = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).unwrap();
    drop((sql, clients));
}

#[cfg(target_os = "linux")]
#[test]
fn outgoing_source_bind_reproduces_old_delayed_ephemeral_listener_collision() {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    let destination = TcpListener::bind("127.0.0.1:0").unwrap();
    let old = TcpListener::bind("127.0.0.1:0").unwrap();
    let released = old.local_addr().unwrap().port();
    drop(old);
    // Force the source choice the kernel may make for a concurrent outgoing
    // connection, so the original failure does not depend on random port reuse.
    let raw = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
    assert!(raw >= 0);
    let socket = unsafe { OwnedFd::from_raw_fd(raw) };
    let address = |port: u16| libc::sockaddr_in {
        sin_family: libc::AF_INET as _,
        sin_port: port.to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes([127, 0, 0, 1]),
        },
        sin_zero: [0; 8],
    };
    let source = address(released);
    assert_eq!(
        unsafe {
            libc::bind(
                socket.as_raw_fd(),
                &source as *const _ as *const libc::sockaddr,
                std::mem::size_of_val(&source) as _,
            )
        },
        0
    );
    let target = address(destination.local_addr().unwrap().port());
    assert_eq!(
        unsafe {
            libc::connect(
                socket.as_raw_fd(),
                &target as *const _ as *const libc::sockaddr,
                std::mem::size_of_val(&target) as _,
            )
        },
        0
    );
    assert_eq!(
        TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, released))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::AddrInUse
    );
}
