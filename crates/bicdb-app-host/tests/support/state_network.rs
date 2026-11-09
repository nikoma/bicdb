//! Disconnect native replication links while leaving all HTTP services alive.
use std::{
    collections::BTreeMap,
    io,
    net::{Shutdown, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

struct Link {
    affected: bool,
    sockets: Arc<Mutex<BTreeMap<u64, (TcpStream, TcpStream)>>>,
    task: Option<JoinHandle<()>>,
}
pub struct Network {
    running: Arc<AtomicBool>,
    isolated: Arc<AtomicBool>,
    links: Vec<Link>,
}
impl Network {
    pub fn open(ports: &[u16], reserved: &mut [Option<TcpListener>]) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let isolated = Arc::new(AtomicBool::new(false));
        let mut links = Vec::new();
        for source in 0..3 {
            for target in 0..3 {
                if source == target {
                    continue;
                }
                // Keep the original reservation: dropping and rebinding it
                // lets another parallel group claim this port in between.
                let listener = reserved[7 + source * 3 + target].take().unwrap();
                listener.set_nonblocking(true).unwrap();
                let sockets = Arc::new(Mutex::new(BTreeMap::new()));
                let affected = source == 0 || target == 0;
                let active = running.clone();
                let fault = isolated.clone();
                let peers = sockets.clone();
                let destination = ports[3 + target];
                let task = thread::spawn(move || {
                    let mut next_connection = 0_u64;
                    while active.load(Ordering::Acquire) {
                        let client = match listener.accept() {
                            Ok((socket, _)) => socket,
                            Err(_) => {
                                thread::sleep(Duration::from_millis(10));
                                continue;
                            }
                        };
                        if affected && fault.load(Ordering::Acquire) {
                            continue;
                        }
                        let Ok(upstream) = TcpStream::connect_timeout(
                            &format!("127.0.0.1:{destination}").parse().unwrap(),
                            Duration::from_millis(300),
                        ) else {
                            continue;
                        };
                        let Ok(client_registered) = client.try_clone() else {
                            continue;
                        };
                        let Ok(upstream_registered) = upstream.try_clone() else {
                            continue;
                        };
                        let Ok(mut client_back) = client.try_clone() else {
                            continue;
                        };
                        let Ok(mut upstream_back) = upstream.try_clone() else {
                            continue;
                        };
                        let mut sockets = peers.lock().unwrap();
                        if (affected && fault.load(Ordering::Acquire))
                            || !active.load(Ordering::Acquire)
                        {
                            let _ = client.shutdown(Shutdown::Both);
                            let _ = upstream.shutdown(Shutdown::Both);
                            continue;
                        }
                        let connection = next_connection;
                        next_connection += 1;
                        sockets.insert(connection, (client_registered, upstream_registered));
                        drop(sockets);
                        let registry = Arc::clone(&peers);
                        thread::spawn(move || {
                            let back = thread::spawn(move || {
                                let _ = io::copy(&mut upstream_back, &mut client_back);
                                let _ = client_back.shutdown(Shutdown::Both);
                            });
                            let mut client = client;
                            let mut upstream = upstream;
                            let _ = io::copy(&mut client, &mut upstream);
                            let _ = upstream.shutdown(Shutdown::Both);
                            let _ = back.join();
                            // Clones retained only for active fault injection
                            // must not keep historical connections open forever.
                            registry.lock().unwrap().remove(&connection);
                        });
                    }
                });
                links.push(Link {
                    affected,
                    sockets,
                    task: Some(task),
                });
            }
        }
        Self {
            running,
            isolated,
            links,
        }
    }
    pub fn isolate(&self) {
        self.isolated.store(true, Ordering::Release);
        for link in &self.links {
            if link.affected {
                for (client, upstream) in link.sockets.lock().unwrap().values() {
                    let _ = client.shutdown(Shutdown::Both);
                    let _ = upstream.shutdown(Shutdown::Both);
                }
            }
        }
    }
    pub fn heal(&self) {
        self.isolated.store(false, Ordering::Release);
    }
}
impl Drop for Network {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
        for link in &mut self.links {
            for (client, upstream) in link.sockets.lock().unwrap().values() {
                let _ = client.shutdown(Shutdown::Both);
                let _ = upstream.shutdown(Shutdown::Both);
            }
            if let Some(task) = link.task.take() {
                let _ = task.join();
            }
        }
    }
}

#[test]
fn finished_forwarded_connections_release_fault_injection_socket_clones() {
    use std::io::{Read, Write};
    let listeners: Vec<_> = (0..19)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect();
    let mut reservations: Vec<_> = listeners.into_iter().map(Some).collect();
    let network = Network::open(&ports, &mut reservations);
    // Source0 -> target1 uses proxy slot8 and destination slot4.
    for _ in 0..64 {
        let mut client = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, ports[8])).unwrap();
        let mut upstream = reservations[4].as_ref().unwrap().accept().unwrap().0;
        upstream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client.write_all(b"x").unwrap();
        let mut byte = [0];
        upstream.read_exact(&mut byte).unwrap();
        assert_eq!(&byte, b"x");
        drop((client, upstream));
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !network.links[0].sockets.lock().unwrap().is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "completed proxy connection retained socket clones"
            );
            thread::sleep(Duration::from_millis(2));
        }
    }
}
