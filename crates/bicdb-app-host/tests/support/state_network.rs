//! Disconnect native replication links while leaving all HTTP services alive.
use std::{
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
    sockets: Arc<Mutex<Vec<TcpStream>>>,
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
                let sockets = Arc::new(Mutex::new(Vec::new()));
                let affected = source == 0 || target == 0;
                let active = running.clone();
                let fault = isolated.clone();
                let peers = sockets.clone();
                let destination = ports[3 + target];
                let task = thread::spawn(move || {
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
                        let mut sockets = peers.lock().unwrap();
                        sockets.push(client.try_clone().unwrap());
                        sockets.push(upstream.try_clone().unwrap());
                        if (affected && fault.load(Ordering::Acquire))
                            || !active.load(Ordering::Acquire)
                        {
                            let _ = client.shutdown(Shutdown::Both);
                            let _ = upstream.shutdown(Shutdown::Both);
                            continue;
                        }
                        drop(sockets);
                        thread::spawn(move || {
                            let mut client_back = client.try_clone().unwrap();
                            let mut upstream_back = upstream.try_clone().unwrap();
                            let back = thread::spawn(move || {
                                let _ = io::copy(&mut upstream_back, &mut client_back);
                                let _ = client_back.shutdown(Shutdown::Both);
                            });
                            let mut client = client;
                            let mut upstream = upstream;
                            let _ = io::copy(&mut client, &mut upstream);
                            let _ = upstream.shutdown(Shutdown::Both);
                            let _ = back.join();
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
                for socket in link.sockets.lock().unwrap().iter() {
                    let _ = socket.shutdown(Shutdown::Both);
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
            for socket in link.sockets.lock().unwrap().iter() {
                let _ = socket.shutdown(Shutdown::Both);
            }
            if let Some(task) = link.task.take() {
                let _ = task.join();
            }
        }
    }
}
