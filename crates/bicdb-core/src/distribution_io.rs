//! A whole-request deadline, including progress made by a slow peer.
//! Socket inactivity timeouts alone restart after every successful read/write.
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use crate::CancellationToken;

pub(crate) struct DeadlineTcpStream {
    stream: TcpStream,
    cancellation: CancellationToken,
    deadline: Instant,
}

impl DeadlineTcpStream {
    pub(crate) fn new(
        stream: TcpStream,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Self {
        Self {
            stream,
            cancellation,
            deadline,
        }
    }

    fn remaining(&self) -> io::Result<Duration> {
        self.cancellation.check().map_err(|error| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("cluster RPC interrupted: {error}"),
            )
        })?;
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "cluster RPC deadline expired"))
    }

    fn poll_timeout(&self) -> io::Result<Duration> {
        Ok(self.remaining()?.min(Duration::from_millis(25)))
    }
}

fn retryable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
    )
}

impl Read for DeadlineTcpStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            self.stream.set_read_timeout(Some(self.poll_timeout()?))?;
            match self.stream.read(bytes) {
                Err(error) if retryable(&error) => continue,
                result => {
                    self.remaining()?;
                    return result;
                }
            }
        }
    }
}

impl Write for DeadlineTcpStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        loop {
            self.stream.set_write_timeout(Some(self.poll_timeout()?))?;
            match self.stream.write(bytes) {
                Err(error) if retryable(&error) => continue,
                result => {
                    self.remaining()?;
                    return result;
                }
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.remaining()?;
        self.stream.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    fn connected() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (peer, _) = listener.accept().unwrap();
        (client, peer)
    }

    #[test]
    fn silent_peer_observes_request_deadline() {
        let (client, _peer) = connected();
        let started = Instant::now();
        let mut client = DeadlineTcpStream::new(
            client,
            CancellationToken::uncancelable(),
            started + Duration::from_millis(80),
        );
        assert_eq!(
            client.read_exact(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn continued_progress_cannot_extend_request_deadline() {
        let (client, mut peer) = connected();
        let (done, wait) = mpsc::channel();
        let writer = thread::spawn(move || {
            while wait.recv_timeout(Duration::from_millis(10)).is_err() {
                if peer.write_all(&[1]).is_err() {
                    break;
                }
            }
        });
        let started = Instant::now();
        let mut client = DeadlineTcpStream::new(
            client,
            CancellationToken::uncancelable(),
            started + Duration::from_millis(80),
        );
        let result = client.read_exact(&mut [0; 1024]);
        let _ = done.send(());
        writer.join().unwrap();
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn cancellation_interrupts_a_silent_peer() {
        let (client, _peer) = connected();
        let token = CancellationToken::uncancelable().child();
        let cancel = token.clone();
        let started = Instant::now();
        let trigger = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            cancel.cancel();
        });
        let mut client = DeadlineTcpStream::new(client, token, started + Duration::from_secs(10));
        let result = client.read_exact(&mut [0]);
        trigger.join().unwrap();
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn healthy_response_is_preserved() {
        let (client, mut peer) = connected();
        peer.write_all(b"complete").unwrap();
        let mut client = DeadlineTcpStream::new(
            client,
            CancellationToken::uncancelable(),
            Instant::now() + Duration::from_secs(2),
        );
        let mut actual = [0; 8];
        client.read_exact(&mut actual).unwrap();
        assert_eq!(&actual, b"complete");
    }

    #[test]
    fn parent_deadline_is_not_extended_by_socket_budget() {
        let (client, _peer) = connected();
        let started = Instant::now();
        let token = CancellationToken::uncancelable()
            .child_with_deadline(started + Duration::from_millis(50));
        let mut client = DeadlineTcpStream::new(client, token, started + Duration::from_secs(10));
        assert_eq!(
            client.read_exact(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn blocked_writer_observes_request_deadline() {
        let (client, _peer) = connected();
        let bytes = vec![0; 32 * 1024 * 1024];
        let started = Instant::now();
        let mut client = DeadlineTcpStream::new(
            client,
            CancellationToken::uncancelable(),
            started + Duration::from_millis(80),
        );
        assert_eq!(
            client.write_all(&bytes).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
