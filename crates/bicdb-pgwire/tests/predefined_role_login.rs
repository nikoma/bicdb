//! Predefined roles (pg_read_all_data, ...) exist as NOLOGIN roles. A
//! connection naming one is refused before any session runs: logging in as
//! pg_read_all_data would otherwise read every table.
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn connect_as(user: &str) -> (u8, thread::JoinHandle<bool>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).is_ok()
    });
    let mut client = TcpStream::connect(address).unwrap();
    let mut payload = Vec::new();
    payload.extend_from_slice(&196_608_i32.to_be_bytes());
    for part in ["user", user] {
        payload.extend_from_slice(part.as_bytes());
        payload.push(0);
    }
    payload.push(0);
    client
        .write_all(&((payload.len() as i32) + 4).to_be_bytes())
        .unwrap();
    client.write_all(&payload).unwrap();
    let mut tag = [0_u8; 1];
    client.read_exact(&mut tag).unwrap();
    (tag[0], server, dir)
}

#[test]
fn predefined_roles_cannot_log_in() {
    for role in ["pg_read_all_data", "pg_write_all_data", "pg_maintain"] {
        let (tag, server, _dir) = connect_as(role);
        // ErrorResponse, never AuthenticationOk.
        assert_eq!(tag, b'E', "{role}");
        let _ = server.join();
    }
    let (tag, server, _dir) = connect_as("bicdb");
    assert_eq!(tag, b'R');
    drop(server);
}
