//! Small standalone server for integration tests and durable queue deployments.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or("usage: serve PATH [PORT] [HOST]")?;
    if path == "--health" {
        use std::io::{Read, Write};
        use std::time::Duration;
        let addr = args
            .next()
            .unwrap_or_else(|| "127.0.0.1:6379".into())
            .parse()?;
        let mut stream = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        if let Ok(password) = std::env::var("BICDB_CACHE_PASSWORD") {
            stream.write_all(
                format!(
                    "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
                    password.len(),
                    password
                )
                .as_bytes(),
            )?;
            let mut reply = [0; 5];
            stream.read_exact(&mut reply)?;
            if &reply != b"+OK\r\n" {
                return Err("queue authentication probe failed".into());
            }
        }
        stream.write_all(b"*1\r\n$4\r\nPING\r\n")?;
        let mut reply = [0; 7];
        stream.read_exact(&mut reply)?;
        if &reply != b"+PONG\r\n" {
            return Err("queue probe failed".into());
        }
        return Ok(());
    }
    let port = args.next().map(|s| s.parse()).transpose()?.unwrap_or(6379);
    let host = args.next().unwrap_or_else(|| "127.0.0.1".into());
    let config = bicdb_resp::RespConfig {
        port,
        host,
        fsync: true,
        password: std::env::var("BICDB_CACHE_PASSWORD").ok(),
        ..bicdb_resp::RespConfig::default()
    };
    bicdb_resp::serve(path, config)?;
    Ok(())
}
