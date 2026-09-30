//! Small standalone server for integration tests and durable queue deployments.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or("usage: serve PATH [PORT] [HOST]")?;
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
