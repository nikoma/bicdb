//! Minimal local compatibility-development server using the production pgwire
//! server and SQL engine. Public deployments should use the full CLI's server
//! controls instead. No serverless/application host is needed for SQL tests.
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = PathBuf::from(args.next().ok_or("expected database directory")?);
    let port: u16 = args.next().ok_or("expected localhost port")?.parse()?;
    let profile = args.next().unwrap_or_else(|| "oracle-preview-v1".into());
    let max_connections = args
        .next()
        .map(|value| value.parse::<usize>())
        .transpose()?;
    let max_active_queries = args
        .next()
        .map(|value| value.parse::<usize>())
        .transpose()?;
    if max_active_queries.is_some_and(|limit| limit == 0 || limit > 256)
        || max_connections.is_some_and(|limit| limit == 0 || limit > 1000)
        || args.next().is_some()
        || (profile != bicdb_sql::oracle::POSTGRES_PROFILE
            && !bicdb_sql::oracle::is_oracle_profile(&profile))
    {
        return Err(
            "expected: oracle_server PATH PORT [oracle-preview-v1|oracle-hr-v1|postgres] [MAX_CONNECTIONS(1..1000)] [MAX_ACTIVE_QUERIES(1..256)]".into(),
        );
    }
    let mut config = PgWireConfig {
        sql_profile: profile,
        ..Default::default()
    };
    // A local application can use several JDBC pools from one loopback IP.
    // Keep limits explicit for that workload without changing server defaults.
    if let Some(limit) = max_connections {
        config.max_connections = limit;
        config.max_connections_per_ip = limit;
    }
    if let Some(limit) = max_active_queries {
        config.max_active_queries = limit;
        config.max_active_reads = limit;
        config.max_active_writes = limit;
    }
    let server = PgWireServer::open(path, config)?;
    let stop = server.clone();
    ctrlc::set_handler(move || stop.request_shutdown())?;
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    println!("BicDB compatibility server: {}", listener.local_addr()?);
    bicdb_pgwire::serve_existing_listener(server, listener)?;
    Ok(())
}
