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
    if args.next().is_some()
        || (profile != bicdb_sql::oracle::POSTGRES_PROFILE
            && !bicdb_sql::oracle::is_oracle_profile(&profile))
    {
        return Err(
            "expected: oracle_server PATH PORT [oracle-preview-v1|oracle-hr-v1|postgres]".into(),
        );
    }
    let server = PgWireServer::open(
        path,
        PgWireConfig {
            sql_profile: profile,
            ..Default::default()
        },
    )?;
    let stop = server.clone();
    ctrlc::set_handler(move || stop.request_shutdown())?;
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    println!("BicDB compatibility server: {}", listener.local_addr()?);
    bicdb_pgwire::serve_existing_listener(server, listener)?;
    Ok(())
}
