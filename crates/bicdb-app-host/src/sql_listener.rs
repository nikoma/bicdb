//! Operator-owned, authenticated SQL access to the supervisor's existing database.
//! Applications cannot enable a listener through a release manifest.
use crate::database::Database;
use anyhow::{ensure, Context, Result};
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use serde::Deserialize;
use std::{
    io::Read,
    net::{SocketAddr, TcpListener},
    path::Path,
    sync::Arc,
    thread,
    time::Duration,
};

pub const CONFIG_FILE: &str = "sql-listener.json";
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    format: u32,
    listen: SocketAddr,
    #[serde(default = "connections")]
    max_connections: usize,
}
fn connections() -> usize {
    64
}

pub struct SqlListener {
    server: Arc<PgWireServer>,
    task: Option<thread::JoinHandle<()>>,
    address: SocketAddr,
}
impl SqlListener {
    pub fn open(app_dir: &Path, database: &Database, replicated: bool) -> Result<Option<Self>> {
        let path = app_dir.join(CONFIG_FILE);
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = match options.open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "SQL listener configuration must be a regular file"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            ensure!(
                metadata.permissions().mode() & 0o077 == 0,
                "SQL listener configuration must have mode 0600"
            );
            ensure!(
                metadata.uid() == unsafe { libc::geteuid() },
                "SQL listener configuration must belong to the supervisor's user"
            );
        }
        let mut bytes = Vec::new();
        file.take(16 * 1024 + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() <= 16 * 1024,
            "SQL listener configuration is too large"
        );
        let configuration: Configuration = serde_json::from_slice(&bytes)?;
        ensure!(
            configuration.format == 1,
            "unsupported SQL listener configuration"
        );
        ensure!(
            configuration.listen.ip().is_loopback(),
            "application SQL listeners must bind to loopback"
        );
        ensure!(
            (1..=512).contains(&configuration.max_connections),
            "invalid SQL connection limit"
        );
        // Replicated databases can be replaced during recovery. Refuse this
        // binding until the listener follows that exact writer lifecycle.
        ensure!(
            !replicated,
            "application SQL listener does not yet support replicated state groups"
        );
        let listener =
            TcpListener::bind(configuration.listen).context("bind application SQL listener")?;
        let address = listener.local_addr()?;
        let server = PgWireServer::open_with_shared(
            app_dir.join("database/data"),
            PgWireConfig {
                host: address.ip().to_string(),
                port: address.port(),
                require_auth: true,
                fsync: true,
                max_connections: configuration.max_connections,
                max_connections_per_ip: configuration.max_connections,
                shutdown_grace_period: Duration::from_secs(2),
                ..Default::default()
            },
            database.db.clone(),
        )?;
        let serving = server.clone();
        let task = thread::Builder::new()
            .name("bic-application-sql".into())
            .spawn(move || {
                if bicdb_pgwire::serve_existing_listener(serving, listener).is_err() {
                    eprintln!("application SQL listener stopped with an error");
                }
            })?;
        Ok(Some(Self {
            server,
            task: Some(task),
            address,
        }))
    }
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    pub fn shutdown(&self) {
        self.server.request_shutdown();
    }
    pub fn healthy(&self) -> bool {
        !self.server.is_shutdown_requested()
            && self.task.as_ref().is_some_and(|task| !task.is_finished())
    }
}
impl Drop for SqlListener {
    fn drop(&mut self) {
        self.shutdown();
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}
