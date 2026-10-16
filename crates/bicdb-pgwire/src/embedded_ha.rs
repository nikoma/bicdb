//! Native replication for hosts that execute SQL in process and do not expose
//! a PostgreSQL listener. The same lease, election and synchronous-acknowledgment
//! paths serve both embedding hosts and protocol clients.
use super::*;

pub struct EmbeddedHaDatabase {
    server: Arc<PgWireServer>,
    authority: bicdb_core::ReplicationWriterAuthority,
}

/// A lease probe that does not hold the database open or take a database lock.
/// Compute supervisors can stop renewal even while a SQL operation is blocked.
#[derive(Clone)]
pub struct HaWriterLease {
    server: Weak<PgWireServer>,
    authority: bicdb_core::ReplicationWriterAuthority,
}
impl HaWriterLease {
    pub fn active(&self) -> bool {
        self.server.upgrade().is_some_and(|server| {
            !server.is_shutdown_requested()
                && self.authority.active()
                && server
                    .config
                    .high_availability
                    .as_ref()
                    .is_some_and(|ha| matches!(ha.role, HaServerRole::Primary { .. }))
        })
    }
}

impl EmbeddedHaDatabase {
    /// Open a database with the supplied storage/audit settings and the resolved
    /// native HA role. This starts replication services, never a SQL listener.
    pub fn open(path: impl AsRef<Path>, config: PgWireConfig, database: DbConfig) -> Result<Self> {
        Self::open_with_initializer(path, config, database, |_| Ok(()))
    }

    /// Validate or bootstrap host-owned storage before any peer can receive
    /// it. Work here is local initialization, not an acknowledged quorum write.
    pub fn open_with_initializer(
        path: impl AsRef<Path>,
        mut config: PgWireConfig,
        database: DbConfig,
        initialize: impl FnOnce(&mut BicDb) -> Result<()>,
    ) -> Result<Self> {
        let path = path.as_ref();
        let ha = config
            .high_availability
            .as_ref()
            .ok_or_else(|| PgWireError::Server("embedded HA requires replication settings".into()))?
            .resolved(bicdb_core::read_persisted_ha_role(path)?);
        config.high_availability = Some(ha.clone());
        let mut database = BicDb::open_with_config(
            path,
            database
                .with_fsync(config.fsync)
                .with_replication(ha.database_replication()),
        )?;
        initialize(&mut database)?;
        let authority = database.replication_writer_authority();
        let server = PgWireServer::open_with_shared(path, config, Arc::new(RwLock::new(database)))?;
        if let Err(error) = high_availability::start(server.clone(), ha) {
            server.request_shutdown();
            server.wait_for_background_quiesce(BACKGROUND_QUIESCE_TIMEOUT);
            return Err(error);
        }
        Ok(Self { server, authority })
    }

    /// Trusted host access to the engine. Mutations through this handle must
    /// release their database guards before calling acknowledge_current().
    pub fn database(&self) -> Arc<RwLock<BicDb>> {
        self.server.db.clone()
    }

    /// Execute under the normal host write admission gate and withhold success
    /// until the required native standbys durably acknowledge the resulting WAL.
    /// A callback may commit several transactions; it owns their atomicity.
    pub fn with_write<T>(&self, work: impl FnOnce(&mut BicDb) -> Result<T>) -> Result<T> {
        if !self.writer_lease().active() {
            return Err(PgWireError::Server(
                "state replica has no active writer authority".into(),
            ));
        }
        let admission = self.server.try_admit_write()?;
        let result = {
            let mut db = self.server.write_db_with_admission(&admission)?;
            if db.is_read_only_standby() {
                return Err(PgWireError::Server(
                    "embedded state replica is not writable".into(),
                ));
            }
            work(&mut db)
        };
        drop(admission);
        let value = result?;
        self.acknowledge_current()?;
        Ok(value)
    }

    /// Wait without holding a database guard. A timeout is indeterminate: the
    /// transaction can be durable locally, so callers must retry idempotently.
    pub fn acknowledge_current(&self) -> Result<()> {
        if !self.writer_lease().active() {
            return Err(PgWireError::Server(
                "state replica has no active writer authority".into(),
            ));
        }
        self.server
            .sync_quorum
            .wait_for(self.server.replication_feed.durable_seq())?;
        Ok(())
    }

    pub fn writer_lease(&self) -> HaWriterLease {
        HaWriterLease {
            server: Arc::downgrade(&self.server),
            authority: self.authority.clone(),
        }
    }

    /// Promotion, demotion and bootstrap request a restart of this engine.
    pub fn restart_required(&self) -> bool {
        self.server.is_shutdown_requested()
    }

    pub fn request_shutdown(&self) {
        self.server.request_shutdown();
    }

    /// Stop replication before releasing the database. Failure means another
    /// service still holds it; the caller must not claim a completed handoff.
    pub fn shutdown(&self, timeout: Duration) -> Result<()> {
        self.request_shutdown();
        if !self.server.wait_for_background_quiesce(timeout) {
            return Err(PgWireError::Server(
                "embedded replication services did not stop within the shutdown deadline".into(),
            ));
        }
        self.server.flush()
    }
}

impl Drop for EmbeddedHaDatabase {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown(BACKGROUND_QUIESCE_TIMEOUT) {
            eprintln!("embedded replication shutdown failed: {error}");
        }
    }
}
