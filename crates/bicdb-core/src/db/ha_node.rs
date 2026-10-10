//! Runtime replication role, epochs, fencing and durable standby positions.
//!
//! A replicating node is a primary, a standby, or fenced. The role starts from
//! the server configuration but is persisted once it changes (promotion,
//! fencing), and the persisted role wins at the next start: a standby that was
//! promoted stays primary after a restart, and a fenced former primary stays
//! fenced until an operator re-seeds it.
//!
//! Every primary term has an *epoch*. Replication messages carry it; a node
//! that sees a higher epoch than its own while acting as primary has been
//! superseded and fences itself, so a primary that was cut off can never keep
//! acknowledging writes once another node has taken over. The node also records
//! at which commit sequence number each epoch began, which tells whether a
//! returning node's history is a prefix of the current primary's.
//!
//! The positions standbys have acknowledged ("slots") are persisted too, so the
//! transaction log stays retained for them across primary restarts.
use super::*;

/// File (in the database directory) holding [`HaNodeFile`].
pub const HA_NODE_FILE: &str = "ha-node.json";

/// Separates a read-only refusal from the current primary's client address.
pub(crate) const PRIMARY_MARKER: &str = "; current primary: ";

/// Client hint for a read-only refusal ([`BicDbError::ReadOnlyStandby`]):
/// where writes should go, when the primary's address is known.
pub fn read_only_standby_hint(message: &str) -> Option<String> {
    let (_, primary) = message.rsplit_once(PRIMARY_MARKER)?;
    Some(format!("send writes to the primary at {primary}"))
}

/// What this node currently does in a replication group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Primary,
    Standby,
    /// A former primary that saw a newer epoch: refuses writes.
    Fenced,
}

impl NodeRole {
    fn code(self) -> u8 {
        match self {
            Self::Primary => 0,
            Self::Standby => 1,
            Self::Fenced => 2,
        }
    }

    fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Primary,
            1 => Self::Standby,
            _ => Self::Fenced,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct HaNodeFile {
    #[serde(default)]
    pub(crate) epoch: u64,
    /// Persisted role; overrides the configured one when present.
    #[serde(default)]
    pub(crate) role: Option<NodeRole>,
    /// Vote cast in an election: (epoch, candidate).
    #[serde(default)]
    pub(crate) voted: Option<(u64, String)>,
    /// Acknowledged commit sequence per standby.
    #[serde(default)]
    pub(crate) slots: BTreeMap<String, u64>,
    /// Commit sequence number after which each epoch's commits begin.
    #[serde(default)]
    pub(crate) epoch_starts: BTreeMap<u64, u64>,
    /// Client-facing address of the current primary, as last announced.
    #[serde(default)]
    pub(crate) primary_sql_address: Option<String>,
}

#[derive(Debug)]
pub(crate) struct HaNode {
    file: Mutex<HaNodeFile>,
    role: AtomicU8,
    epoch: AtomicU64,
    path: PathBuf,
    fsync: bool,
}

impl HaNode {
    pub(crate) fn load(root: &Path, configured: NodeRole, fsync: bool) -> Result<Self> {
        let path = root.join(HA_NODE_FILE);
        let file = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<HaNodeFile>(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => HaNodeFile::default(),
            Err(error) => return Err(error.into()),
        };
        let role = file.role.unwrap_or(configured);
        Ok(Self {
            role: AtomicU8::new(role.code()),
            epoch: AtomicU64::new(file.epoch),
            file: Mutex::new(file),
            path,
            fsync,
        })
    }

    pub(crate) fn role(&self) -> NodeRole {
        NodeRole::from_code(self.role.load(AtomicOrdering::Acquire))
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch.load(AtomicOrdering::Acquire)
    }

    pub(crate) fn with_file<T>(&self, change: impl FnOnce(&mut HaNodeFile) -> T) -> Result<T> {
        let mut file = self.file.lock();
        let value = change(&mut file);
        storage::write_atomic(&self.path, &serde_json::to_vec_pretty(&*file)?, self.fsync)?;
        self.role.store(
            file.role.unwrap_or_else(|| self.role()).code(),
            AtomicOrdering::Release,
        );
        self.epoch.store(file.epoch, AtomicOrdering::Release);
        Ok(value)
    }

    pub(crate) fn read<T>(&self, read: impl FnOnce(&HaNodeFile) -> T) -> T {
        read(&self.file.lock())
    }
}

/// Replication role and epoch as seen from outside.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HaNodeStatus {
    pub role: NodeRole,
    pub epoch: u64,
    pub primary_sql_address: Option<String>,
    pub epoch_starts: BTreeMap<u64, u64>,
}

impl BicDb {
    /// This node's replication role (meaningful when replication is enabled).
    pub fn replication_role(&self) -> NodeRole {
        self.ha_node.role()
    }

    /// The highest primary epoch this node knows of.
    pub fn replication_epoch(&self) -> u64 {
        self.ha_node.epoch()
    }

    pub fn ha_node_status(&self) -> HaNodeStatus {
        self.ha_node.read(|file| HaNodeStatus {
            role: self.ha_node.role(),
            epoch: file.epoch,
            primary_sql_address: file.primary_sql_address.clone(),
            epoch_starts: file.epoch_starts.clone(),
        })
    }

    /// Records a newer epoch announced by a peer. A primary that learns of a
    /// newer epoch has been superseded and fences itself; returns true then.
    pub fn observe_replication_epoch(&self, epoch: u64) -> Result<bool> {
        if epoch <= self.ha_node.epoch() {
            return Ok(false);
        }
        let was_primary = self.ha_node.role() == NodeRole::Primary;
        self.ha_node.with_file(|file| {
            file.epoch = file.epoch.max(epoch);
            if was_primary {
                file.role = Some(NodeRole::Fenced);
            }
        })?;
        if was_primary {
            // A superseded primary must not acknowledge anything more.
            self.sync_quorum
                .close("a newer primary took over and this node is fenced");
            eprintln!(
                "bicdb replication: epoch {epoch} is newer than this primary's; fenced (writes refused)"
            );
        }
        Ok(was_primary)
    }

    /// Fences this node: it refuses writes until it is re-seeded.
    pub fn fence_replication(&self) -> Result<()> {
        self.ha_node
            .with_file(|file| file.role = Some(NodeRole::Fenced))?;
        self.sync_quorum.close("this node is fenced");
        Ok(())
    }

    /// Remembers the client-facing address of the current primary.
    pub fn record_primary_sql_address(&self, address: Option<String>) -> Result<()> {
        if self
            .ha_node
            .read(|file| file.primary_sql_address == address)
        {
            return Ok(());
        }
        self.ha_node
            .with_file(|file| file.primary_sql_address = address)
    }

    /// Persists the standbys' acknowledged positions (log retention survives
    /// a primary restart).
    pub fn persist_standby_slots(&self) -> Result<()> {
        let slots = self.sync_quorum.acknowledged();
        if self.ha_node.read(|file| file.slots == slots) {
            return Ok(());
        }
        self.ha_node.with_file(|file| file.slots = slots)
    }

    /// Forgets standbys that are no longer allowed to connect, so their old
    /// positions stop pinning the log.
    pub fn retain_standby_slots(&self, allowed: &[String]) -> Result<()> {
        self.sync_quorum.retain_standbys(allowed);
        self.persist_standby_slots()
    }
}
