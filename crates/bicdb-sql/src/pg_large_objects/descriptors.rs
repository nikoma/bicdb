//! Connection-owned descriptors. Cursor changes survive subtransaction rollback;
//! handles opened after its mark do not. No descriptor retains a database pointer.
use super::*;
use std::sync::{Mutex, MutexGuard};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DescriptorTransaction {
    database: u64,
    transaction: bicdb_core::TransactionId,
}

impl DescriptorTransaction {
    /// The host holds the matching database guard, as for native transactions.
    pub(super) fn new(db: &BicDb, tx: &Transaction) -> Self {
        Self {
            database: db.instance_id(),
            transaction: tx.id(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct DescriptorSavepoint {
    transaction: DescriptorTransaction,
    next_generation: u64,
}

#[derive(Clone)]
pub(super) enum Access {
    Read(storage::ReadView),
    // Authorization and existence are checked by the SQL host at open. A write
    // descriptor also permits reads through the current command's snapshot.
    Write(u32),
}

impl Access {
    fn oid(&self) -> u32 {
        match self {
            Self::Read(view) => view.oid(),
            Self::Write(oid) => *oid,
        }
    }

    fn retained_bytes(&self) -> usize {
        match self {
            Self::Read(view) => view.retained_bytes(),
            Self::Write(_) => 0,
        }
    }
}

struct Descriptor {
    access: Access,
    position: u64,
    generation: u64,
    cursor_version: u64,
}

#[derive(Default)]
struct Registry {
    transaction: Option<DescriptorTransaction>,
    next_generation: u64,
    descriptors: BTreeMap<i32, Descriptor>,
}

impl Registry {
    fn enter(&mut self, transaction: DescriptorTransaction) {
        if self.transaction != Some(transaction) {
            self.descriptors.clear();
            self.next_generation = 0;
            self.transaction = Some(transaction);
        }
    }

    fn require(&self, transaction: DescriptorTransaction) -> Result<()> {
        if self.transaction != Some(transaction) {
            return Err(SqlError::data_exception(
                "25000",
                "large-object descriptor transaction has ended",
                None,
            ));
        }
        Ok(())
    }

    fn retained_bytes(&self) -> usize {
        self.descriptors
            .values()
            .fold(std::mem::size_of::<Self>() + 64, |sum, descriptor| {
                sum.saturating_add(std::mem::size_of::<Descriptor>() + 128)
                    .saturating_add(descriptor.access.retained_bytes())
            })
    }
}

/// An I/O operation reserves the descriptor's current position, without lending
/// out the registry lock during database access. Finish validates its identity;
/// closing/reusing a descriptor or seeking cannot silently retarget an old I/O.
pub(super) struct IoTicket {
    transaction: DescriptorTransaction,
    fd: i32,
    generation: u64,
    cursor_version: u64,
    pub(super) position: u64,
    pub(super) access: Access,
}

impl std::fmt::Debug for IoTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LargeObjectIoTicket")
            .field("fd", &self.fd)
            .field("oid", &self.access.oid())
            .field("position", &self.position)
            .finish()
    }
}

#[derive(Default)]
pub(crate) struct DescriptorState {
    shared: OnceLock<Arc<Mutex<Registry>>>,
}

impl Clone for DescriptorState {
    fn clone(&self) -> Self {
        Self {
            shared: OnceLock::from(Arc::clone(self.registry())),
        }
    }
}

impl std::fmt::Debug for DescriptorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("LargeObjectDescriptors");
        if let Some(shared) = self.shared.get() {
            match shared.try_lock() {
                Ok(registry) => {
                    debug
                        .field("transaction", &registry.transaction)
                        .field("count", &registry.descriptors.len())
                        .field("retained_bytes", &registry.retained_bytes());
                }
                Err(_) => {
                    debug.field("unavailable", &true);
                }
            }
        } else {
            debug.field("count", &0usize);
        }
        debug.finish()
    }
}

fn invalid_descriptor(fd: i32) -> SqlError {
    SqlError::data_exception(
        "42704",
        format!("invalid large-object descriptor: {fd}"),
        None,
    )
}

fn invalid_seek() -> SqlError {
    SqlError::data_exception("22023", "invalid large object seek target", None)
}

impl DescriptorState {
    fn registry(&self) -> &Arc<Mutex<Registry>> {
        self.shared
            .get_or_init(|| Arc::new(Mutex::new(Registry::default())))
    }

    fn lock(&self) -> Result<MutexGuard<'_, Registry>> {
        self.registry().lock().map_err(|_| {
            SqlError::data_exception(
                "XX000",
                "large-object descriptor registry is unavailable",
                None,
            )
        })
    }

    pub(super) fn retained_bytes(&self) -> Result<usize> {
        if self.shared.get().is_none() {
            return Ok(0);
        }
        Ok(self.lock()?.retained_bytes())
    }

    pub(super) fn mark(&self, transaction: DescriptorTransaction) -> Result<DescriptorSavepoint> {
        let mut registry = self.lock()?;
        registry.enter(transaction);
        Ok(DescriptorSavepoint {
            transaction,
            next_generation: registry.next_generation,
        })
    }

    pub(super) fn rollback_to(&self, mark: DescriptorSavepoint) -> Result<()> {
        let mut registry = self.lock()?;
        registry.require(mark.transaction)?;
        registry
            .descriptors
            .retain(|_, descriptor| descriptor.generation < mark.next_generation);
        // Generations never rewind: an fd number reused after this rollback
        // must still belong to the newer subtransaction, not an older handle.
        Ok(())
    }

    /// Cleanup is independent of database health and remains safe after close.
    pub(super) fn finish(&self, transaction: DescriptorTransaction) {
        if let Some(shared) = self.shared.get() {
            let mut registry = shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if registry.transaction == Some(transaction) {
                registry.descriptors.clear();
                registry.transaction = None;
                registry.next_generation = 0;
            }
        }
    }

    pub(super) fn open(
        &self,
        transaction: DescriptorTransaction,
        access: Access,
        max_retained_bytes: usize,
    ) -> Result<i32> {
        let mut registry = self.lock()?;
        registry.enter(transaction);
        let bytes = registry
            .retained_bytes()
            .saturating_add(std::mem::size_of::<Descriptor>() + 128)
            .saturating_add(access.retained_bytes());
        if bytes > max_retained_bytes {
            return Err(SqlError::BicDb(BicDbError::QueryMemoryLimit(format!(
                "large-object descriptors retain {bytes} bytes; limit is {max_retained_bytes}"
            ))));
        }
        let mut fd = 0i32;
        for &occupied in registry.descriptors.keys() {
            if occupied != fd {
                break;
            }
            fd = fd.checked_add(1).ok_or_else(|| {
                SqlError::data_exception("54000", "too many large-object descriptors", None)
            })?;
        }
        let generation = registry.next_generation;
        registry.next_generation = generation.checked_add(1).ok_or_else(|| {
            SqlError::data_exception(
                "54000",
                "large-object descriptor generations exhausted",
                None,
            )
        })?;
        registry.descriptors.insert(
            fd,
            Descriptor {
                access,
                position: 0,
                generation,
                cursor_version: 0,
            },
        );
        Ok(fd)
    }

    pub(super) fn close(&self, transaction: DescriptorTransaction, fd: i32) -> Result<()> {
        let mut registry = self.lock()?;
        if registry.transaction != Some(transaction) || registry.descriptors.remove(&fd).is_none() {
            return Err(invalid_descriptor(fd));
        }
        Ok(())
    }

    /// Native lo_unlink closes this backend's descriptors for the object.
    /// Another backend's descriptors are independent and retain their rights.
    pub(super) fn close_object(&self, transaction: DescriptorTransaction, oid: u32) -> Result<()> {
        let mut registry = self.lock()?;
        registry.require(transaction)?;
        registry
            .descriptors
            .retain(|_, descriptor| descriptor.access.oid() != oid);
        Ok(())
    }

    pub(super) fn begin_io(
        &self,
        transaction: DescriptorTransaction,
        fd: i32,
        write: bool,
    ) -> Result<IoTicket> {
        let registry = self.lock()?;
        if registry.transaction != Some(transaction) {
            return Err(invalid_descriptor(fd));
        }
        let descriptor = registry
            .descriptors
            .get(&fd)
            .ok_or_else(|| invalid_descriptor(fd))?;
        if write && !matches!(descriptor.access, Access::Write(_)) {
            return Err(SqlError::data_exception(
                "55000",
                "large-object descriptor was not opened for writing",
                None,
            ));
        }
        Ok(IoTicket {
            transaction,
            fd,
            generation: descriptor.generation,
            cursor_version: descriptor.cursor_version,
            position: descriptor.position,
            access: descriptor.access.clone(),
        })
    }

    pub(super) fn finish_io(&self, ticket: IoTicket, bytes: usize) -> Result<()> {
        let mut registry = self.lock()?;
        if registry.transaction != Some(ticket.transaction) {
            return Err(invalid_descriptor(ticket.fd));
        }
        let descriptor = registry
            .descriptors
            .get_mut(&ticket.fd)
            .ok_or_else(|| invalid_descriptor(ticket.fd))?;
        if descriptor.generation != ticket.generation
            || descriptor.cursor_version != ticket.cursor_version
            || descriptor.position != ticket.position
        {
            return Err(invalid_descriptor(ticket.fd));
        }
        let position = ticket
            .position
            .checked_add(bytes as u64)
            .filter(|position| *position <= storage::MAX_LENGTH)
            .ok_or_else(invalid_seek)?;
        descriptor.cursor_version = descriptor.cursor_version.checked_add(1).ok_or_else(|| {
            SqlError::data_exception("54000", "large-object cursor version exhausted", None)
        })?;
        descriptor.position = position;
        Ok(())
    }

    pub(super) fn tell(&self, transaction: DescriptorTransaction, fd: i32) -> Result<u64> {
        Ok(self.begin_io(transaction, fd, false)?.position)
    }

    pub(super) fn seek(
        &self,
        transaction: DescriptorTransaction,
        fd: i32,
        offset: i64,
        whence: i32,
        current_write_length: u64,
    ) -> Result<u64> {
        let mut registry = self.lock()?;
        if registry.transaction != Some(transaction) {
            return Err(invalid_descriptor(fd));
        }
        let descriptor = registry
            .descriptors
            .get_mut(&fd)
            .ok_or_else(|| invalid_descriptor(fd))?;
        let base = match whence {
            0 => 0,
            1 => descriptor.position,
            2 => match &descriptor.access {
                Access::Read(view) => view.length,
                Access::Write(_) => current_write_length,
            },
            _ => return Err(invalid_seek()),
        };
        let position = base
            .checked_add_signed(offset)
            .filter(|position| *position <= storage::MAX_LENGTH)
            .ok_or_else(invalid_seek)?;
        descriptor.cursor_version = descriptor.cursor_version.checked_add(1).ok_or_else(|| {
            SqlError::data_exception("54000", "large-object cursor version exhausted", None)
        })?;
        descriptor.position = position;
        Ok(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_core::{DbConfig, StorageMode};

    fn fixture(mode: StorageMode) -> (tempfile::TempDir, BicDb) {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap();
        storage::ensure_storage(&mut db).unwrap();
        let mut tx = db.begin_transaction().unwrap();
        storage::create(&db, &mut tx, 42001, "bicdb", None).unwrap();
        storage::mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .write(0, b"abc", &CancellationToken::uncancelable())
            .unwrap();
        tx.commit().unwrap();
        (root, db)
    }

    fn is_state<T>(result: Result<T>, expected: &str) -> bool {
        matches!(result, Err(SqlError::DataException { sqlstate, .. }) if sqlstate == expected)
    }

    #[test]
    fn cache_transfers_keep_cursor_changes_but_savepoint_abort_closes_new_handles() {
        let cancel = CancellationToken::uncancelable();
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let (_root, mut db) = fixture(mode);
            let mut tx = db.begin_transaction().unwrap();
            let key = DescriptorTransaction::new(&db, &tx);
            let cache = SqlSessionCatalogCache::default();
            let shared_cache = cache.clone();
            let view = storage::capture_read_view(&tx, 42001, 1024 * 1024, &cancel).unwrap();
            let readfd = cache
                .pg_large_objects
                .open(key, Access::Read(view), 1024 * 1024)
                .unwrap();
            let writefd = shared_cache
                .pg_large_objects
                .open(key, Access::Write(42001), 1024 * 1024)
                .unwrap();
            assert_eq!((readfd, writefd), (0, 1));
            // The same registry survives the real per-statement cache transfer.
            let cache = SqlSession::new(&mut db)
                .with_catalog_cache(cache)
                .into_catalog_cache();
            let mark = cache.pg_large_objects.mark(key).unwrap();
            let tx_mark = tx.rollback_mark();
            let ticket = cache.pg_large_objects.begin_io(key, writefd, true).unwrap();
            storage::mutation(&db, &mut tx, 42001, None)
                .unwrap()
                .write(ticket.position, b"XY", &CancellationToken::uncancelable())
                .unwrap();
            cache.pg_large_objects.finish_io(ticket, 2).unwrap();
            assert_eq!(shared_cache.pg_large_objects.tell(key, writefd).unwrap(), 2);
            let childfd = cache
                .pg_large_objects
                .open(key, Access::Write(42001), 1024 * 1024)
                .unwrap();
            assert_eq!(childfd, 2);
            cache.pg_large_objects.close(key, readfd).unwrap();
            let reused = cache
                .pg_large_objects
                .open(key, Access::Write(42001), 1024 * 1024)
                .unwrap();
            assert_eq!(reused, 0);
            tx.rollback_to_mark(tx_mark).unwrap();
            shared_cache.pg_large_objects.rollback_to(mark).unwrap();
            assert_eq!(cache.pg_large_objects.tell(key, writefd).unwrap(), 2);
            assert!(is_state(cache.pg_large_objects.tell(key, childfd), "42704"));
            assert!(is_state(cache.pg_large_objects.tell(key, reused), "42704"));
            assert_eq!(
                storage::read(&tx, 42001, 0, 10, &CancellationToken::uncancelable()).unwrap(),
                b"abc"
            );
            tx.rollback().unwrap();
            cache.pg_large_objects.finish(key);
        }
    }

    #[test]
    fn reused_fd_numbers_and_intervening_seeks_cannot_retarget_an_io_ticket() {
        let (_root, db) = fixture(StorageMode::EmbeddedMemory);
        let tx = db.begin_transaction().unwrap();
        let key = DescriptorTransaction::new(&db, &tx);
        let state = DescriptorState::default();
        let fd = state.open(key, Access::Write(42001), 4096).unwrap();
        let old = state.begin_io(key, fd, true).unwrap();
        state.close(key, fd).unwrap();
        assert_eq!(state.open(key, Access::Write(42001), 4096).unwrap(), fd);
        assert!(is_state(state.finish_io(old, 1), "42704"));
        let mark = state.mark(key).unwrap();
        let child = state.open(key, Access::Write(42001), 4096).unwrap();
        state.rollback_to(mark).unwrap();
        assert_eq!(state.open(key, Access::Write(42001), 4096).unwrap(), child);
        state.rollback_to(mark).unwrap();
        assert!(is_state(state.tell(key, child), "42704"));
        let pending = state.begin_io(key, fd, false).unwrap();
        state.seek(key, fd, 0, 0, 3).unwrap();
        assert!(is_state(state.finish_io(pending, 1), "42704"));
        assert_eq!(state.tell(key, fd).unwrap(), 0);
        let current = state.begin_io(key, fd, false).unwrap();
        state.finish_io(current, 1).unwrap();
        assert_eq!(state.tell(key, fd).unwrap(), 1);
        tx.rollback().unwrap();
        state.finish(key);
    }

    #[test]
    fn read_snapshot_seek_and_write_permissions_follow_native_descriptor_modes() {
        let cancel = CancellationToken::uncancelable();
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let (_root, db) = fixture(mode);
            let mut reader = db.begin_transaction().unwrap();
            let key = DescriptorTransaction::new(&db, &reader);
            let state = DescriptorState::default();
            let view = storage::capture_read_view(&reader, 42001, 1024 * 1024, &cancel).unwrap();
            let readfd = state.open(key, Access::Read(view), 1024 * 1024).unwrap();
            let writefd = state.open(key, Access::Write(42001), 1024 * 1024).unwrap();
            assert!(is_state(state.begin_io(key, readfd, true), "55000"));
            assert!(state.begin_io(key, writefd, false).is_ok());
            let mut writer = db.begin_transaction().unwrap();
            storage::mutation(&db, &mut writer, 42001, None)
                .unwrap()
                .write(8, b"Z", &CancellationToken::uncancelable())
                .unwrap();
            writer.commit().unwrap();
            reader.refresh_read_committed_snapshot().unwrap();
            let length = storage::physical_length(&reader, 42001, &cancel).unwrap();
            assert_eq!(length, 9);
            assert_eq!(state.seek(key, readfd, 0, 2, length).unwrap(), 3);
            assert_eq!(state.seek(key, writefd, 0, 2, length).unwrap(), 9);
            state.seek(key, readfd, 0, 0, length).unwrap();
            let ticket = state.begin_io(key, readfd, false).unwrap();
            let Access::Read(view) = &ticket.access else {
                panic!("read view missing");
            };
            let bytes = view
                .read(&db, ticket.position, 20, &CancellationToken::uncancelable())
                .unwrap();
            assert_eq!(bytes, b"abc");
            state.finish_io(ticket, bytes.len()).unwrap();
            storage::mutation(&db, &mut reader, 42001, None)
                .unwrap()
                .truncate(1, &CancellationToken::uncancelable())
                .unwrap();
            assert_eq!(state.tell(key, writefd).unwrap(), 9);
            assert!(is_state(state.seek(key, readfd, -1, 0, 1), "22023"));
            assert!(is_state(
                state.seek(key, readfd, (storage::MAX_LENGTH + 1) as i64, 0, 1),
                "22023"
            ));
            assert_eq!(state.tell(key, readfd).unwrap(), 3);
            reader.rollback().unwrap();
            state.finish(key);
        }
    }

    #[test]
    fn unlink_closes_local_handles_and_rollback_does_not_resurrect_them() {
        let cancel = CancellationToken::uncancelable();
        let (_root, db) = fixture(StorageMode::EmbeddedMemory);
        let mut tx = db.begin_transaction().unwrap();
        let key = DescriptorTransaction::new(&db, &tx);
        let local = DescriptorState::default();
        let readfd = local
            .open(
                key,
                Access::Read(storage::capture_read_view(&tx, 42001, 1024 * 1024, &cancel).unwrap()),
                1024 * 1024,
            )
            .unwrap();
        let writefd = local.open(key, Access::Write(42001), 1024 * 1024).unwrap();
        let tx_mark = tx.rollback_mark();
        let mark = local.mark(key).unwrap();
        storage::mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .unlink(&CancellationToken::uncancelable())
            .unwrap();
        local.close_object(key, 42001).unwrap();
        tx.rollback_to_mark(tx_mark).unwrap();
        local.rollback_to(mark).unwrap();
        assert_eq!(
            storage::read(&tx, 42001, 0, 10, &CancellationToken::uncancelable()).unwrap(),
            b"abc"
        );
        assert!(is_state(local.tell(key, readfd), "42704"));
        assert!(is_state(local.tell(key, writefd), "42704"));
        tx.rollback().unwrap();
        local.finish(key);
    }

    #[test]
    fn descriptor_budgets_and_context_cleanup_release_snapshot_pins() {
        let cancel = CancellationToken::uncancelable();
        let (_root, db) = fixture(StorageMode::EmbeddedMemory);
        let tx = db.begin_transaction().unwrap();
        let key = DescriptorTransaction::new(&db, &tx);
        let state = DescriptorState::default();
        let old_mark = state.mark(key).unwrap();
        state
            .open(
                key,
                Access::Read(storage::capture_read_view(&tx, 42001, 1024 * 1024, &cancel).unwrap()),
                1024 * 1024,
            )
            .unwrap();
        let current_bytes = state.retained_bytes().unwrap();
        assert!(matches!(
            state.open(
                key,
                Access::Read(storage::capture_read_view(&tx, 42001, 1024 * 1024, &cancel).unwrap()),
                current_bytes
            ),
            Err(SqlError::BicDb(BicDbError::QueryMemoryLimit(_)))
        ));
        tx.rollback().unwrap();
        assert_eq!(db.mvcc_debug_stats().active_snapshots, 1);
        state.finish(key);
        assert_eq!(db.mvcc_debug_stats().active_snapshots, 0);
        let next_tx = db.begin_transaction().unwrap();
        let next_key = DescriptorTransaction::new(&db, &next_tx);
        let fd = state.open(next_key, Access::Write(42001), 4096).unwrap();
        state.finish(key);
        assert!(is_state(state.rollback_to(old_mark), "25000"));
        assert_eq!(state.tell(next_key, fd).unwrap(), 0);
        assert!(is_state(state.tell(key, fd), "42704"));
        next_tx.rollback().unwrap();
        drop(db);
        state.finish(next_key);
    }
}
