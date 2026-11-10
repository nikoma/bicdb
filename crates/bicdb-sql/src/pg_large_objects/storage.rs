//! Transactional sparse pages. Callers authorize each API operation; the two
//! physical collections are private implementation data, never SQL relations.
use super::*;
use bicdb_core::RecordReadSnapshot;

const PAGE_BYTES: u64 = 2048;
// PostgreSQL 16 caps seek/end at INT_MAX * LOBLKSIZE, not at
// (INT_MAX + 1) pages. Native sparse boundary controls cover this exact value.
const MAX_LENGTH: u64 = PAGE_BYTES * i32::MAX as u64;
const OBJECT_RESERVATION: &str = "postgres-large-object";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Metadata {
    format: u32,
    pub(super) owner: String,
    pub(super) length: u64,
    #[serde(default)]
    pub(super) acl: BTreeMap<String, u8>,
}

fn operation_budget() -> usize {
    let active = bicdb_core::query_budget::limit(usize::MAX);
    if active == usize::MAX {
        64 * 1024 * 1024
    } else {
        active
    }
}

fn corrupt(message: &str) -> SqlError {
    SqlError::data_exception(
        "XX001",
        format!("invalid large object storage: {message}"),
        None,
    )
}

fn invalid(message: &str) -> SqlError {
    SqlError::data_exception("22023", message, None)
}

fn missing(oid: u32) -> SqlError {
    SqlError::data_exception("42704", format!("large object {oid} does not exist"), None)
}

fn object_key(oid: u32) -> String {
    oid.to_string()
}

fn page_prefix(oid: u32) -> String {
    format!("{oid:010}:")
}

fn page_key(oid: u32, page: u64) -> String {
    format!("{oid:010}:{page:010}")
}

pub(super) fn ensure_storage(db: &mut BicDb) -> Result<()> {
    db.create_collection(METADATA)?;
    db.create_collection(PAGES)?;
    Ok(())
}

fn decode_metadata(record: &Record) -> Result<Metadata> {
    let metadata: Metadata = serde_json::from_value(record.metadata.clone())
        .map_err(|_| corrupt("metadata encoding"))?;
    if metadata.format != 1
        || metadata.owner.is_empty()
        || metadata.length > MAX_LENGTH
        || metadata.acl.values().any(|rights| rights & !3 != 0)
    {
        return Err(corrupt("metadata format, owner or length"));
    }
    Ok(metadata)
}

pub(super) fn metadata(tx: &Transaction, oid: u32) -> Result<Metadata> {
    let record = tx
        .get(METADATA, &object_key(oid))?
        .ok_or_else(|| missing(oid))?;
    decode_metadata(&record)
}

fn reserve_object(
    tx: &Transaction,
    oid: u32,
    cancellation: Option<&CancellationToken>,
) -> Result<()> {
    match cancellation {
        Some(cancellation) => tx.reserve_integrity_key_cancellable(
            OBJECT_RESERVATION,
            &object_key(oid),
            cancellation,
        )?,
        None => tx.reserve_integrity_key(OBJECT_RESERVATION, &object_key(oid))?,
    }
    Ok(())
}

// The native current-row recheck acquires the row lock before returning an
// observation. An explicit boundary is essential: an earlier descriptor open
// may have read an older version, which must not turn a later RC write into a
// stale-read conflict. Pending writes retain their read-your-writes overlay.
fn row_for_write(
    tx: &mut Transaction,
    collection: &str,
    id: &str,
) -> Result<(Option<Arc<Record>>, u64)> {
    if let Some((record, boundary)) = tx.read_committed_update_record(collection, id)? {
        return Ok((record.map(Arc::new), boundary));
    }
    Ok((tx.get(collection, id)?, tx.visibility_watermark()))
}

pub(super) fn create(
    db: &BicDb,
    tx: &mut Transaction,
    oid: u32,
    owner: &str,
    cancellation: Option<&CancellationToken>,
) -> Result<()> {
    db.check_recovery_state()?;
    if oid == 0 || owner.is_empty() {
        return Err(invalid(
            "large object OID and owner must be assigned before storage creation",
        ));
    }
    reserve_object(tx, oid, cancellation)?;
    if crate::records::current_integrity_record(db, Some(tx), METADATA, &object_key(oid))?.is_some()
    {
        return Err(SqlError::data_exception(
            "42710",
            format!("large object {oid} already exists"),
            None,
        ));
    }
    // Identity uniqueness uses current committed catalog state even if the
    // caller has a fixed user-data snapshot. The reservation also covers an
    // explicit unlink/recreate of an OID in this transaction.
    let value = Metadata {
        format: 1,
        owner: owner.into(),
        length: 0,
        acl: BTreeMap::new(),
    };
    let record = Record::new(object_key(oid)).with_metadata(serde_json::to_value(value)?);
    tx.write_upserts_with_statement_snapshots(METADATA, [(record, db.last_commit_seq())])?;
    db.check_recovery_state()?;
    Ok(())
}

pub(super) struct Mutation<'a> {
    db: &'a BicDb,
    tx: &'a mut Transaction,
    oid: u32,
    pub(super) metadata: Metadata,
    metadata_boundary: u64,
}

pub(super) fn mutation<'a>(
    db: &'a BicDb,
    tx: &'a mut Transaction,
    oid: u32,
    cancellation: Option<&CancellationToken>,
) -> Result<Mutation<'a>> {
    db.check_recovery_state()?;
    reserve_object(tx, oid, cancellation)?;
    tx.refresh_read_committed_snapshot()?;
    let (record, boundary) = row_for_write(tx, METADATA, &object_key(oid))?;
    let metadata = decode_metadata(record.as_deref().ok_or_else(|| missing(oid))?)?;
    db.check_recovery_state()?;
    Ok(Mutation {
        db,
        tx,
        oid,
        metadata,
        metadata_boundary: boundary,
    })
}

fn page_bytes(record: Option<&Record>) -> Result<&[u8]> {
    match record {
        Some(record) => {
            let payload = record
                .payload
                .as_deref()
                .ok_or_else(|| corrupt("page has no payload"))?;
            if payload.len() > PAGE_BYTES as usize {
                return Err(corrupt("oversized page"));
            }
            Ok(payload)
        }
        None => Ok(&[]),
    }
}

fn read_window(
    oid: u32,
    length: u64,
    offset: u64,
    requested: usize,
    cancellation: &CancellationToken,
    mut get: impl FnMut(&str) -> Result<Option<Arc<Record>>>,
) -> Result<Vec<u8>> {
    let count = length.saturating_sub(offset).min(requested as u64) as usize;
    // Reserve a bounded bytea result; sparse holes never allocate their offset.
    let budget = operation_budget();
    let retained = count.saturating_mul(4).saturating_add(PAGE_BYTES as usize);
    if retained > budget {
        return Err(SqlError::BicDb(BicDbError::QueryMemoryLimit(format!(
            "large object read needs {retained} bytes; limit is {budget}"
        ))));
    }
    bicdb_core::query_budget::observe(retained);
    bicdb_core::query_budget::check()?;
    let mut bytes = vec![0; count];
    let mut copied = 0;
    while copied < count {
        cancellation.check()?;
        let position = offset + copied as u64;
        let page = position / PAGE_BYTES;
        let within = (position % PAGE_BYTES) as usize;
        let chunk = (PAGE_BYTES as usize - within).min(count - copied);
        let record = get(&page_key(oid, page))?;
        let data = page_bytes(record.as_deref())?;
        let available = data.len().saturating_sub(within).min(chunk);
        if available > 0 {
            bytes[copied..copied + available].copy_from_slice(&data[within..within + available]);
        }
        copied += chunk;
    }
    Ok(bytes)
}

pub(super) fn read(
    tx: &Transaction,
    oid: u32,
    offset: u64,
    requested: usize,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>> {
    let metadata = metadata(tx, oid)?;
    read_window(
        oid,
        metadata.length,
        offset,
        requested,
        cancellation,
        |key| Ok(tx.get(PAGES, key)?),
    )
}

/// READ descriptors retain their opening command's rows and pending overlay.
/// Metadata/privileges are checked by the host at open, not on each later read:
/// PostgreSQL permits an existing descriptor after REVOKE or another unlink.
#[derive(Clone, Debug)]
pub(super) struct ReadView {
    oid: u32,
    pub(super) length: u64,
    pages: RecordReadSnapshot,
}

pub(super) fn capture_read_view(
    tx: &Transaction,
    oid: u32,
    max_retained_bytes: usize,
) -> Result<ReadView> {
    let metadata = metadata(tx, oid)?;
    let pages = tx.capture_record_prefix_snapshot_unchecked(
        PAGES,
        &page_prefix(oid),
        max_retained_bytes.saturating_sub(std::mem::size_of::<ReadView>()),
    )?;
    Ok(ReadView {
        oid,
        length: metadata.length,
        pages,
    })
}

impl ReadView {
    pub(super) fn retained_bytes(&self) -> usize {
        self.pages
            .retained_bytes()
            .saturating_add(std::mem::size_of::<Self>())
    }

    pub(super) fn read(
        &self,
        db: &BicDb,
        offset: u64,
        requested: usize,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>> {
        db.check_recovery_state()?;
        let bytes = read_window(
            self.oid,
            self.length,
            offset,
            requested,
            cancellation,
            |key| Ok(self.pages.get_visible_record_unchecked(db, key)?),
        );
        db.check_recovery_state()?;
        bytes
    }
}

impl Mutation<'_> {
    fn persist_metadata(&mut self) -> Result<()> {
        self.db.check_recovery_state()?;
        let record =
            Record::new(object_key(self.oid)).with_metadata(serde_json::to_value(&self.metadata)?);
        self.tx
            .write_upserts_with_statement_snapshots(METADATA, [(record, self.metadata_boundary)])?;
        self.db.check_recovery_state()?;
        Ok(())
    }

    pub(super) fn write(
        &mut self,
        offset: u64,
        bytes: &[u8],
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.db.check_recovery_state()?;
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("invalid large object write offset"))?;
        if end > MAX_LENGTH {
            return Err(invalid("large object exceeds the maximum length"));
        }
        if bytes.is_empty() {
            return Ok(());
        }
        let mut copied = 0;
        while copied < bytes.len() {
            cancellation.check()?;
            let position = offset + copied as u64;
            let page = position / PAGE_BYTES;
            let within = (position % PAGE_BYTES) as usize;
            let chunk = (PAGE_BYTES as usize - within).min(bytes.len() - copied);
            let key = page_key(self.oid, page);
            let (record, boundary) = row_for_write(self.tx, PAGES, &key)?;
            let mut data = page_bytes(record.as_deref())?.to_vec();
            data.resize(data.len().max(within + chunk), 0);
            data[within..within + chunk].copy_from_slice(&bytes[copied..copied + chunk]);
            self.tx.write_upserts_with_statement_snapshots(
                PAGES,
                [(Record::new(key).with_payload(data), boundary)],
            )?;
            copied += chunk;
        }
        self.metadata.length = self.metadata.length.max(end);
        self.persist_metadata()
    }

    fn delete_pages_from(&mut self, first: u64, cancellation: &CancellationToken) -> Result<()> {
        // Visit key ranges without materializing page payloads or sparse
        // holes. Candidate keys are a superset, so recheck each visible row.
        let mut start = page_key(self.oid, first);
        let upper = format!("{}~", page_prefix(self.oid));
        let mut inclusive = true;
        let mut journal_bytes = 0usize;
        let budget = operation_budget();
        loop {
            self.db.check_recovery_state()?;
            cancellation.check()?;
            let lower = if inclusive {
                std::ops::Bound::Included(start.as_str())
            } else {
                std::ops::Bound::Excluded(start.as_str())
            };
            let (ids, complete) = self
                .tx
                .scan_collection_record_key_candidates_in_range_unchecked(
                    PAGES,
                    lower,
                    std::ops::Bound::Excluded(upper.as_str()),
                    false,
                    4096,
                )?;
            if ids.is_empty() {
                return Ok(());
            }
            let candidate_bytes = ids
                .iter()
                .fold(0usize, |n, id| n.saturating_add(id.len() + 64));
            if journal_bytes.saturating_add(candidate_bytes) > budget {
                return Err(SqlError::BicDb(BicDbError::QueryMemoryLimit(
                    "large object deletion exceeds query memory".into(),
                )));
            }
            for id in &ids {
                cancellation.check()?;
                let (record, boundary) = row_for_write(self.tx, PAGES, id)?;
                if record.is_some() {
                    // Charge the pending write/lock journal as well as the
                    // candidate keys; it remains live until top-level commit.
                    journal_bytes = journal_bytes.saturating_add(id.len() + 1024);
                    if journal_bytes.saturating_add(candidate_bytes) > budget {
                        return Err(SqlError::BicDb(BicDbError::QueryMemoryLimit(
                            "large object deletion journal exceeds query memory".into(),
                        )));
                    }
                    bicdb_core::query_budget::observe(
                        journal_bytes.saturating_add(candidate_bytes),
                    );
                    bicdb_core::query_budget::check()?;
                    self.tx
                        .delete_many_with_statement_snapshots(PAGES, [(id, boundary)])?;
                }
            }
            start = ids.last().expect("nonempty batch").clone();
            inclusive = false;
            if complete {
                return Ok(());
            }
        }
    }

    pub(super) fn truncate(&mut self, length: u64, cancellation: &CancellationToken) -> Result<()> {
        self.db.check_recovery_state()?;
        if length > MAX_LENGTH {
            return Err(invalid("large object exceeds the maximum length"));
        }
        let first_removed = if length == 0 {
            1
        } else {
            (length - 1) / PAGE_BYTES + 1
        };
        self.delete_pages_from(first_removed, cancellation)?;
        // Native lo_truncate(0) keeps/creates page zero with empty data.
        // Extensions likewise materialize only their final sparse page.
        let page = if length == 0 {
            0
        } else {
            (length - 1) / PAGE_BYTES
        };
        let within = if length == 0 {
            0
        } else {
            ((length - 1) % PAGE_BYTES + 1) as usize
        };
        let key = page_key(self.oid, page);
        let (record, boundary) = row_for_write(self.tx, PAGES, &key)?;
        let mut data = page_bytes(record.as_deref())?.to_vec();
        data.resize(within, 0);
        self.tx.write_upserts_with_statement_snapshots(
            PAGES,
            [(Record::new(key).with_payload(data), boundary)],
        )?;
        self.metadata.length = length;
        self.persist_metadata()
    }

    pub(super) fn unlink(mut self, cancellation: &CancellationToken) -> Result<()> {
        self.db.check_recovery_state()?;
        self.delete_pages_from(0, cancellation)?;
        self.tx.delete_many_with_statement_snapshots(
            METADATA,
            [(object_key(self.oid), self.metadata_boundary)],
        )?;
        self.db.check_recovery_state()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_core::{DbConfig, StorageMode};

    fn open(path: &std::path::Path, mode: StorageMode) -> BicDb {
        BicDb::open_with_config(
            path,
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap()
    }

    #[test]
    fn sparse_pages_and_open_views_match_native_transactional_bytes() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let mut db = open(root.path(), mode);
            ensure_storage(&mut db).unwrap();
            let mut tx = db.begin_transaction().unwrap();
            let cancel = CancellationToken::uncancelable();
            create(&db, &mut tx, 42001, "bicdb", None).unwrap();
            {
                let mut object = mutation(&db, &mut tx, 42001, None).unwrap();
                object.write(0, &[0, 1, 127, 128, 255], &cancel).unwrap();
                object.write(8, &[171], &cancel).unwrap();
            }
            let bytes = vec![0, 1, 127, 128, 255, 0, 0, 0, 171];
            assert_eq!(read(&tx, 42001, 0, 100, &cancel).unwrap(), bytes);
            let view = capture_read_view(&tx, 42001, 1024 * 1024).unwrap();
            assert!(view.retained_bytes() <= 1024 * 1024);
            mutation(&db, &mut tx, 42001, None)
                .unwrap()
                .truncate(3, &cancel)
                .unwrap();
            assert_eq!(read(&tx, 42001, 0, 100, &cancel).unwrap(), vec![0, 1, 127]);
            assert_eq!(view.read(&db, 0, 100, &cancel).unwrap(), bytes);
            let mark = tx.rollback_mark();
            mutation(&db, &mut tx, 42001, None)
                .unwrap()
                .write(4099, &[254], &cancel)
                .unwrap();
            assert_eq!(
                read(&tx, 42001, 4094, 10, &cancel).unwrap(),
                vec![0, 0, 0, 0, 0, 254]
            );
            tx.rollback_to_mark(mark).unwrap();
            assert_eq!(metadata(&tx, 42001).unwrap().length, 3);
            tx.commit().unwrap();
            // Host descriptors close at transaction end; this low-level owned
            // view also proves its retained own overlay cannot be mutated.
            assert_eq!(view.read(&db, 0, 100, &cancel).unwrap(), bytes);
        }
    }

    #[test]
    fn committed_pages_reopen_and_read_view_survives_unlink() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let mut db = open(root.path(), mode.clone());
            ensure_storage(&mut db).unwrap();
            let cancel = CancellationToken::uncancelable();
            let mut tx = db.begin_transaction().unwrap();
            for oid in [42001, 420010] {
                create(&db, &mut tx, oid, "bicdb", None).unwrap();
                mutation(&db, &mut tx, oid, None)
                    .unwrap()
                    .write(2047, &[17, 34, 51], &cancel)
                    .unwrap();
            }
            tx.commit().unwrap();
            db.checkpoint_for_resume().unwrap();
            drop(db);
            let db = open(root.path(), mode);
            let reader = db.begin_transaction().unwrap();
            let view = capture_read_view(&reader, 42001, 1024 * 1024).unwrap();
            assert_eq!(
                view.read(&db, 2046, 8, &cancel).unwrap(),
                vec![0, 17, 34, 51]
            );
            let mut writer = db.begin_transaction().unwrap();
            mutation(&db, &mut writer, 42001, None)
                .unwrap()
                .unlink(&cancel)
                .unwrap();
            writer.commit().unwrap();
            assert_eq!(
                view.read(&db, 2046, 8, &cancel).unwrap(),
                vec![0, 17, 34, 51]
            );
            let check = db.begin_transaction().unwrap();
            assert!(metadata(&check, 42001).is_err());
            assert!(check
                .scan_collection_record_ids_with_prefix(PAGES, &page_prefix(42001))
                .unwrap()
                .is_empty());
            assert_eq!(
                read(&check, 420010, 2046, 8, &cancel).unwrap(),
                vec![0, 17, 34, 51]
            );
            check.rollback().unwrap();
            reader.rollback().unwrap();
            drop(view);
        }
    }

    #[test]
    fn truncate_extends_sparse_last_page_and_zero_keeps_empty_page() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), StorageMode::EmbeddedMemory);
        ensure_storage(&mut db).unwrap();
        let mut tx = db.begin_transaction().unwrap();
        let cancel = CancellationToken::uncancelable();
        create(&db, &mut tx, 42001, "bicdb", None).unwrap();
        mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .write(0, b"abcde", &cancel)
            .unwrap();
        mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .truncate(4099, &cancel)
            .unwrap();
        assert_eq!(
            tx.get(PAGES, &page_key(42001, 0))
                .unwrap()
                .unwrap()
                .payload
                .as_deref(),
            Some(b"abcde".as_slice())
        );
        assert!(tx.get(PAGES, &page_key(42001, 1)).unwrap().is_none());
        assert_eq!(
            tx.get(PAGES, &page_key(42001, 2))
                .unwrap()
                .unwrap()
                .payload
                .as_deref(),
            Some([0, 0, 0].as_slice())
        );
        assert_eq!(read(&tx, 42001, 4094, 20, &cancel).unwrap(), vec![0; 5]);
        mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .truncate(0, &cancel)
            .unwrap();
        assert_eq!(metadata(&tx, 42001).unwrap().length, 0);
        assert_eq!(
            tx.scan_collection_record_ids_with_prefix(PAGES, &page_prefix(42001))
                .unwrap(),
            vec![page_key(42001, 0)]
        );
        assert_eq!(
            tx.get(PAGES, &page_key(42001, 0))
                .unwrap()
                .unwrap()
                .payload
                .as_deref(),
            Some([].as_slice())
        );
        tx.rollback().unwrap();
    }

    #[test]
    fn read_committed_write_rechecks_after_an_older_descriptor_open() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let mut db = open(root.path(), mode);
            ensure_storage(&mut db).unwrap();
            let cancel = CancellationToken::uncancelable();
            let mut seed = db.begin_transaction().unwrap();
            create(&db, &mut seed, 42001, "bicdb", None).unwrap();
            mutation(&db, &mut seed, 42001, None)
                .unwrap()
                .write(0, b"abc", &cancel)
                .unwrap();
            seed.commit().unwrap();
            let mut a = db.begin_transaction().unwrap();
            let view = capture_read_view(&a, 42001, 1024 * 1024).unwrap();
            let mut b = db.begin_transaction().unwrap();
            mutation(&db, &mut b, 42001, None)
                .unwrap()
                .write(0, b"XYZ", &cancel)
                .unwrap();
            b.commit().unwrap();
            mutation(&db, &mut a, 42001, None)
                .unwrap()
                .write(1, b"x", &cancel)
                .unwrap();
            assert_eq!(read(&a, 42001, 0, 10, &cancel).unwrap(), b"XxZ");
            a.commit().unwrap();
            assert_eq!(view.read(&db, 0, 10, &cancel).unwrap(), b"abc");
            let check = db.begin_transaction().unwrap();
            assert_eq!(read(&check, 42001, 0, 10, &cancel).unwrap(), b"XxZ");
            check.rollback().unwrap();
        }
    }

    #[test]
    fn storage_identity_and_sparse_read_budgets_are_checked() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), StorageMode::EmbeddedMemory);
        ensure_storage(&mut db).unwrap();
        let mut tx = db.begin_transaction().unwrap();
        let cancel = CancellationToken::uncancelable();
        create(&db, &mut tx, 42001, "bicdb", None).unwrap();
        assert!(create(&db, &mut tx, 42001, "other", None).is_err());
        let offset = 8 * 1024 * 1024 * 1024;
        mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .write(offset, &[171], &cancel)
            .unwrap();
        assert_eq!(
            read(&tx, 42001, offset - 2, 100, &cancel).unwrap(),
            vec![0, 0, 171]
        );
        assert!(matches!(
            read(&tx, 42001, 0, usize::MAX, &cancel),
            Err(SqlError::BicDb(BicDbError::QueryMemoryLimit(_)))
        ));
        assert!(matches!(
            capture_read_view(&tx, 42001, 0),
            Err(SqlError::BicDb(BicDbError::QueryMemoryLimit(_)))
        ));
        mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .unlink(&cancel)
            .unwrap();
        create(&db, &mut tx, 42001, "newowner", None).unwrap();
        assert_eq!(metadata(&tx, 42001).unwrap().owner, "newowner");
        tx.rollback().unwrap();
        let mut session = SqlSession::new(&mut db);
        for name in [METADATA, PAGES] {
            assert!(session.execute(&format!("SELECT * FROM {name}")).is_err());
            assert!(session
                .execute(&format!("CREATE TABLE {name}(id integer)"))
                .is_err());
        }
    }

    #[test]
    fn native_sparse_maximum_accepts_empty_write_and_rejects_one_more_byte() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), StorageMode::EmbeddedMemory);
        ensure_storage(&mut db).unwrap();
        let cancel = CancellationToken::uncancelable();
        let mut tx = db.begin_transaction().unwrap();
        create(&db, &mut tx, 42001, "bicdb", None).unwrap();
        mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .write(MAX_LENGTH, &[], &cancel)
            .unwrap();
        assert_eq!(metadata(&tx, 42001).unwrap().length, 0);
        mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .write(MAX_LENGTH - 1, b"Z", &cancel)
            .unwrap();
        assert_eq!(metadata(&tx, 42001).unwrap().length, 4398046509056);
        assert_eq!(
            read(&tx, 42001, MAX_LENGTH - 1, 100, &cancel).unwrap(),
            b"Z"
        );
        assert!(mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .write(MAX_LENGTH, b"x", &cancel)
            .is_err());
        assert!(mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .truncate(MAX_LENGTH + 1, &cancel)
            .is_err());
        tx.rollback().unwrap();
    }

    #[test]
    fn unlink_walks_multiple_lazy_key_batches() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), StorageMode::ServerPaged);
        ensure_storage(&mut db).unwrap();
        let cancel = CancellationToken::uncancelable();
        let mut tx = db.begin_transaction().unwrap();
        create(&db, &mut tx, 42001, "bicdb", None).unwrap();
        let pages = 4100;
        mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .write(0, &vec![17; pages * PAGE_BYTES as usize], &cancel)
            .unwrap();
        tx.commit().unwrap();
        db.checkpoint_for_resume().unwrap();
        drop(db);
        let db = open(root.path(), StorageMode::ServerPaged);
        let mut tx = db.begin_transaction().unwrap();
        mutation(&db, &mut tx, 42001, None)
            .unwrap()
            .unlink(&cancel)
            .unwrap();
        tx.commit().unwrap();
        let tx = db.begin_transaction().unwrap();
        assert!(tx
            .scan_collection_record_ids_with_prefix(PAGES, &page_prefix(42001))
            .unwrap()
            .is_empty());
        tx.rollback().unwrap();
    }
}
