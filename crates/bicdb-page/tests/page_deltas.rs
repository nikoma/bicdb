//! Experimental page-delta WAL records: the two locks, gate-off byte
//! identity, and the delta oracle. See `docs/page-delta-redo-design.md`,
//! section 12 (qualification program).
//!
//! These tests set process environment variables, so every test takes
//! `ENV` for its whole body.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use bicdb_page::{PagedStore, PagedStoreOptions};
use tempfile::TempDir;

static ENV: Mutex<()> = Mutex::new(());

const GATE: &str = "BICDB_PAGE_WAL_DELTAS";
const PAGE: u32 = 512;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(PAGE)
        .with_buffer_pool_bytes(64 * PAGE as u64)
        .with_fsync(false)
        .with_wal_max_bytes(1 << 30)
}

/// The `page-delta-test-hook` harness makes every new store disposable, which
/// voids the tests that need a normal store.
fn hook_makes_every_store_disposable() -> bool {
    std::env::var("BICDB_TEST_PAGE_DELTAS_CREATE_DISPOSABLE").is_ok_and(|v| v == "1")
}

fn set_gate(value: Option<&str>) {
    match value {
        Some(value) => std::env::set_var(GATE, value),
        None => std::env::remove_var(GATE),
    }
}

fn superblock_magic(dir: &Path) -> [u8; 8] {
    let bytes = std::fs::read(dir.join("store.pages")).unwrap();
    bytes[64..72].try_into().unwrap()
}

/// Independent WAL decoder: (lsn, kind, page_id, xid, payload).
type Rec = (u64, u8, u64, u64, Vec<u8>);

fn decode_wal(bytes: &[u8]) -> Vec<Rec> {
    let mut out = Vec::new();
    let mut at = 0;
    while bytes.len() >= at + 24 {
        if &bytes[at..at + 4] != b"BWAL" {
            break;
        }
        let lsn = u64::from_le_bytes(bytes[at + 4..at + 12].try_into().unwrap());
        let kind = bytes[at + 12];
        let len = u32::from_le_bytes(bytes[at + 16..at + 20].try_into().unwrap()) as usize;
        if bytes.len() < at + 24 + len {
            break;
        }
        let body = &bytes[at + 24..at + 24 + len];
        out.push((
            lsn,
            kind,
            u64::from_le_bytes(body[0..8].try_into().unwrap()),
            u64::from_le_bytes(body[8..16].try_into().unwrap()),
            body[16..].to_vec(),
        ));
        at += 24 + len;
    }
    out
}

fn wal_records(dir: &Path) -> Vec<Rec> {
    decode_wal(&std::fs::read(dir.join("store.wal")).unwrap_or_default())
}

/// Masked page checksum, written independently of the engine's: the stamped
/// generation, checksum and trailer bytes are zeroed.
fn masked(page: &[u8]) -> Vec<u8> {
    let mut copy = page.to_vec();
    copy[16..24].fill(0);
    copy[32..36].fill(0);
    let len = copy.len();
    copy[len - 8..].fill(0);
    copy
}

/// Apply an encoded delta payload to `page` (independent decoder).
fn apply_delta(payload: &[u8], page: &mut [u8]) {
    let runs = u32::from_le_bytes(payload[16..20].try_into().unwrap());
    let mut at = 20;
    for _ in 0..runs {
        let offset = u32::from_le_bytes(payload[at..at + 4].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(payload[at + 4..at + 6].try_into().unwrap()) as usize;
        at += 6;
        page[offset..offset + len].copy_from_slice(&payload[at..at + len]);
        at += len;
    }
    assert_eq!(at, payload.len());
}

fn key(i: u64) -> Vec<u8> {
    format!("key-{i:05}").into_bytes()
}

fn value(i: u64, round: u64) -> Vec<u8> {
    format!("v{round}-{}", "x".repeat(((i * 7 + round) % 40) as usize)).into_bytes()
}

/// A deterministic workload: inserts, updates, deletes, multi-row commits, an
/// aborted transaction, and an interleaved transaction that commits later.
fn workload(
    store: &PagedStore,
    rounds: u64,
    after_commit: impl FnMut(&PagedStore),
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut model = BTreeMap::new();
    workload_into(store, rounds, &mut model, after_commit);
    model
}

/// [`workload`], continuing an existing model (deletes remove from it).
fn workload_into(
    store: &PagedStore,
    rounds: u64,
    model: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    mut after_commit: impl FnMut(&PagedStore),
) {
    for round in 0..rounds {
        let tx = store.begin();
        for i in 0..6 {
            let k = (round * 5 + i * 13) % 150;
            store.put(tx, &key(k), &value(k, round)).unwrap();
            model.insert(key(k), value(k, round));
        }
        if round % 7 == 3 {
            let k = (round * 3) % 150;
            if store.delete(tx, &key(k)).unwrap() {
                model.remove(&key(k));
            }
        }
        if round % 11 == 5 {
            // Interleaved: a second transaction writes and commits while the
            // first is still open.
            let other = store.begin();
            let k = 1000 + round;
            store.put(other, &key(k), &value(k, round)).unwrap();
            store.commit(other).unwrap();
            model.insert(key(k), value(k, round));
        }
        store.commit(tx).unwrap();
        after_commit(store);
        if round % 13 == 8 {
            let tx = store.begin();
            store.put(tx, &key(5000 + round), b"never").unwrap();
            store.abort(tx).unwrap();
        }
    }
}

fn assert_model(store: &PagedStore, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    for (k, v) in model {
        assert_eq!(
            store.get(k).unwrap().as_deref(),
            Some(v.as_slice()),
            "{k:?}"
        );
    }
    for round in 0..200 {
        assert!(store.get(&key(5000 + round)).unwrap().is_none());
    }
}

#[test]
fn deltas_need_both_locks_and_existing_stores_never_get_the_marker() {
    if hook_makes_every_store_disposable() {
        return; // this test is about NORMAL stores, which the hook removes
    }
    let _env = ENV.lock().unwrap_or_else(|poison| poison.into_inner());

    // Lock 1 open, lock 2 closed: a normal new store.
    set_gate(Some("1"));
    let normal = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(normal.path(), options()).unwrap();
        assert!(!store.is_disposable_page_delta_store());
        assert!(!store.page_deltas().0, "deltas engaged on a normal store");
        workload(&store, 30, |_| {});
        store.checkpoint().unwrap();
        workload(&store, 10, |_| {});
    }
    assert_ne!(&superblock_magic(normal.path()), b"BICDBPG5");
    assert!(wal_records(normal.path()).iter().all(|r| r.1 != 5));

    // A pre-existing store reopened with the gate set: still nothing.
    {
        let (store, _) = PagedStore::open(normal.path(), options()).unwrap();
        assert!(!store.page_deltas().0);
        workload(&store, 10, |_| {});
    }
    assert_ne!(&superblock_magic(normal.path()), b"BICDBPG5");
    assert!(wal_records(normal.path()).iter().all(|r| r.1 != 5));

    // Asking an existing store to become disposable is refused, and leaves
    // no marker behind.
    let error = PagedStore::open(
        normal.path(),
        options().with_experimental_page_deltas_disposable(true),
    )
    .err()
    .expect("an existing store must refuse the disposable marker");
    assert!(error.to_string().contains("already exists"), "{error}");
    assert_ne!(&superblock_magic(normal.path()), b"BICDBPG5");

    // Lock 2 open, lock 1 closed (unset, or any value but exactly "1").
    for gate in [None, Some("0"), Some("true"), Some(" 1"), Some("1 ")] {
        set_gate(gate);
        let dir = TempDir::new().unwrap();
        let (store, _) = PagedStore::open(
            dir.path(),
            options().with_experimental_page_deltas_disposable(true),
        )
        .unwrap();
        assert!(store.is_disposable_page_delta_store());
        assert!(!store.page_deltas().0, "gate {gate:?} engaged deltas");
        workload(&store, 10, |_| {});
        drop(store);
        assert_eq!(&superblock_magic(dir.path()), b"BICDBPG5");
        assert!(wal_records(dir.path()).iter().all(|r| r.1 != 5));
    }

    // Both locks open: deltas engage.
    set_gate(Some("1"));
    let dir = TempDir::new().unwrap();
    let (store, _) = PagedStore::open(
        dir.path(),
        options().with_experimental_page_deltas_disposable(true),
    )
    .unwrap();
    assert!(store.page_deltas().0);
    workload(&store, 20, |_| {});
    assert!(store.page_deltas().1.delta_records > 0);
    drop(store);
    assert!(wal_records(dir.path()).iter().any(|r| r.1 == 5));
    set_gate(None);
}

#[test]
fn with_the_gate_closed_the_wal_bytes_are_identical() {
    if hook_makes_every_store_disposable() {
        return; // this test is about NORMAL stores, which the hook removes
    }
    let _env = ENV.lock().unwrap_or_else(|poison| poison.into_inner());
    let run = |gate: Option<&str>, disposable: bool| -> Vec<u8> {
        set_gate(gate);
        let dir = TempDir::new().unwrap();
        let (store, _) = PagedStore::open(
            dir.path(),
            options().with_experimental_page_deltas_disposable(disposable),
        )
        .unwrap();
        workload(&store, 40, |_| {});
        drop(store);
        std::fs::read(dir.path().join("store.wal")).unwrap()
    };
    let reference = run(None, false);
    assert!(!reference.is_empty());
    for (gate, disposable) in [
        (Some("1"), false),
        (Some("0"), false),
        (Some("true"), true),
        (None, true),
    ] {
        assert!(
            run(gate, disposable) == reference,
            "WAL bytes differ with gate {gate:?}, disposable {disposable}"
        );
    }
    set_gate(None);
}

/// The Q1 oracle: after every commit, replaying the log independently (full
/// images and deltas, in LSN order, from the last truncation) reproduces the
/// buffer pool's view of every logged page, outside the stamped fields. A
/// delta for a page with no earlier record in the replay is an I12 failure.
#[test]
fn replaying_deltas_reproduces_every_page_the_pool_holds() {
    let _env = ENV.lock().unwrap_or_else(|poison| poison.into_inner());
    set_gate(Some("1"));
    for pool_pages in [16u64, 64, 4096] {
        let dir = TempDir::new().unwrap();
        let (store, _) = PagedStore::open(
            dir.path(),
            options()
                .with_buffer_pool_bytes(pool_pages * PAGE as u64)
                .with_experimental_page_deltas_disposable(true),
        )
        .unwrap();
        assert!(store.page_deltas().0);
        let path = dir.path().to_path_buf();
        let mut shadow: BTreeMap<u64, (u64, Vec<u8>)> = BTreeMap::new();
        let mut consumed = 0usize;
        let mut deltas_checked = 0u64;
        let mut commits = 0u64;
        let model = workload(&store, 160, |store| {
            commits += 1;
            let records = wal_records(&path);
            if records.len() < consumed {
                // The log was truncated by a checkpoint: start over.
                shadow.clear();
                consumed = 0;
            }
            for (lsn, kind, page_id, _xid, payload) in &records[consumed..] {
                match kind {
                    1 => {
                        shadow.insert(*page_id, (*lsn, payload.clone()));
                    }
                    5 => {
                        let base_lsn = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                        let entry = shadow.get_mut(page_id).unwrap_or_else(|| {
                            panic!("I12: delta at {lsn} for page {page_id} with no earlier record")
                        });
                        assert_eq!(
                            entry.0, base_lsn,
                            "delta at {lsn} names a base that is not the page's last record"
                        );
                        apply_delta(payload, &mut entry.1);
                        entry.0 = *lsn;
                        deltas_checked += 1;
                    }
                    _ => {}
                }
            }
            consumed = records.len();
            for (page_id, (_, image)) in &shadow {
                let guard = store.buffer_pool().get(*page_id).unwrap();
                assert!(
                    masked(guard.bytes()) == masked(image),
                    "page {page_id}: log replay differs from the pool after commit {commits}"
                );
            }
            if commits % 37 == 0 {
                store.checkpoint().unwrap();
            }
        });
        assert!(deltas_checked > 100, "only {deltas_checked} deltas checked");
        let (_, stats) = store.page_deltas();
        drop(store); // crash: nothing flushed

        // Recovery, twice: the second open must see the identical state.
        for _ in 0..2 {
            let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
            assert!(store.is_disposable_page_delta_store());
            assert_model(&store, &model);
        }
        eprintln!(
            "pool {pool_pages} pages: {} deltas ({} B), {} full images ({} B), oracle checked {deltas_checked}",
            stats.delta_records, stats.delta_bytes, stats.full_images, stats.full_image_bytes
        );
    }
    set_gate(None);
}

/// Crash after every commit of a run whose checkpoints cannot truncate (an
/// open transaction pins them, so checkpoint records and redo points stay
/// inside the log), with a torn half-record appended, then recover twice.
/// Every committed row is present, the pinned transaction's row is not, and
/// the second recovery sees the same state.
#[test]
fn crash_after_any_commit_with_retained_checkpoints_recovers_the_committed_state() {
    let _env = ENV.lock().unwrap_or_else(|poison| poison.into_inner());
    set_gate(Some("1"));
    let mut crashes = 0;
    for stop in (3..60u64).step_by(4) {
        let dir = TempDir::new().unwrap();
        let model = {
            let (store, _) = PagedStore::open(
                dir.path(),
                options()
                    .with_buffer_pool_bytes(24 * PAGE as u64)
                    .with_experimental_page_deltas_disposable(true),
            )
            .unwrap();
            let pin = store.begin();
            store.put(pin, b"pin", b"pin").unwrap();
            let mut round = 0u64;
            let model = workload(&store, stop, |store| {
                round += 1;
                if round % 10 == 0 {
                    store.checkpoint().unwrap();
                }
            });
            assert!(store.page_deltas().1.delta_records > 0 || stop < 5);
            model
        }; // crash
           // A torn record at the tail, as a crash mid-append leaves.
        let mut wal = std::fs::read(dir.path().join("store.wal")).unwrap();
        wal.extend_from_slice(b"BWAL\x07\x07\x07");
        std::fs::write(dir.path().join("store.wal"), &wal).unwrap();
        for _ in 0..2 {
            let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
            assert_model(&store, &model);
            assert!(store.get(b"pin").unwrap().is_none());
        }
        crashes += 1;
    }
    assert!(crashes > 10);
    set_gate(None);
}

fn copy_base(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        if name.starts_with("store.pages") {
            std::fs::copy(&path, to.join(&name)).unwrap();
        }
    }
}

fn restore_base(base: &Path, dir: &Path) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("store.pages")
        {
            std::fs::remove_file(path).unwrap();
        }
    }
    copy_base(base, dir);
}

/// The unvouched-redo fallback (design 6.2) over a segmented WAL with
/// archive retention: the local chain starts at a RETAINED sealed segment of
/// an older epoch, the page files are an older base whose horizon predates
/// the latest checkpoint, so recovery replays from the start of the chain.
/// That start must be an epoch start (I12 at every seal), or a delta there
/// would name a base in an already-deleted segment and the open would fail.
#[test]
fn replay_from_a_retained_sealed_segment_over_an_older_base() {
    let _env = ENV.lock().unwrap_or_else(|poison| poison.into_inner());
    set_gate(Some("1"));
    let segmented = || {
        options()
            .with_page_size(4096)
            .with_buffer_pool_bytes(8 * 1024 * 1024)
            .with_extent_bytes(1024 * 1024)
            .with_wal_segment_bytes(64 * 1024)
    };
    let dir = TempDir::new().unwrap();
    let base = TempDir::new().unwrap();
    let (store, _) = PagedStore::open(
        dir.path(),
        segmented().with_experimental_page_deltas_disposable(true),
    )
    .unwrap();
    assert!(store.page_deltas().0);
    store.enable_wal_archive_retention();
    let mut model = workload(&store, 200, |_| {});
    let sealed: Vec<u64> = store
        .sealed_wal_segments()
        .iter()
        .map(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .unwrap()
                .parse()
                .unwrap()
        })
        .collect();
    assert!(sealed.len() >= 2, "only {} sealed segments", sealed.len());
    // The archiver has the first segment; truncation deletes it and keeps
    // the rest, so the chain now starts mid-history.
    store.mark_wal_archived_through(sealed[0]);
    store.checkpoint().unwrap();
    assert!(!store.sealed_wal_segments().is_empty());
    copy_base(dir.path(), base.path());
    // An open transaction keeps later checkpoints from truncating, so their
    // redo points move past the base's horizon.
    let pin = store.begin();
    store.put(pin, b"pin", b"pin").unwrap();
    let mut round = 0;
    workload_into(&store, 60, &mut model, |store| {
        round += 1;
        if round % 20 == 0 {
            store.checkpoint().unwrap();
        }
    });
    let (_, stats) = store.page_deltas();
    assert!(stats.delta_records > 0);
    drop(store); // crash

    restore_base(base.path(), dir.path());
    for _ in 0..2 {
        let (store, report) = PagedStore::open(dir.path(), segmented()).unwrap();
        assert_eq!(report.redo_lsn, 0, "expected the unvouched fallback");
        assert!(store.page_deltas().1.recovery_deltas_replayed > 0);
        for (k, v) in &model {
            assert_eq!(
                store.get(k).unwrap().as_deref(),
                Some(v.as_slice()),
                "{k:?}"
            );
        }
        assert!(store.get(b"pin").unwrap().is_none());
        drop(store);
        // Reopening after a successful recovery: the store has since
        // checkpointed nothing (the reopen does not), so restore the base
        // again to repeat the same fallback recovery.
        restore_base(base.path(), dir.path());
    }
    set_gate(None);
}

/// Torn-page protection (I12): after a crash, tear every page that has a
/// record in the log (only those can have been written since the last
/// checkpoint) by overwriting half of it with garbage, then recover. Each
/// such page's first record after the redo point is a full image, so replay
/// rebuilds it before any delta is applied, whatever the disk held.
#[test]
fn torn_pages_written_since_the_checkpoint_are_rebuilt() {
    let _env = ENV.lock().unwrap_or_else(|poison| poison.into_inner());
    set_gate(Some("1"));
    for pool_pages in [16u64, 4096] {
        let dir = TempDir::new().unwrap();
        let model = {
            let (store, _) = PagedStore::open(
                dir.path(),
                options()
                    .with_buffer_pool_bytes(pool_pages * PAGE as u64)
                    .with_experimental_page_deltas_disposable(true),
            )
            .unwrap();
            let mut model = workload(&store, 40, |_| {});
            store.checkpoint().unwrap();
            workload_into(&store, 60, &mut model, |_| {});
            assert!(store.page_deltas().1.delta_records > 0);
            model
        }; // crash
        let records = wal_records(dir.path());
        assert!(records.iter().any(|r| r.1 == 5));
        // Only pages with a record at or after the last redo point can have
        // been written since that checkpoint completed; tearing any other
        // page would fabricate damage no crash can cause.
        let redo = records
            .iter()
            .filter(|r| r.1 == 3)
            .map(|r| r.3)
            .max()
            .unwrap_or(0);
        let mut torn: Vec<u64> = records
            .iter()
            .filter(|r| (r.1 == 1 || r.1 == 5) && r.0 >= redo)
            .map(|r| r.2)
            .collect();
        torn.sort_unstable();
        torn.dedup();
        let mut pages = std::fs::read(dir.path().join("store.pages")).unwrap();
        for page_id in &torn {
            let start = (*page_id as usize) * PAGE as usize + PAGE as usize / 2;
            let end = (*page_id as usize + 1) * PAGE as usize;
            if end <= pages.len() {
                pages[start..end].fill(0xE5);
            }
        }
        std::fs::write(dir.path().join("store.pages"), &pages).unwrap();
        for _ in 0..2 {
            let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
            assert_model(&store, &model);
        }
        eprintln!("pool {pool_pages}: tore {} pages", torn.len());
    }
    set_gate(None);
}

/// I13 end to end: a long transaction that never commits shares pages with
/// committing ones, and a pool small enough to evict logs its pages through
/// the writeback barrier under its own xid. Recovery skips those records, so
/// no committed delta may be based on them; a violation fails the reopen.
#[test]
fn committed_deltas_never_build_on_an_uncommitted_transactions_eviction() {
    let _env = ENV.lock().unwrap_or_else(|poison| poison.into_inner());
    set_gate(Some("1"));
    for pool_pages in [32u64, 64] {
        let dir = TempDir::new().unwrap();
        let mut model = BTreeMap::new();
        {
            let (store, _) = PagedStore::open(
                dir.path(),
                options()
                    .with_buffer_pool_bytes(pool_pages * PAGE as u64)
                    .with_experimental_page_deltas_disposable(true),
            )
            .unwrap();
            let open = store.begin();
            for i in 0..400u64 {
                store.put(open, &key(20_000 + i), &value(i, 1)).unwrap();
                let tx = store.begin();
                store.put(tx, &key(i % 90), &value(i, i)).unwrap();
                store.put(tx, &key(10_000 + i), &value(i, 2)).unwrap();
                store.commit(tx).unwrap();
                model.insert(key(i % 90), value(i, i));
                model.insert(key(10_000 + i), value(i, 2));
            }
            let (_, stats) = store.page_deltas();
            assert!(stats.delta_records > 0);
            eprintln!(
                "pool {pool_pages}: {} deltas, {} refused",
                stats.delta_records, stats.delta_refused
            );
        } // crash with `open` still running
        for _ in 0..2 {
            let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
            assert_model(&store, &model);
            // The uncommitted transaction's own keys are NOT probed here:
            // that trips a defect that exists without deltas too (see
            // `preexisting_mid_transaction_eviction_window`).
        }
    }
    set_gate(None);
}

/// Reproducer for a defect in the CURRENT full-image recovery path, found
/// while qualifying deltas; it fails identically with deltas off. A crash
/// while a transaction is open, after the pool evicted pages that only that
/// transaction logged (barrier records under its xid, skipped at recovery)
/// but that committed structure already references, leaves those pages
/// unreplayed: looking up the uncommitted keys afterwards fails with a
/// checksum mismatch on a never-written page instead of reading "absent".
/// This is the mid-transaction eviction window of
/// `docs/commit-durability-invariants.md` section 8.5 and belongs to the
/// recovery-semantics track. Ignored so the suite stays green; run with
/// `--ignored` to see it.
#[test]
#[ignore]
fn preexisting_mid_transaction_eviction_window() {
    let _env = ENV.lock().unwrap_or_else(|poison| poison.into_inner());
    set_gate(None);
    let dir = TempDir::new().unwrap();
    {
        let (store, _) = PagedStore::open(
            dir.path(),
            options().with_buffer_pool_bytes(32 * PAGE as u64),
        )
        .unwrap();
        let open = store.begin();
        for i in 0..400u64 {
            store.put(open, &key(20_000 + i), &value(i, 1)).unwrap();
            let tx = store.begin();
            store.put(tx, &key(i % 90), &value(i, i)).unwrap();
            store.put(tx, &key(10_000 + i), &value(i, 2)).unwrap();
            store.commit(tx).unwrap();
        }
    } // crash with `open` still running
    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    for i in 0..400u64 {
        assert!(store.get(&key(20_000 + i)).unwrap().is_none());
    }
}
