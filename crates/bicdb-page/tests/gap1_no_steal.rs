//! GAP-1 structural closure: each original crash window remains exercised.
//! Pressure now makes complete invisible cuts instead of refusing valid work.
//! The historical interim refusal assertions are preserved in the PR81 source.
//!
//! A crash is a `drop` without checkpoint: nothing is flushed on drop, so the
//! page file holds exactly what writeback put there and the log holds exactly
//! what was appended (`fsync` is off, so "appended" is "durable" for a process
//! crash). Every case reopens the store and reconciles it against the keys it
//! acknowledged: each committed key must read back with its value, and no key
//! of an uncommitted or aborted transaction may be visible.

use std::path::Path;

use bicdb_page::{
    PageError, PagedCheckpointCursor, PagedCheckpointLimits, PagedCheckpointPhase, PagedStore,
    PagedStoreOptions, WritebackLimits,
};
use tempfile::TempDir;

const PAGE_SIZE: u32 = 512;

/// Large enough that nothing is evicted behind the test's back.
fn roomy() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(PAGE_SIZE)
        .with_buffer_pool_bytes(1024 * u64::from(PAGE_SIZE))
        .with_fsync(false)
        .with_wal_max_bytes(1 << 30)
}

/// 64 frames in 8 shards: reads evict, and a few hundred keys outgrow it.
fn small() -> PagedStoreOptions {
    roomy().with_buffer_pool_bytes(64 * u64::from(PAGE_SIZE))
}

/// 512 frames, 64 per shard: the admission reserve (8 frames a shard) is
/// enough for one operation and the reads next to it.
fn medium() -> PagedStoreOptions {
    roomy().with_buffer_pool_bytes(512 * u64::from(PAGE_SIZE))
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:04}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("value-{index}").into_bytes()
}

fn one_page() -> WritebackLimits {
    WritebackLimits {
        max_candidates: 1,
        max_io_bytes: 2 * u64::from(PAGE_SIZE),
        max_duration_millis: 10_000,
    }
}

/// Commit `range` in small transactions, then checkpoint: from here on a
/// committed page's only copy is the page file.
fn committed_and_checkpointed(
    dir: &Path,
    options: PagedStoreOptions,
    range: std::ops::Range<usize>,
) -> PagedStore {
    let (store, _) = PagedStore::open(dir, options).unwrap();
    let all: Vec<usize> = range.collect();
    for chunk in all.chunks(3) {
        let xid = store.begin();
        for index in chunk {
            store.put(xid, &key(*index), &value(*index)).unwrap();
        }
        store.commit(xid).unwrap();
        // With no other transaction open, a commit leaves nothing unlogged.
        assert_eq!(
            store.buffer_pool().unlogged_pages(),
            0,
            "after committing up to {chunk:?}"
        );
    }
    store.checkpoint().unwrap();
    store
}

/// Keys of an uncommitted transaction that land in one leaf of the committed
/// range and split it.
fn splitting_keys() -> Vec<Vec<u8>> {
    (0..40)
        .map(|j| format!("key-0150-{j:03}").into_bytes())
        .collect()
}

/// Reopen and reconcile: `committed` must read back, `absent` must not.
fn reconcile(dir: &Path, committed: &[(Vec<u8>, Vec<u8>)], absent: &[Vec<u8>]) -> Vec<String> {
    reconcile_with(dir, committed, absent, true)
}

/// `verify = false` skips `verify_integrity`, which never returns on some
/// stores damaged by GAP-1 W1-L (measured; see the steal-before-commit test).
fn reconcile_with(
    dir: &Path,
    committed: &[(Vec<u8>, Vec<u8>)],
    absent: &[Vec<u8>],
    verify: bool,
) -> Vec<String> {
    let trace = std::env::var_os("GAP1_TRACE").is_some();
    if trace {
        eprintln!("reconcile: open");
    }
    let store = match PagedStore::open(dir, roomy()) {
        Ok((store, _)) => store,
        Err(error) => return vec![format!("reopen failed: {error}")],
    };
    let mut failures = Vec::new();
    for (key, value) in committed {
        if trace {
            eprintln!("reconcile: get {}", String::from_utf8_lossy(key));
        }
        match store.get(key) {
            Ok(Some(found)) if &found == value => {}
            other => failures.push(format!(
                "acknowledged {} reads {:?}",
                String::from_utf8_lossy(key),
                other.map(|found| found.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
            )),
        }
    }
    for key in absent {
        if trace {
            eprintln!("reconcile: get absent {}", String::from_utf8_lossy(key));
        }
        match store.get(key) {
            Ok(None) => {}
            other => failures.push(format!(
                "unacknowledged {} reads {:?}",
                String::from_utf8_lossy(key),
                other.map(|found| found.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
            )),
        }
    }
    if !verify {
        return failures;
    }
    if trace {
        eprintln!("reconcile: verify_integrity");
    }
    match store.verify_integrity(8) {
        Ok(report) if report.valid => {}
        Ok(report) => failures.push(format!("integrity: {report:?}")),
        Err(error) => failures.push(format!("integrity check failed: {error}")),
    }
    failures
}

fn acknowledged(range: std::ops::Range<usize>) -> Vec<(Vec<u8>, Vec<u8>)> {
    range.map(|index| (key(index), value(index))).collect()
}

fn assert_no_failures(failures: Vec<String>) {
    let total = failures.len();
    assert!(
        failures.is_empty(),
        "{total} failures, e.g.:\n{}",
        failures.into_iter().take(12).collect::<Vec<_>>().join("\n")
    );
}

/// W1, eviction. An uncommitted transaction splits a leaf of committed keys,
/// then reads of other pages make the pool evict. Eviction used to write
/// whichever dirty frame it met, the uncommitted split pages included. It now
/// takes only frames without unlogged changes, so the split stays in memory
/// and the crash leaves the page file as the last checkpoint wrote it.
#[test]
fn w1_eviction_under_read_pressure_never_writes_an_uncommitted_split() {
    const COMMITTED: usize = 300;
    let crashed = splitting_keys();
    let mut failures = Vec::new();
    for reads in [0usize, 5, 20, 60, 150, 300] {
        let dir = TempDir::new().unwrap();
        let steals = {
            let store = committed_and_checkpointed(dir.path(), small(), 0..COMMITTED);
            let xid = store.begin();
            for key in &crashed {
                store.put(xid, key, b"uncommitted").unwrap();
            }
            for index in (0..COMMITTED).rev().take(reads) {
                assert_eq!(store.get(&key(index)).unwrap(), Some(value(index)));
            }
            let snapshot = store.buffer_pool().snapshot();
            assert!(snapshot.unlogged_pages >= 3, "{snapshot:?}");
            snapshot.unlogged_steals
            // Crash before commit.
        };
        if steals != 0 {
            failures.push(format!("{reads} reads: {steals} unlogged frames stolen"));
        }
        for failure in reconcile(dir.path(), &acknowledged(0..COMMITTED), &crashed) {
            failures.push(format!("{reads} reads: {failure}"));
        }
    }
    assert_no_failures(failures);
}

/// W2, the bounded checkpoint without the writer gate. The pgwire checkpoint
/// supervisor drives `checkpoint_step` next to in-flight transactions. Its
/// Drain phase used to write their pages; it now leaves them dirty, and the
/// phase advances (by escalation) to Freeze and Finalize, which take the
/// gate. A crash at every Drain step keeps the committed keys.
#[test]
fn w2_bounded_drain_next_to_an_in_flight_split_keeps_committed_keys() {
    const COMMITTED: usize = 300;
    let crashed = splitting_keys();
    let limits = PagedCheckpointLimits {
        writeback: one_page(),
        ..PagedCheckpointLimits::default()
    };
    let mut failures = Vec::new();
    for crash_after_steps in [1usize, 2, 4, 8, 16, 64] {
        let dir = TempDir::new().unwrap();
        {
            let store = committed_and_checkpointed(dir.path(), roomy(), 0..COMMITTED);
            // Committed but not yet written: Drain may (and should) write
            // these, next to the in-flight transaction.
            let xid = store.begin();
            store.put(xid, &key(COMMITTED), &value(COMMITTED)).unwrap();
            store.commit(xid).unwrap();
            let in_flight = store.begin();
            for key in &crashed {
                store.put(in_flight, key, b"uncommitted").unwrap();
            }
            let mut cursor = PagedCheckpointCursor::default();
            for _ in 0..crash_after_steps {
                if cursor.phase != PagedCheckpointPhase::Drain {
                    break;
                }
                cursor = store.checkpoint_step(cursor, limits).unwrap().next_cursor;
            }
            let snapshot = store.buffer_pool().snapshot();
            if snapshot.unlogged_pages == 0 {
                failures.push(format!(
                    "{crash_after_steps} steps: the in-flight pages left memory"
                ));
            }
            // Crash with the transaction in flight.
        }
        for failure in reconcile(dir.path(), &acknowledged(0..COMMITTED + 1), &crashed) {
            failures.push(format!("{crash_after_steps} drain steps: {failure}"));
        }
    }
    assert_no_failures(failures);
}

/// W3, the root-split flush, on a store whose committed keys fit in one
/// leaf: the uncommitted transaction grows the tree a level, and its keys
/// sort first, so the committed keys are the ones moved into the new right
/// half. Before, the flush wrote the split old root in place while the meta
/// page still named it.
#[test]
fn w3_an_uncommitted_root_split_never_reaches_the_page_file() {
    let mut failures = Vec::new();
    for committed in [2usize, 5, 8] {
        let crashed: Vec<Vec<u8>> = (0..60).map(|j| format!("a-{j:03}").into_bytes()).collect();
        let dir = TempDir::new().unwrap();
        {
            let store = committed_and_checkpointed(dir.path(), roomy(), 0..committed);
            let pages_before = store.buffer_pool().snapshot().writebacks;
            let xid = store.begin();
            for key in &crashed {
                store.put(xid, key, b"uncommitted").unwrap();
            }
            let written = store.buffer_pool().snapshot().writebacks - pages_before;
            if written != 0 {
                failures.push(format!(
                    "{committed} committed keys: {written} uncommitted pages written"
                ));
            }
            // Crash before commit.
        }
        for failure in reconcile(dir.path(), &acknowledged(0..committed), &crashed) {
            failures.push(format!("{committed} committed keys: {failure}"));
        }
    }
    assert_no_failures(failures);
}

/// W3 followed by commit: the root change becomes durable with its
/// transaction, and a crash after the commit keeps both sides.
#[test]
fn w3_a_committed_root_split_survives_a_crash() {
    let dir = TempDir::new().unwrap();
    let crashed: Vec<(Vec<u8>, Vec<u8>)> = (0..60)
        .map(|j| (format!("a-{j:03}").into_bytes(), b"committed-late".to_vec()))
        .collect();
    {
        let store = committed_and_checkpointed(dir.path(), roomy(), 0..4);
        let xid = store.begin();
        for (key, value) in &crashed {
            store.put(xid, key, value).unwrap();
        }
        store.commit(xid).unwrap();
        // Crash after commit, before any checkpoint.
    }
    let mut expected = acknowledged(0..4);
    expected.extend(crashed);
    assert_no_failures(reconcile(dir.path(), &expected, &[]));
}

/// W5, an aborted transaction's pages. They keep the aborted split in
/// memory. Before, they stayed unlogged until the next commit, and the
/// bounded checkpoint's Finalize (under the gate) wrote them one page per
/// step under the aborted xid, so a crash between steps left part of an
/// aborted split in the page file. The abort now logs them as structural
/// images, and every crash point in the following checkpoint keeps the
/// committed keys and hides the aborted ones.
#[test]
fn w5_an_aborted_split_checkpointed_page_by_page_keeps_committed_keys() {
    const COMMITTED: usize = 300;
    let crashed = splitting_keys();
    let limits = PagedCheckpointLimits {
        writeback: one_page(),
        ..PagedCheckpointLimits::default()
    };
    // Steps to completion, counted once.
    let total_steps = {
        let dir = TempDir::new().unwrap();
        let store = committed_and_checkpointed(dir.path(), roomy(), 0..COMMITTED);
        let xid = store.begin();
        for key in &crashed {
            store.put(xid, key, b"aborted").unwrap();
        }
        store.abort(xid).unwrap();
        assert_eq!(
            store.buffer_pool().unlogged_pages(),
            0,
            "abort must leave no unlogged pages"
        );
        let mut cursor = PagedCheckpointCursor::default();
        let mut steps = 0usize;
        while cursor.phase != PagedCheckpointPhase::Complete {
            cursor = store.checkpoint_step(cursor, limits).unwrap().next_cursor;
            steps += 1;
            assert!(steps < 10_000, "checkpoint does not converge");
        }
        steps
    };
    assert!(total_steps >= 4, "{total_steps}");
    let mut failures = Vec::new();
    for crash_after in 0..=total_steps {
        let dir = TempDir::new().unwrap();
        {
            let store = committed_and_checkpointed(dir.path(), roomy(), 0..COMMITTED);
            let xid = store.begin();
            for key in &crashed {
                store.put(xid, key, b"aborted").unwrap();
            }
            store.abort(xid).unwrap();
            let mut cursor = PagedCheckpointCursor::default();
            for _ in 0..crash_after {
                if cursor.phase == PagedCheckpointPhase::Complete {
                    break;
                }
                cursor = store.checkpoint_step(cursor, limits).unwrap().next_cursor;
            }
        }
        for failure in reconcile(dir.path(), &acknowledged(0..COMMITTED), &crashed) {
            failures.push(format!(
                "crash after {crash_after}/{total_steps} steps: {failure}"
            ));
        }
    }
    assert_no_failures(failures);
}

/// W5, and the abort's unit is all or nothing. The abort appends its abort
/// record, then one image per unlogged page under a fresh transaction, then
/// that transaction's commit record. The log write fails at every position
/// in turn (persistently, as a failing disk would), and the process then
/// crashes. Whatever prefix reached the log, recovery must apply none of the
/// unit's images unless its commit record is there: the committed keys stay
/// readable, the file stays consistent, and the aborted keys stay invisible.
/// Before, the images were structural (xid 0), replayed unconditionally,
/// and a prefix of them left a partial aborted split in the page file.
#[test]
fn w5_an_abort_failing_at_every_log_write_leaves_committed_keys_intact() {
    const COMMITTED: usize = 300;
    let crashed = splitting_keys();
    let pages = {
        let dir = TempDir::new().unwrap();
        let store = committed_and_checkpointed(dir.path(), roomy(), 0..COMMITTED);
        let xid = store.begin();
        for key in &crashed {
            store.put(xid, key, b"aborted").unwrap();
        }
        store.buffer_pool().unlogged_pages()
    };
    assert!(pages >= 3, "{pages}");
    // Write 1: the abort record; writes 2..=pages+1: the images; write
    // pages+2: the unit's commit record; one more: no failure at all.
    let mut failures = Vec::new();
    for failing_write in 1..=pages + 3 {
        let dir = TempDir::new().unwrap();
        {
            let store = committed_and_checkpointed(dir.path(), roomy(), 0..COMMITTED);
            let xid = store.begin();
            for key in &crashed {
                store.put(xid, key, b"aborted").unwrap();
            }
            let fault = bicdb_page::test_faults::arm_io_failure(
                "page_log.write",
                dir.path(),
                failing_write,
                true,
            );
            let aborted = store.abort(xid);
            if failing_write <= pages + 2 && aborted.is_ok() {
                failures.push(format!("write {failing_write}: the abort did not fail"));
            }
            if aborted.is_err() && store.buffer_pool().unlogged_pages() < 3 {
                failures.push(format!(
                    "write {failing_write}: a failed abort marked its pages logged"
                ));
            }
            // Whatever a later writeback attempts, it cannot write them.
            let _ = store.writeback_step(Default::default(), WritebackLimits::default());
            drop(fault);
            // Crash.
        }
        for failure in reconcile(dir.path(), &acknowledged(0..COMMITTED), &crashed) {
            failures.push(format!("write {failing_write} of {}: {failure}", pages + 2));
        }
    }
    assert_no_failures(failures);
}

/// A transaction id is never handed out again after a crash while its
/// versions may be in the recovered pages. Transaction B stays open while A
/// commits (or aborts); A's commit logs every unlogged page, B's included,
/// and B is named by no record of its own. Before, the reopened store began
/// its next transaction at B's id, and committing it made B's uncommitted
/// version visible.
#[test]
fn an_open_transactions_xid_is_not_reissued_after_a_crash() {
    for finish_a_by_commit in [true, false] {
        let dir = TempDir::new().unwrap();
        let b = {
            let store = committed_and_checkpointed(dir.path(), roomy(), 0..20);
            let a = store.begin();
            let b = store.begin();
            store.put(a, b"a-key", b"from a").unwrap();
            store.put(b, b"b-key", b"uncommitted b").unwrap();
            if finish_a_by_commit {
                store.commit(a).unwrap();
            } else {
                store.abort(a).unwrap();
            }
            b
            // Crash with B open.
        };
        let (store, _) = PagedStore::open(dir.path(), roomy()).unwrap();
        let next = store.begin();
        assert!(
            next > b,
            "commit={finish_a_by_commit}: xid {next} reissued (B was {b})"
        );
        store.put(next, b"other", b"x").unwrap();
        store.commit(next).unwrap();
        assert_eq!(
            store.get(b"b-key").unwrap(),
            None,
            "commit={finish_a_by_commit}: B's uncommitted version became visible"
        );
        let expected_a = if finish_a_by_commit {
            Some(b"from a".to_vec())
        } else {
            None
        };
        assert_eq!(store.get(b"a-key").unwrap(), expected_a);
        drop(store);
        assert_no_failures(reconcile(
            dir.path(),
            &acknowledged(0..20),
            &[b"b-key".to_vec()],
        ));
    }
}

/// Oversized page-only work uses complete cuts, then aborts invisibly.
#[test]
fn a_transaction_that_outgrows_the_pool_aborts_without_stealing() {
    let dir = TempDir::new().unwrap();
    let big: Vec<Vec<u8>> = (0..2_000)
        .map(|j| format!("big-{j:05}-{}", "x".repeat(40)).into_bytes())
        .collect();
    {
        let store = committed_and_checkpointed(dir.path(), medium(), 0..100);
        let xid = store.begin();
        for key in &big {
            store.put(xid, key, b"too large a transaction").unwrap();
        }
        assert_eq!(store.buffer_pool().unlogged_steals(), 0);
        store.abort(xid).unwrap();
        assert_eq!(store.buffer_pool().unlogged_pages(), 0);
        let next = store.begin();
        store.put(next, &key(100), &value(100)).unwrap();
        store.commit(next).unwrap();
        assert_eq!(store.buffer_pool().unlogged_steals(), 0);
    }
    assert_no_failures(reconcile(dir.path(), &acknowledged(0..101), &big));
}

/// A large pending writer does not prevent an unrelated writer from committing.
#[test]
fn a_large_pending_writer_allows_an_unrelated_small_commit() {
    let dir = TempDir::new().unwrap();
    let store = committed_and_checkpointed(dir.path(), medium(), 0..100);
    let big = store.begin();
    for index in 0..2_000 {
        store
            .put(
                big,
                format!("big-{index:06}-{}", "x".repeat(40)).as_bytes(),
                b"v",
            )
            .unwrap();
    }
    let small = store.begin();
    store.put(small, b"small", b"v").unwrap();
    store.commit(small).unwrap();
    assert_eq!(
        store
            .get(b"big-000000-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
            .unwrap(),
        None
    );
    store.abort(big).unwrap();
    assert_eq!(store.get(b"small").unwrap().as_deref(), Some(&b"v"[..]));
    drop(store);
    let mut expected = acknowledged(0..100);
    expected.push((b"small".to_vec(), b"v".to_vec()));
    assert_no_failures(reconcile(
        dir.path(),
        &expected,
        &[b"big-000000-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_vec()],
    ));
}

/// Large pending work followed by a crash before the caller can abort.
#[test]
fn a_large_transaction_that_crashes_before_its_abort_leaves_no_visible_trace() {
    let dir = TempDir::new().unwrap();
    let big: Vec<Vec<u8>> = (0..20_000)
        .map(|j| format!("big-{j:05}-{}", "x".repeat(40)).into_bytes())
        .collect();
    let mut written = 0usize;
    {
        let store = committed_and_checkpointed(dir.path(), medium(), 0..100);
        let xid = store.begin();
        for key in big.iter().take(2_000) {
            store.put(xid, key, b"pending").unwrap();
            written += 1;
        }
        // Readers keep working and do not steal.
        for index in 0..100 {
            assert_eq!(store.get(&key(index)).unwrap(), Some(value(index)));
        }
        assert_eq!(store.buffer_pool().unlogged_steals(), 0);
        // Crash.
    }
    assert_no_failures(reconcile(
        dir.path(),
        &acknowledged(0..100),
        &big[..written],
    ));
}

/// External logical redo grants no permission to steal unlogged pages.
/// A committed transaction larger than the pool recovers every row.
#[test]
fn a_redo_backed_transaction_larger_than_the_pool_never_steals() {
    let dir = TempDir::new().unwrap();
    let rows: Vec<(Vec<u8>, Vec<u8>)> = (0..2_000)
        .map(|j| (format!("big-{j:05}").into_bytes(), value(j)))
        .collect();
    {
        let store = committed_and_checkpointed(dir.path(), small(), 0..100);
        let (xid, _) = store.begin_redo_backed();
        for (key, value) in &rows {
            store.put(xid, key, value).unwrap();
        }
        assert_eq!(store.buffer_pool().unlogged_steals(), 0);
        store.commit(xid).unwrap();
        // Crash after commit.
    }
    let mut expected = acknowledged(0..100);
    expected.extend(rows);
    assert_no_failures(reconcile(dir.path(), &expected, &[]));
}

/// The page file's and the page log's bytes, for before/after comparison.
fn store_files(dir: &Path) -> (Vec<u8>, Vec<u8>) {
    let read = |name: &str| std::fs::read(dir.join(name)).unwrap_or_default();
    (read("store.pages"), read("store.wal"))
}

/// Every visible key and value, by a full scan.
fn visible(dir: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (store, _) = PagedStore::open(dir, roomy()).unwrap();
    let snapshot = store.latest_snapshot();
    let rows: Vec<(Vec<u8>, Vec<u8>)> = store
        .scan_from(&snapshot, b"")
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    rows
}

/// Cuts may persist complete invisible structure, but never expose pending rows.
#[test]
fn an_oversized_pending_transaction_persists_only_invisible_complete_cuts() {
    let dir = TempDir::new().unwrap();
    let mut absent = Vec::new();
    {
        let store = committed_and_checkpointed(dir.path(), medium(), 0..100);
        let before_wal = store_files(dir.path()).1.len();
        let before_evictions = store.buffer_pool().snapshot().evictions;
        let xid = store.begin();
        for written in 0..2_000 {
            let key = format!("big-{written:06}-{}", "x".repeat(40)).into_bytes();
            store.put(xid, &key, b"pending").unwrap();
            absent.push(key);
        }
        assert_eq!(store.buffer_pool().unlogged_steals(), 0);
        assert!(store.buffer_pool().snapshot().evictions > before_evictions);
        assert!(store_files(dir.path()).1.len() > before_wal);
    }
    assert_eq!(visible(dir.path()), acknowledged(0..100));
    for _ in 0..2 {
        assert_no_failures(reconcile(dir.path(), &acknowledged(0..100), &absent));
    }
}

/// Both small and oversized aborts preserve only complete invisible state.
/// Repeated recovery retains exactly the earlier acknowledged rows.
#[test]
fn an_aborted_oversized_transaction_persists_only_complete_invisible_state() {
    for limit in [60, 2_000] {
        let dir = TempDir::new().unwrap();
        {
            let store = committed_and_checkpointed(dir.path(), medium(), 0..100);
            let xid = store.begin();
            let mut written = 0usize;
            while written < limit {
                let key = format!("big-{written:06}-{}", "x".repeat(40));
                store.put(xid, key.as_bytes(), b"aborted").unwrap();
                written += 1;
            }
            // Allocation already wrote empty typed pages (W8); the abort
            // itself must write only the page log.
            store.abort(xid).unwrap();
            assert_eq!(store.buffer_pool().unlogged_pages(), 0);
            assert_eq!(store.buffer_pool().unlogged_steals(), 0);
            // Crash.
        }
        let first = visible(dir.path());
        assert_eq!(first, acknowledged(0..100), "limit={limit}");
        assert_no_failures(reconcile(dir.path(), &acknowledged(0..100), &[]));
        // A second recovery (of the already recovered store) changes nothing.
        assert_eq!(visible(dir.path()), first, "limit={limit}");
    }
}

/// The last-resort steal, explicitly, at the page level: a redo-backed
/// transaction many times the pool steals; the process crashes right after
/// the commit that follows `k` steals, and everything must be recovered.
#[test]
fn pressure_cuts_with_a_commit_survive_a_crash() {
    for steals_before_commit in [1u64, 5, 20] {
        let dir = TempDir::new().unwrap();
        let mut expected = acknowledged(0..100);
        {
            let store = committed_and_checkpointed(dir.path(), small(), 0..100);
            let (xid, _) = store.begin_redo_backed();
            let mut index = 0usize;
            let initial_evictions = store.buffer_pool().snapshot().evictions;
            let initial_wal = store_files(dir.path()).1.len();
            while store.buffer_pool().snapshot().evictions
                < initial_evictions + steals_before_commit
                || store_files(dir.path()).1.len() == initial_wal
            {
                let (key, value) = (format!("big-{index:06}").into_bytes(), value(index));
                store.put(xid, &key, &value).unwrap();
                expected.push((key, value));
                index += 1;
                assert!(
                    index < 20_000,
                    "fewer than {steals_before_commit} steals in 20,000 puts"
                );
            }
            assert_eq!(store.buffer_pool().unlogged_steals(), 0);
            store.commit(xid).unwrap();
            // Crash after the commit.
        }
        expected.sort();
        assert_no_failures(
            reconcile(dir.path(), &expected, &[])
                .into_iter()
                .map(|failure| format!("after {steals_before_commit} steals: {failure}"))
                .collect(),
        );
    }
}

/// The last-resort steal followed by a crash *before* the stealing
/// transaction commits: GAP-1 W1-L, the case the interim leaves open. A
/// stolen split moves acknowledged keys the crash can lose, or leaves pages
/// recovery does not restore. Kept to measure the hole; closing it needs the
/// structural cuts of option (b). Reads only: on some of these stores
/// `verify_integrity` does not return (measured after 5 steals).
#[test]
fn pressure_cuts_before_user_commit_keep_acknowledged_keys() {
    let mut failures = Vec::new();
    for steals in [1u64, 5, 20] {
        let dir = TempDir::new().unwrap();
        let mut crashed = Vec::new();
        {
            let store = committed_and_checkpointed(dir.path(), small(), 0..100);
            let (xid, _) = store.begin_redo_backed();
            let mut index = 0usize;
            let initial_evictions = store.buffer_pool().snapshot().evictions;
            let initial_wal = store_files(dir.path()).1.len();
            while store.buffer_pool().snapshot().evictions < initial_evictions + steals
                || store_files(dir.path()).1.len() == initial_wal
            {
                let key = format!("big-{index:06}").into_bytes();
                store.put(xid, &key, b"uncommitted").unwrap();
                crashed.push(key);
                index += 1;
                assert!(index < 20_000, "fewer than {steals} steals in 20,000 puts");
            }
            assert_eq!(store.buffer_pool().unlogged_steals(), 0);
            eprintln!("evictions={steals}: crashing after {index} puts");
            // Crash before commit.
        }
        eprintln!("steals={steals}: reconciling");
        for failure in reconcile_with(dir.path(), &acknowledged(0..100), &crashed, true) {
            failures.push(format!("after {steals} steals: {failure}"));
        }
    }
    assert_no_failures(failures);
}

/// GAP-1 W8 severity probe. `PageStore::allocate` pops a page from the
/// free list in memory and writes it to the page file at once, zeroed with
/// its new type; the superblock that lists the free pages is written
/// separately (by any page-store flush). An uncommitted transaction that
/// reuses free pages and then crashes must leave a store that can allocate
/// again: the next transaction must commit, and every acknowledged key must
/// read back.
#[test]
fn a_crash_after_an_uncommitted_transaction_reused_free_pages_leaves_a_writable_store() {
    let dir = TempDir::new().unwrap();
    let survivors = acknowledged(0..50);
    {
        let store = committed_and_checkpointed(dir.path(), medium(), 0..50);
        // Rows that are written, deleted and vacuumed, so their heap pages
        // return to the free list.
        let doomed: Vec<Vec<u8>> = (0..600)
            .map(|j| format!("doomed-{j:05}-{}", "y".repeat(40)).into_bytes())
            .collect();
        for chunk in doomed.chunks(20) {
            let xid = store.begin();
            for key in chunk {
                store.put(xid, key, b"soon gone").unwrap();
            }
            store.commit(xid).unwrap();
        }
        for chunk in doomed.chunks(20) {
            let xid = store.begin();
            for key in chunk {
                store.delete(xid, key).unwrap();
            }
            store.commit(xid).unwrap();
        }
        store.checkpoint().unwrap();
        for _ in 0..8 {
            store.vacuum(100_000).unwrap();
        }
        store.checkpoint().unwrap();
        let free_before = store.buffer_pool().store().free_page_count();
        assert!(free_before > 0, "precondition: vacuum freed pages");
        // An uncommitted transaction that takes pages from the free list.
        let xid = store.begin();
        let mut index = 0usize;
        while store.buffer_pool().store().free_page_count() + 2 > free_before && index < 5_000 {
            let key = format!("reuser-{index:05}-{}", "z".repeat(40));
            if store.put(xid, key.as_bytes(), b"uncommitted").is_err() {
                break;
            }
            index += 1;
        }
        assert!(
            store.buffer_pool().store().free_page_count() < free_before,
            "precondition: the transaction reused free pages"
        );
        // Crash.
    }
    let (store, _) = PagedStore::open(dir.path(), medium()).unwrap();
    for chunk in (0..400usize).collect::<Vec<_>>().chunks(20) {
        let xid = store.begin();
        for index in chunk {
            store
                .put(xid, format!("after-{index:05}").as_bytes(), b"v")
                .unwrap_or_else(|error| panic!("allocation after the crash failed: {error}"));
        }
        store.commit(xid).unwrap();
    }
    drop(store);
    let mut expected = survivors;
    expected
        .extend((0..400).map(|index| (format!("after-{index:05}").into_bytes(), b"v".to_vec())));
    expected.sort();
    assert_no_failures(reconcile(dir.path(), &expected, &[]));
}

/// Finding from the second review: an operation that fails after changing a
/// page can leave a half-done structure. A B-tree split rewrites the left
/// leaf in place, then records the split in the parent; the test hook makes
/// that second step fail, as an eviction I/O error, `PoolExhausted` or a
/// full disk would. The keys that moved to the new right sibling are then
/// reachable from nowhere in memory. Nothing may log those pages: the
/// transaction is poisoned and the store fails closed at once, so neither
/// its abort (whose unit would make the torn tree durable) nor any other
/// transaction's commit can. After the crash, recovery has only committed
/// records: every acknowledged key reads back, the structure verifies, and
/// no key of the failed transaction is visible.
#[test]
fn a_split_that_fails_half_done_fences_the_store_and_its_abort_logs_nothing() {
    const COMMITTED: usize = 300;
    let crashed = splitting_keys();
    let dir = TempDir::new().unwrap();
    {
        let store = committed_and_checkpointed(dir.path(), roomy(), 0..COMMITTED);
        let log_before = store_files(dir.path()).1;
        let xid = store.begin();
        let fault =
            bicdb_page::test_faults::arm_io_failure("btree.insert_separator", dir.path(), 1, false);
        let mut failed = None;
        for key in &crashed {
            if let Err(error) = store.put(xid, key, b"uncommitted") {
                failed = Some(error);
                break;
            }
        }
        assert!(fault.fired() >= 1, "the split never reached the hook");
        let error = failed.expect("the failed split must fail its put");
        assert!(matches!(error, PageError::Io { .. }), "{error}");
        assert!(
            store.durability_health().is_failed(),
            "a failure after a change must fence the store"
        );
        assert!(
            matches!(store.abort(xid), Err(PageError::WriteFailed { .. })),
            "the poisoned abort must report the fence"
        );
        let other = store.begin();
        assert!(
            store.put(other, b"after", b"refused").is_err(),
            "the fenced store accepted a write"
        );
        assert!(store.commit(other).is_err(), "the fenced store committed");
        assert_eq!(
            store_files(dir.path()).1,
            log_before,
            "the torn pages reached the page log"
        );
        drop(fault);
        // Crash.
    }
    assert_no_failures(reconcile(dir.path(), &acknowledged(0..COMMITTED), &crashed));
}

/// An operation that fails before it changes anything (here, a key too
/// large for the index) does not poison its transaction or fence the store:
/// the heap version is not written before the index check.
#[test]
fn an_operation_refused_before_any_change_does_not_fence_the_store() {
    let dir = TempDir::new().unwrap();
    let store = committed_and_checkpointed(dir.path(), roomy(), 0..20);
    let xid = store.begin();
    let releases = store.buffer_pool().write_guard_releases();
    let oversized = vec![b'k'; PAGE_SIZE as usize];
    assert!(matches!(
        store.put(xid, &oversized, b"v"),
        Err(PageError::ValueTooLarge { .. })
    ));
    assert_eq!(store.buffer_pool().write_guard_releases(), releases);
    assert!(!store.durability_health().is_failed());
    store.put(xid, b"fine", b"v").unwrap();
    store.commit(xid).unwrap();
    assert_eq!(store.get(b"fine").unwrap().as_deref(), Some(&b"v"[..]));
}

/// A transaction that wrote nothing aborts without logging or syncing.
#[test]
fn a_read_only_abort_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let store = committed_and_checkpointed(dir.path(), roomy(), 0..20);
    let before = store_files(dir.path());
    let xid = store.begin();
    assert_eq!(store.get(&key(3)).unwrap(), Some(value(3)));
    store.abort(xid).unwrap();
    // The abort record of a transaction that wrote nothing is all it adds.
    let after = store_files(dir.path());
    assert_eq!(after.0, before.0);
    assert!(
        after.1.len() - before.1.len() < 64,
        "a read-only abort logged pages: {} bytes",
        after.1.len() - before.1.len()
    );
}
