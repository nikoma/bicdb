//! GAP-1 interim (no-steal): one test per closed window of
//! `docs/recovery-semantics.md` section 14.1, plus the refusal that replaces
//! the last-resort steal for refusable transactions.
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
    let store = match PagedStore::open(dir, roomy()) {
        Ok((store, _)) => store,
        Err(error) => return vec![format!("reopen failed: {error}")],
    };
    let mut failures = Vec::new();
    for (key, value) in committed {
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
        match store.get(key) {
            Ok(None) => {}
            other => failures.push(format!(
                "unacknowledged {} reads {:?}",
                String::from_utf8_lossy(key),
                other.map(|found| found.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
            )),
        }
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

/// The last-resort steal, replaced for refusable transactions. A page-only
/// transaction that keeps writing into a small pool is refused before an
/// operation could need to steal, with nothing of that operation written.
/// The caller aborts; the pool is usable again at once, nothing was stolen,
/// and a crash keeps every acknowledged key and none of the refused ones.
#[test]
fn a_transaction_that_outgrows_the_pool_is_refused_before_it_steals() {
    let dir = TempDir::new().unwrap();
    let big: Vec<Vec<u8>> = (0..20_000)
        .map(|j| format!("big-{j:05}-{}", "x".repeat(40)).into_bytes())
        .collect();
    let mut written = 0usize;
    {
        let store = committed_and_checkpointed(dir.path(), medium(), 0..100);
        let xid = store.begin();
        let refusal = loop {
            match store.put(xid, &big[written], b"too large a transaction") {
                Ok(()) => written += 1,
                Err(error) => break error,
            }
            assert!(written < big.len(), "never refused");
        };
        assert!(
            matches!(
                refusal,
                PageError::TransactionExceedsBufferPool { transaction, .. } if transaction == xid
            ),
            "{refusal}"
        );
        assert!(written > 10, "refused too early: after {written} puts");
        assert_eq!(store.buffer_pool().unlogged_steals(), 0);
        assert!(store.refused_for_unlogged_pressure() >= 1);
        store.abort(xid).unwrap();
        assert_eq!(store.buffer_pool().unlogged_pages(), 0);

        // Not wedged: the next transaction commits.
        let next = store.begin();
        store.put(next, &key(100), &value(100)).unwrap();
        store.commit(next).unwrap();
        assert_eq!(store.buffer_pool().unlogged_steals(), 0);
        // Crash.
    }
    assert_no_failures(reconcile(
        dir.path(),
        &acknowledged(0..101),
        &big[..written],
    ));
}

/// Admission is per shard, so with one refusable transaction at the limit a
/// small unrelated one is refused too; the error names the transaction that
/// holds the pool, not only the one refused.
#[test]
fn a_refusal_names_the_transaction_that_holds_the_pool() {
    let dir = TempDir::new().unwrap();
    let store = committed_and_checkpointed(dir.path(), medium(), 0..100);
    let big = store.begin();
    let mut index = 0usize;
    while store
        .put(
            big,
            format!("big-{index:06}-{}", "x".repeat(40)).as_bytes(),
            b"v",
        )
        .is_ok()
    {
        index += 1;
        assert!(index < 50_000, "never refused");
    }
    let small = store.begin();
    match store.put(small, b"small", b"v") {
        Err(PageError::TransactionExceedsBufferPool {
            transaction,
            holder,
            holder_frames,
            ..
        }) => {
            assert_eq!(transaction, small);
            assert_eq!(holder, big, "the holder must be the big transaction");
            assert!(holder_frames > 0);
        }
        other => panic!("expected a refusal naming the holder, got {other:?}"),
    }
    store.abort(big).unwrap();
    // With the holder gone, the small transaction is admitted.
    store.put(small, b"small", b"v").unwrap();
    store.commit(small).unwrap();
    assert_eq!(store.get(b"small").unwrap().as_deref(), Some(&b"v"[..]));
}

/// The same refusal followed by a crash before the caller could abort.
#[test]
fn a_refused_transaction_that_crashes_before_its_abort_leaves_no_trace() {
    let dir = TempDir::new().unwrap();
    let big: Vec<Vec<u8>> = (0..20_000)
        .map(|j| format!("big-{j:05}-{}", "x".repeat(40)).into_bytes())
        .collect();
    let mut written = 0usize;
    {
        let store = committed_and_checkpointed(dir.path(), medium(), 0..100);
        let xid = store.begin();
        while store.put(xid, &big[written], b"refused").is_ok() {
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

/// A transaction whose redo is durable elsewhere (a row commit's page apply)
/// is never refused. If it outgrows the pool, eviction steals as a last
/// resort and counts it: GAP-1 stays open for it until the structural log
/// (option (b)). A commit followed by a crash recovers all of it.
#[test]
fn a_redo_backed_transaction_is_never_refused_and_its_steals_are_counted() {
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
        assert!(store.buffer_pool().unlogged_steals() > 0);
        assert_eq!(store.refused_for_unlogged_pressure(), 0);
        store.commit(xid).unwrap();
        // Crash after commit.
    }
    let mut expected = acknowledged(0..100);
    expected.extend(rows);
    assert_no_failures(reconcile(dir.path(), &expected, &[]));
}
