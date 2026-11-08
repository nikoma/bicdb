//! Recovery-semantics probes for the page store. See
//! `docs/recovery-semantics.md`; each test names the rule or GAP it checks.
//!
//! A crash is a `drop` without checkpoint: nothing is flushed on drop, so the
//! page file holds exactly what writeback put there and the log holds exactly
//! what was appended (these tests run with `fsync` off, so "appended" is
//! "durable" for a process crash).
//!
//! The tests marked `#[ignore]` reproduce a GAP in the current code. They fail
//! today and are kept runnable (`cargo test -- --ignored`) as the acceptance
//! check for a fix.

use std::path::{Path, PathBuf};

use bicdb_page::{PagedStore, PagedStoreOptions, WritebackCursor, WritebackLimits};
use tempfile::TempDir;

const PAGE_SIZE: u32 = 512;

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(PAGE_SIZE)
        // Large enough that nothing is evicted behind the test's back: the
        // test decides exactly which pages reach the page file.
        .with_buffer_pool_bytes(1024 * u64::from(PAGE_SIZE))
        .with_fsync(false)
        .with_wal_max_bytes(1 << 30)
}

fn open(dir: &Path) -> PagedStore {
    PagedStore::open(dir, options()).unwrap().0
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:04}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("value-{index}").into_bytes()
}

fn wal_bytes(dir: &Path) -> u64 {
    std::fs::metadata(dir.join("store.wal"))
        .map(|meta| meta.len())
        .unwrap_or(0)
}

/// Commit `range` in transactions of 50, then checkpoint so the page file
/// holds every committed page and the log is empty. From here on a committed
/// page's only copy is the page file.
fn committed_and_checkpointed(dir: &Path, range: std::ops::Range<usize>) -> PagedStore {
    let store = open(dir);
    let all: Vec<usize> = range.collect();
    for chunk in all.chunks(50) {
        let xid = store.begin();
        for index in chunk {
            store.put(xid, &key(*index), &value(*index)).unwrap();
        }
        store.commit(xid).unwrap();
    }
    store.checkpoint().unwrap();
    assert_eq!(
        wal_bytes(dir),
        0,
        "precondition: the checkpoint must truncate the log, or replay of \
         committed images would mask what the page file holds"
    );
    store
}

/// Every committed key must read back with its value; nothing of the crashed
/// transaction may be visible. Returns the failures instead of panicking so
/// a sweep can report all of them.
fn check_committed(dir: &Path, range: std::ops::Range<usize>, crashed: &[Vec<u8>]) -> Vec<String> {
    let store = match PagedStore::open(dir, options()) {
        Ok((store, _)) => store,
        Err(error) => return vec![format!("reopen failed: {error}")],
    };
    let mut failures = Vec::new();
    for index in range {
        match store.get(&key(index)) {
            Ok(Some(found)) if found == value(index) => {}
            Ok(other) => failures.push(format!(
                "committed key-{index:04} reads {:?}",
                other.map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            )),
            Err(error) => failures.push(format!("committed key-{index:04}: {error}")),
        }
    }
    for key in crashed {
        match store.get(key) {
            Ok(None) => {}
            Ok(Some(_)) => failures.push(format!(
                "uncommitted {} is visible",
                String::from_utf8_lossy(key)
            )),
            Err(error) => failures.push(format!("{}: {error}", String::from_utf8_lossy(key))),
        }
    }
    failures
}

fn one_page() -> WritebackLimits {
    WritebackLimits {
        max_candidates: 1,
        max_io_bytes: 2 * u64::from(PAGE_SIZE),
        max_duration_millis: 10_000,
    }
}

/// Q1, leaf split. A transaction splits a leaf that holds committed keys. A
/// bounded checkpoint drain (`writeback_step`, the Drain phase of
/// `checkpoint_step`, which does not take the writer gate) writes the dirty
/// pages one at a time in page order, and the process crashes after `k` of
/// them, for every `k`. Recovery skips the transaction's images because it
/// never committed, so whatever the drain wrote stays in the page file.
///
/// GAP-1 in `docs/recovery-semantics.md`: a prefix that contains the split
/// leaf but not its new right sibling leaves the committed keys that moved
/// right unreachable.
#[test]
fn q1_crash_after_partial_drain_of_an_uncommitted_leaf_split_keeps_committed_keys() {
    const COMMITTED: usize = 300;
    let crashed: Vec<Vec<u8>> = (0..40)
        .map(|j| format!("key-0150-{j:03}").into_bytes())
        .collect();

    // Count the drain steps once.
    let probe = TempDir::new().unwrap();
    let steps = {
        let store = committed_and_checkpointed(probe.path(), 0..COMMITTED);
        let xid = store.begin();
        for key in &crashed {
            store.put(xid, key, b"uncommitted").unwrap();
        }
        let mut cursor = WritebackCursor::default();
        let mut steps = 0usize;
        loop {
            let report = store.writeback_step(cursor, one_page()).unwrap();
            steps += report.pages_written as usize;
            if report.complete {
                break;
            }
            cursor = if report.complete {
                WritebackCursor::default()
            } else {
                report.next_cursor
            };
        }
        steps
    };
    // With the no-steal mitigation the online writeback skips the
    // uncommitted pages, so there may be nothing to write at all.


    let mut failures = Vec::new();
    for written in 0..=steps {
        let dir = TempDir::new().unwrap();
        {
            let store = committed_and_checkpointed(dir.path(), 0..COMMITTED);
            let xid = store.begin();
            for key in &crashed {
                store.put(xid, key, b"uncommitted").unwrap();
            }
            let mut cursor = WritebackCursor::default();
            let mut done = 0usize;
            while done < written {
                let report = store.writeback_step(cursor, one_page()).unwrap();
                done += report.pages_written as usize;
                cursor = if report.complete {
                    WritebackCursor::default()
                } else {
                    report.next_cursor
                };
            }
            // Crash: no commit record, no abort record.
        }
        for failure in check_committed(dir.path(), 0..COMMITTED, &crashed) {
            failures.push(format!("crash after {written}/{steps} pages: {failure}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Q1, root split. `BTree::insert_separator` flushes the whole pool when the
/// root splits (`btree.rs`, "The new root is made durable BEFORE anyone is
/// told about it"). Inside an uncommitted transaction that flush writes the
/// split root, now holding only its left half, in place. The meta page still
/// names that page as the root. A crash before commit then leaves the
/// committed keys that moved to the right sibling unreachable.
///
/// The crashed transaction's keys sort first, so the committed keys are the
/// ones pushed into the right half.
#[test]
fn q1_crash_after_an_uncommitted_root_split_keeps_committed_keys() {
    let mut failures = Vec::new();
    for committed in [2usize, 4, 6, 8] {
        let crashed: Vec<Vec<u8>> = (0..40).map(|j| format!("a-{j:03}").into_bytes()).collect();
        let dir = TempDir::new().unwrap();
        {
            let store = committed_and_checkpointed(dir.path(), 0..committed);
            let xid = store.begin();
            for key in &crashed {
                store.put(xid, key, b"uncommitted").unwrap();
            }
            // Crash before commit.
        }
        for failure in check_committed(dir.path(), 0..committed, &crashed) {
            failures.push(format!("{committed} committed keys: {failure}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// The safe direction of Q1: an uncommitted transaction whose pages were all
/// written (a full drain, or the checkpoint flush) before the crash. The
/// stolen pages form a complete tree and every committed key stays reachable;
/// the uncommitted versions stay invisible because their transaction id has
/// no commit record.
#[test]
fn a_fully_drained_uncommitted_transaction_keeps_committed_keys_and_stays_invisible() {
    const COMMITTED: usize = 300;
    let crashed: Vec<Vec<u8>> = (0..40)
        .map(|j| format!("key-0150-{j:03}").into_bytes())
        .collect();
    let dir = TempDir::new().unwrap();
    {
        let store = committed_and_checkpointed(dir.path(), 0..COMMITTED);
        let xid = store.begin();
        for key in &crashed {
            store.put(xid, key, b"uncommitted").unwrap();
        }
        store.checkpoint().unwrap();
    }
    let failures = check_committed(dir.path(), 0..COMMITTED, &crashed);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// ---------------------------------------------------------------------------
// Interrupted log reset.

fn segmented_options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(4096)
        .with_buffer_pool_bytes(4 * 1024 * 1024)
        .with_fsync(false)
        .with_wal_max_bytes(1 << 30)
        .with_extent_bytes(1024 * 1024)
        .with_wal_segment_bytes(64 * 1024)
}

fn sealed_segments(dir: &Path) -> Vec<PathBuf> {
    let mut sealed: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_prefix("store.wal."))
                .is_some_and(|suffix| suffix.parse::<u64>().is_ok())
        })
        .collect();
    sealed.sort_by_key(|path| {
        path.extension()
            .and_then(|ext| ext.to_str())
            .and_then(|ext| ext.parse::<u64>().ok())
    });
    sealed
}

/// `Wal::reset` truncates the active file first and deletes the sealed
/// segments after it, oldest first, with no directory sync. A crash (or a
/// power cut before the next journal commit) between the two leaves sealed
/// segments whose records all predate the published checkpoint horizon,
/// next to an empty active file.
///
/// GAP-2 in `docs/recovery-semantics.md`: recovery finds no checkpoint record
/// in that chain, replays it from the start, and writes older committed page
/// images over newer pages. Then the first record appended after the reopen
/// carries the horizon LSN, which does not continue the stale chain, so the
/// next recovery truncates the whole active file as a torn tail.
#[test]
#[ignore = "GAP-2: an interrupted log reset replays stale segments and gaps the chain"]
fn an_interrupted_log_reset_neither_regresses_pages_nor_loses_later_commits() {
    let dir = TempDir::new().unwrap();
    let hot = b"hot".to_vec();
    let filler = format!("filler-{}", "f".repeat(600));
    let stale_copy;
    let final_value;
    {
        let (store, _) = PagedStore::open(dir.path(), segmented_options()).unwrap();
        // Update the hot key until at least two segments sealed, so the hot
        // key's leaf and heap pages have images in the newest sealed segment.
        let mut generation = 0u64;
        while sealed_segments(dir.path()).len() < 2 {
            generation += 1;
            let xid = store.begin();
            store
                .put(xid, &hot, format!("v{generation}").as_bytes())
                .unwrap();
            store
                .put(
                    xid,
                    format!("fill-{generation}").as_bytes(),
                    filler.as_bytes(),
                )
                .unwrap();
            store.commit(xid).unwrap();
        }
        // One more update that lands in the active file only.
        let sealed_before = sealed_segments(dir.path()).len();
        generation += 1;
        let xid = store.begin();
        store
            .put(xid, &hot, format!("v{generation}").as_bytes())
            .unwrap();
        store.commit(xid).unwrap();
        assert_eq!(
            sealed_segments(dir.path()).len(),
            sealed_before,
            "precondition: the final update must not seal"
        );
        final_value = format!("v{generation}").into_bytes();

        // Keep the sealed chain as the crash would have left it.
        let keep = dir.path().join("kept");
        std::fs::create_dir(&keep).unwrap();
        stale_copy = sealed_segments(dir.path())
            .into_iter()
            .map(|path| {
                let target = keep.join(path.file_name().unwrap());
                std::fs::copy(&path, &target).unwrap();
                (target, path)
            })
            .collect::<Vec<_>>();
        store.checkpoint().unwrap();
        assert!(sealed_segments(dir.path()).is_empty());
        assert_eq!(wal_bytes(dir.path()), 0);
    }
    // The crash state: active truncated, sealed segments not yet deleted.
    for (kept, original) in &stale_copy {
        std::fs::rename(kept, original).unwrap();
    }

    let mut failures = Vec::new();
    {
        let (store, _) = PagedStore::open(dir.path(), segmented_options()).unwrap();
        let found = store.get(&hot).unwrap();
        if found.as_deref() != Some(final_value.as_slice()) {
            failures.push(format!(
                "after the interrupted reset the hot key reads {:?}, committed {:?}",
                found.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()),
                String::from_utf8_lossy(&final_value)
            ));
        }
        // A commit after the reopen, then a crash.
        let xid = store.begin();
        store.put(xid, b"after-reopen", b"committed").unwrap();
        store.commit(xid).unwrap();
    }
    {
        let (store, _) = PagedStore::open(dir.path(), segmented_options()).unwrap();
        let found = store.get(b"after-reopen").unwrap();
        if found.as_deref() != Some(&b"committed"[..]) {
            failures.push(format!(
                "a commit made after the reopen reads {:?} after the next crash",
                found.map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The legitimate partial-reset state: a crash after some, not all, sealed
/// segments were deleted while the active file still holds the checkpoint
/// record (deletion before truncation). The chain still reaches the
/// checkpoint record, recovery honours its redo point, and nothing regresses.
#[test]
fn a_chain_that_reaches_the_checkpoint_record_recovers_from_its_redo_point() {
    let dir = TempDir::new().unwrap();
    let hot = b"hot".to_vec();
    let filler = format!("filler-{}", "f".repeat(600));
    let final_value;
    {
        let (store, _) = PagedStore::open(dir.path(), segmented_options()).unwrap();
        let mut generation = 0u64;
        while sealed_segments(dir.path()).len() < 3 {
            generation += 1;
            let xid = store.begin();
            store
                .put(xid, &hot, format!("v{generation}").as_bytes())
                .unwrap();
            store
                .put(
                    xid,
                    format!("fill-{generation}").as_bytes(),
                    filler.as_bytes(),
                )
                .unwrap();
            store.commit(xid).unwrap();
        }
        final_value = format!("v{generation}").into_bytes();
        // A checkpoint that does not truncate: a backup pin defers the reset
        // and keeps every segment.
        store.begin_backup_pin();
        store.checkpoint().unwrap();
        assert!(sealed_segments(dir.path()).len() >= 3);
        assert!(wal_bytes(dir.path()) > 0);
    }
    // Drop the oldest sealed segment, as a reset interrupted after its first
    // deletion would (if it deleted before truncating).
    let oldest = sealed_segments(dir.path()).remove(0);
    std::fs::remove_file(oldest).unwrap();
    let (store, report) = PagedStore::open(dir.path(), segmented_options()).unwrap();
    assert!(report.redo_lsn > 0, "the checkpoint record must be found");
    assert_eq!(
        store.get(&hot).unwrap().as_deref(),
        Some(final_value.as_slice())
    );
}

/// Repeated recovery is idempotent: reopening the same crashed state any
/// number of times yields the same pages and the same answers, including
/// after a torn tail was truncated by the first recovery.
#[test]
fn repeated_recovery_of_one_crash_state_converges() {
    let dir = TempDir::new().unwrap();
    {
        let store = committed_and_checkpointed(dir.path(), 0..200);
        for round in 0..4 {
            let xid = store.begin();
            for index in (round * 10)..(round * 10 + 30) {
                store
                    .put(xid, &key(index), format!("r{round}-{index}").as_bytes())
                    .unwrap();
            }
            store.commit(xid).unwrap();
        }
        let crashed = store.begin();
        store.put(crashed, b"crashed", b"never").unwrap();
        // Crash.
    }
    // A torn tail on top.
    {
        use std::io::Write;
        let mut wal = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("store.wal"))
            .unwrap();
        wal.write_all(b"BWAL\x01\x02\x03").unwrap();
    }
    let read_all = |dir: &Path| -> Vec<Option<Vec<u8>>> {
        let store = open(dir);
        let mut out: Vec<Option<Vec<u8>>> = (0..200)
            .map(|index| store.get(&key(index)).unwrap())
            .collect();
        out.push(store.get(b"crashed").unwrap());
        out
    };
    let first = read_all(dir.path());
    assert_eq!(first[200], None);
    let bytes_after_first = wal_bytes(dir.path());
    for round in 0..5 {
        assert_eq!(
            read_all(dir.path()),
            first,
            "reopen {round} changed the state"
        );
        assert_eq!(wal_bytes(dir.path()), bytes_after_first);
    }
}

/// Second GAP-1 reproducer, found independently on the page-delta branch
/// (`page_deltas.rs::preexisting_mid_transaction_eviction_window`, PR #48)
/// and ported here without the delta machinery. One transaction stays open
/// while 400 others commit, with a 32-page pool (512-byte pages), then the
/// process crashes. The open transaction's pages are evicted under its xid,
/// so recovery skips their images, but committed structure (logged by the
/// other commits) already references pages it allocated. Committed rows read
/// back correctly; the open transaction's keys must read as absent and
/// instead fail with a checksum mismatch on a page that was never written.
///
/// This is the reader-visible failure mode of GAP-1 (windows W1 and W6 of
/// section 14.1): an unreadable page rather than silent row loss.
#[test]
#[ignore = "GAP-1: not closed by the interim; the 32-page pool forces last-resort steals"]
fn an_open_transaction_interleaved_with_commits_reads_absent_after_a_crash() {
    let key = |i: u64| format!("key-{i:05}").into_bytes();
    let value =
        |i: u64, round: u64| format!("v{round}-{}", "x".repeat(((i * 7 + round) % 40) as usize));
    let small = || {
        PagedStoreOptions::default()
            .with_page_size(PAGE_SIZE)
            .with_buffer_pool_bytes(32 * u64::from(PAGE_SIZE))
            .with_fsync(false)
            .with_wal_max_bytes(1 << 30)
    };
    let dir = TempDir::new().unwrap();
    let mut model = std::collections::BTreeMap::new();
    {
        let (store, _) = PagedStore::open(dir.path(), small()).unwrap();
        let open = store.begin();
        for i in 0..400u64 {
            store
                .put(open, &key(20_000 + i), value(i, 1).as_bytes())
                .unwrap();
            let tx = store.begin();
            store.put(tx, &key(i % 90), value(i, i).as_bytes()).unwrap();
            store
                .put(tx, &key(10_000 + i), value(i, 2).as_bytes())
                .unwrap();
            store.commit(tx).unwrap();
            model.insert(key(i % 90), value(i, i));
            model.insert(key(10_000 + i), value(i, 2));
        }
        // Crash with `open` still running.
    }
    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();
    let mut failures = Vec::new();
    for (k, v) in &model {
        match store.get(k) {
            Ok(Some(found)) if found == v.as_bytes() => {}
            other => failures.push(format!(
                "committed {}: {:?}",
                String::from_utf8_lossy(k),
                other.map(|o| o.map(|b| String::from_utf8_lossy(&b).into_owned()))
            )),
        }
    }
    for i in 0..400u64 {
        match store.get(&key(20_000 + i)) {
            Ok(None) => {}
            other => failures.push(format!(
                "uncommitted key-{:05}: {:?}",
                20_000 + i,
                other.map(|o| o.map(|b| String::from_utf8_lossy(&b).into_owned()))
            )),
        }
    }
    let total = failures.len();
    failures.truncate(6);
    assert!(total == 0, "{total} failures, e.g. {failures:#?}");
}
