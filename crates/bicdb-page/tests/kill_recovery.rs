//! Recovery after a real process kill.
//!
//! Gate 3 of the PubMed import plan: "Kill the process during writes/checkpoints
//! and verify recovery."
//!
//! The in-process crash matrix (`crash_matrix.rs`) drops a store without
//! checkpointing, which models losing the buffer pool. It cannot model
//! everything a `SIGKILL` does: a write interrupted mid-syscall, file handles
//! closed by the kernel rather than by us, and no chance to run any cleanup.
//! This spawns a real child process, kills it mid-write, and reopens in the
//! parent.
//!
//! # The contract under test
//!
//! After a kill, reopening must:
//!
//! - return every row whose `commit` had returned before the kill;
//! - return no row from a transaction that had not committed;
//! - succeed with no repair step and no operator intervention.
//!
//! The child announces each committed batch on stdout *after* commit returns, so
//! the parent knows exactly what to demand rather than guessing.
//!
//! # Why a separate binary
//!
//! The child is `kill-writer`, a dedicated binary. An earlier version re-executed
//! this test binary with an env var set — which meant every test in the file
//! spawned another full test binary, each spawning more. An unbounded process
//! explosion. A child that cannot spawn anything is the fix, not a cleverer
//! guard.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use bicdb_page::{PagedStore, PagedStoreOptions};
use tempfile::TempDir;

/// Must match `kill-writer`'s configuration, or the parent would open the store
/// with a different page size and be refused.
fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(4096)
        .with_buffer_pool_bytes(4 * 1024 * 1024)
        .with_fsync(true)
        .with_wal_max_bytes(2 * 1024 * 1024)
}

fn key(index: usize) -> Vec<u8> {
    format!("k{index:07}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("value-{index}-{}", "w".repeat(200)).into_bytes()
}

/// Spawn the writer, wait until it reports at least `target` committed rows,
/// then `SIGKILL` it. Returns the highest committed count the parent saw.
fn kill_writer_after(dir: &str, target: usize) -> usize {
    let mut child: Child = Command::new(env!("CARGO_BIN_EXE_kill-writer"))
        .arg(dir)
        .arg("4096")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn kill-writer");

    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);
    let mut committed = 0usize;
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut line = String::new();

    while committed < target && Instant::now() < deadline {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                if let Some(count) = line.trim().strip_prefix("committed ") {
                    if let Ok(count) = count.parse::<usize>() {
                        committed = count;
                    }
                }
            }
            Err(_) => break,
        }
    }

    // SIGKILL: no unwinding, no destructors, no flush. The real thing.
    let _ = child.kill();
    let _ = child.wait();
    committed
}

#[test]
fn a_sigkill_during_writes_loses_no_committed_row() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    let committed = kill_writer_after(&path, 1_000);
    assert!(
        committed >= 1_000,
        "the writer only committed {committed} rows before the kill; \
         the test did not exercise a meaningful amount of work"
    );

    // Reopen in the parent. No repair step, no intervention.
    let (store, recovery) =
        PagedStore::open(dir.path(), options()).expect("store did not reopen after a SIGKILL");

    for index in 0..committed {
        assert_eq!(
            store.get(&key(index)).unwrap().as_deref(),
            Some(value(index).as_slice()),
            "row {index} was committed before the kill and is now missing \
             (recovery replayed {} pages, discarded {} torn bytes)",
            recovery.pages_replayed,
            recovery.truncated_bytes
        );
    }
}

#[test]
fn a_sigkill_exposes_no_uncommitted_row() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    let committed = kill_writer_after(&path, 600);
    assert!(committed >= 600);

    let (store, _) = PagedStore::open(dir.path(), options()).unwrap();

    // Rows well above the last announced commit belong to a transaction that had
    // not committed. The generous margin covers batches that completed between
    // the child's last print and the kill landing.
    let definitely_uncommitted = committed + 1_000;
    for index in definitely_uncommitted..definitely_uncommitted + 200 {
        assert!(
            store.get(&key(index)).unwrap().is_none(),
            "row {index} was never committed but is visible after recovery"
        );
    }
}

#[test]
fn a_store_survives_repeated_kills() {
    // Each generation writes and is killed; the next must still see everything
    // every previous generation committed.
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    let mut high_water = 0usize;
    for generation in 0..3 {
        let committed = kill_writer_after(&path, 400);
        assert!(
            committed >= 400,
            "generation {generation} committed only {committed} rows"
        );
        high_water = high_water.max(committed);

        let (store, _) = PagedStore::open(dir.path(), options())
            .unwrap_or_else(|error| panic!("generation {generation} failed to reopen: {error}"));
        for index in (0..high_water).step_by(37) {
            assert!(
                store.get(&key(index)).unwrap().is_some(),
                "row {index} lost after kill {generation}"
            );
        }
    }
}
