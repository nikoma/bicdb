use std::sync::Arc;

use bicdb_page::{
    PagedCheckpointCursor, PagedCheckpointLimits, PagedCheckpointPhase, PagedCheckpointStopReason,
    PagedStore, PagedStoreOptions, TailReclaimLimits, WritebackLimits,
};

fn options() -> PagedStoreOptions {
    PagedStoreOptions::default()
        .with_page_size(512)
        .with_buffer_pool_bytes(128 * 512)
        .with_wal_max_bytes(128 * 1024 * 1024)
        .with_fsync(false)
}

fn limits() -> PagedCheckpointLimits {
    PagedCheckpointLimits {
        writeback: WritebackLimits {
            max_candidates: 1,
            max_io_bytes: 1_024,
            max_duration_millis: 10_000,
        },
        max_freeze_xids: 1,
        tail_reclaim: TailReclaimLimits {
            max_page_visits: 64,
            max_io_bytes: 64 * 512,
        },
    }
}

fn insert(store: &PagedStore, index: usize) {
    let xid = store.begin();
    store
        .put(
            xid,
            format!("key-{index:04}").as_bytes(),
            format!("value-{index:04}-{}", "x".repeat(700)).as_bytes(),
        )
        .unwrap();
    store.commit(xid).unwrap();
}

fn finish_checkpoint(
    store: &PagedStore,
    mut cursor: PagedCheckpointCursor,
) -> (PagedCheckpointCursor, usize, usize) {
    let limits = limits();
    let mut steps = 0usize;
    let mut freeze_stops = 0usize;
    for _ in 0..10_000 {
        let report = store.checkpoint_step(cursor, limits).unwrap();
        report.validate(limits, 512).unwrap();
        if let Some(writeback) = report.writeback {
            assert!(writeback.candidates_examined <= 1);
            assert!(writeback.pages_written <= 1);
            assert!(writeback.logical_io_bytes <= 1_024);
        }
        if report.stop_reason == PagedCheckpointStopReason::FreezeLimit {
            freeze_stops += 1;
        }
        steps += 1;
        cursor = report.next_cursor;
        if report.complete {
            assert_eq!(cursor.phase, PagedCheckpointPhase::Complete);
            assert_eq!(report.dirty_pages_remaining, 0);
            return (cursor, steps, freeze_stops);
        }
    }
    panic!("bounded checkpoint did not complete");
}

#[test]
fn checkpoint_drains_and_freezes_in_strictly_bounded_steps() {
    let dir = tempfile::tempdir().unwrap();
    let store = PagedStore::open(dir.path(), options()).unwrap().0;
    for index in 0..12 {
        insert(&store, index);
    }
    let before = store.snapshot().unwrap();
    assert!(before.buffer_pool.dirty_pages > 1);
    assert!(before.wal_bytes > 0);

    let (_, steps, freeze_stops) = finish_checkpoint(&store, PagedCheckpointCursor::default());
    let after = store.snapshot().unwrap();
    assert_eq!(after.buffer_pool.dirty_pages, 0);
    assert_eq!(after.wal_bytes, 0);
    assert!(steps > before.buffer_pool.dirty_pages as usize);
    assert!(freeze_stops >= 10, "one-xid freeze limit was not enforced");
}

#[test]
fn inclusive_phase_resume_survives_process_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cursor = {
        let store = PagedStore::open(dir.path(), options()).unwrap().0;
        for index in 0..20 {
            insert(&store, index);
        }
        let first = store
            .checkpoint_step(PagedCheckpointCursor::default(), limits())
            .unwrap();
        assert!(!first.complete);
        assert_ne!(first.next_cursor, PagedCheckpointCursor::default());
        first.next_cursor
    };

    let store = PagedStore::open(dir.path(), options()).unwrap().0;
    let (_, _, _) = finish_checkpoint(&store, cursor);
    drop(store);

    let reopened = PagedStore::open(dir.path(), options()).unwrap().0;
    let snapshot = reopened.latest_snapshot();
    for index in 0..20 {
        assert!(reopened
            .get_as_of(&snapshot, format!("key-{index:04}").as_bytes())
            .unwrap()
            .is_some());
    }
    assert_eq!(reopened.snapshot().unwrap().wal_bytes, 0);
}

#[test]
fn commits_interleaved_with_online_drain_recover_without_loss() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(PagedStore::open(dir.path(), options()).unwrap().0);
    for index in 0..10 {
        insert(&store, index);
    }
    let writer = {
        let store = Arc::clone(&store);
        std::thread::spawn(move || {
            for index in 10..60 {
                insert(&store, index);
                std::thread::yield_now();
            }
        })
    };

    let mut cursor = PagedCheckpointCursor::default();
    for _ in 0..10_000 {
        let report = store.checkpoint_step(cursor, limits()).unwrap();
        cursor = report.next_cursor;
        if report.complete {
            break;
        }
        std::thread::yield_now();
    }
    writer.join().unwrap();
    // A second operation covers commits that legitimately happened after the
    // first checkpoint's publication boundary.
    finish_checkpoint(&store, PagedCheckpointCursor::default());
    drop(store);

    let reopened = PagedStore::open(dir.path(), options()).unwrap().0;
    let snapshot = reopened.latest_snapshot();
    for index in 0..60 {
        assert!(reopened
            .get_as_of(&snapshot, format!("key-{index:04}").as_bytes())
            .unwrap()
            .is_some());
    }
}

#[test]
fn an_aborted_xid_freezes_as_an_exception_and_no_longer_retains_the_wal() {
    // Before 1.0.85-beta this exact scenario retained the WAL forever:
    // freezing stopped at the aborted xid, the later commit could not freeze,
    // and truncation was refused. The durable abort exception lets the
    // watermark step over it.
    let dir = tempfile::tempdir().unwrap();
    let store = PagedStore::open(dir.path(), options()).unwrap().0;
    let aborted = store.begin();
    store.abort(aborted).unwrap();
    insert(&store, 1);

    let mut cursor = PagedCheckpointCursor::default();
    let mut exceptions_recorded = 0u64;
    let final_report = loop {
        let report = store.checkpoint_step(cursor, limits()).unwrap();
        exceptions_recorded = exceptions_recorded.saturating_add(report.abort_exceptions_recorded);
        cursor = report.next_cursor;
        if report.complete {
            break report;
        }
    };
    let checkpoint = final_report.checkpoint.unwrap();
    assert_eq!(exceptions_recorded, 1);
    assert!(
        checkpoint.wal_truncated,
        "an abort still blocks checkpoint truncation"
    );
    let snapshot = store.snapshot().unwrap();
    assert_eq!(snapshot.abort_exceptions, 1);
    assert_eq!(snapshot.wal_bytes, 0);
    assert!(store.get(b"key-0001").unwrap().is_some());
}

#[test]
fn strict_cursor_limit_and_report_contracts_reject_tampering() {
    let limits = limits();
    let complete_with_cursor = PagedCheckpointCursor {
        phase: PagedCheckpointPhase::Complete,
        writeback: bicdb_page::WritebackCursor { next_page: Some(7) },
        drain_passes: 0,
    };
    assert!(complete_with_cursor.validate().is_err());

    let mut invalid_limits = limits;
    invalid_limits.max_freeze_xids = 0;
    assert!(invalid_limits.validate(512).is_err());

    let dir = tempfile::tempdir().unwrap();
    let store = PagedStore::open(dir.path(), options()).unwrap().0;
    insert(&store, 1);
    let mut report = store
        .checkpoint_step(PagedCheckpointCursor::default(), limits)
        .unwrap();
    report.transactions_frozen = limits.max_freeze_xids + 1;
    assert!(report.validate(limits, 512).is_err());
}

#[test]
fn wal_trigger_advances_one_bounded_phase_instead_of_flushing_the_pool() {
    let dir = tempfile::tempdir().unwrap();
    // A wal_max the commit exceeds but stays within 4x of: the soft-trigger
    // region, where one bounded phase advances per commit. Past 4x the
    // committing thread runs a full checkpoint instead — covered by
    // `sustained_bulk_commits_keep_the_wal_hard_bounded`.
    let automatic = PagedStoreOptions::default()
        .with_page_size(512)
        .with_buffer_pool_bytes(8_192 * 512)
        .with_wal_max_bytes(1024 * 1024)
        .with_fsync(false);
    let store = PagedStore::open(dir.path(), automatic).unwrap().0;
    let xid = store.begin();
    for index in 0..4_000 {
        store
            .put(xid, format!("bulk-{index:05}").as_bytes(), &[b'x'; 400])
            .unwrap();
    }
    store.commit(xid).unwrap();

    let snapshot = store.snapshot().unwrap();
    assert_eq!(snapshot.buffer_pool.writeback_steps, 1);
    assert!(snapshot.buffer_pool.background_writebacks <= 1_024);
    assert!(
        snapshot.buffer_pool.dirty_pages > 0,
        "the WAL trigger regressed to an unbounded all-pool flush"
    );
}

#[test]
fn replaying_finalize_after_lost_schedule_publication_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let store = PagedStore::open(dir.path(), options()).unwrap().0;
    for index in 0..12 {
        insert(&store, index);
    }
    let mut cursor = PagedCheckpointCursor::default();
    let finalize_cursor = loop {
        let before = cursor;
        let report = store.checkpoint_step(cursor, limits()).unwrap();
        cursor = report.next_cursor;
        if before.phase == PagedCheckpointPhase::Finalize && report.complete {
            break before;
        }
    };

    // The checkpoint is durable, but pretend the supervisor process died
    // before atomically publishing the completed cursor.
    let replay = store.checkpoint_step(finalize_cursor, limits()).unwrap();
    assert!(replay.complete);
    assert_eq!(replay.next_cursor.phase, PagedCheckpointPhase::Complete);
    drop(store);

    let reopened = PagedStore::open(dir.path(), options()).unwrap().0;
    let snapshot = reopened.latest_snapshot();
    for index in 0..12 {
        assert!(reopened
            .get_as_of(&snapshot, format!("key-{index:04}").as_bytes())
            .unwrap()
            .is_some());
    }
}
