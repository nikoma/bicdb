//! Actual file IO return ordering; these are component controls, not native
//! Open/Serving or quorum qualification.
use super::*;
fn actual_store(root: &Path) -> RangeWriteStore {
    RangeWriteStore::open(
        root,
        ClusterId::new("actual-io-return").unwrap(),
        ClusterNodeId::new("n0").unwrap(),
        true,
    )
    .unwrap()
}
#[test]
fn later_same_byte_directory_sync_error_is_not_a_known_current_io_return() {
    let directory = tempfile::tempdir().unwrap();
    let store = actual_store(directory.path());
    store.persist().unwrap();
    let before = fs::read(directory.path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap();
    let fault = bicdb_page::test_faults::arm_io_failure(
        "actual_catalogue_io_return.dir_sync",
        directory.path(),
        1,
        false,
    );
    let failed = store.persist_with(|path, bytes, fsync| {
        crate::storage::replace_durably(
            path,
            crate::storage::ReplaceContents::Plain(bytes),
            fsync,
            "actual_catalogue_io_return",
            crate::storage::FailClosed::No,
        )
    });
    let after = fs::read(directory.path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap();
    eprintln!("ACTUAL_CATALOGUE_IO_RETURN failed={:?} fired={} same_bytes={} global_uncertain={} generation={} known={}",
        failed.as_ref().err().map(ToString::to_string),fault.fired(),before==after,store.recovery_persistence_uncertain,
        store.current_catalogue_write_generation(),store.catalogue_successful_io_generation.load(std::sync::atomic::Ordering::SeqCst));
    assert!(failed.is_err());
    assert_eq!(fault.fired(), 1);
    assert_eq!(before, after);
    assert!(
        !store.recovery_persistence_uncertain,
        "actual generic writer did not set recovery flag"
    );
    assert!(store
        .require_current_catalogue_io_returned_ok()
        .unwrap_err()
        .to_string()
        .contains("no known successful return"));
    drop(fault);
    store.persist().unwrap();
    store.require_current_catalogue_io_returned_ok().unwrap();
}
#[test]
fn late_original_success_cannot_cover_a_newer_entered_failed_write() {
    let directory = tempfile::tempdir().unwrap();
    let store = actual_store(directory.path());
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    struct Release(Option<std::sync::mpsc::SyncSender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(send) = self.0.take() {
                let _ = send.try_send(());
            }
        }
    }
    std::thread::scope(|scope| {
        let shared = &store;
        let old = scope.spawn(move || {
            shared.persist_with(|path, bytes, fsync| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                crate::storage::write_atomic(path, bytes, fsync)
            })
        });
        let mut release = Release(Some(release_tx));
        entered_rx.recv().unwrap();
        let fault = bicdb_page::test_faults::arm_io_failure(
            "actual_later_catalogue_io.dir_sync",
            directory.path(),
            1,
            false,
        );
        let later = store.persist_with(|path, bytes, fsync| {
            crate::storage::replace_durably(
                path,
                crate::storage::ReplaceContents::Plain(bytes),
                fsync,
                "actual_later_catalogue_io",
                crate::storage::FailClosed::No,
            )
        });
        let failed_generation = store.current_catalogue_write_generation();
        assert!(later.is_err());
        assert_eq!(fault.fired(), 1);
        drop(fault);
        release.0.take().unwrap().send(()).unwrap();
        old.join().unwrap().unwrap();
        assert_eq!(
            store.current_catalogue_write_generation(),
            failed_generation
        );
        assert!(
            store
                .catalogue_successful_io_generation
                .load(std::sync::atomic::Ordering::SeqCst)
                < failed_generation
        );
        assert!(store.require_current_catalogue_io_returned_ok().is_err());
    });
}
