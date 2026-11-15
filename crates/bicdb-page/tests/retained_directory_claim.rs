//! Genuine linear directory-claim transfer. No metadata/owner is fabricated.
#![cfg(any(unix, windows))]

use std::ffi::OsString;
use std::path::Path;

use bicdb_page::{DirectoryLock, PageError, PagedStore, PagedStoreOptions};

fn inventory(path: &Path) -> Vec<OsString> {
    let mut entries = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

#[test]
fn wrong_directory_and_invalid_options_preserve_same_actual_claim_for_open() {
    let original = tempfile::tempdir().unwrap();
    let wrong = tempfile::tempdir().unwrap();
    let mut claim = Some(DirectoryLock::acquire(original.path()).unwrap());
    let wrong_before = inventory(wrong.path());
    let original_before = inventory(original.path());

    let refused = PagedStore::open_with_directory_claim(
        wrong.path(),
        PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024),
        &mut claim,
    )
    .unwrap_err();
    eprintln!("ACTUAL_WRONG_DIRECTORY_REFUSAL {refused:?}");
    assert!(matches!(refused, PageError::Io { source, .. }
        if source.kind() == std::io::ErrorKind::InvalidInput));
    assert_eq!(inventory(wrong.path()), wrong_before);
    assert_eq!(inventory(original.path()), original_before);
    claim
        .as_ref()
        .unwrap()
        .require_directory(original.path())
        .unwrap();
    assert!(matches!(
        PagedStore::open(
            original.path(),
            PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024)
        ),
        Err(PageError::AlreadyOpen { .. })
    ));

    let invalid = PagedStoreOptions::default()
        .with_buffer_pool_bytes(1024 * 1024)
        .with_buffer_pool_bytes(0);
    let refused =
        PagedStore::open_with_directory_claim(original.path(), invalid, &mut claim).unwrap_err();
    assert!(matches!(
        refused,
        PageError::BudgetTooSmall { budget: 0, .. }
    ));
    assert_eq!(inventory(original.path()), original_before);
    claim
        .as_ref()
        .unwrap()
        .require_directory(original.path())
        .unwrap();

    let (store, _) = PagedStore::open_with_directory_claim(
        original.path(),
        PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024),
        &mut claim,
    )
    .unwrap();
    assert!(
        claim.is_none(),
        "unique original claim transferred exactly once"
    );
    let xid = store.begin();
    store
        .put(xid, b"same-claim", b"complete payload after actual refusal")
        .unwrap();
    store.commit(xid).unwrap();
    assert_eq!(
        store.get(b"same-claim").unwrap(),
        Some(b"complete payload after actual refusal".to_vec())
    );
    drop(store);
    let (reopened, _) = PagedStore::open(
        original.path(),
        PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024),
    )
    .unwrap();
    assert_eq!(
        reopened.get(b"same-claim").unwrap(),
        Some(b"complete payload after actual refusal".to_vec())
    );
}

#[test]
fn absent_or_spent_claim_cannot_reopen_or_mutate_the_actual_store() {
    let directory = tempfile::tempdir().unwrap();
    let mut absent = None;
    let before = inventory(directory.path());
    let refused = PagedStore::open_with_directory_claim(
        directory.path(),
        PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024),
        &mut absent,
    )
    .unwrap_err();
    assert!(
        matches!(refused, PageError::InvalidMaintenanceLimits { reason }
        if reason == "actual retained Page directory claim is absent")
    );
    assert_eq!(inventory(directory.path()), before);

    let mut claim = Some(DirectoryLock::acquire(directory.path()).unwrap());
    let (store, _) = PagedStore::open_with_directory_claim(
        directory.path(),
        PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024),
        &mut claim,
    )
    .unwrap();
    let xid = store.begin();
    store
        .put(xid, b"retained", b"actual committed payload")
        .unwrap();
    store.commit(xid).unwrap();
    let issued = store.transactions().next_xid();
    let lsn = store.wal().next_lsn();
    let pages = store.buffer_pool().store().page_count();
    let before = inventory(directory.path());
    let refused = PagedStore::open_with_directory_claim(
        directory.path(),
        PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024),
        &mut claim,
    )
    .unwrap_err();
    assert!(
        matches!(refused, PageError::InvalidMaintenanceLimits { reason }
        if reason == "actual retained Page directory claim is absent")
    );
    assert_eq!(store.transactions().next_xid(), issued);
    assert_eq!(store.wal().next_lsn(), lsn);
    assert_eq!(store.buffer_pool().store().page_count(), pages);
    assert_eq!(inventory(directory.path()), before);
    assert_eq!(
        store.get(b"retained").unwrap(),
        Some(b"actual committed payload".to_vec())
    );
}
