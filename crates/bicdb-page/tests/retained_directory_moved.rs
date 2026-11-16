//! A moved-path observation never relaxes the original writer-transfer check.
#![cfg(unix)]
use bicdb_page::{DirectoryLock, PageError, PagedStore, PagedStoreOptions};

#[test]
fn same_live_claim_observes_only_its_moved_inode_and_never_admits_writer_transfer() {
    let root = tempfile::tempdir().unwrap();
    let original = root.path().join("original");
    let moved = root.path().join("moved");
    let wrong = root.path().join("wrong");
    std::fs::create_dir_all(&original).unwrap();
    let claim = DirectoryLock::acquire(&original).unwrap();
    let (wrong_owner, _) = PagedStore::open(
        &wrong,
        PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024),
    )
    .unwrap();
    let xid = wrong_owner.begin();
    wrong_owner
        .put(xid, b"wrong-owner", b"complete real other owner payload")
        .unwrap();
    wrong_owner.commit(xid).unwrap();
    std::fs::rename(&original, &moved).unwrap();
    assert!(claim.require_directory(&original).is_err());
    assert!(
        claim.require_directory(&moved).is_err(),
        "strict original writer-transfer precondition is unchanged"
    );
    claim.require_retained_directory_at(&moved).unwrap();
    assert!(claim.require_retained_directory_at(&wrong).is_err());
    assert!(claim.require_retained_directory_at(&original).is_err());
    // A fresh genuine owner at the old NAME cannot replace the moved claim.
    let (new_old_name, _) = PagedStore::open(
        &original,
        PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024),
    )
    .unwrap();
    assert!(claim.require_retained_directory_at(&original).is_err());
    claim.require_retained_directory_at(&moved).unwrap();
    assert!(matches!(
        PagedStore::open(
            &moved,
            PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024)
        ),
        Err(PageError::AlreadyOpen { .. })
    ));
    assert_eq!(
        wrong_owner.get(b"wrong-owner").unwrap(),
        Some(b"complete real other owner payload".to_vec())
    );
    let xid = new_old_name.begin();
    new_old_name
        .put(
            xid,
            b"new-old-name",
            b"different actual owner remains usable",
        )
        .unwrap();
    new_old_name.commit(xid).unwrap();
    assert_eq!(
        new_old_name.get(b"new-old-name").unwrap(),
        Some(b"different actual owner remains usable".to_vec())
    );
    drop(claim);
    let (actual, _) = PagedStore::open(
        &moved,
        PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024),
    )
    .unwrap();
    let xid = actual.begin();
    actual
        .put(
            xid,
            b"after-original-release",
            b"full payload from new lawful owner",
        )
        .unwrap();
    actual.commit(xid).unwrap();
    assert_eq!(
        actual.get(b"after-original-release").unwrap(),
        Some(b"full payload from new lawful owner".to_vec())
    );
}
