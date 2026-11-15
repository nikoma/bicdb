//! The read-only profile check must reject corruption with a valid gate magic.
use super::*;

#[test]
fn physical_gate_profile_requires_actual_page_checksum_and_never_repairs_it() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.pages");
    let store = PageStore::open(&path, PageStoreOptions::default().with_fsync(true)).unwrap();
    store.sync().unwrap();
    drop(store);
    let original = std::fs::read(&path).unwrap();
    let gate = crate::physical_gate::PhysicalGate::for_file(&path).unwrap();
    let exclusive = gate.exclusive().unwrap();
    require_physical_gate_profile(&path).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), original);

    // Real file damage preserves header/profile magic and page-size fields.
    // A decode-only check would accept this changed page-count byte.
    let mut damaged = original.clone();
    damaged[PAGE_HEADER_BYTES + 12] ^= 1;
    std::fs::write(&path, &damaged).unwrap();
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .sync_all()
        .unwrap();
    let error = require_physical_gate_profile(&path).unwrap_err();
    assert!(matches!(
        error,
        PageError::ChecksumMismatch {
            page_id: SUPERBLOCK_PAGE_ID,
            ..
        }
    ));
    assert_eq!(std::fs::read(&path).unwrap(), damaged);

    std::fs::write(&path, &original).unwrap();
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .sync_all()
        .unwrap();
    require_physical_gate_profile(&path).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), original);
    drop(exclusive);
    let reopened = PageStore::open(&path, PageStoreOptions::default()).unwrap();
    reopened.sync().unwrap();
}
