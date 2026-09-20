//! L6: archived-WAL pruning honoring a verified base's WAL floor.

use bicdb_core::{prune_archived_wal, wal_floor_of_base};
use bicdb_page::{PageStore, PageStoreOptions, Wal};

#[test]
fn prune_deletes_below_the_base_floor_and_keeps_the_chain() {
    let archive = tempfile::tempdir().unwrap();
    let base = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(base.path().join("paged")).unwrap();

    let paged = base.path().join("paged");
    let store = PageStore::open(
        paged.join("store.pages"),
        PageStoreOptions::default().with_fsync(false),
    )
    .unwrap();
    let wal = Wal::open_with_segments_and_floor(
        paged.join("store.wal"),
        false,
        1,
        store.checkpoint_lsn(),
    )
    .unwrap();
    for transaction in 1..=5 {
        wal.log_page_image(1, transaction, &[transaction as u8])
            .unwrap();
    }
    wal.seal_active().unwrap();
    let sealed = wal.sealed_segment_paths();
    assert!(sealed.len() >= 3);
    for source in &sealed {
        std::fs::copy(source, archive.path().join(source.file_name().unwrap())).unwrap();
    }
    let floor = wal_floor_of_base(base.path()).unwrap();
    std::fs::write(
        archive.path().join(format!("store.wal.{}", floor + 10)),
        vec![0u8; 100],
    )
    .unwrap();
    std::fs::write(archive.path().join("store.wal.99.partial"), b"staging").unwrap();
    std::fs::write(archive.path().join("unrelated.txt"), b"stranger").unwrap();

    let report = prune_archived_wal(archive.path(), &[base.path().to_path_buf()]).unwrap();
    assert_eq!(report.examined, sealed.len() + 1);
    assert_eq!(report.pruned, sealed.len() - 1);
    assert_eq!(report.kept, 2, "the floor and everything above stay");
    assert!(report.bytes_freed > 0);

    // The chain from the floor forward is intact; strangers untouched.
    for kept in [
        format!("store.wal.{floor}"),
        format!("store.wal.{}", floor + 10),
        "store.wal.99.partial".to_string(),
        "unrelated.txt".to_string(),
    ] {
        assert!(archive.path().join(&kept).exists(), "{kept} must survive");
    }
    assert!(!archive.path().join(sealed[0].file_name().unwrap()).exists());

    // Guardrails: zero floors refuse; baseless dirs refuse.
    assert!(prune_archived_wal(archive.path(), &[]).is_err());
    let empty_base = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(empty_base.path().join("paged")).unwrap();
    assert!(wal_floor_of_base(empty_base.path()).is_err());
}
