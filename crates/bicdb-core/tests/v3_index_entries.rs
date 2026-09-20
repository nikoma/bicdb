//! v3 ordered-index entries: durable format record, intern-id entry
//! roundtrips, mixed-format decoding, and the escape-boundary edge that the
//! exact scan's terminator filter exists for.

use bicdb_core::{IndexEntryFormat, IndexEntryRef, PagedRecords, PagedRecordsOptions};
use tempfile::TempDir;

fn paged(dir: &TempDir) -> PagedRecords {
    PagedRecords::open(
        dir.path().join("paged"),
        PagedRecordsOptions {
            fsync: false,
            ..Default::default()
        },
    )
    .unwrap()
}

fn refs(paged: &PagedRecords, index: &str, encoded: &[u8]) -> Vec<IndexEntryRef> {
    let snapshot = paged.latest_snapshot();
    paged
        .scan_index_exact_refs(&snapshot, index, encoded)
        .unwrap()
        .map(|entry| entry.map(|(entry_ref, _)| entry_ref))
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn entry_format_record_is_durable_and_defaults_to_v2() {
    let dir = TempDir::new().unwrap();
    {
        let paged = paged(&dir);
        assert_eq!(
            paged.entry_format_cached("idx_a").unwrap(),
            IndexEntryFormat::V2
        );
        let (xid, _) = paged.begin();
        paged
            .set_index_entry_format(xid, "idx_a", "places", IndexEntryFormat::V3)
            .unwrap();
        paged.commit(xid).unwrap();
        assert_eq!(
            paged.entry_format_cached("idx_a").unwrap(),
            IndexEntryFormat::V3
        );
    }
    let paged = paged(&dir);
    assert_eq!(
        paged.entry_format_cached("idx_a").unwrap(),
        IndexEntryFormat::V3
    );
    assert_eq!(
        paged.entry_format_cached("idx_other").unwrap(),
        IndexEntryFormat::V2
    );
}

#[test]
fn v3_entries_roundtrip_and_coexist_with_v2() {
    let dir = TempDir::new().unwrap();
    let paged = paged(&dir);
    let key = [0x41u8, 0x42];

    let (xid, _) = paged.begin();
    let id = paged.intern_or_alloc(xid, "places", "row-a").unwrap();
    paged
        .put_index_entry_intern(xid, "idx", &key, id, b"hint-a")
        .unwrap();
    // A v2 entry under the SAME encoded key (mixed decode must handle both).
    paged
        .put_index_entry(xid, "idx", &key, "row-b", b"hint-b")
        .unwrap();
    paged.commit(xid).unwrap();

    // v2 sorts before v3 (terminator 0x00 0x00 < 0x00 0x01).
    assert_eq!(
        refs(&paged, "idx", &key),
        vec![
            IndexEntryRef::Pk("row-b".to_string()),
            IndexEntryRef::Intern(id)
        ]
    );

    let (xid, _) = paged.begin();
    assert!(paged
        .delete_index_entry_intern(xid, "idx", &key, id)
        .unwrap());
    paged.commit(xid).unwrap();
    assert_eq!(
        refs(&paged, "idx", &key),
        vec![IndexEntryRef::Pk("row-b".to_string())]
    );
}

#[test]
fn exact_scan_stops_at_the_escape_boundary() {
    // An encoded key that EXTENDS the probed key with a NUL byte produces
    // `...0x00 0xFF...` right where the probed key's terminators live. The
    // exact scan must yield only the probed key's entries.
    let dir = TempDir::new().unwrap();
    let paged = paged(&dir);
    let short = [0x41u8];
    let longer = [0x41u8, 0x00, 0x07];

    let (xid, _) = paged.begin();
    let id_short = paged.intern_or_alloc(xid, "places", "short-row").unwrap();
    let id_longer = paged.intern_or_alloc(xid, "places", "longer-row").unwrap();
    paged
        .put_index_entry_intern(xid, "idx", &short, id_short, b"")
        .unwrap();
    paged
        .put_index_entry_intern(xid, "idx", &longer, id_longer, b"")
        .unwrap();
    paged
        .put_index_entry(xid, "idx", &longer, "longer-row-v2", b"")
        .unwrap();
    paged.commit(xid).unwrap();

    assert_eq!(
        refs(&paged, "idx", &short),
        vec![IndexEntryRef::Intern(id_short)]
    );
    assert_eq!(
        refs(&paged, "idx", &longer),
        vec![
            IndexEntryRef::Pk("longer-row-v2".to_string()),
            IndexEntryRef::Intern(id_longer)
        ]
    );

    // The prefix scan sees all three with their encoded keys intact.
    let snapshot = paged.latest_snapshot();
    let all: Vec<(Vec<u8>, IndexEntryRef)> = paged
        .scan_index_encoded_prefix_refs(&snapshot, "idx", &short)
        .unwrap()
        .map(|entry| entry.map(|(encoded, entry_ref, _)| (encoded, entry_ref)))
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(all[0], (short.to_vec(), IndexEntryRef::Intern(id_short)));
    assert_eq!(all[1].0, longer.to_vec());
    assert_eq!(all[2].0, longer.to_vec());
}
