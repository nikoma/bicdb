//! Durable FULL-TEXT postings in the page store (Phase 4, second slice).
//!
//! The contract mirrors the B-tree slice: an FTS index's term entries are
//! written in the SAME paged transaction as the rows they describe (one entry
//! per TERM, diffed on update so unchanged terms cost nothing), live in the
//! reserved index keyspace, survive restart and crash with no rebuild, and —
//! the actual point — stop disqualifying their collection from registry mode,
//! so an FTS-indexed corpus reopens with ZERO resident rows.

use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, PagedRecords, PagedRecordsOptions,
    Record, StorageMode,
};
use serde_json::json;
use tempfile::TempDir;

fn paged_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn fts_index() -> IndexDefinition {
    IndexDefinition {
        name: "articles_fts".to_string(),
        collection: "articles".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["terms".to_string()])],
        unique: false,
        kind: IndexKind::FullText,
        predicate: None,
        exclusion: None,
    }
}

fn article(id: &str, terms: &[&str]) -> Record {
    Record::new(id).with_metadata(json!({ "terms": terms, "title": format!("t-{id}") }))
}

#[test]
fn declared_raw_text_fields_are_tokenized_natively_with_positions() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("resources").unwrap();
        db.insert(
            "resources",
            Record::new("r1").with_metadata(json!({
                "title": "Native Carrier Runtime",
                "body": "Search text stays in its declared source fields."
            })),
        )
        .unwrap();
        db.create_index(IndexDefinition {
            name: "resources_search".to_string(),
            collection: "resources".to_string(),
            fields: vec![
                IndexField::MetadataPath(vec!["title".to_string()]),
                IndexField::MetadataPath(vec!["body".to_string()]),
            ],
            unique: false,
            kind: IndexKind::FullText,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
        assert_eq!(
            db.lookup_full_text_term("resources_search", "carrier", false)
                .unwrap(),
            vec!["r1"]
        );
        assert!(db
            .full_text_term_postings("resources_search", "search", false)
            .unwrap()
            .first()
            .is_some_and(|(_, _, payload)| !payload.is_empty()));
        db.close().unwrap();
    }

    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    assert_eq!(
        db.lookup_full_text_term("resources_search", "runtime", false)
            .unwrap(),
        vec!["r1"]
    );
}

/// Durable `(encoded_key, pk)` entries read straight from the CLOSED
/// database's page store — what a future open can trust without any scan.
fn durable_entries(dir: &TempDir, index: &str) -> Vec<(Vec<u8>, String)> {
    let paged = PagedRecords::open(
        dir.path().join("paged"),
        PagedRecordsOptions {
            fsync: false,
            ..Default::default()
        },
    )
    .unwrap();
    let snapshot = paged.latest_snapshot();
    paged
        .scan_index(&snapshot, index)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn resident_rows(db: &BicDb, collection: &str) -> u64 {
    db.residency_report()
        .unwrap()
        .collections
        .iter()
        .find(|entry| entry.name == collection)
        .map(|entry| entry.record_count)
        .unwrap_or(0)
}

#[test]
fn postings_survive_restart_and_serve_lookups_with_zero_resident_rows() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(fts_index()).unwrap();
        db.insert("articles", article("a", &["anxiety", "ashwagandha"]))
            .unwrap();
        db.insert("articles", article("b", &["anxiety", "sleep"]))
            .unwrap();
        db.insert("articles", article("c", &["ashwagandha", "stress"]))
            .unwrap();
        db.close().unwrap();
    }

    // Postings are in the durable keyspace of the CLOSED database.
    let entries = durable_entries(&dir, "articles_fts");
    assert_eq!(entries.len(), 6, "3 rows x 2 terms each");

    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    assert_eq!(
        resident_rows(&db, "articles"),
        0,
        "an FTS-indexed collection must reopen in registry mode (no resident \
         rows), not as materialized stubs"
    );
    assert_eq!(
        db.lookup_full_text_term("articles_fts", "anxiety", false)
            .unwrap(),
        vec!["a".to_string(), "b".to_string()]
    );
    assert_eq!(
        db.lookup_full_text_term("articles_fts", "ashwagandha", false)
            .unwrap(),
        vec!["a".to_string(), "c".to_string()]
    );
    // Prefix lookups walk the same loaded store.
    assert_eq!(
        db.lookup_full_text_term("articles_fts", "s", true).unwrap(),
        vec!["b".to_string(), "c".to_string()]
    );
    // And the loaded store agrees with a from-rows recompute (the shadow
    // check: durable postings vs page-store scan).
    let report = db.verify_index("articles_fts").unwrap();
    assert!(report.valid, "verify diverged: {report:?}");
}

#[test]
fn direct_primary_key_fts_reader_can_skip_the_startup_rowid_registry() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(fts_index()).unwrap();
        db.create_index(IndexDefinition {
            name: "articles_pkey".to_string(),
            collection: "articles".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["document_id".to_string()])],
            unique: true,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
        for index in 0..2_000 {
            let id = format!("article-{index:05}");
            db.insert(
                "articles",
                Record::new(&id).with_metadata(json!({
                    "document_id": id,
                    "terms": ["zebrafish", "crispr"]
                })),
            )
            .unwrap();
        }
        db.close().unwrap();
    }

    let db = BicDb::open_with_config(dir.path(), paged_config().with_paged_rowid_registry(false))
        .unwrap();
    let primary_keys = db
        .lookup_full_text_term("articles_fts", "zebrafish", false)
        .unwrap()
        .into_iter()
        .take(10)
        .collect::<Vec<_>>();
    let records = db.get_records_by_pks("articles", &primary_keys).unwrap();
    assert_eq!(records.len(), 10);
    assert!(records.iter().all(Option::is_some));
    assert_eq!(resident_rows(&db, "articles"), 0);
}

#[test]
fn updates_diff_terms_and_deletes_remove_them() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(fts_index()).unwrap();
        db.insert("articles", article("a", &["anxiety", "ashwagandha"]))
            .unwrap();
        db.insert("articles", article("b", &["sleep"])).unwrap();
        // Update: "anxiety" stays, "ashwagandha" -> "stress".
        db.insert("articles", article("a", &["anxiety", "stress"]))
            .unwrap();
        // Delete b entirely.
        assert!(db.delete("articles", "b").unwrap());
        db.close().unwrap();
    }

    let entries = durable_entries(&dir, "articles_fts");
    assert_eq!(
        entries.len(),
        2,
        "expected exactly a's two current terms, got {entries:?}"
    );
    assert!(entries.iter().all(|(_, pk)| pk == "a"));

    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    assert_eq!(
        db.lookup_full_text_term("articles_fts", "stress", false)
            .unwrap(),
        vec!["a".to_string()]
    );
    assert!(db
        .lookup_full_text_term("articles_fts", "ashwagandha", false)
        .unwrap()
        .is_empty());
    assert!(db
        .lookup_full_text_term("articles_fts", "sleep", false)
        .unwrap()
        .is_empty());
    assert!(db.verify_index("articles_fts").unwrap().valid);
}

#[test]
fn postings_survive_a_crash_shaped_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(fts_index()).unwrap();
        for index in 0..50 {
            db.insert(
                "articles",
                article(&format!("r{index:03}"), &["common", "anxiety"]),
            )
            .unwrap();
        }
        // Drop WITHOUT close: recovery must trust row and postings together
        // out of the page WAL.
    }
    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    assert_eq!(
        db.lookup_full_text_term("articles_fts", "common", false)
            .unwrap()
            .len(),
        50
    );
    assert!(db.verify_index("articles_fts").unwrap().valid);
}

#[test]
fn create_index_backfills_rows_that_predate_it() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.insert("articles", article("old-1", &["anxiety"]))
            .unwrap();
        db.insert("articles", article("old-2", &["stress", "anxiety"]))
            .unwrap();
        // The index arrives AFTER the rows.
        db.create_index(fts_index()).unwrap();
        db.insert("articles", article("new-1", &["anxiety"]))
            .unwrap();
        db.close().unwrap();
    }
    let entries = durable_entries(&dir, "articles_fts");
    assert_eq!(
        entries.len(),
        4,
        "backfill missed pre-index rows: {entries:?}"
    );

    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    assert_eq!(
        db.lookup_full_text_term("articles_fts", "anxiety", false)
            .unwrap(),
        vec![
            "new-1".to_string(),
            "old-1".to_string(),
            "old-2".to_string()
        ]
    );
    assert!(db.verify_index("articles_fts").unwrap().valid);
}

#[test]
fn a_database_without_durable_postings_rebuilds_from_paged_rows() {
    // Upgrade path: a DB indexed before durable FTS entries existed loads with
    // an empty durable keyspace. The fallback must rebuild from the PAGE
    // STORE's rows — resident shards hold nothing in registry mode, and a
    // rebuild from them would be a silently empty index.
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(fts_index()).unwrap();
        db.insert("articles", article("a", &["anxiety"])).unwrap();
        db.insert("articles", article("b", &["anxiety", "sleep"]))
            .unwrap();
        db.close().unwrap();
    }
    // Simulate the pre-upgrade database by dropping the durable entries.
    {
        let paged = PagedRecords::open(
            dir.path().join("paged"),
            PagedRecordsOptions {
                fsync: false,
                ..Default::default()
            },
        )
        .unwrap();
        let snapshot = paged.latest_snapshot();
        let entries: Vec<(Vec<u8>, String)> = paged
            .scan_index(&snapshot, "articles_fts")
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(!entries.is_empty());
        let (xid, _) = paged.begin();
        for (key, pk) in &entries {
            paged
                .delete_index_entry(xid, "articles_fts", key, pk)
                .unwrap();
        }
        paged.commit(xid).unwrap();
        paged.checkpoint().unwrap();
    }

    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    assert_eq!(
        db.lookup_full_text_term("articles_fts", "anxiety", false)
            .unwrap(),
        vec!["a".to_string(), "b".to_string()],
        "the no-durable-entries fallback rebuilt from empty resident shards"
    );
}

/// Shadow test: the same operation sequence drives an EMBEDDED database (the
/// reference implementation — resident index, rebuilt from full records) and
/// a PAGED database (durable postings, loaded from the reserved keyspace
/// after reopen). Any divergence in term lookups is a paged-postings bug.
/// The sequence is deterministic (seeded LCG) so failures reproduce.
#[test]
fn paged_postings_shadow_the_embedded_index_under_churn() {
    let vocabulary = [
        "anxiety",
        "ashwagandha",
        "sleep",
        "stress",
        "yoga",
        "valerian",
        "chamomile",
        "calm",
    ];
    let mut lcg: u64 = 0x5eed_cafe;
    let mut next = move || {
        lcg = lcg
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (lcg >> 33) as usize
    };

    let embedded_dir = TempDir::new().unwrap();
    let paged_dir = TempDir::new().unwrap();
    let embedded_config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::EmbeddedMemory);
    {
        let mut embedded =
            BicDb::open_with_config(embedded_dir.path(), embedded_config.clone()).unwrap();
        let mut paged = BicDb::open_with_config(paged_dir.path(), paged_config()).unwrap();
        for db in [&mut embedded, &mut paged] {
            db.create_collection("articles").unwrap();
            db.create_index(fts_index()).unwrap();
        }
        for _ in 0..400 {
            let id = format!("r{:02}", next() % 40);
            let op = next() % 10;
            if op < 7 {
                let count = 1 + next() % 3;
                let terms: Vec<&str> = (0..count)
                    .map(|_| vocabulary[next() % vocabulary.len()])
                    .collect();
                let record = article(&id, &terms);
                embedded.insert("articles", record.clone()).unwrap();
                paged.insert("articles", record).unwrap();
            } else {
                let embedded_removed = embedded.delete("articles", &id).unwrap();
                let paged_removed = paged.delete("articles", &id).unwrap();
                assert_eq!(embedded_removed, paged_removed, "delete diverged on {id}");
            }
        }
        embedded.close().unwrap();
        paged.close().unwrap();
    }

    // Reopen BOTH: embedded rebuilds its index from records, paged loads
    // durable postings. The lookups must agree exactly.
    let embedded = BicDb::open_with_config(embedded_dir.path(), embedded_config).unwrap();
    let paged = BicDb::open_with_config(paged_dir.path(), paged_config()).unwrap();
    for term in vocabulary {
        assert_eq!(
            embedded
                .lookup_full_text_term("articles_fts", term, false)
                .unwrap(),
            paged
                .lookup_full_text_term("articles_fts", term, false)
                .unwrap(),
            "postings diverged from the embedded reference for `{term}`"
        );
    }
    // Prefix shape too (walks every key of the loaded store).
    for prefix in ["a", "s", "c", "v", "y"] {
        assert_eq!(
            embedded
                .lookup_full_text_term("articles_fts", prefix, true)
                .unwrap(),
            paged
                .lookup_full_text_term("articles_fts", prefix, true)
                .unwrap(),
            "prefix postings diverged for `{prefix}`"
        );
    }
    assert!(paged.verify_index("articles_fts").unwrap().valid);
}

/// The point of read-through postings: after reopen the index keeps ZERO
/// resident posting bytes — lookups are bounded range scans over the durable
/// keyspace — yet answers exactly as before, and same-session commits are
/// immediately visible through the read-through path.
#[test]
fn read_through_postings_keep_zero_resident_bytes() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.create_index(fts_index()).unwrap();
        for index in 0..500 {
            db.insert(
                "articles",
                article(
                    &format!("r{index:04}"),
                    &["anxiety", "ashwagandha", "sleep"],
                ),
            )
            .unwrap();
        }
        db.close().unwrap();
    }
    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();

    let report = db.residency_report().unwrap();
    let index_bytes: u64 = report
        .indexes
        .iter()
        .filter(|entry| entry.name == "articles_fts")
        .map(|entry| entry.store_bytes)
        .sum();
    assert_eq!(
        index_bytes, 0,
        "read-through FTS index is holding resident posting bytes"
    );
    assert_eq!(
        db.lookup_full_text_term("articles_fts", "anxiety", false)
            .unwrap()
            .len(),
        500
    );

    // Same-session write: committed durable entries must be visible to the
    // read-through lookup with no resident mirror involved.
    db.insert("articles", article("fresh", &["chamomile"]))
        .unwrap();
    assert_eq!(
        db.lookup_full_text_term("articles_fts", "chamomile", false)
            .unwrap(),
        vec!["fresh".to_string()]
    );
    assert!(db.delete("articles", "fresh").unwrap());
    assert!(db
        .lookup_full_text_term("articles_fts", "chamomile", false)
        .unwrap()
        .is_empty());
    assert!(db.verify_index("articles_fts").unwrap().valid);
}

/// The shadow oracle survives interleaved folds: embedded reference vs paged
/// with a compact every ~100 operations.
#[test]
fn churn_with_periodic_folds_shadows_the_embedded_oracle() {
    let vocabulary = ["anxiety", "ashwagandha", "sleep", "stress", "yoga", "calm"];
    let mut lcg: u64 = 0xf01d;
    let mut next = move || {
        lcg = lcg
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (lcg >> 33) as usize
    };
    let embedded_dir = TempDir::new().unwrap();
    let paged_dir = TempDir::new().unwrap();
    let mut embedded = BicDb::open_with_config(
        embedded_dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::EmbeddedMemory),
    )
    .unwrap();
    {
        // Paged side needs a reopen for read-through before folding works.
        let mut paged = BicDb::open_with_config(paged_dir.path(), paged_config()).unwrap();
        paged.create_collection("articles").unwrap();
        paged.create_index(fts_index()).unwrap();
        paged.close().unwrap();
    }
    let mut paged = BicDb::open_with_config(paged_dir.path(), paged_config()).unwrap();
    embedded.create_collection("articles").unwrap();
    embedded.create_index(fts_index()).unwrap();

    for step in 0..600 {
        let id = format!("r{:02}", next() % 50);
        if next() % 10 < 7 {
            let count = 1 + next() % 3;
            let terms: Vec<&str> = (0..count)
                .map(|_| vocabulary[next() % vocabulary.len()])
                .collect();
            let record = article(&id, &terms);
            embedded.insert("articles", record.clone()).unwrap();
            paged.insert("articles", record).unwrap();
        } else {
            assert_eq!(
                embedded.delete("articles", &id).unwrap(),
                paged.delete("articles", &id).unwrap(),
                "delete diverged at step {step}"
            );
        }
        if step % 100 == 99 {
            paged.compact_full_text_index("articles_fts").unwrap();
        }
    }
    for term in vocabulary {
        assert_eq!(
            embedded
                .lookup_full_text_term("articles_fts", term, false)
                .unwrap(),
            paged
                .lookup_full_text_term("articles_fts", term, false)
                .unwrap(),
            "postings diverged for `{term}` after folded churn"
        );
    }
    assert!(paged.verify_index("articles_fts").unwrap().valid);
}

/// Legacy array projections carry no positions, so blocks cannot represent
/// them: a backfill over such rows must fall back to per-posting entries
/// (and stay correct) instead of direct-building blocks.
#[test]
fn legacy_array_projection_backfill_stays_per_posting() {
    let dir = TempDir::new().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    db.create_collection("articles").unwrap();
    db.insert("articles", article("a", &["anxiety", "ashwagandha"]))
        .unwrap();
    db.insert("articles", article("b", &["anxiety", "sleep"]))
        .unwrap();
    // Rows exist BEFORE the index: the direct-build trigger. The array
    // projection must bounce it back to the per-posting path.
    db.create_index(fts_index()).unwrap();
    let (tail, blocks, impact_blocks, sentinel) =
        db.full_text_index_layout("articles_fts").unwrap();
    assert!(tail > 0, "legacy projection must keep per-posting entries");
    assert_eq!(blocks, 0, "no blocks may survive the legacy unwind");
    assert_eq!(impact_blocks, 0);
    assert!(sentinel, "per-posting backfill still plants the sentinel");
    db.close().unwrap();

    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    let mut anxiety = db
        .lookup_full_text_term("articles_fts", "anxiety", false)
        .unwrap();
    anxiety.sort();
    assert_eq!(anxiety, vec!["a".to_string(), "b".to_string()]);
    let ash = db
        .lookup_full_text_term("articles_fts", "ashwagandha", false)
        .unwrap();
    assert_eq!(ash, vec!["a".to_string()]);
}

/// Object-projection record: the modern positional shape, which is what the
/// SQL layer materializes and what authorizes a direct-to-blocks build.
fn projected(id: &str, lex: &[(&str, &[u16])]) -> Record {
    let mut map = serde_json::Map::new();
    let mut len = 0usize;
    for (term, positions) in lex {
        len += positions.len();
        map.insert(
            (*term).to_string(),
            serde_json::Value::Array(positions.iter().map(|p| serde_json::json!(*p)).collect()),
        );
    }
    Record::new(id).with_metadata(json!({
        "terms": { "v": 1, "len": len, "distinct": lex.len(), "lex": map }
    }))
}

/// Direct-built (blocks-only) index: exact AND prefix term lookups must be
/// served from the blocks — there is no v1 tail at all.
#[test]
fn direct_built_index_serves_exact_and_prefix_lookups_from_blocks() {
    let dir = TempDir::new().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    db.create_collection("articles").unwrap();
    db.insert(
        "articles",
        projected("a", &[("anxiety", &[1]), ("ashwagandha", &[2, 5])]),
    )
    .unwrap();
    db.insert(
        "articles",
        projected("b", &[("anxiety", &[1]), ("sleep", &[2])]),
    )
    .unwrap();
    db.insert(
        "articles",
        projected("c", &[("ashwagandha", &[1]), ("stress", &[2])]),
    )
    .unwrap();
    db.create_index(fts_index()).unwrap();
    let (tail, blocks, _, sentinel) = db.full_text_index_layout("articles_fts").unwrap();
    assert_eq!(tail, 0, "direct build must not write per-posting entries");
    assert!(blocks > 0);
    assert!(sentinel);
    db.close().unwrap();

    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    let mut exact = db
        .lookup_full_text_term("articles_fts", "ashwagandha", false)
        .unwrap();
    exact.sort();
    assert_eq!(exact, vec!["a".to_string(), "c".to_string()]);
    let mut prefixed = db
        .lookup_full_text_term("articles_fts", "as", true)
        .unwrap();
    prefixed.sort();
    assert_eq!(
        prefixed,
        vec!["a".to_string(), "c".to_string()],
        "prefix over blocks"
    );
    let mut postings = db
        .full_text_term_postings("articles_fts", "an", true)
        .unwrap();
    postings.sort_by(|l, r| l.1.cmp(&r.1));
    assert_eq!(postings.len(), 2, "prefix postings over blocks");
    assert!(postings
        .iter()
        .all(|(term, _, payload)| term == "anxiety" && !payload.is_empty()));
}

/// Crash window: a direct build that died before its completeness sentinel
/// must NOT serve read-through from the partial blocks, and must not answer
/// "no matches" either. There is no safe generation to serve, so lookups
/// refuse until the index is rebuilt.
#[test]
fn an_aborted_direct_build_refuses_lookups_rather_than_answering_empty() {
    // The abort knob matches by index name; a unique name keeps parallel
    // sibling tests (which create `articles_fts`) out of the blast radius.
    let abort_index = || IndexDefinition {
        name: "articles_fts_abort".to_string(),
        ..fts_index()
    };
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        for index in 0..200 {
            db.insert(
                "articles",
                projected(
                    &format!("r{index:04}"),
                    &[("anxiety", &[1u16][..]), ("ashwagandha", &[2u16, 5][..])],
                ),
            )
            .unwrap();
        }
        std::env::set_var("BICDB_FTS_BACKFILL_CHUNK_ROWS", "64");
        std::env::set_var("BICDB_TEST_FTS_BACKFILL_ABORT", "articles_fts_abort");
        let error = db.create_index(abort_index()).unwrap_err();
        std::env::remove_var("BICDB_TEST_FTS_BACKFILL_ABORT");
        std::env::remove_var("BICDB_FTS_BACKFILL_CHUNK_ROWS");
        assert!(format!("{error}").contains("injected backfill abort"));
        db.close().unwrap();
    }

    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    let (_, blocks, _, sentinel) = db.full_text_index_layout("articles_fts_abort").unwrap();
    if std::env::var("BICDB_FTS_PACKED_SEGMENTS").as_deref() != Ok("0") {
        // Packed segments are STRICTLY safer here: the pk merge streamed into
        // unpublished segment files, so an aborted build leaves nothing
        // visible at all — where the keyed format left committed partial
        // blocks that the fallback then had to ignore.
        assert_eq!(blocks, 0, "an aborted packed build must publish nothing");
    } else {
        assert!(blocks > 0, "the abort landed after a committed flush");
    }
    assert!(!sentinel);
    // An index whose FIRST build was interrupted has no safe generation to
    // serve: the resident store is empty and, under packed segments, nothing
    // was published to read through either. It used to answer every query
    // with zero rows, which a caller cannot tell apart from "no documents
    // matched" -- a silently broken search index. It must refuse instead.
    let error = db
        .lookup_full_text_term("articles_fts_abort", "ashwagandha", false)
        .expect_err("an index with no completed build must refuse lookups");
    assert!(
        error.to_string().contains("no completed build"),
        "the refusal must say why, got: {error}"
    );
    let postings = db
        .full_text_term_postings("articles_fts_abort", "anxiety", false)
        .err()
        .expect("postings must refuse too");
    assert!(postings.to_string().contains("no completed build"));
}

/// Multi-chunk direct build: blocks appended across chunks must stay
/// disjoint and pk-ascending per term. Point probes (the ranked-AND driver)
/// must hit every pk exactly, and the merged posting list must have no
/// duplicates and no holes.
#[test]
fn multi_chunk_direct_build_probes_every_pk_without_duplicates() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        for index in 0..200 {
            db.insert(
                "articles",
                projected(
                    &format!("r{index:04}"),
                    &[("anxiety", &[1u16][..]), ("ashwagandha", &[2u16, 5][..])],
                ),
            )
            .unwrap();
        }
        std::env::set_var("BICDB_FTS_BACKFILL_CHUNK_ROWS", "64");
        db.create_index(fts_index()).unwrap();
        std::env::remove_var("BICDB_FTS_BACKFILL_CHUNK_ROWS");
        db.close().unwrap();
    }
    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    let (tail, blocks, impact_blocks, sentinel) =
        db.full_text_index_layout("articles_fts").unwrap();
    assert_eq!(tail, 0);
    assert!(blocks >= 2, "200 postings across chunk 64 must span blocks");
    assert!(impact_blocks > 0);
    assert!(sentinel);

    let postings = db
        .full_text_term_postings("articles_fts", "ashwagandha", false)
        .unwrap();
    let mut pks: Vec<&String> = postings.iter().map(|(_, pk, _)| pk).collect();
    pks.sort();
    pks.dedup();
    assert_eq!(postings.len(), 200, "duplicate or missing block postings");
    assert_eq!(pks.len(), 200);

    for index in 0..200 {
        let pk = format!("r{index:04}");
        let hit = db
            .full_text_posting_probe("articles_fts", "ashwagandha", &pk)
            .unwrap();
        assert!(hit.is_some(), "probe missed pk {pk}");
    }
    assert!(db
        .full_text_posting_probe("articles_fts", "ashwagandha", "zzzz")
        .unwrap()
        .is_none());
}

/// The importer's resume barrier: rows committed before
/// `checkpoint_for_resume` must survive a reopen WITHOUT a clean close,
/// and the paged WAL must be bounded (the whole point vs close+reopen).
#[test]
fn checkpoint_for_resume_is_a_durable_barrier() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("articles").unwrap();
        for index in 0..500 {
            db.insert("articles", article(&format!("r{index:04}"), &["anxiety"]))
                .unwrap();
        }
        db.checkpoint_for_resume().unwrap();
        let wal = std::fs::metadata(dir.path().join("paged").join("store.wal"))
            .map(|meta| meta.len())
            .unwrap_or(0);
        assert!(
            wal < 64 * 1024,
            "paged WAL not bounded by the barrier: {wal}"
        );
        // Crash: drop without close.
    }
    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    let mut count = 0usize;
    for index in 0..500 {
        if db
            .get("articles", &format!("r{index:04}"))
            .unwrap()
            .is_some()
        {
            count += 1;
        }
    }
    assert_eq!(count, 500, "rows lost across the resume barrier");
}
