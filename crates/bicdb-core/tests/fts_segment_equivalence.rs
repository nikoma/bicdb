//! THE GATE: packed segments must be indistinguishable from the keyed format.
//!
//! Physical-format v2 stores the same encoded blocks in packed files instead
//! of 18 million B-tree rows. Because only WHERE the bytes live changes, a v2
//! index must reproduce v1 **exactly** — same hits, same scores, same block
//! streams, same statistics — before any cleverness is permitted
//! (`memory: fts-packed-segments-v2-directive`).
//!
//! Every test here builds the same corpus twice, once per format, and
//! compares the full read surface.

use bicdb_core::{
    BicDb, Bm25Parameters, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode,
};
use serde_json::json;

const INDEX: &str = "articles_fts";

/// Zipf-ish multilingual vocabulary with a term that spans MANY blocks
/// (`the` appears in most documents; FTS_BLOCK_DOC_CAP is 128, so at 3,000
/// documents its posting list is ~20 blocks) and a long tail of df=1 terms.
const WORDS: &[&str] = &[
    "the",
    "and",
    "for",
    "with",
    "search",
    "index",
    "content",
    "service",
    "provenance",
    "distributed",
    "café",
    "müller",
    "ของบริษัท",
    "xenolith",
];

fn config(packed: bool) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
        .with_fts_packed_segments(packed)
}

fn body(seed: u64, words: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).max(1);
    let mut text = String::with_capacity(words * 8);
    for position in 0..words {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let span = WORDS.len() * (WORDS.len() + 1) / 2;
        let pick = ((state >> 33) as usize) % span;
        let mut acc = 0usize;
        let mut word = WORDS.len() - 1;
        for rank in 0..WORDS.len() {
            acc += WORDS.len() - rank;
            if pick < acc {
                word = rank;
                break;
            }
        }
        text.push_str(WORDS[word]);
        text.push(' ');
        // A per-document unique term, so the dictionary has a df=1 tail like
        // real web text.
        if position == 0 {
            text.push_str(&format!("uniq{seed:06} "));
        }
    }
    text
}

fn build(dir: &std::path::Path, packed: bool, rows: usize) -> BicDb {
    let mut db = BicDb::open_with_config(dir, config(packed)).unwrap();
    db.create_collection("articles").unwrap();
    db.bulk_load_insert(
        "articles",
        (0..rows)
            .map(|index| {
                Record::new(format!("doc-{index:06}"))
                    .with_metadata(json!({ "body": body(index as u64, 120) }))
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    db.create_index(IndexDefinition {
        name: INDEX.into(),
        collection: "articles".into(),
        fields: vec![IndexField::MetadataPath(vec!["body".into()])],
        kind: IndexKind::FullText,
        unique: false,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    db
}

/// Every ranked answer, as an exactly comparable fingerprint (pk + score
/// bits — bit-exact, not approximately equal).
fn ranked_fingerprint(db: &BicDb) -> Vec<(String, Vec<(String, u32)>)> {
    let mut out = Vec::new();
    let queries: Vec<Vec<&str>> = vec![
        vec!["the"],
        vec!["xenolith"],
        vec!["the", "and"],
        vec!["the", "xenolith"],
        vec!["search", "index"],
        vec!["café", "müller"],
        vec!["ของบริษัท"],
        vec!["the", "and", "for", "with"],
    ];
    for terms in queries {
        for keep in [5usize, 50, 500] {
            let hits = db
                .full_text_bm25_top_k(INDEX, &terms, Bm25Parameters::default(), keep, true)
                .unwrap()
                .unwrap_or_default();
            out.push((
                format!("{}@{keep}", terms.join("+")),
                hits.into_iter()
                    .map(|hit| (hit.primary_key, hit.score.to_bits()))
                    .collect(),
            ));
        }
    }
    out
}

/// The core equivalence: fresh builds answer identically.
#[test]
fn packed_and_keyed_builds_answer_identically() {
    let legacy_dir = tempfile::tempdir().unwrap();
    let packed_dir = tempfile::tempdir().unwrap();
    let legacy = build(legacy_dir.path(), false, 3_000);
    let packed = build(packed_dir.path(), true, 3_000);

    let legacy_prints = ranked_fingerprint(&legacy);
    let packed_prints = ranked_fingerprint(&packed);
    assert_eq!(legacy_prints.len(), packed_prints.len());
    for (legacy_q, packed_q) in legacy_prints.iter().zip(packed_prints.iter()) {
        assert_eq!(legacy_q.0, packed_q.0);
        assert_eq!(
            legacy_q.1, packed_q.1,
            "query `{}` answers differently under packed segments",
            legacy_q.0
        );
    }
    // And there is real work being compared: the broad query must rank many
    // documents, not zero.
    assert!(
        legacy_prints
            .iter()
            .any(|(query, hits)| query.starts_with("the@500") && hits.len() >= 400),
        "the corpus produced too few hits for the gate to mean anything"
    );
}

#[test]
fn impact_ordered_bm25_matches_document_order_for_single_and_multiple_terms() {
    let directory = tempfile::tempdir().unwrap();
    let db = build(directory.path(), true, 3_000);
    let session = db.full_text_read_session(INDEX).unwrap();

    for terms in [vec!["the"], vec!["xenolith"], vec!["the", "and"]] {
        for keep in [5usize, 50, 500] {
            let impact = session
                .impact_ordered_bm25_and_top_k(&terms, Bm25Parameters::default(), keep, None)
                .unwrap()
                .unwrap();
            let document_order = session
                .block_max_bm25_and_top_k(&terms, Bm25Parameters::default(), keep, None)
                .unwrap()
                .unwrap();
            let fingerprint = |hits: Vec<bicdb_core::FullTextRankedPosting>| {
                hits.into_iter()
                    .map(|hit| (hit.primary_key, hit.score.to_bits()))
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                fingerprint(impact),
                fingerprint(document_order),
                "impact order changed `{}`@{keep}",
                terms.join("+")
            );
        }
    }
}

#[test]
fn packed_dictionary_scan_is_bounded_and_resumable() {
    let directory = tempfile::tempdir().unwrap();
    let db = build(directory.path(), true, 3_000);
    let session = db.full_text_read_session(INDEX).unwrap();

    let first = session.sealed_term_dictionary_page(None, 3).unwrap();
    assert_eq!(first.len(), 3);
    assert!(first.windows(2).all(|pair| pair[0].term < pair[1].term));
    assert!(first
        .iter()
        .all(|entry| entry.statistics.document_frequency > 0));

    let second = session
        .sealed_term_dictionary_page(first.last().map(|entry| entry.term.as_str()), 3)
        .unwrap();
    assert!(!second.is_empty());
    assert!(second[0].term > first[2].term);
}

#[test]
fn single_term_bm25_does_not_read_the_impact_sidecar() {
    let directory = tempfile::tempdir().unwrap();
    let db = build(directory.path(), true, 3_000);
    let segment_dir = std::fs::read_dir(directory.path().join("paged/fts-segments"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let impacts = segment_dir.join("impacts.dat");
    assert!(std::fs::metadata(&impacts).unwrap().len() > 1);

    // Corrupt only the impact sidecar after the sealed reader is open. The
    // exact single-term route uses document-order posting blocks, so this
    // query must remain independent of that multi-term acceleration data.
    std::fs::write(&impacts, [0_u8]).unwrap();
    let hits = db
        .full_text_bm25_top_k(INDEX, &["the"], Bm25Parameters::default(), 10, true)
        .unwrap()
        .unwrap();
    assert_eq!(hits.len(), 10);
}

#[test]
fn multi_term_bm25_prefers_document_order_over_the_impact_sidecar() {
    let directory = tempfile::tempdir().unwrap();
    let db = build(directory.path(), true, 3_000);
    let segment_dir = std::fs::read_dir(directory.path().join("paged/fts-segments"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let impacts = segment_dir.join("impacts.dat");
    assert!(std::fs::metadata(&impacts).unwrap().len() > 1);

    // Multi-term conjunctions use document-order Block-Max WAND first. A
    // damaged optional impact sidecar must not divert or break that route.
    std::fs::write(&impacts, [0_u8]).unwrap();
    let hits = db
        .full_text_bm25_top_k(INDEX, &["the", "and"], Bm25Parameters::default(), 10, true)
        .unwrap()
        .unwrap();
    assert_eq!(hits.len(), 10);
}

/// Post-build writes land in the transactional tail either way. Insert,
/// update and delete after the build; both formats must stay identical.
#[test]
fn post_build_writes_behave_identically() {
    let legacy_dir = tempfile::tempdir().unwrap();
    let packed_dir = tempfile::tempdir().unwrap();
    let mut legacy = build(legacy_dir.path(), false, 1_200);
    let mut packed = build(packed_dir.path(), true, 1_200);

    for db in [&mut legacy, &mut packed] {
        // New document containing an indexed rare term plus a new term.
        db.insert(
            "articles",
            Record::new("late-1").with_metadata(json!({
                "body": "xenolith freshterm the and content"
            })),
        )
        .unwrap();
        // Update an existing document to REMOVE `xenolith` occurrences it
        // may have had and add a marker.
        db.insert(
            "articles",
            Record::new("doc-000007").with_metadata(json!({
                "body": "updated marker body the and"
            })),
        )
        .unwrap();
        // Delete one document outright.
        db.delete("articles", "doc-000011").unwrap();
    }

    let legacy_prints = ranked_fingerprint(&legacy);
    let packed_prints = ranked_fingerprint(&packed);
    assert_eq!(legacy_prints, packed_prints, "post-build writes diverged");

    // The fresh term is findable in both.
    for db in [&legacy, &packed] {
        let hits = db
            .full_text_bm25_top_k(INDEX, &["freshterm"], Bm25Parameters::default(), 5, true)
            .unwrap()
            .unwrap_or_default();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].primary_key, "late-1");
    }
}

/// Term statistics — the planner's food — must match term by term.
#[test]
fn term_statistics_match() {
    let legacy_dir = tempfile::tempdir().unwrap();
    let packed_dir = tempfile::tempdir().unwrap();
    let legacy = build(legacy_dir.path(), false, 2_000);
    let packed = build(packed_dir.path(), true, 2_000);

    for term in [
        "the",
        "xenolith",
        "café",
        "ของบริษัท",
        "uniq000042",
        "absent-term",
    ] {
        let legacy_count = legacy
            .full_text_term_count_capped(INDEX, term, 1_000_000)
            .unwrap();
        let packed_count = packed
            .full_text_term_count_capped(INDEX, term, 1_000_000)
            .unwrap();
        assert_eq!(
            legacy_count, packed_count,
            "term `{term}` has different planner counts"
        );
    }
}

/// A reopened database must serve the packed index identically — the segment
/// registry is rebuilt lazily from disk, not carried in memory.
#[test]
fn packed_index_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let before = {
        let db = build(dir.path(), true, 1_500);
        let prints = ranked_fingerprint(&db);
        db.close().unwrap();
        prints
    };
    let db = BicDb::open_with_config(dir.path(), config(true)).unwrap();
    let after = ranked_fingerprint(&db);
    assert_eq!(before, after, "reopen changed packed-segment answers");
}

/// DROP INDEX must remove the segment directory, and a REBUILD must produce a
/// fresh working index.
#[test]
fn drop_and_rebuild_reclaim_and_recreate_the_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = build(dir.path(), true, 800);
    let segments = dir.path().join("paged").join("fts-segments");
    let count_dirs = |path: &std::path::Path| {
        std::fs::read_dir(path)
            .map(|entries| entries.count())
            .unwrap_or(0)
    };
    assert!(count_dirs(&segments) > 0, "the build produced no segment");

    db.drop_index(INDEX).unwrap();
    assert_eq!(
        count_dirs(&segments),
        0,
        "DROP INDEX left the segment directory behind"
    );

    db.create_index(IndexDefinition {
        name: INDEX.into(),
        collection: "articles".into(),
        fields: vec![IndexField::MetadataPath(vec!["body".into()])],
        kind: IndexKind::FullText,
        unique: false,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    let hits = db
        .full_text_bm25_top_k(INDEX, &["xenolith"], Bm25Parameters::default(), 10, true)
        .unwrap()
        .unwrap_or_default();
    assert!(
        !hits.is_empty(),
        "the rebuilt segment index answers nothing"
    );
}

/// A torn segment must be refused loudly, never served silently empty.
#[test]
fn a_truncated_segment_file_is_refused_not_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = build(dir.path(), true, 800);
    db.close().unwrap();

    // Truncate postings.dat behind the manifest's back.
    let segments = dir.path().join("paged").join("fts-segments");
    let segment_dir = std::fs::read_dir(&segments)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let postings = segment_dir.join("postings.dat");
    let bytes = std::fs::read(&postings).unwrap();
    std::fs::write(&postings, &bytes[..bytes.len() / 2]).unwrap();

    let db = BicDb::open_with_config(dir.path(), config(true)).unwrap();
    let outcome = db.full_text_bm25_top_k(INDEX, &["the"], Bm25Parameters::default(), 10, true);
    match outcome {
        Err(error) => {
            let error = error.to_string();
            assert!(
                error.contains("torn") || error.contains("REINDEX"),
                "unhelpful torn-segment error: {error}"
            );
        }
        Ok(hits) => {
            assert!(
                hits.is_none(),
                "a torn segment served ranked results as if nothing were wrong"
            );
        }
    }
}

/// The impact-ordered sidecar must stream identically in both formats — for
/// stored blocks AND for the ones packed segments elide (single-pk-block
/// terms) and resynthesize at read. The fingerprint pins the block cut
/// points, both header bounds, the (bucket desc, doc asc) posting order and
/// every rehydrated per-document field, which together determine the bytes.
#[test]
fn impact_scan_streams_are_identical() {
    let keyed_dir = tempfile::tempdir().unwrap();
    let packed_dir = tempfile::tempdir().unwrap();
    let keyed = build(keyed_dir.path(), false, 3_000);
    let packed = build(packed_dir.path(), true, 3_000);

    // Rare (df=1, elided), medium (elided multi-posting), and multi-block
    // common terms (stored sidecar) — every representation path.
    let probes = [
        "xenolith",
        "uniq000042",
        "provenance",
        "distributed",
        "search",
        "the",
        "and",
    ];
    type Stream = (
        Vec<(u16, Option<u32>)>,
        Vec<(u16, String, u32, u32, Vec<u16>)>,
    );
    let capture = |db: &bicdb_core::BicDb, term: &str| -> Stream {
        let mut gates = Vec::new();
        let mut visits = Vec::new();
        let mut budget = bicdb_core::FtsQueryBudget::unlimited();
        let served = db
            .full_text_block_impact_scan(
                INDEX,
                term,
                &mut budget,
                |max_impact, max_rank| {
                    gates.push((max_impact, max_rank.map(f32::to_bits)));
                    bicdb_core::BlockGate::Scan
                },
                |bucket, pk, doc_length, doc_distinct, positions| {
                    visits.push((
                        bucket,
                        pk.to_string(),
                        doc_length,
                        doc_distinct,
                        positions.to_vec(),
                    ));
                    Ok(true)
                },
            )
            .unwrap();
        assert!(served, "impact scan must be served for `{term}`");
        (gates, visits)
    };

    for term in probes {
        let keyed_stream = capture(&keyed, term);
        let packed_stream = capture(&packed, term);
        assert!(
            !keyed_stream.1.is_empty(),
            "probe `{term}` matched nothing; the corpus drifted"
        );
        assert_eq!(
            keyed_stream, packed_stream,
            "impact stream diverged for `{term}`"
        );
    }

    // The comparison above is only meaningful if elision actually happened —
    // otherwise the synthesis path never ran. With ~3,000 df=1 terms elided,
    // impacts.dat holds only the handful of multi-block terms and must be a
    // small fraction of the postings file; storing every sidecar would put
    // the ratio near one half.
    let segments = packed_dir.path().join("paged").join("fts-segments");
    let segment_dir = std::fs::read_dir(&segments)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .expect("packed build published a segment");
    let bytes = |name: &str| std::fs::metadata(segment_dir.join(name)).unwrap().len();
    let (impacts, postings) = (bytes("impacts.dat"), bytes("postings.dat"));
    assert!(
        impacts * 4 < postings,
        "sidecar elision regressed: impacts.dat={impacts} postings.dat={postings}"
    );
}

/// Row-backed builds must not write doc-terms blobs — the rows themselves
/// are the retraction source — and updates/deletes must still retract
/// exactly. The blob namespace measured 0.82 GB on the retained corpus, a
/// fifth of the database, all of it recomputable.
#[test]
fn row_backed_builds_write_no_doc_terms_and_still_retract_exactly() {
    for packed in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = build(dir.path(), packed, 400);
        assert_eq!(
            db.full_text_doc_terms_count(INDEX).unwrap(),
            0,
            "row-backed build (packed={packed}) wrote doc-terms blobs"
        );

        // Delete a document and prove full retraction: its unique term must
        // stop matching entirely, not linger as a phantom posting.
        let victim_terms = ["uniq000042", "the"];
        for term in victim_terms {
            let hits = db
                .full_text_bm25_top_k(INDEX, &[term], Bm25Parameters::default(), 2_000, true)
                .unwrap()
                .unwrap_or_default();
            assert!(
                hits.iter().any(|hit| hit.primary_key == "doc-000042"),
                "corpus drifted: doc-000042 does not match `{term}` before deletion"
            );
        }
        db.delete("articles", "doc-000042").unwrap();
        let unique_hits = db
            .full_text_bm25_top_k(INDEX, &["uniq000042"], Bm25Parameters::default(), 10, true)
            .unwrap()
            .unwrap_or_default();
        assert!(
            unique_hits.is_empty(),
            "deleted document still matches its unique term (packed={packed}): {unique_hits:?}"
        );
        let common_hits = db
            .full_text_bm25_top_k(INDEX, &["the"], Bm25Parameters::default(), 2_000, true)
            .unwrap()
            .unwrap_or_default();
        assert!(
            !common_hits
                .iter()
                .any(|hit| hit.primary_key == "doc-000042"),
            "deleted document still matches a common term (packed={packed})"
        );

        // An update must retract the old terms and index the new ones.
        db.insert(
            "articles",
            Record::new("doc-000107".to_string())
                .with_metadata(json!({ "body": "replacement zzyzxonly content" })),
        )
        .unwrap();
        let stale = db
            .full_text_bm25_top_k(INDEX, &["uniq000107"], Bm25Parameters::default(), 10, true)
            .unwrap()
            .unwrap_or_default();
        assert!(
            stale.is_empty(),
            "updated document still matches its OLD unique term (packed={packed})"
        );
        let fresh = db
            .full_text_bm25_top_k(INDEX, &["zzyzxonly"], Bm25Parameters::default(), 10, true)
            .unwrap()
            .unwrap_or_default();
        assert_eq!(
            fresh.len(),
            1,
            "updated document does not match its NEW term (packed={packed})"
        );
        assert_eq!(fresh[0].primary_key, "doc-000107");
    }
}
