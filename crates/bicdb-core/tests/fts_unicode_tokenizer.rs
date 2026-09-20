//! Scripts whose words contain combining marks must be indexed as words.
//!
//! Thai, Lao, Khmer, Devanagari, and Arabic/Hebrew with diacritics all place
//! vowel signs and tone marks INSIDE a word. Unicode classifies those as
//! category `Mn`/`Mc`, for which `char::is_alphanumeric()` is false — so a
//! tokenizer that splits on "not alphanumeric" cuts every such word at its own
//! vowels.
//!
//! Found against real Common Crawl text, where a 600-character Thai passage
//! yielded six English words and not one Thai term.

use bicdb_core::{
    BicDb, Bm25Parameters, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode,
};
use serde_json::json;

/// "ศึกษา วิจัย" — study, research. Both words carry vowel signs (U+0E36,
/// U+0E31) and would be split by a mark-blind tokenizer.
const THAI_STUDY: &str = "ศึกษา";
const THAI_RESEARCH: &str = "วิจัย";
const THAI_BODY: &str = "ศึกษา วิจัย และเสนอแนะด้านการวางผังและพัฒนาเมือง เพื่อสร้างสมดุลในการพัฒนาเมือง";

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
}

fn indexed(dir: &std::path::Path, bodies: &[&str]) -> BicDb {
    let mut db = BicDb::open_with_config(dir, config()).unwrap();
    db.create_collection("docs").unwrap();
    db.bulk_load_insert(
        "docs",
        bodies
            .iter()
            .enumerate()
            .map(|(index, body)| {
                Record::new(format!("d{index:04}")).with_metadata(json!({ "body": *body }))
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    db.create_index(IndexDefinition {
        name: "docs_fts".into(),
        collection: "docs".into(),
        fields: vec![IndexField::MetadataPath(vec!["body".into()])],
        kind: IndexKind::FullText,
        unique: false,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    db
}

/// The reproducer: a whole Thai word must find the document containing it.
#[test]
fn a_thai_word_matches_the_document_containing_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut bodies = vec![THAI_BODY];
    // Padding so the index is not degenerate.
    for _ in 0..40 {
        bodies.push("unrelated english filler text about databases and storage");
    }
    let db = indexed(dir.path(), &bodies);

    let hits = db
        .full_text_bm25_top_k(
            "docs_fts",
            &[THAI_STUDY, THAI_RESEARCH],
            Bm25Parameters::default(),
            10,
            true,
        )
        .unwrap()
        .unwrap_or_default();

    assert!(
        !hits.is_empty(),
        "searching for the Thai words `{THAI_STUDY}` and `{THAI_RESEARCH}` found \
         nothing in a document that contains both — the tokenizer split them at \
         their own vowel signs"
    );
    assert_eq!(hits[0].primary_key, "d0000");
}

/// Devanagari: the matra in "भारत" (India) is U+0940, also category Mc.
#[test]
fn a_devanagari_word_matches() {
    let dir = tempfile::tempdir().unwrap();
    let mut bodies = vec!["भारत एक देश है"];
    for _ in 0..40 {
        bodies.push("unrelated english filler text about databases and storage");
    }
    let db = indexed(dir.path(), &bodies);
    let hits = db
        .full_text_bm25_top_k("docs_fts", &["भारत"], Bm25Parameters::default(), 10, true)
        .unwrap()
        .unwrap_or_default();
    assert!(
        !hits.is_empty(),
        "Devanagari word did not match its document"
    );
}

/// Latin text with combining accents must behave the same as precomposed.
#[test]
fn combining_accents_do_not_split_a_latin_word() {
    let dir = tempfile::tempdir().unwrap();
    // "café" written with a COMBINING ACUTE ACCENT (U+0301) rather than é.
    let decomposed = "cafe\u{301} serves espresso";
    let mut bodies = vec![decomposed];
    for _ in 0..40 {
        bodies.push("unrelated english filler text about databases and storage");
    }
    let db = indexed(dir.path(), &bodies);
    let hits = db
        .full_text_bm25_top_k(
            "docs_fts",
            &["cafe\u{301}"],
            Bm25Parameters::default(),
            10,
            true,
        )
        .unwrap()
        .unwrap_or_default();
    assert!(
        !hits.is_empty(),
        "a combining accent split `café` into `cafe` plus a stray mark"
    );
}

/// Plain ASCII must be unaffected by the fix.
#[test]
fn ascii_tokenisation_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let mut bodies = vec!["the quick brown fox jumps over the lazy dog"];
    for _ in 0..40 {
        bodies.push("unrelated english filler text about databases and storage");
    }
    let db = indexed(dir.path(), &bodies);
    let hits = db
        .full_text_bm25_top_k(
            "docs_fts",
            &["quick", "brown"],
            Bm25Parameters::default(),
            10,
            true,
        )
        .unwrap()
        .unwrap_or_default();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].primary_key, "d0000");
}

/// Thai is written WITHOUT spaces between words. The earlier test in this file
/// passes only because the Common Crawl page it came from happened to space
/// its words apart. Real Thai prose does not.
///
/// This is a different problem from combining marks and is not fixed by
/// admitting marks into tokens: no boundary character exists to split on, so
/// the whole run becomes one term and a search for a word inside it fails.
#[test]
fn thai_written_without_spaces_does_not_match_a_word_inside_it() {
    let dir = tempfile::tempdir().unwrap();
    // "study research and recommend" as one unbroken run, which is how Thai
    // is actually written.
    let run_together = "ศึกษาวิจัยและเสนอแนะด้านการวางผัง";
    let mut bodies = vec![run_together];
    for _ in 0..40 {
        bodies.push("unrelated english filler text about databases and storage");
    }
    let db = indexed(dir.path(), &bodies);

    let hits = db
        .full_text_bm25_top_k("docs_fts", &["วิจัย"], Bm25Parameters::default(), 10, true)
        .unwrap()
        .unwrap_or_default();

    // Documented, not asserted away: this is a KNOWN LIMITATION shared with
    // Tantivy's default analyzer, which produces zero tokens for the same
    // input. Proper support needs dictionary-based segmentation for Thai,
    // Lao, Khmer, Burmese, Chinese and Japanese.
    assert!(
        hits.is_empty(),
        "run-together Thai now matches a word inside it — if this fails, word \
         segmentation has been implemented and this test should become a \
         positive assertion"
    );

    // The whole run IS findable as a single term, which is what the tokenizer
    // actually produced.
    let whole = db
        .full_text_bm25_top_k(
            "docs_fts",
            &["ศึกษาวิจัยและเสนอแนะด้านการวางผัง"],
            Bm25Parameters::default(),
            10,
            true,
        )
        .unwrap()
        .unwrap_or_default();
    assert!(
        !whole.is_empty(),
        "the unbroken run is not even findable as a whole term"
    );
}
