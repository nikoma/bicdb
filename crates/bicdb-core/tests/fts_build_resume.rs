//! A full-text index build that is interrupted must resume, not restart —
//! and must produce exactly the index a clean build would have produced.
//!
//! At corpus scale this build runs for hours or days. `fts_build` has always
//! been written to be restartable (a phase checkpoint, atomically completed
//! sorted runs, and the rule that a crash may leave a `.tmp` file but never a
//! run a resume mistakes for complete). Until now that was a **design claim
//! with no test behind it**, while the cheaper spatial pack had
//! `spatial_pack_resume.rs`. The expensive build was the untested one.
//!
//! These tests interrupt at every phase boundary and require the recovered
//! index to answer identically to one built without interruption.
//!
//! **What they found:** the machinery resumes, but nothing reaches it.
//! `create_index` on an existing full-text index is rejected with "already
//! exists" — there is no FTS equivalent of the paged B-tree's
//! `reconcile_published_paged_btree_build` — so the operator's only path is
//! DROP then CREATE, which discards the workspace. Measured: a crash entering
//! `Publishing`, with nearly all the work done, costs **1.11x of a full
//! build**. Correct, and a total loss of the run.

use bicdb_core::{
    BicDb, Bm25Parameters, DbConfig, FullTextBuildLifecycleState, FullTextBuildRecommendedAction,
    FullTextBuildReconcileOutcome, IndexDefinition, IndexField, IndexKind, Record, StorageMode,
};
use serde_json::json;

/// The crash knob is process-global.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

const INDEX: &str = "articles_fts";
const WORDS: &[&str] = &[
    "the",
    "and",
    "search",
    "index",
    "content",
    "service",
    "research",
    "framework",
    "distributed",
    "provenance",
    "xenolith",
    "zeolite",
];

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
}

fn body(seed: u64, words: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).max(1);
    let mut text = String::with_capacity(words * 8);
    for _ in 0..words {
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
    }
    text
}

fn seeded(dir: &std::path::Path, rows: usize) -> BicDb {
    let mut db = BicDb::open_with_config(dir, config()).unwrap();
    db.create_collection("articles").unwrap();
    db.bulk_load_insert(
        "articles",
        (0..rows)
            .map(|index| {
                Record::new(format!("doc-{index:06}"))
                    .with_metadata(json!({ "body": body(index as u64, 90) }))
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    db
}

fn definition() -> IndexDefinition {
    IndexDefinition {
        name: INDEX.to_string(),
        collection: "articles".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["body".to_string()])],
        kind: IndexKind::FullText,
        unique: false,
        predicate: None,
        exclusion: None,
    }
}

/// Every ranked answer the index can give, as a comparable fingerprint.
fn fingerprint(db: &BicDb) -> Vec<(String, Vec<(String, i64)>)> {
    let mut out = Vec::new();
    for terms in [
        vec!["the", "and"],
        vec!["search", "index"],
        vec!["the", "content"],
        vec!["research", "framework"],
        vec!["the", "and", "search"],
    ] {
        let hits = db
            .full_text_bm25_top_k(INDEX, &terms, Bm25Parameters::default(), 25, true)
            .unwrap()
            .unwrap_or_default();
        out.push((
            terms.join("+"),
            hits.into_iter()
                // Scores are f32; compare them at a fixed scale so a
                // fingerprint mismatch means a different ANSWER, not a
                // different last bit.
                .map(|hit| (hit.primary_key, (hit.score * 10_000.0) as i64))
                .collect(),
        ));
    }
    out
}

fn build_clean(dir: &std::path::Path, rows: usize) -> Vec<(String, Vec<(String, i64)>)> {
    let mut db = seeded(dir, rows);
    db.create_index(definition()).unwrap();
    let reference = fingerprint(&db);
    db.close().unwrap();
    assert!(
        reference.iter().any(|(_, hits)| !hits.is_empty()),
        "the reference build produced no hits, so nothing is being compared"
    );
    reference
}

/// Interrupt entering `phase`, reopen, recover, and require the finished
/// index to be indistinguishable from an uninterrupted build.
///
/// Named `recover`, not `resume`, on purpose: today this path REBUILDS. See
/// `recovery_after_an_interrupted_build_is_measured_not_assumed`.
fn recover_after_crash_at(phase: &str) {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let rows = 4_000;

    let reference_dir = tempfile::tempdir().unwrap();
    let reference = build_clean(reference_dir.path(), rows);

    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path(), rows);

    std::env::set_var("BICDB_FTS_BUILD_CRASH_AT", phase);
    let interrupted = db.create_index(definition());
    std::env::remove_var("BICDB_FTS_BUILD_CRASH_AT");
    assert!(
        interrupted.is_err(),
        "the injected crash at `{phase}` did not interrupt the build"
    );
    // Drop without a clean close: the state on disk is what a killed process
    // would have left.
    drop(db);

    // The operator's actual recovery path: simply re-issue CREATE INDEX.
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    assert_eq!(
        db.full_text_open_metrics().fallback_row_ids_registered,
        0,
        "an incomplete external build must reopen from its workspace without a corpus-wide row-id registry"
    );
    db.create_index(definition())
        .unwrap_or_else(|error| panic!("resume after `{phase}` failed: {error}"));
    let resumed = fingerprint(&db);
    db.close().unwrap();

    assert_eq!(
        resumed, reference,
        "an index resumed after a crash at `{phase}` answers differently from \
         one built without interruption"
    );
}

#[test]
fn a_build_interrupted_while_tokenizing_resumes_correctly() {
    recover_after_crash_at("mergepk");
}

#[test]
fn a_two_pass_era_workspace_resumes_through_the_single_pass_merge() {
    // Builds checkpointed at `MergeImpact` come from the two-pass era; the
    // driver resets them to the single-pass merge. Manufacture one: crash
    // entering the merge, then rewrite the checkpoint the way an old binary
    // would have left it.
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let rows = 4_000;
    let reference_dir = tempfile::tempdir().unwrap();
    let reference = build_clean(reference_dir.path(), rows);

    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path(), rows);
    std::env::set_var("BICDB_FTS_BUILD_CRASH_AT", "publishing");
    let _ = db.create_index(definition());
    std::env::remove_var("BICDB_FTS_BUILD_CRASH_AT");
    drop(db);

    let mut rewritten = 0usize;
    for entry in walk(dir.path()) {
        if entry.file_name().and_then(|name| name.to_str()) == Some("checkpoint.json") {
            let raw = std::fs::read_to_string(&entry).unwrap();
            assert!(raw.contains("merge_pk"), "unexpected checkpoint: {raw}");
            std::fs::write(&entry, raw.replace("merge_pk", "merge_impact")).unwrap();
            rewritten += 1;
        }
    }
    assert_eq!(rewritten, 1, "expected exactly one build checkpoint");

    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    db.create_index(definition())
        .expect("a two-pass-era checkpoint must resume");
    let resumed = fingerprint(&db);
    db.close().unwrap();
    assert_eq!(
        resumed, reference,
        "resumed index differs from a clean build"
    );
}

#[test]
fn a_build_interrupted_while_publishing_resumes_correctly() {
    recover_after_crash_at("publishing");
}

/// A partial `.tmp` run left by a crash must never be adopted as a completed
/// run. This is the module's own stated contract; it had no test.
#[test]
fn a_stray_tmp_run_is_never_adopted_as_complete() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let rows = 4_000;

    let reference_dir = tempfile::tempdir().unwrap();
    let reference = build_clean(reference_dir.path(), rows);

    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path(), rows);
    std::env::set_var("BICDB_FTS_BUILD_CRASH_AT", "mergepk");
    let _ = db.create_index(definition());
    std::env::remove_var("BICDB_FTS_BUILD_CRASH_AT");
    drop(db);

    // Plant a truncated run alongside the real ones, named as a temp file the
    // way an interrupted writer would leave it.
    let mut planted = 0usize;
    for entry in walk(dir.path()) {
        if entry.extension().and_then(|value| value.to_str()) == Some("run") {
            let tmp = entry.with_extension("run.tmp");
            std::fs::write(&tmp, b"BICFTR03 truncated garbage").unwrap();
            planted += 1;
            break;
        }
    }
    assert!(
        planted > 0,
        "no run files were produced, so nothing was planted"
    );

    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    db.create_index(definition())
        .expect("resume must ignore the stray temp run");
    let resumed = fingerprint(&db);
    db.close().unwrap();
    assert_eq!(
        resumed, reference,
        "a stray .tmp run changed the finished index"
    );
}

fn walk(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let child = entry.path();
            if child.is_dir() {
                stack.push(child);
            } else {
                found.push(child);
            }
        }
    }
    found
}

/// Does the recovery path RESUME from the checkpoint, or silently restart?
///
/// For a build that runs for hours this is the only question that matters:
/// a correct rebuild that discards a day of work is not recovery.
#[test]
fn recovery_after_an_interrupted_build_is_measured_not_assumed() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let rows = 20_000;

    // Baseline: a full build from nothing.
    let clean_dir = tempfile::tempdir().unwrap();
    let mut db = seeded(clean_dir.path(), rows);
    let clean_started = std::time::Instant::now();
    db.create_index(definition()).unwrap();
    let clean = clean_started.elapsed();
    db.close().unwrap();

    // Interrupt late — entering Publishing, so nearly all the work is done.
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path(), rows);
    std::env::set_var("BICDB_FTS_BUILD_CRASH_AT", "publishing");
    let _ = db.create_index(definition());
    std::env::remove_var("BICDB_FTS_BUILD_CRASH_AT");
    drop(db);

    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let recovery_started = std::time::Instant::now();
    let report = db
        .create_index_online(definition())
        .expect("re-issuing CREATE INDEX must resume, not be refused");
    let recovery = recovery_started.elapsed();
    assert!(
        report.interrupted_previous,
        "the retry did not report that it continued a previous run"
    );
    db.close().unwrap();

    let ratio = recovery.as_secs_f64() / clean.as_secs_f64().max(1e-9);
    println!(
        "clean build {clean:?}; recovery after a crash entering Publishing \
         {recovery:?} ({ratio:.2}x of a full build)"
    );
    assert!(
        ratio < 0.6,
        "recovery took {ratio:.2}x a full build ({recovery:?} vs {clean:?}) — \
         the interrupted build was RESTARTED, not resumed. At corpus scale \
         that discards the entire run."
    );
}

// ---- run-file integrity -------------------------------------------------

fn run_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    walk(dir)
        .into_iter()
        .filter(|path| path.extension().and_then(|v| v.to_str()) == Some("run"))
        .collect()
}

/// Build far enough to produce runs, then stop.
fn build_partial_with_runs(dir: &std::path::Path, rows: usize) -> Vec<std::path::PathBuf> {
    let mut db = seeded(dir, rows);
    std::env::set_var("BICDB_FTS_BUILD_CRASH_AT", "mergepk");
    let _ = db.create_index(definition());
    std::env::remove_var("BICDB_FTS_BUILD_CRASH_AT");
    drop(db);
    let runs = run_files(dir);
    assert!(!runs.is_empty(), "no run files were produced");
    runs
}

/// A run truncated by power loss must be refused, not read as data.
///
/// The magic header alone proved only that a file STARTED as a run: a torn
/// write leaves a valid header above truncated payload, and the merge would
/// have consumed it happily.
#[test]
fn a_truncated_run_is_refused() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let runs = build_partial_with_runs(dir.path(), 4_000);

    let victim = &runs[0];
    let bytes = std::fs::read(victim).unwrap();
    assert!(bytes.len() > 64);
    // Chop the tail: header intact, trailer gone.
    std::fs::write(victim, &bytes[..bytes.len() * 2 / 3]).unwrap();

    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let error = db
        .create_index(definition())
        .expect_err("a truncated run must not be built from")
        .to_string();
    assert!(
        error.contains("trailer") || error.contains("torn") || error.contains("completely written"),
        "unhelpful error for a truncated run: {error}"
    );
}

/// A run whose SIZE is right but whose CONTENT was corrupted in place must
/// also be refused. The length check cannot see this; the digest can.
#[test]
fn a_corrupted_run_of_the_right_length_is_refused() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let runs = build_partial_with_runs(dir.path(), 4_000);

    let victim = &runs[0];
    let mut bytes = std::fs::read(victim).unwrap();
    assert!(bytes.len() > 128);
    // Flip a bit in the middle of the payload; length and trailer untouched.
    let at = bytes.len() / 2;
    bytes[at] ^= 0xFF;
    std::fs::write(victim, &bytes).unwrap();

    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let outcome = db.create_index(definition());
    match outcome {
        Err(error) => {
            let error = error.to_string();
            assert!(
                error.contains("checksum")
                    || error.contains("run")
                    || error.contains("beyond the checkpoint count"),
                "unhelpful error for a corrupted run: {error}"
            );
        }
        // A flipped byte can also derail decoding before EOF; either way the
        // build must not silently succeed on corrupt input.
        Ok(_) => panic!("a corrupted run produced a successful build"),
    }
}

/// Every run a normal build writes carries a verifiable trailer.
#[test]
fn completed_runs_carry_a_verifiable_trailer() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let runs = build_partial_with_runs(dir.path(), 4_000);
    for run in &runs {
        let bytes = std::fs::read(run).unwrap();
        assert!(bytes.len() >= 48, "run {run:?} is too short for a trailer");
        assert_eq!(
            &bytes[bytes.len() - 48..bytes.len() - 40],
            b"BICFTT01",
            "run {run:?} has no completion trailer"
        );
    }
}

// ---- progress reporting -------------------------------------------------

/// A multi-day build must be able to answer "which phase, how far" from
/// another process, or an operator cannot tell slow from stuck.
#[test]
fn an_interrupted_build_reports_its_phase_and_progress() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let rows = 4_000;
    let mut db = seeded(dir.path(), rows);

    // Nothing started yet.
    assert!(
        db.full_text_build_progress(INDEX).unwrap().is_none(),
        "a build that never started reported progress"
    );

    std::env::set_var("BICDB_FTS_BUILD_CRASH_AT", "publishing");
    let _ = db.create_index(definition());
    std::env::remove_var("BICDB_FTS_BUILD_CRASH_AT");
    drop(db);

    // Read it the way an operator would: a separate open, build not running.
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let progress = db
        .full_text_build_progress(INDEX)
        .unwrap()
        .expect("an interrupted build must report progress");

    assert_eq!(progress.index, INDEX);
    assert_eq!(progress.collection, "articles");
    // The crash fires ENTERING publish, so the checkpoint still records the
    // last phase that actually completed — the single-pass merge. A
    // checkpoint must never claim work that did not happen.
    assert_eq!(
        progress.phase, "merging_primary_key",
        "phase should name the last COMPLETED phase"
    );
    assert!(
        progress.documents_tokenized > 0,
        "no tokenized documents reported despite reaching the merge phase"
    );
    assert!(progress.runs_written > 0, "no runs reported");
    // Percent is deliberately absent outside tokenizing: the merge phases have
    // no honest denominator, and a fabricated one is exactly the number an
    // operator would use to decide whether to kill a build.
    assert_eq!(progress.percent, None);
}

/// During tokenizing there IS a real denominator, so a percentage is reported.
#[test]
fn a_tokenizing_build_reports_a_percentage() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path(), 4_000);
    std::env::set_var("BICDB_FTS_BUILD_CRASH_AT", "mergepk");
    let _ = db.create_index(definition());
    std::env::remove_var("BICDB_FTS_BUILD_CRASH_AT");
    drop(db);

    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let progress = db.full_text_build_progress(INDEX).unwrap().unwrap();
    assert_eq!(progress.phase, "tokenizing");
    assert_eq!(progress.documents_total, Some(4_000));
    let percent = progress
        .percent
        .expect("tokenizing must report a percentage");
    assert!(percent <= 100);
}

/// A finished build cleans up its workspace, so it reports nothing.
#[test]
fn a_completed_build_reports_no_progress() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path(), 2_000);
    db.create_index(definition()).unwrap();
    assert!(
        db.full_text_build_progress(INDEX).unwrap().is_none(),
        "a completed build still reports an in-flight workspace"
    );
}

/// A supervisor should need exactly one operation after restart: reconcile.
/// BicDB decides whether to resume bounded tokenization or finalize, and the
/// same call remains harmless after publication.
#[test]
fn lifecycle_reconcile_converges_and_is_idempotent_after_reopen() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path(), 2_000);
    let first = db.full_text_build_step(definition(), 200).unwrap();
    assert!(matches!(
        first,
        bicdb_core::FullTextBuildStep::InProgress { .. }
    ));
    drop(db);

    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let status = db.full_text_build_lifecycle(INDEX).unwrap();
    assert_eq!(status.state, FullTextBuildLifecycleState::Tokenizing);
    assert_eq!(
        status.recommended_action,
        FullTextBuildRecommendedAction::Reconcile
    );
    assert!(status.resumable);
    assert!(status.checkpoint_updated_unix_ms.is_some());

    let mut calls = 0usize;
    loop {
        let report = db.reconcile_full_text_build(INDEX, 200).unwrap();
        calls += 1;
        assert!(calls < 32, "reconciliation failed to converge");
        match report.outcome {
            FullTextBuildReconcileOutcome::Progressed => {
                assert!(report.documents_indexed > 0);
            }
            FullTextBuildReconcileOutcome::Published => break,
            unexpected => panic!("unexpected reconciliation outcome: {unexpected:?}"),
        }
    }
    let published = db.full_text_build_lifecycle(INDEX).unwrap();
    assert_eq!(published.state, FullTextBuildLifecycleState::Published);
    assert!(published.serving);
    assert!(!published.resumable);
    assert!(published.published.is_some());

    let repeated = db.reconcile_full_text_build(INDEX, 200).unwrap();
    assert_eq!(
        repeated.outcome,
        FullTextBuildReconcileOutcome::AlreadyPublished
    );
    assert_eq!(repeated.after.state, FullTextBuildLifecycleState::Published);
    db.close().unwrap();
}

/// The sharpest publication crash is after the alias swap but before the
/// workspace is marked complete. The serving generation must remain visible,
/// and reconciliation must cleanly converge without creating another one.
#[test]
fn reconcile_recovers_a_crash_after_atomic_publication() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path(), 2_000);
    std::env::set_var("BICDB_FTS_BUILD_CRASH_AT", "complete");
    let interrupted = db.create_index(definition());
    std::env::remove_var("BICDB_FTS_BUILD_CRASH_AT");
    assert!(interrupted.is_err(), "publication crash was not injected");
    drop(db);

    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let interrupted = db.full_text_build_lifecycle(INDEX).unwrap();
    assert_eq!(interrupted.state, FullTextBuildLifecycleState::Publishing);
    assert!(
        interrupted.serving,
        "the atomically published generation disappeared after reopen"
    );
    let physical = interrupted
        .published
        .as_ref()
        .expect("published generation identity missing")
        .physical_index
        .clone();

    let recovered = db.reconcile_full_text_build(INDEX, 1).unwrap();
    assert_eq!(recovered.outcome, FullTextBuildReconcileOutcome::Published);
    assert_eq!(
        recovered.after.state,
        FullTextBuildLifecycleState::Published
    );
    assert_eq!(
        recovered
            .after
            .published
            .as_ref()
            .expect("reconciled generation missing")
            .physical_index,
        physical,
        "reconciliation replaced an already atomically published generation"
    );
    assert!(
        fingerprint(&db).iter().any(|(_, hits)| !hits.is_empty()),
        "reconciled generation does not answer queries"
    );
    assert_eq!(
        db.reconcile_full_text_build(INDEX, 1).unwrap().outcome,
        FullTextBuildReconcileOutcome::AlreadyPublished
    );
    db.close().unwrap();
}

/// The progressive flag's whole contract: a build interrupted before its
/// final merge must already ANSWER QUERIES over the documents its published
/// sub-segments cover, and the resumed build must converge to exactly the
/// index a non-progressive build produces — with the disposable
/// sub-segments gone.
#[test]
fn a_progressive_build_is_searchable_before_its_final_merge() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let rows = 4_000;
    let reference_dir = tempfile::tempdir().unwrap();
    let reference = build_clean(reference_dir.path(), rows);

    let progressive_config = || {
        config()
            .with_fts_packed_segments(true)
            .with_fts_progressive(true)
            .with_fts_progressive_interval_docs(500)
    };
    let dir = tempfile::tempdir().unwrap();
    let mut db = {
        let mut db = BicDb::open_with_config(dir.path(), progressive_config()).unwrap();
        db.create_collection("articles").unwrap();
        db.bulk_load_insert(
            "articles",
            (0..rows)
                .map(|index| {
                    Record::new(format!("doc-{index:06}"))
                        .with_metadata(json!({ "body": body(index as u64, 90) }))
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        db
    };
    // Crash ENTERING the merge: tokenization complete, every interval's
    // sub-segment published, final segment never built.
    std::env::set_var("BICDB_FTS_BUILD_CRASH_AT", "mergepk");
    let _ = db.create_index(definition());
    std::env::remove_var("BICDB_FTS_BUILD_CRASH_AT");
    drop(db);

    // Reopen the way an operator would after a crash, re-issue CREATE INDEX
    // to resume, but FIRST prove the partial index answers. Resume re-flips
    // on open of the build — so query through a crash-interrupted resume:
    // interrupt AGAIN entering the merge, which leaves the re-flipped
    // visibility standing with no build running.
    let mut db = BicDb::open_with_config(dir.path(), progressive_config()).unwrap();
    std::env::set_var("BICDB_FTS_BUILD_CRASH_AT", "mergepk");
    let _ = db.create_index(definition());
    std::env::remove_var("BICDB_FTS_BUILD_CRASH_AT");
    let partial = db
        .full_text_bm25_top_k(INDEX, &["the"], Bm25Parameters::default(), 25, true)
        .unwrap()
        .expect("a progressive build must be served before its final merge");
    assert!(
        !partial.is_empty(),
        "sub-segments published, but the partial index answered nothing"
    );

    // Resume to completion: identical to a clean build, sub-segments gone.
    db.create_index(definition())
        .expect("progressive build must resume to completion");
    let resumed = fingerprint(&db);
    assert_eq!(
        resumed, reference,
        "progressive final differs from a clean build"
    );
    let segments = dir.path().join("paged").join("fts-segments");
    let mut sub_dirs = 0usize;
    for entry in walk(&segments) {
        if entry
            .file_name()
            .map(|name| name.to_string_lossy().starts_with("sub-"))
            .unwrap_or(false)
        {
            sub_dirs += 1;
        }
    }
    assert_eq!(
        sub_dirs, 0,
        "disposable sub-segments must be deleted at completion"
    );
    db.close().unwrap();
}

/// The stepped build is the online-build loop: bounded work per call,
/// queries answered between calls over everything previous steps published,
/// and a final state identical to CREATE INDEX.
#[test]
fn a_stepped_progressive_build_answers_queries_between_steps() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let rows = 4_000;
    let reference_dir = tempfile::tempdir().unwrap();
    let reference = build_clean(reference_dir.path(), rows);

    let dir = tempfile::tempdir().unwrap();
    let mut db = {
        let mut db = BicDb::open_with_config(
            dir.path(),
            config()
                .with_fts_packed_segments(true)
                .with_fts_progressive(true)
                .with_fts_progressive_interval_docs(400),
        )
        .unwrap();
        db.create_collection("articles").unwrap();
        db.bulk_load_insert(
            "articles",
            (0..rows)
                .map(|index| {
                    Record::new(format!("doc-{index:06}"))
                        .with_metadata(json!({ "body": body(index as u64, 90) }))
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        db
    };

    let mut steps = 0usize;
    let mut answered_mid_build = false;
    loop {
        match db.full_text_build_step(definition(), 600).unwrap() {
            bicdb_core::FullTextBuildStep::InProgress { documents_indexed } => {
                assert!(documents_indexed > 0);
                steps += 1;
                assert!(steps < 64, "the stepped build failed to converge");
                // Everything PREVIOUS steps published answers right now.
                if steps >= 2 {
                    let hits = db
                        .full_text_bm25_top_k(INDEX, &["the"], Bm25Parameters::default(), 10, true)
                        .unwrap();
                    if hits.map(|hits| !hits.is_empty()).unwrap_or(false) {
                        answered_mid_build = true;
                    }
                }
            }
            bicdb_core::FullTextBuildStep::Complete => break,
        }
    }
    assert!(
        steps >= 3,
        "budget 600 over 4000 rows must take several steps"
    );
    assert!(
        answered_mid_build,
        "no query was answered between steps despite published sub-segments"
    );
    let stepped = fingerprint(&db);
    assert_eq!(
        stepped, reference,
        "stepped final differs from a clean build"
    );
    db.close().unwrap();
}
