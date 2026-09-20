//! G5.4: make the machine hostile. Real process death (`SIGKILL` from a
//! parent that the child cannot anticipate), plus deliberate corruption,
//! truncation and orphan temp files.
//!
//! Acceptance is brutally simple: **no silent wrong result, ever.** Either
//! the projection reconciles exactly against authoritative recomputation, or
//! BicDB refuses to load the snapshot.

use std::process::{Command, Stdio};
use std::time::Duration;

use bicdb_core::aggregate_projection::AggregateProjection;
use bicdb_core::{BicDb, DbConfig};

fn projection() -> AggregateProjection {
    AggregateProjection::new(
        "chaos",
        "businesses",
        vec!["state".into(), "category".into(), "host".into()],
        vec!["score".into()],
    )
    .unwrap()
}

fn open_db(dir: &std::path::Path) -> BicDb {
    BicDb::open_with_config(
        dir,
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap()
}

/// Locate the chaos child. Cargo emits it as `chaos_child-<hash>` (and
/// sometimes an unhashed copy), so match on the stem rather than an exact
/// name — a silently-skipped chaos test is worse than a failing one.
fn child_binary() -> Option<std::path::PathBuf> {
    let mut path = std::env::current_exe().ok()?;
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    let examples = path.join("examples");
    let exact = examples.join("chaos_child");
    if exact.exists() {
        return Some(exact);
    }
    let mut newest: Option<(std::time::SystemTime, std::path::PathBuf)> = None;
    for entry in std::fs::read_dir(&examples).ok()?.flatten() {
        let name = entry.file_name();
        let name = name.to_str().unwrap_or_default();
        if !name.starts_with("chaos_child-") || name.ends_with(".d") {
            continue;
        }
        let Ok(modified) = entry.metadata().and_then(|meta| meta.modified()) else {
            continue;
        };
        if newest.as_ref().is_none_or(|(best, _)| modified > *best) {
            newest = Some((modified, entry.path()));
        }
    }
    newest.map(|(_, path)| path)
}

/// The published manifest — the atomicity boundary. Corruption is injected
/// here and into the pages it references; the ORACLE is unchanged.
fn snapshot_path(state: &std::path::Path) -> std::path::PathBuf {
    state.join("chaos.projection").join("manifest.json")
}

/// Every page file the checkpoint references.
fn page_paths(state: &std::path::Path) -> Vec<std::path::PathBuf> {
    let pages = state.join("chaos.projection").join("pages");
    std::fs::read_dir(pages)
        .map(|entries| entries.flatten().map(|entry| entry.path()).collect())
        .unwrap_or_default()
}

/// THE test: a child process that does not know when it will die, murdered
/// repeatedly with SIGKILL at arbitrary instants, must never leave state that
/// reconciles wrong.
#[test]
fn sigkill_storm_never_produces_a_wrong_result() {
    let child = child_binary().expect(
        "chaos_child example must be built: run with --examples. A chaos test that \
         silently skips is worse than one that fails.",
    );
    let db_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(state_dir.path()).unwrap();

    // Seed from the parent so every kill lands in the window that matters —
    // mutation, catch-up and snapshot publication — rather than in initial
    // load, where there would be nothing to reconcile yet.
    {
        let mut db = open_db(db_dir.path());
        db.create_collection("businesses").unwrap();
        let records: Vec<bicdb_core::Record> = (0..1_500)
            .map(|index| {
                bicdb_core::Record::new(format!("biz-{index:06}")).with_metadata(
                    serde_json::json!({
                        "state": format!("state-{}", index % 12),
                        "category": format!("cat-{}", index % 30),
                        "host": format!("host-{}", index % 7),
                        "score": (index % 100) as f64,
                    }),
                )
            })
            .collect();
        db.bulk_load_insert("businesses", records).unwrap();
        db.close().unwrap();
    }

    let mut kills = 0;
    let mut recoveries = 0;
    for round in 0..12 {
        let mut process = Command::new(&child)
            .arg(db_dir.path())
            .arg(state_dir.path())
            .arg("1500")
            .arg(format!("{}", 0x1234_5678u64 + round * 7919))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn chaos child");

        // Let it run an unpredictable while, then murder it mid-flight. The
        // spread deliberately straddles startup, mutation, catch-up and
        // snapshot publication.
        let live_ms = 120 + (round * 37) % 400;
        std::thread::sleep(Duration::from_millis(live_ms));
        let _ = process.kill();
        let _ = process.wait();
        kills += 1;

        // Recover from whatever survived and demand exactness.
        let db = open_db(db_dir.path());
        let recovered =
            AggregateProjection::open(state_dir.path(), "chaos", &db, || Ok(projection()))
                .expect("a killed child must leave a loadable or absent snapshot");
        let drift = recovered
            .verify_durable_invariants(&db)
            .expect("durability invariants must hold after SIGKILL");
        assert!(
            drift.is_clean(),
            "round {round}: SIGKILL left a wrong result: {drift:?}"
        );
        recoveries += 1;
    }
    assert_eq!(
        kills, recoveries,
        "every kill must be followed by a clean recovery"
    );
}

/// A truncated snapshot must be REFUSED, not partially interpreted.
#[test]
fn truncated_snapshots_are_refused() {
    let db_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let mut db = open_db(db_dir.path());
    db.create_collection("businesses").unwrap();
    for index in 0..200 {
        db.insert(
            "businesses",
            bicdb_core::Record::new(format!("b{index:04}")).with_metadata(serde_json::json!({
                "state": "UK", "category": "dentist", "host": "h1", "score": 10.0,
            })),
        )
        .unwrap();
    }
    let mut projection = projection();
    projection.catch_up(&db).unwrap();
    projection.save(state_dir.path(), true).unwrap();
    let good = std::fs::read(snapshot_path(state_dir.path())).unwrap();

    for fraction in [0.99, 0.95, 0.80, 0.50, 0.01] {
        let cut = ((good.len() as f64) * fraction) as usize;
        std::fs::write(snapshot_path(state_dir.path()), &good[..cut]).unwrap();
        let loaded = AggregateProjection::load(state_dir.path(), "chaos");
        assert!(
            loaded.is_err() || loaded.unwrap().is_none(),
            "a snapshot truncated to {fraction} was accepted"
        );
    }
    // One byte.
    std::fs::write(snapshot_path(state_dir.path()), b"{").unwrap();
    assert!(AggregateProjection::load(state_dir.path(), "chaos").is_err());

    // The intact snapshot still loads and is exact.
    std::fs::write(snapshot_path(state_dir.path()), &good).unwrap();
    let restored = AggregateProjection::load(state_dir.path(), "chaos")
        .unwrap()
        .unwrap();
    assert!(restored.verify_durable_invariants(&db).unwrap().is_clean());
}

/// A bit flip inside the payload must be caught by the checksum. Without one,
/// a flipped digit inside a count parses cleanly and yields plausible, wrong
/// aggregates — the exact failure this subsystem exists to prevent.
#[test]
fn bit_flips_are_caught_rather_than_interpreted() {
    let db_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let mut db = open_db(db_dir.path());
    db.create_collection("businesses").unwrap();
    for index in 0..120 {
        db.insert(
            "businesses",
            bicdb_core::Record::new(format!("b{index:04}")).with_metadata(serde_json::json!({
                "state": "UK", "category": "dentist", "host": "h1", "score": 42.0,
            })),
        )
        .unwrap();
    }
    let mut projection = projection();
    projection.catch_up(&db).unwrap();
    projection.save(state_dir.path(), true).unwrap();
    let good = std::fs::read(snapshot_path(state_dir.path())).unwrap();

    let mut refused = 0;
    let mut attempts = 0;
    let mut seed = 0xdead_beef_cafe_1234u64;
    for _ in 0..40 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let mut damaged = good.clone();
        // Flip a byte somewhere inside the payload region.
        let at = (seed as usize) % damaged.len();
        damaged[at] ^= 0x20;
        if damaged == good {
            continue;
        }
        attempts += 1;
        std::fs::write(snapshot_path(state_dir.path()), &damaged).unwrap();
        match AggregateProjection::load(state_dir.path(), "chaos") {
            Err(_) => refused += 1,
            Ok(None) => refused += 1,
            Ok(Some(loaded)) => {
                // If it loaded at all, it MUST still be exactly right —
                // silently-wrong is the forbidden outcome.
                let drift = loaded.verify_durable_invariants(&db);
                assert!(
                    drift.map(|drift| drift.is_clean()).unwrap_or(false),
                    "a corrupted snapshot loaded and produced wrong aggregates"
                );
            }
        }
    }
    assert!(attempts > 0);
    assert!(
        refused > 0,
        "no corruption was detected at all — the checksum is not working"
    );

    // Pages are a NEW corruption surface under incremental persistence: the
    // manifest can be pristine while a page it references is damaged. Each
    // page carries its own checksum for exactly this reason.
    std::fs::write(snapshot_path(state_dir.path()), &good).unwrap();
    let pages = page_paths(state_dir.path());
    assert!(!pages.is_empty(), "a checkpoint must reference page files");
    let mut page_refusals = 0;
    for page in &pages {
        let original = std::fs::read(page).unwrap();
        if original.len() < 40 {
            continue;
        }
        let mut damaged = original.clone();
        let at = original.len() / 2;
        damaged[at] ^= 0xff;
        std::fs::write(page, &damaged).unwrap();
        match AggregateProjection::load(state_dir.path(), "chaos") {
            Err(_) => page_refusals += 1,
            Ok(None) => page_refusals += 1,
            Ok(Some(loaded)) => {
                assert!(
                    loaded
                        .verify_durable_invariants(&db)
                        .map(|drift| drift.is_clean())
                        .unwrap_or(false),
                    "a corrupted PAGE loaded and produced wrong aggregates"
                );
            }
        }
        std::fs::write(page, &original).unwrap();
    }
    assert!(
        page_refusals > 0,
        "corrupting a referenced page was never detected"
    );
}

/// A crash can leave an unpublished `.tmp` beside the snapshot. The published
/// file wins; the orphan is never preferred on timestamp.
#[test]
fn orphan_temp_files_are_ignored() {
    let db_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let mut db = open_db(db_dir.path());
    db.create_collection("businesses").unwrap();
    for index in 0..50 {
        db.insert(
            "businesses",
            bicdb_core::Record::new(format!("b{index:04}")).with_metadata(serde_json::json!({
                "state": "UK", "category": "yoga", "host": "h2", "score": 5.0,
            })),
        )
        .unwrap();
    }
    let mut projection = projection();
    projection.catch_up(&db).unwrap();
    projection.save(state_dir.path(), true).unwrap();
    let published = std::fs::read(snapshot_path(state_dir.path())).unwrap();

    // A NEWER but unpublished temp file containing garbage.
    let tmp = state_dir
        .path()
        .join("chaos.projection")
        .join("manifest.json.tmp");
    std::fs::write(&tmp, b"{\"magic\":\"lies\"}").unwrap();

    let restored = AggregateProjection::load(state_dir.path(), "chaos")
        .unwrap()
        .unwrap();
    assert!(
        restored.verify_durable_invariants(&db).unwrap().is_clean(),
        "an orphan temp file influenced recovery"
    );
    assert_eq!(
        std::fs::read(snapshot_path(state_dir.path())).unwrap(),
        published,
        "the published snapshot must be untouched"
    );
}

/// Generations advance on publish, so a recovery can prefer the newest valid
/// one rather than trusting filesystem timestamps.
#[test]
fn generations_advance_monotonically() {
    let db_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let mut db = open_db(db_dir.path());
    db.create_collection("businesses").unwrap();
    db.insert(
        "businesses",
        bicdb_core::Record::new("b1").with_metadata(serde_json::json!({
            "state": "UK", "category": "yoga", "host": "h1", "score": 1.0,
        })),
    )
    .unwrap();
    let mut projection = projection();
    projection.catch_up(&db).unwrap();

    let mut last = 0;
    for _ in 0..5 {
        projection.save(state_dir.path(), true).unwrap();
        let loaded = AggregateProjection::load(state_dir.path(), "chaos")
            .unwrap()
            .unwrap();
        assert!(
            loaded.generation() > last,
            "generation did not advance: {} <= {last}",
            loaded.generation()
        );
        last = loaded.generation();
        projection = loaded;
    }
}
