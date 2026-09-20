//! A page is the atomic unit of bounded vacuum work.
//!
//! `VacuumCursor` is page-granular — it can say "resume at page N" and nothing
//! finer. A byte envelope that could interrupt work *inside* a page therefore
//! produced a state the cursor could not represent, and the only encoding
//! available was to point back at the page just entered, so the next step
//! redid it. Forever.
//!
//! The contract these tests pin down:
//!
//! * the byte envelope is checked **between** pages;
//! * once a page is entered it runs to completion, overshooting the budget by
//!   at most one page's work;
//! * the cursor advances monotonically;
//! * an unchanged cursor after entering a page is an **error**, not a success
//!   with suspicious counters.

use bicdb_core::{
    BicDb, DbConfig, PagedVacuumScheduleAdvance, PagedVacuumScheduleLimits, Record,
    ResourceGovernor, ResourceGovernorConfig, StorageMode, VacuumLimits,
};
use serde_json::json;

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

/// `max_bytes` deliberately far below the cost of a single page.
fn starved_steps(max_pages: u64) -> PagedVacuumScheduleLimits {
    let vacuum = VacuumLimits {
        max_pages,
        max_bytes: 2 * 1024 * 1024,
        max_duration_millis: 1_000,
    };
    PagedVacuumScheduleLimits {
        vacuum,
        step_interval_ms: 1,
        saturation_retry_ms: 1,
        failure_retry_base_ms: 1,
        failure_retry_max_ms: 8,
        max_consecutive_failures: 4,
        max_schedule_state_bytes: 64 * 1024,
        demand: bicdb_core::ResourceDemand {
            memory_bytes: 4 * 1024 * 1024,
            io_bytes: vacuum.max_bytes,
            cpu_slots: 1,
            io_charge_bytes: vacuum.max_bytes,
        },
    }
}

/// `rows` records of `payload_bytes` each, then deleted — dead versions for
/// vacuum to reclaim.
fn seeded(rows: usize, payload_bytes: usize) -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
    db.create_collection("events").unwrap();
    let payload = "x".repeat(payload_bytes);
    let ids: Vec<String> = (0..rows).map(|index| format!("row-{index:04}")).collect();
    {
        let mut tx = db.begin_transaction().unwrap();
        tx.batch_insert(
            "events",
            ids.iter().map(|id| {
                Record::new(id.clone()).with_metadata(json!({ "payload": payload.clone() }))
            }),
        )
        .unwrap();
        tx.commit().unwrap();
    }
    {
        let mut tx = db.begin_transaction().unwrap();
        tx.delete_many("events", &ids).unwrap();
        tx.commit().unwrap();
    }
    db.flush().unwrap();
    (root, db)
}

struct Sweep {
    cursors: Vec<Option<u64>>,
    steps: usize,
    completed: bool,
    reclaimed_per_page: Vec<(Option<u64>, u64)>,
}

/// Drive the supervisor to completion (or to `max_steps`), recording the
/// cursor at every step.
fn drive(db: &mut BicDb, limits: PagedVacuumScheduleLimits, max_steps: usize) -> Sweep {
    let schedule = db.start_paged_vacuum_maintenance(100, limits).unwrap();
    let operation_id = schedule.operation_id;
    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
    let mut now = 100;
    let mut sweep = Sweep {
        cursors: Vec::new(),
        steps: 0,
        completed: false,
        reclaimed_per_page: Vec::new(),
    };
    for _ in 0..max_steps {
        let outcome = db
            .tick_paged_vacuum_maintenance(operation_id, &governor, now)
            .unwrap();
        sweep.steps += 1;
        let durable = db.paged_vacuum_maintenance_status().unwrap().unwrap();
        if let PagedVacuumScheduleAdvance::Progress { report, .. } = &outcome {
            sweep
                .reclaimed_per_page
                .push((report.start_page.map(u64::from), report.versions_reclaimed));
        }
        sweep
            .cursors
            .push(durable.cursor.next_page_id.map(u64::from));
        if matches!(outcome, PagedVacuumScheduleAdvance::Complete { .. }) {
            sweep.completed = true;
            break;
        }
        match durable.next_attempt_at_ms {
            Some(next) => now = next,
            None => break,
        }
    }
    sweep
}

/// A budget smaller than the cost of the first page must still complete that
/// page and move on. This is the exact shape of the original hang.
#[test]
fn a_budget_smaller_than_the_first_page_still_advances() {
    let (_root, mut db) = seeded(600, 4 * 1024);
    let sweep = drive(&mut db, starved_steps(1), 500);

    assert!(
        sweep.completed,
        "sweep did not finish in {} steps; cursors: {:?}",
        sweep.steps,
        &sweep.cursors[..sweep.cursors.len().min(12)]
    );
    assert!(
        sweep.steps < 100,
        "sweep took {} steps, which suggests it is re-doing work",
        sweep.steps
    );
}

/// The cursor must advance monotonically and never repeat a page.
#[test]
fn the_cursor_advances_monotonically_and_never_repeats() {
    let (_root, mut db) = seeded(600, 4 * 1024);
    let sweep = drive(&mut db, starved_steps(1), 500);
    assert!(sweep.completed);

    // Ignore the terminal `None` that marks a finished sweep.
    let pages: Vec<u64> = sweep.cursors.iter().filter_map(|page| *page).collect();
    for pair in pages.windows(2) {
        assert!(
            pair[1] > pair[0],
            "cursor went {} -> {} (must strictly advance); full trace {:?}",
            pair[0],
            pair[1],
            pages
        );
    }
    let mut unique = pages.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        unique.len(),
        pages.len(),
        "a page was revisited; cursor trace {pages:?}"
    );
}

/// No page may have its versions reclaimed more than once across the sweep.
/// This is the assertion that would have caught `versions_reclaimed = 61,000`
/// on a 600-row table.
#[test]
fn no_page_is_reclaimed_twice() {
    let (_root, mut db) = seeded(600, 4 * 1024);
    let sweep = drive(&mut db, starved_steps(1), 500);
    assert!(sweep.completed);

    let mut seen: Vec<Option<u64>> = Vec::new();
    for (page, reclaimed) in &sweep.reclaimed_per_page {
        if *reclaimed == 0 {
            continue;
        }
        assert!(
            !seen.contains(page),
            "page {page:?} reclaimed versions on two different steps: {:?}",
            sweep.reclaimed_per_page
        );
        seen.push(*page);
    }

    let total: u64 = sweep
        .reclaimed_per_page
        .iter()
        .map(|(_, reclaimed)| reclaimed)
        .sum();
    assert!(
        total <= 600 * 2,
        "reclaimed {total} versions from a 600-row table — the same work is \
         being counted repeatedly"
    );
}

/// A full sweep terminates at `None`.
#[test]
fn a_full_sweep_reaches_the_end() {
    let (_root, mut db) = seeded(400, 2 * 1024);
    let sweep = drive(&mut db, starved_steps(1), 500);
    assert!(sweep.completed);
    assert_eq!(
        sweep.cursors.last().copied().flatten(),
        None,
        "a finished sweep must end with an empty cursor"
    );
}

/// A generous page budget stops between pages rather than mid-page, so the
/// byte envelope still bounds a step — it just cannot strand one.
#[test]
fn a_generous_page_budget_still_stops_between_pages() {
    let (_root, mut db) = seeded(600, 4 * 1024);
    let sweep = drive(&mut db, starved_steps(2), 500);
    assert!(sweep.completed);
    let pages: Vec<u64> = sweep.cursors.iter().filter_map(|page| *page).collect();
    for pair in pages.windows(2) {
        assert!(pair[1] > pair[0], "cursor trace {pages:?}");
    }
}

/// One pathological page with very long version chains: the step overshoots
/// the byte budget to finish it, and advances.
#[test]
fn a_pathological_page_finishes_and_advances() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
    db.create_collection("events").unwrap();
    // Rewrite the same small key many times: one page, a very long chain.
    for round in 0..400 {
        let mut tx = db.begin_transaction().unwrap();
        tx.insert(
            "events",
            Record::new("hot").with_metadata(json!({ "round": round })),
        )
        .unwrap();
        tx.commit().unwrap();
    }
    {
        let mut tx = db.begin_transaction().unwrap();
        tx.delete_many("events", &["hot".to_string()]).unwrap();
        tx.commit().unwrap();
    }
    db.flush().unwrap();

    let sweep = drive(&mut db, starved_steps(1), 500);
    assert!(
        sweep.completed,
        "a single long-chain page stranded the sweep; cursors {:?}",
        sweep.cursors
    );
}

/// A tiny budget still terminates rather than spinning.
#[test]
fn a_tiny_budget_still_terminates() {
    let (_root, mut db) = seeded(300, 4 * 1024);
    let mut limits = starved_steps(1);
    // The smallest envelope the validator accepts.
    limits.vacuum.max_bytes = 64 * 1024;
    limits.demand.io_bytes = limits.vacuum.max_bytes;
    limits.demand.io_charge_bytes = limits.vacuum.max_bytes;
    let sweep = drive(&mut db, limits, 500);
    assert!(
        sweep.completed,
        "a tiny budget did not terminate; cursors {:?}",
        &sweep.cursors[..sweep.cursors.len().min(12)]
    );
}

/// Vacuum still actually reclaims, and a completed sweep leaves nothing a
/// second sweep can pick up. Asserting a specific count would encode
/// assumptions about heap layout; asserting that the sweep is a FIXED POINT
/// is the property that actually matters.
#[test]
fn a_completed_sweep_is_a_fixed_point() {
    let (_root, mut db) = seeded(600, 4 * 1024);
    let first = drive(&mut db, starved_steps(1), 500);
    assert!(first.completed);
    let first_total: u64 = first
        .reclaimed_per_page
        .iter()
        .map(|(_, reclaimed)| reclaimed)
        .sum();
    assert!(first_total > 0, "the sweep reclaimed nothing at all");

    // Re-run from scratch: whatever the first sweep could reclaim, it did.
    let second = drive(&mut db, starved_steps(1), 500);
    assert!(second.completed);
    let second_total: u64 = second
        .reclaimed_per_page
        .iter()
        .map(|(_, reclaimed)| reclaimed)
        .sum();
    assert_eq!(
        second_total, 0,
        "a second sweep reclaimed {second_total} more versions after the first \
         reported Complete, so the first sweep silently skipped work"
    );
}
