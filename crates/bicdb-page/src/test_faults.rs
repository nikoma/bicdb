//! Deterministic crash injection for durability tests.
//!
//! `BICDB_TEST_CRASH_AT=<point>:<n>` aborts the process at the `n`-th time
//! (1-based, default 1) execution reaches [`crash_point`] with that name. The
//! abort is immediate: no destructors, no flushes, exactly like a kill.
//!
//! `BICDB_TEST_WAL_VOLATILE_TAIL=1` additionally emulates **power loss** for
//! the page WAL at that moment: before aborting, every registered log's active
//! file is truncated back to the offset its last successful sync covered. A
//! kill alone keeps unsynced bytes in the page cache; a power cut may not, and
//! this discards exactly the bytes a power cut is allowed to discard.
//!
//! Both variables are read once. With neither set, [`crash_point`] is one
//! relaxed load.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::wal::Wal;

struct CrashPlan {
    point: String,
    hit: u64,
    seen: AtomicU64,
    volatile_tail: bool,
}

fn plan() -> Option<&'static CrashPlan> {
    static PLAN: OnceLock<Option<CrashPlan>> = OnceLock::new();
    PLAN.get_or_init(|| {
        let spec = std::env::var("BICDB_TEST_CRASH_AT").ok()?;
        let (point, hit) = match spec.rsplit_once(':') {
            Some((point, hit)) => (point.to_string(), hit.parse().unwrap_or(1)),
            None => (spec, 1),
        };
        Some(CrashPlan {
            point,
            hit: hit.max(1),
            seen: AtomicU64::new(0),
            volatile_tail: volatile_tail_enabled(),
        })
    })
    .as_ref()
}

/// Whether page-WAL power-loss emulation is requested for this process.
pub fn volatile_tail_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_TEST_WAL_VOLATILE_TAIL").is_ok_and(|value| value == "1")
    })
}

fn registry() -> &'static Mutex<Vec<Weak<Wal>>> {
    static WALS: OnceLock<Mutex<Vec<Weak<Wal>>>> = OnceLock::new();
    WALS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Track `wal` so a crash point can discard its unsynced tail.
pub(crate) fn register_wal(wal: &Arc<Wal>) {
    if volatile_tail_enabled() {
        let mut wals = registry().lock().unwrap_or_else(|e| e.into_inner());
        wals.retain(|wal| wal.strong_count() > 0);
        wals.push(Arc::downgrade(wal));
    }
}

/// Whether the next [`crash_point`] call for `point` will abort, so a caller
/// can stage a partial write right before it.
pub fn crash_point_armed(point: &str) -> bool {
    plan().is_some_and(|plan| {
        plan.point == point && plan.seen.load(Ordering::Acquire) + 1 == plan.hit
    })
}

/// Abort here if `BICDB_TEST_CRASH_AT` names this point and its hit count is
/// reached.
pub fn crash_point(point: &str) {
    let Some(plan) = plan() else {
        return;
    };
    if plan.point != point {
        return;
    }
    if plan.seen.fetch_add(1, Ordering::AcqRel) + 1 != plan.hit {
        return;
    }
    crash_now(plan.volatile_tail);
}

/// Abort the process, first discarding unsynced page-WAL bytes when power
/// loss emulation is on.
pub fn crash_now(volatile_tail: bool) -> ! {
    if volatile_tail {
        let wals = registry().lock().unwrap_or_else(|e| e.into_inner());
        for wal in wals.iter().filter_map(Weak::upgrade) {
            wal.discard_unsynced_tail_for_test();
        }
    }
    eprintln!("bicdb test crash point reached");
    std::process::abort();
}
