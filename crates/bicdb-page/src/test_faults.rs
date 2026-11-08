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
//!
//! `BICDB_TEST_FAIL_IO=<site>:<n>[+][,<site>:<n>[+]...]` makes the `n`-th
//! call (1-based) of the named I/O site return `EIO` instead of doing the
//! I/O. A trailing `+` keeps failing every later call too (a dead disk);
//! without it only the `n`-th call fails (the "fsync failed once, then
//! succeeded" pattern that fail-closed handling exists for). In-process tests
//! use [`arm_io_failure`] instead, scoped to their own directory so parallel
//! tests do not see each other's faults. With nothing armed, [`io_fault`] is
//! one atomic load. Site names are listed in
//! `docs/durability-failure-handling.md`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

struct IoFaultPlan {
    site: String,
    /// Only calls on paths under this directory count. `None`: every path.
    scope: Option<PathBuf>,
    nth: u64,
    persistent: bool,
    seen: AtomicU64,
    fired: AtomicU64,
}

fn io_fault_plans() -> &'static Mutex<Vec<Arc<IoFaultPlan>>> {
    static PLANS: OnceLock<Mutex<Vec<Arc<IoFaultPlan>>>> = OnceLock::new();
    PLANS.get_or_init(|| {
        let mut plans = Vec::new();
        if let Ok(spec) = std::env::var("BICDB_TEST_FAIL_IO") {
            for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
                let (site, count) = entry.rsplit_once(':').unwrap_or((entry, "1"));
                let persistent = count.ends_with('+');
                let nth = count.trim_end_matches('+').parse().unwrap_or(1u64).max(1);
                plans.push(Arc::new(IoFaultPlan {
                    site: site.to_string(),
                    scope: None,
                    nth,
                    persistent,
                    seen: AtomicU64::new(0),
                    fired: AtomicU64::new(0),
                }));
            }
        }
        IO_FAULTS_ARMED.store(!plans.is_empty(), Ordering::Release);
        Mutex::new(plans)
    })
}

static IO_FAULTS_ARMED: AtomicBool = AtomicBool::new(false);

/// Removes its fault plan when dropped.
pub struct IoFaultGuard {
    plan: Arc<IoFaultPlan>,
}

impl IoFaultGuard {
    /// How many calls of the site (within scope) failed so far.
    pub fn fired(&self) -> u64 {
        self.plan.fired.load(Ordering::Acquire)
    }

    /// How many calls of the site (within scope) were seen so far.
    pub fn seen(&self) -> u64 {
        self.plan.seen.load(Ordering::Acquire)
    }
}

impl Drop for IoFaultGuard {
    fn drop(&mut self) {
        let mut plans = io_fault_plans().lock().unwrap_or_else(|e| e.into_inner());
        plans.retain(|plan| !Arc::ptr_eq(plan, &self.plan));
        IO_FAULTS_ARMED.store(!plans.is_empty(), Ordering::Release);
    }
}

/// Make the `nth` call (1-based) of `site` on a path under `scope` fail with
/// `EIO`; with `persistent`, every later call fails too.
pub fn arm_io_failure(site: &str, scope: &Path, nth: u64, persistent: bool) -> IoFaultGuard {
    let plan = Arc::new(IoFaultPlan {
        site: site.to_string(),
        scope: Some(scope.to_path_buf()),
        nth: nth.max(1),
        persistent,
        seen: AtomicU64::new(0),
        fired: AtomicU64::new(0),
    });
    let mut plans = io_fault_plans().lock().unwrap_or_else(|e| e.into_inner());
    plans.push(plan.clone());
    IO_FAULTS_ARMED.store(true, Ordering::Release);
    IoFaultGuard { plan }
}

/// Injection point for a durability-critical I/O call named `site` on
/// `path`. Returns `EIO` when an armed plan says this call fails.
pub fn io_fault(site: &str, path: &Path) -> std::io::Result<()> {
    // Parses `BICDB_TEST_FAIL_IO` on first use; afterwards one atomic load.
    let plans = io_fault_plans();
    if !IO_FAULTS_ARMED.load(Ordering::Acquire) {
        return Ok(());
    }
    let plans = plans.lock().unwrap_or_else(|e| e.into_inner());
    for plan in plans.iter() {
        if plan.site != site {
            continue;
        }
        if let Some(scope) = &plan.scope {
            if !path.starts_with(scope) {
                continue;
            }
        }
        let call = plan.seen.fetch_add(1, Ordering::AcqRel) + 1;
        if call == plan.nth || (plan.persistent && call > plan.nth) {
            plan.fired.fetch_add(1, Ordering::AcqRel);
            return Err(std::io::Error::from_raw_os_error(libc::EIO));
        }
    }
    Ok(())
}
