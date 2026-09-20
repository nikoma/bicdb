//! Opt-in, thread-local exception/rollback accounting for synchronous callers.
//! No query text, arguments, or customer data are retained.
use std::cell::Cell;
use std::sync::OnceLock;

#[derive(Clone, Copy, Debug, Default)]
pub struct RoutineOutcomeCounters {
    pub serialization: u64,
    pub deadlock: u64,
    pub no_data: u64,
    pub other: u64,
    pub discarded_writes: u64,
    pub rollback_ns: u64,
}

thread_local! {
    static COUNTERS: Cell<RoutineOutcomeCounters> = const { Cell::new(RoutineOutcomeCounters {
        serialization: 0, deadlock: 0, no_data: 0, other: 0,
        discarded_writes: 0, rollback_ns: 0,
    }) };
}

pub fn routine_outcome_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_ROUTINE_OUTCOME_TRACE")
            .is_ok_and(|value| matches!(value.as_str(), "1" | "on" | "true"))
    })
}

/// Read before/after one synchronous execution on the same OS thread. Unlike
/// global exception counters, another connection cannot contaminate the delta.
pub fn routine_outcome_counters_snapshot() -> RoutineOutcomeCounters {
    COUNTERS.with(Cell::get)
}

pub(crate) fn record_routine_rollback(sqlstate: &str, discarded_writes: usize, rollback_ns: u64) {
    COUNTERS.with(|cell| {
        let mut counts = cell.get();
        let failures = match sqlstate {
            "40001" => &mut counts.serialization,
            "40P01" => &mut counts.deadlock,
            "P0002" => &mut counts.no_data,
            _ => &mut counts.other,
        };
        *failures = failures.saturating_add(1);
        counts.discarded_writes = counts
            .discarded_writes
            .saturating_add(discarded_writes as u64);
        counts.rollback_ns = counts.rollback_ns.saturating_add(rollback_ns);
        cell.set(counts);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollback_counts_are_thread_local_and_separate_causes() {
        let before = routine_outcome_counters_snapshot();
        std::thread::spawn(|| record_routine_rollback("40001", 99, 1000))
            .join()
            .unwrap();
        assert_eq!(
            routine_outcome_counters_snapshot().serialization,
            before.serialization
        );
        record_routine_rollback("40001", 3, 17);
        record_routine_rollback("P0002", 0, 2);
        let after = routine_outcome_counters_snapshot();
        assert_eq!(after.serialization - before.serialization, 1);
        assert_eq!(after.no_data - before.no_data, 1);
        assert_eq!(after.discarded_writes - before.discarded_writes, 3);
        assert_eq!(after.rollback_ns - before.rollback_ns, 19);
    }
}
