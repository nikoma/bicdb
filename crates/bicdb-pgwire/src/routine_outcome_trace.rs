//! Diagnostic server-side latency, including execution, rollback, commit and
//! WAL durability. Excludes outer query admission and network transmission.
use bicdb_sql::{
    routine_outcome_counters_snapshot, routine_outcome_trace_enabled, RoutineOutcomeCounters,
};
use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

pub(crate) struct RoutineOutcomeTrace<'a> {
    name: &'a str,
    started: Instant,
    before: RoutineOutcomeCounters,
}

#[derive(Default)]
struct Totals {
    calls: u64,
    elapsed_ns: u128,
    max_ns: u64,
    discarded_writes: u64,
    rollback_ns: u64,
}

#[derive(Default)]
struct TraceState {
    totals: BTreeMap<String, BTreeMap<&'static str, Totals>>,
    last_dump: Option<Instant>,
}

impl<'a> RoutineOutcomeTrace<'a> {
    pub(crate) fn start(normalized: &'a str) -> Option<Self> {
        if !routine_outcome_trace_enabled() || !normalized.starts_with("call ") {
            return None;
        }
        Some(Self {
            name: crate::query_exec::top_level_proc_name(normalized)?,
            started: Instant::now(),
            before: routine_outcome_counters_snapshot(),
        })
    }

    pub(crate) fn finish(self, result_ok: bool) {
        let elapsed_ns = self.started.elapsed().as_nanos();
        let after = routine_outcome_counters_snapshot();
        let outcome = if !result_ok {
            "error"
        } else if after.serialization > self.before.serialization
            || after.deadlock > self.before.deadlock
        {
            "handled_conflict"
        } else if after.no_data > self.before.no_data {
            "handled_no_data"
        } else if after.other > self.before.other {
            "handled_other"
        } else {
            "ok_no_handler"
        };
        static STATE: OnceLock<Mutex<TraceState>> = OnceLock::new();
        let mut state = STATE
            .get_or_init(|| Mutex::new(TraceState::default()))
            .lock()
            .unwrap();
        if !state.totals.contains_key(self.name) {
            state.totals.insert(self.name.to_string(), BTreeMap::new());
        }
        let totals = state
            .totals
            .get_mut(self.name)
            .unwrap()
            .entry(outcome)
            .or_default();
        totals.calls += 1;
        totals.elapsed_ns += elapsed_ns;
        totals.max_ns = totals
            .max_ns
            .max(elapsed_ns.min(u128::from(u64::MAX)) as u64);
        totals.discarded_writes += after
            .discarded_writes
            .saturating_sub(self.before.discarded_writes);
        totals.rollback_ns += after.rollback_ns.saturating_sub(self.before.rollback_ns);
        let now = Instant::now();
        let last_dump = state.last_dump.get_or_insert(now);
        if now.duration_since(*last_dump).as_secs() < 5 {
            return;
        }
        state.last_dump = Some(now);
        for (name, outcomes) in &state.totals {
            for (outcome, totals) in outcomes {
                eprintln!("ROUTINE_OUTCOME proc={name} outcome={outcome} calls={} total_ns={} mean_us={} max_us={} discarded_writes={} rollback_ns={}",
                    totals.calls, totals.elapsed_ns, totals.elapsed_ns / u128::from(totals.calls) / 1000,
                    totals.max_ns / 1000, totals.discarded_writes, totals.rollback_ns);
            }
        }
    }
}
