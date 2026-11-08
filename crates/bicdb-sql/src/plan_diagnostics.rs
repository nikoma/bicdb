//! Opt-in single-table planner diagnostics for offline optimizer research.
//!
//! Everything here is inert unless an embedding Rust caller installs a scope
//! with [`with_plan_diagnostics`] on the current thread. Nothing in this module
//! is reachable from SQL text, GUCs, pgwire startup parameters, or environment
//! variables, so a production server can never start tracing or forcing plans.
//!
//! **What is recorded.** Only plan *structure* and *counts*: the plan kind, the
//! index identifier it uses, the length of an equality prefix, the planner's
//! estimated rows and cost for every legal candidate, the table's total row
//! estimate, and the number of rows the executed access path fetched and the
//! number that survived the WHERE clause. Literal values, bound parameters,
//! index keys, record ids and row contents are never captured — the
//! [`PlanSignature`] type cannot hold them.
//!
//! **Forcing.** A forced signature only *selects among the candidates the
//! planner already generated for that query*. Candidate generation keeps
//! enforcing every correctness gate (transaction write sets, RLS, index
//! readability, ordering support), and every executor path re-applies the full
//! WHERE clause to the fetched rows, so a forced plan changes cost, not
//! results. When no generated candidate matches, the normal choice is used and
//! the decision is reported as not forced.

use std::cell::RefCell;

use crate::engine::{PlanKind, QueryPlan};

/// Structural identity of a candidate plan. Deliberately value-free.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PlanSignature {
    /// Plan kind, e.g. `FullScan`, `IndexLookup`, `IndexRange`.
    pub kind: &'static str,
    /// Index identifier used by the access path, when there is one.
    pub index: Option<String>,
    /// Number of equality-prefix fields (`IndexLookup`,
    /// `IndexPrefixOrderedScan`) or literal ids (`PrimaryKeyInLookup`).
    pub prefix_len: usize,
}

impl PlanSignature {
    pub fn full_scan() -> Self {
        Self {
            kind: "FullScan",
            index: None,
            prefix_len: 0,
        }
    }

    /// Compact, value-free label such as `IndexLookup(orders_customer/1)`.
    pub fn label(&self) -> String {
        match &self.index {
            Some(index) => format!("{}({index}/{})", self.kind, self.prefix_len),
            None => format!("{}(/{})", self.kind, self.prefix_len),
        }
    }
}

/// One legal candidate the planner generated for a single-table query.
#[derive(Clone, Debug)]
pub struct CandidateTrace {
    pub signature: PlanSignature,
    pub estimated_rows: usize,
    pub estimated_cost: f64,
}

/// One single-table access-path decision.
#[derive(Clone, Debug)]
pub struct DecisionTrace {
    /// Which chooser decided: `"planner"` (the cost-based `plan_query`),
    /// `"locator"` (the rule-based index locator the row-set executor uses:
    /// most matched index columns wins, ties broken by probing; a full scan
    /// only when no index matches), or `"pending-writes"` (the record-id
    /// chooser used while the table has uncommitted writes in the
    /// transaction; same rule, but it probes every candidate).
    pub chooser: &'static str,
    /// Table identifier (schema metadata, not data).
    pub collection: String,
    /// Planner's total-row estimate for the table.
    pub total_rows: usize,
    /// Whether ANALYZE statistics existed when planning.
    pub has_statistics: bool,
    /// Every generated candidate in the planner's preference order.
    pub candidates: Vec<CandidateTrace>,
    /// Index into `candidates` of the plan that was returned.
    pub chosen: usize,
    /// True when `chosen` was selected by a forced signature.
    pub forced: bool,
    /// True when the deterministic contradiction detector proved the WHERE
    /// clause can never be TRUE.
    pub contradiction: bool,
    /// Rows the executed access path fetched (before the WHERE re-check),
    /// summed over executions of this decision. `None` if it never executed.
    pub rows_fetched: Option<usize>,
    /// Rows that survived the WHERE re-check, when the executing path counts
    /// them. `None` if not observed.
    pub rows_matched: Option<usize>,
    /// Wall time spent making this decision (planning, and for the locator
    /// the index probes it ran to choose), in microseconds.
    pub decision_micros: Option<u64>,
    /// Locator only: time spent probing tied candidates that were then NOT
    /// used, in microseconds: pure optimization overhead.
    pub wasted_probe_micros: Option<u64>,
    /// Locator only: the statistics-guided hybrid nominated a shorter
    /// candidate for an exact probe.
    pub hybrid_nominated: bool,
    /// Locator only: the nominee's probe won, replacing the rule's choice.
    pub hybrid_override: bool,
}

/// Options for one diagnostics scope.
#[derive(Clone, Debug, Default)]
pub struct DiagnosticsOptions {
    /// Select this candidate when the planner generated it.
    pub force: Option<PlanSignature>,
    /// Turn the deterministic contradiction detector off (for differential
    /// testing and for measuring its effect).
    pub disable_contradiction_detection: bool,
    /// Override the locator hybrid gate (`BICDB_LOCATOR_HYBRID`) for this
    /// scope: `Some(true)` on, `Some(false)` off, `None` follow the gate.
    pub locator_hybrid: Option<bool>,
}

struct State {
    options: DiagnosticsOptions,
    decisions: Vec<DecisionTrace>,
}

thread_local! {
    static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
    static STREAMING_DRY_RUN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Run `f` (the streaming routes, from EXPLAIN) with streamed scans in dry-run
/// mode: they report whether they would stream and read no rows.
pub(crate) fn with_streaming_dry_run<R>(f: impl FnOnce() -> R) -> R {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            STREAMING_DRY_RUN.with(|flag| flag.set(self.0));
        }
    }
    let _restore = Restore(STREAMING_DRY_RUN.with(|flag| flag.replace(true)));
    f()
}

#[inline]
pub(crate) fn streaming_dry_run() -> bool {
    STREAMING_DRY_RUN.with(|flag| flag.get())
}

/// Run `f` with planner diagnostics active on this thread and return its
/// result together with every single-table planning decision it made.
/// Scopes do not nest: an inner scope replaces the outer one for its duration
/// and the outer scope is restored afterwards.
pub fn with_plan_diagnostics<R>(
    options: DiagnosticsOptions,
    f: impl FnOnce() -> R,
) -> (R, Vec<DecisionTrace>) {
    let previous = STATE.with(|state| {
        state.borrow_mut().replace(State {
            options,
            decisions: Vec::new(),
        })
    });
    // Restores the outer scope even if `f` panics.
    struct Restore(Option<State>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let previous = self.0.take();
            STATE.with(|state| *state.borrow_mut() = previous);
        }
    }
    let _restore = Restore(previous);
    let result = f();
    let decisions = STATE.with(|state| {
        state
            .borrow_mut()
            .as_mut()
            .map(|state| std::mem::take(&mut state.decisions))
            .unwrap_or_default()
    });
    (result, decisions)
}

/// Run `f` under an internal capture scope that inherits the current scope's
/// options, returning the decisions it made. Used by EXPLAIN to learn the
/// executing route's choice from the same code that executes it.
pub(crate) fn capture<R>(f: impl FnOnce() -> R) -> (R, Vec<DecisionTrace>) {
    let options = STATE.with(|state| {
        state
            .borrow()
            .as_ref()
            .map(|state| state.options.clone())
            .unwrap_or_default()
    });
    with_plan_diagnostics(options, f)
}

#[inline]
pub(crate) fn active() -> bool {
    STATE.with(|state| state.borrow().is_some())
}

pub(crate) fn contradiction_detection_disabled() -> bool {
    STATE.with(|state| {
        state
            .borrow()
            .as_ref()
            .is_some_and(|state| state.options.disable_contradiction_detection)
    })
}

pub(crate) fn signature_of(kind: &PlanKind) -> PlanSignature {
    let (kind, index, prefix_len): (&'static str, Option<&String>, usize) = match kind {
        PlanKind::PrimaryKeyLookup { .. } => ("PrimaryKeyLookup", None, 1),
        PlanKind::PrimaryKeyInLookup { record_ids } => {
            ("PrimaryKeyInLookup", None, record_ids.len())
        }
        PlanKind::PrimaryKeyPrefixLookup { prefix_values, .. } => {
            ("PrimaryKeyPrefixLookup", None, prefix_values.len())
        }
        PlanKind::IndexLookup { index_name, prefix } => {
            ("IndexLookup", Some(index_name), prefix.len())
        }
        PlanKind::DeferredIndexLookup {
            index_name,
            prefix_exprs,
        } => ("IndexLookup", Some(index_name), prefix_exprs.len()),
        PlanKind::IndexRange { index_name, .. } => ("IndexRange", Some(index_name), 0),
        PlanKind::OrderedIndexScan { index_name, .. } => ("OrderedIndexScan", Some(index_name), 0),
        PlanKind::IndexPrefixOrderedScan {
            index_name, prefix, ..
        } => ("IndexPrefixOrderedScan", Some(index_name), prefix.len()),
        PlanKind::SpatialIndexScan { index_name, .. } => ("SpatialIndexScan", Some(index_name), 0),
        // The user-visible index name (what EXPLAIN prints), not the
        // per-column internal storage name.
        PlanKind::GeometricIndexScan { display_name, .. } => {
            ("GeometricIndexScan", Some(display_name), 0)
        }
        PlanKind::GeometricKnnIndexScan { display_name, .. } => {
            ("GeometricKnnIndexScan", Some(display_name), 0)
        }
        PlanKind::FullTextIndexScan { index_name, .. } => {
            ("FullTextIndexScan", Some(index_name), 0)
        }
        PlanKind::JsonbIndexScan { index_name, .. } => ("JsonbIndexScan", Some(index_name), 0),
        PlanKind::ArrayIndexScan { index_name, .. } => ("ArrayIndexScan", Some(index_name), 0),
        PlanKind::FullScan => ("FullScan", None, 0),
    };
    PlanSignature {
        kind,
        index: index.cloned(),
        prefix_len,
    }
}

/// Pick the plan to return from the planner's sorted candidate list, honoring
/// a forced signature, and record the decision. Only called while active.
pub(crate) fn choose_and_record(
    collection: &str,
    total_rows: usize,
    has_statistics: bool,
    contradiction: bool,
    candidates: &[QueryPlan],
) -> usize {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        let Some(state) = state.as_mut() else {
            return 0;
        };
        let traces = candidates
            .iter()
            .map(|plan| CandidateTrace {
                signature: signature_of(&plan.kind),
                estimated_rows: plan.estimated_rows,
                estimated_cost: plan.estimated_cost,
            })
            .collect::<Vec<_>>();
        let forced_index = state.options.force.as_ref().and_then(|wanted| {
            traces
                .iter()
                .position(|candidate| &candidate.signature == wanted)
        });
        let chosen = forced_index.unwrap_or(0);
        state.decisions.push(DecisionTrace {
            chooser: "planner",
            collection: collection.to_string(),
            total_rows,
            has_statistics,
            candidates: traces,
            chosen,
            forced: forced_index.is_some(),
            contradiction,
            rows_fetched: None,
            rows_matched: None,
            decision_micros: None,
            wasted_probe_micros: None,
            hybrid_nominated: false,
            hybrid_override: false,
        });
        chosen
    })
}

/// The forced signature, when a diagnostics scope with one is active.
pub(crate) fn forced_signature() -> Option<PlanSignature> {
    STATE.with(|state| {
        state
            .borrow()
            .as_ref()
            .and_then(|state| state.options.force.clone())
    })
}

/// Record a rule-based locator decision. `candidates` are value-free
/// signatures; for this chooser `estimated_rows`/`estimated_cost` are unused
/// (zero) and `prefix_len` is the number of matched index columns.
pub(crate) fn record_locator_decision(
    collection: &str,
    candidates: Vec<PlanSignature>,
    chosen: &PlanSignature,
    forced: bool,
    rows_fetched: Option<usize>,
) {
    record_decision_with(
        "locator",
        collection,
        candidates,
        chosen,
        forced,
        rows_fetched,
    );
}

/// Record a decision of another rule-based chooser (e.g. `"pending-writes"`,
/// the record-id path used while the table has uncommitted writes).
pub(crate) fn record_decision_for(
    chooser: &'static str,
    collection: &str,
    candidates: Vec<PlanSignature>,
    chosen: &PlanSignature,
    rows_fetched: Option<usize>,
) {
    record_decision_with(chooser, collection, candidates, chosen, false, rows_fetched);
}

fn record_decision_with(
    chooser: &'static str,
    collection: &str,
    candidates: Vec<PlanSignature>,
    chosen: &PlanSignature,
    forced: bool,
    rows_fetched: Option<usize>,
) {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        let Some(state) = state.as_mut() else {
            return;
        };
        let chosen = candidates
            .iter()
            .position(|candidate| candidate == chosen)
            .unwrap_or(0);
        state.decisions.push(DecisionTrace {
            chooser,
            collection: collection.to_string(),
            total_rows: 0,
            has_statistics: false,
            candidates: candidates
                .into_iter()
                .map(|signature| CandidateTrace {
                    signature,
                    estimated_rows: 0,
                    estimated_cost: 0.0,
                })
                .collect(),
            chosen,
            forced,
            contradiction: false,
            rows_fetched,
            rows_matched: None,
            decision_micros: None,
            wasted_probe_micros: None,
            hybrid_nominated: false,
            hybrid_override: false,
        });
    });
}

/// Annotate the most recent decision (timings, hybrid flag).
pub(crate) fn annotate_last_decision(update: impl FnOnce(&mut DecisionTrace)) {
    STATE.with(|state| {
        if let Some(decision) = state
            .borrow_mut()
            .as_mut()
            .and_then(|state| state.decisions.last_mut())
        {
            update(decision);
        }
    });
}

/// The scope's override of the locator hybrid gate, if any.
pub(crate) fn locator_hybrid_override() -> Option<bool> {
    STATE.with(|state| {
        state
            .borrow()
            .as_ref()
            .and_then(|state| state.options.locator_hybrid)
    })
}

/// Record that the executed chooser answered a provably contradictory WHERE
/// with an empty access path.
pub(crate) fn record_locator_contradiction(collection: &str) {
    record_locator_decision(
        collection,
        vec![PlanSignature {
            kind: "Contradiction",
            index: None,
            prefix_len: 0,
        }],
        &PlanSignature {
            kind: "Contradiction",
            index: None,
            prefix_len: 0,
        },
        false,
        Some(0),
    );
    STATE.with(|state| {
        if let Some(decision) = state
            .borrow_mut()
            .as_mut()
            .and_then(|state| state.decisions.last_mut())
        {
            decision.contradiction = true;
        }
    });
}

/// Attribute rows fetched by an executed access path to the latest decision.
pub(crate) fn record_rows_fetched(rows: usize) {
    STATE.with(|state| {
        if let Some(decision) = state
            .borrow_mut()
            .as_mut()
            .and_then(|state| state.decisions.last_mut())
        {
            *decision.rows_fetched.get_or_insert(0) += rows;
        }
    });
}

/// Attribute rows surviving the WHERE re-check to the latest decision.
pub(crate) fn record_rows_matched(rows: usize) {
    STATE.with(|state| {
        if let Some(decision) = state
            .borrow_mut()
            .as_mut()
            .and_then(|state| state.decisions.last_mut())
        {
            *decision.rows_matched.get_or_insert(0) += rows;
        }
    });
}
