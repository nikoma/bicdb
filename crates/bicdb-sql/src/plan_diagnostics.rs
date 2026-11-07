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

/// One single-table planning decision.
#[derive(Clone, Debug)]
pub struct DecisionTrace {
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
}

/// Options for one diagnostics scope.
#[derive(Clone, Debug, Default)]
pub struct DiagnosticsOptions {
    /// Select this candidate when the planner generated it.
    pub force: Option<PlanSignature>,
    /// Turn the deterministic contradiction detector off (for differential
    /// testing and for measuring its effect).
    pub disable_contradiction_detection: bool,
}

struct State {
    options: DiagnosticsOptions,
    decisions: Vec<DecisionTrace>,
}

thread_local! {
    static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
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
        PlanKind::IndexRange { index_name, .. } => ("IndexRange", Some(index_name), 0),
        PlanKind::OrderedIndexScan { index_name, .. } => {
            ("OrderedIndexScan", Some(index_name), 0)
        }
        PlanKind::IndexPrefixOrderedScan {
            index_name, prefix, ..
        } => ("IndexPrefixOrderedScan", Some(index_name), prefix.len()),
        PlanKind::SpatialIndexScan { index_name, .. } => {
            ("SpatialIndexScan", Some(index_name), 0)
        }
        PlanKind::GeometricIndexScan { index_name, .. } => {
            ("GeometricIndexScan", Some(index_name), 0)
        }
        PlanKind::GeometricKnnIndexScan { index_name, .. } => {
            ("GeometricKnnIndexScan", Some(index_name), 0)
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
            collection: collection.to_string(),
            total_rows,
            has_statistics,
            candidates: traces,
            chosen,
            forced: forced_index.is_some(),
            contradiction,
            rows_fetched: None,
            rows_matched: None,
        });
        chosen
    })
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
