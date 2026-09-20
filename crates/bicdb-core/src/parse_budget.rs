//! Host-defined budgets for parsing and decoding untrusted structure.
//!
//! Three engineering rules live here as code rather than as review habits,
//! because the alternative is rediscovering them one parser at a time:
//!
//! 1. **No remotely reachable parser may recurse according to
//!    attacker-controlled structure without a host-defined budget.** A
//!    recursive-descent parser over client text recurses once per nesting
//!    token; a few kilobytes of `(((((…` exhausts the thread stack. A stack
//!    overflow is a hardware fault, not a panic — `catch_unwind` cannot
//!    contain it, so a read-only query kills the entire process and every
//!    tenant in it.
//! 2. **No wire-provided count or index may directly determine an
//!    allocation.** A four-byte `0x7FFFFFFF` reaching `Vec::with_capacity`
//!    asks for tens of gigabytes; allocation failure aborts the process,
//!    which no `catch_unwind` intercepts either. Counts are bounded against
//!    the bytes that could possibly encode them before any reservation, and
//!    reservations that remain large use `try_reserve` so failure is an
//!    error value rather than an abort.
//! 3. A budget refusal is an ordinary error: the session sees a message and
//!    the process keeps serving everyone else.
//!
//! The budget is deliberately a plain counter rather than a guard object:
//! recursive-descent parsers need `&mut self` for the recursive call, which
//! a borrowing guard would block. Callers pair [`ParseBudget::enter`] with
//! [`ParseBudget::leave`] on the success path; on the error path the whole
//! parse unwinds, so an unbalanced counter cannot outlive the failure.

use crate::error::BicDbError;

/// A parse or decode that exceeded its host-defined budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BudgetExceeded {
    message: String,
}

impl BudgetExceeded {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for BudgetExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for BudgetExceeded {}

impl From<BudgetExceeded> for BicDbError {
    fn from(error: BudgetExceeded) -> Self {
        BicDbError::InvalidEvent(error.message)
    }
}

/// Nesting and node budget for one parse of untrusted input.
///
/// `label` names the grammar in refusal messages ("tsquery", "jsonpath", …)
/// so an operator can tell which input was rejected.
#[derive(Clone, Debug)]
pub struct ParseBudget {
    label: &'static str,
    depth: usize,
    max_depth: usize,
    nodes: usize,
    max_nodes: usize,
}

impl ParseBudget {
    /// Nesting depth allowed by default. Far above any human-written query
    /// and far below what the default 2 MiB thread stack tolerates: the
    /// shallowest recursive frame observed in these parsers is on the order
    /// of a hundred bytes, so a hundred levels costs single-digit kilobytes.
    pub const DEFAULT_MAX_DEPTH: usize = 100;
    /// Total nodes allowed by default, bounding breadth as depth bounds
    /// height — a flat list of ten million operands is also an attack.
    pub const DEFAULT_MAX_NODES: usize = 1_000_000;

    pub fn new(label: &'static str) -> Self {
        Self::with_limits(label, Self::DEFAULT_MAX_DEPTH, Self::DEFAULT_MAX_NODES)
    }

    pub fn with_limits(label: &'static str, max_depth: usize, max_nodes: usize) -> Self {
        Self {
            label,
            depth: 0,
            max_depth,
            nodes: 0,
            max_nodes,
        }
    }

    /// Descend one nesting level, refusing input nested past the budget.
    pub fn enter(&mut self) -> Result<(), BudgetExceeded> {
        self.depth += 1;
        if self.depth > self.max_depth {
            return Err(BudgetExceeded::new(format!(
                "{} input is nested deeper than {} levels",
                self.label, self.max_depth
            )));
        }
        Ok(())
    }

    /// Return from a nesting level entered with [`Self::enter`].
    pub fn leave(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }

    /// Account for one produced node, refusing inputs that are merely wide.
    pub fn charge_node(&mut self) -> Result<(), BudgetExceeded> {
        self.nodes += 1;
        if self.nodes > self.max_nodes {
            return Err(BudgetExceeded::new(format!(
                "{} input exceeds {} nodes",
                self.label, self.max_nodes
            )));
        }
        Ok(())
    }

    pub fn depth(&self) -> usize {
        self.depth
    }

    pub fn nodes(&self) -> usize {
        self.nodes
    }
}

/// Validate a wire-provided element count before it reaches an allocation.
///
/// `remaining_bytes` is what is left of the encoded input and
/// `min_item_bytes` the smallest number of bytes any single element can
/// occupy (at least one — an element that encodes to nothing is not an
/// element). A count larger than `remaining_bytes / min_item_bytes` cannot
/// be honest, whatever the sender claims, so it is refused before a single
/// byte is reserved. `hard_limit` caps the honest-but-enormous case.
pub fn checked_collection_capacity(
    label: &'static str,
    count: usize,
    remaining_bytes: usize,
    min_item_bytes: usize,
    hard_limit: usize,
) -> Result<usize, BudgetExceeded> {
    let encodable = remaining_bytes / min_item_bytes.max(1);
    if count > encodable {
        return Err(BudgetExceeded::new(format!(
            "{label} declares {count} elements but only {encodable} can be encoded \
             by the {remaining_bytes} bytes that follow"
        )));
    }
    if count > hard_limit {
        return Err(BudgetExceeded::new(format!(
            "{label} declares {count} elements, above the {hard_limit} element limit"
        )));
    }
    Ok(count)
}

/// Reserve capacity for `count` elements without risking an abort.
///
/// `Vec::with_capacity` aborts the process when the allocator refuses;
/// `try_reserve` returns an error the session can be told about. Use this
/// wherever the count originates outside the process, after bounding it with
/// [`checked_collection_capacity`].
pub fn try_reserve_exact<T>(
    label: &'static str,
    vector: &mut Vec<T>,
    count: usize,
) -> Result<(), BudgetExceeded> {
    vector.try_reserve_exact(count).map_err(|_| {
        BudgetExceeded::new(format!(
            "{label} needs {count} elements of memory that this host cannot provide"
        ))
    })
}

/// [`checked_collection_capacity`] followed by a fallible reservation.
pub fn bounded_vec<T>(
    label: &'static str,
    count: usize,
    remaining_bytes: usize,
    min_item_bytes: usize,
    hard_limit: usize,
) -> Result<Vec<T>, BudgetExceeded> {
    let count =
        checked_collection_capacity(label, count, remaining_bytes, min_item_bytes, hard_limit)?;
    let mut vector = Vec::new();
    try_reserve_exact(label, &mut vector, count)?;
    Ok(vector)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_budget_refuses_runaway_nesting_and_recovers() {
        let mut budget = ParseBudget::with_limits("test", 3, 100);
        for _ in 0..3 {
            budget.enter().expect("within budget");
        }
        let error = budget.enter().expect_err("past budget");
        assert!(error.message().contains("nested deeper than 3"));
        // Leaving restores headroom: a sibling branch of legal depth still
        // parses after a deep one was refused.
        for _ in 0..4 {
            budget.leave();
        }
        assert_eq!(budget.depth(), 0);
        budget.enter().expect("budget is reusable");
    }

    #[test]
    fn node_budget_bounds_breadth() {
        let mut budget = ParseBudget::with_limits("test", 100, 2);
        budget.charge_node().unwrap();
        budget.charge_node().unwrap();
        assert!(budget.charge_node().is_err());
    }

    #[test]
    fn counts_are_bounded_by_what_the_input_could_encode() {
        // The attack: four bytes claiming two billion elements.
        let error = checked_collection_capacity("tsvector", 0x7FFF_FFFF, 8, 1, usize::MAX)
            .expect_err("count cannot exceed the bytes that follow");
        assert!(error.message().contains("only 8 can be encoded"));
        // Honest counts pass untouched.
        assert_eq!(
            checked_collection_capacity("tsvector", 4, 64, 3, 1_000).unwrap(),
            4
        );
        // Multi-byte elements tighten the bound.
        assert!(checked_collection_capacity("tsvector", 20, 30, 3, 1_000).is_err());
        // The hard limit catches encodable-but-absurd counts.
        assert!(checked_collection_capacity("tsvector", 5_000, 100_000, 1, 1_000).is_err());
    }

    #[test]
    fn bounded_vec_refuses_before_reserving() {
        assert!(bounded_vec::<u64>("test", usize::MAX / 8, 16, 1, usize::MAX).is_err());
        let vector = bounded_vec::<u64>("test", 4, 64, 1, 1_000).unwrap();
        assert!(vector.capacity() >= 4);
    }
}
