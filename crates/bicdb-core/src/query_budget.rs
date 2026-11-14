//! Cooperative query-memory envelopes, scoped to the executing thread.
use crate::{BicDbError, Result};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
struct Budget {
    limit: usize,
    peak: usize,
    shared_peak: Option<Arc<AtomicUsize>>,
    deadline: Option<std::time::Instant>,
}
thread_local! { static BUDGETS: RefCell<Vec<Budget>> = const { RefCell::new(Vec::new()) }; }

impl Budget {
    fn peak(&self) -> usize {
        self.shared_peak.as_ref().map_or(self.peak, |shared| {
            self.peak.max(shared.load(Ordering::Acquire))
        })
    }
}

/// Local numeric execution context, never database or distributed authority.
/// Capturing shares only peak counters; guards remain on their own threads.
#[derive(Clone)]
pub(crate) struct QueryBudgetContext {
    budgets: Vec<Budget>,
}
impl QueryBudgetContext {
    pub(crate) fn capture() -> Self {
        BUDGETS.with(|stack| {
            let mut stack = stack.borrow_mut();
            for budget in stack.iter_mut() {
                let peak = budget.peak;
                budget
                    .shared_peak
                    .get_or_insert_with(|| Arc::new(AtomicUsize::new(peak)));
            }
            Self {
                budgets: stack.clone(),
            }
        })
    }
    pub(crate) fn deadline(&self) -> Option<std::time::Instant> {
        self.budgets
            .iter()
            .filter_map(|budget| budget.deadline)
            .min()
    }
    pub(crate) fn install(&self) -> InstalledQueryBudgetContext {
        let base = BUDGETS.with(|stack| {
            let mut stack = stack.borrow_mut();
            let base = stack.len();
            stack.extend(self.budgets.iter().cloned());
            base
        });
        InstalledQueryBudgetContext {
            base,
            _thread: PhantomData,
        }
    }
}

pub(crate) struct InstalledQueryBudgetContext {
    base: usize,
    _thread: PhantomData<*mut ()>,
}
impl Drop for InstalledQueryBudgetContext {
    fn drop(&mut self) {
        BUDGETS.with(|stack| stack.borrow_mut().truncate(self.base));
    }
}

/// Nested statements inherit the tightest envelope. The guard cannot move to
/// another thread; workers must explicitly install their own envelope.
pub struct QueryMemoryScope {
    _thread: PhantomData<*mut ()>,
}
impl QueryMemoryScope {
    pub fn new(limit: usize) -> Self {
        BUDGETS.with(|budgets| {
            budgets.borrow_mut().push(Budget {
                limit,
                peak: 0,
                shared_peak: None,
                deadline: None,
            })
        });
        Self {
            _thread: PhantomData,
        }
    }
    pub fn with_deadline(limit: usize, deadline: std::time::Instant) -> Self {
        let scope = Self::new(limit);
        BUDGETS.with(|b| b.borrow_mut().last_mut().unwrap().deadline = Some(deadline));
        scope
    }
    pub fn peak_bytes(&self) -> usize {
        BUDGETS.with(|b| b.borrow().last().map_or(0, Budget::peak))
    }
}
impl Drop for QueryMemoryScope {
    fn drop(&mut self) {
        BUDGETS.with(|b| {
            b.borrow_mut().pop();
        });
    }
}

pub fn active() -> bool {
    BUDGETS.with(|b| b.borrow().iter().any(|b| b.limit > 0))
}
/// Read-only diagnostic scalar; unlike context capture this shares no counters.
#[cfg(test)]
pub(crate) fn test_current_deadline() -> Option<std::time::Instant> {
    BUDGETS.with(|budgets| budgets.borrow().iter().filter_map(|b| b.deadline).min())
}
pub fn limit(default: usize) -> usize {
    BUDGETS.with(|b| {
        b.borrow()
            .iter()
            .filter(|b| b.limit > 0)
            .fold(default, |limit, b| limit.min(b.limit))
    })
}
pub fn observe(bytes: usize) {
    BUDGETS.with(|budgets| {
        for budget in budgets.borrow_mut().iter_mut() {
            budget.peak = budget.peak.max(bytes);
            if let Some(shared) = &budget.shared_peak {
                shared.fetch_max(bytes, Ordering::AcqRel);
            }
        }
    });
}
pub fn check() -> Result<()> {
    BUDGETS.with(|budgets| {
        if budgets.borrow().iter().any(|b| {
            b.deadline
                .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        }) {
            return Err(BicDbError::QueryTimedOut);
        }
        if let Some(budget) = budgets
            .borrow()
            .iter()
            .find(|b| b.limit > 0 && b.peak() > b.limit)
        {
            Err(BicDbError::QueryMemoryLimit(format!(
                "query materialized {} bytes; limit is {}",
                budget.peak(),
                budget.limit
            )))
        } else {
            Ok(())
        }
    })
}

/// Check a scan before retaining another materialized record. This is a
/// conservative working-set estimate, including JSON's tree overhead.
pub(crate) fn retain_record(bytes: &mut usize, record: &crate::Record) -> Result<()> {
    if !active() {
        return Ok(());
    }
    let size = materialized_record_bytes(record)?;
    *bytes = bytes.saturating_add(size);
    observe(*bytes);
    check()
}

/// The existing cooperative record estimate, also usable by bounded caches.
/// This estimates one materialized body; Arc references do not duplicate it.
pub(crate) fn materialized_record_bytes(record: &crate::Record) -> Result<usize> {
    Ok(record
        .id
        .len()
        .saturating_add(
            serde_json::to_vec(&record.metadata)?
                .len()
                .saturating_mul(8),
        )
        .saturating_add(record.payload.as_ref().map_or(0, Vec::len))
        .saturating_add(
            record
                .vector
                .as_ref()
                .map_or(0, |v| v.len().saturating_mul(4)),
        )
        .saturating_add(std::mem::size_of::<crate::Record>() + 128))
}

pub fn parse_duration(value: &str) -> Option<std::time::Duration> {
    let value = value.trim();
    let (number, scale) = if let Some(number) = value.strip_suffix("ms") {
        (number, 0.001)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1.0)
    } else if let Some(number) = value.strip_suffix("min") {
        (number, 60.0)
    } else {
        (value, 1.0)
    };
    let seconds = number.trim().parse::<f64>().ok()? * scale;
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    std::time::Duration::try_from_secs_f64(seconds).ok()
}

#[cfg(test)]
mod worker_context_tests;
