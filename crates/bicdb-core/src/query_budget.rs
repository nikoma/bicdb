//! Cooperative query-memory envelopes, scoped to the executing thread.
use crate::{BicDbError, Result};
use std::cell::RefCell;
use std::marker::PhantomData;

#[derive(Clone, Copy)]
struct Budget {
    limit: usize,
    peak: usize,
    deadline: Option<std::time::Instant>,
}
thread_local! { static BUDGETS: RefCell<Vec<Budget>> = const { RefCell::new(Vec::new()) }; }

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
        BUDGETS.with(|b| b.borrow().last().map_or(0, |b| b.peak))
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
            .find(|b| b.limit > 0 && b.peak > b.limit)
        {
            Err(BicDbError::QueryMemoryLimit(format!(
                "query materialized {} bytes; limit is {}",
                budget.peak, budget.limit
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
    let size = record
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
        .saturating_add(std::mem::size_of::<crate::Record>() + 128);
    *bytes = bytes.saturating_add(size);
    observe(*bytes);
    check()
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
