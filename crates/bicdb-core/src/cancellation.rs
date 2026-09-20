use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::error::{BicDbError, Result};

#[derive(Clone, Debug)]
pub struct CancellationToken {
    cancel_requested: Arc<AtomicBool>,
    upstream_cancel_requests: Arc<[Arc<AtomicBool>]>,
    deadline: Option<Instant>,
}

impl CancellationToken {
    pub fn new(cancel_requested: Arc<AtomicBool>, deadline: Option<Instant>) -> Self {
        Self {
            cancel_requested,
            upstream_cancel_requests: Arc::from([]),
            deadline,
        }
    }

    pub fn uncancelable() -> Self {
        Self {
            cancel_requested: Arc::new(AtomicBool::new(false)),
            upstream_cancel_requests: Arc::from([]),
            deadline: None,
        }
    }

    /// Create an independently cancelable child that also observes every
    /// cancellation source and deadline of its parent.
    ///
    /// Coordinators use this to stop sibling work after a failure without
    /// marking the caller's token as canceled.
    pub fn child(&self) -> Self {
        let mut upstream =
            Vec::with_capacity(self.upstream_cancel_requests.len().saturating_add(1));
        upstream.extend(self.upstream_cancel_requests.iter().cloned());
        upstream.push(Arc::clone(&self.cancel_requested));
        Self {
            cancel_requested: Arc::new(AtomicBool::new(false)),
            upstream_cancel_requests: Arc::from(upstream),
            deadline: self.deadline,
        }
    }

    pub fn cancel(&self) {
        self.cancel_requested.store(true, Ordering::SeqCst);
    }

    pub fn is_cancel_requested(&self) -> bool {
        self.cancel_requested.load(Ordering::SeqCst)
            || self
                .upstream_cancel_requests
                .iter()
                .any(|request| request.load(Ordering::SeqCst))
    }

    pub fn check(&self) -> Result<()> {
        if self.is_cancel_requested() {
            return Err(BicDbError::QueryCanceled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(BicDbError::QueryTimedOut);
        }
        Ok(())
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::uncancelable()
    }
}
