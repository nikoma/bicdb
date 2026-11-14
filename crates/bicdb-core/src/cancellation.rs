use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::error::{BicDbError, Result};

/// Host execution admission may be yielded while waiting for another session.
/// The host must reacquire its permit before returning from `resume`.
pub trait BlockingWaitAdmission: std::fmt::Debug + Send + Sync {
    /// Returns true only when this invocation released a permit.
    fn suspend(&self) -> bool;
    fn resume(&self, cancellation: &CancellationToken) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct CancellationToken {
    cancel_requested: Arc<AtomicBool>,
    upstream_cancel_requests: Arc<[Arc<AtomicBool>]>,
    deadline: Option<Instant>,
    wait_admission: Option<Arc<dyn BlockingWaitAdmission>>,
}

impl CancellationToken {
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn new(cancel_requested: Arc<AtomicBool>, deadline: Option<Instant>) -> Self {
        Self {
            cancel_requested,
            upstream_cancel_requests: Arc::from([]),
            deadline,
            wait_admission: None,
        }
    }

    pub fn uncancelable() -> Self {
        Self {
            cancel_requested: Arc::new(AtomicBool::new(false)),
            upstream_cancel_requests: Arc::from([]),
            deadline: None,
            wait_admission: None,
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
            wait_admission: self.wait_admission.clone(),
        }
    }

    /// Create a child with an operation-local deadline. An earlier parent
    /// deadline and all parent cancellation sources still apply. Expiring or
    /// canceling this child does not change its parent or sibling operations.
    pub fn child_with_deadline(&self, deadline: Instant) -> Self {
        let mut child = self.child();
        child.deadline = Some(
            self.deadline
                .map_or(deadline, |parent| parent.min(deadline)),
        );
        child
    }

    /// Independently cancelable child observing both REAL parents and their
    /// earliest deadline. Used only to constrain the new local read phase;
    /// this never extends or cancels either original request.
    pub(crate) fn child_linked_to(&self, other: &Self) -> Self {
        let mut child = self.child();
        let mut upstream = child.upstream_cancel_requests.to_vec();
        upstream.extend(other.upstream_cancel_requests.iter().cloned());
        upstream.push(Arc::clone(&other.cancel_requested));
        child.upstream_cancel_requests = Arc::from(upstream);
        child.deadline = match (child.deadline, other.deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        child
    }

    pub fn cancel(&self) {
        self.cancel_requested.store(true, Ordering::SeqCst);
    }

    pub fn with_wait_admission(mut self, admission: Arc<dyn BlockingWaitAdmission>) -> Self {
        self.wait_admission = Some(admission);
        self
    }

    /// Release host execution capacity only around an actual blocking wait.
    /// The closure must drop engine/lock-manager latches before it returns:
    /// reacquiring admission while retaining them would invert lock ordering.
    /// Memory reservations, transaction locks and cancellation remain in force.
    pub fn blocking_wait<T>(&self, wait: impl FnOnce() -> T) -> Result<T> {
        self.check()?;
        let suspended = self.wait_admission.as_ref().is_some_and(|a| a.suspend());
        let result = wait();
        if suspended {
            self.wait_admission.as_ref().unwrap().resume(self)?;
        }
        self.check()?;
        Ok(result)
    }

    pub fn is_cancel_requested(&self) -> bool {
        self.cancel_requested.load(Ordering::SeqCst)
            || self
                .upstream_cancel_requests
                .iter()
                .any(|request| request.load(Ordering::SeqCst))
    }

    pub fn check(&self) -> Result<()> {
        crate::query_budget::check()?;
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

#[cfg(test)]
mod linked_read_phase_tests {
    use super::*;
    #[test]
    fn linked_read_phase_observes_both_original_sources_without_canceling_parents() {
        let first = CancellationToken::uncancelable();
        let second = CancellationToken::uncancelable();
        let child = first.child_linked_to(&second);
        child.cancel();
        assert!(!first.is_cancel_requested());
        assert!(!second.is_cancel_requested());
        let child = first.child_linked_to(&second);
        second.cancel();
        assert!(matches!(child.check(), Err(BicDbError::QueryCanceled)));
        let first = CancellationToken::uncancelable();
        let second = CancellationToken::uncancelable();
        let child = first.child_linked_to(&second);
        first.cancel();
        assert!(matches!(child.check(), Err(BicDbError::QueryCanceled)));
    }
    #[test]
    fn linked_read_phase_preserves_earlier_deadline_and_upstream_cancellation() {
        let expired = Instant::now() - std::time::Duration::from_millis(1);
        let root = CancellationToken::uncancelable();
        let first = root.child();
        let second = CancellationToken::new(Arc::new(AtomicBool::new(false)), Some(expired));
        let child = first.child_linked_to(&second);
        assert_eq!(child.deadline(), Some(expired));
        assert!(matches!(child.check(), Err(BicDbError::QueryTimedOut)));
        let child = first.child_linked_to(&CancellationToken::uncancelable());
        root.cancel();
        assert!(matches!(child.check(), Err(BicDbError::QueryCanceled)));
    }
}
