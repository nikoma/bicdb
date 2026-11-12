//! Test-only diagnostic spans. They observe real work/budgets, including early
//! error returns, without changing cancellation, deadlines or authority.
use crate::{CancellationToken, ClusterNodeId};
use std::time::Instant;

pub(crate) struct NativeJointPhaseTrace {
    stage: &'static str,
    node: ClusterNodeId,
    started: Instant,
    token: Option<CancellationToken>,
    deadline: Option<Instant>,
    completed: bool,
}
impl NativeJointPhaseTrace {
    pub(crate) fn new(
        stage: &'static str,
        node: &ClusterNodeId,
        token: &CancellationToken,
    ) -> Self {
        let result = Self {
            stage,
            node: node.clone(),
            started: Instant::now(),
            token: Some(token.clone()),
            deadline: token.deadline(),
            completed: false,
        };
        eprintln!(
            "ACTUAL_JOINT_COMPONENT begin stage={stage} node={node} remaining_ms={:?} canceled={}",
            result.remaining(),
            token.is_cancel_requested()
        );
        result
    }
    pub(crate) fn from_deadline(
        stage: &'static str,
        node: &ClusterNodeId,
        deadline: Instant,
    ) -> Self {
        let result = Self {
            stage,
            node: node.clone(),
            started: Instant::now(),
            token: None,
            deadline: Some(deadline),
            completed: false,
        };
        eprintln!("ACTUAL_JOINT_COMPONENT begin stage={stage} node={node} remaining_ms={:?} canceled=unknown",result.remaining());
        result
    }
    fn remaining(&self) -> Option<u128> {
        self.deadline
            .map(|d| d.saturating_duration_since(Instant::now()).as_millis())
    }
    pub(crate) fn complete(&mut self) {
        self.completed = true;
    }
}
impl Drop for NativeJointPhaseTrace {
    fn drop(&mut self) {
        eprintln!("ACTUAL_JOINT_COMPONENT end stage={} node={} elapsed_us={} remaining_ms={:?} canceled={:?} completed={} query_budget_active={} query_budget_error={:?}",self.stage,self.node,self.started.elapsed().as_micros(),self.remaining(),self.token.as_ref().map(|token|token.is_cancel_requested()),self.completed,crate::query_budget::active(),crate::query_budget::check().err().map(|error|error.to_string()));
    }
}
