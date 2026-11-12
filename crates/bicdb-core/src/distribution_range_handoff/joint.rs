//! Native joint-data handoff. Old/new sets are independent data quorums;
//! metadata correlation and decoded audit projections confer no capability.
use super::*;
use sha2::Digest;
fn invalid(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("native joint data handoff: {}", detail.into()))
}
mod scope;
use scope::NativeJointScopeData;
