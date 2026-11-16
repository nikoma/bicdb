//! A real unrelated-range metadata commit after the original commit, before
//! post-read. No data/quorum/native proof is manufactured by this test seam.
use super::*;
use std::cell::RefCell;
thread_local! { static CUT:RefCell<Option<Cut>>=const { RefCell::new(None) }; }
struct Cut {
    remaining: usize,
    results: Vec<serde_json::Value>,
}
struct HeadDriftCut(std::marker::PhantomData<std::rc::Rc<()>>);
pub(in crate::distribution_native_host) fn arm() -> Result<impl Fn() -> serde_json::Value> {
    let guard = CUT.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_some() {
            return Err(invalid("actual metadata drift cut already armed"));
        }
        *slot = Some(Cut {
            remaining: 3,
            results: Vec::new(),
        });
        Ok(HeadDriftCut(std::marker::PhantomData))
    })?;
    Ok(move || {
        let _keep = &guard;
        CUT.with(|slot| {
            slot.borrow()
                .as_ref()
                .map(|s| serde_json::json!({"remaining":s.remaining,"actual_returns":s.results}))
                .unwrap_or(serde_json::Value::Null)
        })
    })
}
impl Drop for HeadDriftCut {
    fn drop(&mut self) {
        CUT.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

pub(super) fn after_commit(
    backend: &Arc<
        crate::distribution_native_host::provisional_listener::NativeMetadataControlBackend,
    >,
    config: &DistributionConfig,
    selected: crate::RangeId,
    parent: &CancellationToken,
) -> Result<()> {
    let armed = CUT.with(|slot| slot.borrow().as_ref().is_some_and(|s| s.remaining > 0));
    if !armed {
        return Ok(());
    }
    let mut evidence = serde_json::json!({"selected_range":selected,"cut":"original commit before post-read",
        "actual_other_range_proposal_return":null,"actual_committed_read":null});
    let result = (|| -> Result<()> {
        parent.check()?;
        let business = Uuid::now_v7();
        let own = backend.transport.acquire_host_metadata_observation(
            &backend.node_id,
            business,
            remaining_finite(parent)?,
            parent,
        )?;
        let current = own.committed_topology(business)?;
        let other = current
            .ranges
            .values()
            .find(|r| r.id != selected)
            .ok_or_else(|| {
                invalid("actual metadata head-drift control needs a real second range")
            })?;
        evidence["other_range"] = serde_json::to_value(other.id)?;
        evidence["before_generation"] = serde_json::to_value(current.generation)?;
        let bytes = other
            .approximate_bytes
            .checked_add(1)
            .ok_or_else(|| invalid("actual other-range bytes overflow"))?;
        let mut view =
            crate::DistributionStore::native_registration_plan(config.clone(), current.clone())?;
        if !view.update_range_load(
            other.id,
            bytes,
            other.approximate_qps,
            &backend.node_id,
            now_finite_ms(),
        )? {
            return Err(invalid(
                "actual unrelated-range metadata change was a no-op",
            ));
        }
        let candidate = view.topology().clone();
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &backend.service,
            &own,
            business,
            parent,
        )?
        .ok_or_else(|| invalid("actual other-range commit needs native MStore point"))?;
        let actual = {
            let mut store = backend.lock_metadata(parent)?;
            store.require_native_control_leader(&own, business)?;
            point.require_current_memory()?;
            parent.check()?;
            store.propose_topology(candidate)
        };
        // Retain the actual helper return before network/post-read checks.
        let entry = match actual {
            Ok(entry) => {
                evidence["actual_other_range_proposal_return"] = serde_json::json!({"ok":true,"term":entry.term,"index":entry.index,"generation":entry.topology.generation});
                entry
            }
            Err(error) => {
                evidence["actual_other_range_proposal_return"] =
                    serde_json::json!({"ok":false,"error":error.to_string()});
                return Err(error);
            }
        };
        backend.heartbeat(parent)?;
        let post_business = Uuid::now_v7();
        let post = backend.transport.acquire_host_metadata_observation(
            &backend.node_id,
            post_business,
            remaining_finite(parent)?,
            parent,
        )?;
        let actual = post.committed_topology(post_business)?;
        evidence["actual_committed_read"] = serde_json::json!({"term":post.term(),"read_index":post.read_index(),"generation":actual.generation,
            "other_approximate_bytes":actual.range_by_id(other.id).map(|r|r.approximate_bytes)});
        if actual != &entry.topology || post.term() < entry.term || post.read_index() < entry.index
        {
            return Err(invalid(
                "actual unrelated-range native metadata proposal did not commit",
            ));
        }
        Ok(())
    })();
    evidence["returned_ok"] = serde_json::json!(result.is_ok());
    evidence["error"] = serde_json::json!(result.as_ref().err().map(ToString::to_string));
    CUT.with(|slot| {
        if let Some(slot) = slot.borrow_mut().as_mut() {
            slot.remaining -= 1;
            slot.results.push(evidence);
        }
    });
    result
}
