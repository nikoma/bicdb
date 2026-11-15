//! Test-owned native-WAL faults AFTER actual scope acquisition. The normal
//! callback API cannot bypass the journal: these deliberate helpers run while
//! the original synchronous publisher retains that exact journal and Frozen.
use super::*;
use std::cell::RefCell;
thread_local! {
    static ARMED: RefCell<Option<u8>> = const { RefCell::new(None) };
    static OBSERVED: RefCell<Option<serde_json::Value>> = const { RefCell::new(None) };
}
impl BicDb {
    pub(crate) fn test_arm_closed_publication_scope_fault(code: u8) -> Result<()> {
        if !(1..=2).contains(&code) {
            return Err(invalid("unknown CLOSED scope test fault"));
        }
        ARMED.with(|slot| {
            if slot.borrow().is_some() {
                return Err(invalid("CLOSED scope test fault already armed"));
            }
            *slot.borrow_mut() = Some(code);
            Ok(())
        })?;
        OBSERVED.with(|slot| *slot.borrow_mut() = None);
        Ok(())
    }
    pub(crate) fn test_take_closed_publication_scope_fault() -> Option<serde_json::Value> {
        ARMED.with(|slot| slot.borrow_mut().take());
        OBSERVED.with(|slot| slot.borrow_mut().take())
    }
}
pub(crate) fn after_scope(
    scope: &NativeJointPreparedPublicationScope<'_>,
    parent: &CancellationToken,
) -> Result<()> {
    let Some(code) = ARMED.with(|slot| slot.borrow_mut().take()) else {
        return Ok(());
    };
    // Record actual scope entry before any mutator/read. The following faults
    // are test-only; they are not ordinary journal-free mutation producers.
    let mut value = serde_json::json!({"requested_fault":code,"actual_scope_completed":true,
        "local_node_id":scope.prepared.local_node_id(),"range_id":scope.prepared.record.range_id,
        "original_own_at_cut":scope.committed.test_request_memory(),"fault_completed":false});
    OBSERVED.with(|slot| *slot.borrow_mut() = Some(value.clone()));
    let result = mutate(scope, parent, code);
    match &result {
        Ok((xid, index)) => {
            value["fault_completed"] = true.into();
            value["fault_xid"] = (*xid).into();
            value["history_index"] = (*index).into();
        }
        Err(error) => value["primary_fault_error"] = error.to_string().into(),
    }
    // Baseline AFTER intentional fault WAL, before actual publication proceeds.
    value["after_fault_storage"] = match scope
        .database
        .test_joint_prepublication_storage(scope.prepared.record.range_id)
    {
        Ok(v) => v,
        Err(error) => serde_json::json!({"diagnostic_error":error.to_string()}),
    };
    value["gate_after_fault"] = serde_json::to_value(
        scope
            .database
            .range_mutation_gate()
            .test_source_capture_pin_state(scope.prepared.record.range_id)
            .ok(),
    )?;
    OBSERVED.with(|slot| *slot.borrow_mut() = Some(value));
    result.map(|_| ())
}
fn mutate(
    scope: &NativeJointPreparedPublicationScope<'_>,
    parent: &CancellationToken,
    code: u8,
) -> Result<(u64, u64)> {
    parent.check()?;
    let Some(crate::db::NativeJointWriterDomain::Closed(domain)) =
        scope.committed.closed_writer_domain()
    else {
        return Err(invalid(
            "CLOSED scope control requires original actual CLOSED survivor",
        ));
    };
    scope.require_closed_source_lineage(scope.database, scope.store, domain)?;
    let range = scope.prepared.record.range_id;
    let gate = scope.database.range_mutation_gate();
    let controlled = gate.try_controlled_mutation(scope.frozen)?;
    gate.validate(
        range,
        &crate::distribution_range_mutation::RangeMutationAuthority::Controlled(&controlled),
    )?;
    let result = if code == 1 {
        domain
            .test_remove_archive_after_scope(scope, &controlled, parent)
            .map(|xid| (xid, 0))
    } else {
        let index = *scope
            .catalogue
            .decisions()
            .keys()
            .find(|index| **index > 0 && **index < scope.lineage.range_index())
            .ok_or_else(|| invalid("CLOSED scope control lacks retained middle history"))?;
        let generation = scope.lineage.history_generation();
        let _original = scope
            .database
            .read_native_history_entry(range, generation, index)?;
        let key = crate::db::range_image_native::history::history_key(range, generation, index);
        scope
            .database
            .in_paged_transaction(|paged, xid| {
                parent.check()?;
                paged
                    .store()
                    .delete(xid, &native_key(&key))
                    .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                Ok(())
            })?
            .ok_or_else(|| invalid("CLOSED scope control actual page store absent"))
            .map(|xid| (xid, index))
    };
    let post = gate.validate(
        range,
        &crate::distribution_range_mutation::RangeMutationAuthority::Controlled(&controlled),
    );
    drop(controlled); // no gate mutex is held; SAME original Frozen remains.
    match result {
        Ok(value) => {
            post?;
            Ok(value)
        }
        Err(primary) => {
            let _ = post;
            Err(primary)
        }
    }
}
