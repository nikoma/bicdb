//! Exact retained-control check for the separate materialized-prefix lane.
//! No abort, higher tail, competing variant or absent slot becomes permission.
use super::*;
impl RangeWriteStore {
    pub(crate) fn require_known_pending_replica_controls(
        &self,
        db: &crate::BicDb,
        known: &crate::db::KnownPendingNativeReplicaPrefix,
        pending: &RangeWriteCommand,
    ) -> Result<()> {
        self.require_actual_abort_catalogue(db)?;
        let actual = db.native_abort_slot_inventory()?;
        let actual = actual
            .iter()
            .filter(|s| s.command.range_id == pending.range_id)
            .collect::<Vec<_>>();
        let mirror = self
            .catalog
            .native_abort_protocol
            .as_ref()
            .and_then(|p| p.ranges.get(&pending.range_id));
        if mirror.map_or(0, |r| r.slots.len()) != actual.len()
            || !actual.iter().any(|s| s.command == *pending)
        {
            return Err(range_write_error("KnownPending requires complete actual protected slot inventory and exact pending slot"));
        }
        for record in actual {
            let slot = mirror
                .and_then(|r| r.slots.get(&record.command.index))
                .ok_or_else(|| {
                    range_write_error("KnownPending protected slot missing from original catalogue")
                })?;
            if slot.domain != record.domain
                || slot.command_id != record.command.command_id
                || slot.command_sha256 != record.command.checksum_sha256
                || slot.transaction_id != record.command.transaction_id
                || slot.original_epoch != record.command.range_epoch
                || slot.original_leader != record.command.leader_node_id
                || !slot.ever_commit
                || !record.ever_commit
                || slot.no_effect_sha256.is_some()
                || record.no_effect_sha256.is_some()
                || slot.promise.is_some()
                || record.abort_promise_sha256.is_some()
                || slot.resolved_native_receipt_sha256.is_some()
                || db
                    .observe_actual_native_abort_promise(pending.range_id, record.command.index)?
                    .is_some()
            {
                return Err(range_write_error(
                    "KnownPending refuses unresolved/abort/conflicting protected control",
                ));
            }
            if record.command.index <= known.range_index() {
                known.require_materialized(db, &record.command)?;
            } else if record.command != *pending
                || known.range_index().checked_add(1) != Some(pending.index)
            {
                return Err(range_write_error(
                    "KnownPending refuses any higher or competing durable obligation",
                ));
            }
        }
        Ok(())
    }
}
