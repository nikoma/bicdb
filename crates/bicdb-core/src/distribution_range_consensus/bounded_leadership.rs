//! Finite native-only catalogue leadership validation. Same recovery predicates;
//! no legacy replay, historical adoption or Native Ready/Strong grant is minted.
use super::*;

pub(super) fn recovery_parent_check(parent: Option<&CancellationToken>) -> Result<()> {
    if let Some(parent) = parent {
        parent.check()?;
    }
    Ok(())
}
pub(super) fn recovery_lock<'a, T>(
    lock: &'a Mutex<T>,
    parent: Option<&CancellationToken>,
) -> Result<parking_lot::MutexGuard<'a, T>> {
    let Some(parent) = parent else {
        return Ok(lock.lock());
    };
    loop {
        parent.check()?;
        if let Some(guard) = lock.try_lock_for(Duration::from_millis(5)) {
            parent.check()?;
            return Ok(guard);
        }
    }
}
impl<Transport: RangeWriteTransport> RangeWriteCoordinator<Transport> {
    /// Native-only bounded sweep for a retained INITIAL owner. Cursor and
    /// completion markers are DATA; each selected range still validates its
    /// current native history independently under the same finite caller.
    pub(crate) fn recover_native_local_leadership_bounded(
        &self,
        max_ranges: usize,
        parent: &CancellationToken,
    ) -> Result<usize> {
        parent.check()?;
        if parent.deadline().is_none()
            || !self.native_image_mode
            || max_ranges == 0
            || max_ranges > 64
        {
            return Err(range_write_error(
                "native leadership sweep requires a finite native caller and limit 1..=64",
            ));
        }
        let mut ranges = {
            let topology = self.recovery_topology(Some(parent))?;
            let recovered = recovery_lock(&self.recovered_epochs, Some(parent))?;
            topology
                .ranges
                .values()
                .filter(|range| {
                    range.leader == self.local_node_id
                        && recovered.get(&range.id) != Some(&range.epoch)
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        ranges.sort_by_key(|range| range.id);
        {
            let mut cursor = recovery_lock(&self.recovery_cursor, Some(parent))?;
            if let Some(last) = *cursor {
                let split = ranges.partition_point(|range| range.id <= last);
                if split < ranges.len() {
                    ranges.rotate_left(split);
                }
            }
            ranges.truncate(max_ranges);
            parent.check()?;
            if ranges.is_empty() {
                *cursor = None;
            }
        }
        let mut completed = 0usize;
        let mut failures = Vec::new();
        for range in ranges {
            parent.check()?;
            {
                // Advance for the range actually selected for this attempt,
                // never the unattempted tail of the batch. If this range uses
                // the whole caller window, the next call starts after it.
                let mut cursor = recovery_lock(&self.recovery_cursor, Some(parent))?;
                parent.check()?;
                *cursor = Some(range.id);
            }
            match self.recover_native_range_leadership_bounded(&range, parent) {
                Ok(changed) => completed = completed.saturating_add(usize::from(changed)),
                Err(error @ (BicDbError::QueryCanceled | BicDbError::QueryTimedOut)) => {
                    return Err(error);
                }
                Err(error) => failures.push(format!("{}: {error}", range.id)),
            }
        }
        {
            let topology = self.recovery_topology(Some(parent))?;
            let mut recovered = recovery_lock(&self.recovered_epochs, Some(parent))?;
            parent.check()?;
            recovered.retain(|range_id, epoch| {
                topology.range_by_id(*range_id).is_some_and(|range| {
                    range.leader == self.local_node_id && range.epoch == *epoch
                })
            });
        }
        parent.check()?;
        if failures.is_empty() {
            Ok(completed)
        } else {
            Err(range_write_error(format!(
                "native leadership recovery deferred for {} range(s): {}",
                failures.len(),
                failures.join(" | ")
            )))
        }
    }
    pub(super) fn recovery_service(
        &self,
        parent: Option<&CancellationToken>,
    ) -> Result<parking_lot::MutexGuard<'_, ClusterDataNodeService>> {
        recovery_lock(&self.local_service, parent)
    }
    pub(super) fn recovery_topology(
        &self,
        parent: Option<&CancellationToken>,
    ) -> Result<parking_lot::RwLockReadGuard<'_, ClusterTopology>> {
        let Some(parent) = parent else {
            return Ok(self.topology.read_recursive());
        };
        loop {
            parent.check()?;
            if let Some(guard) = self
                .topology
                .try_read_recursive_for(Duration::from_millis(5))
            {
                parent.check()?;
                return Ok(guard);
            }
        }
    }

    /// Validate only this exact currently led enforced native range, within
    /// the original finite caller. The ordinary API remains unchanged.
    /// Native prefix/tail repair still requires its genuine existing producer.
    /// A returned boolean describes this generic epoch marker only: it is not
    /// an installed policy, a Native Ready proof or successful Strong read.
    pub(crate) fn recover_native_range_leadership_bounded(
        &self,
        selected: &RangeDescriptor,
        parent: &CancellationToken,
    ) -> Result<bool> {
        parent.check()?;
        if parent.deadline().is_none() || !self.native_image_mode {
            return Err(range_write_error(
                "bounded native leadership needs an actual finite caller and native store",
            ));
        }
        {
            let topology = self.recovery_topology(Some(parent))?;
            let actual = topology
                .range_by_id(selected.id)
                .ok_or_else(|| range_write_error("bounded native leadership range absent"))?;
            if actual.leader != self.local_node_id
                || crate::distribution_native_read::canonical_range(actual)
                    != crate::distribution_native_read::canonical_range(selected)
                || !actual
                    .replicas
                    .iter()
                    .any(|r| r.node_id == self.local_node_id && r.role == RangeReplicaRole::Voter)
                || actual.replicas.len() > 64
                || topology
                    .nodes
                    .get(&self.local_node_id)
                    .and_then(|n| n.native_store_birth_v1.as_ref())
                    .is_none_or(|b| b.writer_enforcement_version() != 1)
            {
                return Err(range_write_error(
                    "bounded native leadership changed actual selected enforced range/member",
                ));
            }
        }
        {
            let service = self.recovery_service(Some(parent))?;
            if service.native_writer_enforcement_version()? != 1 {
                return Err(range_write_error(
                    "bounded native leadership cannot validate a legacy store",
                ));
            }
            parent.check()?;
        }
        self.require_local_schema_compatibility_for_recovery(selected, Some(parent))?;
        let mutation = self.range_mutations.try_enter(selected.id)?;
        let authority = RangeMutationAuthority::Mutation(&mutation);
        let _reservation = self.try_reserve_range_for_recovery(selected.id, Some(parent))?;
        // SAME selected mutation/reservation remains held through final marker
        // publication; another recovery cannot revoke it in a release gap.
        self.recover_one_range_leadership_held(selected, &authority, Some(parent))?;
        // These memory guards are acquired before the final parent check;
        // waiting for either can never publish a late/stale recovered epoch.
        let topology = self.recovery_topology(Some(parent))?;
        let mut recovered = recovery_lock(&self.recovered_epochs, Some(parent))?;
        parent.check()?;
        if topology.range_by_id(selected.id).is_none_or(|r| {
            r.leader != self.local_node_id
                || crate::distribution_native_read::canonical_range(r)
                    != crate::distribution_native_read::canonical_range(selected)
        }) {
            return Err(range_write_error(
                "bounded native leadership changed before epoch publication",
            ));
        }
        let previous = recovered.insert(selected.id, selected.epoch);
        Ok(previous != Some(selected.epoch))
    }
}
