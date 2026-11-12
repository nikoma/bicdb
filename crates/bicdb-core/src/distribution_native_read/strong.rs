//! Consuming internal point-read protocol. Only the finalizer may publish an
//! owned row/absence. Intermediate snapshots and quorum facts are not permits.
use super::quorum::{scope_from_metadata, EnrolledNativeReadQuorum};
use super::*;
use crate::db::{InstalledNativeWriterHostGuard, RecordReadSnapshot};
use crate::distribution_consensus::MetadataReadObservation;
use crate::distribution_range_consensus::{RangeWriteCoordinator, RangeWriteTransport};
use crate::{ClusterDataNodeService, Record};
use std::sync::Arc;

const MAX_SNAPSHOT_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct NativeStrongPointReadRequest {
    pub business_request_id: Uuid,
    pub challenge: Uuid,
    pub range_id: crate::RangeId,
    pub range_epoch: u64,
    pub collection: String,
    pub id: String,
    pub budget_ms: u64,
}
impl NativeStrongPointReadRequest {
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        if self.business_request_id.is_nil()
            || self.challenge.is_nil()
            || self.range_epoch == 0
            || self.collection.is_empty()
            || self.collection.len() > 1024
            || self.id.is_empty()
            || self.id.len() > 64 * 1024
            || self.budget_ms == 0
            || self.budget_ms > 5000
        {
            return Err(invalid("invalid bounded strong point-read request"));
        }
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(self.budget_ms));
        token.check()?;
        Ok(token)
    }
    pub(crate) fn require_metadata(
        &self,
        metadata: &MetadataReadObservation,
        local: &ClusterNodeId,
    ) -> Result<NativeRangeReadScope> {
        metadata.require_live_request(self.business_request_id)?;
        let scope = scope_from_metadata(metadata, self.business_request_id, local)?;
        let topology = metadata.committed_topology(self.business_request_id)?;
        let range = topology.range_for_key(&self.collection, &self.id)?;
        if range.id != self.range_id
            || range.epoch != self.range_epoch
            || scope.range.id != range.id
            || scope.range.epoch != range.epoch
            || range.leader != *local
        {
            return Err(invalid(
                "strong point key/route/leader differs from actual metadata",
            ));
        }
        Ok(scope)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum NativeStrongReadStage {
    Request,
    Route,
    Reservation,
    WriterAdmission,
    DataQuorum,
    Snapshot,
    FinalMetadata,
    FinalCurrentness,
    Canceled,
}
#[derive(Debug)]
pub(crate) struct NativeStrongPointReadUnavailable {
    pub stage: NativeStrongReadStage,
    pub detail: String,
}
impl NativeStrongPointReadUnavailable {
    pub(crate) fn new(stage: NativeStrongReadStage, error: impl std::fmt::Display) -> Self {
        Self {
            stage,
            detail: error.to_string(),
        }
    }
}
#[derive(Debug)]
pub(crate) enum NativeStrongPointReadOutcome {
    Observed(NativeStrongPointReadValue),
    Unavailable(NativeStrongPointReadUnavailable),
}

/// Audit data, not a serializable permission or a new serving/read authority.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeStrongPointReadAudit {
    pub business_request_id: Uuid,
    pub challenge: Uuid,
    pub range_id: crate::RangeId,
    pub range_epoch: u64,
    pub range_index: u64,
    pub logical_genesis_sha256: String,
    pub logical_history_sha256: String,
    pub target_local_visibility_db_sequence: u64,
    pub snapshot_sequence: u64,
    pub first_metadata_context_id: Uuid,
    pub final_metadata_context_id: Uuid,
    pub final_metadata_generation: u64,
    pub final_metadata_term: u64,
    pub final_metadata_read_index: u64,
}

/// Its constructor is private to the final actual protocol. An observed None
/// has passed exactly the same barriers and object policy as an observed row.
#[derive(Debug)]
pub(crate) struct NativeStrongPointReadValue {
    record: Option<Arc<Record>>,
    audit: NativeStrongPointReadAudit,
}
impl NativeStrongPointReadValue {
    pub(crate) fn record(&self) -> Option<&Arc<Record>> {
        self.record.as_ref()
    }
    pub(crate) fn audit(&self) -> &NativeStrongPointReadAudit {
        &self.audit
    }
    pub(crate) fn into_owned_record(self) -> Option<Arc<Record>> {
        self.record
    }
}

/// Retains the actual snapshot pin and original enrolled/physical proof until
/// after fresh final metadata and full local currentness have been checked.
#[derive(Debug)]
pub(crate) struct PendingNativePointRead {
    enrolled: EnrolledNativeReadQuorum,
    request: NativeStrongPointReadRequest,
    host: Arc<InstalledNativeWriterHostGuard>,
    _snapshot: RecordReadSnapshot,
    record: Option<Arc<Record>>,
    snapshot_sequence: u64,
    cancellation: CancellationToken,
}

impl EnrolledNativeReadQuorum {
    pub(crate) fn capture_actual_point_read<T: RangeWriteTransport>(
        self,
        service: &ClusterDataNodeService,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        host: Arc<InstalledNativeWriterHostGuard>,
        request: &NativeStrongPointReadRequest,
        parent: &CancellationToken,
    ) -> Result<PendingNativePointRead> {
        self.require_lifetime()?;
        let round = &self.candidate.round;
        let cancellation = request
            .cancellation(parent)?
            .child_with_deadline(round.metadata.deadline());
        cancellation.check()?;
        let scope = request.require_metadata(&round.metadata, &round.local_node)?;
        if request.challenge != round.request.challenge
            || request.business_request_id != round.business
            || scope.checksum_sha256()? != round.request.scope.checksum_sha256()?
        {
            return Err(invalid(
                "actual enrolled quorum belongs to another point request",
            ));
        }
        let topology = round.metadata.committed_topology(round.business)?;
        for id in scope.members.keys() {
            if topology.nodes[id]
                .native_store_birth_v1
                .as_ref()
                .is_none_or(|b| b.writer_enforcement_version() != 1)
            {
                return Err(invalid(
                    "strong point scope includes a legacy unenforced writer birth",
                ));
            }
        }
        for id in &self.candidate.matching {
            if round.observations[id]
                .facts()
                .native_writer_enforcement_version
                != 1
            {
                return Err(invalid(
                    "strong point quorum includes unenforced actual writer facts",
                ));
            }
        }
        let local = round.observations[&round.local_node]
            .local()
            .ok_or_else(|| invalid("strong point has no actual same-service local proof"))?;
        local.require_current(service)?;
        let (snapshot, record, snapshot_sequence) = {
            let database = service.database();
            let database = database
                .try_read()
                .ok_or_else(|| invalid("database busy capturing actual strong snapshot"))?;
            host.require_for_coordinator(&database, &scope.range, coordinator)?;
            if host.writer_enforcement_version() != 1 {
                return Err(invalid("strong point host is not enforced from birth"));
            }
            // This number originates ONLY in the actual local materialization
            // receipt. It is never a range R, source B or caller input.
            let floor = local.local_visibility().get();
            let mut tx = database.begin_transaction()?;
            tx.set_snapshot_isolation(false, floor)?;
            let snapshot_sequence = tx.visibility_watermark();
            if snapshot_sequence < floor {
                return Err(invalid(
                    "actual snapshot does not cover local materialization",
                ));
            }
            let snapshot = tx.capture_record_prefix_snapshot_unchecked(
                &request.collection,
                &request.id,
                MAX_SNAPSHOT_BYTES,
            )?;
            // Native object policy remains mandatory. The private snapshot
            // constructor is not a permission to bypass protected collections.
            let record = snapshot.get(&database, &request.id)?;
            crate::distribution_range_consensus::validate_native_json_size(
                &record,
                crate::distribution_range_consensus::MAX_NATIVE_RECOVERY_ENTRY_BYTES,
            )?;
            cancellation.check()?;
            host.require_for_coordinator(&database, &scope.range, coordinator)?;
            (snapshot, record, snapshot_sequence)
        };
        // Reacquire only after the borrowed DB guard above has been dropped.
        local.require_current(service)?;
        cancellation.check()?;
        Ok(PendingNativePointRead {
            enrolled: self,
            request: request.clone(),
            host,
            _snapshot: snapshot,
            record,
            snapshot_sequence,
            cancellation,
        })
    }
}

impl PendingNativePointRead {
    pub(crate) fn finalize<T: RangeWriteTransport>(
        self,
        final_metadata: MetadataReadObservation,
        service: &ClusterDataNodeService,
        coordinator: &Arc<RangeWriteCoordinator<T>>,
        parent: &CancellationToken,
    ) -> Result<NativeStrongPointReadValue> {
        parent.check()?;
        self.cancellation.check()?;
        self.enrolled.require_lifetime()?;
        let round = &self.enrolled.candidate.round;
        let final_scope = self
            .request
            .require_metadata(&final_metadata, &round.local_node)?;
        if final_metadata.context_id() == round.metadata.context_id()
            || final_metadata.term() < round.metadata.term()
            || final_metadata.read_index() < round.metadata.read_index()
            || final_scope.checksum_sha256()? != round.request.scope.checksum_sha256()?
        {
            return Err(invalid(
                "final fresh metadata changed native point authority/domain",
            ));
        }
        let first = round.metadata.committed_topology(round.business)?;
        let last = final_metadata.committed_topology(round.business)?;
        for id in round.request.scope.members.keys() {
            if first.nodes[id].native_store_birth_v1 != last.nodes[id].native_store_birth_v1 {
                return Err(invalid(
                    "final metadata changed enrolled physical voter identity",
                ));
            }
        }
        let local = round.observations[&round.local_node]
            .local()
            .ok_or_else(|| invalid("final native point lost its actual local proof"))?;
        local.require_current(service)?;
        {
            let database = service.database();
            let database = database
                .try_read()
                .ok_or_else(|| invalid("database busy finalizing actual strong point"))?;
            self.host
                .require_for_coordinator(&database, &final_scope.range, coordinator)?;
            database.check_recovery_state()?;
        }
        local.require_current(service)?;
        self.cancellation.check()?;
        parent.check()?;
        final_metadata.require_live_request(round.business)?;
        let facts = local.facts();
        let audit = NativeStrongPointReadAudit {
            business_request_id: round.business,
            challenge: self.request.challenge,
            range_id: final_scope.range.id,
            range_epoch: final_scope.range.epoch,
            range_index: facts.range_index,
            logical_genesis_sha256: facts.logical_genesis_sha256.clone(),
            logical_history_sha256: facts.logical_history_sha256.clone(),
            target_local_visibility_db_sequence: facts.local_visibility_db_sequence,
            snapshot_sequence: self.snapshot_sequence,
            first_metadata_context_id: round.metadata.context_id(),
            final_metadata_context_id: final_metadata.context_id(),
            final_metadata_generation: last.generation,
            final_metadata_term: final_metadata.term(),
            final_metadata_read_index: final_metadata.read_index(),
        };
        Ok(NativeStrongPointReadValue {
            record: self.record,
            audit,
        })
    }
}
