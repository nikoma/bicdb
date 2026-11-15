//! The retired source's own retirement from a range it handed off.
//!
//! The source acts only on ITS OWN committed metadata read, bound to its own
//! attached metadata store where the member has one. It writes a durable
//! membership tombstone behind its unchanged permanent promise; it deletes no
//! row and releases no fence. See the catalogue side for the record.
use super::*;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_range_consensus::NativeJointSourceRetirementReceipt;

/// The retired source's actual service and the receipt it issued. It cannot
/// be built from a digest or a reply; the completing metadata stage takes it.
pub(crate) struct NativeJointSourceRetired {
    service: Arc<Mutex<ClusterDataNodeService>>,
    receipt: NativeJointSourceRetirementReceipt,
}
impl NativeJointSourceRetired {
    /// What `source_cleanup_sha256` records.
    pub(crate) fn checksum_sha256(&self) -> &str {
        self.receipt.checksum_sha256()
    }
    pub(crate) fn cluster_id(&self) -> &crate::distribution::ClusterId {
        self.receipt.cluster_id()
    }
    pub(crate) fn node_id(&self) -> &ClusterNodeId {
        self.receipt.node_id()
    }
    pub(crate) fn node_incarnation(&self) -> u64 {
        self.receipt.node_incarnation()
    }
    pub(crate) fn relocation_id(&self) -> crate::distribution::RelocationId {
        self.receipt.relocation_id()
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.receipt.range_id()
    }
    pub(crate) fn session_id(&self) -> Uuid {
        self.receipt.session_id()
    }
    pub(crate) fn nonce(&self) -> Uuid {
        self.receipt.nonce()
    }
    pub(crate) fn activation_epoch(&self) -> u64 {
        self.receipt.activation_epoch()
    }
    pub(crate) fn decision_sha256(&self) -> &str {
        self.receipt.decision_sha256()
    }
    pub(crate) fn activated_quorum_sha256(&self) -> &str {
        self.receipt.activated_quorum_sha256()
    }
    /// The same open store of the same service still holds the record.
    pub(crate) fn require_current(&self) -> Result<()> {
        let actual = self
            .service
            .try_lock_for(std::time::Duration::from_secs(1))
            .ok_or_else(|| data_error("joint source retirement service busy"))?;
        if &actual.node_id != self.receipt.node_id() {
            return Err(data_error(
                "joint source retirement belongs to another actual service",
            ));
        }
        self.receipt.require_current(&actual.range_writes)
    }
}

/// Seal for [`NativeJointSourceRetirementProof`]: nameable only inside the
/// DATA service module tree, so no other module can supply a proof.
pub(in crate::distribution_data) mod source_retirement_seal {
    pub trait Sealed {}
}
/// Exactly what the completing metadata stage reads from the retired
/// source's retirement. An implementor is a capability, never decoded DATA:
/// it exists only where the source's own service issued the receipt in this
/// process, or where the pinned transport authenticated the source's reply.
pub(crate) trait NativeJointSourceRetirementProof: source_retirement_seal::Sealed {
    /// What `source_cleanup_sha256` records.
    fn checksum_sha256(&self) -> &str;
    fn cluster_id(&self) -> &crate::distribution::ClusterId;
    fn node_id(&self) -> &ClusterNodeId;
    fn node_incarnation(&self) -> u64;
    fn relocation_id(&self) -> crate::distribution::RelocationId;
    fn range_id(&self) -> RangeId;
    fn session_id(&self) -> Uuid;
    fn nonce(&self) -> Uuid;
    fn activation_epoch(&self) -> u64;
    fn decision_sha256(&self) -> &str;
    fn activated_quorum_sha256(&self) -> &str;
    fn require_current(&self) -> Result<()>;
}
impl source_retirement_seal::Sealed for NativeJointSourceRetired {}
impl NativeJointSourceRetirementProof for NativeJointSourceRetired {
    fn checksum_sha256(&self) -> &str {
        NativeJointSourceRetired::checksum_sha256(self)
    }
    fn cluster_id(&self) -> &crate::distribution::ClusterId {
        NativeJointSourceRetired::cluster_id(self)
    }
    fn node_id(&self) -> &ClusterNodeId {
        NativeJointSourceRetired::node_id(self)
    }
    fn node_incarnation(&self) -> u64 {
        NativeJointSourceRetired::node_incarnation(self)
    }
    fn relocation_id(&self) -> crate::distribution::RelocationId {
        NativeJointSourceRetired::relocation_id(self)
    }
    fn range_id(&self) -> RangeId {
        NativeJointSourceRetired::range_id(self)
    }
    fn session_id(&self) -> Uuid {
        NativeJointSourceRetired::session_id(self)
    }
    fn nonce(&self) -> Uuid {
        NativeJointSourceRetired::nonce(self)
    }
    fn activation_epoch(&self) -> u64 {
        NativeJointSourceRetired::activation_epoch(self)
    }
    fn decision_sha256(&self) -> &str {
        NativeJointSourceRetired::decision_sha256(self)
    }
    fn activated_quorum_sha256(&self) -> &str {
        NativeJointSourceRetired::activated_quorum_sha256(self)
    }
    fn require_current(&self) -> Result<()> {
        NativeJointSourceRetired::require_current(self)
    }
}

impl ClusterDataNodeService {
    /// Record this member's retirement from `range` (or confirm the record it
    /// already holds) on its own committed metadata read `own`.
    pub(crate) fn retire_actual_native_joint_source(
        service: &Arc<Mutex<Self>>,
        own: &HostMetadataReadObservation,
        business: Uuid,
        range: RangeId,
        parent: &CancellationToken,
    ) -> Result<NativeJointSourceRetired> {
        parent.check()?;
        own.require_live_request(business)?;
        // The attached metadata store is observed before any service, DB or
        // catalogue guard is held.
        let point = Self::capture_actual_joint_metadata_point(service, own, business, parent)?;
        if let Some(point) = &point {
            point.require_external_current()?;
        }
        let receipt = {
            let mut actual = service
                .try_lock_for(std::time::Duration::from_secs(1))
                .ok_or_else(|| data_error("joint source retirement service busy"))?;
            if own.requester_node_id() != &actual.node_id {
                return Err(data_error(
                    "joint source retirement metadata read is not this member's own",
                ));
            }
            {
                let database = checked_database_read(&actual.db, &actual.storage_policy)?;
                actual.require_native_catalog_database(&database)?;
                actual.storage_policy.require_database(&database)?;
                if let Some(point) = &point {
                    point.require_request(own, business)?;
                    point.require_native_store_point(&database, &actual.range_writes)?;
                }
            }
            parent.check()?;
            actual
                .range_writes
                .current_mut()
                .retire_native_joint_source(own, business, range)?
        };
        let result = NativeJointSourceRetired {
            service: service.clone(),
            receipt,
        };
        result.require_current()?;
        Ok(result)
    }
}
