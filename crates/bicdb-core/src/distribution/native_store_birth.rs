//! Immutable metadata identity data. Decoding this record grants no authority.
use super::*;

pub const NATIVE_STORE_BIRTH_TOPOLOGY_FORMAT_VERSION: u32 = 2;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeStoreBirthV1 {
    version: u32,
    bootstrap_id: Uuid,
    birth_id: Uuid,
    birth_sha256: String,
    node_incarnation: u64,
}

impl NativeStoreBirthV1 {
    pub fn version(&self) -> u32 {
        self.version
    }
    pub fn bootstrap_id(&self) -> Uuid {
        self.bootstrap_id
    }
    pub fn birth_id(&self) -> Uuid {
        self.birth_id
    }
    pub fn birth_sha256(&self) -> &str {
        &self.birth_sha256
    }
    pub fn node_incarnation(&self) -> u64 {
        self.node_incarnation
    }

    pub(crate) fn from_actual_native(native: &crate::db::NativeRecoveryAdmission) -> Result<Self> {
        let value = Self {
            version: 1,
            bootstrap_id: native.bootstrap_id(),
            birth_id: native.birth_id(),
            birth_sha256: native.birth_sha256().to_owned(),
            node_incarnation: native.local_member_incarnation(),
        };
        value.validate_for(Some(value.bootstrap_id), value.node_incarnation)?;
        Ok(value)
    }

    pub(crate) fn validate_for(&self, bootstrap: Option<Uuid>, incarnation: u64) -> Result<()> {
        if self.version != 1
            || self.bootstrap_id.is_nil()
            || self.birth_id.is_nil()
            || self.node_incarnation == 0
            || self.node_incarnation != incarnation
            || bootstrap != Some(self.bootstrap_id)
            || self.birth_sha256.len() != 64
            || !self
                .birth_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(cluster_error("invalid native store birth metadata binding"));
        }
        Ok(())
    }
}

/// Validate a sequential live proposal, including every pending log entry.
/// This preserves identity data; it does not attest the physical store.
pub(crate) fn validate_native_store_birth_transition(
    previous: &ClusterTopology,
    next: &ClusterTopology,
) -> Result<()> {
    validate_native_store_birth_preservation(previous, next)?;
    for (id, node) in &next.nodes {
        let old = previous.nodes.get(id);
        if let Some(old) = old {
            if old.incarnation != node.incarnation
                && (old.native_store_birth_v1.is_some() || node.native_store_birth_v1.is_some())
                && (node.incarnation <= old.incarnation
                    || old.lifecycle != ClusterNodeLifecycle::Decommissioned
                    || !is_unassigned_metadata_learner(next, node))
            {
                return Err(cluster_error("native store replacement requires a decommissioned predecessor and a newer unassigned metadata learner"));
            }
        }
        if node.native_store_birth_v1.is_some()
            && old
                .and_then(|node| node.native_store_birth_v1.as_ref())
                .is_none()
            && !is_unassigned_metadata_learner(next, node)
        {
            return Err(cluster_error(
                "native store enrollment requires an unassigned metadata learner",
            ));
        }
    }
    Ok(())
}

/// Check an authenticated authoritative snapshot/cache jump. Such a snapshot
/// may skip a new member's learner enrollment and promotion, but cannot erase
/// an identity already known by this observer or auto-enroll its old voter.
pub(crate) fn validate_native_store_birth_snapshot(
    previous: &ClusterTopology,
    next: &ClusterTopology,
) -> Result<()> {
    validate_native_store_birth_preservation(previous, next)?;
    for (id, node) in &next.nodes {
        if let Some(old) = previous.nodes.get(id) {
            if old.incarnation != node.incarnation
                && (old.native_store_birth_v1.is_some() || node.native_store_birth_v1.is_some())
                && node.incarnation <= old.incarnation
            {
                return Err(cluster_error("native store incarnation cannot regress"));
            }
            if old.incarnation == node.incarnation
                && old.native_store_birth_v1.is_none()
                && node.native_store_birth_v1.is_some()
                && (old.metadata_role != MetadataMemberRole::Learner
                    || previous.ranges.values().any(|range| {
                        range.replicas.iter().any(|replica| {
                            replica.node_id == *id && replica.role == RangeReplicaRole::Voter
                        })
                    }))
            {
                return Err(cluster_error(
                    "snapshot cannot auto-enroll a known existing native voter",
                ));
            }
        }
    }
    Ok(())
}

/// Identity-only check for retained anchors and overlapping pending entries.
/// Bound nodes remain as tombstones until an explicit higher-incarnation
/// replacement, so deleting and re-adding the same incarnation cannot rebind it.
pub(crate) fn validate_native_store_birth_preservation(
    previous: &ClusterTopology,
    next: &ClusterTopology,
) -> Result<()> {
    if previous.format_version >= NATIVE_STORE_BIRTH_TOPOLOGY_FORMAT_VERSION
        && next.format_version < NATIVE_STORE_BIRTH_TOPOLOGY_FORMAT_VERSION
    {
        return Err(cluster_error(
            "native store birth topology format cannot be downgraded",
        ));
    }
    for (id, old) in &previous.nodes {
        if old.native_store_birth_v1.is_none() {
            continue;
        }
        let node = next.nodes.get(id).ok_or_else(|| {
            cluster_error("enrolled native store node must retain its identity tombstone")
        })?;
        if old.incarnation == node.incarnation {
            if old.native_store_birth_v1 != node.native_store_birth_v1 {
                return Err(cluster_error(
                    "native store birth is immutable within a node incarnation",
                ));
            }
        } else if node.incarnation <= old.incarnation
            || (node.native_store_birth_v1.is_none() && !is_unassigned_metadata_learner(next, node))
        {
            return Err(cluster_error("native store replacement must advance incarnation and preserve enrollment or enter learner admission"));
        }
    }
    Ok(())
}

fn is_unassigned_metadata_learner(topology: &ClusterTopology, node: &ClusterNode) -> bool {
    node.metadata_role == MetadataMemberRole::Learner
        && !topology.ranges.values().any(|range| {
            range.replicas.iter().any(|replica| {
                replica.node_id == node.id && replica.role == RangeReplicaRole::Voter
            })
        })
}
