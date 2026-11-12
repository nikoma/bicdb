//! A countable native Append ACK must come from actual owned persistence or
//! the request-correlated authenticated transport. Serialized identity data is
//! not that provenance. This is the existing CFT transport trust boundary, not
//! a transferable signature or a lease on a still-running remote process.
use super::*;
use crate::distribution::NativeStoreBirthV1;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeMetadataAppendReceiver {
    version: u32,
    native_birth: NativeStoreBirthV1,
    metadata_store_id: Uuid,
}

impl NativeMetadataAppendReceiver {
    pub fn native_birth(&self) -> &NativeStoreBirthV1 {
        &self.native_birth
    }
    pub fn metadata_store_id(&self) -> Uuid {
        self.metadata_store_id
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NativeMetadataAppendProvenance {
    response_sha256: String,
    actual_pins: BTreeSet<String>,
}

// Equality describes the wire response, not its deliberately skipped local
// provenance. In particular JSON round-trip equality never grants an ACK.
impl PartialEq for MetadataAppendResponse {
    fn eq(&self, other: &Self) -> bool {
        self.cluster_id == other.cluster_id
            && self.term == other.term
            && self.node_id == other.node_id
            && self.success == other.success
            && self.match_index == other.match_index
            && self.conflict_index == other.conflict_index
            && self.native_receiver == other.native_receiver
    }
}
impl Eq for MetadataAppendResponse {}

impl MetadataAppendResponse {
    fn native_ack_digest(&self) -> Result<String> {
        let bytes = crate::distribution_range_image::bounded_json(self, 64 * 1024)?;
        let mut digest = Sha256::new();
        digest.update(b"bicdb-native-owned-metadata-append-ack-v1\0");
        digest.update(bytes);
        Ok(hex::encode(digest.finalize()))
    }

    /// The input cannot be built from a response DTO: its only constructor is
    /// the transport's completed original TLS/envelope exchange.
    #[cfg(feature = "tls-replication")]
    pub(crate) fn from_authenticated_native_append(
        sealed: crate::distribution_transport::metadata_append::AuthenticatedNativeMetadataAppendReply,
    ) -> Result<Self> {
        let (request, destination, expected, peer, mut response) = sealed.into_parts();
        response.native_provenance = None;
        if response.cluster_id != request.cluster_id || response.node_id != destination {
            return Err(metadata_error(
                "native append ACK request/receiver mismatch",
            ));
        }
        // Negative or higher-term responses do not supply replication progress.
        if !response.success {
            return Ok(response);
        }
        if let Some(binding) = response.native_receiver.as_ref() {
            if response.term != request.term
                || binding.version != 1
                || binding.metadata_store_id.is_nil()
                || expected.as_ref() != Some(&binding.native_birth)
                || peer.node_id() != &destination
                || peer.member_incarnation() != binding.native_birth.node_incarnation()
                || peer.plaintext_loopback()
                || peer.certificate_sha256().is_none()
            {
                return Err(metadata_error(
                    "native append ACK lacks original authenticated owned identity",
                ));
            }
            let actual_pins = peer
                .certificate_sha256()
                .map(str::to_owned)
                .into_iter()
                .collect();
            response.native_provenance = Some(NativeMetadataAppendProvenance {
                response_sha256: response.native_ack_digest()?,
                actual_pins,
            });
        } else if expected
            .as_ref()
            .is_some_and(|birth| birth.writer_enforcement_version() == 1)
        {
            return Err(metadata_error(
                "native append ACK omitted actual owned metadata identity",
            ));
        }
        Ok(response)
    }
}

impl MetadataConsensusStore {
    /// Persistence is part of this producer, not a caller-order convention.
    /// No caller-provided index, birth, or StoreUUID enters the local producer.
    pub(in crate::distribution_consensus) fn persist_owned_append_response(
        &self,
    ) -> Result<MetadataAppendResponse> {
        self.persist()?;
        // Persist learned retirement/replacement FIRST, but never acknowledge
        // catch-up/voting as the replacement physical incarnation.
        if self.require_native_metadata_actor_current(false).is_err() {
            return Ok(self.append_response(false, self.last_log_index().saturating_add(1)));
        }
        let mut response = self.append_response(true, self.last_log_index().saturating_add(1));
        if let Some(owner) = self.native_owner.as_ref() {
            self.require_native_metadata_actor_current(false)?;
            let actor = owner
                .actor
                .as_ref()
                .ok_or_else(|| metadata_error("native ACK actor absent"))?;
            let store_id = owner.registered_store_id.ok_or_else(|| {
                metadata_error("native ACK requires protected Registered metadata store")
            })?;
            if !self.fsync || self.state.native_store_id != Some(store_id) {
                return Err(metadata_error(
                    "native ACK protected metadata identity mismatch",
                ));
            }
            response.native_receiver = Some(NativeMetadataAppendReceiver {
                version: 1,
                native_birth: actor.birth.clone(),
                metadata_store_id: store_id,
            });
            response.native_provenance = Some(NativeMetadataAppendProvenance {
                response_sha256: response.native_ack_digest()?,
                actual_pins: actor.pins.clone(),
            });
        }
        Ok(response)
    }

    /// Validate against COMMITTED identity before any match_index mutation.
    /// A real old unbound ACK cannot acquire a new birth merely by arriving
    /// after enrollment commits and resets the leader's numeric progress.
    pub(in crate::distribution_consensus) fn require_current_native_append_ack(
        &self,
        expected_node: &ClusterNodeId,
        response: &MetadataAppendResponse,
    ) -> Result<()> {
        let Some(member) = self.state.committed_topology.nodes.get(expected_node) else {
            return Ok(());
        };
        let Some(birth) = member.native_store_birth_v1.as_ref() else {
            return Ok(());
        };
        // Pre-enforcement birth data never promised an owned metadata store.
        // Do not silently retrofit that old CFT metadata contract.
        if birth.writer_enforcement_version() != 1 {
            return Ok(());
        }
        let binding = response.native_receiver.as_ref().ok_or_else(|| {
            metadata_error("native append ACK requires actual owned receiver birth")
        })?;
        let provenance = response.native_provenance.as_ref().ok_or_else(|| {
            metadata_error(
                "native append ACK requires owned persistence/authenticated provenance, not a DTO",
            )
        })?;
        birth.validate_for(
            self.state.committed_topology.range_image_bootstrap_id,
            member.incarnation,
        )?;
        let current_pins = member
            .tls_certificate_sha256
            .iter()
            .chain(member.pending_tls_certificate_sha256.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        let pins_match = if current_pins.is_empty() {
            provenance.actual_pins.is_empty()
        } else {
            !current_pins.is_disjoint(&provenance.actual_pins)
        };
        if binding.version != 1
            || binding.metadata_store_id.is_nil()
            || &binding.native_birth != birth
            || binding.native_birth.node_incarnation() != member.incarnation
            || provenance.response_sha256 != response.native_ack_digest()?
            || !pins_match
        {
            return Err(metadata_error(
                "native append ACK current birth/owned store/body/pin mismatch",
            ));
        }
        Ok(())
    }
}
