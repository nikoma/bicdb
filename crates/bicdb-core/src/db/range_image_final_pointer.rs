//! Unconditional persisted integrity DATA, never final-image/native authority.
use super::*;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRegisteredRemoteFinalImagePointer {
    pub(crate) attempt_id: Uuid,
    pub(crate) native_page_xid: u64,
    pub(crate) checksum_sha256: String,
}
impl NativeRegisteredRemoteFinalImagePointer {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.attempt_id.is_nil()
            || self.native_page_xid == 0
            || self.checksum_sha256.len() != 64
            || !self.checksum_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(BicDbError::Cluster(
                "damaged Registered final-image pointer DATA".into(),
            ));
        }
        Ok(())
    }
}

/// Immutable persistence DATA, never an original-owner or phase grant.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeJoinedRemoteFinalImagePointer {
    pub(crate) attempt_id: Uuid,
    pub(crate) native_page_xid: u64,
    pub(crate) checksum_sha256: String,
}
impl NativeJoinedRemoteFinalImagePointer {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.attempt_id.is_nil()
            || self.native_page_xid == 0
            || self.checksum_sha256.len() != 64
            || !self.checksum_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(BicDbError::Cluster(
                "invalid Joined final-image pointer DATA".into(),
            ));
        }
        Ok(())
    }
}
