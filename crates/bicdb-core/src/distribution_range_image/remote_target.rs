//! Historical source DATA observed over the real pinned transport. These
//! values are deliberately separate from local SourceRangeImageLineage and
//! from the target's independently acquired row/transaction permissions.
use super::*;
use crate::distribution_transport::AuthenticatedNativeRemoteImageSourceFrame;

/// The sole constructor consumes an actual Manifest response, while its
/// original request is live. No DTO, expected manifest, path or numeric cut
/// can construct this value. Historical issuance survives request expiry;
/// it asserts neither continuous remote liveness nor target write authority.
pub(crate) struct NativeRemoteImageFact {
    actual_manifest: AuthenticatedNativeRemoteImageSourceFrame,
}
impl NativeRemoteImageFact {
    pub(crate) fn from_actual_manifest(
        actual: AuthenticatedNativeRemoteImageSourceFrame,
    ) -> Result<Self> {
        actual.require_request_live()?;
        let NativeRemoteImageSourcePayloadV1::Manifest(manifest) = &actual.reply().payload else {
            return Err(invalid(
                "remote source fact requires an actual Manifest response",
            ));
        };
        manifest.require_shape()?;
        if !matches!(
            actual.reply().request.operation,
            NativeRemoteImageSourceOperationV1::Manifest
        ) || manifest.checksum_sha256 != actual.manifest().checksum_sha256
            || actual.reply().manifest_sha256 != manifest.checksum_sha256
        {
            return Err(invalid(
                "actual remote Manifest differs from its authenticated source cut",
            ));
        }
        actual.require_request_live()?;
        Ok(Self {
            actual_manifest: actual,
        })
    }

    /// Immutable historical data only. This is not an alternate constructor
    /// for local lineage, OLD quorum, NativeRecoveryAdmission or an image grant.
    pub(crate) fn manifest(&self) -> &NativeRemoteImageManifestV1 {
        match &self.actual_manifest.reply().payload {
            NativeRemoteImageSourcePayloadV1::Manifest(manifest) => manifest,
            _ => unreachable!("private constructor requires Manifest"),
        }
    }

    /// Attach only a separately authenticated, live bounded step from this
    /// exact issued source session. No network or engine lock occurs here.
    pub(crate) fn bind_step(
        &self,
        actual: AuthenticatedNativeRemoteImageSourceFrame,
    ) -> Result<NativeRemoteImageStep<'_>> {
        actual.require_request_live()?;
        let original = self.manifest();
        let reply = actual.reply();
        if matches!(reply.payload, NativeRemoteImageSourcePayloadV1::Manifest(_))
            || actual.manifest().checksum_sha256 != original.checksum_sha256
            || reply.manifest_sha256 != original.checksum_sha256
            || reply.source_id != original.source_id
            || reply.target_id != original.target_id
            || reply.source_birth != original.source_birth
            || reply.request.source_session != original.source_session
            || reply.request.target_attempt != original.target_attempt
            || reply.request.manifest_sha256 != original.checksum_sha256
        {
            return Err(invalid(
                "remote image step differs from actual issued Manifest/session",
            ));
        }
        // The actual transport already validates request/response equality,
        // cursor, operation-specific cut, native destination and frame hash.
        // Retain that original object; never re-seal its decoded projection.
        actual.require_request_live()?;
        Ok(NativeRemoteImageStep {
            source: self,
            actual,
        })
    }
}

/// This is a live source observation, not a target row/Tx grant. Its lifetime
/// and request must be checked in addition to actual target ownership, fresh
/// roles/OWN metadata, protected native state and the owned Installing fence.
pub(crate) struct NativeRemoteImageStep<'source> {
    source: &'source NativeRemoteImageFact,
    actual: AuthenticatedNativeRemoteImageSourceFrame,
}
impl NativeRemoteImageStep<'_> {
    /// Preserve the original authenticated request's exact cancellation and
    /// deadline through one private synchronous row transaction. This is only
    /// liveness DATA; it is neither a renewed request nor target authority.
    pub(crate) fn retain_original_request_cancellation(&self) -> Result<CancellationToken> {
        self.require_request_live()?;
        self.actual.retain_original_request_cancellation()
    }
    pub(crate) fn require_request_live(&self) -> Result<()> {
        self.actual.require_request_live()
    }
    pub(crate) fn source(&self) -> &NativeRemoteImageFact {
        self.source
    }
    pub(crate) fn payload(&self) -> Result<&NativeRemoteImageSourcePayloadV1> {
        self.require_request_live()?;
        Ok(&self.actual.reply().payload)
    }
}
