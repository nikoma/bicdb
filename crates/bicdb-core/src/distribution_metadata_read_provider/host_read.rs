//! Data-only fresh metadata for an authenticated member, including learners.
use super::*;
use crate::distribution_consensus::{
    HostMetadataReadAcquireRequest, HostMetadataReadAcquireResponse,
};

impl MetadataReadProvider {
    pub(crate) fn acquire_host_metadata(
        &self,
        caller: &MetadataReadParticipant,
        request: HostMetadataReadAcquireRequest,
        parent: &CancellationToken,
    ) -> Result<HostMetadataReadAcquireResponse> {
        request.validate()?;
        parent.check()?;
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(request.budget_ms));
        let subject = {
            let consensus = self.lock_consensus(&token)?;
            request.subject(consensus.committed_topology())?
        };
        let authority = self.acquire_authority(
            caller,
            subject.clone(),
            ReadAdmission::HostMember(&request),
            &token,
        )?;
        let response = authority.into_host_metadata_response(request, subject)?;
        self.bound_response(&response)?;
        token.check()?;
        Ok(response)
    }
}
