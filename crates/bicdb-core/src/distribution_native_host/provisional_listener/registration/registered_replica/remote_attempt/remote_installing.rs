//! Same original Registered host, A lease and protected proposal; actual
//! catalogue installation is distinct from proposal publication and row copy.
use super::*;
use crate::distribution_range_consensus::NativeRegisteredRemoteInstallingFence;
impl NativeMetadataControlHost {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_registered_remote_installing(
        &mut self,
        source: &Arc<NativeRemoteImageFact>,
        frame: &NativeRemoteImageStep<'_>,
        role: HandoffRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeRegisteredRemoteInstallingFence>> {
        parent.check()?;
        if self.assigned.is_some() || self.data.is_some() {
            return Err(invalid("Registered Installing cannot replace DATA owner"));
        }
        let receiver = self
            .replica
            .as_mut()
            .ok_or_else(|| invalid("actual Registered receiver absent"))?;
        let range = role.role().current_range().id;
        let attempt = receiver
            .remote_attempts
            .get_mut(&range)
            .ok_or_else(|| invalid("actual retained remote attempt absent"))?;
        if !Arc::ptr_eq(source, attempt.preflight.source()) {
            return Err(invalid(
                "Registered Installing requires identical original MF Arc",
            ));
        }
        let lease = attempt
            .lease
            .as_ref()
            .ok_or_else(|| invalid("actual A lease absent"))?;
        lease.require_retained()?;
        let request = if let Some(publication) = &attempt.installing {
            receiver.admission.observe_remote_installing_request(
                frame,
                role,
                own,
                business,
                lease.try_as_frozen()?,
                parent,
                publication,
                &attempt.preflight,
            )?
        } else {
            receiver
                .admission
                .observe_remote_image_step(
                    frame,
                    role,
                    own,
                    business,
                    lease.try_as_frozen()?,
                    parent,
                )?
                .into_transfer_request()
        };
        attempt.preflight.require_metadata_current(&request)?;
        let point = NativeRegisteredRemoteProposalPoint {
            preflight: &attempt.preflight,
            lease,
            request: &request,
            frame,
        };
        point.require_live()?;
        let receipt = attempt
            .proposal
            .as_ref()
            .and_then(|publication| publication.receipt())
            .ok_or_else(|| {
                invalid("Registered Installing requires actual verified protected proposal")
            })?;
        let result = {
            let mut actual = receiver
                .backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Registered Installing actual service busy"))?;
            let database = actual.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Registered Installing actual DB busy"))?;
            db.with_registered_remote_attempt_exclusion(
                attempt.preflight.native(),
                &receiver.admission.original_hook,
                lease.try_as_frozen()?,
                attempt.preflight.attempt_id(),
                request.cancellation(),
                |scope| {
                    actual.persist_registered_remote_installing(
                        &db,
                        &point,
                        scope,
                        receipt,
                        &mut attempt.installing,
                    )
                },
            )
        };
        // Attempt fresh metadata/current request checks even when the sink
        // failed; the original owner's retained candidate always survives.
        let post = attempt
            .preflight
            .require_metadata_current(&request)
            .and_then(|()| point.require_live());
        let fence = result?;
        post?;
        parent.check()?;
        Ok(fence)
    }
}
