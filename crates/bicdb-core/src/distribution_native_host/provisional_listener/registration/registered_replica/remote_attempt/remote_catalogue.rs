//! Original Registered native-base owner -> still-CLOSED catalogue floor.
use super::*;
use crate::distribution_range_consensus::NativeRegisteredRemoteCataloguePoint;
impl NativeMetadataControlHost {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn adopt_registered_remote_catalogue_floor(
        &mut self,
        source: &Arc<NativeRemoteImageFact>,
        frame: &NativeRemoteImageStep<'_>,
        role: HandoffRoleAdmission,
        own: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeRegisteredRemoteImageProgress> {
        parent.check()?;
        if self.assigned.is_some() || self.data.is_some() {
            return Err(invalid(
                "Registered catalogue adoption cannot replace DATA owner",
            ));
        }
        let receiver = self
            .replica
            .as_mut()
            .ok_or_else(|| invalid("actual Registered receiver absent"))?;
        let range = role.role().current_range().id;
        let attempt = receiver
            .remote_attempts
            .get_mut(&range)
            .ok_or_else(|| invalid("actual Registered attempt absent"))?;
        if !Arc::ptr_eq(source, attempt.preflight.source()) {
            return Err(invalid(
                "Registered catalogue adoption changed original MF Arc",
            ));
        }
        let fence = attempt
            .installing
            .as_ref()
            .and_then(|p| p.fence())
            .cloned()
            .ok_or_else(|| {
                invalid("Registered catalogue adoption lacks actual Installing fence")
            })?;
        let image = receiver
            .remote_images
            .get(&range)
            .ok_or_else(|| invalid("Registered catalogue adoption lacks actual image owner"))?;
        let physical = image.verified_base().ok_or_else(|| {
            invalid("Registered catalogue adoption requires original actual verified native base")
        })?;
        let boundary = image.base_boundary().ok_or_else(|| {
            invalid("Registered catalogue adoption lacks original native base boundary")
        })?;
        let final_image = image
            .final_image_receipt()
            .ok_or_else(|| invalid("Registered catalogue adoption lacks original final image"))?;
        let lease = attempt
            .lease
            .as_ref()
            .ok_or_else(|| invalid("Registered catalogue adoption lacks actual A lease"))?;
        lease.require_retained()?;
        let request = if let Some(publication) = &attempt.catalogue_floor {
            receiver.admission.observe_remote_catalogue_request(
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
            receiver.admission.observe_remote_installing_request(
                frame,
                role,
                own,
                business,
                lease.try_as_frozen()?,
                parent,
                attempt.installing.as_ref().unwrap(),
                &attempt.preflight,
            )?
        };
        attempt.preflight.require_metadata_current(&request)?;
        let point = NativeRegisteredRemoteProposalPoint {
            preflight: &attempt.preflight,
            lease,
            request: &request,
            frame,
        };
        point.require_live()?;
        let result = {
            let mut service = receiver
                .backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Registered catalogue adoption actual service busy"))?;
            let database = service.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Registered catalogue adoption actual DB busy"))?;
            db.with_registered_remote_mutation_exclusion(
                attempt.preflight.native(),
                &receiver.admission.original_hook,
                lease.try_as_frozen()?,
                attempt.preflight.attempt_id(),
                request.cancellation(),
                |scope| {
                    let actual = NativeRegisteredRemoteCataloguePoint::new(
                        &point,
                        scope,
                        &fence,
                        physical,
                        &boundary,
                        final_image,
                    );
                    service.persist_registered_remote_catalogue_floor(
                        &db,
                        &actual,
                        &mut attempt.catalogue_floor,
                    )
                },
            )
        };
        let post = attempt
            .preflight
            .require_metadata_current(&request)
            .and_then(|()| point.require_live());
        match result {
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
            Ok(progress) => {
                post?;
                parent.check()?;
                Ok(progress)
            }
        }
    }
}
