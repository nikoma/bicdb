//! Actual selected catalogue operation; generic image Context still refuses
//! any persistence uncertainty. The original host retains its retry owner.
use super::*;
use crate::distribution_range_consensus::NativeJoinedRemoteCataloguePoint;
use crate::distribution_range_image::NativeRemoteImageStep;
impl NativeMetadataControlHost {
    pub(crate) fn adopt_joined_remote_catalogue_floor(
        &mut self,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        parent: &CancellationToken,
    ) -> Result<crate::db::NativeJoinedRemoteImageProgress> {
        parent.check()?;
        let host = self.joined_receive_host_mut()?;
        request.origin().require_host(host)?;
        let backend = host.parts.backend.clone();
        let factory = host.parts.factory.clone();
        let attempt = host
            .remote_attempts
            .get_mut(&request.origin().range_id())
            .ok_or_else(|| invalid("Joined adoption original attempt absent"))?;
        let preflight = attempt.preflight.clone();
        let lease = attempt
            .lease
            .as_ref()
            .ok_or_else(|| invalid("Joined adoption original A absent"))?;
        lease.require_retained()?;
        preflight.require_metadata_current(request)?;
        let fence = attempt
            .installing
            .as_ref()
            .and_then(|p| p.fence())
            .cloned()
            .ok_or_else(|| invalid("Joined adoption original Installing absent"))?;
        let image = attempt
            .image
            .as_ref()
            .ok_or_else(|| invalid("Joined adoption original actual image absent"))?;
        let physical = image
            .verified_base()
            .ok_or_else(|| invalid("Joined adoption original actual base absent"))?;
        let boundary = image
            .base_boundary()
            .ok_or_else(|| invalid("Joined adoption actual base attempt absent"))?;
        let final_image = image
            .final_image_receipt()
            .ok_or_else(|| invalid("Joined adoption actual final image absent"))?;
        let point = NativeJoinedRemoteImagePoint {
            preflight: &preflight,
            lease,
            request,
            frame,
        };
        point.require_live()?;
        let result = {
            let _original = preflight.selected().origin().original()?;
            let _admission = preflight.selected().origin().admission()?;
            let mut service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Joined adoption actual service busy"))?;
            let database = service.database();
            if !Arc::ptr_eq(&database, &factory.database) {
                return Err(invalid("Joined adoption changed original DB"));
            }
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Joined adoption actual DB busy"))?;
            db.with_joined_remote_mutation_scope(&preflight, request, frame, lease, |scope| {
                // Proposal identity/read is actual and finite; no ordinary
                // full-catalogue durability callback is used on this path.
                service.persist_joined_remote_catalogue_from_original(
                    &db,
                    &point,
                    scope,
                    &fence,
                    physical,
                    boundary,
                    final_image,
                    &mut attempt.catalogue,
                )
            })
        };
        let post = preflight
            .require_metadata_current(request)
            .and_then(|()| lease.require_retained())
            .and_then(|()| point.require_live())
            .and_then(|()| parent.check());
        match result {
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
            Ok(progress) => {
                post?;
                Ok(progress)
            }
        }
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    pub(crate) fn test_joined_remote_catalogue_state(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        let attempt = self
            .joined
            .as_ref()
            .and_then(|h| h.remote_attempts.get(&range))
            .ok_or_else(|| invalid("Joined actual catalogue owner absent"))?;
        Ok(
            serde_json::json!({"catalogue":attempt.catalogue.as_ref().map(|p|p.test_state()),"materialized_owner_retained":attempt.materialized.is_some(),"lease_still_owned_in_original_host":attempt.lease.is_some(),"no_NEW_Open_or_Serving":true}),
        )
    }
}
