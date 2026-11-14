//! Actual owner-thread test ingress. All outputs are DATA, never a host,
//! Role/OWN, original token, service, Frozen, raw writer or mutable DB.
use super::*;
use crate::distribution_native_host::remote_first::NativeFirstIngressFault;
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_first_receiver_arm(
        &self,
        fault: NativeFirstIngressFault,
    ) -> Result<()> {
        self.replica
            .as_ref()
            .ok_or_else(|| invalid("actual FIRST receiver absent"))?
            .first_receive
            .test_arm(fault)
    }
    pub(in crate::distribution_native_host) fn test_first_receiver_snapshot(
        &self,
    ) -> Result<serde_json::Value> {
        let receiver = self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("actual FIRST receiver absent"))?;
        // Memory clocks/control snapshot precedes all diagnostic storage work.
        let queue = receiver.first_receive.test_snapshot();
        let attempts=receiver.first_attempts.values().map(|a|serde_json::json!({
            "attempt":a.attempt,"operation":a.operation,"source":a.donor,
            "original_process_live_at_snapshot":a.process.check().is_ok(),
            "manifest":a.source.get().map(|s|s.manifest()),"last_api_return":*a.last_api_return.lock(),
        })).collect::<Vec<_>>();
        let custody=receiver.remote_attempts.keys().map(|range| {
            let result=self.test_registered_remote_attempt_memory_custody(*range);
            serde_json::json!({"range":range,"actual_memory_custody":result.as_ref().ok(),
                "diagnostic_error":result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)})
        }).collect::<Vec<_>>();
        let native_states = receiver
            .remote_images
            .iter()
            .map(|(range, image)| {
                serde_json::json!({
                    "range":range,"state":image.test_state(),
                })
            })
            .collect::<Vec<_>>();
        let retained_targets = receiver
            .remote_joint_targets
            .try_lock()
            .ok_or_else(|| invalid("actual FIRST target table diagnostic busy"))?
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let database = {
            self.backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("actual FIRST diagnostic service busy"))?
                .database()
        };
        let forensics = {
            let db = database
                .try_read()
                .ok_or_else(|| invalid("actual FIRST diagnostic DB busy"))?;
            receiver.first_attempts.values().map(|a| {
                let result=db.test_registered_native_final_forensic_status(a.attempt,None);
                serde_json::json!({"attempt":a.attempt,"actual_native":result.as_ref().ok(),
                    "diagnostic_error":result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)})
            }).collect::<Vec<_>>()
        };
        Ok(
            serde_json::json!({"queue":queue,"attempts":attempts,"native_states":native_states,
            "retained_materialized_ranges":retained_targets,"native_forensics":forensics,"custody":custody,
            "global_capture_complete":false,"data_only":true}),
        )
    }
}
