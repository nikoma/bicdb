//! The explicit finite caller joins the SAME original maintenance admission.
//! No job is skipped/reset/replaced and no scheduling scalar grants readiness.
use super::*;
#[cfg(test)]
mod controls;

impl NativeHotRecoveryMaintenance {
    pub(in crate::distribution_native_host::provisional_listener::registration::initial_data) fn tick_for_request(
        &self,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.live(parent)?;
        if parent.deadline().is_none() {
            return Err(invalid(
                "waiting hot DATA turn requires original finite request",
            ));
        }
        let request = match self.process.get() {
            Some(process) => parent.child_linked_to(process),
            None => parent.child(),
        };
        loop {
            self.live(&request)?;
            if let Some(mut state) = self.state.try_lock_for(Duration::from_millis(5)) {
                self.live(&request)?;
                return self.tick_with_actual_state(&request, &mut state);
            }
            #[cfg(test)]
            controls::after_actual_contention(self, &request)?;
        }
    }
}
