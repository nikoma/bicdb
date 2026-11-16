//! Finite identity reads outside service/DB/MStore scopes. A returned open
//! identity is necessary comparison data, never an application admission.
use super::*;
#[cfg(all(test, feature = "tls-replication"))]
mod controls;

impl ClusterDataNodeService {
    /// Only an already finite original caller may wait here. No deadline is
    /// created, no metadata acquisition is retried, and the service guard is
    /// dropped before the caller can acquire MStore or perform network work.
    pub(crate) fn capture_actual_listener_identity_waiting(
        service: &Arc<Mutex<Self>>,
        parent: &CancellationToken,
    ) -> Result<ClusterServiceOpenGuard> {
        parent.check()?;
        if parent.deadline().is_none() {
            return Err(data_error(
                "waiting listener identity requires original finite request",
            ));
        }
        loop {
            parent.check()?;
            if let Some(actual) = service.try_lock_for(std::time::Duration::from_millis(5)) {
                parent.check()?;
                let open = actual.listener_open_guard();
                drop(actual);
                open.require_live()?;
                parent.check()?;
                return Ok(open);
            }
            #[cfg(test)]
            waiting_tests::after_actual_contention(service)?;
            #[cfg(all(test, feature = "tls-replication"))]
            Self::test_after_actual_request_service_contention(service, parent)?;
        }
    }
}
