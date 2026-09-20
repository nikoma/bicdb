//! Continuous admission for the Cell serving lifetime.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bicdb_app_runtime::HttpAdmissionCheck;
use bicdb_cell_admission::{AdmissionError, VerifiedAdmissionEvidence};

use crate::{current_unix_timestamp, Result};

trait AdmissionClock: std::fmt::Debug + Send + Sync {
    fn now(&self) -> i64;
}

#[cfg(test)]
impl AdmissionClock for std::sync::atomic::AtomicI64 {
    fn now(&self) -> i64 {
        self.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct SystemAdmissionClock {
    opened_at: Duration,
    started: Instant,
}

impl AdmissionClock for SystemAdmissionClock {
    fn now(&self) -> i64 {
        // A backwards wall-clock adjustment cannot extend the original lease.
        let monotonic_now = self.opened_at.saturating_add(self.started.elapsed());
        current_unix_timestamp().max(i64::try_from(monotonic_now.as_secs()).unwrap_or(i64::MAX))
    }
}

#[derive(Debug)]
pub(crate) struct CellAdmissionLease {
    pub(crate) evidence: VerifiedAdmissionEvidence,
    clock: Arc<dyn AdmissionClock>,
    expired: AtomicBool,
}

impl CellAdmissionLease {
    #[cfg(test)]
    pub(crate) fn with_test_clock(
        evidence: VerifiedAdmissionEvidence,
        clock: Arc<std::sync::atomic::AtomicI64>,
    ) -> Self {
        Self {
            evidence,
            clock,
            expired: AtomicBool::new(false),
        }
    }

    pub(crate) fn new(evidence: VerifiedAdmissionEvidence) -> Result<Self> {
        let lease = Self {
            evidence,
            clock: Arc::new(SystemAdmissionClock {
                opened_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::MAX),
                started: Instant::now(),
            }),
            expired: AtomicBool::new(false),
        };
        lease.check()?;
        Ok(lease)
    }

    pub(crate) fn check(&self) -> bicdb_cell_admission::Result<()> {
        if self.expired.load(Ordering::SeqCst) {
            return Err(AdmissionError::Evidence(
                "Cell admission has expired; fresh admission requires a restart".to_string(),
            ));
        }
        let result = self
            .evidence
            .regulated_admission_result_at(self.clock.now());
        if result.is_err() {
            // A later clock rollback must never readmit an expired runtime.
            self.expired.store(true, Ordering::SeqCst);
        }
        result
    }
}

impl HttpAdmissionCheck for CellAdmissionLease {
    fn is_admitted(&self) -> bool {
        self.check().is_ok()
    }
}

/// Process termination guard used by both Cell serving binaries. Keep it alive
/// through serving and shutdown. Expiry terminates the process with status 78,
/// including background workers and streams that cannot be cooperatively drained.
/// Embedded launchers must retain this guard or implement equivalent fencing.
#[must_use = "retain the admission watchdog through serving and shutdown"]
pub struct CellAdmissionWatchdog {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl CellAdmissionWatchdog {
    pub(crate) fn start(lease: Option<Arc<CellAdmissionLease>>) -> Result<Self> {
        let Some(lease) = lease else {
            return Ok(Self {
                stop: None,
                thread: None,
            });
        };
        lease.check()?;
        let (stop, receiver) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("cell-admission".to_string())
            .spawn(move || monitor_admission(&lease, receiver, || std::process::exit(78)))?;
        Ok(Self {
            stop: Some(stop),
            thread: Some(thread),
        })
    }
}

fn monitor_admission(
    lease: &CellAdmissionLease,
    stop: mpsc::Receiver<()>,
    on_expiry: impl FnOnce(),
) {
    loop {
        if lease.check().is_err() {
            // Do not log before fencing: a full stderr pipe must not block
            // termination of a Cell whose admission has expired.
            on_expiry();
            return;
        }
        match stop.recv_timeout(Duration::from_millis(100)) {
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            _ => return,
        }
    }
}

impl Drop for CellAdmissionWatchdog {
    fn drop(&mut self) {
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_cell_admission::{AdmissionDigest, AdmissionGate};
    use std::sync::atomic::AtomicI64;

    #[derive(Debug)]
    struct FakeClock(AtomicI64);

    impl AdmissionClock for FakeClock {
        fn now(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn lease(clock: Arc<FakeClock>) -> CellAdmissionLease {
        CellAdmissionLease {
            evidence: VerifiedAdmissionEvidence {
                policy_digest: AdmissionDigest::of_bytes(b"policy"),
                bundle_digest: AdmissionDigest::of_bytes(b"bundle"),
                authorization_id: "test".to_string(),
                checkpoint_sequence: 1,
                evidence_expires_at: 100,
                deployment_expires_at: 101,
                authorization_expires_at: 102,
                verified_gates: AdmissionGate::ALL.into_iter().collect(),
                evidence_complete: true,
                regulated_admission_enabled: true,
            },
            clock,
            expired: AtomicBool::new(false),
        }
    }

    #[test]
    fn live_admission_expires_and_clock_rollback_cannot_readmit() {
        let clock = Arc::new(FakeClock(AtomicI64::new(99)));
        let lease = lease(clock.clone());
        assert!(lease.is_admitted());
        clock.0.store(103, Ordering::SeqCst);
        assert!(!lease.is_admitted());
        clock.0.store(99, Ordering::SeqCst);
        assert!(!lease.is_admitted());
    }

    #[test]
    fn backwards_wall_clock_cannot_extend_monotonic_deadline() {
        let clock = SystemAdmissionClock {
            opened_at: Duration::from_secs(current_unix_timestamp() as u64 + 1000),
            started: Instant::now() - Duration::from_secs(2),
        };
        assert!(clock.now() >= clock.opened_at.as_secs() as i64 + 2);
    }

    #[test]
    fn watchdog_fences_a_running_lease_after_fake_clock_expiry() {
        let clock = Arc::new(FakeClock(AtomicI64::new(99)));
        let lease = Arc::new(lease(clock.clone()));
        let (_stop, receiver) = mpsc::channel();
        let (fenced, result) = mpsc::channel();
        let observed = lease.clone();
        let thread = std::thread::spawn(move || {
            monitor_admission(&observed, receiver, || fenced.send(()).unwrap());
        });
        assert!(result.try_recv().is_err());
        assert!(lease.is_admitted());
        clock.0.store(103, Ordering::SeqCst);
        result.recv_timeout(Duration::from_secs(2)).unwrap();
        thread.join().unwrap();
        assert!(!lease.is_admitted());
    }

    #[test]
    fn dropping_watchdog_stops_monitor_before_expiry() {
        let clock = Arc::new(FakeClock(AtomicI64::new(99)));
        let lease = Arc::new(lease(clock.clone()));
        let watchdog = CellAdmissionWatchdog::start(Some(lease.clone())).unwrap();
        drop(watchdog);
        assert!(lease.is_admitted());
    }

    #[test]
    fn admission_watchdog_terminates_the_process() {
        const CHILD: &str = "BICDB_TEST_ADMISSION_WATCHDOG_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let clock = Arc::new(FakeClock(AtomicI64::new(99)));
            let lease = Arc::new(lease(clock.clone()));
            let _watchdog = CellAdmissionWatchdog::start(Some(lease)).unwrap();
            clock.0.store(103, Ordering::SeqCst);
            std::thread::sleep(Duration::from_secs(3));
            panic!("admission watchdog failed to terminate the process");
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "admission::tests::admission_watchdog_terminates_the_process",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(78));
    }
}
