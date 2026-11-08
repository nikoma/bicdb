//! Fail-closed durability health for one database.
//!
//! A durability-critical write or sync that fails leaves the database in an
//! unknown state on disk. On Linux a failed `fsync` may drop the dirty pages
//! it could not write and clear the error, so a later `fsync` of the same
//! file can return success for data that never reached the disk. Retrying is
//! therefore not a recovery: the only safe answer is to stop acknowledging
//! writes until the database is reopened and recovery has rebuilt a state
//! that is known to be on disk.
//!
//! [`DurabilityHealth`] is that answer. It starts `Healthy`. The first
//! durability-critical failure moves it to `WriteFailed` with the site, the
//! error and the time; nothing moves it back. Every write path checks it
//! before acknowledging anything. A new handle (a reopen, which runs
//! recovery) starts with a fresh, healthy state.
//!
//! See `docs/durability-failure-handling.md`.

use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::{PageError, Result};

/// The first durability-critical failure a database saw.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WriteFailure {
    /// Stable name of the write or sync that failed, e.g. `page_log.sync`.
    pub site: String,
    /// The error the operating system (or the failing layer) reported.
    pub error: String,
    /// Wall-clock time of the failure, in milliseconds since the Unix epoch.
    pub at_unix_millis: u64,
}

impl fmt::Display for WriteFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.site, self.error)
    }
}

/// The durability state of one database handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurabilityState {
    /// No durability-critical failure has been seen.
    Healthy,
    /// A durability-critical write or sync failed. Writes are refused until
    /// the database is reopened and recovery verifies it.
    WriteFailed(WriteFailure),
}

impl DurabilityState {
    pub fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }

    pub fn failure(&self) -> Option<&WriteFailure> {
        match self {
            Self::Healthy => None,
            Self::WriteFailed(failure) => Some(failure),
        }
    }
}

/// Shared, one-way health flag. Clone the `Arc` into every layer that writes
/// durable state for the same database.
#[derive(Default)]
pub struct DurabilityHealth {
    failed: AtomicBool,
    failure: Mutex<Option<WriteFailure>>,
    /// Best-effort marker written at the first failure, so the next open
    /// knows it must verify before resuming writes.
    marker: Mutex<Option<PathBuf>>,
}

impl fmt::Debug for DurabilityHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurabilityHealth")
            .field("state", &self.state())
            .finish()
    }
}

impl DurabilityHealth {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// One relaxed-enough load; the hot paths call this on every write.
    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub fn state(&self) -> DurabilityState {
        if !self.is_failed() {
            return DurabilityState::Healthy;
        }
        match self.failure.lock().clone() {
            Some(failure) => DurabilityState::WriteFailed(failure),
            // `failed` is published after `failure` is stored; this arm is
            // unreachable but must not report healthy.
            None => DurabilityState::WriteFailed(WriteFailure {
                site: "unknown".into(),
                error: "durability failure".into(),
                at_unix_millis: 0,
            }),
        }
    }

    pub fn failure(&self) -> Option<WriteFailure> {
        self.state().failure().cloned()
    }

    /// `Err(PageError::WriteFailed)` once any durability-critical failure has
    /// been recorded.
    pub fn check(&self) -> Result<()> {
        if !self.is_failed() {
            return Ok(());
        }
        let failure = self.failure().expect("failed state has a failure");
        Err(PageError::WriteFailed {
            site: failure.site,
            detail: failure.error,
        })
    }

    /// Where to write the failure marker. The page crate itself never sets
    /// one; the database host names a file in its directory.
    pub fn set_marker_path(&self, path: PathBuf) {
        *self.marker.lock() = Some(path);
    }

    /// Record a durability-critical failure at `site` and return the error
    /// the caller should report. The first failure wins: later calls keep
    /// the original site and error, which is what an operator needs.
    pub fn record_failure(&self, site: &str, error: &dyn fmt::Display) -> PageError {
        let first = {
            let mut slot = self.failure.lock();
            if slot.is_none() {
                *slot = Some(WriteFailure {
                    site: site.to_string(),
                    error: error.to_string(),
                    at_unix_millis: unix_millis(),
                });
                self.failed.store(true, Ordering::Release);
                slot.clone()
            } else {
                None
            }
        };
        if let Some(failure) = first {
            // Never `eprintln!`: it panics when stderr cannot be written,
            // and stderr is often a file on the very disk that just failed.
            log_event(&serde_json::json!({
                "event": "durability.write_failed",
                "severity": "critical",
                "site": failure.site,
                "error": failure.error,
                "at_unix_millis": failure.at_unix_millis,
                "action": "writes are refused; reopen the database to run recovery",
            }));
            self.write_marker(&failure);
        }
        PageError::WriteFailed {
            site: site.to_string(),
            detail: error.to_string(),
        }
    }

    fn write_marker(&self, failure: &WriteFailure) {
        let Some(path) = self.marker.lock().clone() else {
            return;
        };
        // Best effort: the disk that just failed may refuse this too. The
        // handle is fenced either way; the marker only tells the next open
        // to run the verification step.
        let Ok(bytes) = serde_json::to_vec_pretty(failure) else {
            return;
        };
        let write = || -> std::io::Result<()> {
            use std::io::Write;
            let mut file = std::fs::File::create(&path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            if let Some(parent) = path.parent() {
                std::fs::File::open(parent)?.sync_all()?;
            }
            Ok(())
        };
        if let Err(error) = write() {
            log_event(&serde_json::json!({
                "event": "durability.marker_write_failed",
                "severity": "error",
                "path": path.display().to_string(),
                "error": error.to_string(),
            }));
        }
    }
}

/// Write one JSON event line to stderr, ignoring write errors.
pub fn log_event(event: &serde_json::Value) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "{event}");
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_failure_wins_and_never_clears() {
        let health = DurabilityHealth::new();
        assert!(health.check().is_ok());
        assert_eq!(health.state(), DurabilityState::Healthy);
        let error = health.record_failure("page_log.sync", &"EIO");
        assert!(error.is_write_failed());
        health.record_failure("page_file.sync", &"later");
        let failure = health.failure().unwrap();
        assert_eq!(failure.site, "page_log.sync");
        assert_eq!(failure.error, "EIO");
        assert!(health.check().unwrap_err().is_write_failed());
        assert!(health.is_failed());
    }

    #[test]
    fn marker_records_the_first_failure() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("durability-failed.json");
        let health = DurabilityHealth::new();
        health.set_marker_path(marker.clone());
        health.record_failure("transaction_log.write", &"EIO");
        let written: WriteFailure =
            serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
        assert_eq!(written.site, "transaction_log.write");
    }
}
