//! Archive guard: confirmed-prefix verification records, disk capacity,
//! escalating alerts and write backpressure for a failed or lagging archive.
//! See `docs/durability-failure-handling.md`, section 7.6.
//!
//! An archive failure never fences the database (section 7.5): local logs
//! are retained instead. Retention costs local disk, so a prolonged failure
//! would eventually fill the data volume and fail the database the hard way.
//! The guard turns that into a controlled sequence: alerts escalate
//! (warning, then critical) with the failure's duration, the retained bytes
//! and the free space; then, while the archive is still failed or lagging,
//! commits are throttled and finally refused with SQLSTATE 53100
//! (`disk_full`) before the volume is full. Retention is never dropped and
//! no unarchived log is ever deleted. When the archive recovers (it confirms
//! again and the retained bytes drain), the next evaluation lifts the
//! backpressure and writes resume on their own.

use crate::error::{BicDbError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::Duration;

/// Persisted result of the last commit-log archive verification, in the
/// database directory (node-local, never in a backup).
pub const COMMIT_LOG_VERIFICATION_FILE: &str = "log-archive-verification.json";

/// Result of reading back an archive's confirmed prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveVerification {
    /// `commit_log` or `page_log`.
    pub archive: String,
    pub ok: bool,
    pub at_unix_millis: u64,
    /// Confirmed end that was verified: a commit sequence (commit log) or a
    /// sealed-segment name (page log).
    pub through: u64,
    /// What was read back, or why it failed.
    pub detail: String,
}

pub(crate) fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// The persisted verification at `path`, if any (unreadable: none).
pub fn load_verification(path: &Path) -> Option<ArchiveVerification> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

pub(crate) fn store_verification(
    path: &Path,
    verification: &ArchiveVerification,
    fsync: bool,
) -> Result<()> {
    crate::storage::write_atomic(path, &serde_json::to_vec_pretty(verification)?, fsync)
}

/// Thresholds. Every field can be overridden with an environment variable
/// `BICDB_ARCHIVE_GUARD_<FIELD>` (durations in seconds, or milliseconds for
/// `throttle_delay`; fractions as decimals).
#[derive(Clone, Debug, PartialEq)]
pub struct ArchiveGuardConfig {
    /// How often the server evaluates the guard.
    pub check_interval: Duration,
    /// How often the confirmed prefix of each archive is read back.
    pub verify_interval: Duration,
    /// Alert by how long an archiver has been failed.
    pub warn_failure_after: Duration,
    pub critical_failure_after: Duration,
    /// Alert by local bytes retained for the archives.
    pub warn_retained_bytes: u64,
    pub critical_retained_bytes: u64,
    /// Alert by free space on the data volume and on each archive volume.
    pub warn_free_fraction: f64,
    pub critical_free_fraction: f64,
    /// Backpressure (only while an archive is failed, its verification
    /// failed, or it retains at least `warn_retained_bytes`): throttle, then
    /// refuse, by data-volume free space or by retained bytes.
    pub throttle_free_fraction: f64,
    pub refuse_free_fraction: f64,
    /// Refuse also when fewer than this many bytes are free.
    pub refuse_free_bytes: u64,
    pub throttle_retained_bytes: u64,
    pub refuse_retained_bytes: u64,
    /// Delay added to each write commit while throttled.
    pub throttle_delay: Duration,
}

const GIB: u64 = 1024 * 1024 * 1024;

impl Default for ArchiveGuardConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
            verify_interval: Duration::from_secs(3600),
            warn_failure_after: Duration::from_secs(5 * 60),
            critical_failure_after: Duration::from_secs(30 * 60),
            warn_retained_bytes: 8 * GIB,
            critical_retained_bytes: 32 * GIB,
            warn_free_fraction: 0.20,
            critical_free_fraction: 0.10,
            throttle_free_fraction: 0.10,
            refuse_free_fraction: 0.05,
            refuse_free_bytes: GIB,
            throttle_retained_bytes: 32 * GIB,
            refuse_retained_bytes: 64 * GIB,
            throttle_delay: Duration::from_millis(50),
        }
    }
}

impl ArchiveGuardConfig {
    /// Defaults overridden by `BICDB_ARCHIVE_GUARD_*`.
    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();
        let var = |name: &str| std::env::var(format!("BICDB_ARCHIVE_GUARD_{name}")).ok();
        let bad = |name: &str, value: &str| {
            BicDbError::InvalidConfig(format!(
                "BICDB_ARCHIVE_GUARD_{name}={value} is not a valid value"
            ))
        };
        macro_rules! secs {
            ($field:ident, $name:literal) => {
                if let Some(value) = var($name) {
                    config.$field =
                        Duration::from_secs(value.parse().map_err(|_| bad($name, &value))?);
                }
            };
        }
        macro_rules! num {
            ($field:ident, $name:literal) => {
                if let Some(value) = var($name) {
                    config.$field = value.parse().map_err(|_| bad($name, &value))?;
                }
            };
        }
        secs!(check_interval, "CHECK_INTERVAL");
        secs!(verify_interval, "VERIFY_INTERVAL");
        secs!(warn_failure_after, "WARN_FAILURE_AFTER");
        secs!(critical_failure_after, "CRITICAL_FAILURE_AFTER");
        num!(warn_retained_bytes, "WARN_RETAINED_BYTES");
        num!(critical_retained_bytes, "CRITICAL_RETAINED_BYTES");
        num!(warn_free_fraction, "WARN_FREE_FRACTION");
        num!(critical_free_fraction, "CRITICAL_FREE_FRACTION");
        num!(throttle_free_fraction, "THROTTLE_FREE_FRACTION");
        num!(refuse_free_fraction, "REFUSE_FREE_FRACTION");
        num!(refuse_free_bytes, "REFUSE_FREE_BYTES");
        num!(throttle_retained_bytes, "THROTTLE_RETAINED_BYTES");
        num!(refuse_retained_bytes, "REFUSE_RETAINED_BYTES");
        if let Some(value) = var("THROTTLE_DELAY_MS") {
            config.throttle_delay = Duration::from_millis(
                value
                    .parse()
                    .map_err(|_| bad("THROTTLE_DELAY_MS", &value))?,
            );
        }
        config.validate()?;
        Ok(config)
    }

    /// Thresholds must escalate in order.
    pub fn validate(&self) -> Result<()> {
        let fractions = [
            self.warn_free_fraction,
            self.critical_free_fraction,
            self.throttle_free_fraction,
            self.refuse_free_fraction,
        ];
        let ordered = self.warn_failure_after <= self.critical_failure_after
            && self.warn_retained_bytes <= self.critical_retained_bytes
            && self.critical_free_fraction <= self.warn_free_fraction
            && self.refuse_free_fraction <= self.throttle_free_fraction
            && self.throttle_retained_bytes <= self.refuse_retained_bytes
            && fractions.iter().all(|f| (0.0..=1.0).contains(f));
        if ordered {
            Ok(())
        } else {
            Err(BicDbError::InvalidConfig(
                "archive guard thresholds must escalate: warning <= critical, throttle <= refuse, \
                 fractions within 0..1"
                    .into(),
            ))
        }
    }
}

/// Free and total bytes of a volume.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct VolumeSpace {
    pub free_bytes: u64,
    pub total_bytes: u64,
}

impl VolumeSpace {
    fn free_fraction(&self) -> f64 {
        if self.total_bytes == 0 {
            1.0
        } else {
            self.free_bytes as f64 / self.total_bytes as f64
        }
    }
}

/// Space available to an unprivileged writer on the volume holding `path`
/// (the nearest existing ancestor when `path` does not exist yet).
#[cfg(unix)]
pub fn volume_space(path: &Path) -> Option<VolumeSpace> {
    use std::os::unix::ffi::OsStrExt;
    let mut probe = path;
    while !probe.exists() {
        probe = probe.parent()?;
    }
    let c_path = std::ffi::CString::new(probe.as_os_str().as_bytes()).ok()?;
    // SAFETY: zeroed plain-old-data out-parameter.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: a valid NUL-terminated path and a valid out-parameter.
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) } != 0 {
        return None;
    }
    let block = stat.f_frsize as u64;
    Some(VolumeSpace {
        free_bytes: (stat.f_bavail as u64).saturating_mul(block),
        total_bytes: (stat.f_blocks as u64).saturating_mul(block),
    })
}

#[cfg(not(unix))]
pub fn volume_space(_path: &Path) -> Option<VolumeSpace> {
    None
}

/// What the guard evaluates.
#[derive(Clone, Debug, Default)]
pub struct ArchiveGuardInputs {
    pub now_unix_millis: u64,
    /// Start of the oldest current archiver failure, if any.
    pub failure_since_unix_millis: Option<u64>,
    /// A confirmed-prefix verification failed (and was not cleared).
    pub verification_failed: bool,
    /// Local bytes retained only for the archives.
    pub retained_bytes: u64,
    pub data_volume: Option<VolumeSpace>,
    /// `(name, space)` of each archive's volume.
    pub archive_volumes: Vec<(String, VolumeSpace)>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum AlertLevel {
    #[default]
    Ok = 0,
    Warning = 1,
    Critical = 2,
}

impl AlertLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warning => "warning",
            Self::Critical => "critical",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum Backpressure {
    #[default]
    None = 0,
    Throttle = 1,
    Refuse = 2,
}

impl Backpressure {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Throttle => "throttle",
            Self::Refuse => "refuse",
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Throttle,
            2 => Self::Refuse,
            _ => Self::None,
        }
    }
}

/// Space of one volume with its projected time to full.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct VolumeStatus {
    pub name: String,
    pub free_bytes: u64,
    pub total_bytes: u64,
    /// At the recently observed consumption rate; `None` when not filling.
    pub seconds_to_full: Option<u64>,
}

/// One evaluation.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct ArchiveGuardStatus {
    pub level: AlertLevel,
    /// Every threshold that is crossed, most severe first.
    pub reasons: Vec<String>,
    pub backpressure: Backpressure,
    pub backpressure_reason: Option<String>,
    pub retained_bytes: u64,
    pub failure_seconds: u64,
    pub verification_failed: bool,
    pub data_volume: Option<VolumeStatus>,
    pub archive_volumes: Vec<VolumeStatus>,
    pub evaluated_at_unix_millis: u64,
}

#[derive(Default)]
struct RateState {
    samples: std::collections::BTreeMap<String, (u64, u64, f64)>,
    level: AlertLevel,
    backpressure: Backpressure,
}

/// Evaluates inputs against a config, keeping the consumption-rate history
/// for time-to-full and emitting an event whenever the alert level or the
/// backpressure changes.
pub struct ArchiveGuard {
    config: ArchiveGuardConfig,
    state: parking_lot::Mutex<RateState>,
}

impl ArchiveGuard {
    pub fn new(config: ArchiveGuardConfig) -> Self {
        Self {
            config,
            state: parking_lot::Mutex::new(RateState::default()),
        }
    }

    pub fn config(&self) -> &ArchiveGuardConfig {
        &self.config
    }

    fn volume(
        &self,
        state: &mut RateState,
        name: &str,
        space: VolumeSpace,
        now: u64,
    ) -> VolumeStatus {
        // Exponentially weighted consumption rate (bytes per second).
        let rate = match state.samples.get(name) {
            Some(&(at, free, rate)) if now > at => {
                let seconds = (now - at) as f64 / 1000.0;
                let observed = (free as f64 - space.free_bytes as f64) / seconds;
                0.5 * rate + 0.5 * observed
            }
            Some(&(_, _, rate)) => rate,
            None => 0.0,
        };
        state
            .samples
            .insert(name.to_string(), (now, space.free_bytes, rate));
        VolumeStatus {
            name: name.to_string(),
            free_bytes: space.free_bytes,
            total_bytes: space.total_bytes,
            seconds_to_full: (rate > 1.0).then(|| (space.free_bytes as f64 / rate) as u64),
        }
    }

    pub fn evaluate(&self, inputs: &ArchiveGuardInputs) -> ArchiveGuardStatus {
        let config = &self.config;
        let now = inputs.now_unix_millis;
        let mut state = self.state.lock();
        let mut status = ArchiveGuardStatus {
            retained_bytes: inputs.retained_bytes,
            verification_failed: inputs.verification_failed,
            evaluated_at_unix_millis: now,
            ..ArchiveGuardStatus::default()
        };
        let raise = |status: &mut ArchiveGuardStatus, level: AlertLevel, reason: String| {
            status.level = status.level.max(level);
            status.reasons.push(format!("{}: {reason}", level.as_str()));
        };
        if let Some(since) = inputs.failure_since_unix_millis {
            let seconds = now.saturating_sub(since) / 1000;
            status.failure_seconds = seconds;
            let failed_for = Duration::from_secs(seconds);
            if failed_for >= config.critical_failure_after {
                raise(
                    &mut status,
                    AlertLevel::Critical,
                    format!("archive failed for {seconds}s"),
                );
            } else if failed_for >= config.warn_failure_after {
                raise(
                    &mut status,
                    AlertLevel::Warning,
                    format!("archive failed for {seconds}s"),
                );
            }
        }
        if inputs.verification_failed {
            raise(
                &mut status,
                AlertLevel::Critical,
                "archive confirmed-prefix verification failed".into(),
            );
        }
        let retained = inputs.retained_bytes;
        if retained >= config.critical_retained_bytes {
            raise(
                &mut status,
                AlertLevel::Critical,
                format!("{retained} bytes retained for the archive"),
            );
        } else if retained >= config.warn_retained_bytes {
            raise(
                &mut status,
                AlertLevel::Warning,
                format!("{retained} bytes retained for the archive"),
            );
        }
        let check_space = |status: &mut ArchiveGuardStatus, name: &str, space: VolumeSpace| {
            let fraction = space.free_fraction();
            if fraction < config.critical_free_fraction {
                raise(
                    status,
                    AlertLevel::Critical,
                    format!("{name} volume {:.1}% free", fraction * 100.0),
                );
            } else if fraction < config.warn_free_fraction {
                raise(
                    status,
                    AlertLevel::Warning,
                    format!("{name} volume {:.1}% free", fraction * 100.0),
                );
            }
        };
        if let Some(space) = inputs.data_volume {
            check_space(&mut status, "data", space);
            status.data_volume = Some(self.volume(&mut state, "data", space, now));
        }
        for (name, space) in &inputs.archive_volumes {
            check_space(&mut status, name, *space);
            let volume = self.volume(&mut state, name, *space, now);
            status.archive_volumes.push(volume);
        }
        status
            .reasons
            .sort_by_key(|reason| !reason.starts_with("critical"));

        // Backpressure only for an archive that is failed, unverifiable or
        // lagging: it protects the data volume from the archive's retention,
        // it is not a general disk-full policy.
        let degraded = inputs.failure_since_unix_millis.is_some()
            || inputs.verification_failed
            || retained >= config.warn_retained_bytes;
        if degraded {
            let data = inputs.data_volume;
            let below = |fraction: f64, bytes: u64| {
                data.is_some_and(|space| {
                    let floor = ((space.total_bytes as f64) * fraction) as u64;
                    space.free_bytes < floor.max(bytes)
                })
            };
            if below(config.refuse_free_fraction, config.refuse_free_bytes) {
                status.backpressure = Backpressure::Refuse;
                status.backpressure_reason =
                    Some("data volume free space below the refuse threshold".into());
            } else if retained >= config.refuse_retained_bytes {
                status.backpressure = Backpressure::Refuse;
                status.backpressure_reason = Some(format!(
                    "{retained} bytes retained for a failed or lagging archive"
                ));
            } else if below(config.throttle_free_fraction, 0) {
                status.backpressure = Backpressure::Throttle;
                status.backpressure_reason =
                    Some("data volume free space below the throttle threshold".into());
            } else if retained >= config.throttle_retained_bytes {
                status.backpressure = Backpressure::Throttle;
                status.backpressure_reason = Some(format!(
                    "{retained} bytes retained for a failed or lagging archive"
                ));
            }
        }
        if status.level != state.level {
            bicdb_page::health::log_event(&serde_json::json!({
                "event": "archive.alert",
                "severity": status.level.as_str(),
                "previous": state.level.as_str(),
                "reasons": status.reasons,
                "retained_bytes": retained,
            }));
            state.level = status.level;
        }
        if status.backpressure != state.backpressure {
            bicdb_page::health::log_event(&serde_json::json!({
                "event": "archive.backpressure",
                "severity": if status.backpressure == Backpressure::None { "info" } else { "critical" },
                "state": status.backpressure.as_str(),
                "previous": state.backpressure.as_str(),
                "reason": status.backpressure_reason,
                "action": match status.backpressure {
                    Backpressure::None => "writes accepted",
                    Backpressure::Throttle => "write commits are delayed",
                    Backpressure::Refuse => "write commits are refused (SQLSTATE 53100) until the archive recovers; retention is kept",
                },
            }));
            state.backpressure = status.backpressure;
        }
        status
    }
}

/// The write gate a database enforces (set from the guard's evaluation).
#[derive(Debug, Default)]
pub(crate) struct BackpressureGate {
    state: AtomicU8,
    delay_micros: AtomicU64,
    reason: parking_lot::Mutex<String>,
}

impl BackpressureGate {
    pub(crate) fn set(&self, backpressure: Backpressure, reason: &str, delay: Duration) {
        *self.reason.lock() = reason.to_string();
        self.delay_micros.store(
            u64::try_from(delay.as_micros()).unwrap_or(u64::MAX),
            Ordering::Release,
        );
        self.state.store(backpressure as u8, Ordering::Release);
    }

    pub(crate) fn get(&self) -> Backpressure {
        Backpressure::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Refuse or delay a write; one atomic load when nothing is set.
    pub(crate) fn enforce(&self, operation: &str) -> Result<()> {
        match self.get() {
            Backpressure::None => Ok(()),
            Backpressure::Refuse => Err(BicDbError::ArchiveBackpressure(format!(
                "{operation} is refused: {}; the archive is failed or lagging and the local logs \
                 it needs are retained. Writes resume when the archive recovers",
                self.reason.lock()
            ))),
            Backpressure::Throttle => {
                if operation.starts_with("commit") {
                    std::thread::sleep(Duration::from_micros(
                        self.delay_micros.load(Ordering::Acquire),
                    ));
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GB: u64 = 1_000_000_000;

    fn inputs(free: u64, retained: u64, failed_for: Option<u64>) -> ArchiveGuardInputs {
        ArchiveGuardInputs {
            now_unix_millis: 10_000_000,
            failure_since_unix_millis: failed_for.map(|seconds| 10_000_000 - seconds * 1000),
            verification_failed: false,
            retained_bytes: retained,
            data_volume: Some(VolumeSpace {
                free_bytes: free,
                total_bytes: 100 * GB,
            }),
            archive_volumes: vec![],
        }
    }

    #[test]
    fn alerts_escalate_by_duration_retained_bytes_and_free_space() {
        let guard = ArchiveGuard::new(ArchiveGuardConfig::default());
        assert_eq!(
            guard.evaluate(&inputs(90 * GB, 0, None)).level,
            AlertLevel::Ok
        );
        // A fresh failure is reported by the archiver itself; the guard
        // escalates it with its duration.
        assert_eq!(
            guard.evaluate(&inputs(90 * GB, 0, Some(10))).level,
            AlertLevel::Ok
        );
        assert_eq!(
            guard.evaluate(&inputs(90 * GB, 0, Some(301))).level,
            AlertLevel::Warning
        );
        assert_eq!(
            guard.evaluate(&inputs(90 * GB, 0, Some(1801))).level,
            AlertLevel::Critical
        );
        assert_eq!(
            guard.evaluate(&inputs(90 * GB, 9 * GIB, None)).level,
            AlertLevel::Warning
        );
        assert_eq!(
            guard.evaluate(&inputs(90 * GB, 33 * GIB, None)).level,
            AlertLevel::Critical
        );
        assert_eq!(
            guard.evaluate(&inputs(15 * GB, 0, None)).level,
            AlertLevel::Warning
        );
        let low = guard.evaluate(&inputs(5 * GB, 0, None));
        assert_eq!(low.level, AlertLevel::Critical);
        assert!(low.reasons[0].starts_with("critical"), "{:?}", low.reasons);
        let mut verification = inputs(90 * GB, 0, None);
        verification.verification_failed = true;
        assert_eq!(guard.evaluate(&verification).level, AlertLevel::Critical);
    }

    #[test]
    fn backpressure_needs_a_degraded_archive_and_escalates_then_lifts() {
        let guard = ArchiveGuard::new(ArchiveGuardConfig::default());
        // Low space alone, with a healthy archive retaining nothing: alert
        // only, never refuse writes on the archive's account.
        assert_eq!(
            guard.evaluate(&inputs(2 * GB, 0, None)).backpressure,
            Backpressure::None
        );
        // A failed archive: throttle below 10 % free, refuse below 5 %.
        assert_eq!(
            guard.evaluate(&inputs(50 * GB, 0, Some(5))).backpressure,
            Backpressure::None
        );
        assert_eq!(
            guard.evaluate(&inputs(9 * GB, 0, Some(5))).backpressure,
            Backpressure::Throttle
        );
        assert_eq!(
            guard.evaluate(&inputs(4 * GB, 0, Some(5))).backpressure,
            Backpressure::Refuse
        );
        // Retained bytes alone escalate a lagging (not failed) archive.
        assert_eq!(
            guard
                .evaluate(&inputs(50 * GB, 33 * GIB, None))
                .backpressure,
            Backpressure::Throttle
        );
        assert_eq!(
            guard
                .evaluate(&inputs(50 * GB, 65 * GIB, None))
                .backpressure,
            Backpressure::Refuse
        );
        // The archive recovered: retained bytes drained, failure gone.
        assert_eq!(
            guard.evaluate(&inputs(50 * GB, 0, None)).backpressure,
            Backpressure::None
        );
    }

    #[test]
    fn time_to_full_follows_the_consumption_rate() {
        let guard = ArchiveGuard::new(ArchiveGuardConfig::default());
        let mut sample = inputs(50 * GB, 0, None);
        sample.now_unix_millis = 0;
        assert_eq!(
            guard.evaluate(&sample).data_volume.unwrap().seconds_to_full,
            None
        );
        // 1 GB per second for 10 s.
        sample.now_unix_millis = 10_000;
        sample.data_volume.as_mut().unwrap().free_bytes = 40 * GB;
        let first = guard
            .evaluate(&sample)
            .data_volume
            .unwrap()
            .seconds_to_full
            .unwrap();
        sample.now_unix_millis = 20_000;
        sample.data_volume.as_mut().unwrap().free_bytes = 30 * GB;
        let second = guard
            .evaluate(&sample)
            .data_volume
            .unwrap()
            .seconds_to_full
            .unwrap();
        assert!(first > 40 && first < 200, "{first}");
        assert!((30..=60).contains(&second), "{second}");
        // Space came back (a cleanup): not filling, no projection.
        sample.now_unix_millis = 30_000;
        sample.data_volume.as_mut().unwrap().free_bytes = 80 * GB;
        assert_eq!(
            guard.evaluate(&sample).data_volume.unwrap().seconds_to_full,
            None
        );
    }

    #[test]
    fn thresholds_must_escalate() {
        let mut config = ArchiveGuardConfig::default();
        assert!(config.validate().is_ok());
        config.refuse_free_fraction = 0.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn the_gate_refuses_and_lifts() {
        let gate = BackpressureGate::default();
        assert!(gate.enforce("commit").is_ok());
        gate.set(Backpressure::Refuse, "test", Duration::ZERO);
        let error = gate.enforce("commit").unwrap_err();
        assert!(
            matches!(error, BicDbError::ArchiveBackpressure(_)),
            "{error}"
        );
        gate.set(Backpressure::Throttle, "test", Duration::from_millis(30));
        let started = std::time::Instant::now();
        gate.enforce("commit write transaction").unwrap();
        assert!(started.elapsed() >= Duration::from_millis(30));
        gate.set(Backpressure::None, "", Duration::ZERO);
        assert!(gate.enforce("commit").is_ok());
    }
}
