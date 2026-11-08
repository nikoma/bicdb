//! Resolving commits whose outcome a durability failure made unknown.
//!
//! A commit that fails with `CommitOutcomeUnknown` (SQLSTATE 08007) carries
//! the identity needed to resolve it later: the handle **incarnation** (a
//! random id chosen at every open), the **commit sequence** it reserved, and
//! its transaction id. Commit sequences and transaction ids are recovered
//! from the log at open, so a later incarnation can reuse the numbers of a
//! commit that never reached the disk. The incarnation is what makes the
//! pair unique.
//!
//! Every open appends one record to `incarnations.log` before the database
//! accepts any commit: its incarnation and the commit sequence through which
//! recovery rebuilt the database (the contiguous durable prefix). An
//! uncertain commit `(incarnation I, sequence N)` therefore resolves against
//! the first open after `I`:
//!
//! - `N <= recovered_through_seq`: the commit's decision reached the disk and
//!   recovery kept it. **Committed.** Within one incarnation a sequence is
//!   reserved by exactly one transaction, so the kept commit is that one.
//! - otherwise: recovery did not find it. **Not committed.**
//!
//! While `I` is still the open handle, or no later open is recorded (or the
//! record was pruned), the answer is `Unresolved`, never a guess.
//!
//! The answer is about this database's history. After a failover the
//! promoted standby's history is authoritative; see
//! `docs/durability-failure-handling.md`.

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{BicDbError, Result};

/// File holding one record per open; see the module documentation.
pub const INCARNATION_JOURNAL: &str = "incarnations.log";

/// Records kept after pruning. Each open adds one line (about 120 bytes),
/// so the journal stays under about half a megabyte.
const KEEP_RECORDS: usize = 2_048;
const PRUNE_AT_BYTES: u64 = 512 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IncarnationRecord {
    incarnation: String,
    opened_at_unix_millis: u64,
    /// Highest commit sequence recovered at this open: every sequence at or
    /// below it is committed (or was an empty slot), none above it is.
    recovered_through_seq: u64,
}

/// The definitive outcome of a commit, or `Unresolved` when the evidence
/// needed to decide does not exist (yet).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitResolution {
    Committed,
    NotCommitted,
    Unresolved,
}

impl CommitResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::NotCommitted => "not_committed",
            Self::Unresolved => "unresolved",
        }
    }
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn read_records(root: &Path) -> Result<Vec<IncarnationRecord>> {
    let text = match std::fs::read_to_string(root.join(INCARNATION_JOURNAL)) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    // A crash can tear only the last line, and an open whose record was torn
    // never completed, so it took no commit; skipping it is exact.
    Ok(text
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect())
}

/// Append this open's record and make it durable. Runs after recovery and
/// before the database accepts any commit; a failure fails the open.
pub(crate) fn record_incarnation(
    root: &Path,
    incarnation: &str,
    recovered_through_seq: u64,
    fsync: bool,
) -> Result<()> {
    // Shared backup write gate: an online backup streams this directory and
    // must never see the journal change under it.
    let _backup_gate = crate::storage::acquire_database_backup_write_guard(root)?;
    let path = root.join(INCARNATION_JOURNAL);
    let created = !path.exists();
    let record = IncarnationRecord {
        incarnation: incarnation.to_string(),
        opened_at_unix_millis: unix_millis(),
        recovered_through_seq,
    };
    let mut line = serde_json::to_vec(&record)?;
    line.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&path)?;
    let len = file.metadata()?.len();
    if len > 0 {
        // Never glue this record onto a torn previous line.
        let mut last = [0u8; 1];
        file.seek(SeekFrom::Start(len - 1))?;
        file.read_exact(&mut last)?;
        if last[0] != b'\n' {
            line.insert(0, b'\n');
        }
    }
    file.write_all(&line)?;
    if fsync {
        file.sync_data()?;
        if created {
            std::fs::File::open(root)?.sync_all()?;
        }
    }
    drop(file);
    if len + line.len() as u64 <= PRUNE_AT_BYTES {
        return Ok(());
    }
    let records = read_records(root)?;
    if records.len() > KEEP_RECORDS {
        let mut kept = Vec::new();
        for record in &records[records.len() - KEEP_RECORDS..] {
            kept.extend_from_slice(&serde_json::to_vec(record)?);
            kept.push(b'\n');
        }
        crate::storage::write_atomic(&path, &kept, fsync)?;
    }
    Ok(())
}

/// Resolve the commit `sequence` reserved by incarnation `incarnation`.
pub(crate) fn resolve(
    root: &Path,
    current_incarnation: &str,
    incarnation: &str,
    sequence: u64,
) -> Result<CommitResolution> {
    if incarnation.is_empty() {
        return Err(BicDbError::InvalidEvent(
            "commit outcome resolution needs the incarnation reported with the commit".into(),
        ));
    }
    if incarnation == current_incarnation {
        // The handle that reported the outcome unknown is still open; only a
        // reopen's recovery decides.
        return Ok(CommitResolution::Unresolved);
    }
    let records = read_records(root)?;
    let Some(position) = records
        .iter()
        .position(|record| record.incarnation == incarnation)
    else {
        return Ok(CommitResolution::Unresolved);
    };
    let Some(next) = records.get(position + 1) else {
        return Ok(CommitResolution::Unresolved);
    };
    Ok(if sequence <= next.recovered_through_seq {
        CommitResolution::Committed
    } else {
        CommitResolution::NotCommitted
    })
}

impl super::BicDb {
    /// This handle's incarnation: a random id chosen at open. It is part of
    /// the identity a `CommitOutcomeUnknown` reports.
    pub fn incarnation_id(&self) -> &str {
        &self.incarnation
    }

    /// Resolve a commit whose outcome was reported unknown (08007), by the
    /// incarnation and commit sequence the error carried. Call it on the
    /// reopened database (any later incarnation). Not fenced: it reads only
    /// the incarnation journal.
    pub fn resolve_commit_outcome(
        &self,
        incarnation: &str,
        sequence: u64,
    ) -> Result<CommitResolution> {
        resolve(&self.path, &self.incarnation, incarnation, sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_against_the_next_open_and_never_guesses() {
        let dir = tempfile::tempdir().unwrap();
        record_incarnation(dir.path(), "a", 10, true).unwrap();
        assert_eq!(
            resolve(dir.path(), "a", "a", 11).unwrap(),
            CommitResolution::Unresolved
        );
        assert_eq!(
            resolve(dir.path(), "z", "a", 11).unwrap(),
            CommitResolution::Unresolved,
            "no later open recorded yet"
        );
        record_incarnation(dir.path(), "b", 12, true).unwrap();
        assert_eq!(
            resolve(dir.path(), "b", "a", 12).unwrap(),
            CommitResolution::Committed
        );
        assert_eq!(
            resolve(dir.path(), "b", "a", 13).unwrap(),
            CommitResolution::NotCommitted
        );
        assert_eq!(
            resolve(dir.path(), "b", "unknown", 1).unwrap(),
            CommitResolution::Unresolved
        );
    }

    #[test]
    fn a_torn_last_record_is_skipped_and_never_glued() {
        let dir = tempfile::tempdir().unwrap();
        record_incarnation(dir.path(), "a", 5, true).unwrap();
        let path = dir.path().join(INCARNATION_JOURNAL);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"incarnation\":\"torn").unwrap();
        drop(file);
        record_incarnation(dir.path(), "b", 5, true).unwrap();
        assert_eq!(read_records(dir.path()).unwrap().len(), 2);
        assert_eq!(
            resolve(dir.path(), "b", "a", 6).unwrap(),
            CommitResolution::NotCommitted
        );
    }

    #[test]
    fn pruning_keeps_the_newest_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = 0usize;
        while std::fs::metadata(dir.path().join(INCARNATION_JOURNAL)).map_or(0, |meta| meta.len())
            < PRUNE_AT_BYTES - 1_024
        {
            record_incarnation(dir.path(), &format!("i{index}"), index as u64, false).unwrap();
            index += 1;
        }
        for _ in 0..16 {
            record_incarnation(dir.path(), &format!("i{index}"), index as u64, false).unwrap();
            index += 1;
        }
        let records = read_records(dir.path()).unwrap();
        assert!(records.len() <= KEEP_RECORDS + 16, "{}", records.len());
        assert_eq!(
            records.last().unwrap().incarnation,
            format!("i{}", index - 1)
        );
    }
}
