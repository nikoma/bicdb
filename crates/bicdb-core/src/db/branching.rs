//! Database branches: an independent copy of a database at an exact commit
//! boundary, and a comparison of two databases record by record.
//!
//! A branch is taken from the open database while it keeps serving: commits
//! pause for the copy (on copy-on-write filesystems the copy shares blocks
//! and takes moments), and the branch then evolves on its own.
use super::*;

/// Records compared per batch when diffing.
const DIFF_BATCH_ROWS: usize = 2048;
const DIFF_BATCH_BYTES: usize = 32 * 1024 * 1024;

/// How a record differs between two databases.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordChangeKind {
    /// Only in the compared database (added on the branch).
    Added,
    /// Only in the base database (removed on the branch).
    Removed,
    /// In both, with different contents.
    Changed,
}

impl RecordChangeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Removed => "removed",
            Self::Changed => "changed",
        }
    }
}

/// One differing record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordChange {
    pub collection: String,
    pub id: String,
    pub change: RecordChangeKind,
}

/// Differences in one collection.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionDiff {
    pub collection: String,
    pub added: u64,
    pub removed: u64,
    pub changed: u64,
    /// The collection exists only in the compared database / only in the base.
    pub only_in_compared: bool,
    pub only_in_base: bool,
}

/// The result of comparing two databases.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseDiff {
    /// Collections that differ, by name.
    pub collections: Vec<CollectionDiff>,
    /// Up to the requested number of individual changes, in collection and
    /// id order.
    pub changes: Vec<RecordChange>,
}

impl BicDb {
    /// Creates a branch of this database in `target`, which must not exist:
    /// a complete, independent database holding exactly the commits up to the
    /// returned commit sequence number.
    pub fn create_branch(&self, target: &Path) -> Result<u64> {
        if target.exists() {
            return Err(BicDbError::Branch(format!(
                "branch target {} already exists",
                target.display()
            )));
        }
        let mut staging_name = target
            .file_name()
            .map(|name| name.to_os_string())
            .unwrap_or_default();
        staging_name.push(".branching");
        let staging = target.with_file_name(staging_name);
        if staging.exists() {
            fs::remove_dir_all(&staging)?;
        }
        fs::create_dir_all(&staging)?;
        let copied = (|| {
            // No commit may be in flight or start while the files are copied.
            let _quiesced = self.serializable_commit_admission.write();
            self.flush()?;
            crate::backup::copy_open_database_files(&self.path, &staging, self.config.fsync)?;
            Ok::<_, BicDbError>(self.last_applied_commit_seq())
        })();
        let commit_seq = match copied {
            Ok(commit_seq) => commit_seq,
            Err(error) => {
                let _ = fs::remove_dir_all(&staging);
                return Err(error);
            }
        };
        fs::rename(&staging, target)?;
        Ok(commit_seq)
    }
}

/// Compares `compared` (typically a branch) against `base`, collection by
/// collection and record by record. System collections are skipped. Keeps up
/// to `change_limit` individual changes.
pub fn diff_databases(compared: &BicDb, base: &BicDb, change_limit: usize) -> Result<DatabaseDiff> {
    let names = |db: &BicDb| {
        db.collections()
            .into_iter()
            .map(|meta| meta.name)
            .filter(|name| !name.starts_with("__bicdb"))
            .collect::<BTreeSet<_>>()
    };
    let (compared_names, base_names) = (names(compared), names(base));
    let mut diff = DatabaseDiff::default();
    for name in compared_names.union(&base_names) {
        let mut collection = CollectionDiff {
            collection: name.clone(),
            only_in_compared: !base_names.contains(name),
            only_in_base: !compared_names.contains(name),
            ..CollectionDiff::default()
        };
        let mut left = RecordCursor::new(compared, name, compared_names.contains(name));
        let mut right = RecordCursor::new(base, name, base_names.contains(name));
        loop {
            let change = match (left.peek()?, right.peek()?) {
                (None, None) => break,
                (Some(record), None) => {
                    let id = record.id.clone();
                    left.advance();
                    (id, RecordChangeKind::Added)
                }
                (None, Some(record)) => {
                    let id = record.id.clone();
                    right.advance();
                    (id, RecordChangeKind::Removed)
                }
                (Some(ours), Some(theirs)) => match ours.id.cmp(&theirs.id) {
                    std::cmp::Ordering::Less => {
                        let id = ours.id.clone();
                        left.advance();
                        (id, RecordChangeKind::Added)
                    }
                    std::cmp::Ordering::Greater => {
                        let id = theirs.id.clone();
                        right.advance();
                        (id, RecordChangeKind::Removed)
                    }
                    std::cmp::Ordering::Equal => {
                        let same = ours == theirs;
                        let id = ours.id.clone();
                        left.advance();
                        right.advance();
                        if same {
                            continue;
                        }
                        (id, RecordChangeKind::Changed)
                    }
                },
            };
            match change.1 {
                RecordChangeKind::Added => collection.added += 1,
                RecordChangeKind::Removed => collection.removed += 1,
                RecordChangeKind::Changed => collection.changed += 1,
            }
            if diff.changes.len() < change_limit {
                diff.changes.push(RecordChange {
                    collection: name.clone(),
                    id: change.0,
                    change: change.1,
                });
            }
        }
        if collection.added + collection.removed + collection.changed > 0
            || collection.only_in_compared
            || collection.only_in_base
        {
            diff.collections.push(collection);
        }
    }
    Ok(diff)
}

/// Reads a collection in record id order, a batch at a time.
struct RecordCursor<'db> {
    db: &'db BicDb,
    collection: &'db str,
    batch: std::collections::VecDeque<Record>,
    after: Option<String>,
    exhausted: bool,
}

impl<'db> RecordCursor<'db> {
    fn new(db: &'db BicDb, collection: &'db str, exists: bool) -> Self {
        Self {
            db,
            collection,
            batch: Default::default(),
            after: None,
            exhausted: !exists,
        }
    }

    fn peek(&mut self) -> Result<Option<&Record>> {
        if self.batch.is_empty() && !self.exhausted {
            let records = self.db.scan_collection_batch_after(
                self.collection,
                self.after.as_deref(),
                DIFF_BATCH_ROWS,
                DIFF_BATCH_BYTES,
            )?;
            if records.is_empty() {
                self.exhausted = true;
            }
            self.after = records.last().map(|record| record.id.clone());
            self.batch.extend(records);
        }
        Ok(self.batch.front())
    }

    fn advance(&mut self) {
        self.batch.pop_front();
    }
}
