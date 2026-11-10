//! PostgreSQL SSI dependency decisions. The executor supplies only
//! read-write edges between overlapping snapshots. A successful decision
//! becomes visible to other participants after the durable commit.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

type ParticipantId = u64;

struct Dependencies<Collection = u64> {
    order: u64,
    participants: BTreeMap<ParticipantId, Participant<Collection>>,
}

struct Participant<Collection> {
    snapshot: u64,
    read_only: bool,
    committed: Option<u64>,
    doomed: bool,
    readers: BTreeSet<ParticipantId>,
    writers: BTreeSet<ParticipantId>,
    read_scopes: BTreeSet<ReadScope<Collection>>,
    committed_writes: Vec<WriteFootprint<Collection>>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ReadScope<Collection> {
    collection: Collection,
    keys: ReadKeys,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum ReadKeys {
    Relation,
    Record(KeyRange),
    Index { identity: String, range: KeyRange },
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum KeyRange {
    Exact(Vec<u8>),
    Prefix(Vec<u8>),
    Bounded {
        // Bound itself does not define an ordering for use in the read set.
        lower: Option<(Vec<u8>, bool)>,
        upper: Option<(Vec<u8>, bool)>,
    },
}

impl KeyRange {
    fn bounded(lower: Bound<Vec<u8>>, upper: Bound<Vec<u8>>) -> Self {
        fn endpoint(bound: Bound<Vec<u8>>) -> Option<(Vec<u8>, bool)> {
            match bound {
                Bound::Unbounded => None,
                Bound::Included(key) => Some((key, true)),
                Bound::Excluded(key) => Some((key, false)),
            }
        }
        Self::Bounded {
            lower: endpoint(lower),
            upper: endpoint(upper),
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        match self {
            Self::Exact(target) => key == target,
            Self::Prefix(prefix) => key.starts_with(prefix),
            Self::Bounded { lower, upper } => {
                lower.as_ref().is_none_or(|(bound, inclusive)| {
                    key > bound.as_slice() || (*inclusive && key == bound)
                }) && upper.as_ref().is_none_or(|(bound, inclusive)| {
                    key < bound.as_slice() || (*inclusive && key == bound)
                })
            }
        }
    }
}

struct WriteFootprint<Collection> {
    collection: Collection,
    record_key: Vec<u8>,
    /// Both old and new keys, including partial-index membership. An empty
    /// entry means neither image belongs to that index. An absent entry means
    /// the writer cannot prove membership and may intersect any index read.
    index_keys: BTreeMap<String, BTreeSet<Vec<u8>>>,
}

impl<Collection: Eq> ReadScope<Collection> {
    fn intersects(&self, write: &WriteFootprint<Collection>) -> bool {
        self.collection == write.collection
            && match &self.keys {
                ReadKeys::Relation => true,
                ReadKeys::Record(range) => range.contains(&write.record_key),
                ReadKeys::Index { identity, range } => write
                    .index_keys
                    .get(identity)
                    .is_none_or(|keys| keys.iter().any(|key| range.contains(key))),
            }
    }
}

struct CommitDecision {
    transaction: ParticipantId,
    order: u64,
    victims: BTreeSet<ParticipantId>,
}

struct SafeSnapshot {
    reader: ParticipantId,
    writers: Vec<ParticipantId>,
}

#[derive(Debug, PartialEq, Eq)]
enum SnapshotSafety {
    Waiting,
    Safe,
    Retry,
}

/// Holds the dependency graph exclusively from validation through the caller's
/// durable commit boundary. Any incoming edges added by this attempt disappear
/// if the caller returns an error before publishing the durable result.
struct PendingCommit<'a, Collection: Ord> {
    graph: &'a mut Dependencies<Collection>,
    transaction: ParticipantId,
    added_readers: Vec<ParticipantId>,
    decision: Option<CommitDecision>,
    writes: Vec<WriteFootprint<Collection>>,
}

impl<Collection: Ord> PendingCommit<'_, Collection> {
    fn committed(mut self) {
        let decision = self.decision.take().unwrap();
        self.graph
            .did_commit(decision, std::mem::take(&mut self.writes));
    }
}

impl<Collection: Ord> Drop for PendingCommit<'_, Collection> {
    fn drop(&mut self) {
        if self.decision.is_some() {
            self.graph
                .remove_readers(self.transaction, &self.added_readers);
        }
    }
}

impl<Collection> Default for Dependencies<Collection> {
    fn default() -> Self {
        Self {
            order: 0,
            participants: BTreeMap::new(),
        }
    }
}

impl<Collection: Ord> Dependencies<Collection> {
    fn begin(&mut self, id: ParticipantId, read_only: bool) {
        self.order += 1;
        assert!(self
            .participants
            .insert(
                id,
                Participant {
                    snapshot: self.order,
                    read_only,
                    committed: None,
                    doomed: false,
                    readers: BTreeSet::new(),
                    writers: BTreeSet::new(),
                    read_scopes: BTreeSet::new(),
                    committed_writes: Vec::new(),
                }
            )
            .is_none());
    }

    fn begin_safe_snapshot(&mut self, id: ParticipantId) -> SafeSnapshot {
        let writers = self
            .participants
            .iter()
            .filter(|(_, transaction)| !transaction.read_only && transaction.committed.is_none())
            .map(|(&id, _)| id)
            .collect();
        self.begin(id, true);
        SafeSnapshot {
            reader: id,
            writers,
        }
    }

    fn snapshot_safety(&self, candidate: &SafeSnapshot) -> SnapshotSafety {
        let reader = &self.participants[&candidate.reader];
        let mut waiting = false;
        for writer in &candidate.writers {
            let Some(writer) = self.participants.get(writer) else {
                // An aborted writer cannot be the pivot of a committed cycle.
                continue;
            };
            if writer.committed.is_none() {
                waiting = true;
            } else if writer.writers.iter().any(|id| {
                self.participants[id]
                    .committed
                    .is_some_and(|end| end < reader.snapshot)
            }) {
                // A concurrent writer with an outgoing dependency to an
                // earlier commit can be the pivot of a read-only anomaly.
                // Discard this snapshot before exposing any of its reads.
                return SnapshotSafety::Retry;
            }
        }
        if waiting {
            SnapshotSafety::Waiting
        } else {
            SnapshotSafety::Safe
        }
    }

    fn observe_read(&mut self, id: ParticipantId, scope: ReadScope<Collection>) {
        let reader = &self.participants[&id];
        assert!(reader.committed.is_none());
        if reader.read_scopes.contains(&scope) {
            return;
        }
        let writers: Vec<_> = self
            .participants
            .iter()
            .filter(|(_, writer)| {
                writer.committed.is_some_and(|end| end > reader.snapshot)
                    && writer
                        .committed_writes
                        .iter()
                        .any(|write| scope.intersects(write))
            })
            .map(|(&writer_id, _)| writer_id)
            .collect();
        for writer in writers {
            self.observe(id, writer);
        }
        self.participants
            .get_mut(&id)
            .unwrap()
            .read_scopes
            .insert(scope);
        // A newly registered read may affect a subsequent writer's decision
        // even when it adds no edge to an already committed writer.
        self.order += 1;
    }

    fn observe_writes(
        &mut self,
        id: ParticipantId,
        writes: &[WriteFootprint<Collection>],
    ) -> Vec<ParticipantId> {
        let writer = &self.participants[&id];
        assert!(writer.committed.is_none());
        assert!(!writer.read_only || writes.is_empty());
        let readers: Vec<_> = self
            .participants
            .iter()
            .filter(|(&reader_id, reader)| {
                reader_id != id
                    && !writer.readers.contains(&reader_id)
                    && reader.committed.is_none_or(|end| end > writer.snapshot)
                    && reader
                        .read_scopes
                        .iter()
                        .any(|scope| writes.iter().any(|write| scope.intersects(write)))
            })
            .map(|(&reader_id, _)| reader_id)
            .collect();
        for &reader in &readers {
            self.observe(reader, id);
        }
        readers
    }

    fn remove_readers(&mut self, writer: ParticipantId, readers: &[ParticipantId]) {
        for reader in readers {
            self.participants
                .get_mut(reader)
                .unwrap()
                .writers
                .remove(&writer);
            self.participants
                .get_mut(&writer)
                .unwrap()
                .readers
                .remove(reader);
        }
    }

    fn prepare_write_commit(
        &mut self,
        id: ParticipantId,
        writes: Vec<WriteFootprint<Collection>>,
    ) -> Option<PendingCommit<'_, Collection>> {
        let added_readers = self.observe_writes(id, &writes);
        let Some(decision) = self.prepare_commit(id) else {
            self.remove_readers(id, &added_readers);
            return None;
        };
        Some(PendingCommit {
            graph: self,
            transaction: id,
            added_readers,
            decision: Some(decision),
            writes,
        })
    }

    fn observe(&mut self, reader: ParticipantId, writer: ParticipantId) {
        if reader == writer {
            return;
        }
        let from = &self.participants[&reader];
        let to = &self.participants[&writer];
        assert!(
            !to.read_only,
            "a read-only transaction cannot be an edge's writer"
        );
        assert!(
            from.committed.is_none() || to.committed.is_none(),
            "a dependency must be observed while one endpoint is still active"
        );
        if from.committed.is_some_and(|end| end < to.snapshot)
            || to.committed.is_some_and(|end| end < from.snapshot)
            || from.writers.contains(&writer)
        {
            return;
        }
        self.participants
            .get_mut(&reader)
            .unwrap()
            .writers
            .insert(writer);
        self.participants
            .get_mut(&writer)
            .unwrap()
            .readers
            .insert(reader);
        self.order += 1;
    }

    /// Decide under the same guard that protects the graph until the caller
    /// learns whether its commit succeeded. Dropping the decision after an
    /// I/O failure does not mark any peer doomed.
    fn prepare_commit(&self, id: ParticipantId) -> Option<CommitDecision> {
        let committing = &self.participants[&id];
        assert!(committing.committed.is_none());
        if committing.doomed {
            return None;
        }
        let order = self.order + 1;
        let mut victims = BTreeSet::new();
        for (&pivot_id, pivot) in &self.participants {
            if pivot.doomed {
                continue;
            }
            for &out_id in &pivot.writers {
                let out = &self.participants[&out_id];
                let Some(out_commit) = out.committed.or((out_id == id).then_some(order)) else {
                    continue;
                };
                if pivot.committed.is_some_and(|end| end < out_commit) {
                    continue;
                }
                for &in_id in &pivot.readers {
                    let reader = &self.participants[&in_id];
                    if reader.doomed
                        || reader.committed.is_some_and(|end| end < out_commit)
                        || (reader.read_only && reader.snapshot <= out_commit)
                    {
                        continue;
                    }
                    // Prefer a live pivot. A committed pivot leaves its
                    // still-live reader as the participant that can retry.
                    let victim = if pivot.committed.is_none() {
                        pivot_id
                    } else {
                        assert!(reader.committed.is_none());
                        in_id
                    };
                    if victim == id {
                        return None;
                    }
                    victims.insert(victim);
                }
            }
        }
        Some(CommitDecision {
            transaction: id,
            order,
            victims,
        })
    }

    fn did_commit(&mut self, decision: CommitDecision, writes: Vec<WriteFootprint<Collection>>) {
        assert_eq!(
            decision.order,
            self.order + 1,
            "commit decision was superseded"
        );
        self.order = decision.order;
        for victim in decision.victims {
            self.participants.get_mut(&victim).unwrap().doomed = true;
        }
        let participant = self.participants.get_mut(&decision.transaction).unwrap();
        participant.committed = Some(self.order);
        participant.committed_writes = writes;
        self.prune_finished();
    }

    fn abort(&mut self, id: ParticipantId) {
        let aborted = self.participants.remove(&id).unwrap();
        assert!(aborted.committed.is_none());
        for reader in aborted.readers {
            self.participants
                .get_mut(&reader)
                .unwrap()
                .writers
                .remove(&id);
        }
        for writer in aborted.writers {
            self.participants
                .get_mut(&writer)
                .unwrap()
                .readers
                .remove(&id);
        }
        self.order += 1;
        self.prune_finished();
    }

    fn prune_finished(&mut self) {
        let Some(oldest_snapshot) = self
            .participants
            .values()
            .filter(|participant| participant.committed.is_none())
            .map(|participant| participant.snapshot)
            .min()
        else {
            self.participants.clear();
            return;
        };
        // An older commit cannot gain an edge to any live or future
        // participant. Keep it if an existing edge links it to a component
        // that can still gain edges. In particular, a committed writer can
        // establish the commit order of a dangerous structure discovered
        // by a later snapshot.
        let mut retained = BTreeSet::new();
        let mut pending = Vec::new();
        for (&id, participant) in &self.participants {
            if participant
                .committed
                .is_none_or(|end| end >= oldest_snapshot)
            {
                retained.insert(id);
                pending.push(id);
            }
        }
        while let Some(id) = pending.pop() {
            let participant = &self.participants[&id];
            for &peer in participant.readers.iter().chain(&participant.writers) {
                if retained.insert(peer) {
                    pending.push(peer);
                }
            }
        }
        self.participants.retain(|id, _| retained.contains(id));
    }
}

#[test]
fn one_dependency_does_not_abort_either_participant() {
    let mut graph = Dependencies::<u64>::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.observe(1, 2);
    graph.did_commit(graph.prepare_commit(2).unwrap(), Vec::new());
    graph.did_commit(graph.prepare_commit(1).unwrap(), Vec::new());
    assert!(graph.participants.is_empty());
}

#[test]
fn a_failed_commit_does_not_doom_the_other_side_of_a_cycle() {
    let mut graph = Dependencies::<u64>::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.observe(1, 2);
    graph.observe(2, 1);
    let failed_commit = graph.prepare_commit(1).unwrap();
    assert_eq!(failed_commit.victims, BTreeSet::from([2]));
    drop(failed_commit);
    graph.abort(1);
    graph.did_commit(graph.prepare_commit(2).unwrap(), Vec::new());
}

#[test]
fn the_first_successful_commit_in_a_cycle_dooms_the_pivot() {
    let mut graph = Dependencies::<u64>::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.observe(1, 2);
    graph.observe(2, 1);
    graph.did_commit(graph.prepare_commit(1).unwrap(), Vec::new());
    assert!(graph.prepare_commit(2).is_none());
    graph.abort(2);
    assert!(graph.participants.is_empty());
}

#[test]
fn a_read_only_snapshot_can_create_a_dangerous_structure_only_after_the_out_commit() {
    for read_only_starts_late in [false, true] {
        let mut graph = Dependencies::<u64>::default();
        graph.begin(1, false);
        graph.begin(2, false);
        if !read_only_starts_late {
            graph.begin(3, true);
        }
        graph.observe(2, 1);
        graph.did_commit(graph.prepare_commit(1).unwrap(), Vec::new());
        if read_only_starts_late {
            graph.begin(3, true);
        }
        graph.observe(3, 2);
        assert_eq!(graph.prepare_commit(2).is_none(), read_only_starts_late);
    }
}

#[test]
fn late_dependency_to_committed_pivot_rejects_the_live_reader() {
    let mut graph = Dependencies::<u64>::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.begin(3, false);
    graph.observe(2, 1);
    graph.did_commit(graph.prepare_commit(1).unwrap(), Vec::new());
    graph.did_commit(graph.prepare_commit(2).unwrap(), Vec::new());
    graph.observe(3, 2);
    assert!(graph.prepare_commit(3).is_none());
}

#[test]
fn an_outgoing_writer_that_commits_last_cannot_close_a_dangerous_structure() {
    let mut graph = Dependencies::<u64>::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.begin(3, false);
    graph.observe(2, 1);
    graph.observe(3, 2);
    graph.did_commit(graph.prepare_commit(2).unwrap(), Vec::new());
    graph.did_commit(graph.prepare_commit(3).unwrap(), Vec::new());
    graph.did_commit(graph.prepare_commit(1).unwrap(), Vec::new());
}

#[test]
fn finished_components_are_reclaimed_without_waiting_for_quiescence() {
    let mut graph = Dependencies::<u64>::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.begin(3, false);
    graph.observe(2, 1);
    graph.did_commit(graph.prepare_commit(1).unwrap(), Vec::new());
    graph.did_commit(graph.prepare_commit(2).unwrap(), Vec::new());
    graph.begin(4, false);
    assert!(graph.participants.contains_key(&1));
    graph.abort(3);
    assert_eq!(graph.participants.keys().copied().collect::<Vec<_>>(), [4]);
}

fn record_scope(collection: u64, id: &str) -> ReadScope<u64> {
    ReadScope {
        collection,
        keys: ReadKeys::Record(KeyRange::Exact(id.as_bytes().to_vec())),
    }
}

fn write_footprint(collection: u64, id: &str) -> WriteFootprint<u64> {
    WriteFootprint {
        collection,
        record_key: id.as_bytes().to_vec(),
        index_keys: BTreeMap::new(),
    }
}

#[test]
fn row_reads_match_only_the_same_collection_and_key() {
    let mut graph = Dependencies::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.begin(3, false);
    graph.begin(4, false);
    graph.observe_read(1, record_scope(1, "42"));
    graph.observe_writes(2, &[write_footprint(1, "41")]);
    graph.observe_writes(3, &[write_footprint(2, "42")]);
    graph.observe_writes(4, &[write_footprint(1, "42")]);
    assert_eq!(graph.participants[&1].writers, BTreeSet::from([4]));
}

#[test]
fn a_late_read_conflicts_with_a_write_invisible_to_its_snapshot() {
    let mut graph = Dependencies::default();
    graph.begin(1, false);
    graph.begin(2, false);
    let writes = vec![write_footprint(1, "42")];
    graph.observe_writes(2, &writes);
    graph.did_commit(graph.prepare_commit(2).unwrap(), writes);
    graph.begin(3, false);
    graph.observe_read(1, record_scope(1, "42"));
    graph.observe_read(3, record_scope(1, "42"));
    assert_eq!(graph.participants[&1].writers, BTreeSet::from([2]));
    assert!(graph.participants[&3].writers.is_empty());
}

#[test]
fn an_earlier_committed_reader_conflicts_with_its_overlapping_writer() {
    let mut graph = Dependencies::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.observe_read(1, record_scope(1, "42"));
    graph.did_commit(graph.prepare_commit(1).unwrap(), Vec::new());
    graph.observe_writes(2, &[write_footprint(1, "42")]);
    assert_eq!(graph.participants[&2].readers, BTreeSet::from([1]));
}

#[test]
fn an_empty_index_range_conflicts_on_entry_and_exit_at_the_included_boundary() {
    let range = KeyRange::bounded(Bound::Excluded(vec![10]), Bound::Included(vec![20]));
    let scope = ReadScope {
        collection: 1,
        keys: ReadKeys::Index {
            identity: "partial".to_string(),
            range,
        },
    };
    let mut write = write_footprint(1, "42");
    assert!(scope.intersects(&write));
    for (old, new, expected) in [
        (None, None, false),
        (Some(10), Some(21), false),
        (Some(20), None, true),
        (None, Some(20), true),
        (Some(11), Some(21), true),
    ] {
        let keys = [old, new].into_iter().flatten().map(|key| vec![key]);
        write
            .index_keys
            .insert("partial".to_string(), keys.collect());
        assert_eq!(scope.intersects(&write), expected, "{old:?} -> {new:?}");
    }
}

#[test]
fn a_prefix_read_includes_missing_rows_and_a_relation_read_covers_all_keys() {
    let prefix = ReadScope {
        collection: 1,
        keys: ReadKeys::Record(KeyRange::Prefix(b"42,".to_vec())),
    };
    let relation = ReadScope {
        collection: 1,
        keys: ReadKeys::Relation,
    };
    assert!(prefix.intersects(&write_footprint(1, "42,1")));
    assert!(!prefix.intersects(&write_footprint(1, "420,1")));
    assert!(relation.intersects(&write_footprint(1, "420,1")));
    assert!(!relation.intersects(&write_footprint(2, "42,1")));
}

#[test]
fn a_failed_write_commit_rolls_back_its_tentative_edges_before_releasing_the_graph() {
    let mut graph = Dependencies::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.observe_read(1, record_scope(1, "42"));
    graph.observe(2, 1);
    {
        let attempt = graph
            .prepare_write_commit(2, vec![write_footprint(1, "42")])
            .unwrap();
        assert_eq!(
            attempt.decision.as_ref().unwrap().victims,
            BTreeSet::from([1])
        );
    }
    assert!(graph.participants[&1].writers.is_empty());
    assert!(!graph.participants[&1].doomed);
    assert!(graph.participants[&2].readers.is_empty());
    assert_eq!(graph.participants[&2].writers, BTreeSet::from([1]));
    graph
        .prepare_write_commit(2, vec![write_footprint(1, "42")])
        .unwrap()
        .committed();
    assert!(graph.participants[&1].doomed);
}

#[test]
fn a_deferrable_snapshot_waits_for_its_existing_writers_only() {
    let mut graph = Dependencies::<u64>::default();
    graph.begin(1, false);
    let snapshot = graph.begin_safe_snapshot(2);
    graph.begin(3, false);
    assert_eq!(graph.snapshot_safety(&snapshot), SnapshotSafety::Waiting);
    graph.did_commit(graph.prepare_commit(1).unwrap(), Vec::new());
    assert_eq!(graph.snapshot_safety(&snapshot), SnapshotSafety::Safe);
}

#[test]
fn a_deferrable_snapshot_retries_when_an_overlapping_writer_has_an_earlier_out_commit() {
    for writer_commits in [false, true] {
        let mut graph = Dependencies::<u64>::default();
        graph.begin(1, false);
        graph.begin(2, false);
        graph.observe(2, 1);
        graph.did_commit(graph.prepare_commit(1).unwrap(), Vec::new());
        let snapshot = graph.begin_safe_snapshot(3);
        assert_eq!(graph.snapshot_safety(&snapshot), SnapshotSafety::Waiting);
        if writer_commits {
            graph.did_commit(graph.prepare_commit(2).unwrap(), Vec::new());
            assert_eq!(graph.snapshot_safety(&snapshot), SnapshotSafety::Retry);
        } else {
            graph.abort(2);
            assert_eq!(graph.snapshot_safety(&snapshot), SnapshotSafety::Safe);
        }
    }
}
