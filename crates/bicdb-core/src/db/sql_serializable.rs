//! PostgreSQL SSI dependency decisions. The executor supplies only
//! read-write edges between overlapping snapshots. A successful decision
//! becomes visible to other participants after the durable commit.

use std::collections::{BTreeMap, BTreeSet};

type ParticipantId = u64;

#[derive(Default)]
struct Dependencies {
    order: u64,
    participants: BTreeMap<ParticipantId, Participant>,
}

struct Participant {
    snapshot: u64,
    read_only: bool,
    committed: Option<u64>,
    doomed: bool,
    readers: BTreeSet<ParticipantId>,
    writers: BTreeSet<ParticipantId>,
}

struct CommitDecision {
    transaction: ParticipantId,
    order: u64,
    victims: BTreeSet<ParticipantId>,
}

impl Dependencies {
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
                }
            )
            .is_none());
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

    fn did_commit(&mut self, decision: CommitDecision) {
        assert_eq!(
            decision.order,
            self.order + 1,
            "commit decision was superseded"
        );
        self.order = decision.order;
        for victim in decision.victims {
            self.participants.get_mut(&victim).unwrap().doomed = true;
        }
        self.participants
            .get_mut(&decision.transaction)
            .unwrap()
            .committed = Some(self.order);
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
    let mut graph = Dependencies::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.observe(1, 2);
    graph.did_commit(graph.prepare_commit(2).unwrap());
    graph.did_commit(graph.prepare_commit(1).unwrap());
    assert!(graph.participants.is_empty());
}

#[test]
fn a_failed_commit_does_not_doom_the_other_side_of_a_cycle() {
    let mut graph = Dependencies::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.observe(1, 2);
    graph.observe(2, 1);
    let failed_commit = graph.prepare_commit(1).unwrap();
    assert_eq!(failed_commit.victims, BTreeSet::from([2]));
    drop(failed_commit);
    graph.abort(1);
    graph.did_commit(graph.prepare_commit(2).unwrap());
}

#[test]
fn the_first_successful_commit_in_a_cycle_dooms_the_pivot() {
    let mut graph = Dependencies::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.observe(1, 2);
    graph.observe(2, 1);
    graph.did_commit(graph.prepare_commit(1).unwrap());
    assert!(graph.prepare_commit(2).is_none());
    graph.abort(2);
    assert!(graph.participants.is_empty());
}

#[test]
fn a_read_only_snapshot_can_create_a_dangerous_structure_only_after_the_out_commit() {
    for read_only_starts_late in [false, true] {
        let mut graph = Dependencies::default();
        graph.begin(1, false);
        graph.begin(2, false);
        if !read_only_starts_late {
            graph.begin(3, true);
        }
        graph.observe(2, 1);
        graph.did_commit(graph.prepare_commit(1).unwrap());
        if read_only_starts_late {
            graph.begin(3, true);
        }
        graph.observe(3, 2);
        assert_eq!(graph.prepare_commit(2).is_none(), read_only_starts_late);
    }
}

#[test]
fn late_dependency_to_committed_pivot_rejects_the_live_reader() {
    let mut graph = Dependencies::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.begin(3, false);
    graph.observe(2, 1);
    graph.did_commit(graph.prepare_commit(1).unwrap());
    graph.did_commit(graph.prepare_commit(2).unwrap());
    graph.observe(3, 2);
    assert!(graph.prepare_commit(3).is_none());
}

#[test]
fn an_outgoing_writer_that_commits_last_cannot_close_a_dangerous_structure() {
    let mut graph = Dependencies::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.begin(3, false);
    graph.observe(2, 1);
    graph.observe(3, 2);
    graph.did_commit(graph.prepare_commit(2).unwrap());
    graph.did_commit(graph.prepare_commit(3).unwrap());
    graph.did_commit(graph.prepare_commit(1).unwrap());
}

#[test]
fn finished_components_are_reclaimed_without_waiting_for_quiescence() {
    let mut graph = Dependencies::default();
    graph.begin(1, false);
    graph.begin(2, false);
    graph.begin(3, false);
    graph.observe(2, 1);
    graph.did_commit(graph.prepare_commit(1).unwrap());
    graph.did_commit(graph.prepare_commit(2).unwrap());
    graph.begin(4, false);
    assert!(graph.participants.contains_key(&1));
    graph.abort(3);
    assert_eq!(graph.participants.keys().copied().collect::<Vec<_>>(), [4]);
}
