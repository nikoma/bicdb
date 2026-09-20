//! Optional row-lock deadlock detection. Edges exist only for live waiters;
//! independent concurrent waits by the same transaction are reference counted.
use super::*;

#[derive(Debug, Default)]
pub(super) struct WaitForGraph {
    edges: Mutex<FxHashMap<TransactionId, smallvec::SmallVec<[TransactionId; 1]>>>,
}

pub(super) struct WaitEdge<'a> {
    graph: &'a WaitForGraph,
    waiter: TransactionId,
    owner: TransactionId,
}

impl WaitEdge<'_> {
    pub(super) fn owner(&self) -> TransactionId {
        self.owner
    }
}

impl WaitForGraph {
    /// Atomically detect a cycle and register an acyclic wait. The caller
    /// holds the owning row-lock shard, so `owner` cannot change underneath
    /// this check. No graph operation acquires a row-lock shard.
    pub(super) fn wait_for(
        &self,
        waiter: TransactionId,
        owner: TransactionId,
    ) -> Option<WaitEdge<'_>> {
        let mut edges = self.edges.lock();
        let mut pending = smallvec::SmallVec::<[TransactionId; 8]>::new();
        let mut visited = rustc_hash::FxHashSet::default();
        pending.push(owner);
        while let Some(next) = pending.pop() {
            if next == waiter {
                return None;
            }
            if !visited.insert(next) {
                continue;
            }
            if let Some(owners) = edges.get(&next) {
                pending.extend(owners.iter().copied());
            }
        }
        edges.entry(waiter).or_default().push(owner);
        Some(WaitEdge {
            graph: self,
            waiter,
            owner,
        })
    }
}

impl Drop for WaitEdge<'_> {
    fn drop(&mut self) {
        let mut edges = self.graph.edges.lock();
        if let Some(owners) = edges.get_mut(&self.waiter) {
            if let Some(index) = owners.iter().position(|owner| *owner == self.owner) {
                owners.swap_remove(index);
            }
            if owners.is_empty() {
                edges.remove(&self.waiter);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_actual_cycles_are_rejected_and_drop_removes_edges() {
        let graph = WaitForGraph::default();
        let a = graph.wait_for(TransactionId(1), TransactionId(2)).unwrap();
        let b = graph.wait_for(TransactionId(2), TransactionId(3)).unwrap();
        assert!(graph.wait_for(TransactionId(3), TransactionId(1)).is_none());
        drop(b);
        let c = graph.wait_for(TransactionId(3), TransactionId(1)).unwrap();
        drop((a, c));
        assert!(graph.edges.lock().is_empty());
    }

    #[test]
    fn parallel_edges_do_not_disappear_when_one_wait_finishes() {
        let graph = WaitForGraph::default();
        let a = graph.wait_for(TransactionId(1), TransactionId(2)).unwrap();
        let b = graph.wait_for(TransactionId(1), TransactionId(2)).unwrap();
        drop(a);
        assert!(graph.wait_for(TransactionId(2), TransactionId(1)).is_none());
        drop(b);
        assert!(graph.wait_for(TransactionId(2), TransactionId(1)).is_some());
        assert!(graph.edges.lock().is_empty());
    }

    #[test]
    fn simultaneous_cycle_registration_rejects_exactly_one_waiter() {
        let graph = WaitForGraph::default();
        let barrier = std::sync::Barrier::new(2);
        let accepted = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for (waiter, owner) in [(1, 2), (2, 1)] {
                let (graph, barrier, accepted) = (&graph, &barrier, &accepted);
                scope.spawn(move || {
                    let edge = graph.wait_for(TransactionId(waiter), TransactionId(owner));
                    if edge.is_some() {
                        accepted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    barrier.wait();
                    drop(edge);
                });
            }
        });
        assert_eq!(accepted.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!(graph.edges.lock().is_empty());
    }
}
