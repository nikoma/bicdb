//! Opt-in key-specific row-lock notifications. Registration happens while the
//! row-lock shard is held, so a release cannot pass between checking ownership
//! and registering the condition variable. Queue operations never take a row
//! lock; the lock order is always row shard -> notification registry.
use super::*;

fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_ROW_LOCK_KEY_NOTIFY")
            .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "on"))
    })
}

fn wait_period() -> Duration {
    static PERIOD: OnceLock<Duration> = OnceLock::new();
    *PERIOD.get_or_init(|| {
        Duration::from_micros(
            std::env::var("BICDB_ROW_LOCK_KEY_WAIT_US")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(250)
                .clamp(1, 250_000),
        )
    })
}

fn notify_one_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_ROW_LOCK_KEY_NOTIFY_ONE")
            .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "on"))
    })
}

#[derive(Debug)]
struct Queue {
    signal: Arc<Condvar>,
    waiters: usize,
}

type QueueShard = Mutex<FxHashMap<RecordLockKey, Queue>>;

#[derive(Debug)]
pub(super) struct RowLockTable {
    inner: ShardedMutexMap<RecordLockKey, TransactionId>,
    queues: OnceLock<Box<[QueueShard]>>,
}

impl std::ops::Deref for RowLockTable {
    type Target = ShardedMutexMap<RecordLockKey, TransactionId>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl RowLockTable {
    pub(super) fn with_shards(count: usize) -> Self {
        Self {
            inner: ShardedMutexMap::with_shards(count),
            queues: OnceLock::new(),
        }
    }

    pub(super) fn register_if_enabled(
        &self,
        key: &RecordLockKey,
        shard: usize,
        max_attempts: usize,
    ) -> Option<KeyWait<'_>> {
        enabled().then(|| {
            let mut wait = self.register(key, shard);
            // A longer condition-variable sleep must not multiply the existing
            // total timeout. Owner release still wakes the waiter immediately.
            if wait_period() > Duration::from_micros(250) {
                let budget = Duration::from_micros(250)
                    .saturating_mul(u32::try_from(max_attempts).unwrap_or(u32::MAX));
                wait.deadline = Some(Instant::now() + budget);
            }
            wait
        })
    }

    fn register(&self, key: &RecordLockKey, shard: usize) -> KeyWait<'_> {
        let queues = self.queues.get_or_init(|| {
            (0..self.inner.shards.len())
                .map(|_| Mutex::new(FxHashMap::default()))
                .collect()
        });
        let mut entries = queues[shard].lock();
        let queue = entries.entry(key.clone()).or_insert_with(|| Queue {
            signal: Arc::new(Condvar::new()),
            waiters: 0,
        });
        queue.waiters += 1;
        KeyWait {
            registry: &queues[shard],
            key: key.clone(),
            signal: queue.signal.clone(),
            deadline: None,
        }
    }

    pub(super) fn notify_released(&self, key: &RecordLockKey, shard: usize) {
        if enabled() {
            self.notify_key(key, shard);
        } else {
            self.inner.waiters[shard].notify_all();
        }
    }

    fn notify_key(&self, key: &RecordLockKey, shard: usize) {
        let signal = self.queues.get().and_then(|queues| {
            queues[shard]
                .lock()
                .get(key)
                .map(|queue| queue.signal.clone())
        });
        if let Some(signal) = signal {
            if notify_one_enabled() {
                signal.notify_one();
            } else {
                signal.notify_all();
            }
        }
    }
}

pub(super) struct KeyWait<'a> {
    registry: &'a QueueShard,
    key: RecordLockKey,
    signal: Arc<Condvar>,
    deadline: Option<Instant>,
}

impl KeyWait<'_> {
    pub(super) fn wait_for(
        &self,
        shard: &mut parking_lot::MutexGuard<'_, FxHashMap<RecordLockKey, TransactionId>>,
    ) -> bool {
        let mut duration = wait_period();
        if let Some(deadline) = self.deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            duration = duration.min(remaining);
        }
        self.signal.wait_for(shard, duration);
        true
    }
}

impl Drop for KeyWait<'_> {
    fn drop(&mut self) {
        let mut entries = self.registry.lock();
        let queue = entries.get_mut(&self.key).expect("registered row waiter");
        queue.waiters -= 1;
        let next = (notify_one_enabled() && queue.waiters > 0).then(|| Arc::clone(&queue.signal));
        if queue.waiters == 0 {
            entries.remove(&self.key);
        }
        drop(entries);
        // A selected waiter may cancel or exhaust its budget instead of
        // taking the row. Pass the wake to another waiter so an unlocked row
        // is not stranded until a polling timeout. On successful acquisition
        // this can cause one harmless ownership recheck, not a whole herd.
        if let Some(signal) = next {
            signal.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_one_cancellation_passes_the_wake_to_the_next_waiter() {
        if !notify_one_enabled() {
            return;
        }
        let table = RowLockTable::with_shards(1);
        let key = ("table".into(), "key".into());
        let (ready_send, ready_recv) = std::sync::mpsc::channel();
        let (wake_send, wake_recv) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let mut releases = Vec::new();
            for position in 0..2 {
                let (release_send, release_recv) = std::sync::mpsc::channel();
                releases.push(release_send);
                let (table, key, ready_send, wake_send) =
                    (&table, &key, ready_send.clone(), wake_send.clone());
                scope.spawn(move || {
                    let mut shard = table.shards[0].lock();
                    let registration = table.register(key, 0);
                    ready_send.send(()).unwrap();
                    let notified = !registration
                        .signal
                        .wait_for(&mut shard, Duration::from_secs(2))
                        .timed_out();
                    drop(shard);
                    wake_send.send((position, notified)).unwrap();
                    release_recv.recv().unwrap();
                    drop(registration);
                });
            }
            for _ in 0..2 {
                ready_recv.recv().unwrap();
            }
            let shard = table.shards[0].lock();
            table.notify_key(&key, 0);
            drop(shard);
            let (first, notified) = wake_recv.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(notified);
            assert!(wake_recv.recv_timeout(Duration::from_millis(20)).is_err());
            releases[first].send(()).unwrap();
            let (second, notified) = wake_recv.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(notified);
            assert_ne!(first, second);
            releases[second].send(()).unwrap();
        });
        assert!(table.queues.get().unwrap()[0].lock().is_empty());
    }

    #[test]
    fn exhausted_budget_does_not_wait_or_extend_the_deadline() {
        let table = RowLockTable::with_shards(1);
        let key = ("table".into(), "key".into());
        let mut registration = table.register(&key, 0);
        registration.deadline = Some(Instant::now());
        let mut shard = table.shards[0].lock();
        assert!(!registration.wait_for(&mut shard));
    }

    #[test]
    fn same_key_shares_signal_and_final_waiter_removes_queue() {
        let table = RowLockTable::with_shards(1);
        let key = ("table".into(), "key".into());
        let a = table.register(&key, 0);
        let b = table.register(&key, 0);
        assert!(Arc::ptr_eq(&a.signal, &b.signal));
        drop(a);
        assert_eq!(table.queues.get().unwrap()[0].lock()[&key].waiters, 1);
        drop(b);
        assert!(table.queues.get().unwrap()[0].lock().is_empty());
    }

    #[test]
    fn matching_key_release_wakes_the_registered_waiter() {
        let table = RowLockTable::with_shards(1);
        let key = ("table".into(), "key".into());
        let registration = table.register(&key, 0);
        let mut shard = table.shards[0].lock();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let held = table.shards[0].lock();
                table.notify_key(&key, 0);
                drop(held);
            });
            let timed_out = registration
                .signal
                .wait_for(&mut shard, Duration::from_secs(2))
                .timed_out();
            drop(shard);
            assert!(
                !timed_out,
                "matching release must notify, not rely on polling"
            );
        });
    }

    #[test]
    fn timed_out_row_lock_removes_its_notification_queue() {
        if !enabled() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let db =
            BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
        let held = Mutex::new(LockedKeys::default());
        let wanted = Mutex::new(LockedKeys::default());
        let owner = TransactionId(1);
        let requester = TransactionId(2);
        db.lock_tx_record_with_attempts(owner, "table", "key", 0, true, true, &held)
            .unwrap();
        assert!(db
            .lock_tx_record_with_attempts(requester, "table", "key", 12, true, true, &wanted)
            .is_err());
        let queues = db
            .write_locks
            .queues
            .get()
            .expect("the contention registered a queue");
        assert!(queues.iter().all(|shard| shard.lock().is_empty()));
        release_owned_write_locks(&db.write_locks, owner, &held.lock().take_all());
    }

    #[test]
    fn unrelated_key_release_does_not_notify_a_waiter_on_the_same_shard() {
        let table = RowLockTable::with_shards(1);
        let key = ("table".into(), "a".into());
        let other = ("table".into(), "b".into());
        let mut shard = table.shards[0].lock();
        let registration = table.register(&key, 0);
        let (ready, receive) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let (table, other) = (&table, &other);
            scope.spawn(move || {
                receive.recv().unwrap();
                // Acquiring this lock proves the waiter atomically unlocked
                // and entered its condition-variable wait.
                let held = table.shards[0].lock();
                table.notify_key(other, 0);
                drop(held);
            });
            ready.send(()).unwrap();
            let timed_out = registration
                .signal
                .wait_for(&mut shard, Duration::from_millis(30))
                .timed_out();
            drop(shard);
            assert!(timed_out);
        });
        drop(registration);
        assert!(table.queues.get().unwrap()[0].lock().is_empty());
    }
}
