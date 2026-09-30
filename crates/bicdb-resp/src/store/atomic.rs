use super::*;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub(crate) struct TypedEntry {
    pub value: Vec<u8>,
    pub kind: Option<String>,
    pub expires_at_ms: Option<i64>,
}

/// Lazily read only touched keys; buffered writes publish in one WAL commit.
pub(crate) struct AtomicCache<'a> {
    source: &'a dyn Fn(&[u8]) -> Result<Option<TypedEntry>>,
    writes: BTreeMap<Vec<u8>, Option<TypedEntry>>,
    bytes: usize,
    pub now: i64,
}

impl AtomicCache<'_> {
    pub fn get(&self, key: &[u8]) -> Result<Option<TypedEntry>> {
        let entry = if let Some(entry) = self.writes.get(key) {
            entry.clone()
        } else {
            (self.source)(key)?
        };
        Ok(entry.filter(|e| e.expires_at_ms.is_none_or(|deadline| deadline > self.now)))
    }
    pub fn put(&mut self, key: Vec<u8>, value: Option<TypedEntry>) -> Result<()> {
        let old = self
            .writes
            .get(&key)
            .map_or(0, |v| key.len() + v.as_ref().map_or(0, |e| e.value.len()));
        let size = key
            .len()
            .saturating_add(value.as_ref().map_or(0, |e| e.value.len()));
        let bytes = self.bytes.saturating_sub(old).saturating_add(size);
        if bytes > 64 * 1024 * 1024
            || self.writes.len() >= 16_384 && !self.writes.contains_key(&key)
        {
            return Err(RespServerError::Command(
                "OOM atomic write set limit exceeded".into(),
            ));
        }
        self.bytes = bytes;
        self.writes.insert(key, value);
        Ok(())
    }
}

impl CacheStore {
    /// Caller holds command_lock, so commands, scripts and expiry cannot
    /// interleave. The engine lock also covers reads and the final commit.
    pub(crate) fn atomic<T>(
        &self,
        index: u8,
        execute: impl FnOnce(&mut AtomicCache<'_>) -> Result<T>,
    ) -> Result<T> {
        if let Some(maps) = &self.mem {
            let mut map = maps[index as usize].lock();
            let read = |key: &[u8]| {
                Ok(map.get(&encode_key(key)).map(|e| TypedEntry {
                    value: e.value.clone(),
                    kind: e.kind.clone(),
                    expires_at_ms: e.expires_at_ms,
                }))
            };
            let mut cache = AtomicCache {
                source: &read,
                writes: BTreeMap::new(),
                bytes: 0,
                now: now_ms(),
            };
            let result = execute(&mut cache)?;
            let writes = std::mem::take(&mut cache.writes);
            let delta = writes
                .iter()
                .map(|(key, value)| {
                    i64::from(value.is_some()) - i64::from(map.contains_key(&encode_key(key)))
                })
                .sum::<i64>();
            self.check_atomic_budget(delta)?;
            for (key, value) in writes {
                let id = encode_key(&key);
                if let Some(e) = value {
                    self.track_expiry(e.expires_at_ms, index, &id);
                    map.insert(
                        id,
                        MemEntry {
                            value: e.value,
                            kind: e.kind,
                            expires_at_ms: e.expires_at_ms,
                        },
                    );
                } else {
                    map.remove(&id);
                }
            }
            self.adjust_count(delta);
            return Ok(result);
        }
        let mut db = self.db.write();
        let collection = collection_name(index);
        let read = |key: &[u8]| -> Result<Option<TypedEntry>> {
            Ok(
                absent_ok(db.get(&collection, &encode_key(key)))?.map(|r| TypedEntry {
                    value: r.payload.clone().unwrap_or_default(),
                    kind: r
                        .metadata
                        .get("redis_type")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned),
                    expires_at_ms: r.timestamp,
                }),
            )
        };
        let mut cache = AtomicCache {
            source: &read,
            writes: BTreeMap::new(),
            bytes: 0,
            now: now_ms(),
        };
        let result = execute(&mut cache)?;
        let writes = std::mem::take(&mut cache.writes);
        if writes.is_empty() {
            return Ok(result);
        }
        let mut delta = 0i64;
        for (key, value) in &writes {
            delta += i64::from(value.is_some())
                - i64::from(absent_ok(db.get(&collection, &encode_key(key)))?.is_some());
        }
        self.check_atomic_budget(delta)?;
        db.create_collection(&collection)?;
        let mut tx = db.begin_transaction()?;
        for (key, value) in &writes {
            let id = encode_key(key);
            if let Some(e) = value {
                let mut record = Record::new(id).with_payload(e.value.clone());
                record.timestamp = e.expires_at_ms;
                if let Some(kind) = &e.kind {
                    record.metadata = serde_json::json!({"redis_type": kind});
                }
                tx.insert(&collection, record)?;
            } else {
                tx.delete(&collection, &id)?;
            }
        }
        tx.commit()?;
        self.adjust_count(delta);
        for (key, value) in writes {
            if let Some(e) = value {
                self.track_expiry(e.expires_at_ms, index, &encode_key(&key));
            }
        }
        Ok(result)
    }
    fn check_atomic_budget(&self, delta: i64) -> Result<()> {
        if delta > 0
            && self.max_keys.is_some_and(|max| {
                self.key_count
                    .load(Ordering::Relaxed)
                    .saturating_add(delta as usize)
                    > max
            })
        {
            return Err(RespServerError::Command(
                "OOM atomic command exceeds key budget".into(),
            ));
        }
        Ok(())
    }
    fn adjust_count(&self, delta: i64) {
        if delta >= 0 {
            self.key_count.fetch_add(delta as usize, Ordering::Relaxed);
        } else {
            self.key_count
                .fetch_sub((-delta) as usize, Ordering::Relaxed);
        }
    }
}
