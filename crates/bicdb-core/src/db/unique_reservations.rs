//! Shared transaction-lifetime announcements of native unique-index keys.
//! Writers may prepare duplicate keys concurrently; an SQL arbiter takes an
//! exclusive reservation and waits for every pending writer before choosing. Commit's
//! existing pre-WAL unique claims remain authoritative; these reservations let
//! READ COMMITTED SQL wait for a not-yet-committed key before arbitration.
use super::*;

fn namespace(index: &str) -> String {
    format!("native-unique-index:{index}")
}

#[derive(Debug)]
pub(super) struct UniqueIndexCache {
    generation: u64,
    collections: FxHashMap<String, Arc<[String]>>,
}

impl BicDb {
    // Avoid constructing row-image caches (or allocating an empty Arc slice)
    // on the ordinary path for collections without a native unique index.
    pub(super) fn has_unique_reservation_indexes(&self, collection: &str) -> bool {
        if self.indexes.is_empty() {
            return false;
        }
        let generation = self.index_generation();
        {
            let cache = self.unique_reservation_indexes.read();
            if let Some(cache) = cache
                .as_ref()
                .filter(|cache| cache.generation == generation)
            {
                return cache.collections.contains_key(collection);
            }
        }
        !self.unique_reservation_index_names(collection).is_empty()
    }

    // Catalog changes hold the outer exclusive DB guard. Rebuild once per
    // catalog generation, then visit only this collection's unique indexes
    // for each row, including when other collections have thousands of them.
    fn unique_reservation_index_names(&self, collection: &str) -> Arc<[String]> {
        let generation = self.index_generation();
        {
            let cache = self.unique_reservation_indexes.read();
            if let Some(cache) = cache
                .as_ref()
                .filter(|cache| cache.generation == generation)
            {
                return cache
                    .collections
                    .get(collection)
                    .cloned()
                    .unwrap_or_default();
            }
        }
        let mut cache = self.unique_reservation_indexes.write();
        if cache
            .as_ref()
            .is_none_or(|cache| cache.generation != generation)
        {
            let mut collections: FxHashMap<String, Vec<String>> = FxHashMap::default();
            for (name, index) in &self.indexes {
                let state = index.read();
                if state.definition.unique {
                    collections
                        .entry(state.definition.collection.clone())
                        .or_default()
                        .push(name.clone());
                }
            }
            *cache = Some(UniqueIndexCache {
                generation,
                collections: collections
                    .into_iter()
                    .map(|(name, indexes)| (name, indexes.into()))
                    .collect(),
            });
        }
        cache
            .as_ref()
            .expect("cache initialized")
            .collections
            .get(collection)
            .cloned()
            .unwrap_or_default()
    }
}

impl Transaction {
    /// Reserve the physical unique key this proposed row contributes to an
    /// index. Index latches are released before any wait; the owner can commit.
    /// NULL-distinct keys and rows outside a partial predicate need no claim.
    pub fn reserve_unique_index_record_cancellable(
        &self,
        index: &str,
        record: &Record,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.reserve_unique_index_record_inner(index, record, Some(cancellation))
    }

    /// Nonparking sibling for an executor retaining an exclusive DB guard.
    pub fn reserve_unique_index_record(&self, index: &str, record: &Record) -> Result<()> {
        self.reserve_unique_index_record_inner(index, record, None)
    }

    fn reserve_unique_index_record_inner(
        &self,
        index: &str,
        record: &Record,
        cancellation: Option<&CancellationToken>,
    ) -> Result<()> {
        if self.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        let db = self.db_ref();
        db.check_recovery_state()?;
        let encoded = {
            let state = db
                .indexes
                .get(index)
                .ok_or_else(|| BicDbError::Index(format!("index `{index}` not found")))?
                .read();
            if !state.definition.unique
                || !record_matches_index_predicate(record, &state.definition)?
            {
                None
            } else {
                let values = record_index_key(record, &state.definition.fields);
                state
                    .definition
                    .enforces_unique_key(&values)
                    .then(|| hex::encode(encode_index_key(&values)))
            }
        };
        db.check_recovery_state()?;
        let Some(encoded) = encoded else {
            return Ok(());
        };
        match cancellation {
            Some(token) => {
                self.reserve_integrity_key_cancellable(&namespace(index), &encoded, token)
            }
            None => self.reserve_integrity_key(&namespace(index), &encoded),
        }
    }

    pub(super) fn previous_unique_reservation_image(
        &self,
        collection: &str,
        record_id: &str,
        previous_stored: Option<&Arc<StoredRecord>>,
    ) -> Result<Option<OldImage>> {
        let db = self.db_ref();
        if let Some(write) = self.last_pending_write(collection, record_id) {
            return Ok(Some(match write.op {
                TxWriteOp::Delete => OldImage::none(),
                TxWriteOp::Upsert => match &write.stored {
                    Some(stored) => OldImage::from_stored(Arc::clone(stored)),
                    None => OldImage::from_record(write.record_opt().cloned()),
                },
            }));
        }
        if let Some(stored) = previous_stored {
            return Ok(Some(OldImage::from_stored(Arc::clone(stored))));
        }
        if let Some((stored, _)) = self.read_images.lock().get(collection, record_id) {
            return Ok(Some(OldImage::from_stored(stored)));
        }
        // A blind update of a resident row can prove that its unique key is
        // unchanged without parsing/reading the page store. A speculative
        // duplicate INSERT must not unnecessarily exclude that update.
        // This is only a reservation optimization: original statement-version
        // conflict checks and pre-WAL unique validation remain authoritative.
        let state = db.collection_state(collection)?;
        let shard = state.shard(record_id).read();
        Ok(shard
            .get_record(record_id)
            .filter(|entry| entry.record.evicted.is_none())
            .map(|entry| OldImage::from_stored(Arc::clone(&entry.record))))
    }

    /// Called before buffering every native write, including signed and stored
    /// fast paths, so a SQL waiter also observes writers that bypass SQL.
    pub(super) fn reserve_unique_image(
        &self,
        collection: &str,
        image: &OldImage,
        previous: Option<&OldImage>,
        changed: Option<&[Box<str>]>,
    ) -> Result<()> {
        let db = self.db_ref();
        db.check_recovery_state()?;
        let mut claims = Vec::new();
        for name in db.unique_reservation_index_names(collection).iter() {
            let Some(index) = db.indexes.get(name) else {
                continue;
            };
            let state = index.read();
            let definition = &state.definition;
            if index_definition_untouched(
                definition,
                previous.and_then(OldImage::id),
                image.id(),
                changed,
            ) {
                continue;
            }
            if !definition.unique
                || definition.collection != collection
                || image.matches_predicate(definition)? != Some(true)
            {
                continue;
            }
            let key = image
                .index_key(&definition.fields)?
                .expect("write image exists");
            if !definition.enforces_unique_key(&key) {
                continue;
            }
            if let Some(previous) = previous {
                if previous.id() == image.id()
                    && previous.matches_predicate(definition)? == Some(true)
                    && previous.index_key(&definition.fields)?.as_ref() == Some(&key)
                {
                    continue;
                }
            }
            let encoded = image
                .encoded_index_key(&definition.fields)?
                .expect("write image key encodes");
            claims.push((definition.name.clone(), hex::encode(encoded.as_ref())));
        }
        // Never retain a catalog/index latch while the owning transaction
        // needs that same index's write latch to finish its commit.
        claims.sort_unstable();
        let cancellation = self.blocking_row_waits.lock().clone();
        for (name, encoded) in claims {
            if let Some(token) = &cancellation {
                token.check()?;
            }
            let scope = namespace(&name);
            match self.reserve_integrity_key_shared(&scope, &encoded) {
                Ok(()) => {}
                Err(BicDbError::TransactionConflict(_)) => {
                    // A cold paged blind update may have no resident preimage.
                    // Only on contention, read its current physical row to
                    // prove this index key already belongs to the same ID.
                    // Leaving an existing key unchanged needs no new claim.
                    // No index latch is retained across that page-store read.
                    let current = match image.id() {
                        Some(id) => db.get_unchecked(collection, id)?,
                        None => None,
                    };
                    let unchanged = if let Some(current) = current {
                        let state = db.indexes.get(&name).expect("catalog stable").read();
                        record_matches_index_predicate(&current, &state.definition)?
                            && hex::encode(encode_index_key(&record_index_key(
                                &current,
                                &state.definition.fields,
                            ))) == encoded
                    } else {
                        false
                    };
                    if !unchanged {
                        match &cancellation {
                            Some(token) => self.reserve_integrity_key_shared_cancellable(
                                &scope, &encoded, token,
                            )?,
                            None => {
                                return Err(BicDbError::TransactionConflict(
                                    "constraint key is reserved by another transaction".into(),
                                ))
                            }
                        }
                    }
                }
                Err(error) => return Err(error),
            }
            if let Some(token) = &cancellation {
                token.check()?;
            }
        }
        db.check_recovery_state()
    }
}
