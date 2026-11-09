//! Transaction-lifetime announcements of native unique-index keys. Commit's
//! existing pre-WAL unique claims remain authoritative; these reservations let
//! READ COMMITTED SQL wait for a not-yet-committed key before arbitration.
use super::*;

fn namespace(index: &str) -> String {
    format!("native-unique-index:{index}")
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
        self.reserve_integrity_key_cancellable(&namespace(index), &encoded, cancellation)
    }

    fn previous_unique_reservation_image(
        &self,
        collection: &str,
        record_id: &str,
        previous_stored: Option<&Arc<StoredRecord>>,
    ) -> Option<OldImage> {
        if let Some(write) = self.last_pending_write(collection, record_id) {
            return Some(match write.op {
                TxWriteOp::Delete => OldImage::none(),
                TxWriteOp::Upsert => match &write.stored {
                    Some(stored) => OldImage::from_stored(Arc::clone(stored)),
                    None => OldImage::from_record(write.record_opt().cloned()),
                },
            });
        }
        if let Some(stored) = previous_stored {
            return Some(OldImage::from_stored(Arc::clone(stored)));
        }
        self.read_images
            .lock()
            .get(collection, record_id)
            .map(|(stored, _)| OldImage::from_stored(stored))
    }

    /// Called before buffering every native write, including signed and stored
    /// fast paths, so a SQL waiter also observes writers that bypass SQL.
    fn reserve_unique_image(
        &self,
        collection: &str,
        image: &OldImage,
        previous: Option<&OldImage>,
    ) -> Result<()> {
        let db = self.db_ref();
        db.check_recovery_state()?;
        let mut claims = Vec::new();
        for index in db.indexes.values() {
            let state = index.read();
            let definition = &state.definition;
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
            let scope = namespace(&name);
            match &cancellation {
                Some(token) => self.reserve_integrity_key_cancellable(&scope, &encoded, token)?,
                None => self.reserve_integrity_key(&scope, &encoded)?,
            }
        }
        db.check_recovery_state()
    }
}
