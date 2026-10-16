//! Logical copies never inherit database identity, authorization or event logs.
use super::*;

fn copy_blob_values(source: &BicDb, target: &BicDb, value: &mut Value) -> Result<()> {
    match value {
        Value::Object(fields)
            if fields.get("marker").and_then(Value::as_str)
                == Some(crate::LARGE_VALUE_REF_MARKER) =>
        {
            let original = LargeValueRef::from_value(value)?;
            let reader = large_value::open_reader(&source.path, &original, &source.encryption)?;
            let mut copied = large_value::write_from_reader(
                &target.path,
                reader,
                &target.encryption,
                target.config.fsync,
                original.media_type.clone(),
            )?;
            copied.created_at = original.created_at;
            *value = serde_json::to_value(copied)?;
        }
        Value::Object(fields) => {
            for value in fields.values_mut() {
                copy_blob_values(source, target, value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                copy_blob_values(source, target, value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn flush_copy_batch(target: &mut BicDb, collection: &str, batch: &mut Vec<Record>) -> Result<()> {
    if !batch.is_empty() {
        target.batch_insert(collection, batch.drain(..))?;
    }
    Ok(())
}

impl BicDb {
    /// Configuration needed to rebuild this collection's vector index in an
    /// independent database. Record and credential state is not included.
    pub fn vector_index_config(&self, collection: &str) -> Option<HnswIndexConfig> {
        self.hnsw_indexes
            .read()
            .get(collection)
            .map(HnswIndex::config)
    }

    /// Materialize one unprotected collection into an existing compatible
    /// collection of an independent database. Only records and their referenced
    /// blobs are copied; identity, events and authorizations remain independent.
    /// The target must be unpublished staging: a failure can leave a committed
    /// prefix. Callers must quiesce the source across a multi-collection copy.
    /// Batches contain at most 128 records and 8 MiB of encoded record data.
    pub fn copy_unprotected_collection_to(
        &self,
        collection: &str,
        target: &mut BicDb,
    ) -> Result<u64> {
        if std::ptr::eq(self, target) || self.path == target.path {
            return Err(BicDbError::Branch(
                "logical copy requires an independent target".into(),
            ));
        }
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        target.ensure_unprotected_legacy_access(collection, SecureOperation::Write)?;
        target.ensure_writable("logical collection copy")?;
        target.ensure_no_native_mutation_grant_required(collection, SecureOperation::Write)?;
        if self.collection_state(collection)?.meta.mode
            != target.collection_state(collection)?.meta.mode
        {
            return Err(BicDbError::Branch(
                "logical copy requires matching collection modes".into(),
            ));
        }
        let _quiesced = self.serializable_commit_admission.write();
        let mut batch = Vec::new();
        let mut bytes = 0usize;
        let mut count = 0u64;
        // Keep the target borrow inside this visitor so copying a large table
        // never clones its complete JSON payload or its primary-key registry.
        let mut visit = |mut record: Record| -> Result<()> {
            // Large values stay streamed and are re-encrypted for the target.
            // This is reachable only from an authorized collection scan.
            copy_blob_values(self, target, &mut record.metadata)?;
            // Resealing can change descriptor fields, so bound the bytes that
            // will actually be inserted rather than the source encoding.
            let size = serde_json::to_vec(&record)?.len();
            if size > 8 * 1024 * 1024 {
                return Err(BicDbError::Branch(
                    "logical copy record exceeds 8 MiB".into(),
                ));
            }
            if batch.len() >= 128 || bytes.saturating_add(size) > 8 * 1024 * 1024 {
                flush_copy_batch(target, collection, &mut batch)?;
                bytes = 0;
            }
            bytes += size;
            batch.push(record);
            count += 1;
            Ok(())
        };
        if let Some(paged) = &self.paged_records {
            drop(self.collection_state(collection)?);
            let snapshot = paged.latest_snapshot();
            for record in paged.scan(&snapshot, collection)? {
                visit(record?)?;
            }
        } else {
            let state = self.collection_state(collection)?;
            for shard in &state.shards {
                let shard = shard.read();
                for entry in shard.records.values() {
                    visit(entry.record.to_record()?)?;
                }
            }
        }
        flush_copy_batch(target, collection, &mut batch)?;
        Ok(count)
    }
}
