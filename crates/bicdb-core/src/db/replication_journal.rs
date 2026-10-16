//! Native transport preserves origin envelopes after event publication. The
//! transaction WAL remains authoritative for both rows and the journal outbox.
use super::*;

/// Apply publication metadata without duplicating the potentially large event
/// payload. Missing raw entries are possible in checkpointed WAL prefixes.
pub(super) fn freeze_audit_origin(events: &mut [Event], envelope: &EventEnvelope) -> Result<()> {
    if let Some(event) = events
        .iter_mut()
        .find(|event| event.id == envelope.event_id)
    {
        if let Some(existing) = envelope_from_metadata(event)? {
            if existing != *envelope {
                return Err(replication_error(
                    "record journal origin changed in retained WAL",
                ));
            }
        }
        *event = event_with_sync_metadata(event.clone(), envelope)?;
    }
    Ok(())
}

impl BicDb {
    /// Restored or promoted replicas can hold original positions larger than
    /// their own compacted event file. Reserve a new local authorship suffix
    /// without changing any envelope that has already left this database.
    pub(crate) fn prepare_replication_journal_origin(&mut self) -> Result<()> {
        if !self.config.replication.enabled
            || !self.config.replication.record_journal
            || self.ha_node.role() != NodeRole::Primary
        {
            return Ok(());
        }
        let mut events = self.events.lock();
        let maximum = events
            .events_iter()
            .filter_map(|stored| match envelope_from_metadata(&stored.event) {
                Ok(Some(envelope)) if envelope.node_id == self.sync_state.node_id => {
                    Some(Ok(envelope.sequence))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max();
        let Some(maximum) = maximum else {
            return Ok(());
        };
        let next = self
            .sync_state
            .origin_position_base
            .checked_add(file_len(events.path())?)
            .ok_or_else(|| replication_error("mesh origin position exhausted"))?;
        if next > maximum {
            return Ok(());
        }
        // Freeze raw local entries before advancing their offset base. A crash
        // after this rewrite is safe: the next open recomputes the same floor.
        self.freeze_local_event_positions(&mut events)?;
        let maximum = events
            .events_iter()
            .filter_map(|stored| match envelope_from_metadata(&stored.event) {
                Ok(Some(envelope)) if envelope.node_id == self.sync_state.node_id => {
                    Some(Ok(envelope.sequence))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max()
            .unwrap_or(maximum);
        let minimum_base = maximum
            .checked_add(1)
            .ok_or_else(|| replication_error("mesh origin position exhausted"))?
            .saturating_sub(file_len(events.path())?);
        drop(events);
        self.sync_state.origin_position_base =
            self.sync_state.origin_position_base.max(minimum_base);
        self.persist_sync_state()?;
        self.invalidate_sync_vector_cache();
        Ok(())
    }
    /// A durable row commit may still be publishing its audit outbox. Such a
    /// commit cannot leave this node until every journal entry is available.
    pub(crate) fn replication_journal_events(&self, audit: &[Event]) -> Result<Option<Vec<Event>>> {
        if !self.config.replication.record_journal {
            return Ok(Some(Vec::new()));
        }
        let events = self.events.lock();
        let mut journal = Vec::with_capacity(audit.len());
        for event in audit {
            if envelope_from_metadata(event)?.is_some() {
                journal.push(event.clone());
                continue;
            }
            let Some(stored) = events.event_by_id(&event.id) else {
                return Ok(None);
            };
            let mut entry = sync_mesh::bundle_event_for_stored_based(
                &self.sync_state.node_id,
                self.sync_state.origin_position_base,
                stored,
            )?;
            self.sign_own_envelope(&mut entry);
            journal.push(event_with_sync_metadata(entry.event, &entry.envelope)?);
        }
        Ok(Some(journal))
    }

    pub(crate) fn validate_replication_journal(&self, journal: &[Event]) -> Result<()> {
        if journal.is_empty() {
            return Ok(());
        }
        let entries = journal
            .iter()
            .map(|event| {
                let envelope = envelope_from_metadata(event)?.ok_or_else(|| {
                    replication_error("replicated journal lacks an origin envelope")
                })?;
                let claimed_hash = event
                    .metadata
                    .get("_bicdb_sync")
                    .and_then(|metadata| metadata.get("payload_hash"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        replication_error("replicated journal lacks its original payload hash")
                    })?;
                if hex::decode(claimed_hash).ok().as_deref()
                    != Some(envelope.payload_hash.as_slice())
                {
                    return Err(replication_error(
                        "replicated journal original payload hash mismatch",
                    ));
                }
                Ok(sync_mesh::SyncBundleEvent {
                    event: event.clone(),
                    envelope,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // Native peers are trusted whole-database replicas authenticated by the
        // transport. Origin authorization remains the edge receiver's pinned
        // key policy; this checks the original envelope's content integrity.
        let mut bundle = SyncBundle::new(
            self.node_id(),
            Default::default(),
            Default::default(),
            Vec::new(),
        )?;
        // Newly authored bundles normalize their hashes in the constructor.
        // Received hashes are evidence: verify them without that normalization.
        bundle.event_count = entries.len();
        bundle.events = entries;
        bundle.checksum = bundle.calculate_checksum()?;
        bundle.verify()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_stream_waits_for_outbox_publication_and_resumes_after_recovery() {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(true)
            .with_audit_events(true)
            .with_replication(ReplicationConfig {
                enabled: true,
                mode: ReplicationMode::Primary,
                listen_addr: Some("127.0.0.1:0".into()),
                record_journal: true,
                tls: Some(crate::ReplicationTlsConfig {
                    cert_path: Default::default(),
                    key_path: Default::default(),
                    ca_path: Default::default(),
                    require_client_cert: false,
                    dev_localhost_plaintext: true,
                }),
                ..Default::default()
            });
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("items").unwrap();
        db.set_collection_mesh_sync_enabled("items", true).unwrap();
        let mut cursor = ReplicationCursor::new(0);
        db.replication_frames_after(&mut cursor, 1000).unwrap();
        let before = cursor.sent_seq();
        let mut transaction = db.begin_transaction().unwrap();
        transaction.insert("items", Record::new("one")).unwrap();
        let committed = db.commit_transaction(&mut transaction).unwrap();
        db.tx_log.write_durable(committed).unwrap();
        assert!(db
            .replication_frames_after(&mut cursor, 1000)
            .unwrap()
            .is_empty());
        assert_eq!(cursor.sent_seq(), before);
        assert!(db.export_replication_frames_since(before, 1000).is_err());
        let event_id = transaction.pending_record_audit_events[0].id;
        // Die after the row WAL became durable and before the event segment copy.
        drop(transaction);
        drop(db);
        let db = BicDb::open_with_config(root.path(), config).unwrap();
        let frames = db.replication_frames_after(&mut cursor, 1000).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].journal[0].id, event_id);
        assert_eq!(cursor.sent_seq(), committed);
        let envelope = envelope_from_metadata(&frames[0].journal[0])
            .unwrap()
            .unwrap();
        assert!(envelope.signature.is_some());
    }

    #[test]
    fn retained_native_wal_preserves_original_journal_after_event_retention() {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(true)
            .with_audit_events(true)
            .with_replication(ReplicationConfig {
                enabled: true,
                mode: ReplicationMode::Primary,
                listen_addr: Some("127.0.0.1:0".into()),
                record_journal: true,
                tls: Some(crate::ReplicationTlsConfig {
                    cert_path: Default::default(),
                    key_path: Default::default(),
                    ca_path: Default::default(),
                    require_client_cert: false,
                    dev_localhost_plaintext: true,
                }),
                ..Default::default()
            });
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("items").unwrap();
        db.set_collection_mesh_sync_enabled("items", true).unwrap();
        let before = db.current_commit_seq();
        db.insert("items", Record::new("one")).unwrap();
        let expected = db.export_replication_frames_since(before, 1000).unwrap();
        assert_eq!(expected.len(), 1);
        assert_eq!(expected[0].journal.len(), 1);
        // Event retention is allowed to retire published entries. Native WAL
        // retention has an independent lifetime and must retain provenance.
        db.events.lock().rewrite_events(Vec::new()).unwrap();
        let frames = db.export_replication_frames_since(before, 1000).unwrap();
        assert_eq!(frames, expected);
        let mut cursor = ReplicationCursor::new(before);
        assert_eq!(
            db.replication_frames_after(&mut cursor, 1000).unwrap(),
            expected
        );
        drop(db);
        let db = BicDb::open_with_config(root.path(), config).unwrap();
        assert_eq!(db.events.lock().events_iter().count(), 0);
        assert_eq!(
            db.export_replication_frames_since(before, 1000).unwrap(),
            expected
        );
    }

    #[test]
    fn a_restored_writer_cannot_reuse_a_previously_exported_origin_position() {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(true)
            .with_audit_events(true)
            .with_replication(ReplicationConfig {
                enabled: true,
                mode: ReplicationMode::Primary,
                listen_addr: Some("127.0.0.1:0".into()),
                record_journal: true,
                tls: Some(crate::ReplicationTlsConfig {
                    cert_path: Default::default(),
                    key_path: Default::default(),
                    ca_path: Default::default(),
                    require_client_cert: false,
                    dev_localhost_plaintext: true,
                }),
                ..Default::default()
            });
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("items").unwrap();
        db.set_collection_mesh_sync_enabled("items", true).unwrap();
        db.insert("items", Record::new("one")).unwrap();
        let mut entry = db
            .export_sync_bundle_delta(&Default::default())
            .unwrap()
            .events
            .remove(0);
        entry.envelope.sequence = 1_000_000_000;
        entry.envelope.signature = None;
        db.sign_own_envelope(&mut entry);
        let original = entry.envelope.clone();
        // Model a restored file whose physical offsets are smaller than the
        // original immutable positions it carries.
        db.events
            .lock()
            .rewrite_events(vec![
                event_with_sync_metadata(entry.event, &entry.envelope).unwrap()
            ])
            .unwrap();
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        db.insert("items", Record::new("two")).unwrap();
        let exported = db.sync_vector().unwrap();
        assert!(exported.origins[&db.node_id().to_string()] > original.sequence);
        let frozen = db
            .events
            .lock()
            .event_by_id(&original.event_id)
            .unwrap()
            .event
            .clone();
        assert_eq!(envelope_from_metadata(&frozen).unwrap().unwrap(), original);
    }
}
