//! Repeatable integrity evidence and isolated recovery/promotion drills.
use super::*;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableChecksum {
    pub rows: u64,
    /// XOR of SHA-256 hashes including each row's identity and all stored fields.
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationManifest {
    pub commit_seq: u64,
    pub tables: BTreeMap<String, TableChecksum>,
}

impl BicDb {
    /// Pins one MVCC snapshot. Writers pause only while the snapshot is pinned;
    /// hashing does not hold commit admission. Callers must exclude catalog DDL.
    pub fn verification_manifest(&self) -> Result<VerificationManifest> {
        let tx = {
            let _admission = self.serializable_commit_admission.write();
            self.begin_transaction_after(0)?
        };
        let mut tables = BTreeMap::new();
        let mut checked_blobs = BTreeSet::new();
        for collection in self.collections() {
            let mut hash = [0u8; 32];
            let mut rows = 0u64;
            let mut visit = |record: &Record| -> Result<()> {
                self.verify_attachment_references(&record.metadata, &mut checked_blobs, true)?;
                let digest = Sha256::digest(serde_json::to_vec(record)?);
                for (out, byte) in hash.iter_mut().zip(digest) {
                    *out ^= byte;
                }
                rows += 1;
                Ok(())
            };
            let state = self.collection_state(&collection.name)?;
            if state.paged_lazy {
                // The admission barrier pinned the page snapshot and MVCC
                // watermark together. Stream it with one decoded row at a time.
                let paged = self
                    .paged_records
                    .as_ref()
                    .ok_or_else(|| replication_error("lazy collection has no page store"))?;
                let snapshot = tx
                    .paged_snapshot
                    .as_ref()
                    .ok_or_else(|| replication_error("verification did not pin page storage"))?;
                for record in paged.scan(snapshot, &collection.name)? {
                    visit(&record?)?;
                }
            } else {
                for shard in &state.shards {
                    let shard = shard.read();
                    for versions in shard.versions.values() {
                        if let Some(version) = visible_version(versions, tx.snapshot_tx) {
                            visit(&version.record.to_record()?)?;
                        }
                    }
                }
            }
            tables.insert(
                collection.name,
                TableChecksum {
                    rows,
                    sha256: hex::encode(hash),
                },
            );
        }
        Ok(VerificationManifest {
            commit_seq: tx.snapshot_tx.0,
            tables,
        })
    }

    /// Indexes and tables must describe the same instant; an index's own read
    /// lock alone does not prevent a concurrent commit changing its table.
    pub fn verify_indexes_consistently(&self) -> Result<Vec<IndexVerifyReport>> {
        let mut reports = Vec::new();
        for definition in self.index_definitions() {
            let _admission = self.serializable_commit_admission.write();
            reports.push(self.verify_index(&definition.name)?);
        }
        Ok(reports)
    }

    pub fn verify_vector_indexes_consistently(&self) -> Result<usize> {
        let mut checked = 0;
        for collection in self.collections() {
            if self.has_vector_index(&collection.name) {
                let _admission = self.serializable_commit_admission.write();
                if !self.verify_vector_index(&collection.name)?.valid {
                    return Err(replication_error(format!(
                        "vector index differs from table {}",
                        collection.name
                    )));
                }
                checked += 1;
            }
        }
        Ok(checked)
    }

    /// Validate actual post-images against the commit stream before confirming
    /// a replica's commit. Repeated writes of one key compare the final image.
    pub fn verify_replication_rows(&self, frame: &CommitFrame) -> Result<()> {
        if self.last_applied_commit_seq() != frame.commit_seq {
            return Err(replication_error(
                "row verification requires the exact applied commit",
            ));
        }
        let tx = self.begin_transaction_after(0)?;
        let mut seen = BTreeSet::new();
        for write in frame.writes.iter().rev() {
            // Catalog operations can rename/drop a collection in this commit.
            if write.collection == collection_rename::CATALOG_LOG_COLLECTION {
                continue;
            }
            if !seen.insert((&write.collection, &write.record_id)) {
                continue;
            }
            let actual = tx.get_for_integrity_check(&write.collection, &write.record_id)?;
            let expected = match write.operation {
                ReplicationOperationType::Delete => None,
                ReplicationOperationType::Insert | ReplicationOperationType::Upsert => {
                    Some(serde_json::from_slice::<Record>(&write.payload)?)
                }
            };
            if actual.as_deref() != expected.as_ref() {
                return Err(replication_error(format!(
                    "replica row diverged at commit {}: {}/{}",
                    frame.commit_seq, write.collection, write.record_id
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RestoreDrillReport {
    pub restored_commit_seq: u64,
    pub tables: usize,
    pub indexes: usize,
    pub promotion_and_restart: bool,
}

/// Restores to an exact source snapshot, compares every table, verifies all
/// indexes, promotes the scratch standby, writes a probe and reopens it.
/// The randomly named child is always cleaned up; the caller's root is retained.
pub fn verify_restore_drill(
    archive: &Path,
    key: &str,
    scratch_root: &Path,
    expected: Option<&VerificationManifest>,
) -> Result<RestoreDrillReport> {
    fs::create_dir_all(scratch_root)?;
    let scratch = scratch_root.join(format!("bicdb-verify-{}", Uuid::new_v4().simple()));
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(scratch.clone());
    let replay = restore_point_in_time(
        archive,
        &scratch,
        key,
        false,
        crate::log_archive::ReplayTarget {
            commit_seq: expected.map(|manifest| manifest.commit_seq),
            timestamp: None,
        },
    )?;
    let storage = format::load_metadata(&scratch)?
        .map(|meta| meta.storage_mode)
        .unwrap_or_default();
    let config = DbConfig::default().with_storage_mode(storage);
    let manifest;
    let indexes;
    {
        let db = BicDb::open_with_config(&scratch, config.clone())?;
        let integrity = db.verify_integrity()?;
        if !integrity.large_value_checksum_failures.is_empty()
            || integrity
                .paged_storage
                .as_ref()
                .is_some_and(|report| !report.valid)
        {
            return Err(replication_error(
                "restored storage or attachment integrity verification failed",
            ));
        }
        manifest = db.verification_manifest()?;
        if let Some(expected) = expected {
            if &manifest != expected {
                return Err(replication_error(
                    "restored tables differ from the source snapshot",
                ));
            }
        }
        let reports = db.verify_indexes_consistently()?;
        if reports.iter().any(|report| !report.valid) {
            return Err(replication_error("restored index differs from its table"));
        }
        indexes = reports.len() + db.verify_vector_indexes_consistently()?;
        db.close()?;
    }
    let replication = ReplicationConfig {
        enabled: true,
        mode: ReplicationMode::Standby,
        listen_addr: Some("127.0.0.1:0".into()),
        tls: Some(crate::replication::ReplicationTlsConfig {
            cert_path: PathBuf::new(),
            key_path: PathBuf::new(),
            ca_path: PathBuf::new(),
            require_client_cert: true,
            dev_localhost_plaintext: true,
        }),
        ..ReplicationConfig::default()
    };
    let promoted_config = config.with_replication(replication);
    {
        let db = BicDb::open_with_config(&scratch, promoted_config.clone())?;
        let term = db.begin_candidacy("verify-drill")?;
        db.promote_by_election(term, "verify-drill", None)?;
        db.close()?;
    }
    let probe = format!("__bicdb_verify_{}", Uuid::new_v4().simple());
    {
        let mut db = BicDb::open_with_config(&scratch, promoted_config.clone())?;
        if db.replication_role() != NodeRole::Primary {
            return Err(replication_error(
                "scratch promotion did not survive restart",
            ));
        }
        db.create_collection(&probe)?;
        db.insert(&probe, Record::new("durability-probe"))?;
        db.close()?;
    }
    let db = BicDb::open_with_config(&scratch, promoted_config)?;
    if db.get(&probe, "durability-probe")?.is_none() {
        return Err(replication_error(
            "scratch promotion lost its acknowledged write",
        ));
    }
    db.close()?;
    Ok(RestoreDrillReport {
        restored_commit_seq: replay.restored_commit_seq,
        tables: manifest.tables.len(),
        indexes,
        promotion_and_restart: true,
    })
}

impl BicDb {
    pub fn replication_verification_healthy(&self) -> bool {
        self.ha_node.read(|file| file.verification_error.is_none())
    }

    /// Retain failed integrity evidence across restart. A fresh snapshot is
    /// required: reconnecting at a larger applied sequence cannot certify a
    /// row whose actual post-image already failed verification.
    pub fn quarantine_replication_verification(&self, error: &str) -> Result<()> {
        self.ha_node.with_file(|file| {
            file.verification_error = Some(error.into());
        })
    }
}

impl BicDb {
    fn verify_attachment_references(
        &self,
        metadata: &Value,
        seen: &mut BTreeSet<String>,
        read_content: bool,
    ) -> Result<()> {
        let mut pending = vec![metadata];
        while let Some(value) = pending.pop() {
            match value {
                Value::Object(object)
                    if object.get("marker").and_then(Value::as_str)
                        == Some(crate::LARGE_VALUE_REF_MARKER) =>
                {
                    let reference = crate::LargeValueRef::from_value(value)?;
                    if seen.insert(serde_json::to_string(&reference)?) {
                        let mut reader = large_value::open_reader(&self.path, &reference, &self.encryption)
                            .map_err(|error| replication_error(format!("referenced attachment is missing or unreadable; a full snapshot is required: {error}")))?;
                        if read_content {
                            std::io::copy(&mut reader, &mut std::io::sink())?;
                        }
                    }
                }
                Value::Object(object) => pending.extend(object.values()),
                Value::Array(values) => pending.extend(values),
                _ => {}
            }
        }
        Ok(())
    }

    pub(super) fn verify_replication_attachments(&self, frame: &CommitFrame) -> Result<()> {
        let mut seen = BTreeSet::new();
        for write in &frame.writes {
            if write.operation != ReplicationOperationType::Delete {
                let record: Record = serde_json::from_slice(&write.payload)?;
                self.verify_attachment_references(&record.metadata, &mut seen, false)?;
            }
        }
        Ok(())
    }
}
