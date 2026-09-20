//! Optional sparse encoding of a full WAL write, readable by existing decoders.
//!
//! Keep Record's public serialization and content hash unchanged. Only this WAL
//! view omits absent optional record fields and live-only statement_snapshot.
//! Recovery applies committed absolute writes, not live conflict validation;
//! retained-WAL replication also reconstructs mutations, not statement snapshots.
//! TxWriteWire has always defaulted an absent statement_snapshot to zero.
use super::*;

pub(super) fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_WAL_COMPACT_FRAME")
            .map(|value| matches!(value.as_str(), "1" | "on" | "true" | "yes"))
            .unwrap_or(false)
    })
}

#[derive(Serialize)]
pub(super) struct StoredWriteFrame<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    tx_id: TransactionId,
    write: StoredWrite<'a>,
}

#[derive(Serialize)]
struct StoredWrite<'a> {
    collection: &'a str,
    op: TxWriteOp,
    record_id: &'a str,
    record: SparseRecord<'a>,
    timestamp: i64,
}

impl<'a> StoredWriteFrame<'a> {
    pub(super) fn new(tx_id: TransactionId, write: &'a TxWrite, row: &'a StoredRecord) -> Self {
        Self {
            kind: "tx_write",
            tx_id,
            write: StoredWrite {
                collection: &write.collection,
                op: write.op,
                record_id: &write.record_id,
                record: SparseRecord(row),
                timestamp: write.timestamp,
            },
        }
    }
}

struct SparseRecord<'a>(&'a StoredRecord);

impl Serialize for SparseRecord<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let row = self.0;
        if row.evicted.is_some() {
            return Err(serde::ser::Error::custom(
                "cannot write an evicted record stub to WAL",
            ));
        }
        #[derive(Serialize)]
        struct Wire<'a> {
            id: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            vector: &'a Option<Vec<f32>>,
            metadata: &'a RawValue,
            #[serde(skip_serializing_if = "Option::is_none")]
            geometry: &'a Option<crate::geometry::Geometry>,
            #[serde(skip_serializing_if = "Option::is_none")]
            timestamp: &'a Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            payload: &'a Option<Vec<u8>>,
        }
        Wire {
            id: &row.id,
            vector: &row.vector,
            metadata: &row.metadata,
            geometry: &row.geometry,
            timestamp: &row.timestamp,
            payload: &row.payload,
        }
        .serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoded_full_wal_write_keeps_metadata_compact_until_requested() {
        let record = Record::new("row-雪").with_metadata(json!({
            "null": null, "boolean": true, "text": "quote\" and newline\n",
            "nested": {"array": [1, 2.5, false]},
            "exact": {"$bicdb_typed": {"version": 1, "pg_type": "numeric", "text": "12345678901234567890.1250"}}
        })).with_vector(vec![1.25, -2.5]).with_payload(vec![0, 255]);
        let frame = TxFrame::Write {
            tx_id: TransactionId(7),
            write: Box::new(write_for(&record)),
        };
        let bytes = serde_json::to_vec(&frame).unwrap();
        let TxFrame::Write { tx_id, write } = decode_tx_frame(&bytes).unwrap() else {
            panic!("expected write frame");
        };
        assert_eq!(tx_id, TransactionId(7));
        assert!(
            write.record.get().is_none(),
            "WAL decode eagerly expanded metadata"
        );
        let stored = write.stored.as_ref().expect("compact WAL row");
        assert_eq!(
            stored.to_record().unwrap().content_hash().unwrap(),
            record.content_hash().unwrap()
        );
        assert_eq!(write.record().unwrap().unwrap().as_ref(), &record);
        assert_eq!(write.statement_snapshot, 12345);
        let replay_frame = TxFrame::Write { tx_id, write };
        assert_eq!(
            serde_json::to_value(replay_frame).unwrap(),
            serde_json::to_value(frame).unwrap()
        );
    }

    fn write_for(record: &Record) -> TxWrite {
        TxWrite {
            collection: "rows".to_string(),
            op: TxWriteOp::Upsert,
            record_id: record.id.clone(),
            record: OnceLock::from(Some(Arc::new(record.clone()))),
            timestamp: 9876,
            statement_snapshot: 12345,
            repair: None,
            stored: None,
            previous: None,
            previous_stored: None,
            changed: None,
        }
    }

    #[test]
    fn sparse_wal_preserves_all_record_values_and_public_hashes() {
        for mask in 0..16 {
            let mut record = Record::new("row\"雪").with_metadata(json!({
                "null": null, "text": "line\nbreak", "nested": {"list": [1, true]},
                "amount": {"$bicdb_typed": {"version": 1, "pg_type": "numeric", "text": "12.50"}}
            }));
            if mask & 1 != 0 {
                record.vector = Some(vec![]);
            }
            if mask & 2 != 0 {
                record.payload = Some(vec![0, 255]);
            }
            if mask & 4 != 0 {
                record.timestamp = Some(0);
            }
            if mask & 8 != 0 {
                record.geometry = Some(crate::geometry::Geometry::Point(geo_types::Point::new(
                    1.0, 2.0,
                )));
            }
            let stored = StoredRecord::from_record(&record).unwrap();
            let write = write_for(&record);
            let bytes =
                serde_json::to_vec(&StoredWriteFrame::new(TransactionId(7), &write, &stored))
                    .unwrap();
            let TxFrame::Write {
                tx_id,
                write: decoded,
            } = decode_tx_frame(&bytes).unwrap()
            else {
                panic!("existing decoder must read sparse tx_write");
            };
            assert_eq!(tx_id, TransactionId(7));
            assert_eq!(decoded.collection, write.collection);
            assert_eq!(decoded.record_id, write.record_id);
            assert_eq!(decoded.op, write.op);
            assert_eq!(decoded.timestamp, write.timestamp);
            assert_eq!(
                decoded.statement_snapshot, 0,
                "live-only snapshot is not persisted"
            );
            let restored = decoded.record().unwrap().unwrap();
            assert_eq!(restored.as_ref(), &record);
            assert_eq!(
                restored.content_hash().unwrap(),
                record.content_hash().unwrap()
            );
            assert_eq!(
                serde_json::to_vec(&stored).unwrap(),
                serde_json::to_vec(&record).unwrap()
            );
            if mask == 0 {
                let original = serde_json::to_vec(&TxFrameRef::Write {
                    tx_id,
                    write: &write,
                })
                .unwrap();
                assert!(
                    original.len() >= bytes.len() + 60,
                    "sparse WAL must remove actual bytes"
                );
            }
        }
    }

    #[test]
    fn sparse_wal_refuses_evicted_stub_bytes() {
        let record = Record::new("stub");
        let stored = StoredRecord::evicted_stub(
            &record,
            crate::record::EvictedPayload {
                fetch: Arc::new(|_| panic!("serialization must reject a stub, not fetch it")),
                pk: Arc::from("stub"),
            },
        );
        let write = write_for(&record);
        assert!(
            serde_json::to_vec(&StoredWriteFrame::new(TransactionId(7), &write, &stored))
                .unwrap_err()
                .to_string()
                .contains("evicted")
        );
    }

    #[test]
    fn sparse_wal_recovery_respects_commit_abort_and_savepoint_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::EmbeddedMemory);
        {
            let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
            db.create_collection("rows").unwrap();
        }
        let path = dir.path().join(DEFAULT_TRANSACTION_LOG);
        let mut payloads = Vec::new();
        for (tx, id, value) in [
            (10, "kept", 1),
            (10, "truncated", 2),
            (11, "aborted", 3),
            (12, "pending", 4),
        ] {
            let record = Record::new(id).with_metadata(json!({"value": value}));
            let stored = StoredRecord::from_record(&record).unwrap();
            let write = write_for(&record);
            payloads.push(
                serde_json::to_vec(&StoredWriteFrame::new(TransactionId(tx), &write, &stored))
                    .unwrap(),
            );
        }
        payloads.push(
            serde_json::to_vec(&TxFrame::Truncate {
                tx_id: TransactionId(10),
                len: 1,
            })
            .unwrap(),
        );
        payloads.push(
            serde_json::to_vec(&TxFrame::Commit {
                tx_id: TransactionId(10),
                timestamp: 9877,
                commit_seq: 1,
            })
            .unwrap(),
        );
        payloads.push(
            serde_json::to_vec(&TxFrame::Abort {
                tx_id: TransactionId(11),
                timestamp: 9877,
            })
            .unwrap(),
        );
        storage::append_frames(
            &path,
            FrameKind::Transaction,
            &payloads,
            false,
            &CompressionConfig::default(),
            &EncryptionRuntime::disabled(),
        )
        .unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
            assert_eq!(
                db.get("rows", "kept").unwrap().unwrap().metadata["value"],
                1
            );
            for id in ["truncated", "aborted", "pending"] {
                assert!(
                    db.get("rows", id).unwrap().is_none(),
                    "must not replay {id}"
                );
            }
        }
    }
}
