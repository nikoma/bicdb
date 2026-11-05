//! Identity-bearing BLOB cells and transactional current-owner metadata.
//! Runtime locator binding/APPEND are a separate integration layer.
use super::*;
use uuid::Uuid;

pub(crate) const OWNERS: &str = "__bicdb_oracle_lob_owners";
const CELL: &str = "$bicdb_oracle_blob_v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Descriptor {
    pub value_id: Uuid,
    pub collection: String,
    pub column: String,
    pub record_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stored {
    format: u32,
    value_id: String,
    payload: JsonValue,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Owner {
    format: u32,
    collection: String,
    column: String,
    record_id: String,
}

fn corrupt() -> SqlError {
    SqlError::data_exception("XX001", "invalid stored Oracle BLOB identity", None)
}

pub(crate) fn is_column(column: &ColumnSchema) -> bool {
    matches!(column.type_modifier, Some(PgTypeModifier::OracleBlobV2))
}

pub(crate) fn has_columns(schema: &TableSchema) -> bool {
    schema.columns.iter().any(is_column)
}

fn decode(value: &JsonValue) -> Result<Option<(Uuid, Vec<u8>)>> {
    let Some(cell) = value.get(CELL) else {
        return Ok(None);
    };
    if value.as_object().is_none_or(|object| object.len() != 1) {
        return Err(corrupt());
    }
    let stored: Stored = serde_json::from_value(cell.clone()).map_err(|_| corrupt())?;
    let id = Uuid::parse_str(&stored.value_id).map_err(|_| corrupt())?;
    if stored.format != 1 || id.is_nil() || id.to_string() != stored.value_id {
        return Err(corrupt());
    }
    let value = storage_json_to_sql_value(&stored.payload, "bytea");
    let bytes = crate::eval::bytea_argument(&value)
        .map_err(|_| corrupt())?
        .ok_or_else(corrupt)?;
    let canonical = storage_envelope(SqlValue::String(format_bytea_hex(&bytes)), Some("bytea"))
        .map_err(|_| corrupt())?
        .into_json();
    if stored.payload != canonical {
        return Err(corrupt());
    }
    Ok(Some((id, bytes)))
}

fn encode(id: Uuid, value: SqlValue) -> Result<JsonValue> {
    let stored = Stored {
        format: 1,
        value_id: id.to_string(),
        payload: storage_envelope(value, Some("bytea"))?.into_json(),
    };
    Ok(serde_json::json!({(CELL):serde_json::to_value(stored).expect("serializable BLOB cell")}))
}

pub(crate) fn storage(value: SqlValue) -> Result<JsonValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(JsonValue::Null);
    }
    let id = match &value {
        SqlValue::OracleLob(blob) => blob
            .stored_descriptor()
            .map(|descriptor| descriptor.value_id),
        _ => None,
    }
    .unwrap_or_else(Uuid::new_v4);
    encode(id, value)
}

/// The ordinary internal column reader remains a byte-value reader. Only the
/// fallible, schema-qualified projection path below constructs a descriptor.
pub(crate) fn binary_value(value: &JsonValue) -> Result<SqlValue> {
    match decode(value)? {
        Some((_, bytes)) => Ok(SqlValue::String(format_bytea_hex(&bytes))),
        None => Ok(storage_json_to_sql_value(value, "bytea")),
    }
}

pub(crate) fn read(record: &Record, collection: &str, column: &str) -> Result<SqlValue> {
    let Some(value) = record.metadata.get(column) else {
        // Legacy native payload storage remains value-only until reconciliation.
        return Ok(if column.eq_ignore_ascii_case("payload") {
            record
                .payload
                .as_ref()
                .map(|bytes| SqlValue::String(format_bytea_hex(bytes)))
                .unwrap_or(SqlValue::Null)
        } else {
            SqlValue::Null
        });
    };
    if value.is_null() {
        return Ok(SqlValue::Null);
    }
    let Some((id, bytes)) = decode(value)? else {
        return binary_value(value);
    };
    let descriptor = Descriptor {
        value_id: id,
        collection: collection.into(),
        column: column.into(),
        record_id: record.id.clone(),
    };
    Ok(SqlValue::OracleLob(Arc::new(lobs::Value::stored(
        descriptor, bytes,
    ))))
}

pub(crate) fn lookup(db: &BicDb, tx: Option<&Transaction>, id: Uuid) -> Result<Option<Descriptor>> {
    let record = match tx {
        Some(tx) => tx.get(OWNERS, &id.to_string())?,
        None => db.get(OWNERS, &id.to_string())?,
    };
    let Some(record) = record else {
        return Ok(None);
    };
    let owner: Owner = serde_json::from_value(record.metadata.clone()).map_err(|_| corrupt())?;
    if owner.format != 1
        || owner.collection.is_empty()
        || owner.column.is_empty()
        || owner.record_id.is_empty()
    {
        return Err(corrupt());
    }
    let current = match tx {
        Some(tx) => tx.get(&owner.collection, &owner.record_id),
        None => db.get(&owner.collection, &owner.record_id),
    };
    let current = match current {
        Ok(current) => current,
        Err(BicDbError::CollectionNotFound(_)) => None,
        Err(error) => return Err(error.into()),
    };
    let matches = current
        .as_ref()
        .and_then(|record| record.metadata.get(&owner.column))
        .map(decode)
        .transpose()?
        .flatten()
        .is_some_and(|(current, _)| current == id);
    Ok(matches.then(|| Descriptor {
        value_id: id,
        collection: owner.collection,
        column: owner.column,
        record_id: owner.record_id,
    }))
}

impl SqlSession<'_> {
    pub(crate) fn prepare_blob_records(
        &mut self,
        table: &str,
        records: &mut [Record],
        moved_from: Option<&Record>,
    ) -> Result<()> {
        let Some(schema) =
            load_schema_shared(self.db_ref(), table)?.filter(|schema| has_columns(schema))
        else {
            return Ok(());
        };
        let tx = self.tx.as_mut().ok_or_else(|| {
            unsupported("identity-bearing BLOB writes require a statement transaction")
        })?;
        for record in records {
            let before = match moved_from {
                Some(before) => Some(Arc::new(before.clone())),
                None => tx.get(table, &record.id)?,
            };
            for column in schema.columns.iter().filter(|column| is_column(column)) {
                let previous = before
                    .as_ref()
                    .and_then(|row| row.metadata.get(&column.name))
                    .map(decode)
                    .transpose()?
                    .flatten()
                    .map(|(id, _)| id);
                let value = record
                    .metadata
                    .get(&column.name)
                    .cloned()
                    .or_else(|| {
                        (column.name.eq_ignore_ascii_case("payload"))
                            .then(|| {
                                record
                                    .payload
                                    .as_ref()
                                    .map(|bytes| JsonValue::String(format_bytea_hex(bytes)))
                            })
                            .flatten()
                    })
                    .unwrap_or(JsonValue::Null);
                let candidate = decode(&value)?;
                let next = if value.is_null() {
                    None
                } else {
                    let (candidate_id, bytes) = match candidate {
                        Some((id, bytes)) => (Some(id), bytes),
                        None => (
                            None,
                            crate::eval::bytea_argument(&storage_json_to_sql_value(
                                &value, "bytea",
                            ))?
                            .ok_or_else(corrupt)?,
                        ),
                    };
                    let id = if candidate_id.is_some_and(|id| Some(id) == previous) {
                        candidate_id.unwrap_or_else(Uuid::new_v4)
                    } else {
                        Uuid::new_v4()
                    };
                    let cell = encode(id, SqlValue::String(format_bytea_hex(&bytes)))?;
                    set_json_object_value(&mut record.metadata, &column.name, cell);
                    if column.name.eq_ignore_ascii_case("payload") {
                        record.payload = None;
                    }
                    Some(id)
                };
                if previous != next {
                    if let Some(id) = previous {
                        tx.delete(OWNERS, &id.to_string())?;
                    }
                }
                if let Some(id) = next {
                    let owner = Owner {
                        format: 1,
                        collection: table.into(),
                        column: column.name.clone(),
                        record_id: record.id.clone(),
                    };
                    tx.insert(
                        OWNERS,
                        Record::new(id.to_string()).with_metadata(
                            serde_json::to_value(owner).expect("serializable BLOB owner"),
                        ),
                    )?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn remove_blob_owners(&mut self, table: &str, ids: &[(String, u64)]) -> Result<()> {
        let Some(schema) =
            load_schema_shared(self.db_ref(), table)?.filter(|schema| has_columns(schema))
        else {
            return Ok(());
        };
        let tx = self.tx.as_mut().ok_or_else(|| {
            unsupported("identity-bearing BLOB deletes require a statement transaction")
        })?;
        for (id, _) in ids {
            if let Some(record) = tx.get(table, id)? {
                for column in schema.columns.iter().filter(|column| is_column(column)) {
                    if let Some((id, _)) = record
                        .metadata
                        .get(&column.name)
                        .map(decode)
                        .transpose()?
                        .flatten()
                    {
                        tx.delete(OWNERS, &id.to_string())?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(session: &mut SqlSession<'_>, sql: &str) -> Result<SqlResult> {
        let sql = lower_session_sql(session.db_ref(), sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        session.execute(&sql)
    }
    fn selected(session: &mut SqlSession<'_>, id: i32) -> Descriptor {
        let value = run(session, &format!("SELECT b FROM lob_values WHERE id={id}"))
            .unwrap()
            .rows
            .remove(0)
            .remove(0);
        let SqlValue::OracleLob(value) = value else {
            panic!("missing stored identity: {value:?}")
        };
        value.stored_descriptor().unwrap().clone()
    }
    fn owner(session: &SqlSession<'_>, id: Uuid) -> Option<Descriptor> {
        lookup(session.db_ref(), session.tx.as_ref(), id).unwrap()
    }
    #[test]
    fn persistent_identity_copy_replacement_and_transactional_owner_lookup() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut s = SqlSession::new(&mut db);
        run(
            &mut s,
            "CREATE TABLE lob_values(id NUMBER PRIMARY KEY,b BLOB,n NUMBER)",
        )
        .unwrap();
        for name in [OWNERS, PROFILE_COLLECTION] {
            assert!(s.execute(&format!("SELECT * FROM \"{name}\"")).is_err());
            assert!(s.execute(&format!("DELETE FROM \"{name}\"")).is_err());
        }
        run(&mut s, "INSERT INTO lob_values VALUES(1,HEXTORAW('AB'),0)").unwrap();
        let first = selected(&mut s, 1);
        assert_eq!(owner(&s, first.value_id), Some(first.clone()));
        run(&mut s, "UPDATE lob_values SET n=1 WHERE id=1").unwrap();
        assert_eq!(selected(&mut s, 1), first);
        run(&mut s, "UPDATE lob_values SET b=b WHERE id=1").unwrap();
        assert_eq!(selected(&mut s, 1), first);
        run(
            &mut s,
            "INSERT INTO lob_values SELECT 2,b,n FROM lob_values WHERE id=1",
        )
        .unwrap();
        let copied = selected(&mut s, 2);
        assert_ne!(copied.value_id, first.value_id);
        run(&mut s, "BEGIN").unwrap();
        run(&mut s, "UPDATE lob_values SET b=HEXTORAW('AB') WHERE id=1").unwrap();
        let replacement = selected(&mut s, 1);
        assert_ne!(replacement.value_id, first.value_id);
        assert!(owner(&s, first.value_id).is_none());
        assert_eq!(owner(&s, replacement.value_id), Some(replacement.clone()));
        run(&mut s, "ROLLBACK").unwrap();
        assert_eq!(selected(&mut s, 1), first);
        assert_eq!(owner(&s, first.value_id), Some(first.clone()));
        assert!(owner(&s, replacement.value_id).is_none());
        run(&mut s, "BEGIN").unwrap();
        run(&mut s, "DELETE FROM lob_values WHERE id=1").unwrap();
        assert!(owner(&s, first.value_id).is_none());
        run(&mut s, "ROLLBACK").unwrap();
        assert_eq!(owner(&s, first.value_id), Some(first.clone()));
        drop(s);
        migrate_legacy_typed_storage(&mut db, 8).unwrap();
        let mut s = SqlSession::new(&mut db);
        assert_eq!(selected(&mut s, 1), first);
        assert_eq!(owner(&s, first.value_id), Some(first.clone()));
        drop(s);
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut s = SqlSession::new(&mut db);
        assert_eq!(selected(&mut s, 1), first);
        assert_eq!(owner(&s, first.value_id), Some(first));
        assert_eq!(selected(&mut s, 2), copied);
    }

    #[test]
    fn primary_key_move_preserves_value_identity_and_rolls_back_owner_location() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut s = SqlSession::new(&mut db);
        run(
            &mut s,
            "CREATE TABLE lob_values(id NUMBER PRIMARY KEY,b BLOB)",
        )
        .unwrap();
        run(&mut s, "INSERT INTO lob_values VALUES(1,HEXTORAW('AB'))").unwrap();
        let first = selected(&mut s, 1);
        run(&mut s, "BEGIN").unwrap();
        run(&mut s, "SAVEPOINT before_move").unwrap();
        run(&mut s, "UPDATE lob_values SET id=3 WHERE id=1").unwrap();
        let moved = selected(&mut s, 3);
        assert_eq!(moved.value_id, first.value_id);
        assert_ne!(moved.record_id, first.record_id);
        assert_eq!(owner(&s, first.value_id), Some(moved));
        run(&mut s, "ROLLBACK TO SAVEPOINT before_move").unwrap();
        assert_eq!(selected(&mut s, 1), first);
        assert_eq!(owner(&s, first.value_id), Some(first.clone()));
        run(&mut s, "UPDATE lob_values SET id=4 WHERE id=1").unwrap();
        run(&mut s, "COMMIT").unwrap();
        let moved = selected(&mut s, 4);
        assert_eq!(moved.value_id, first.value_id);
        assert_eq!(owner(&s, first.value_id), Some(moved));
    }

    #[test]
    fn corrupt_identity_envelopes_fail_closed() {
        let id = Uuid::new_v4();
        let valid = encode(id, SqlValue::String("\\xab".into())).unwrap();
        assert_eq!(decode(&valid).unwrap(), Some((id, vec![0xab])));
        for invalid in [
            serde_json::json!({(CELL):{"format":1,"value_id":id.to_string(),"payload":{"unknown":1}}}),
            serde_json::json!({(CELL):{"format":1,"value_id":id.to_string(),"payload":"\\xab"}}),
            serde_json::json!({(CELL):{"format":1,"value_id":Uuid::nil().to_string(),"payload":"\\xab"}}),
            serde_json::json!({(CELL):{"format":2,"value_id":id.to_string(),"payload":"\\xab"}}),
            serde_json::json!({(CELL):{"format":1,"value_id":id.to_string(),"payload":"\\xab","extra":1}}),
            serde_json::json!({(CELL):valid[CELL],"extra":1}),
        ] {
            assert!(decode(&invalid).is_err(), "{invalid}");
        }
    }
}
