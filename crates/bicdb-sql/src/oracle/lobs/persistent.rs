//! Stored-column acquisition and authorized row-backed BLOB operations.
use super::*;

fn lock_collections(session: &SqlSession<'_>, query: &Query) -> Result<BTreeSet<String>> {
    let mut result = BTreeSet::new();
    if query.locks.is_empty() {
        return Ok(result);
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(unsupported("LOB locking acquisition requires SELECT"));
    };
    for lock in &query.locks {
        let requested = lock.of.as_ref().map(object_name).transpose()?;
        for from in &select.from {
            for relation in
                std::iter::once(&from.relation).chain(from.joins.iter().map(|join| &join.relation))
            {
                let TableFactor::Table {
                    name,
                    alias,
                    args: None,
                    ..
                } = relation
                else {
                    continue;
                };
                let logical = object_name(name)?;
                let alias = alias
                    .as_ref()
                    .map(|alias| ident_value(&alias.name))
                    .unwrap_or_else(|| object_name_parts(name).last().cloned().unwrap_or_default());
                if requested
                    .as_ref()
                    .is_none_or(|requested| requested == &logical || requested == &alias)
                {
                    result.insert(resolve_session_relation_name(
                        session.db_ref(),
                        &relation_name(name)?,
                    )?);
                }
            }
        }
    }
    Ok(result)
}

impl SqlSession<'_> {
    pub(crate) fn bind_blob_result(
        &mut self,
        result: &mut SqlResult,
        query: Option<&Query>,
        returning: bool,
    ) -> Result<()> {
        let locked = query
            .map(|query| lock_collections(self, query))
            .transpose()?
            .unwrap_or_default();
        let instance = self.db_ref().instance_id();
        for row in &mut result.rows {
            for value in row {
                let SqlValue::OracleLob(blob) = value else {
                    continue;
                };
                if blob.version.is_some() {
                    continue;
                }
                let Some(mut descriptor) = blob.stored_descriptor().cloned() else {
                    continue;
                };
                // RETURNING projections can precede normalization of copy/new
                // identities. Acquire from the actual buffered row, never its
                // provisional UUID.
                if returning {
                    let tx = self.tx.as_ref().ok_or_else(|| {
                        unsupported("BLOB RETURNING requires a pending transaction")
                    })?;
                    let record = tx
                        .get(&descriptor.collection, &descriptor.record_id)?
                        .ok_or_else(|| {
                            errors::database_error(-22922, "55000", "nonexistent LOB value")
                        })?;
                    let actual = super::super::lob_storage::read(
                        &record,
                        &descriptor.collection,
                        &descriptor.column,
                    )?;
                    let SqlValue::OracleLob(actual) = actual else {
                        return Err(unsupported(
                            "BLOB RETURNING requires identity-bearing storage",
                        ));
                    };
                    descriptor = actual.stored_descriptor().cloned().expect("stored column");
                    *blob = actual;
                }
                let bytes = blob.read_bytes()?;
                let tx = self.tx.as_ref().ok_or_else(|| {
                    unsupported("persistent BLOB acquisition requires a transaction-backed routine")
                })?;
                let acquisition = if locked.contains(&descriptor.collection) {
                    history::Acquisition::ForUpdate
                } else {
                    history::Acquisition::Snapshot
                };
                let read = self.oracle_lobs.capture(
                    tx,
                    history::ValueIdentity {
                        collection: descriptor.collection.clone(),
                        column: descriptor.column.clone(),
                        value_id: descriptor.value_id,
                    },
                    descriptor.record_id.clone(),
                    bytes,
                    acquisition,
                )?;
                let blob = Arc::make_mut(blob);
                blob.version = Some(read);
                blob.database_instance = Some(instance);
            }
        }
        Ok(())
    }
}

pub(super) fn append(
    session: &mut SqlSession<'_>,
    destination: &Value,
    source: &[u8],
) -> Result<Value> {
    let descriptor = destination
        .stored_descriptor()
        .ok_or_else(|| unsupported("persistent APPEND requires a stored BLOB"))?;
    if destination.database_instance != Some(session.db_ref().instance_id()) {
        return Err(unsupported(
            "BLOB locator belongs to another database instance",
        ));
    }
    // Readability and mutation validity differ. In particular, ordinary
    // snapshots can remain readable after their origin replaced/deleted a row.
    let base = destination.read_bytes()?;
    let owner = super::super::lob_storage::lookup(
        session.db_ref(),
        session.tx.as_ref(),
        descriptor.value_id,
    )?;
    let location = owner.as_ref().unwrap_or(descriptor);
    let missing_lock = || {
        errors::database_error(
            -22920,
            "55000",
            "row containing the LOB value is not locked",
        )
    };
    let tx = session.tx.as_ref().ok_or_else(missing_lock)?;
    if !tx.holds_record_lock(&location.collection, &location.record_id) {
        return Err(missing_lock());
    }
    let Some(owner) = owner else {
        return Err(
            match tx.record_write_tag(WRITE_TAG_NAMESPACE, descriptor.value_id.as_u128())? {
                Some(1) => errors::database_error(-22275, "55000", "invalid LOB locator specified"),
                _ => errors::database_error(-22922, "55000", "nonexistent LOB value"),
            },
        );
    };
    let schema = load_schema_shared(session.db_ref(), &owner.collection)?
        .ok_or_else(|| unsupported("LOB owner schema is unavailable"))?;
    let column = schema
        .column(&owner.column)
        .filter(|column| super::super::lob_storage::is_column(column))
        .ok_or_else(|| unsupported("LOB owner column has changed"))?;
    let assignment = Assignment {
        target: AssignmentTarget::ColumnName(ObjectName::from(Ident::with_quote(
            '"',
            &owner.column,
        ))),
        value: Expr::Value(sqlparser::ast::Value::Null.into()),
    };
    session.require_update_privileges(&owner.collection, &[assignment])?;
    let before = session
        .tx
        .as_ref()
        .expect("checked transaction")
        .get(&owner.collection, &owner.record_id)?
        .ok_or_else(|| errors::database_error(-22922, "55000", "nonexistent LOB value"))?;
    if schema.rls_enabled {
        let prepared = prepare_rls_with_schema(
            session.db_ref(),
            &owner.collection,
            PolicyAction::Update,
            Some(&schema),
            &session.session_gucs,
            session.security_context.as_ref(),
            false,
        )?;
        if let PreparedRls::Filter(filter) = prepared {
            let verdict = filter.verdict(&session.sql_engine(), &schema, &before)?;
            if !matches!(verdict, RlsRowVerdict::Allowed) {
                return Err(rls_check_denied(&owner.collection, &verdict));
            }
        }
    }
    let current = super::super::lob_storage::read(&before, &owner.collection, &owner.column)?;
    let SqlValue::OracleLob(current) = current else {
        return Err(unsupported("LOB owner has no identity-bearing value"));
    };
    // Reuse in a later transaction keeps the old append base, but rollback
    // must use this transaction's CURRENT owner baseline.
    let version = match &destination.version {
        Some(version) if version.belongs_to(session.tx.as_ref().expect("checked transaction")) => {
            version.clone()
        }
        _ => session
            .oracle_lobs
            .capture(
                session.tx.as_ref().expect("checked transaction"),
                history::ValueIdentity {
                    collection: owner.collection.clone(),
                    column: owner.column.clone(),
                    value_id: owner.value_id,
                },
                owner.record_id.clone(),
                current.read_bytes()?,
                history::Acquisition::Snapshot,
            )?
            .with_read_bytes(base.clone()),
    };
    let mut bytes = base.to_vec();
    bytes.try_reserve(source.len()).map_err(|_| {
        SqlError::data_exception("53200", "insufficient memory for BLOB append", None)
    })?;
    bytes.extend_from_slice(source);
    let mut after = (*before).clone();
    let stored = Value::stored(owner.clone(), bytes);
    let encoded =
        column_storage_envelope(SqlValue::OracleLob(Arc::new(stored)), Some(column))?.into_json();
    set_json_object_value(&mut after.metadata, &owner.column, encoded);
    session.materialize_generated_columns(
        &owner.collection,
        &schema,
        std::slice::from_mut(&mut after),
    )?;
    validate_updated_records(
        session.db_ref(),
        session.tx.as_ref(),
        &owner.collection,
        &schema,
        std::slice::from_ref(&after),
        &[((*before).clone(), after.clone())],
        Some(&session.sql_engine()),
        &[],
    )?;
    session.enforce_rls_checks(
        &owner.collection,
        Some(&schema),
        PolicyAction::Update,
        std::slice::from_ref(&after),
    )?;
    // Native/SQL protection grants, indexes and transaction rollback remain
    // active; ordinary SQL UPDATE triggers are deliberately not invoked.
    session.update_session_record(&owner.collection, after)?;
    let version = version.after_current_write(
        session.tx.as_mut().expect("checked transaction"),
        &owner.record_id,
    )?;
    let mut result = destination.clone();
    result.storage = Storage::Stored(owner);
    result.version = Some(version);
    result.bytes = result.read_bytes()?.to_vec();
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(s: &mut SqlSession<'_>, sql: &str) -> Result<SqlResult> {
        let sql = lower_session_sql(s.db_ref(), sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        s.execute(&sql)
    }
    fn setup() -> (tempfile::TempDir, BicDb) {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut s = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE lob_values(id NUMBER PRIMARY KEY,b BLOB)",
            "INSERT INTO lob_values VALUES(1,HEXTORAW('AB'))",
            "CREATE TABLE captures(id NUMBER PRIMARY KEY,b BLOB)",
            "CREATE TABLE error_codes(id NUMBER PRIMARY KEY,n NUMBER)",
        ] {
            run(&mut s, sql).unwrap();
        }
        drop(s);
        (root, db)
    }
    fn captures(s: &mut SqlSession<'_>) -> Vec<String> {
        run(s, "SELECT b FROM captures ORDER BY id")
            .unwrap()
            .rows
            .into_iter()
            .map(|row| row[0].to_cell())
            .collect()
    }
    fn procedure(s: &mut SqlSession<'_>, decl: &str, body: &str) {
        run(
            s,
            &format!(
                "CREATE PROCEDURE exercise_lobs IS caught_code NUMBER; {decl} BEGIN {body} END;"
            ),
        )
        .unwrap();
        run(s, "CALL exercise_lobs()").unwrap();
    }
    #[test]
    fn stored_append_and_self_append_preserve_owner_and_reopen_bytes() {
        let (root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        procedure(&mut s,"d BLOB;s BLOB:=HEXTORAW('01');", "SELECT b INTO d FROM lob_values WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(d,s); DBMS_LOB.APPEND(d,d);");
        assert_eq!(
            run(&mut s, "SELECT b FROM lob_values").unwrap().rows[0][0].to_cell(),
            "\\xab01ab01"
        );
        drop(s);
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert_eq!(
            run(&mut SqlSession::new(&mut db), "SELECT b FROM lob_values")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "\\xab01ab01"
        );
    }
    #[test]
    fn returning_binds_the_actual_insert_and_replacement_identity() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        procedure(&mut s,"d BLOB;s BLOB:=HEXTORAW('01');", "INSERT INTO lob_values VALUES(2,EMPTY_BLOB()) RETURNING b INTO d; DBMS_LOB.APPEND(d,s); INSERT INTO captures VALUES(1,d); UPDATE lob_values SET b=EMPTY_BLOB() WHERE id=2 RETURNING b INTO d; DBMS_LOB.APPEND(d,s); DBMS_LOB.APPEND(d,s); INSERT INTO captures VALUES(2,d);");
        assert_eq!(captures(&mut s), vec!["\\x01", "\\x0101"]);
        assert_eq!(
            run(&mut s, "SELECT b FROM lob_values WHERE id=2")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "\\x0101"
        );
    }
    #[test]
    fn independent_alias_writes_keep_pending_snapshots_then_share_origin_commit() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        procedure(&mut s,"d BLOB;e BLOB;s BLOB:=HEXTORAW('01');t BLOB:=HEXTORAW('02');", "SELECT b INTO d FROM lob_values WHERE id=1 FOR UPDATE; SELECT b INTO e FROM lob_values WHERE id=1; DBMS_LOB.APPEND(d,s); DBMS_LOB.APPEND(e,t); INSERT INTO captures VALUES(1,d); INSERT INTO captures VALUES(2,e); COMMIT; INSERT INTO captures VALUES(3,d); INSERT INTO captures VALUES(4,e);");
        assert_eq!(
            captures(&mut s),
            vec!["\\xab01", "\\xab02", "\\xab02", "\\xab02"]
        );
    }
    #[test]
    fn rollback_distinguishes_ordinary_and_locked_late_acquisitions() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        procedure(&mut s,"d BLOB;e BLOB;x BLOB;s BLOB:=HEXTORAW('01');", "SELECT b INTO d FROM lob_values WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(d,s); SELECT b INTO e FROM lob_values WHERE id=1; SELECT b INTO x FROM lob_values WHERE id=1 FOR UPDATE; ROLLBACK; INSERT INTO captures VALUES(1,d); INSERT INTO captures VALUES(2,e); INSERT INTO captures VALUES(3,x);");
        assert_eq!(captures(&mut s), vec!["\\xab", "\\xab01", "\\xab"]);
    }
    #[test]
    fn rolled_back_alias_write_follows_the_retained_prefix_without_refreshing_unwritten_alias() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        procedure(&mut s,"d BLOB;e BLOB;f BLOB;s BLOB:=HEXTORAW('01');t BLOB:=HEXTORAW('02');", "SELECT b INTO d FROM lob_values WHERE id=1 FOR UPDATE; e:=d; SELECT b INTO f FROM lob_values WHERE id=1; DBMS_LOB.APPEND(d,s); SAVEPOINT kept; DBMS_LOB.APPEND(e,t); ROLLBACK TO kept; INSERT INTO captures VALUES(1,d); INSERT INTO captures VALUES(2,e); INSERT INTO captures VALUES(3,f);");
        assert_eq!(captures(&mut s), vec!["\\xab01", "\\xab01", "\\xab"]);
    }
    #[test]
    fn pending_deletion_and_replacement_have_distinct_append_errors_and_committed_locked_reads_expire(
    ) {
        for (mutation, code) in [
            ("DELETE FROM lob_values WHERE id=1", -22275),
            ("UPDATE lob_values SET b=HEXTORAW('CD') WHERE id=1", -22922),
        ] {
            let (_root, mut db) = setup();
            let mut s = SqlSession::new(&mut db);
            procedure(&mut s,"d BLOB;s BLOB:=HEXTORAW('01');", &format!("SELECT b INTO d FROM lob_values WHERE id=1 FOR UPDATE; {mutation}; INSERT INTO captures VALUES(1,d); BEGIN DBMS_LOB.APPEND(d,s); EXCEPTION WHEN OTHERS THEN caught_code:=SQLCODE; INSERT INTO error_codes VALUES(1,caught_code); END; COMMIT; BEGIN INSERT INTO captures VALUES(2,d); EXCEPTION WHEN OTHERS THEN caught_code:=SQLCODE; INSERT INTO error_codes VALUES(2,caught_code); END;"));
            assert_eq!(captures(&mut s), vec!["\\xab"]);
            let codes = run(&mut s, "SELECT n FROM error_codes ORDER BY id")
                .unwrap()
                .rows
                .into_iter()
                .map(|row| row[0].to_cell())
                .collect::<Vec<_>>();
            assert_eq!(codes, vec![code.to_string(), "-22922".into()]);
        }
    }
    #[test]
    fn ordinary_snapshot_survives_origin_replacement_commit_but_append_requires_a_new_lock() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        procedure(&mut s,"d BLOB;s BLOB:=HEXTORAW('01');", "SELECT b INTO d FROM lob_values WHERE id=1; UPDATE lob_values SET b=HEXTORAW('CD') WHERE id=1; COMMIT; INSERT INTO captures VALUES(1,d); BEGIN DBMS_LOB.APPEND(d,s); EXCEPTION WHEN OTHERS THEN caught_code:=SQLCODE; INSERT INTO error_codes VALUES(1,caught_code); END;");
        assert_eq!(captures(&mut s), vec!["\\xab"]);
        assert_eq!(
            run(&mut s, "SELECT n FROM error_codes").unwrap().rows[0][0].to_cell(),
            "-22920"
        );
    }
    #[test]
    fn primary_key_move_keeps_the_original_locator_writable() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        procedure(&mut s,"d BLOB;s BLOB:=HEXTORAW('01');", "SELECT b INTO d FROM lob_values WHERE id=1 FOR UPDATE; UPDATE lob_values SET id=3 WHERE id=1; DBMS_LOB.APPEND(d,s); COMMIT; INSERT INTO captures VALUES(1,d);");
        assert_eq!(captures(&mut s), vec!["\\xab01"]);
        assert_eq!(
            run(&mut s, "SELECT b FROM lob_values WHERE id=3")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "\\xab01"
        );
    }
    #[test]
    fn later_transaction_rebind_uses_current_owner_baseline_for_rollback() {
        for rollback in ["ROLLBACK", "ROLLBACK TO before_append"] {
            let (_root, mut db) = setup();
            let mut s = SqlSession::new(&mut db);
            procedure(&mut s,"d BLOB;e BLOB;x BLOB;s BLOB:=HEXTORAW('01');t BLOB:=HEXTORAW('02');", &format!("SELECT b INTO d FROM lob_values WHERE id=1 FOR UPDATE; COMMIT; SELECT b INTO e FROM lob_values WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(e,s); COMMIT; SELECT b INTO e FROM lob_values WHERE id=1 FOR UPDATE; SAVEPOINT before_append; DBMS_LOB.APPEND(d,t); SELECT b INTO x FROM lob_values WHERE id=1 FOR UPDATE; {rollback}; INSERT INTO captures VALUES(1,d); INSERT INTO captures VALUES(2,e); INSERT INTO captures VALUES(3,x);"));
            assert_eq!(
                captures(&mut s),
                vec!["\\xab01", "\\xab01", "\\xab01"],
                "{rollback}"
            );
        }
    }
    #[test]
    fn sql_called_persistent_append_is_refused_without_mutation() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        run(&mut s,"CREATE FUNCTION illegal_lob_write RETURN NUMBER IS d BLOB;s BLOB:=HEXTORAW('01');BEGIN SELECT b INTO d FROM lob_values WHERE id=1 FOR UPDATE; DBMS_LOB.APPEND(d,s); RETURN 1; END;").unwrap();
        let error = run(&mut s, "SELECT illegal_lob_write() FROM DUAL").unwrap_err();
        assert!(error.to_string().contains("ORA-14553"), "{error}");
        assert_eq!(
            run(&mut s, "SELECT b FROM lob_values").unwrap().rows[0][0].to_cell(),
            "\\xab"
        );
    }
    fn acquire(s: &mut SqlSession<'_>) -> Arc<Value> {
        s.execute("BEGIN").unwrap();
        let sql = lower_session_sql(
            s.db_ref(),
            "SELECT b FROM lob_values WHERE id=1 FOR UPDATE",
            ORACLE_PREVIEW_PROFILE,
            None,
            "bicdb",
        )
        .unwrap();
        let query = parse_single_query(&sql).unwrap();
        let mut result = s.execute(&sql).unwrap();
        s.bind_blob_result(&mut result, Some(&query), false)
            .unwrap();
        let SqlValue::OracleLob(blob) = result.rows.remove(0).remove(0) else {
            panic!("missing BLOB")
        };
        blob
    }
    #[test]
    fn previously_acquired_locator_does_not_bypass_current_update_rights_or_row_policy() {
        for policy in [false, true] {
            let (_root, mut db) = setup();
            let mut s = SqlSession::new(&mut db);
            s.execute("CREATE ROLE limited; GRANT SELECT ON \"LOB_VALUES\" TO limited")
                .unwrap();
            if policy {
                s.execute("GRANT UPDATE ON \"LOB_VALUES\" TO limited; ALTER TABLE \"LOB_VALUES\" ENABLE ROW LEVEL SECURITY; CREATE POLICY visible ON \"LOB_VALUES\" FOR SELECT USING(true); CREATE POLICY no_update ON \"LOB_VALUES\" FOR UPDATE USING(false) WITH CHECK(false)").unwrap();
            }
            let blob = acquire(&mut s);
            s.execute("SET ROLE limited").unwrap();
            let error = super::append(&mut s, &blob, &[1]).unwrap_err();
            assert_eq!(error.sqlstate(), "42501", "{error}");
            assert_eq!(
                run(&mut s, "SELECT b FROM lob_values").unwrap().rows[0][0].to_cell(),
                "\\xab"
            );
            s.execute("ROLLBACK").unwrap();
        }
    }
    #[test]
    fn persistent_locator_cannot_mutate_another_instance_with_the_same_stored_uuid() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        let blob = acquire(&mut s);
        let descriptor = blob.stored_descriptor().unwrap().clone();
        let record = (*s
            .tx
            .as_ref()
            .unwrap()
            .get(&descriptor.collection, &descriptor.record_id)
            .unwrap()
            .unwrap())
        .clone();
        s.execute("ROLLBACK").unwrap();
        drop(s);
        let (_other_root, mut other) = setup();
        other.insert(&descriptor.collection, record).unwrap();
        other.insert(super::super::super::lob_storage::OWNERS,Record::new(descriptor.value_id.to_string())
            .with_metadata(serde_json::json!({"format":1,"collection":descriptor.collection,"column":descriptor.column,"record_id":descriptor.record_id}))).unwrap();
        let mut s = SqlSession::new(&mut other);
        let _other_locator = acquire(&mut s);
        let error = super::append(&mut s, &blob, &[1]).unwrap_err();
        assert!(
            error.to_string().contains("another database instance"),
            "{error}"
        );
        assert_eq!(
            run(&mut s, "SELECT b FROM lob_values").unwrap().rows[0][0].to_cell(),
            "\\xab"
        );
    }
    #[test]
    fn chunked_anonymous_blob_append_reproduces_binary_application_submission() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        let expected = (0..8193usize)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut block=String::from("DECLARE d BLOB; BEGIN UPDATE lob_values SET b=EMPTY_BLOB() WHERE id=1; SELECT b INTO d FROM lob_values WHERE id=1 FOR UPDATE;");
        for chunk in expected.chunks(1024) {
            let hex = chunk
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            block.push_str(&format!("DBMS_LOB.APPEND(d,HEXTORAW('{hex}'));"));
        }
        block.push_str("END;");
        run(&mut s, &block).unwrap();
        let value = run(&mut s, "SELECT b FROM lob_values")
            .unwrap()
            .rows
            .remove(0)
            .remove(0);
        assert_eq!(
            crate::eval::bytea_argument(&value).unwrap().unwrap(),
            expected
        );
    }
    #[test]
    fn explicit_binary_reconciliation_creates_writable_owners_and_bytea_conversion_removes_them() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        run(
            &mut s,
            "CREATE TABLE legacy_binary(id NUMBER PRIMARY KEY,b BYTEA)",
        )
        .unwrap();
        run(&mut s, "INSERT INTO legacy_binary VALUES(1,HEXTORAW('AB'))").unwrap();
        run(&mut s, "ALTER TABLE legacy_binary MODIFY(b BLOB)").unwrap();
        run(&mut s,"CREATE PROCEDURE migrate_then_append IS d BLOB;s BLOB:=HEXTORAW('01');BEGIN SELECT b INTO d FROM legacy_binary WHERE id=1 FOR UPDATE;DBMS_LOB.APPEND(d,s);END;").unwrap();
        run(&mut s, "CALL migrate_then_append()").unwrap();
        let value = run(&mut s, "SELECT b FROM legacy_binary")
            .unwrap()
            .rows
            .remove(0)
            .remove(0);
        assert_eq!(value.to_cell(), "\\xab01");
        let SqlValue::OracleLob(blob) = value else {
            panic!("missing identity")
        };
        let id = blob.stored_descriptor().unwrap().value_id;
        assert!(
            super::super::super::lob_storage::lookup(s.db_ref(), None, id)
                .unwrap()
                .is_some()
        );
        run(&mut s, "ALTER TABLE legacy_binary MODIFY(b BYTEA)").unwrap();
        assert_eq!(
            run(&mut s, "SELECT b FROM legacy_binary").unwrap().rows[0][0],
            SqlValue::String("\\xab01".into())
        );
        assert!(
            super::super::super::lob_storage::lookup(s.db_ref(), None, id)
                .unwrap()
                .is_none()
        );
    }
}
