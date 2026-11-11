//! Exact record scope for rename undo. The caller must reserve these relations
//! before capture or mutation. SQL dispatch currently retains conservative
//! native quiescence; scoped concurrent admission is still unqualified.
use super::*;

pub(super) struct RenameDependencies {
    pub(super) relations: BTreeSet<String>,
    metadata: BTreeSet<(String, String)>,
    value_tables: BTreeSet<String>,
}

impl RenameDependencies {
    pub(super) fn reserve(
        &self,
        db: &BicDb,
        owner: &Transaction,
    ) -> Result<bicdb_core::CatalogScopeToken> {
        let mut names = self.relations.clone();
        names.extend(
            db.catalog_operation_scope_collections()
                .iter()
                .map(|name| (*name).into()),
        );
        names.extend(
            rename_journal::scope_collections()
                .iter()
                .map(|name| (*name).into()),
        );
        let records = self.metadata.iter().cloned().collect::<Vec<_>>();
        Ok(owner.reserve_catalog_resources(&names.into_iter().collect::<Vec<_>>(), &records)?)
    }

    /// Includes absent destination keys, so rollback removes newly published
    /// schemas, grants and trigger identities without touching unrelated keys.
    pub(super) fn plan(db: &BicDb, previous: &TableSchema, updated: &TableSchema) -> Result<Self> {
        let old = &previous.name;
        let new = &updated.name;
        let physical_relations = db
            .collections()
            .into_iter()
            .map(|meta| meta.name)
            .collect::<BTreeSet<_>>();
        let mut plan = Self {
            relations: BTreeSet::from([old.clone(), new.clone()]),
            metadata: BTreeSet::from([
                (SCHEMA_COLLECTION.into(), old.clone()),
                (SCHEMA_COLLECTION.into(), new.clone()),
            ]),
            value_tables: BTreeSet::new(),
        };
        for sequence in list_sequences(db)? {
            if sequence.owned_by_table.as_deref() == Some(old.as_str()) {
                plan.metadata
                    .insert((SEQUENCE_COLLECTION.into(), sequence.name));
            }
        }
        for mut grant in list_privileges(db)? {
            if grant.object_type == PrivilegeObjectType::Table && grant.object_name == *old {
                plan.metadata
                    .insert((PRIVILEGE_COLLECTION.into(), privilege_key(&grant)));
                grant.object_name = new.clone();
                plan.metadata
                    .insert((PRIVILEGE_COLLECTION.into(), privilege_key(&grant)));
            }
        }
        for trigger in list_triggers(db)? {
            if trigger.table_name == *old {
                plan.metadata
                    .insert((TRIGGER_COLLECTION.into(), trigger_key(old, &trigger.name)));
                plan.metadata
                    .insert((TRIGGER_COLLECTION.into(), trigger_key(new, &trigger.name)));
            }
        }
        let root = table_row_type_schema(updated);
        let replacements = privilege_projection::composite_type_replacements(db, &root)?;
        let previous_type = table_row_type_schema(previous);
        for mut routine in list_routines(db)? {
            if rewrite_routine_schema_type_identity(&mut routine, &previous_type, &root)? {
                plan.metadata
                    .insert((ROUTINE_COLLECTION.into(), routine_storage_key(&routine)));
            }
        }
        if let Some(partition) = &previous.partition_of {
            plan.relations.insert(partition.parent_table.clone());
        }
        for view in list_views(db)? {
            let direct = view_dependencies::relations(&view.query_sql)?.contains(old);
            let mut columns = view.columns.clone();
            for column in &mut columns {
                hydrate_view_column_type(db, column)?;
            }
            let values = columns.iter_mut().fold(false, |changed, column| {
                replace_column_user_type_references(column, &replacements) || changed
            });
            if direct || values {
                plan.relations.insert(view.name.clone());
                plan.metadata
                    .insert((VIEW_COLLECTION.into(), view.name.clone()));
            }
            // A view descriptor alone does not imply a physical backing.
            // Legacy materialized descriptors are still query-expanded. If a
            // backing exists, its stored values need ordinary row coverage.
            if values && view.materialized && physical_relations.contains(&view.name) {
                plan.value_tables.insert(view.name);
            }
        }
        for user_type in replacements.values().filter(|t| t.oid != root.oid) {
            plan.metadata.insert((
                USER_TYPE_COLLECTION.into(),
                user_type_key(&user_type.schema_name, &user_type.name),
            ));
        }
        for schema in list_schemas(db)? {
            let incoming=schema.constraints.iter().any(|c| matches!(c,ConstraintSchema::ForeignKey{foreign_table,..} if foreign_table==old));
            let mut columns = schema.columns.clone();
            let values = columns.iter_mut().fold(false, |changed, column| {
                replace_column_user_type_references(column, &replacements) || changed
            });
            let partition = schema
                .partition_of
                .as_ref()
                .is_some_and(|part| part.parent_table == *old);
            if incoming || values || partition {
                plan.relations.insert(schema.name.clone());
                plan.metadata
                    .insert((SCHEMA_COLLECTION.into(), schema.name.clone()));
            }
            if values {
                plan.value_tables.insert(schema.name);
            }
        }
        Ok(plan)
    }

    /// Capture before invoking any rename/schema/value mutator. Store each row
    /// separately; do not retain a whole table's row payloads in the SQL host.
    pub(super) fn capture(
        &self,
        db: &mut BicDb,
        old: &str,
        new: &str,
    ) -> Result<rename_journal::PreparedRename> {
        let mut prepared = rename_journal::prepare(db)?;
        let captured = (|| -> Result<()> {
            rename_journal::capture_rename(db, &mut prepared, old, new)?;
            for (collection, id) in &self.metadata {
                rename_journal::capture_row(db, &mut prepared, collection, id)?;
            }
            for collection in &self.value_tables {
                let tx = db.begin_transaction()?;
                let mut after: Option<String> = None;
                loop {
                    let lower = after
                        .as_deref()
                        .map(std::ops::Bound::Excluded)
                        .unwrap_or(std::ops::Bound::Unbounded);
                    let (ids, complete) = tx.scan_collection_record_key_batch_in_range_unchecked(
                        collection,
                        lower,
                        std::ops::Bound::Unbounded,
                        false,
                        128,
                    )?;
                    if ids.is_empty() && !complete {
                        return Err(SqlError::InvalidSql(
                            "rename dependency scan made no progress".into(),
                        ));
                    }
                    for id in &ids {
                        // Key scans are candidates. Resolve visibility through the
                        // normal read gate, never capture tombstones as live rows.
                        if tx.get(collection, id)?.is_some() {
                            rename_journal::capture_row(db, &mut prepared, collection, id)?;
                        }
                    }
                    if complete {
                        break;
                    }
                    after = ids.last().cloned();
                }
                tx.rollback()?;
            }
            Ok(())
        })();
        if let Err(error) = captured {
            rename_journal::discard_unapplied_capture(db, &prepared)?;
            return Err(error);
        }
        Ok(prepared)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bicdb_core::{DbConfig, StorageMode};

    fn catalog<T: serde::Serialize>(rows: Vec<T>) -> Vec<String> {
        let mut rows = rows
            .into_iter()
            .map(|row| serde_json::to_string(&row).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }

    #[test]
    fn captured_dependency_scope_restores_actual_rename_without_reverting_unrelated_rows() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let mut db =
                BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode))
                    .unwrap();
            {
                let mut session = SqlSession::new(&mut db);
                for statement in [
                    "CREATE ROLE rename_reader LOGIN NOSUPERUSER",
                    "CREATE TABLE original(rowkey SERIAL PRIMARY KEY,payload TEXT)",
                    "INSERT INTO original VALUES(1,'acknowledged')",
                    "CREATE TABLE child(id INT PRIMARY KEY,parent INT REFERENCES original(rowkey))",
                    "INSERT INTO child VALUES(1,1)",
                    "CREATE TABLE holder(id INT PRIMARY KEY,item original,items original[])",
                    "INSERT INTO holder VALUES(1,ROW(1,'kept')::original,ARRAY[ROW(1,'array')::original])",
                    "CREATE TYPE wrapper AS(member original)",
                    "CREATE TABLE nested_holder(id INT PRIMARY KEY,item wrapper)",
                    "INSERT INTO nested_holder VALUES(1,ROW(ROW(1,'nested')::original)::wrapper)",
                    "CREATE TABLE unrelated(id INT PRIMARY KEY,payload INT)",
                    "INSERT INTO unrelated VALUES(1,10)",
                    "CREATE VIEW direct_row AS SELECT original AS item FROM original",
                    "CREATE VIEW indirect_row AS SELECT item FROM direct_row",
                    "CREATE MATERIALIZED VIEW stored_row AS SELECT item FROM direct_row",
                    "CREATE VIEW unrelated_view AS SELECT payload FROM unrelated",
                    "GRANT SELECT ON original TO rename_reader",
                    "CREATE FUNCTION rename_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
                    "CREATE TRIGGER rename_trigger BEFORE UPDATE ON original FOR EACH ROW EXECUTE FUNCTION rename_guard()",
                ] { session.execute(statement).unwrap_or_else(|e|panic!("{statement}: {e}")); }
            }
            let previous = load_schema(&db, "original").unwrap().unwrap();
            // Model a descriptor with a real native backing, independently
            // of the legacy SQL materialized-view query-expansion behavior.
            db.create_collection("stored_row").unwrap();
            db.insert(
                "stored_row",
                Record::new("captured").with_metadata(serde_json::json!({"audit":"acknowledged"})),
            )
            .unwrap();
            let mut updated = previous.clone();
            updated.primary_key_name = Some(previous.primary_key_constraint_name());
            updated.name = "renamed".into();
            let schemas = catalog(list_schemas(&db).unwrap());
            let types = catalog(list_user_types(&db).unwrap());
            let privileges = catalog(list_privileges(&db).unwrap());
            let triggers = catalog(list_triggers(&db).unwrap());
            let sequences = catalog(list_sequences(&db).unwrap());
            let views = catalog(list_views(&db).unwrap());
            let plan = RenameDependencies::plan(&db, &previous, &updated).unwrap();
            assert!(plan.relations.contains("child"));
            assert!(plan.relations.contains("holder"));
            assert!(plan.relations.contains("nested_holder"));
            assert!(!plan.relations.contains("unrelated"));
            assert!(plan.relations.contains("direct_row"));
            assert!(plan.relations.contains("indirect_row"));
            assert!(plan.relations.contains("stored_row"));
            assert!(plan.value_tables.contains("stored_row"));
            assert!(!plan.relations.contains("unrelated_view"));
            let prepared = plan.capture(&mut db, "original", "renamed").unwrap();
            {
                let mut session = SqlSession::new(&mut db);
                session
                    .execute("UPDATE unrelated SET payload=99 WHERE id=1")
                    .unwrap();
                session
                    .execute("ALTER TABLE original RENAME TO renamed")
                    .unwrap();
                assert_eq!(
                    session
                        .execute("SELECT pg_typeof(item) FROM holder")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::String("renamed".into())]]
                );
            }
            db.insert(
                "stored_row",
                Record::new("captured").with_metadata(serde_json::json!({"audit":"during rename"})),
            )
            .unwrap();
            // Deliberately leave the captured decision uncommitted to exercise
            // undo of all real mutators before ordinary SQL dispatch is wired.
            rename_journal::rollback(&mut db, prepared.id()).unwrap();
            assert_eq!(
                db.get("stored_row", "captured").unwrap().unwrap().metadata,
                serde_json::json!({"audit":"acknowledged"})
            );
            assert_eq!(catalog(list_schemas(&db).unwrap()), schemas);
            assert_eq!(catalog(list_user_types(&db).unwrap()), types);
            assert_eq!(catalog(list_privileges(&db).unwrap()), privileges);
            assert_eq!(catalog(list_triggers(&db).unwrap()), triggers);
            assert_eq!(catalog(list_sequences(&db).unwrap()), sequences);
            assert_eq!(catalog(list_views(&db).unwrap()), views);
            {
                let mut session = SqlSession::new(&mut db);
                assert_eq!(
                    session
                        .execute(
                            "SELECT pg_typeof(item),(item).payload,(items[1]).payload FROM holder"
                        )
                        .unwrap()
                        .rows,
                    vec![vec![
                        SqlValue::String("original".into()),
                        SqlValue::String("kept".into()),
                        SqlValue::String("array".into())
                    ]]
                );
                assert_eq!(
                    session
                        .execute("SELECT ((item).member).payload FROM nested_holder")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::String("nested".into())]]
                );
                assert_eq!(
                    session
                        .execute("SELECT payload FROM unrelated")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::Int(99)]]
                );
                assert_eq!(
                    session
                        .execute("SELECT (item).payload FROM indirect_row")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::String("acknowledged".into())]]
                );
                assert_eq!(
                    session
                        .execute("SELECT (item).payload FROM stored_row")
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::String("acknowledged".into())]]
                );
                session.execute("INSERT INTO child VALUES(2,1)").unwrap();
                assert_eq!(
                    session
                        .execute("INSERT INTO child VALUES(3,999)")
                        .unwrap_err()
                        .sqlstate(),
                    "23503"
                );
            }
            assert!(db
                .verify_indexes_consistently()
                .unwrap()
                .iter()
                .all(|i| i.valid));
            db.close().unwrap();
        }
    }
    #[test]
    fn failed_capture_cannot_undo_a_later_acknowledged_write_on_reopen() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let config = DbConfig::default().with_storage_mode(mode);
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            SqlSession::new(&mut db).execute(
                "CREATE TABLE original(id INT PRIMARY KEY,payload INT); INSERT INTO original VALUES(1,10)").unwrap();
            let before = load_schema(&db, "original").unwrap().unwrap();
            let mut after = before.clone();
            after.name = "renamed".into();
            let mut plan = RenameDependencies::plan(&db, &before, &after).unwrap();
            // Fail after several real before-images have become durable,
            // while still before the producer is allowed to mutate a source.
            plan.metadata
                .insert(("missing_capture_collection".into(), "key".into()));
            assert!(plan.capture(&mut db, "original", "renamed").is_err());
            assert!(rename_journal::pending(&db).unwrap().is_empty());
            SqlSession::new(&mut db)
                .execute("UPDATE original SET payload=99 WHERE id=1")
                .unwrap();
            db.close().unwrap();
            let mut db = BicDb::open_with_config(root.path(), config).unwrap();
            oracle::initialize_existing_database_profile(&mut db).unwrap();
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT payload FROM original")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(99)]]
            );
            db.close().unwrap();
        }
    }

    #[test]
    fn legacy_view_type_spellings_are_captured_by_oid_before_rename() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE original(id INT,payload TEXT); INSERT INTO original VALUES(1,'kept'); CREATE VIEW direct_row AS SELECT original AS item FROM original; CREATE VIEW indirect_row AS SELECT item FROM direct_row; CREATE MATERIALIZED VIEW stored_row AS SELECT item FROM direct_row").unwrap();
        assert!(load_view(&db, "direct_row").unwrap().unwrap().columns[0]
            .user_type
            .is_some());
        // This is the descriptor shape produced by the independently tested
        // ef48/a683 binaries. Keep the payloads; remove only the newer marker.
        for name in ["direct_row", "indirect_row", "stored_row"] {
            let mut view = load_view(&db, name).unwrap().unwrap();
            view.columns[0].user_type = None;
            save_view(&mut db, &view).unwrap();
        }
        let before = load_schema(&db, "original").unwrap().unwrap();
        let mut after = before.clone();
        after.name = "renamed".into();
        let plan = RenameDependencies::plan(&db, &before, &after).unwrap();
        for name in ["direct_row", "indirect_row", "stored_row"] {
            assert!(plan.relations.contains(name), "{name}");
        }
        assert!(
            !plan.value_tables.contains("stored_row"),
            "descriptor has no physical rows"
        );
        db.create_collection("stored_row").unwrap();
        let backed = RenameDependencies::plan(&db, &before, &after).unwrap();
        assert!(backed.value_tables.contains("stored_row"));
        assert!(
            load_view(&db, "indirect_row").unwrap().unwrap().columns[0]
                .user_type
                .is_none(),
            "planning must not rewrite legacy metadata"
        );
        db.close().unwrap();
    }
}
