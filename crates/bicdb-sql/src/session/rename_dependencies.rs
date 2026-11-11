//! Exact record scope for rename undo. The caller must reserve these relations
//! before capture or mutation; ordinary SQL dispatch is not connected yet.
use super::*;

pub(super) struct RenameDependencies {
    pub(super) relations: BTreeSet<String>,
    metadata: BTreeSet<(String, String)>,
    value_tables: BTreeSet<String>,
}

impl RenameDependencies {
    /// Includes absent destination keys, so rollback removes newly published
    /// schemas, grants and trigger identities without touching unrelated keys.
    pub(super) fn plan(db: &BicDb, previous: &TableSchema, updated: &TableSchema) -> Result<Self> {
        let old = &previous.name;
        let new = &updated.name;
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
                let (ids, complete) = tx.scan_collection_record_key_candidates_in_range_unchecked(
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
                    "GRANT SELECT ON original TO rename_reader",
                    "CREATE FUNCTION rename_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
                    "CREATE TRIGGER rename_trigger BEFORE UPDATE ON original FOR EACH ROW EXECUTE FUNCTION rename_guard()",
                ] { session.execute(statement).unwrap_or_else(|e|panic!("{statement}: {e}")); }
            }
            let previous = load_schema(&db, "original").unwrap().unwrap();
            let mut updated = previous.clone();
            updated.primary_key_name = Some(previous.primary_key_constraint_name());
            updated.name = "renamed".into();
            let schemas = catalog(list_schemas(&db).unwrap());
            let types = catalog(list_user_types(&db).unwrap());
            let privileges = catalog(list_privileges(&db).unwrap());
            let triggers = catalog(list_triggers(&db).unwrap());
            let sequences = catalog(list_sequences(&db).unwrap());
            let plan = RenameDependencies::plan(&db, &previous, &updated).unwrap();
            assert!(plan.relations.contains("child"));
            assert!(plan.relations.contains("holder"));
            assert!(plan.relations.contains("nested_holder"));
            assert!(!plan.relations.contains("unrelated"));
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
            // Deliberately leave the captured decision uncommitted to exercise
            // undo of all real mutators before ordinary SQL dispatch is wired.
            rename_journal::rollback(&mut db, prepared.id()).unwrap();
            assert_eq!(catalog(list_schemas(&db).unwrap()), schemas);
            assert_eq!(catalog(list_user_types(&db).unwrap()), types);
            assert_eq!(catalog(list_privileges(&db).unwrap()), privileges);
            assert_eq!(catalog(list_triggers(&db).unwrap()), triggers);
            assert_eq!(catalog(list_sequences(&db).unwrap()), sequences);
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
}
