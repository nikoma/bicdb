//! Unique constraint NULL policies, independent of the client wire protocol.
use crate::*;
use bicdb_core::IndexPredicate;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UniqueNullPolicy {
    #[default]
    Distinct,
    NotDistinct,
    /// Equal partially NULL keys conflict, while wholly NULL keys are omitted.
    AllNullsDistinct,
}

impl UniqueNullPolicy {
    pub(crate) fn skips_key(self, values: &[SqlValue]) -> bool {
        match self {
            Self::Distinct => unique_key_has_null(values),
            Self::NotDistinct => false,
            Self::AllNullsDistinct => values.iter().all(|value| matches!(value, SqlValue::Null)),
        }
    }
    pub(crate) fn from_sql(option: sqlparser::ast::NullsDistinctOption, fallback: Self) -> Self {
        match option {
            sqlparser::ast::NullsDistinctOption::None => fallback,
            sqlparser::ast::NullsDistinctOption::Distinct => Self::Distinct,
            sqlparser::ast::NullsDistinctOption::NotDistinct => Self::NotDistinct,
        }
    }
    pub(crate) fn index_options(
        self,
        schema: &TableSchema,
        columns: &[String],
    ) -> (bool, Option<IndexPredicate>) {
        match self {
            Self::Distinct => (false, None),
            Self::NotDistinct => (true, None),
            Self::AllNullsDistinct => {
                let nullable =
                    |name: &String| schema.column(name).is_none_or(|column| column.nullable);
                if columns.len() <= 1 || !columns.iter().any(nullable) {
                    return (false, None);
                }
                if columns.iter().any(|name| !nullable(name)) {
                    return (true, None);
                }
                let predicate = primary_key_index_fields_for_columns(columns)
                    .into_iter()
                    .map(|field| IndexPredicate::Binary {
                        operator: bicdb_core::IndexPredicateBinaryOperator::NotEqual,
                        left: Box::new(IndexPredicate::Field { field }),
                        right: Box::new(IndexPredicate::Literal {
                            value: JsonValue::Null,
                        }),
                        value_type: bicdb_core::IndexPredicateValueType::Json,
                    })
                    .reduce(|left, right| IndexPredicate::Binary {
                        operator: bicdb_core::IndexPredicateBinaryOperator::Or,
                        left: Box::new(left),
                        right: Box::new(right),
                        value_type: bicdb_core::IndexPredicateValueType::Bool,
                    });
                (true, predicate)
            }
        }
    }

    pub(crate) fn information_schema_value(
        self,
        schema: &TableSchema,
        columns: &[String],
    ) -> SqlValue {
        match self {
            Self::Distinct => SqlValue::String("YES".into()),
            Self::NotDistinct => SqlValue::String("NO".into()),
            Self::AllNullsDistinct => match self.index_options(schema, columns) {
                (false, _) => SqlValue::String("YES".into()),
                (true, None) => SqlValue::String("NO".into()),
                // The SQL-standard yes/no field cannot express this hybrid.
                (true, Some(_)) => SqlValue::Null,
            },
        }
    }

    pub(crate) fn index_predicate_sql(
        self,
        schema: &TableSchema,
        columns: &[String],
    ) -> Option<String> {
        self.index_options(schema, columns).1.map(|_| {
            columns
                .iter()
                .map(|column| format!("\"{}\" IS NOT NULL", column.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(" OR ")
        })
    }
}

impl ConstraintSchema {
    pub(crate) fn unique(
        name: String,
        columns: Vec<String>,
        validated: bool,
        null_policy: UniqueNullPolicy,
    ) -> Self {
        if null_policy == UniqueNullPolicy::Distinct {
            Self::Unique {
                name,
                columns,
                validated,
            }
        } else {
            Self::UniqueWithNullPolicy {
                name,
                columns,
                validated,
                null_policy,
            }
        }
    }
    pub(crate) fn unique_null_policy(&self) -> UniqueNullPolicy {
        match self {
            Self::UniqueWithNullPolicy { null_policy, .. } => *null_policy,
            _ => UniqueNullPolicy::Distinct,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_null_policies_follow_the_database_profile_and_survive_reopen() {
        for mode in [
            bicdb_core::StorageMode::EmbeddedMemory,
            bicdb_core::StorageMode::ServerPaged,
        ] {
            for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_PREVIEW_PROFILE] {
                let root = tempfile::tempdir().unwrap();
                let config = bicdb_core::DbConfig::default().with_storage_mode(mode.clone());
                {
                    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
                    oracle::ensure_database_profile(&mut db, profile).unwrap();
                    // A mutable GUC cannot select the database's constraint rules.
                    let mut gucs = HashMap::new();
                    gucs.insert(
                        "bicdb.sql_profile".into(),
                        if profile == oracle::POSTGRES_PROFILE {
                            oracle::ORACLE_PREVIEW_PROFILE
                        } else {
                            oracle::POSTGRES_PROFILE
                        }
                        .into(),
                    );
                    let mut session = SqlSession::new(&mut db).with_session_gucs(gucs);
                    session.execute("CREATE TABLE null_contract(k INT PRIMARY KEY,a NUMERIC(10,2),b TEXT,CONSTRAINT null_contract_key UNIQUE(a,b))").unwrap();
                    session.execute("INSERT INTO null_contract VALUES(1,NULL,NULL),(2,NULL,NULL),(3,1,NULL)").unwrap();
                    let duplicate = session.execute("INSERT INTO null_contract VALUES(4,1,NULL)");
                    assert_eq!(
                        duplicate.is_err(),
                        profile == oracle::ORACLE_PREVIEW_PROFILE,
                        "{profile}: {mode:?}"
                    );
                    session
                        .execute("INSERT INTO null_contract VALUES(5,NULL,'x'),(7,1,'x')")
                        .unwrap();
                    assert!(session
                        .execute("INSERT INTO null_contract VALUES(8,1,'x')")
                        .is_err());
                    let schema = load_schema(session.db_ref(), "null_contract")
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        schema
                            .constraints
                            .iter()
                            .find(|c| constraint_name(c) == "null_contract_key")
                            .unwrap()
                            .unique_null_policy(),
                        if profile == oracle::POSTGRES_PROFILE {
                            UniqueNullPolicy::Distinct
                        } else {
                            UniqueNullPolicy::AllNullsDistinct
                        }
                    );
                    let info=session.execute("SELECT nulls_distinct FROM information_schema.table_constraints WHERE constraint_name='null_contract_key'").unwrap();
                    assert_eq!(
                        info.rows[0][0],
                        if profile == oracle::POSTGRES_PROFILE {
                            SqlValue::String("YES".into())
                        } else {
                            SqlValue::Null
                        }
                    );
                    let index=session.execute("SELECT i.indnullsnotdistinct,i.indpred IS NOT NULL FROM pg_constraint c JOIN pg_index i ON i.indexrelid=c.conindid WHERE c.conname='null_contract_key'").unwrap();
                    assert_eq!(
                        index.rows,
                        vec![vec![
                            SqlValue::Bool(profile == oracle::ORACLE_PREVIEW_PROFILE),
                            SqlValue::Bool(profile == oracle::ORACLE_PREVIEW_PROFILE)
                        ]]
                    );
                }
                let mut db = BicDb::open_with_config(root.path(), config).unwrap();
                let mut session = SqlSession::new(&mut db);
                let duplicate = session.execute("INSERT INTO null_contract VALUES(9,1,NULL)");
                assert_eq!(
                    duplicate.is_err(),
                    profile == oracle::ORACLE_PREVIEW_PROFILE,
                    "reopen: {profile}: {mode:?}"
                );
                session
                    .execute("INSERT INTO null_contract VALUES(10,NULL,NULL)")
                    .unwrap();
            }
        }
    }

    #[test]
    fn explicit_nulls_not_distinct_constraints_cover_updates_conflicts_and_ddl_backfill() {
        for mode in [
            bicdb_core::StorageMode::EmbeddedMemory,
            bicdb_core::StorageMode::ServerPaged,
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(
                root.path(),
                bicdb_core::DbConfig::default().with_storage_mode(mode.clone()),
            )
            .unwrap();
            let mut session = SqlSession::new(&mut db);
            session.execute("CREATE TABLE typed_keys(k INT PRIMARY KEY,a NUMERIC(10,2),b TEXT,note TEXT,CONSTRAINT typed_key UNIQUE NULLS NOT DISTINCT(a,b))").unwrap();
            session
                .execute("INSERT INTO typed_keys VALUES(1,1,NULL,'seed'),(2,1,'','empty')")
                .unwrap();
            assert!(session
                .execute("INSERT INTO typed_keys VALUES(3,1.00,NULL,'duplicate')")
                .is_err());
            session.execute("INSERT INTO typed_keys VALUES(3,1.0,NULL,'changed') ON CONFLICT ON CONSTRAINT typed_key DO UPDATE SET note=excluded.note").unwrap();
            assert_eq!(
                session
                    .execute("SELECT note FROM typed_keys WHERE k=1")
                    .unwrap()
                    .rows[0][0],
                SqlValue::String("changed".into())
            );
            assert!(session
                .execute("UPDATE typed_keys SET b=NULL WHERE k=2")
                .is_err());
            assert_eq!(
                session
                    .execute("SELECT b FROM typed_keys WHERE k=2")
                    .unwrap()
                    .rows[0][0],
                SqlValue::String("".into())
            );
            let rows = session
                .execute(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname='typed_key'",
                )
                .unwrap();
            assert_eq!(
                rows.rows[0][0],
                SqlValue::String("UNIQUE NULLS NOT DISTINCT (a, b)".into())
            );
            session.execute("CREATE TABLE inline_keys(k INT PRIMARY KEY,v TEXT UNIQUE); INSERT INTO inline_keys VALUES(1,'same')").unwrap();
            assert!(session
                .execute("INSERT INTO inline_keys VALUES(2,'same')")
                .is_err());
            session.execute("CREATE TABLE late_keys(k INT PRIMARY KEY,a INT); INSERT INTO late_keys VALUES(1,NULL),(2,NULL)").unwrap();
            assert!(session
                .execute(
                    "ALTER TABLE late_keys ADD CONSTRAINT late_key UNIQUE NULLS NOT DISTINCT(a)"
                )
                .is_err());
            assert_eq!(
                session
                    .execute("SELECT COUNT(*) FROM pg_constraint WHERE conname='late_key'")
                    .unwrap()
                    .rows[0][0],
                SqlValue::Int(0)
            );
            assert!(session
                .execute("ALTER TABLE late_keys ADD COLUMN b TEXT DEFAULT 'same' UNIQUE")
                .is_err());
            assert!(load_schema(session.db_ref(), "late_keys")
                .unwrap()
                .unwrap()
                .column("b")
                .is_none());
            session.execute("DELETE FROM late_keys WHERE k=2; ALTER TABLE late_keys ADD COLUMN b TEXT DEFAULT 'same' UNIQUE").unwrap();
            assert!(session
                .execute("INSERT INTO late_keys(k,a,b) VALUES(2,2,'same')")
                .is_err());
            session.execute("CREATE TABLE index_collision(a INT,b INT); CREATE UNIQUE INDEX collision_key ON index_collision(a,b)").unwrap();
            assert!(session.execute("ALTER TABLE index_collision ADD CONSTRAINT collision_key UNIQUE NULLS NOT DISTINCT(a,b)").is_err(), "a weaker existing index must not become the constraint backing index");
            assert!(load_schema(session.db_ref(), "index_collision")
                .unwrap()
                .unwrap()
                .constraints
                .is_empty());
            assert!(integrity_check(session.db_mut().unwrap()).unwrap().valid);
        }
    }

    #[test]
    fn constraint_catalog_uses_the_owning_application_namespace() {
        for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_PREVIEW_PROFILE] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(root.path()).unwrap();
            oracle::ensure_database_profile(&mut db, profile).unwrap();
            let mut session = SqlSession::new(&mut db);
            session.execute("CREATE SCHEMA app_keys; CREATE TABLE app_keys.parent(k INT PRIMARY KEY,v TEXT UNIQUE,a INT NOT NULL,CONSTRAINT positive CHECK(a>0)); CREATE TABLE app_keys.child(k INT REFERENCES app_keys.parent(k))").unwrap();
            let rows = session.execute("SELECT c.contype,n.nspname,c.connamespace=r.relnamespace FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid JOIN pg_namespace n ON n.oid=c.connamespace WHERE r.relname IN ('parent','child') ORDER BY c.contype").unwrap();
            assert_eq!(
                rows.rows,
                ["c", "f", "n", "n", "p", "u"]
                    .iter()
                    .map(|kind| vec![
                        SqlValue::String((*kind).into()),
                        SqlValue::String("app_keys".into()),
                        SqlValue::Bool(true)
                    ])
                    .collect::<Vec<_>>(),
                "{profile}"
            );
        }
    }

    #[test]
    fn extended_constraint_wire_tags_fail_closed_for_legacy_readers() {
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case")]
        enum Legacy {
            Unique {
                name: String,
                columns: Vec<String>,
                validated: bool,
            },
        }
        let legacy = ConstraintSchema::unique(
            "legacy".into(),
            vec!["v".into()],
            true,
            UniqueNullPolicy::Distinct,
        );
        let value = serde_json::to_value(&legacy).unwrap();
        let Legacy::Unique {
            name,
            columns,
            validated,
        } = serde_json::from_value::<Legacy>(value).unwrap();
        assert_eq!(name, "legacy");
        assert_eq!(columns, vec!["v"]);
        assert!(validated);
        for policy in [
            UniqueNullPolicy::NotDistinct,
            UniqueNullPolicy::AllNullsDistinct,
        ] {
            let constraint =
                ConstraintSchema::unique("extended".into(), vec!["v".into()], true, policy);
            let value = serde_json::to_value(&constraint).unwrap();
            assert!(serde_json::from_value::<Legacy>(value.clone()).is_err());
            assert_eq!(
                serde_json::from_value::<ConstraintSchema>(value)
                    .unwrap()
                    .unique_null_policy(),
                policy
            );
        }
    }
}
