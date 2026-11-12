//! Native catalog descriptor controls must not need data execution.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{infer_query_result_columns_for_describe, SqlSession};

#[test]
fn partition_catalog_descriptors_match_native_without_reading_reserved_rows() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE source(item_key INT PRIMARY KEY)")
            .unwrap();
        let owner = db.begin_transaction().unwrap();
        let token = owner
            .reserve_catalog_resources(
                &["source".into()],
                &[("__bicdb_pg_schema".into(), "source".into())],
            )
            .unwrap();
        // Only immutable declared descriptors are requested. Any fallback to
        // catalog data would conflict with this real foreign reservation.
        let native: &[(&str, &[(&str, &str)])] = &[
            (
                "pg_attrdef",
                &[
                    ("oid", "oid"),
                    ("adrelid", "oid"),
                    ("adnum", "int2"),
                    ("adbin", "pg_node_tree"),
                ],
            ),
            (
                "pg_indexes",
                &[
                    ("schemaname", "name"),
                    ("tablename", "name"),
                    ("indexname", "name"),
                    ("tablespace", "name"),
                    ("indexdef", "text"),
                ],
            ),
            (
                "pg_inherits",
                &[
                    ("inhrelid", "oid"),
                    ("inhparent", "oid"),
                    ("inhseqno", "int4"),
                    ("inhdetachpending", "bool"),
                ],
            ),
            (
                "pg_partitioned_table",
                &[
                    ("partrelid", "oid"),
                    ("partstrat", "char"),
                    ("partnatts", "int2"),
                    ("partdefid", "oid"),
                    ("partattrs", "int2vector"),
                    ("partclass", "oidvector"),
                    ("partcollation", "oidvector"),
                    ("partexprs", "pg_node_tree"),
                ],
            ),
            (
                "pg_sequence",
                &[
                    ("seqrelid", "oid"),
                    ("seqtypid", "oid"),
                    ("seqstart", "int8"),
                    ("seqincrement", "int8"),
                    ("seqmax", "int8"),
                    ("seqmin", "int8"),
                    ("seqcache", "int8"),
                    ("seqcycle", "bool"),
                ],
            ),
            (
                "pg_tables",
                &[
                    ("schemaname", "name"),
                    ("tablename", "name"),
                    ("tableowner", "name"),
                    ("tablespace", "name"),
                    ("hasindexes", "bool"),
                    ("hasrules", "bool"),
                    ("hastriggers", "bool"),
                    ("rowsecurity", "bool"),
                ],
            ),
        ];
        for (table, expected) in native {
            for prefix in ["", "pg_catalog."] {
                let query = format!("SELECT * FROM {prefix}{table} WHERE 1=0");
                let actual = infer_query_result_columns_for_describe(&db, &query)
                    .unwrap()
                    .expect("Native descriptor must be available without execution");
                let actual = actual
                    .iter()
                    .map(|(name, kind)| (name.as_str(), kind.as_deref().unwrap()))
                    .collect::<Vec<_>>();
                assert_eq!(actual, *expected, "{query}");
            }
        }
        let actual = infer_query_result_columns_for_describe(&db, "SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='adempiere' AND tablename=$1").unwrap().unwrap();
        assert_eq!(
            actual,
            vec![
                ("indexname".into(), Some("name".into())),
                ("indexdef".into(), Some("text".into()))
            ]
        );
        owner.release_catalog_scope(&token).unwrap();
        owner.rollback().unwrap();
    }
}
