//! Fixed Oracle 23 dictionary type descriptors for the AL32UTF8 preview.
//! This registry supplies type anchors; it does not provide dictionary rows.
use super::*;

const USER_TABLES: &[(&str, &str, u32)] = &[
    ("TABLE_NAME", "VARCHAR2", 128),
    ("TABLESPACE_NAME", "VARCHAR2", 30),
    ("CLUSTER_NAME", "VARCHAR2", 128),
    ("IOT_NAME", "VARCHAR2", 128),
    ("STATUS", "VARCHAR2", 8),
    ("PCT_FREE", "NUMBER", 22),
    ("PCT_USED", "NUMBER", 22),
    ("INI_TRANS", "NUMBER", 22),
    ("MAX_TRANS", "NUMBER", 22),
    ("INITIAL_EXTENT", "NUMBER", 22),
    ("NEXT_EXTENT", "NUMBER", 22),
    ("MIN_EXTENTS", "NUMBER", 22),
    ("MAX_EXTENTS", "NUMBER", 22),
    ("PCT_INCREASE", "NUMBER", 22),
    ("FREELISTS", "NUMBER", 22),
    ("FREELIST_GROUPS", "NUMBER", 22),
    ("LOGGING", "VARCHAR2", 3),
    ("BACKED_UP", "VARCHAR2", 1),
    ("NUM_ROWS", "NUMBER", 22),
    ("BLOCKS", "NUMBER", 22),
    ("EMPTY_BLOCKS", "NUMBER", 22),
    ("AVG_SPACE", "NUMBER", 22),
    ("CHAIN_CNT", "NUMBER", 22),
    ("AVG_ROW_LEN", "NUMBER", 22),
    ("AVG_SPACE_FREELIST_BLOCKS", "NUMBER", 22),
    ("NUM_FREELIST_BLOCKS", "NUMBER", 22),
    ("DEGREE", "VARCHAR2", 10),
    ("INSTANCES", "VARCHAR2", 10),
    ("CACHE", "VARCHAR2", 5),
    ("TABLE_LOCK", "VARCHAR2", 8),
    ("SAMPLE_SIZE", "NUMBER", 22),
    ("LAST_ANALYZED", "DATE", 7),
    ("PARTITIONED", "VARCHAR2", 3),
    ("IOT_TYPE", "VARCHAR2", 12),
    ("TEMPORARY", "VARCHAR2", 1),
    ("SECONDARY", "VARCHAR2", 1),
    ("NESTED", "VARCHAR2", 3),
    ("BUFFER_POOL", "VARCHAR2", 7),
    ("FLASH_CACHE", "VARCHAR2", 7),
    ("CELL_FLASH_CACHE", "VARCHAR2", 7),
    ("ROW_MOVEMENT", "VARCHAR2", 8),
    ("GLOBAL_STATS", "VARCHAR2", 3),
    ("USER_STATS", "VARCHAR2", 3),
    ("DURATION", "VARCHAR2", 15),
    ("SKIP_CORRUPT", "VARCHAR2", 8),
    ("MONITORING", "VARCHAR2", 3),
    ("CLUSTER_OWNER", "VARCHAR2", 128),
    ("DEPENDENCIES", "VARCHAR2", 8),
    ("COMPRESSION", "VARCHAR2", 8),
    ("COMPRESS_FOR", "VARCHAR2", 30),
    ("DROPPED", "VARCHAR2", 3),
    ("READ_ONLY", "VARCHAR2", 3),
    ("SEGMENT_CREATED", "VARCHAR2", 3),
    ("RESULT_CACHE", "VARCHAR2", 7),
    ("CLUSTERING", "VARCHAR2", 3),
    ("ACTIVITY_TRACKING", "VARCHAR2", 23),
    ("DML_TIMESTAMP", "VARCHAR2", 25),
    ("HAS_IDENTITY", "VARCHAR2", 3),
    ("CONTAINER_DATA", "VARCHAR2", 3),
    ("INMEMORY", "VARCHAR2", 8),
    ("INMEMORY_PRIORITY", "VARCHAR2", 8),
    ("INMEMORY_DISTRIBUTE", "VARCHAR2", 15),
    ("INMEMORY_COMPRESSION", "VARCHAR2", 17),
    ("INMEMORY_DUPLICATE", "VARCHAR2", 13),
    ("DEFAULT_COLLATION", "VARCHAR2", 100),
    ("DUPLICATED", "VARCHAR2", 1),
    ("SYNCHRONOUS_DUPLICATED", "VARCHAR2", 1),
    ("SHARDED", "VARCHAR2", 1),
    ("EXTERNALLY_SHARDED", "VARCHAR2", 1),
    ("EXTERNALLY_DUPLICATED", "VARCHAR2", 1),
    ("EXTERNAL", "VARCHAR2", 3),
    ("HYBRID", "VARCHAR2", 3),
    ("CELLMEMORY", "VARCHAR2", 24),
    ("CONTAINERS_DEFAULT", "VARCHAR2", 3),
    ("CONTAINER_MAP", "VARCHAR2", 3),
    ("EXTENDED_DATA_LINK", "VARCHAR2", 3),
    ("EXTENDED_DATA_LINK_MAP", "VARCHAR2", 3),
    ("INMEMORY_SERVICE", "VARCHAR2", 12),
    ("INMEMORY_SERVICE_NAME", "VARCHAR2", 1000),
    ("CONTAINER_MAP_OBJECT", "VARCHAR2", 3),
    ("MEMOPTIMIZE_READ", "VARCHAR2", 8),
    ("MEMOPTIMIZE_WRITE", "VARCHAR2", 8),
    ("HAS_SENSITIVE_COLUMN", "VARCHAR2", 3),
    ("ADMIT_NULL", "VARCHAR2", 3),
    ("DATA_LINK_DML_ENABLED", "VARCHAR2", 3),
    ("LOGICAL_REPLICATION", "VARCHAR2", 8),
    ("STAGING", "VARCHAR2", 3),
    ("ROW_CHANGE_TRACKING", "VARCHAR2", 3),
    ("HAS_RESERVABLE_COLUMN", "VARCHAR2", 3),
    ("VECTOR_INDEX_TYPE", "VARCHAR2", 28),
];

const USER_TAB_COLUMNS: &[(&str, &str, u32)] = &[
    ("TABLE_NAME", "VARCHAR2", 128),
    ("COLUMN_NAME", "VARCHAR2", 128),
    ("DATA_TYPE", "VARCHAR2", 128),
    ("DATA_TYPE_MOD", "VARCHAR2", 3),
    ("DATA_TYPE_OWNER", "VARCHAR2", 128),
    ("DATA_LENGTH", "NUMBER", 22),
    ("DATA_PRECISION", "NUMBER", 22),
    ("DATA_SCALE", "NUMBER", 22),
    ("NULLABLE", "VARCHAR2", 1),
    ("COLUMN_ID", "NUMBER", 22),
    ("DEFAULT_LENGTH", "NUMBER", 22),
    ("DATA_DEFAULT", "LONG", 0),
    ("DATA_DEFAULT_VC", "VARCHAR2", 4000),
    ("NUM_DISTINCT", "NUMBER", 22),
    ("LOW_VALUE", "RAW", 1000),
    ("HIGH_VALUE", "RAW", 1000),
    ("DENSITY", "NUMBER", 22),
    ("NUM_NULLS", "NUMBER", 22),
    ("NUM_BUCKETS", "NUMBER", 22),
    ("LAST_ANALYZED", "DATE", 7),
    ("SAMPLE_SIZE", "NUMBER", 22),
    ("CHARACTER_SET_NAME", "VARCHAR2", 44),
    ("CHAR_COL_DECL_LENGTH", "NUMBER", 22),
    ("GLOBAL_STATS", "VARCHAR2", 3),
    ("USER_STATS", "VARCHAR2", 3),
    ("AVG_COL_LEN", "NUMBER", 22),
    ("CHAR_LENGTH", "NUMBER", 22),
    ("CHAR_USED", "VARCHAR2", 1),
    ("V80_FMT_IMAGE", "VARCHAR2", 3),
    ("DATA_UPGRADED", "VARCHAR2", 3),
    ("HISTOGRAM", "VARCHAR2", 15),
    ("DEFAULT_ON_NULL", "VARCHAR2", 3),
    ("DEFAULT_ON_NULL_UPD", "VARCHAR2", 3),
    ("RESERVABLE_COLUMN", "VARCHAR2", 3),
    ("IDENTITY_COLUMN", "VARCHAR2", 3),
    ("EVALUATION_EDITION", "VARCHAR2", 128),
    ("UNUSABLE_BEFORE", "VARCHAR2", 128),
    ("UNUSABLE_BEGINNING", "VARCHAR2", 128),
    ("COLLATION", "VARCHAR2", 100),
    ("DOMAIN_OWNER", "VARCHAR2", 128),
    ("DOMAIN_NAME", "VARCHAR2", 128),
    ("DOMAIN_COLUMN_NAME", "VARCHAR2", 128),
    ("DOMAIN_ASSOCIATION_ID", "NUMBER", 22),
    ("JSON_MODIFIER", "VARCHAR2", 448),
    ("VECTOR_INFO", "VARCHAR2", 68),
];
pub(super) fn anchor_type(view: &str, column: &str, preserve_date: bool) -> Result<Option<String>> {
    let descriptors = match view {
        "USER_TABLES" => USER_TABLES,
        "USER_TAB_COLUMNS" => USER_TAB_COLUMNS,
        _ => return Ok(None),
    };
    let descriptor = descriptors
        .iter()
        .find(|d| d.0 == column)
        .ok_or_else(|| unsupported(format!("unknown dictionary %TYPE column {view}.{column}")))?;
    Ok(Some(match descriptor.1 {
        "VARCHAR2" => format!("VARCHAR({} OCTETS)", descriptor.2),
        "NUMBER" => "NUMERIC".into(),
        "DATE" => if preserve_date {
            "DATE"
        } else {
            "TIMESTAMP(0)"
        }
        .into(),
        kind => {
            return Err(unsupported(format!(
                "dictionary %TYPE source type {kind} requires separate implementation"
            )))
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(db: &mut BicDb, sql: &str) -> Result<SqlResult> {
        let sql = lower_profile_sql(db, sql, ORACLE_PREVIEW_PROFILE)?;
        SqlSession::new(db).execute(&sql)
    }

    fn database(path: &std::path::Path) -> BicDb {
        let mut db = BicDb::open(path).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db, "CREATE SCHEMA red").unwrap();
        db
    }

    #[test]
    fn public_dictionary_anchors_keep_byte_capacity_and_do_not_inherit_not_null() {
        let root = tempfile::tempdir().unwrap();
        let mut db = database(root.path());
        for (name, anchor) in [
            ("data_type", "USER_TAB_COLUMNS.DATA_TYPE"),
            ("table_name", "SYS.USER_TABLES.TABLE_NAME"),
        ] {
            run(&mut db, &format!("CREATE FUNCTION red.{name}(p VARCHAR2) RETURN VARCHAR2 IS v {anchor}%TYPE;BEGIN v:=p;RETURN v;END;")).unwrap();
        }
        let fitting = "😀".repeat(32);
        assert_eq!(
            run(
                &mut db,
                &format!("SELECT red.data_type('{fitting}'),red.table_name(NULL) FROM DUAL")
            )
            .unwrap()
            .rows[0],
            vec![SqlValue::String(fitting), SqlValue::Null]
        );
        let oversized = "😀".repeat(33);
        assert_eq!(
            run(
                &mut db,
                &format!("SELECT red.data_type('{oversized}') FROM DUAL")
            )
            .unwrap_err()
            .sqlstate(),
            "22001"
        );
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        assert_eq!(
            run(&mut db, "SELECT red.data_type('VARCHAR2') FROM DUAL")
                .unwrap()
                .rows[0][0],
            SqlValue::String("VARCHAR2".into())
        );
    }

    #[test]
    fn local_relations_shadow_public_dictionary_synonyms_without_qualified_fallback() {
        let root = tempfile::tempdir().unwrap();
        let mut db = database(root.path());
        for anchor in [
            "red.USER_TAB_COLUMNS.DATA_TYPE",
            "\"user_tab_columns\".DATA_TYPE",
            "USER_TAB_COLUMNS.NOPE",
            "USER_TAB_COLUMNS.DATA_DEFAULT",
            "USER_TAB_COLUMNS.LOW_VALUE",
        ] {
            assert!(lower_profile_sql(&db,&format!("CREATE FUNCTION red.invalid_anchor(p VARCHAR2) RETURN VARCHAR2 IS v {anchor}%TYPE;BEGIN v:=p;RETURN v;END;"),ORACLE_PREVIEW_PROFILE).is_err(),"{anchor}");
        }
        run(
            &mut db,
            "CREATE TABLE red.USER_TAB_COLUMNS(DATA_TYPE NUMBER(5,2))",
        )
        .unwrap();
        for (name, anchor) in [
            ("local_anchor", "USER_TAB_COLUMNS.DATA_TYPE"),
            ("qualified_anchor", "red.USER_TAB_COLUMNS.DATA_TYPE"),
            ("system_anchor", "SYS.USER_TAB_COLUMNS.DATA_TYPE"),
        ] {
            run(&mut db, &format!("CREATE FUNCTION red.{name}(p VARCHAR2) RETURN VARCHAR2 IS v {anchor}%TYPE;BEGIN v:=p;RETURN v;END;")).unwrap();
        }
        assert_eq!(run(&mut db,"SELECT red.local_anchor('12.345'),red.qualified_anchor('12.345'),red.system_anchor('12.345') FROM DUAL").unwrap().rows[0],vec![SqlValue::String("12.35".into()),SqlValue::String("12.35".into()),SqlValue::String("12.345".into())]);
        assert!(lower_profile_sql(&db,"CREATE FUNCTION red.bad_column RETURN VARCHAR2 IS v USER_TAB_COLUMNS.NOPE%TYPE;BEGIN RETURN v;END;",ORACLE_PREVIEW_PROFILE).is_err());
        run(&mut db, "DROP TABLE red.USER_TAB_COLUMNS").unwrap();
        run(
            &mut db,
            "CREATE TABLE red.dictionary_source(b VARCHAR2(7 BYTE),c VARCHAR2(2 CHAR))",
        )
        .unwrap();
        run(
            &mut db,
            "CREATE VIEW red.USER_TAB_COLUMNS AS SELECT b AS DATA_TYPE FROM red.dictionary_source",
        )
        .unwrap();
        run(&mut db,"CREATE FUNCTION red.view_anchor(p VARCHAR2) RETURN VARCHAR2 IS v USER_TAB_COLUMNS.DATA_TYPE%TYPE;BEGIN v:=p;RETURN v;END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT red.view_anchor('😀😀') FROM DUAL")
                .unwrap_err()
                .sqlstate(),
            "22001"
        );
        run(
            &mut db,
            "CREATE VIEW red.USER_TABLES AS SELECT c AS TABLE_NAME FROM red.dictionary_source",
        )
        .unwrap();
        run(&mut db,"CREATE FUNCTION red.char_view_anchor(p VARCHAR2) RETURN VARCHAR2 IS v USER_TABLES.TABLE_NAME%TYPE;BEGIN v:=p;RETURN v;END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT red.char_view_anchor('😀😀') FROM DUAL")
                .unwrap()
                .rows[0][0],
            SqlValue::String("😀😀".into())
        );
    }

    #[test]
    fn dictionary_date_anchors_keep_logical_date_assignment() {
        let root = tempfile::tempdir().unwrap();
        let mut db = database(root.path());
        run(&mut db,"CREATE FUNCTION red.date_anchor(p TIMESTAMP) RETURN TIMESTAMP IS v USER_TABLES.LAST_ANALYZED%TYPE;BEGIN v:=p;RETURN v;END;").unwrap();
        assert_eq!(run(&mut db,"SELECT EXTRACT(SECOND FROM red.date_anchor(TIMESTAMP '2024-02-29 23:59:58.987654'))=58 FROM DUAL").unwrap().rows[0][0],SqlValue::Bool(true));
    }
}
