//! Oracle logical partitioning through public SQL APIs: partition DDL and the
//! partition data-dictionary views, independent of application fixtures.
use bicdb_core::BicDb;
use bicdb_sql::oracle::{ensure_database_profile, lower_session_sql, ORACLE_PREVIEW_PROFILE};
use bicdb_sql::{SqlSession, SqlValue};

fn database(profile: &str) -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    ensure_database_profile(&mut db, profile).unwrap();
    (root, db)
}

fn run(db: &mut BicDb, sql: &str) -> bicdb_sql::Result<Vec<Vec<SqlValue>>> {
    let lowered = lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, Some("\"APP\""), "app")?;
    SqlSession::new(db)
        .execute(&lowered)
        .map(|result| result.rows)
}

fn texts(rows: Vec<Vec<SqlValue>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    SqlValue::Null => "<null>".to_string(),
                    other => other.to_cell(),
                })
                .collect()
        })
        .collect()
}

#[test]
fn automatic_list_partitioning_is_described_by_the_partition_dictionary() {
    let (_root, mut db) = database(ORACLE_PREVIEW_PROFILE);
    SqlSession::new(&mut db)
        .execute("CREATE SCHEMA \"APP\"")
        .unwrap();
    run(
        &mut db,
        "CREATE TABLE T_P (AD_Client_ID NUMBER(10), AD_Org_ID NUMBER(10), Name VARCHAR2(20))",
    )
    .unwrap();
    run(&mut db, "CREATE INDEX T_P_Name ON T_P (Name)").unwrap();
    assert!(run(
        &mut db,
        "SELECT 1 FROM USER_TAB_PARTITIONS WHERE TABLE_NAME = 'T_P'"
    )
    .unwrap()
    .is_empty());
    assert!(run(
        &mut db,
        "SELECT AutoList FROM User_Part_Tables WHERE Table_Name = 'T_P'"
    )
    .unwrap()
    .is_empty());

    run(
        &mut db,
        "ALTER TABLE T_P MODIFY PARTITION BY List (AD_Client_ID) AUTOMATIC SUBPARTITION BY List (AD_Org_ID) (PARTITION default_partition VALUES (NULL) ) ",
    )
    .unwrap();
    assert_eq!(
        texts(run(&mut db, "SELECT Partitioning_Type, Subpartitioning_Type, AutoList, Partition_Count FROM User_Part_Tables WHERE Table_Name = 'T_P'").unwrap()),
        vec![vec!["LIST", "LIST", "YES", "1"]]
    );
    assert_eq!(
        texts(run(&mut db, "SELECT Column_Name FROM User_Part_Key_Columns WHERE Name = 'T_P' ORDER BY Column_Position").unwrap()),
        vec![vec!["AD_CLIENT_ID"]]
    );
    assert_eq!(
        texts(
            run(
                &mut db,
                "SELECT Column_Name FROM User_Subpart_Key_Columns WHERE Name = 'T_P'"
            )
            .unwrap()
        ),
        vec![vec!["AD_ORG_ID"]]
    );
    assert_eq!(
        texts(run(&mut db, "SELECT Partition_Name, High_Value FROM User_Tab_Partitions WHERE Table_Name = 'T_P'").unwrap()),
        vec![vec!["DEFAULT_PARTITION", "NULL"]]
    );
    let default_sub = texts(run(&mut db, "SELECT High_Value, SubPartition_Name FROM User_Tab_SubPartitions WHERE Table_Name = 'T_P' AND Partition_Name = 'DEFAULT_PARTITION'").unwrap());
    assert_eq!(default_sub.len(), 1);
    assert_eq!(default_sub[0][0], "DEFAULT");
    assert!(default_sub[0][1].starts_with("SYS_SUBP"));

    // Automatic partitions appear for the key values rows bring, with stable names.
    run(&mut db, "INSERT INTO T_P VALUES (11, 0, 'a')").unwrap();
    run(&mut db, "INSERT INTO T_P VALUES (12, 0, 'b')").unwrap();
    run(&mut db, "INSERT INTO T_P VALUES (11, 50000, 'c')").unwrap();
    let partitions = texts(run(&mut db, "SELECT Partition_Name, High_Value FROM User_Tab_Partitions WHERE Table_Name = 'T_P' ORDER BY Partition_Position").unwrap());
    assert_eq!(partitions.len(), 3, "{partitions:?}");
    assert_eq!(partitions[1][1], "11");
    assert_eq!(partitions[2][1], "12");
    assert!(partitions[1][0].starts_with("SYS_P"));
    assert_eq!(
        partitions,
        texts(run(&mut db, "SELECT Partition_Name, High_Value FROM User_Tab_Partitions WHERE Table_Name = 'T_P' ORDER BY Partition_Position").unwrap())
    );
    assert_eq!(
        texts(
            run(
                &mut db,
                "SELECT Index_Name, Status, Partitioned FROM User_Indexes WHERE Table_Name = 'T_P'"
            )
            .unwrap()
        ),
        vec![vec!["T_P_NAME", "VALID", "YES"]]
    );
    assert!(run(
        &mut db,
        "SELECT index_name FROM user_ind_partitions WHERE status = 'UNUSABLE'"
    )
    .unwrap()
    .is_empty());
    // ALL_ views carry the owner.
    assert_eq!(
        texts(run(&mut db, "SELECT Owner, Table_Name FROM All_Part_Tables").unwrap()),
        vec![vec!["APP", "T_P"]]
    );
    // Rows themselves are unaffected by partitioning.
    assert_eq!(
        texts(run(&mut db, "SELECT COUNT(*) FROM T_P WHERE AD_Client_ID = 11").unwrap()),
        vec![vec!["2"]]
    );
}

#[test]
fn interval_range_partitioning_and_unsupported_maintenance() {
    let (_root, mut db) = database(ORACLE_PREVIEW_PROFILE);
    SqlSession::new(&mut db)
        .execute("CREATE SCHEMA \"APP\"")
        .unwrap();
    run(&mut db, "CREATE TABLE T_R (Created DATE, Amt NUMBER)").unwrap();
    run(
        &mut db,
        "ALTER TABLE T_R MODIFY PARTITION BY RANGE (Created) INTERVAL(NUMTOYMINTERVAL(1,'MONTH')) (PARTITION default_partition VALUES LESS THAN (TO_DATE('1970-01-01','YYYY-MM-DD')))",
    )
    .unwrap();
    assert_eq!(
        texts(run(&mut db, "SELECT Partitioning_Type, AutoList, Interval FROM User_Part_Tables WHERE Table_Name = 'T_R'").unwrap()),
        vec![vec!["RANGE", "NO", "NUMTOYMINTERVAL(1,'MONTH')"]]
    );
    assert_eq!(
        texts(
            run(
                &mut db,
                "SELECT High_Value FROM User_Tab_Partitions WHERE Table_Name = 'T_R'"
            )
            .unwrap()
        ),
        vec![vec!["TO_DATE('1970-01-01','YYYY-MM-DD')"]]
    );
    let error = run(
        &mut db,
        "ALTER TABLE T_R SPLIT PARTITION default_partition AT (TO_DATE('2000-01-01','YYYY-MM-DD'))",
    )
    .unwrap_err();
    assert!(error.to_string().contains("ORA-14006"), "{error}");
    let error = run(&mut db, "ALTER TABLE T_R MODIFY PARTITION BY RANGE (Missing) (PARTITION p VALUES LESS THAN (MAXVALUE))").unwrap_err();
    assert!(error.to_string().contains("ORA-00904"), "{error}");
    let error = run(&mut db, "ALTER TABLE T_R MODIFY PARTITION BY RANGE (Amt) AUTOMATIC (PARTITION p VALUES LESS THAN (MAXVALUE))").unwrap_err();
    assert!(error.to_string().contains("ORA-14751"), "{error}");
}

#[test]
fn partition_metadata_option_is_not_a_native_table_option() {
    let (_root, mut db) = database("postgres");
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE t (id INTEGER)").unwrap();
    assert!(session
        .execute("ALTER TABLE t SET (bicdb_oracle_partitioning = '{\"method\":\"LIST\",\"columns\":[\"id\"]}')")
        .is_err());
    // A same-named table in the application schema shadows the dictionary view.
    let (_root, mut db) = database(ORACLE_PREVIEW_PROFILE);
    SqlSession::new(&mut db)
        .execute("CREATE SCHEMA \"APP\"")
        .unwrap();
    run(
        &mut db,
        "CREATE TABLE USER_INDEXES (Index_Name VARCHAR2(30))",
    )
    .unwrap();
    run(&mut db, "INSERT INTO USER_INDEXES VALUES ('mine')").unwrap();
    assert_eq!(
        texts(run(&mut db, "SELECT Index_Name FROM USER_INDEXES").unwrap()),
        vec![vec!["mine"]]
    );
}

#[test]
fn interval_stays_a_literal_keyword_and_column_modify_is_not_partition_ddl() {
    let (_root, mut db) = database(ORACLE_PREVIEW_PROFILE);
    SqlSession::new(&mut db)
        .execute("CREATE SCHEMA \"APP\"")
        .unwrap();
    run(
        &mut db,
        "CREATE TABLE T_I (Interval NUMBER(10), Name VARCHAR2(10))",
    )
    .unwrap();
    run(&mut db, "ALTER TABLE T_I MODIFY (Name VARCHAR2(40))").unwrap();
    run(
        &mut db,
        "INSERT INTO T_I (Interval, Name) VALUES (7, 'seven')",
    )
    .unwrap();
    assert_eq!(
        texts(
            run(
                &mut db,
                "SELECT t.Interval, Interval + 1 FROM T_I t WHERE Interval = 7"
            )
            .unwrap()
        ),
        vec![vec!["7", "8"]]
    );
    assert_eq!(
        texts(
            run(
                &mut db,
                "SELECT EXTRACT(DAY FROM INTERVAL '3' DAY) FROM DUAL"
            )
            .unwrap()
        ),
        vec![vec!["3"]]
    );
}
