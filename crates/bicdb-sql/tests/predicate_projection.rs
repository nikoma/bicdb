use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{oracle, SqlSession, SqlValue};

fn check(profile: &str) {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    oracle::ensure_database_profile(&mut db, profile).unwrap();
    let mut execute = |sql: &str| {
        let sql = if profile == oracle::POSTGRES_PROFILE {
            sql.into()
        } else {
            oracle::lower_session_sql(&db, sql, profile, None, "bicdb").unwrap()
        };
        SqlSession::new(&mut db).execute(&sql).unwrap()
    };
    execute("CREATE TABLE predicate_values(id INTEGER PRIMARY KEY,n INTEGER,v BOOLEAN)");
    execute("INSERT INTO predicate_values VALUES(1,7,TRUE)");
    execute("INSERT INTO predicate_values VALUES(2,NULL,FALSE)");
    execute("INSERT INTO predicate_values VALUES(3,NULL,NULL)");
    let expected = vec![
        vec![false, true, true, false, false, true, false, true],
        vec![true, false, false, true, true, false, false, true],
        vec![true, false, false, true, false, true, true, false],
    ];
    for projection in [
        "n IS NULL,n IS NOT NULL,v IS TRUE,v IS NOT TRUE,v IS FALSE,v IS NOT FALSE,v IS UNKNOWN,v IS NOT UNKNOWN",
        "(n IS NULL) AS missing,(n IS NOT NULL),((v IS TRUE)),v IS NOT TRUE,v IS FALSE,v IS NOT FALSE,v IS UNKNOWN,v IS NOT UNKNOWN",
    ] {
        // Oracle expresses the unknown boolean state using its NULL predicate.
        let projection = if profile == oracle::POSTGRES_PROFILE { projection.to_owned() }
            else { projection.replace("IS NOT UNKNOWN", "IS NOT NULL").replace("IS UNKNOWN", "IS NULL") };
        let result=execute(&format!("SELECT {projection} FROM predicate_values ORDER BY id"));
        assert_eq!(result.rows,expected.iter().map(|row|row.iter().map(|value|SqlValue::Bool(*value)).collect::<Vec<_>>()).collect::<Vec<_>>(),"{profile}: {projection}");
    }
    if profile == oracle::POSTGRES_PROFILE {
        assert_eq!(
            execute("SELECT id FROM predicate_values ORDER BY n IS NULL DESC,id").rows,
            vec![
                vec![SqlValue::Int(2)],
                vec![SqlValue::Int(3)],
                vec![SqlValue::Int(1)]
            ]
        );
    }
    assert_eq!(
        execute("SELECT id FROM predicate_values WHERE n IS NULL ORDER BY id").rows,
        vec![vec![SqlValue::Int(2)], vec![SqlValue::Int(3)]]
    );
}

#[test]
fn postgres_predicate_projections_keep_three_valued_logic() {
    check(oracle::POSTGRES_PROFILE);
}
#[test]
fn oracle_predicate_projections_keep_three_valued_logic() {
    check(oracle::ORACLE_PREVIEW_PROFILE);
}
