use super::*;

fn execute(session: &mut SqlSession<'_>, profile: &str, source: &str) -> Result<SqlResult> {
    let lowered = if profile == POSTGRES_PROFILE {
        source.to_string()
    } else {
        lower_session_sql(session.db_ref(), source, profile, None, "bicdb")?
    };
    session.execute(&lowered)
}

#[test]
fn driver_release_savepoint_preserves_writes_and_outer_rollback_in_each_profile() {
    for profile in [POSTGRES_PROFILE, ORACLE_HR_PROFILE, ORACLE_PREVIEW_PROFILE] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, profile).unwrap();
        let mut sql = SqlSession::new(&mut db);
        execute(&mut sql, profile, "CREATE TABLE driver_rows(id INTEGER)").unwrap();
        for statement in [
            "BEGIN",
            r#"SAVEPOINT "JDBC_Mixed""#,
            "INSERT INTO driver_rows VALUES(1)",
            r#"RELEASE SAVEPOINT "JDBC_Mixed""#,
            "INSERT INTO driver_rows VALUES(2)",
            "COMMIT",
        ] {
            execute(&mut sql, profile, statement).unwrap();
        }
        assert_eq!(
            execute(&mut sql, profile, "SELECT id FROM driver_rows ORDER BY id")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
        );
        for statement in [
            "BEGIN",
            r#"SAVEPOINT "JDBC_rollback""#,
            "INSERT INTO driver_rows VALUES(3)",
            r#"ROLLBACK TO SAVEPOINT "JDBC_rollback""#,
            r#"RELEASE SAVEPOINT "JDBC_rollback""#,
            "COMMIT",
        ] {
            execute(&mut sql, profile, statement).unwrap();
        }
        assert_eq!(
            execute(&mut sql, profile, "SELECT id FROM driver_rows ORDER BY id")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
        );
        // Application-authored unquoted names must resolve consistently across
        // SAVEPOINT, ROLLBACK TO and the PostgreSQL-driver RELEASE extension.
        for statement in [
            "BEGIN",
            "SAVEPOINT App_Mixed",
            "INSERT INTO driver_rows VALUES(30)",
            "ROLLBACK TO SAVEPOINT app_mixed",
            "RELEASE SAVEPOINT APP_MIXED",
            "SAVEPOINT app_released",
            "INSERT INTO driver_rows VALUES(31)",
            "RELEASE SAVEPOINT App_Released",
            "ROLLBACK",
        ] {
            execute(&mut sql, profile, statement).unwrap();
        }
        for statement in [
            "BEGIN",
            r#"SAVEPOINT "JDBC_outer""#,
            "INSERT INTO driver_rows VALUES(4)",
            r#"RELEASE SAVEPOINT "JDBC_outer""#,
            "ROLLBACK",
        ] {
            execute(&mut sql, profile, statement).unwrap();
        }
        assert_eq!(
            execute(&mut sql, profile, "SELECT COUNT(*) FROM driver_rows")
                .unwrap()
                .rows[0][0],
            SqlValue::Int(2)
        );
    }
}

#[test]
fn transport_release_preserves_escaped_names_without_reserving_column_identifiers() {
    for profile in [ORACLE_HR_PROFILE, ORACLE_PREVIEW_PROFILE] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, profile).unwrap();
        let mut sql = SqlSession::new(&mut db);
        for statement in [
            "BEGIN",
            r#"SAVEPOINT "JDBC_""point""#,
            r#"RELEASE SAVEPOINT "JDBC_""point""#,
            "ROLLBACK",
        ] {
            execute(&mut sql, profile, statement).unwrap();
        }
        execute(
            &mut sql,
            profile,
            "CREATE TABLE transport_words(release INTEGER)",
        )
        .unwrap();
        execute(
            &mut sql,
            profile,
            "INSERT INTO transport_words(release) VALUES(9)",
        )
        .unwrap();
        assert_eq!(
            execute(&mut sql, profile, "SELECT release FROM transport_words")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(9)]]
        );
        assert!(
            lower_session_sql(sql.db_ref(), "RELEASE SAVEPOINT", profile, None, "bicdb").is_err()
        );
        assert!(lower_session_sql(
            sql.db_ref(),
            "RELEASE SAVEPOINT x extra",
            profile,
            None,
            "bicdb"
        )
        .is_err());
    }
}
