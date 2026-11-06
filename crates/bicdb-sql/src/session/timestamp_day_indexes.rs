use super::*;

fn run(session: &mut SqlSession<'_>, profile: &str, sql: &str) -> Result<SqlResult> {
    let lowered = if profile == oracle::POSTGRES_PROFILE {
        sql.to_string()
    } else {
        oracle::lower_session_sql(session.db_ref(), sql, profile, None, "bicdb")?
    };
    session.execute(&lowered)
}

fn qualify(profile: &str) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    oracle::ensure_database_profile(&mut db, profile).unwrap();
    let expression = if profile == oracle::POSTGRES_PROFILE {
        "date_trunc('day', seen)"
    } else {
        "TRUNC(seen)"
    };
    let mut sql = SqlSession::new(&mut db);
    run(
        &mut sql,
        profile,
        "CREATE TABLE calendar_rows(id INTEGER PRIMARY KEY,seen TIMESTAMP)",
    )
    .unwrap();
    run(&mut sql,profile,"INSERT INTO calendar_rows VALUES(1,TIMESTAMP '1999-12-31 23:59:59.999999'),(2,TIMESTAMP '2000-01-01 12:00:00'),(3,NULL)").unwrap();
    run(
        &mut sql,
        profile,
        "INSERT INTO calendar_rows VALUES(6,TIMESTAMP '2000-01-02 12:00:00')",
    )
    .unwrap();
    // Give the planner a selective predicate: a three-row table can correctly
    // cost a sequential scan below an index probe.
    for day in 0..64 {
        let date =
            chrono::NaiveDate::from_ymd_opt(2002, 1, 1).unwrap() + chrono::Duration::days(day);
        run(
            &mut sql,
            profile,
            &format!(
                "INSERT INTO calendar_rows VALUES({},TIMESTAMP '{date} 12:00:00')",
                100 + day
            ),
        )
        .unwrap();
    }
    run(
        &mut sql,
        profile,
        &format!("CREATE UNIQUE INDEX calendar_day ON calendar_rows({expression})"),
    )
    .unwrap();
    let definition = sql
        .db_ref()
        .index_definitions()
        .into_iter()
        .find(|index| matches!(index.fields.as_slice(), [IndexField::TimestampDay(_)]))
        .expect("physical timestamp-day index required");
    let predicate = format!("{expression}=TIMESTAMP '1999-12-31 00:00:00'");
    let query = format!("SELECT id FROM calendar_rows WHERE {predicate}");
    let lowered = if profile == oracle::POSTGRES_PROFILE {
        query
    } else {
        oracle::lower_session_sql(sql.db_ref(), &query, profile, None, "bicdb").unwrap()
    };
    let explanation = sql.execute(&format!("EXPLAIN {lowered}")).unwrap();
    assert!(
        explanation.rows.iter().flatten().any(|cell| {
            let line = cell.to_cell();
            line.contains("Index") && line.contains(&definition.name)
        }),
        "day equality must use the physical expression index: {:?}",
        explanation.rows
    );
    assert_eq!(
        run(
            &mut sql,
            profile,
            &format!("SELECT id FROM calendar_rows WHERE {predicate}")
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    let range = format!("{expression} >= TIMESTAMP '1999-12-31 00:00:00' AND {expression} < TIMESTAMP '2000-01-02 00:00:00'");
    let range_query = format!("SELECT id FROM calendar_rows WHERE {range}");
    let lowered_range = if profile == oracle::POSTGRES_PROFILE {
        range_query.clone()
    } else {
        oracle::lower_session_sql(sql.db_ref(), &range_query, profile, None, "bicdb").unwrap()
    };
    let range_plan = sql.execute(&format!("EXPLAIN {lowered_range}")).unwrap();
    assert!(
        range_plan.rows.iter().flatten().any(|cell| {
            let line = cell.to_cell();
            line.contains("Index") && line.contains(&definition.name)
        }),
        "day range must use its physical index: {:?}",
        range_plan.rows
    );
    assert_eq!(
        run(&mut sql, profile, &(range_query + " ORDER BY id"))
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
    );

    // Rounding before truncation changes which day the row belongs to. The
    // uncast index must not exclude a row rounded across the epoch boundary.
    let rounded = if profile == oracle::POSTGRES_PROFILE {
        "date_trunc('day',CAST(seen AS TIMESTAMP(0)))"
    } else {
        "TRUNC(CAST(seen AS TIMESTAMP(0)))"
    };
    assert_eq!(run(&mut sql,profile,&format!("SELECT id FROM calendar_rows WHERE {rounded}=TIMESTAMP '2000-01-01 00:00:00' ORDER BY id")).unwrap().rows,
               vec![vec![SqlValue::Int(1)],vec![SqlValue::Int(2)]]);

    assert!(run(
        &mut sql,
        profile,
        "INSERT INTO calendar_rows VALUES(4,TIMESTAMP '1999-12-31 01:00:00')"
    )
    .is_err());
    run(
        &mut sql,
        profile,
        "INSERT INTO calendar_rows VALUES(4,NULL)",
    )
    .unwrap();
    run(&mut sql,profile,"BEGIN; SAVEPOINT before_move; UPDATE calendar_rows SET seen=TIMESTAMP '2001-01-01 12:00:00' WHERE id=1; ROLLBACK TO SAVEPOINT before_move; COMMIT").unwrap();
    assert_eq!(
        run(
            &mut sql,
            profile,
            &format!("SELECT id FROM calendar_rows WHERE {predicate}")
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    run(
        &mut sql,
        profile,
        "UPDATE calendar_rows SET seen=TIMESTAMP '2001-01-01 12:00:00' WHERE id=1",
    )
    .unwrap();
    assert!(run(
        &mut sql,
        profile,
        &format!("SELECT id FROM calendar_rows WHERE {predicate}")
    )
    .unwrap()
    .rows
    .is_empty());
    run(
        &mut sql,
        profile,
        "INSERT INTO calendar_rows VALUES(5,TIMESTAMP '1999-12-31 01:00:00')",
    )
    .unwrap();
    run(&mut sql, profile, "DELETE FROM calendar_rows WHERE id=2").unwrap();
    drop(sql);
    assert!(db.verify_index(&definition.name).unwrap().valid);
    db.flush().unwrap();
    drop(db);
    let mut db = BicDb::open(root.path()).unwrap();
    assert!(db.verify_index(&definition.name).unwrap().valid);
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        run(
            &mut sql,
            profile,
            &format!("SELECT id FROM calendar_rows WHERE {predicate}")
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::Int(5)]]
    );
}

#[test]
fn postgres_day_expression_index_backfill_mutation_savepoint_and_reopen() {
    qualify(oracle::POSTGRES_PROFILE);
}
#[test]
fn oracle_day_expression_index_backfill_mutation_savepoint_and_reopen() {
    qualify(oracle::ORACLE_PREVIEW_PROFILE);
}

#[test]
fn zoned_and_nonconstant_truncation_do_not_get_executable_day_keys() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE zoned_times(id INTEGER PRIMARY KEY,seen TIMESTAMPTZ,unit TEXT)")
        .unwrap();
    assert!(sql
        .execute("CREATE UNIQUE INDEX zoned_day ON zoned_times(date_trunc('day',seen))")
        .is_err());
    assert!(sql
        .execute("CREATE UNIQUE INDEX dynamic_day ON zoned_times(date_trunc(unit,seen))")
        .is_err());
    sql.execute("CREATE TABLE precise_times(seen TIMESTAMP)")
        .unwrap();
    assert!(sql.execute("CREATE UNIQUE INDEX rounded_day ON precise_times(date_trunc('day',CAST(seen AS TIMESTAMP(0))))").is_err());
    assert!(!sql
        .db_ref()
        .index_definitions()
        .iter()
        .any(|index| matches!(index.fields.as_slice(), [IndexField::TimestampDay(_)])));
}
