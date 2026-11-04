//! Row-valued UPDATE sources are evaluated once against the original target row.
use super::*;

pub(super) fn assignment_columns(target: &AssignmentTarget) -> &[ObjectName] {
    match target {
        AssignmentTarget::ColumnName(name) => std::slice::from_ref(name),
        AssignmentTarget::Tuple(names) => names,
    }
}

fn source_expr(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    expr
}

pub(super) fn independent_row_query(assignment: &Assignment) -> Option<&Query> {
    let Expr::Subquery(query) = source_expr(&assignment.value) else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if !select.from.is_empty() || query.with.is_some() {
        return None;
    }
    // Route a flat uncorrelated SELECT through the mutable session, exactly
    // as an ordinary standalone SELECT. Do this before any evaluation: retrying
    // after an unsupported later expression could repeat an earlier side effect.
    use sqlparser::ast::visit_expressions;
    if visit_expressions(query.as_ref(), |expr| {
        if matches!(
            expr,
            Expr::Identifier(_)
                | Expr::CompoundIdentifier(_)
                | Expr::Subquery(_)
                | Expr::Exists { .. }
                | Expr::InSubquery { .. }
        ) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .is_break()
    {
        return None;
    }
    Some(query)
}

fn row_query_value(result: &SqlResult, count: usize) -> Result<SqlValue> {
    if result.columns.len() != count {
        return Err(SqlError::InvalidSql(format!(
            "UPDATE assignment has {count} columns but subquery returns {}",
            result.columns.len()
        )));
    }
    let values = match result.rows.as_slice() {
        [] => vec![SqlValue::Null; count],
        [row] => row.clone(),
        _ => {
            return Err(SqlError::data_exception(
                "21000",
                "UPDATE row subquery returned more than one row",
                None,
            ))
        }
    };
    Ok(anonymous_record_value(values, result.column_types.clone()))
}

pub(super) fn evaluate_row_assignment(
    engine: &SqlEngine<'_>,
    row: &SlotRow,
    context: &BoundRowContext,
    assignment: &Assignment,
) -> Result<SqlValue> {
    let count = assignment_columns(&assignment.target).len();
    if let Expr::Subquery(query) = source_expr(&assignment.value) {
        // A scalar-subquery evaluator would reject multiple columns. Retain
        // the entire row and perform the row cardinality check only once.
        let result = engine.execute_correlated_slot_query(row, context, query)?;
        return row_query_value(&result, count);
    }
    engine.eval_slot_row_value(row, context, &assignment.value)
}

pub(super) fn unpack_row_assignment(value: SqlValue, count: usize) -> Result<Vec<SqlValue>> {
    match value {
        SqlValue::Composite(row) if row.fields.len() == count => {
            Ok(row.fields.into_iter().map(|field| field.value).collect())
        }
        _ => Err(SqlError::InvalidSql(format!(
            "UPDATE row assignment requires a row with {count} fields"
        ))),
    }
}

impl SqlSession<'_> {
    pub(super) fn validate_row_assignments(
        &self,
        update: &sqlparser::ast::Update,
        table: &str,
        schema: Option<&TableSchema>,
    ) -> Result<()> {
        if !update
            .assignments
            .iter()
            .any(|a| matches!(a.target, AssignmentTarget::Tuple(_)))
        {
            return Ok(());
        }
        if let Some(schema) = schema {
            // Reuse the existing column-read proof. Nested query scopes keep
            // its conservative table SELECT requirement until correlated
            // column ACL binding is shared with the planner.
            self.require_key_update_read_privileges(table, schema, update)?;
        }
        let mut assigned = BTreeSet::new();
        for assignment in &update.assignments {
            let tuple = matches!(assignment.target, AssignmentTarget::Tuple(_));
            if tuple {
                match source_expr(&assignment.value) {
                    Expr::Subquery(_) | Expr::Tuple(_) => {}
                    Expr::Function(function) if is_row_constructor(function) => {}
                    _ => {
                        return Err(SqlError::Unsupported(
                            "UPDATE row assignment requires a row constructor or subquery".into(),
                        ))
                    }
                }
            }
            for name in assignment_columns(&assignment.target) {
                let column = relation_name(name)?;
                if !assigned.insert(column.clone()) {
                    return Err(SqlError::InvalidSql(format!(
                        "multiple UPDATE assignments to column \"{column}\""
                    )));
                }
                if let Some(schema) = schema {
                    ensure_schema_column(table, schema, &column)?;
                    if tuple
                        && schema
                            .column(&column)
                            .is_some_and(|c| c.generated_expr.is_some())
                    {
                        return Err(SqlError::Unsupported(
                            "row assignment to a generated column is not supported".into(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn update_row_assignment_value(
        &mut self,
        ctes: &BTreeMap<String, CteResult>,
        columns: &[String],
        row: &SlotRow,
        assignment: &Assignment,
    ) -> Result<Vec<SqlValue>> {
        if let Some(query) = independent_row_query(assignment) {
            let result = self.execute_query(query)?;
            let value = row_query_value(&result, assignment_columns(&assignment.target).len())?;
            return unpack_row_assignment(value, assignment_columns(&assignment.target).len());
        }
        let engine = self.sql_engine_with_ctes(ctes.clone());
        let (_, context) = engine.bound_row_context(columns);
        let value = evaluate_row_assignment(&engine, row, &context, assignment)?;
        unpack_row_assignment(value, assignment_columns(&assignment.target).len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(db: &mut BicDb, sql: &str, oracle: bool) -> Result<SqlResult> {
        let sql = if oracle {
            crate::oracle::lower_session_sql(
                db,
                sql,
                crate::oracle::ORACLE_PREVIEW_PROFILE,
                None,
                "bicdb",
            )?
        } else {
            sql.into()
        };
        SqlSession::new(db).execute(&sql)
    }

    #[test]
    fn row_updates_preserve_query_cardinality_and_old_values() {
        for oracle in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(
                dir.path(),
                bicdb_core::DbConfig::default().with_fsync(false),
            )
            .unwrap();
            if oracle {
                crate::oracle::ensure_database_profile(
                    &mut db,
                    crate::oracle::ORACLE_PREVIEW_PROFILE,
                )
                .unwrap();
            }
            for sql in [
                "CREATE TABLE row_target(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER)",
                "CREATE TABLE row_source(parent INTEGER,x INTEGER,y INTEGER)",
                "INSERT INTO row_target VALUES(1,10,20),(2,30,40),(3,50,60)",
                "INSERT INTO row_source VALUES(1,2,3),(2,4,5),(2,6,7)",
                "UPDATE row_target t SET(a,b)=(SELECT SUM(x),SUM(y) FROM row_source s WHERE s.parent=t.id)",
            ] { run(&mut db,sql,oracle).unwrap(); }
            let rows = run(&mut db, "SELECT a,b FROM row_target ORDER BY id", oracle)
                .unwrap()
                .rows;
            assert_eq!(
                rows,
                vec![
                    vec![SqlValue::Int(2), SqlValue::Int(3)],
                    vec![SqlValue::Int(10), SqlValue::Int(12)],
                    vec![SqlValue::Null, SqlValue::Null]
                ]
            );
            run(
                &mut db,
                "UPDATE row_target SET(a,b)=(b,a) WHERE id=1",
                oracle,
            )
            .unwrap();
            let before = run(&mut db, "SELECT id,a,b FROM row_target ORDER BY id", oracle)
                .unwrap()
                .rows;
            assert_eq!(run(&mut db,"UPDATE row_target t SET(a,b)=(SELECT x,y FROM row_source s WHERE s.parent=t.id)",oracle).unwrap_err().sqlstate(),"21000");
            assert_eq!(
                run(&mut db, "SELECT id,a,b FROM row_target ORDER BY id", oracle)
                    .unwrap()
                    .rows,
                before
            );
            run(&mut db,"UPDATE row_target SET(a,b)=(SELECT x,y FROM row_source WHERE parent=99) WHERE id=1",oracle).unwrap();
            assert_eq!(
                run(&mut db, "SELECT a,b FROM row_target WHERE id=1", oracle)
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Null, SqlValue::Null]]
            );
            assert!(run(
                &mut db,
                "UPDATE row_target SET(a,b)=(SELECT x FROM row_source WHERE parent=1) WHERE id=1",
                oracle
            )
            .is_err());
            run(&mut db,"UPDATE row_target SET(id,a)=(SELECT 4,8 FROM row_source WHERE parent=1) WHERE id=1",oracle).unwrap();
            assert_eq!(
                run(&mut db, "SELECT a FROM row_target WHERE id=4", oracle)
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(8)]]
            );
        }
    }

    #[test]
    fn row_updates_require_every_write_and_source_read_privilege() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE row_acl(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER,secret INTEGER)",
            "INSERT INTO row_acl VALUES(1,10,20,99)",
            "CREATE ROLE row_writer LOGIN",
            "GRANT UPDATE(a,b) ON row_acl TO row_writer",
            "SET ROLE row_writer",
        ] {
            session.execute(sql).unwrap();
        }
        assert_eq!(
            session
                .execute("UPDATE row_acl SET(a,b)=(SELECT secret,7)")
                .unwrap_err()
                .sqlstate(),
            "42501"
        );
        session.execute("RESET ROLE").unwrap();
        session
            .execute("GRANT SELECT ON row_acl TO row_writer")
            .unwrap();
        session.execute("SET ROLE row_writer").unwrap();
        session
            .execute("UPDATE row_acl SET(a,b)=(SELECT b,a FROM row_acl WHERE id=1)")
            .unwrap();
        assert_eq!(
            session
                .execute("UPDATE row_acl SET(a,secret)=(SELECT 7,8)")
                .unwrap_err()
                .sqlstate(),
            "42501"
        );
        session.execute("RESET ROLE").unwrap();
        assert_eq!(
            session
                .execute("SELECT a,b,secret FROM row_acl")
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::Int(20),
                SqlValue::Int(10),
                SqlValue::Int(99)
            ]]
        );
    }

    #[test]
    fn row_updates_infer_each_prepared_parameter_from_its_target() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE row_parameters(id INTEGER PRIMARY KEY,n NUMERIC,label TEXT)")
            .unwrap();
        for source in ["(SELECT $1,$2)", "($1,$2)", "ROW($1,$2)"] {
            let sql = format!("UPDATE row_parameters SET(n,label)={source} WHERE id=$3");
            assert_eq!(
                infer_parameter_types(&db, &sql).unwrap(),
                ["numeric", "text", "int4"].map(|value| Some(value.to_string())),
                "{sql}"
            );
        }
    }

    #[test]
    fn row_updates_keep_distinct_quoted_candidate_columns() {
        for oracle in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(dir.path()).unwrap();
            if oracle {
                crate::oracle::ensure_database_profile(
                    &mut db,
                    crate::oracle::ORACLE_PREVIEW_PROFILE,
                )
                .unwrap();
            }
            for sql in [
                "CREATE TABLE row_quotes(id INTEGER PRIMARY KEY,\"Key\" INTEGER,\"key\" INTEGER)",
                "INSERT INTO row_quotes VALUES(1,10,20)",
            ] {
                run(&mut db, sql, oracle).unwrap();
            }
            let dual = if oracle { " FROM DUAL" } else { "" };
            run(&mut db,&format!("UPDATE row_quotes t SET(\"Key\",\"key\")=(SELECT t.\"key\",t.\"Key\"{dual}) WHERE id=1"),oracle).unwrap();
            assert_eq!(
                run(&mut db, "SELECT \"Key\",\"key\" FROM row_quotes", oracle)
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(20), SqlValue::Int(10)]]
            );
        }
    }

    #[test]
    fn row_updates_run_independent_volatile_sources_in_the_mutable_session() {
        for oracle in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(dir.path()).unwrap();
            if oracle {
                crate::oracle::ensure_database_profile(
                    &mut db,
                    crate::oracle::ORACLE_PREVIEW_PROFILE,
                )
                .unwrap();
            }
            let mut session = SqlSession::new(&mut db);
            let run =
                |session: &mut SqlSession<'_>, sql: &str, oracle: bool| -> Result<SqlResult> {
                    let sql = if oracle {
                        crate::oracle::lower_session_sql(
                            session.db_mut()?,
                            sql,
                            crate::oracle::ORACLE_PREVIEW_PROFILE,
                            None,
                            "bicdb",
                        )?
                    } else {
                        sql.to_string()
                    };
                    session.execute(&sql)
                };
            for sql in [
                "CREATE TABLE row_effects(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER)",
                "INSERT INTO row_effects VALUES(1,10,20)",
                "CREATE SEQUENCE row_counter START WITH 1",
            ] {
                run(&mut session, sql, oracle).unwrap();
            }
            let function = if oracle {
                "CREATE FUNCTION row_tick RETURN NUMBER AS BEGIN RETURN row_counter.NEXTVAL; END;"
            } else {
                "CREATE FUNCTION row_tick() RETURNS BIGINT LANGUAGE sql VOLATILE AS $$SELECT nextval('row_counter')$$"
            };
            run(&mut session, function, oracle).unwrap();
            let dual = if oracle { " FROM DUAL" } else { "" };
            run(
                &mut session,
                &format!("UPDATE row_effects SET(a,b)=(SELECT row_tick(),7{dual}) WHERE id=1"),
                oracle,
            )
            .unwrap();
            assert_eq!(
                run(&mut session, "SELECT a,b FROM row_effects", oracle)
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(1), SqlValue::Int(7)]]
            );
            let current = if oracle {
                "SELECT row_counter.CURRVAL FROM DUAL"
            } else {
                "SELECT currval('row_counter')"
            };
            assert_eq!(
                run(&mut session, current, oracle).unwrap().rows[0][0].to_cell(),
                "1"
            );
            run(
                &mut session,
                &format!("UPDATE row_effects SET(a,b)=(SELECT row_tick(),7{dual}) WHERE id=99"),
                oracle,
            )
            .unwrap();
            assert_eq!(
                run(&mut session, current, oracle).unwrap().rows[0][0].to_cell(),
                "1"
            );
        }
    }

    #[test]
    fn integer_ranges_retain_sql_numeric_bounds() {
        for oracle in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(dir.path()).unwrap();
            if oracle {
                crate::oracle::ensure_database_profile(
                    &mut db,
                    crate::oracle::ORACLE_PREVIEW_PROFILE,
                )
                .unwrap();
            }
            for sql in [
                "CREATE TABLE integer_bounds(id INTEGER PRIMARY KEY,n INTEGER)",
                "INSERT INTO integer_bounds VALUES(-2,-2),(0,0),(2,2),(3,3)",
                "CREATE INDEX integer_bounds_n ON integer_bounds(n)",
            ] {
                run(&mut db, sql, oracle).unwrap();
            }
            for column in ["id", "n"] {
                for (predicate, expected) in [
                    (">0", vec![2, 3]),
                    (">=1", vec![2, 3]),
                    (">1.5", vec![2, 3]),
                    ("<=2.5", vec![-2, 0, 2]),
                    (">9223372036854775808", vec![]),
                    ("<9223372036854775808", vec![-2, 0, 2, 3]),
                ] {
                    let sql = format!(
                        "SELECT id FROM integer_bounds WHERE {column}{predicate} ORDER BY id"
                    );
                    assert_eq!(
                        run(&mut db, &sql, oracle).unwrap().rows,
                        expected
                            .into_iter()
                            .map(|v| vec![SqlValue::Int(v)])
                            .collect::<Vec<_>>(),
                        "{oracle}: {sql}"
                    );
                }
            }
            run(
                &mut db,
                "CREATE TABLE quoted_integer_bounds(\"ID\" INTEGER PRIMARY KEY)",
                oracle,
            )
            .unwrap();
            run(
                &mut db,
                "INSERT INTO quoted_integer_bounds VALUES(0),(2),(3)",
                oracle,
            )
            .unwrap();
            assert_eq!(
                run(
                    &mut db,
                    "SELECT \"ID\" FROM quoted_integer_bounds WHERE \"ID\">0 ORDER BY \"ID\"",
                    oracle
                )
                .unwrap()
                .rows,
                vec![vec![SqlValue::Int(2)], vec![SqlValue::Int(3)]]
            );
        }
    }
}
