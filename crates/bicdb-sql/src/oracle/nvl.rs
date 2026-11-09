//! Type-directed eager NVL, independent of COALESCE evaluation.
use super::routine_arithmetic::Kind;
use super::*;

fn type_name(kind: Kind) -> Result<&'static str> {
    match kind {
        Kind::Text | Kind::NationalText | Kind::FixedNationalText | Kind::Null => Ok("text"),
        Kind::Number => Ok("number"),
        Kind::Date => Ok("date"),
        Kind::Timestamp(_) => Ok("timestamp"),
        _ => Err(unsupported(
            "NVL requires resolved character, NUMBER, DATE or TIMESTAMP operands",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn preview(path: &std::path::Path) -> BicDb {
        let mut db =
            BicDb::open_with_config(path, bicdb_core::DbConfig::default().with_fsync(false))
                .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        db
    }
    fn run(db: &mut BicDb, sql: &str) -> Result<SqlResult> {
        let lowered = lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        SqlSession::new(db).execute(&lowered)
    }

    #[test]
    fn set_operands_resolve_local_columns_ctes_and_temporal_defaults() {
        for profile in [POSTGRES_PROFILE, ORACLE_PREVIEW_PROFILE] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(root.path()).unwrap();
            ensure_database_profile(&mut db, profile).unwrap();
            let execute = |db: &mut BicDb, sql: &str| {
                if profile == ORACLE_PREVIEW_PROFILE {
                    run(db, sql)
                } else {
                    SqlSession::new(db).execute(sql)
                }
            };
            for sql in [
                "CREATE TABLE operand_groups(id NUMERIC PRIMARY KEY,happened DATE)",
                "CREATE TABLE operand_numbers(row_id NUMERIC PRIMARY KEY,group_id NUMERIC,amount NUMERIC)",
                "CREATE TABLE operand_text(row_id NUMERIC PRIMARY KEY,amount VARCHAR(12))",
                "INSERT INTO operand_groups VALUES(1,DATE '2024-02-29'),(2,NULL)",
                "INSERT INTO operand_numbers VALUES(1,1,NULL),(2,1,7)",
                "INSERT INTO operand_text VALUES(1,'abc'),(2,NULL)",
            ] { execute(&mut db, sql).unwrap(); }
            let function = if profile == ORACLE_PREVIEW_PROFILE {
                "NVL"
            } else {
                "COALESCE"
            };
            let empty = "SELECT id,CAST(0 AS NUMERIC) AS qty FROM operand_groups WHERE id<0";
            let grouped = format!("SELECT g.id,SUM({function}(n.amount,0)) AS qty FROM operand_groups g LEFT JOIN operand_numbers n ON g.id=n.group_id GROUP BY g.id");
            let union = format!("{empty} UNION ALL {grouped}");
            let difference = if profile == ORACLE_PREVIEW_PROFILE {
                "MINUS"
            } else {
                "EXCEPT"
            };

            let check = |db: &mut BicDb, sql: &str, wanted: &[[f64; 2]]| {
                let result = execute(db, sql).unwrap();
                let actual: Vec<[f64; 2]> = result
                    .rows
                    .iter()
                    .map(|row| {
                        [
                            row[0].to_cell().parse().unwrap(),
                            row[1].to_cell().parse().unwrap(),
                        ]
                    })
                    .collect();
                assert_eq!(actual, wanted, "{profile}: {sql}");
            };
            check(
                &mut db,
                &format!("{union} ORDER BY 1"),
                &[[1.0, 7.0], [2.0, 0.0]],
            );
            check(
                &mut db,
                &format!("{union} ORDER BY 2 DESC FETCH FIRST 1 ROW ONLY"),
                &[[1.0, 7.0]],
            );
            check(
                &mut db,
                &format!(
                    "({union}) UNION ALL SELECT 3,5 FROM operand_groups WHERE id=1 ORDER BY 1"
                ),
                &[[1.0, 7.0], [2.0, 0.0], [3.0, 5.0]],
            );
            check(&mut db, &format!("SELECT id,CASE WHEN id=1 THEN 8 ELSE 0 END AS qty FROM operand_groups {difference} {grouped} ORDER BY 1"), &[[1.0,8.0]]);
            let keyword = execute(
                &mut db,
                "SELECT 'MINUS' AS \"MINUS\" FROM operand_groups WHERE id=1",
            )
            .unwrap();
            assert_eq!(keyword.columns, vec!["MINUS"]);
            assert_eq!(keyword.rows, vec![vec![SqlValue::String("MINUS".into())]]);
            if profile == POSTGRES_PROFILE {
                assert_eq!(
                    execute(&mut db, "SELECT id AS minus FROM operand_groups WHERE id=1")
                        .unwrap()
                        .columns,
                    vec!["minus"]
                );
            }
            execute(&mut db, &format!("CREATE VIEW operand_union AS {union}")).unwrap();
            for _ in 0..20 {
                check(
                    &mut db,
                    "SELECT * FROM operand_union ORDER BY 1",
                    &[[1.0, 7.0], [2.0, 0.0]],
                );
                assert_eq!(
                    execute(&mut db, "SELECT COUNT(*) FROM operand_union")
                        .unwrap()
                        .rows[0][0]
                        .to_cell(),
                    "2"
                );
            }
            let cte = format!("WITH chosen AS (SELECT * FROM operand_groups) {empty} UNION ALL SELECT g.id,SUM({function}(n.amount,0)) AS qty FROM chosen g LEFT JOIN operand_numbers n ON g.id=n.group_id GROUP BY g.id ORDER BY 1");
            check(&mut db, &cte, &[[1.0, 7.0], [2.0, 0.0]]);
            check(&mut db, &format!("{empty} UNION ALL SELECT g.id,{function}(g.happened,DATE '2024-03-01')-DATE '2024-02-28' FROM operand_groups g ORDER BY 1"), &[[1.0,1.0],[2.0,2.0]]);
            check(&mut db, &format!("{empty} UNION ALL SELECT g.id,{function}((SELECT {function}(n.amount,3) FROM operand_numbers n WHERE n.group_id=g.id ORDER BY row_id FETCH FIRST 1 ROW ONLY),7) FROM operand_groups g ORDER BY 1"), &[[1.0,3.0],[2.0,7.0]]);
            let result = execute(&mut db, &format!("SELECT CAST(n.amount AS VARCHAR(12)) AS label FROM operand_numbers n WHERE row_id<0 UNION ALL SELECT {function}(n.amount,'none') FROM operand_text n ORDER BY 1")).unwrap();
            assert_eq!(
                result.rows,
                vec![
                    vec![SqlValue::String("abc".into())],
                    vec![SqlValue::String("none".into())]
                ],
                "{profile}"
            );
            let eager = format!("{empty} UNION ALL SELECT g.id,{function}(g.id,1/(g.id-1)) FROM operand_groups g WHERE g.id=1");
            if profile == ORACLE_PREVIEW_PROFILE {
                assert_eq!(execute(&mut db, &eager).unwrap_err().sqlstate(), "22012");
            } else {
                check(&mut db, &eager, &[[1.0, 1.0]]);
            }
        }
    }
    #[test]
    fn scalar_subquery_defaults_retain_declared_character_number_and_date_types() {
        for profile in [POSTGRES_PROFILE, ORACLE_PREVIEW_PROFILE] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(root.path()).unwrap();
            ensure_database_profile(&mut db, profile).unwrap();
            let execute = |db: &mut BicDb, sql: &str| {
                if profile == ORACLE_PREVIEW_PROFILE {
                    run(db, sql)
                } else {
                    SqlSession::new(db).execute(sql)
                }
            };
            for sql in [
                "CREATE TABLE scalar_defaults(id NUMERIC(10,0),label VARCHAR(12),amount NUMERIC(10,2),day_value DATE,seen TIMESTAMP(6))",
                "CREATE TABLE scalar_references(id INTEGER,target_id INTEGER)",
                "INSERT INTO scalar_defaults VALUES(1,'alpha',12.50,DATE '2024-02-29',TIMESTAMP '2024-02-29 12:34:56.125')",
                "INSERT INTO scalar_defaults VALUES(2,NULL,NULL,NULL,NULL)",
                "INSERT INTO scalar_references VALUES(1,1)",
                "INSERT INTO scalar_references VALUES(2,2)",
                "INSERT INTO scalar_references VALUES(3,9)",
            ] {
                execute(&mut db, sql).unwrap();
            }
            let function = if profile == ORACLE_PREVIEW_PROFILE {
                "NVL"
            } else {
                "COALESCE"
            };
            for (expression, expected) in [
                (format!("{function}((SELECT d.label FROM scalar_defaults d WHERE d.id=r.target_id),'none')"), ["alpha", "none", "none"]),
                (format!("{function}((SELECT d.label||'_x' FROM scalar_defaults d WHERE d.id=r.target_id),'none')"), if profile==ORACLE_PREVIEW_PROFILE { ["alpha_x", "_x", "none"] } else { ["alpha_x", "none", "none"] }),
                (format!("{function}((SELECT {function}(d.label,'-1')||'_x' FROM scalar_defaults d WHERE d.id=r.target_id),'none')"), ["alpha_x", "-1_x", "none"]),
                (format!("{function}((SELECT d.amount FROM scalar_defaults d WHERE d.id=r.target_id),7)"), ["12.5", "7", "7"]),
                (format!("{function}((SELECT d.day_value FROM scalar_defaults d WHERE d.id=r.target_id),DATE '2024-03-01')-DATE '2024-02-28'"), ["1", "2", "2"]),
                (format!("CASE WHEN {function}((SELECT d.seen FROM scalar_defaults d WHERE d.id=r.target_id),TIMESTAMP '2000-01-01 00:00:07.25')=TIMESTAMP '2024-02-29 12:34:56.125' THEN 1 ELSE 0 END"), ["1", "0", "0"]),
            ] {
                let sql = format!("SELECT {expression} FROM scalar_references r ORDER BY r.id");
                let result = execute(&mut db, &sql).unwrap_or_else(|error| panic!("{profile}: {sql}: {error}"));
                let actual: Vec<_> = result.rows.iter().map(|row| {
                    let value = row[0].to_cell();
                    if value.contains('.') { value.trim_end_matches('0').trim_end_matches('.').to_string() } else { value }
                }).collect();
                assert_eq!(actual, expected, "{profile}: {sql}");
            }
            let sql=format!("SELECT {function}((SELECT d.amount FROM scalar_defaults d WHERE d.id=r.target_id),1/(r.id-1)) FROM scalar_references r WHERE r.id=1");
            let result = execute(&mut db, &sql);
            if profile == ORACLE_PREVIEW_PROFILE {
                assert_eq!(result.unwrap_err().sqlstate(), "22012");
            } else {
                let value = result.unwrap().rows[0][0].to_cell();
                assert_eq!(value.trim_end_matches('0').trim_end_matches('.'), "12.5");
            }
        }
    }

    #[test]
    fn aggregate_nvl_uses_query_scope_and_retains_eager_conversion() {
        let root = tempfile::tempdir().unwrap();
        let mut db = preview(root.path());
        for sql in [
            "CREATE TABLE aggregate_values(k NUMBER,n NUMBER,label VARCHAR2(8),day_value DATE)",
            "INSERT INTO aggregate_values VALUES(1,10,'A',DATE '2024-02-29')",
            "INSERT INTO aggregate_values VALUES(1,5,'Z',DATE '2024-03-01')",
            "CREATE FUNCTION aggregate_total(p_key NUMBER) RETURN NUMBER IS result NUMBER; BEGIN SELECT NVL(SUM(a.n),0) INTO result FROM aggregate_values a WHERE a.k=p_key;RETURN result;END;",
        ] {run(&mut db,sql).unwrap_or_else(|e|panic!("{sql}: {e}"));}
        for (sql, expected) in [
            ("SELECT aggregate_total(1) FROM DUAL", "15"),
            ("SELECT aggregate_total(9) FROM DUAL", "0"),
            ("SELECT NVL(SUM(n),0) FROM aggregate_values WHERE k=1", "15"),
            ("SELECT NVL(SUM(n),0) FROM aggregate_values WHERE k=9", "0"),
            (
                "SELECT NVL(MAX(label),'none') FROM aggregate_values WHERE k=1",
                "Z",
            ),
            (
                "SELECT NVL(MAX(label),'none') FROM aggregate_values WHERE k=9",
                "none",
            ),
            (
                "SELECT NVL(MAX(day_value),DATE '2000-01-01') FROM aggregate_values WHERE k=1",
                "2024-03-01 00:00:00",
            ),
            (
                "SELECT NVL(MAX(day_value),DATE '2000-01-01') FROM aggregate_values WHERE k=9",
                "2000-01-01 00:00:00",
            ),
        ] {
            assert_eq!(
                run(&mut db, sql)
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                    .rows[0][0]
                    .to_cell(),
                expected,
                "{sql}"
            );
        }
        for (sql, state) in [
            (
                "SELECT NVL(SUM(n),1/0) FROM aggregate_values WHERE k=1",
                "22012",
            ),
            (
                "SELECT NVL(SUM(n),'bad') FROM aggregate_values WHERE k=1",
                "22P02",
            ),
        ] {
            assert_eq!(run(&mut db, sql).unwrap_err().sqlstate(), state, "{sql}");
        }
    }

    #[test]
    fn native_aggregate_coalesce_keeps_its_own_lazy_behavior() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in ["CREATE TABLE native_aggregate_values(k integer,n numeric)",
            "INSERT INTO native_aggregate_values VALUES(1,10),(1,5)",
            "CREATE FUNCTION native_total(p_key integer) RETURNS numeric LANGUAGE plpgsql AS $$DECLARE result numeric; BEGIN SELECT COALESCE(SUM(a.n),0) INTO result FROM native_aggregate_values a WHERE a.k=p_key;RETURN result;END$$",
        ] {session.execute(sql).unwrap_or_else(|e|panic!("{sql}: {e}"));}
        for (sql, expected) in [
            ("SELECT native_total(1)", "15"),
            ("SELECT native_total(9)", "0"),
            (
                // The denominator must depend on the aggregate. PostgreSQL can
                // fold a constant 1/0 during planning even in a lazy branch.
                "SELECT COALESCE(SUM(n),1/(COUNT(*)-2)) FROM native_aggregate_values WHERE k=1",
                "15",
            ),
            ("SELECT CASE WHEN SUM(n)>0 THEN SUM(n) ELSE 1/(COUNT(*)-2) END FROM native_aggregate_values WHERE k=1", "15"),
            ("SELECT COALESCE(SUM(n),(SELECT SUM(1/(n-n)) FROM native_aggregate_values)) FROM native_aggregate_values WHERE k=1", "15"),
        ] {
            assert_eq!(
                session.execute(sql).unwrap().rows[0][0].to_cell(),
                expected,
                "{sql}"
            );
        }
        for sql in [
            "SELECT COALESCE(SUM(n),SUM(1/(n-n))) FROM native_aggregate_values WHERE k=1",
            "SELECT CASE WHEN SUM(n)>0 THEN SUM(n) ELSE SUM(1/(n-n)) END FROM native_aggregate_values WHERE k=1",
        ] {assert_eq!(session.execute(sql).unwrap_err().sqlstate(),"22012","{sql}");}
        // A failed projection must not leave cached values in the next group or
        // statement, even when it reuses the same parsed SQL template.
        assert_eq!(
            session
                .execute("SELECT COALESCE(SUM(n),0) FROM native_aggregate_values WHERE k=9")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "0"
        );
    }
    #[test]
    fn nvl_is_eager_and_converts_the_unused_replacement() {
        let root = tempfile::tempdir().unwrap();
        let mut db = preview(root.path());
        for (sql, state) in [
            ("SELECT NVL(1,1/0) FROM DUAL", "22012"),
            ("SELECT NVL('A',CHR(-1)) FROM DUAL", "22003"),
            ("SELECT NVL(1,'bad') FROM DUAL", "22P02"),
        ] {
            assert_eq!(run(&mut db, sql).unwrap_err().sqlstate(), state, "{sql}");
        }
        assert_eq!(
            run(&mut db, "SELECT COALESCE('A',CHR(-1)) FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "A"
        );
        run(&mut db,"CREATE FUNCTION nvl_eager(p_value NUMBER) RETURN NUMBER IS BEGIN RETURN NVL(p_value,1/0); END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT nvl_eager(1) FROM DUAL")
                .unwrap_err()
                .sqlstate(),
            "22012"
        );
    }
    #[test]
    fn nvl_selects_declared_types_and_keeps_empty_character_identity() {
        let root = tempfile::tempdir().unwrap();
        let mut db = preview(root.path());
        for (expression, expected, ty) in [
            ("NVL(NULL,7)", "7", "numeric"),
            ("NVL('',7)", "7", "text"),
            ("NVL('A',7)", "A", "text"),
            ("NVL(CAST(NULL AS CHAR(3)),'longer')", "longer", "text"),
            ("NVL(CAST('A' AS CHAR(3)),'B')", "A  ", "text"),
            ("NVL(COALESCE('','x'),7)", "x", "text"),
        ] {
            let result = run(&mut db, &format!("SELECT {expression} FROM DUAL")).unwrap();
            assert_eq!(result.rows[0][0].to_cell(), expected, "{expression}");
            assert_eq!(result.column_types, vec![Some(ty.into())], "{expression}");
        }
        for expression in ["NVL(NULL,NULL)", "NVL('','')"] {
            assert_eq!(
                run(&mut db, &format!("SELECT {expression} FROM DUAL"))
                    .unwrap()
                    .rows[0][0],
                SqlValue::Null
            );
        }
        assert_eq!(
            run(
                &mut db,
                "SELECT NVL(CAST(NULL AS TIMESTAMP),'29-FEB-24') FROM DUAL"
            )
            .unwrap()
            .rows[0][0]
                .to_cell(),
            "2024-02-29 00:00:00"
        );
        for expression in ["NVL(1,DATE '2024-02-29')", "NVL(DATE '2024-02-29',1)"] {
            assert_eq!(
                run(&mut db, &format!("SELECT {expression} FROM DUAL"))
                    .unwrap_err()
                    .sqlstate(),
                "42804"
            );
        }
    }
    #[test]
    fn nvl_datetime_precision_survives_columns_ctes_and_procedural_records() {
        let root = tempfile::tempdir().unwrap();
        let mut db = preview(root.path());
        run(
            &mut db,
            "CREATE TABLE stamps(k NUMBER,t3 TIMESTAMP(3),d DATE)",
        )
        .unwrap();
        run(&mut db,"INSERT INTO stamps VALUES(1,TIMESTAMP '2024-02-29 12:34:56.123456',TIMESTAMP '2024-02-29 12:34:56')").unwrap();
        for sql in [
            "SELECT NVL('',t3) FROM stamps",
            "WITH chosen AS (SELECT t3 AS stamp FROM stamps) SELECT NVL('',stamp) FROM chosen",
        ] {
            assert_eq!(
                run(&mut db, sql).unwrap().rows[0][0].to_cell(),
                "29-FEB-24 12.34.56.123 PM"
            );
        }
        run(&mut db,"CREATE FUNCTION stamp_record RETURN VARCHAR2 IS CURSOR c IS SELECT t3 FROM stamps; BEGIN FOR r IN c LOOP RETURN NVL('',r.t3); END LOOP; RETURN NULL; END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT stamp_record() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "29-FEB-24 12.34.56.123 PM"
        );
        run(&mut db,"CREATE FUNCTION stamp_local RETURN VARCHAR2 IS v TIMESTAMP(3):=TIMESTAMP '2024-02-29 12:34:56.123456'; BEGIN RETURN NVL('',v); END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT stamp_local() FROM DUAL").unwrap().rows[0][0].to_cell(),
            "29-FEB-24 12.34.56.123 PM"
        );
        run(&mut db,"CREATE FUNCTION stamp_bound(p_stamp TIMESTAMP) RETURN VARCHAR2 IS v VARCHAR2(100); BEGIN SELECT NVL('',p_stamp) INTO v FROM DUAL; RETURN v; END;").unwrap();
        assert_eq!(
            run(
                &mut db,
                "SELECT stamp_bound(TIMESTAMP '2024-02-29 12:34:56.123456') FROM DUAL"
            )
            .unwrap()
            .rows[0][0]
                .to_cell(),
            "29-FEB-24 12.34.56.123456000 PM"
        );
        run(&mut db,"CREATE FUNCTION stamp_return RETURN stamps.t3%TYPE IS BEGIN RETURN TIMESTAMP '2024-02-29 12:34:56.123456'; END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT stamp_return() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "2024-02-29 12:34:56.123"
        );
        assert_eq!(
            run(&mut db, "SELECT NVL('',stamp_return()) FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "29-FEB-24 12.34.56.123000000 PM"
        );
        let value = run(
            &mut db,
            "SELECT NVL(d,DATE '2024-01-01')-DATE '2024-02-28' FROM stamps",
        )
        .unwrap();
        assert_eq!(value.column_types, vec![Some("numeric".into())]);
        assert_eq!(
            run(
                &mut db,
                "SELECT NVL(CAST(NULL AS DATE),TIMESTAMP '2024-02-29 12:34:56.999999') FROM DUAL"
            )
            .unwrap()
            .rows[0][0]
                .to_cell(),
            "2024-02-29 12:34:56"
        );
    }
    #[test]
    fn native_and_previous_routine_contracts_remain_independent() {
        let root = tempfile::tempdir().unwrap();
        let mut db = preview(root.path());
        SqlSession::new(&mut db).execute("CREATE FUNCTION previous_nvl() RETURNS INTEGER LANGUAGE bicdb_oracle_preview_v10 AS $$BEGIN RETURN coalesce(1,1/0); END$$").unwrap();
        assert_eq!(
            run(&mut db, "SELECT previous_nvl() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "1"
        );
        let native = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(native.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE FUNCTION nvl(a INTEGER,b INTEGER) RETURNS INTEGER LANGUAGE sql AS $$SELECT a+b$$").unwrap();
        assert_eq!(
            session.execute("SELECT nvl(2,3)").unwrap().rows[0][0].to_cell(),
            "5"
        );
        assert_eq!(
            session.execute("SELECT coalesce(1,1/0)").unwrap().rows[0][0].to_cell(),
            "1"
        );
    }
}

pub(super) fn lower_call(
    expr: &mut Expr,
    kind: impl Fn(&Expr) -> Kind,
    sql_context: bool,
    defer_unknown: bool,
) -> Result<()> {
    let Expr::Function(function) = expr else {
        return Ok(());
    };
    if function.name.0.len() != 1
        || !object_name_lowercase(&function.name).is_ok_and(|name| name == "nvl")
    {
        return Ok(());
    }
    let FunctionArguments::List(list) = &function.args else {
        return Err(unsupported("NVL requires two scalar arguments"));
    };
    let mut args = function_args(function);
    if args.len() != 2
        || list.args.len() != 2
        || list.duplicate_treatment.is_some()
        || !list.clauses.is_empty()
        || function.over.is_some()
        || function.filter.is_some()
    {
        return Err(SqlError::InvalidSql(
            "NVL requires two scalar arguments".into(),
        ));
    }
    let kinds = [kind(&args[0]), kind(&args[1])];
    if defer_unknown && kinds.contains(&Kind::Unknown) {
        return Ok(());
    }
    let target_kind = if kinds[0] == Kind::Null {
        kinds[1]
    } else {
        kinds[0]
    };
    let target = type_name(target_kind)?;
    for source in kinds {
        type_name(source)?;
        if target == "number" && matches!(source, Kind::Date | Kind::Timestamp(_))
            || matches!(target, "date" | "timestamp") && source == Kind::Number
        {
            return Err(SqlError::data_exception(
                "42804",
                "incompatible Oracle NVL operand types",
                None,
            ));
        }
    }
    let precision = if target == "timestamp" {
        match (kinds[0], kinds[1]) {
            (Kind::Timestamp(Some(a)), Kind::Timestamp(Some(b))) => Some(a.max(b)),
            (Kind::Timestamp(p), _) | (_, Kind::Timestamp(p)) => p,
            _ => None,
        }
    } else {
        None
    };
    let context = if sql_context { "sql" } else { "plsql" };
    // Oracle diagnoses character substitution against a non-null TIMESTAMP
    // literal as a type mismatch. A typed NULL or column, on the other hand,
    // uses default timestamp input conversion. Keep those paths distinct.
    fn timestamp_constant(expr: &Expr) -> bool {
        match expr {
            Expr::TypedString(value) => matches!(value.data_type, DataType::Timestamp(..)),
            Expr::Cast { expr, .. } | Expr::Nested(expr) => timestamp_constant(expr),
            _ => false,
        }
    }
    let timestamp_constant = timestamp_constant(&args[0]);
    let mut converted = Vec::new();
    for (arg, source) in args.iter_mut().zip(kinds) {
        let label = if target == "text" {
            if let Kind::Timestamp(precision) = source {
                format!(
                    "timestamp:{}",
                    precision.ok_or_else(|| unsupported(
                        "NVL timestamp character conversion requires declared fractional precision"
                    ))?
                )
            } else {
                type_name(source)?.into()
            }
        } else if target == "timestamp" && source == Kind::Text && timestamp_constant {
            "text_literal".into()
        } else {
            type_name(source)?.into()
        };
        super::decode::physical_dates(arg)?;
        let arg = render_sql(arg);
        converted.push(if target == "text" {
            format!("bicdb_oracle_nvl_convert_text({arg},'{label}','{context}')")
        } else if target == "timestamp" {
            format!("bicdb_oracle_nvl_convert_timestamp({arg},'{label}')")
        } else {
            format!("bicdb_oracle_decode_{target}({arg},'{label}')")
        });
    }
    let extra = if target == "timestamp" {
        format!(",{}", precision.map_or(-1, i16::from))
    } else {
        String::new()
    };
    *expr = expression(&format!(
        "bicdb_oracle_nvl_{target}({},{}{extra})",
        converted[0], converted[1]
    ))?;
    Ok(())
}
