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
