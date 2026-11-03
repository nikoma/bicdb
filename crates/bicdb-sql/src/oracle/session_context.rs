//! Bounded USERENV inspection using SQL identity and host-installed transport facts.
use super::*;

thread_local! { static ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
pub(crate) struct Scope(bool);
impl Scope {
    pub(crate) fn new(enabled: bool) -> Self {
        Self(ENABLED.with(|flag| flag.replace(enabled)))
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ENABLED.with(|flag| flag.set(self.0));
    }
}

pub(crate) fn lower_expression(expr: &mut Expr) -> Result<()> {
    if !ENABLED.with(std::cell::Cell::get) {
        return Ok(());
    }
    let Expr::Function(function) = expr else {
        return Ok(());
    };
    let [part] = function.name.0.as_slice() else {
        return Ok(());
    };
    if !part.as_ident().is_some_and(|name| {
        name.quote_style.is_none() && name.value.eq_ignore_ascii_case("sys_context")
    }) {
        return Ok(());
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(unsupported("SYS_CONTEXT requires positional arguments"));
    };
    if function.filter.is_some()
        || function.over.is_some()
        || arguments.duplicate_treatment.is_some()
        || !arguments.clauses.is_empty()
        || arguments
            .args
            .iter()
            .any(|arg| !matches!(arg, FunctionArg::Unnamed(FunctionArgExpr::Expr(_))))
    {
        return Err(unsupported(
            "SYS_CONTEXT requires positional scalar arguments",
        ));
    }
    function.name = ObjectName::from(vec![
        Ident::new("pg_catalog"),
        Ident::new("bicdb_oracle_sys_context"),
    ]);
    *expr = expression(&format!("CAST({} AS VARCHAR)", render_sql(expr)))?;
    Ok(())
}

fn display_role(db: &BicDb, role: &str) -> Result<String> {
    // Preserve real quoted role names. Ordinary roles use the preview's uppercase
    // identifier convention; authorization continues to use the native role ID.
    Ok(load_role_schema(db, role)?
        .map(|r| r.name)
        .unwrap_or_else(|| role.to_uppercase()))
}

pub(crate) fn evaluate(
    db: &BicDb,
    args: &[SqlValue],
    gucs: &HashMap<String, String>,
) -> Result<SqlValue> {
    if args.len() < 2 {
        return Err(errors::database_error(
            -938,
            "42601",
            "not enough arguments for SYS_CONTEXT",
        ));
    }
    if args.len() > 3 {
        return Err(errors::database_error(
            -939,
            "42601",
            "too many arguments for SYS_CONTEXT",
        ));
    }
    if args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let mut limit = 256usize;
    if let Some(length) = args.get(2) {
        let value = parse_decimal(&length.to_cell()).map_err(|_| {
            errors::database_error(-1722, "22P02", "invalid SYS_CONTEXT length number")
        })?;
        // Compare before conversion: arbitrarily large numeric lengths use the
        // default rather than overflowing a machine integer.
        let integral = if value.scale == 0 {
            value.mantissa
        } else {
            value.mantissa / pow10_bigint(value.scale)
        };
        if integral >= BigInt::from(1) && integral <= BigInt::from(4000) {
            limit = integral
                .to_string()
                .parse()
                .map_err(|_| unsupported("invalid SYS_CONTEXT length"))?;
        }
    }
    let namespace = args[0].to_cell();
    let attribute = args[1].to_cell();
    if namespace.is_empty() || attribute.is_empty() || !namespace.eq_ignore_ascii_case("USERENV") {
        return Ok(SqlValue::Null);
    }
    let current = current_user_from_gucs(gucs);
    let text = match attribute.to_ascii_uppercase().as_str() {
        "CURRENT_USER" => Some(display_role(db, &current)?),
        "SESSION_USER" => Some(display_role(db, &session_user_from_gucs(gucs))?),
        "CURRENT_SCHEMA" => Some(
            preview_schema(db, gucs.get("search_path").map(String::as_str), &current)?
                .unwrap_or_else(default_schema_name),
        ),
        "DB_NAME" => Some(current_database_from_gucs(gucs)),
        // BicDB has no configured Oracle database domain. A missing transport
        // fact is NULL; never infer the client host from the server hostname.
        "DB_DOMAIN" => None,
        "HOST" => gucs.get(SESSION_CLIENT_HOST_GUC).cloned(),
        "IP_ADDRESS" => gucs.get(SESSION_CLIENT_ADDRESS_GUC).cloned(),
        _ => {
            return Err(errors::database_error(
                -2003,
                "22023",
                "unsupported or invalid USERENV parameter",
            ))
        }
    };
    let Some(mut text) = text.filter(|text| !text.is_empty()) else {
        return Ok(SqlValue::Null);
    };
    if text.len() > limit {
        // PostgreSQL text cannot transport an incomplete UTF8 sequence. Retain
        // only complete codepoints when a byte limit cuts a multibyte character.
        let mut boundary = limit;
        while !text.is_char_boundary(boundary) {
            boundary -= 1;
        }
        text.truncate(boundary);
    }
    if text.is_empty() {
        Ok(SqlValue::Null)
    } else {
        Ok(SqlValue::String(text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn text(value: &str) -> SqlValue {
        SqlValue::String(value.into())
    }
    fn run(db: &mut BicDb, source: &str) -> Result<SqlResult> {
        let lowered = lower_profile_sql(db, source, ORACLE_PREVIEW_PROFILE)?;
        SqlSession::new(db).execute(&lowered)
    }
    #[test]
    fn nulls_errors_lengths_and_case_follow_source_contract() {
        let root = tempfile::tempdir().unwrap();
        let db = BicDb::open(root.path()).unwrap();
        let gucs = HashMap::from([("session_authorization".into(), "ordinary_user".into())]);
        let call = |args: &[SqlValue]| evaluate(&db, args, &gucs);
        for (length, expected) in [
            (1, "O"),
            (2, "OR"),
            (0, "ORDINARY_USER"),
            (-1, "ORDINARY_USER"),
            (4001, "ORDINARY_USER"),
        ] {
            assert_eq!(
                call(&[text("userenv"), text("current_user"), SqlValue::Int(length)]).unwrap(),
                text(expected)
            );
        }
        assert_eq!(
            call(&[text("USERENV"), text("CURRENT_USER"), text("1.9")]).unwrap(),
            text("O")
        );
        assert_eq!(
            call(&[
                text("USERENV"),
                text("CURRENT_USER"),
                text("100000000000000000000000000000000000000")
            ])
            .unwrap(),
            text("ORDINARY_USER")
        );
        for args in [
            vec![SqlValue::Null, text("CURRENT_USER")],
            vec![text("USERENV"), SqlValue::Null],
            vec![text("USERENV"), text("CURRENT_USER"), SqlValue::Null],
            vec![text("MISSING"), text("X")],
            vec![text(""), text("CURRENT_USER")],
        ] {
            assert_eq!(call(&args).unwrap(), SqlValue::Null);
        }
        for (args, code) in [
            (vec![], -938),
            (vec![text("USERENV")], -938),
            (
                vec![
                    text("USERENV"),
                    text("CURRENT_USER"),
                    SqlValue::Int(1),
                    SqlValue::Int(2),
                ],
                -939,
            ),
            (vec![text("USERENV"), text("missing")], -2003),
            (vec![text("USERENV"), text("CURRENT_USER ")], -2003),
            (
                vec![text("USERENV"), text("CURRENT_USER"), text("invalid")],
                -1722,
            ),
        ] {
            let detail: JsonValue =
                serde_json::from_str(&errors::transport_detail(&call(&args).unwrap_err())).unwrap();
            assert_eq!(detail["oracle_error_stack"][0]["code"].as_i64(), Some(code));
        }
    }
    #[test]
    fn sql_calls_provide_text_metadata_and_resolve_schema() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let result=run(&mut db,"SELECT SYS_CONTEXT('USERENV','DB_NAME'),SYS_CONTEXT('USERENV','DB_DOMAIN'),SYS_CONTEXT('USERENV','HOST'),SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL").unwrap();
        assert_eq!(
            result.rows[0],
            vec![
                text("bicdb"),
                SqlValue::Null,
                SqlValue::Null,
                text("public")
            ]
        );
        assert!(result
            .column_types
            .iter()
            .all(|kind| kind.as_deref() == Some("varchar")));
        run(&mut db, "CREATE SCHEMA purple").unwrap();
        let mut gucs = HashMap::from([("search_path".into(), "purple".into())]);
        assert_eq!(
            evaluate(&db, &[text("USERENV"), text("CURRENT_SCHEMA")], &gucs).unwrap(),
            text("PURPLE")
        );
        gucs.insert(SESSION_CLIENT_ADDRESS_GUC.into(), "::1".into());
        assert_eq!(
            evaluate(&db, &[text("USERENV"), text("IP_ADDRESS")], &gucs).unwrap(),
            text("::1")
        );
    }
    #[test]
    fn legacy_and_native_routines_do_not_gain_new_context_calls() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let definition="CREATE FUNCTION context_name RETURN VARCHAR2 IS BEGIN RETURN SYS_CONTEXT('USERENV','DB_NAME');END;";
        let legacy = lower_profile_sql(&db, definition, ORACLE_PREVIEW_PROFILE)
            .unwrap()
            .replace(PREVIEW_ROUTINE_LANGUAGE, PREVIEW_ROUTINE_LANGUAGE_V21);
        SqlSession::new(&mut db).execute(&legacy).unwrap();
        assert!(run(&mut db, "SELECT context_name() FROM DUAL").is_err());
        run(
            &mut db,
            &definition.replace("context_name", "new_context_name"),
        )
        .unwrap();
        assert_eq!(
            run(&mut db, "SELECT new_context_name() FROM DUAL")
                .unwrap()
                .rows[0][0],
            text("bicdb")
        );
        SqlSession::new(&mut db).execute("CREATE FUNCTION native_context_name() RETURNS text LANGUAGE plpgsql AS $$BEGIN RETURN sys_context('USERENV','DB_NAME');END;$$").unwrap();
        assert!(SqlSession::new(&mut db)
            .execute("SELECT native_context_name()")
            .is_err());
    }
    #[test]
    fn native_syntax_and_authored_quoted_names_remain_independent() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        assert!(SqlSession::new(&mut db)
            .execute("SELECT sys_context('USERENV','DB_NAME')")
            .is_err());
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        SqlSession::new(&mut db).execute("CREATE FUNCTION \"SYS_CONTEXT\"(a text,b text) RETURNS text LANGUAGE sql AS $$SELECT 'authored'$$").unwrap();
        assert_eq!(
            run(
                &mut db,
                "SELECT \"SYS_CONTEXT\"('USERENV','DB_NAME') FROM DUAL"
            )
            .unwrap()
            .rows[0][0],
            text("authored")
        );
    }
    #[test]
    fn transport_metadata_is_immutable_and_survives_reset_and_discard() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut gucs = SqlSessionGucState::default();
        gucs.insert_session_guc(SESSION_CLIENT_ADDRESS_GUC.into(), "127.0.0.1".into());
        gucs.insert_session_guc(SESSION_CLIENT_HOST_GUC.into(), "client.example".into());
        let mut session = SqlSession::new(&mut db).with_session_guc_state(gucs);
        for key in [SESSION_CLIENT_ADDRESS_GUC, SESSION_CLIENT_HOST_GUC] {
            for source in [
                format!("SET {key}='forged'"),
                format!("SELECT set_config('{key}','forged',false)"),
                format!("RESET {key}"),
            ] {
                assert_eq!(session.execute(&source).unwrap_err().sqlstate(), "42501");
            }
        }
        for source in ["RESET ALL", "DISCARD ALL"] {
            session.execute(source).unwrap();
            assert_eq!(
                session
                    .session_guc_state()
                    .session_gucs()
                    .get(SESSION_CLIENT_ADDRESS_GUC)
                    .map(String::as_str),
                Some("127.0.0.1")
            );
            assert_eq!(
                session
                    .session_guc_state()
                    .session_gucs()
                    .get(SESSION_CLIENT_HOST_GUC)
                    .map(String::as_str),
                Some("client.example")
            );
        }
    }
}
