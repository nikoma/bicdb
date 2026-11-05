//! Runtime BLOB values. Locator capabilities never cross JSON or wire boundaries.
use super::*;

thread_local! { static ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

/// Declaration provenance belongs to a persisted compiler contract, rather
/// than whichever profile happens to be active in its caller.
pub(crate) struct Scope(bool);
impl Scope {
    pub(crate) fn new(enabled: bool) -> Self {
        Self(ENABLED.with(|value| value.replace(enabled)))
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ENABLED.with(|value| value.set(self.0));
    }
}
pub(crate) fn enabled() -> bool {
    ENABLED.with(std::cell::Cell::get)
}
pub(crate) fn is_modifier(modifier: Option<&PgTypeModifier>) -> bool {
    matches!(modifier, Some(PgTypeModifier::OracleBlobV1))
}
pub(crate) fn routine_type(data_type: &DataType) -> Result<Option<RoutineTypeSchema>> {
    if !enabled() {
        return Ok(None);
    }
    match data_type {
        DataType::Blob(None) => Ok(Some(RoutineTypeSchema {
            pg_type: "bytea".into(),
            type_modifier: Some(PgTypeModifier::OracleBlobV1),
        })),
        DataType::Blob(Some(_)) => Err(unsupported(
            "Oracle BLOB declarations do not accept a length",
        )),
        _ => Ok(None),
    }
}

pub(crate) fn returns_blob(routine: &RoutineSchema) -> Result<bool> {
    if routine.kind == RoutineKind::Procedure
        || !preview_routine_language_version(&routine.language).is_some_and(|v| v >= 29)
    {
        return Ok(false);
    }
    let _scope = Scope::new(true);
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, &routine.definition)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    let [Statement::CreateFunction(function)] = statements.as_slice() else {
        return Err(unsupported("BLOB return requires a function definition"));
    };
    match &function.return_type {
        Some(FunctionReturnType::DataType(data_type) | FunctionReturnType::SetOf(data_type)) => {
            Ok(routine_type(data_type)?.is_some())
        }
        _ => Ok(false),
    }
}

pub(crate) fn package_operation(function: &Function) -> Option<String> {
    let names = function
        .name
        .0
        .iter()
        .map(|part| {
            part.as_ident().map(|id| {
                if id.quote_style.is_some() {
                    id.value.clone()
                } else {
                    id.value.to_uppercase()
                }
            })
        })
        .collect::<Option<Vec<_>>>()?;
    match names
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["DBMS_LOB", name] | ["SYS", "DBMS_LOB", name] => Some((*name).to_string()),
        _ => None,
    }
}

/// Keep the writable destination as an lvalue in the routine IR.
pub(crate) fn lower(
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
) -> Result<()> {
    for statement in statements {
        match statement {
            RoutineStmt::Sql(Statement::Call(function))
                if package_operation(function).as_deref() == Some("APPEND") =>
            {
                let params =
                    parse_routine_params(&["INOUT dest_lob bytea".into(), "src_lob bytea".into()])?;
                let arguments = procedure_actual_arguments(function, &params, false)?
                    .into_iter()
                    .map(|arg| {
                        arg.cloned()
                            .map(RoutineExpr::unbound)
                            .ok_or_else(|| unsupported("missing LOB package argument"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                *statement = RoutineStmt::OracleBuiltin {
                    builtin: errors::Builtin::LobAppend,
                    arguments,
                };
            }
            RoutineStmt::Block { body, handlers, .. } => lower(body, handlers)?,
            RoutineStmt::If {
                then_body,
                else_body,
                ..
            } => {
                lower(then_body, &mut [])?;
                lower(else_body, &mut [])?;
            }
            RoutineStmt::Case {
                arms, else_body, ..
            } => {
                for body in arms {
                    lower(body, &mut [])?;
                }
                if let Some(body) = else_body {
                    lower(body, &mut [])?;
                }
            }
            RoutineStmt::ForLoop { body, .. }
            | RoutineStmt::ForeachLoop { body, .. }
            | RoutineStmt::QueryForLoop { body, .. }
            | RoutineStmt::CursorForLoop { body, .. }
            | RoutineStmt::Loop { body }
            | RoutineStmt::WhileLoop { body, .. } => lower(body, &mut [])?,
            _ => {}
        }
    }
    for handler in handlers {
        lower(&mut handler.statements, &mut [])?;
    }
    Ok(())
}

pub(crate) fn append(
    session: &mut SqlSession<'_>,
    frame: &mut RoutineFrame,
    args: &[RoutineExpr],
) -> Result<RoutineControl> {
    session.cancellation.check()?;
    let [destination, source] = args else {
        return Err(unsupported("LOB APPEND requires two arguments"));
    };
    let Expr::Identifier(id) = &destination.expr else {
        return Err(unsupported(
            "LOB APPEND destination requires a declared BLOB variable",
        ));
    };
    let target = RoutineAssignmentTarget::Variable(normalize_object_name(&ident_value(id)));
    if crate::statement_context::describing() {
        return Ok(RoutineControl::Continue);
    }
    // Evaluate both before changing the destination, including self-append.
    let mut destination = session.eval_routine_expr(frame, destination)?;
    let source = session.eval_routine_expr(frame, source)?;
    if matches!(destination, SqlValue::Null) || matches!(source, SqlValue::Null) {
        return Err(errors::database_error(
            -6502,
            "22004",
            "PL/SQL: numeric or value error: invalid LOB locator",
        ));
    }
    let (SqlValue::OracleLob(destination), SqlValue::OracleLob(source)) =
        (&mut destination, source)
    else {
        return Err(unsupported("LOB APPEND requires qualified BLOB operands"));
    };
    if !destination.is_temporary() && (session.row_routine_call || session.in_oracle_sql_function) {
        return Err(errors::database_error(
            -14553,
            "25006",
            "cannot perform a lob write operation inside a query",
        ));
    }
    Arc::make_mut(destination).append_temporary(source.bytes())?;
    session.assign_routine_value(frame, &target, SqlValue::OracleLob(destination.clone()))?;
    Ok(RoutineControl::Continue)
}

#[derive(Clone, PartialEq, Eq)]
enum Storage {
    Empty,
    Temporary,
}

/// An execution value with binary wire encoding and private locator provenance.
/// Construction and mutation remain inside the SQL engine; deserialization of
/// `SqlValue` cannot manufacture one of these capabilities.
#[derive(Clone, PartialEq, Eq)]
pub struct Value {
    storage: Storage,
    bytes: Vec<u8>,
}

impl std::fmt::Debug for Value {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OracleBlob")
            .field(
                "storage",
                &match &self.storage {
                    Storage::Empty => "empty",
                    Storage::Temporary => "temporary",
                },
            )
            .field("length", &self.bytes.len())
            .finish()
    }
}

impl Value {
    pub(crate) fn empty() -> Self {
        Self {
            storage: Storage::Empty,
            bytes: Vec::new(),
        }
    }
    pub(crate) fn temporary(bytes: Vec<u8>) -> Self {
        Self {
            storage: Storage::Temporary,
            bytes,
        }
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn is_temporary(&self) -> bool {
        matches!(self.storage, Storage::Temporary)
    }
    pub(crate) fn to_cell(&self) -> String {
        format_bytea_hex(&self.bytes)
    }
    pub(crate) fn append_temporary(&mut self, source: &[u8]) -> Result<()> {
        if !self.is_temporary() {
            return Err(errors::database_error(
                -22275,
                "55000",
                "invalid LOB locator specified",
            ));
        }
        self.bytes.try_reserve(source.len()).map_err(|_| {
            SqlError::data_exception("53200", "insufficient memory for temporary LOB", None)
        })?;
        self.bytes.extend_from_slice(source);
        Ok(())
    }
}

pub(crate) fn coerce(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null | SqlValue::OracleLob(_) => Ok(value),
        _ => Err(unsupported(
            "BLOB assignment requires a qualified binary conversion",
        )),
    }
}

/// Called only after type resolution has selected a RAW-to-BLOB conversion.
/// Character values must pass through their own Oracle RAW conversion first.
pub(crate) fn from_raw(value: SqlValue) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(value);
    }
    let bytes =
        crate::eval::bytea_argument(&value)?.ok_or_else(|| unsupported("missing RAW input"))?;
    Ok(SqlValue::OracleLob(Arc::new(Value::temporary(bytes))))
}

pub(crate) fn scalar(name: &str, args: &[SqlValue]) -> Result<Option<SqlValue>> {
    match name {
        "bicdb_oracle_empty_blob" => {
            if !args.is_empty() {
                return Err(unsupported("EMPTY_BLOB takes no arguments"));
            }
            Ok(Some(SqlValue::OracleLob(Arc::new(Value::empty()))))
        }
        "bicdb_oracle_blob_from_raw" => {
            let [value] = args else {
                return Err(unsupported("RAW-to-BLOB conversion requires one input"));
            };
            from_raw(value.clone()).map(Some)
        }
        "bicdb_oracle_blob_getlength" | "bicdb_oracle_blob_istemporary" => {
            let [value] = args else {
                return Err(unsupported("LOB scalar requires one argument"));
            };
            match value {
                SqlValue::Null => Ok(Some(SqlValue::Null)),
                SqlValue::OracleLob(value) => {
                    Ok(Some(SqlValue::Int(if name.ends_with("getlength") {
                        value.bytes().len() as i64
                    } else {
                        i64::from(value.is_temporary())
                    })))
                }
                _ => Err(unsupported("LOB scalar requires a qualified BLOB operand")),
            }
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(db: &mut BicDb, source: &str) -> Result<SqlResult> {
        let lowered = lower_session_sql(db, source, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        SqlSession::new(db).execute(&lowered)
    }

    #[test]
    fn routine_blob_declarations_preserve_typed_values_and_reopen() {
        let root = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open_with_config(
                root.path(),
                bicdb_core::DbConfig::default().with_fsync(false),
            )
            .unwrap();
            ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
            for source in [
                "CREATE FUNCTION empty_payload RETURN BLOB IS v BLOB := EMPTY_BLOB(); BEGIN RETURN v; END;",
                "CREATE FUNCTION raw_payload RETURN BLOB IS v BLOB := HEXTORAW('AB'); BEGIN v := HEXTORAW('CD'); RETURN v; END;",
                "CREATE FUNCTION direct_payload RETURN BLOB IS BEGIN RETURN HEXTORAW('EF'); END;",
                "CREATE FUNCTION nested_payload RETURN BLOB IS BEGIN DECLARE v BLOB := HEXTORAW('12'); BEGIN RETURN v; END; END;",
            ] {
                run(&mut db, source).unwrap_or_else(|error| panic!("{source}: {error}"));
            }
        }
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        for (function, expected, temporary) in [
            ("empty_payload", "\\x", false),
            ("raw_payload", "\\xcd", true),
            ("direct_payload", "\\xef", true),
            ("nested_payload", "\\x12", true),
        ] {
            let result = run(&mut db, &format!("SELECT {function}() FROM DUAL")).unwrap();
            assert_eq!(result.rows[0][0].to_cell(), expected);
            assert!(
                matches!(&result.rows[0][0], SqlValue::OracleLob(value) if value.is_temporary() == temporary)
            );
        }
        run(
            &mut db,
            "DECLARE v BLOB := EMPTY_BLOB(); BEGIN v := NULL; END;",
        )
        .unwrap();
        assert!(run(&mut db, "DECLARE v BLOB := '\\xAB'; BEGIN NULL; END;").is_err());
    }

    #[test]
    fn blob_declaration_scope_is_nested_and_does_not_enable_postgres() {
        assert!(!enabled());
        {
            let _outer = Scope::new(true);
            assert!(routine_type(&DataType::Blob(None)).unwrap().is_some());
            {
                let _inner = Scope::new(false);
                assert!(routine_type(&DataType::Blob(None)).unwrap().is_none());
            }
            assert!(enabled());
        }
        assert!(!enabled());
        assert!(routine_type(&DataType::Blob(None)).unwrap().is_none());
    }

    #[test]
    fn persisted_v28_binary_returns_keep_value_semantics() {
        let root = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open_with_config(
                root.path(),
                bicdb_core::DbConfig::default().with_fsync(false),
            )
            .unwrap();
            ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
            // This is the declaration an older compiler actually saved: its
            // canonicalizer flattened BLOB to BYTEA before persistence.
            SqlSession::new(&mut db).execute("CREATE FUNCTION previous_blob() RETURNS BYTEA LANGUAGE bicdb_oracle_preview_v28 AS $$BEGIN RETURN bicdb_oracle_hextoraw('AB'); END;$$").unwrap();
        }
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        let value = run(&mut db, "SELECT previous_blob() FROM DUAL")
            .unwrap()
            .rows[0][0]
            .clone();
        assert_eq!(value, SqlValue::String("\\xab".into()));
        assert!(!matches!(value, SqlValue::OracleLob(_)));
        let mut session = SqlSession::new(&mut db);
        assert!(session
            .execute("DO LANGUAGE bicdb_oracle_preview_v28 $$DECLARE v BLOB; BEGIN NULL; END;$$")
            .is_err());
        let native = tempfile::tempdir().unwrap();
        let mut pg = BicDb::open(native.path()).unwrap();
        let mut session = SqlSession::new(&mut pg);
        session.execute("CREATE FUNCTION native_bytes() RETURNS bytea LANGUAGE plpgsql AS $$DECLARE v bytea := '\\xab'::bytea; BEGIN RETURN v; END;$$").unwrap();
        assert_eq!(
            session.execute("SELECT native_bytes()").unwrap().rows[0][0],
            SqlValue::String("\\xab".into())
        );
        assert!(!enabled());
    }

    #[test]
    fn temporary_package_append_preserves_aliases_and_self_source() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db, "CREATE FUNCTION alias_payload(which_value NUMBER) RETURN BLOB IS d BLOB:=HEXTORAW('AB'); e BLOB; BEGIN e:=d; SYS.DBMS_LOB.APPEND(src_lob=>HEXTORAW('01'),dest_lob=>d); IF which_value=1 THEN RETURN d; ELSE RETURN e; END IF; END;").unwrap();
        let result = run(
            &mut db,
            "SELECT alias_payload(1),alias_payload(2) FROM DUAL",
        )
        .unwrap();
        assert_eq!(
            result.rows[0]
                .iter()
                .map(SqlValue::to_cell)
                .collect::<Vec<_>>(),
            vec!["\\xab01", "\\xab"]
        );
        run(&mut db, "CREATE FUNCTION doubled_payload RETURN BLOB IS d BLOB:=HEXTORAW('AB'); BEGIN DBMS_LOB.APPEND(d,d); RETURN d; END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT doubled_payload() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "\\xabab"
        );
        run(&mut db, "CREATE FUNCTION measured_payload RETURN NUMBER IS d BLOB:=HEXTORAW('AB'); BEGIN DBMS_LOB.APPEND(d,HEXTORAW('0102')); RETURN DBMS_LOB.GETLENGTH(lob_loc=>d)*10+SYS.DBMS_LOB.ISTEMPORARY(d); END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT measured_payload() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "31"
        );
    }

    #[test]
    fn temporary_package_values_survive_transaction_controls() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(
            &mut db,
            "CREATE TABLE temp_lob_results(id NUMBER,payload BLOB)",
        )
        .unwrap();
        for (id, control) in [(1, "COMMIT"), (2, "ROLLBACK"), (3, "ROLLBACK TO lob_point")] {
            run(&mut db, &format!("DECLARE d BLOB:=HEXTORAW('AB'); BEGIN SAVEPOINT lob_point; DBMS_LOB.APPEND(d,HEXTORAW('01')); {control}; INSERT INTO temp_lob_results VALUES({id},d); END;")).unwrap();
        }
        let result = run(&mut db, "SELECT payload FROM temp_lob_results ORDER BY id").unwrap();
        assert_eq!(result.rows.len(), 3);
        assert!(result.rows.iter().all(|row| row[0].to_cell() == "\\xab01"));
    }

    #[test]
    fn blob_procedure_inout_and_sorted_function_results_keep_provenance() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default()
                .with_fsync(false)
                .with_query_work_memory_bytes(64 * 1024),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db,"CREATE PROCEDURE extend_payload(d IN OUT BLOB,s IN BLOB) IS BEGIN DBMS_LOB.APPEND(d,s); END;").unwrap();
        run(&mut db,"CREATE FUNCTION through_procedure RETURN BLOB IS d BLOB:=HEXTORAW('AB'); s BLOB:=HEXTORAW('01'); BEGIN extend_payload(d,s); RETURN d; END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT through_procedure() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "\\xab01"
        );
        let payload = "AB".repeat(1024);
        run(&mut db,&format!("CREATE FUNCTION sorted_payload RETURN BLOB IS d BLOB:=HEXTORAW('{payload}'); BEGIN RETURN d; END;")).unwrap();
        run(&mut db, "CREATE TABLE blob_sort_inputs(id INTEGER)").unwrap();
        SqlSession::new(&mut db)
            .execute("INSERT INTO \"BLOB_SORT_INPUTS\" SELECT generate_series(1,160)")
            .unwrap();
        let result = run(
            &mut db,
            "SELECT id,sorted_payload() FROM blob_sort_inputs ORDER BY id DESC",
        )
        .unwrap();
        assert_eq!(result.rows.len(), 160);
        for (index, row) in result.rows.iter().enumerate() {
            assert_eq!(row[0].to_cell(), (160 - index).to_string());
            assert!(
                matches!(&row[1],SqlValue::OracleLob(value) if value.is_temporary() && value.bytes()==vec![0xAB;1024])
            );
        }
    }

    #[test]
    fn temporary_package_null_priority_and_empty_destination_errors() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        for (destination, source, expected) in [
            ("EMPTY_BLOB()", "HEXTORAW('01')", "ORA-22275"),
            ("EMPTY_BLOB()", "NULL", "ORA-06502"),
            ("NULL", "EMPTY_BLOB()", "ORA-06502"),
            ("HEXTORAW('AB')", "NULL", "ORA-06502"),
        ] {
            let error = run(
                &mut db,
                &format!("DECLARE d BLOB:={destination}; BEGIN DBMS_LOB.APPEND(d,{source}); END;"),
            )
            .unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "{destination}, {source}: {error}"
            );
        }
        run(
            &mut db,
            "DECLARE d BLOB:=HEXTORAW('AB'); BEGIN DBMS_LOB.APPEND(d,EMPTY_BLOB()); END;",
        )
        .unwrap();
        run(&mut db, "CREATE FUNCTION empty_measure RETURN NUMBER IS d BLOB:=EMPTY_BLOB(); BEGIN RETURN DBMS_LOB.GETLENGTH(d)+DBMS_LOB.ISTEMPORARY(d); END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT empty_measure() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "0"
        );
        run(&mut db, "CREATE FUNCTION null_measure RETURN NUMBER IS d BLOB; BEGIN RETURN DBMS_LOB.GETLENGTH(d); END;").unwrap();
        assert_eq!(
            run(&mut db, "SELECT null_measure() FROM DUAL")
                .unwrap()
                .rows[0][0],
            SqlValue::Null
        );
    }
    #[test]
    fn query_empty_destination_obeys_sql_write_restriction_after_null_check() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        run(&mut db,"CREATE FUNCTION query_empty_write RETURN BLOB IS d BLOB:=EMPTY_BLOB(); BEGIN DBMS_LOB.APPEND(d,HEXTORAW('01')); RETURN d; END;").unwrap();
        let error = run(&mut db, "SELECT query_empty_write() FROM DUAL").unwrap_err();
        assert!(error.to_string().contains("ORA-14553"), "{error}");
        run(&mut db,"CREATE FUNCTION query_null_write RETURN BLOB IS d BLOB:=EMPTY_BLOB(); BEGIN DBMS_LOB.APPEND(d,NULL); RETURN d; END;").unwrap();
        let error = run(&mut db, "SELECT query_null_write() FROM DUAL").unwrap_err();
        assert!(error.to_string().contains("ORA-06502"), "{error}");
    }

    #[test]
    fn empty_blob_is_non_null_binary_and_cannot_be_deserialized_as_a_locator() {
        let value = scalar("bicdb_oracle_empty_blob", &[]).unwrap().unwrap();
        assert!(
            matches!(&value,SqlValue::OracleLob(blob) if blob.bytes().is_empty() && !blob.is_temporary())
        );
        assert_eq!(value.to_cell(), "\\x");
        let json = serde_json::to_string(&value).unwrap();
        let decoded: SqlValue = serde_json::from_str(&json).unwrap();
        assert!(matches!(decoded, SqlValue::String(_)));
        let attempted: SqlValue = serde_json::from_str(
            r#"{"storage":"persistent","relation":"any","value_id":"fabricated"}"#,
        )
        .unwrap();
        assert!(!matches!(attempted, SqlValue::OracleLob(_)));
    }
    #[test]
    fn empty_blob_sql_is_non_null_and_has_binary_result_metadata() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let sql = lower_session_sql(
            &db,
            "SELECT EMPTY_BLOB() AS payload,EMPTY_BLOB() IS NULL AS missing FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
            None,
            "bicdb",
        )
        .unwrap();
        let result = SqlSession::new(&mut db).execute(&sql).unwrap();
        assert!(matches!(&result.rows[0][0],SqlValue::OracleLob(blob) if blob.bytes().is_empty()));
        assert_eq!(result.rows[0][1], SqlValue::Bool(false));
        assert_eq!(
            infer_query_result_types(&db, &sql).unwrap().unwrap()[0],
            Some("bytea".into())
        );
        let native = tempfile::tempdir().unwrap();
        let mut pg = BicDb::open(native.path()).unwrap();
        assert!(SqlSession::new(&mut pg)
            .execute("SELECT EMPTY_BLOB()")
            .is_err());
    }

    #[test]
    fn storing_empty_blob_keeps_zero_bytes_distinct_from_null() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        for source in [
            "CREATE TABLE blob_payloads(id NUMBER PRIMARY KEY,payload BLOB)",
            "INSERT INTO blob_payloads VALUES(1,EMPTY_BLOB())",
            "INSERT INTO blob_payloads VALUES(2,NULL)",
        ] {
            let sql =
                lower_session_sql(&db, source, ORACLE_PREVIEW_PROFILE, None, "bicdb").unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap();
        }
        let sql = lower_session_sql(
            &db,
            "SELECT payload,payload IS NULL AS missing FROM blob_payloads ORDER BY id",
            ORACLE_PREVIEW_PROFILE,
            None,
            "bicdb",
        )
        .unwrap();
        let result = SqlSession::new(&mut db).execute(&sql).unwrap();
        assert_eq!(result.rows[0][0].to_cell(), "\\x");
        assert_eq!(result.rows[0][1], SqlValue::Bool(false));
        assert!(matches!(result.rows[1][0], SqlValue::Null));
        assert_eq!(result.rows[1][1], SqlValue::Bool(true));
    }

    #[test]
    fn temporary_assignment_aliases_copy_on_write_and_empty_locator_is_read_only() {
        let mut first = from_raw(SqlValue::String("\\xab".into())).unwrap();
        let original = first.clone();
        let SqlValue::OracleLob(value) = &mut first else {
            panic!("typed value");
        };
        Arc::make_mut(value).append_temporary(&[1]).unwrap();
        assert_eq!(first.to_cell(), "\\xab01");
        assert_eq!(original.to_cell(), "\\xab");
        let mut empty = Value::empty();
        assert!(empty
            .append_temporary(&[1])
            .unwrap_err()
            .to_string()
            .contains("ORA-22275"));
        assert!(coerce(SqlValue::String("\\xab".into())).is_err());
    }
}
